#!/usr/bin/env python3
"""
sync_contact_reasons.py
------------------------
WHEN TO RUN: Daily, after sync_last_contact.py and sync_next_checkin.py.
Runs automatically via .github/workflows/daily_cs_sync.yml.

Two jobs in one pass:

Job 1 — Compute combined last-contact date per customer:
  • Reads "📅 Last Meeting Date 🔒" (GCal meetings) from every active MCT row
  • Fetches Intercom closed conversations from the last LOOKBACK_DAYS days
  • Matches conversation contact email domain → MCT customer domain
  • combined = max(meeting_date, intercom_date) per customer
  • Writes combined date to "📞 Last Contact Date 🔒"

Job 2 — Set/clear "💎 Reason for contact":
  • Bug-fixed entries ("Bug fixed! 🎉...") are left as-is — set by n8n webhook
  • If combined date was updated in the last 48h → clear (customer was just reached)
  • Else if Health = Red AND days since combined ≥ 14 → "At risk + no contact in 14+ days"
  • Else → clear (no action needed)

Usage:
  python3 sync_contact_reasons.py            # live run
  python3 sync_contact_reasons.py --dry-run  # print plan, no writes
"""

import json
import re
import sys
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta, timezone
import creds

# ── Config ────────────────────────────────────────────────────────────────────
NOTION_TOKEN      = creds.get("NOTION_TOKEN")
MCT_DS_ID         = "3ceb1ad0-91f1-40db-945a-c51c58035898"
INTERCOM_TOKEN    = creds.get("INTERCOM_TOKEN")

# How far back to pull Intercom conversations
LOOKBACK_DAYS = 180

# CS manager admin IDs in Intercom
ALEX_ADMIN_ID = "7484673"
AYA_ADMIN_ID  = "8411967"

# Generic/personal email domains — skip these for domain matching
GENERIC_DOMAINS = {
    "gmail.com", "hotmail.com", "outlook.com", "yahoo.com",
    "icloud.com", "protonmail.com", "live.com", "me.com",
    "konvoai.com",
}

# Contact rules
RED_THRESHOLD_DAYS = 14   # Red health customers: flag if no contact ≥ this many days
RECENTLY_UPDATED_HOURS = 48  # "just contacted" window — clear reason if combined date updated

# Reason option values (must match MCT select options exactly)
REASON_AT_RISK = "At risk + no contact in 14+ days"
BUG_FIXED_PREFIX = "Bug fixed!"  # webhook-set value starts with this

DRY_RUN = "--dry-run" in sys.argv
TODAY   = date.today()


# ── Notion helper ─────────────────────────────────────────────────────────────

def notion_request(method, path, body=None, version="2022-06-28"):
    url  = f"https://api.notion.com/v1/{path}"
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization",  f"Bearer {NOTION_TOKEN}")
    req.add_header("Notion-Version", version)
    req.add_header("Content-Type",   "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        raise Exception(f"HTTP {e.code} {e.reason} — {body_text}") from None


# ── Intercom helper ───────────────────────────────────────────────────────────

def intercom_request(method, path, body=None):
    url  = f"https://api.intercom.io/{path}"
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization",    f"Bearer {INTERCOM_TOKEN}")
    req.add_header("Intercom-Version", "2.11")
    req.add_header("Accept",           "application/json")
    req.add_header("Content-Type",     "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        raise Exception(f"HTTP {e.code} {e.reason} — {body_text}") from None


# ── Step 1: Fetch MCT active customers ────────────────────────────────────────

def fetch_active_customers():
    """
    Returns list of dicts:
      { page_id, name, domain, meeting_date (date|None),
        health (str), current_reason (str|None) }
    """
    customers = []
    cursor    = None

    while True:
        req_body = {"page_size": 100}
        if cursor:
            req_body["start_cursor"] = cursor

        resp = notion_request(
            "POST",
            f"data_sources/{MCT_DS_ID}/query",
            req_body,
            version="2025-09-03",
        )

        for page in resp.get("results", []):
            props = page.get("properties", {})

            billing_val = (
                ((props.get("💰 Billing Status") or {}).get("select") or {})
                .get("name", "")
            )
            if billing_val != "Active":
                continue

            name_parts = (props.get("🏢 Company Name") or {}).get("title", [])
            name = "".join(t.get("plain_text", "") for t in name_parts).strip()
            if not name:
                continue

            domain_parts = (props.get("🏢 Domain") or {}).get("rich_text", [])
            domain = "".join(t.get("plain_text", "") for t in domain_parts).strip().lower()
            # Normalize: strip protocol, www, trailing slash
            domain = re.sub(r"^https?://", "", domain)
            domain = re.sub(r"^www\.", "", domain)
            domain = domain.rstrip("/")

            meeting_date_raw = (
                ((props.get("📅 Last Meeting Date 🔒") or {}).get("date") or {})
                .get("start", None)
            )
            meeting_date = None
            if meeting_date_raw:
                try:
                    meeting_date = date.fromisoformat(meeting_date_raw[:10])
                except ValueError:
                    pass

            health_raw = (
                ((props.get("🚦 Health Status") or {}).get("formula") or {})
                .get("string", "") or ""
            )
            health = "Red" if "Red" in health_raw else health_raw

            current_reason = (
                ((props.get("💎 Reason for contact") or {}).get("select") or {})
                .get("name", None)
            )

            customers.append({
                "page_id":        page["id"],
                "name":           name,
                "domain":         domain,
                "meeting_date":   meeting_date,
                "health":         health,
                "current_reason": current_reason,
            })

        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break

    return customers


# ── Step 2: Fetch Intercom closed conversations ───────────────────────────────

def fetch_intercom_last_contact_by_domain():
    """
    Fetches all conversations closed by Alex or Aya in the past LOOKBACK_DAYS.
    Returns { domain: date } — most recent closed conversation date per domain.
    Only conversations where the closing admin is Alex or Aya are included.
    """
    lookback_dt = datetime.combine(
        TODAY - timedelta(days=LOOKBACK_DAYS), datetime.min.time()
    ).replace(tzinfo=timezone.utc)
    lookback_ts = int(lookback_dt.timestamp())

    search_body = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "open",                     "operator": "=",  "value": False},
                {"field": "statistics.last_close_at", "operator": ">",  "value": lookback_ts},
            ],
        },
        "pagination": {"per_page": 150},
    }

    domain_last_date = {}  # domain → most recent closed date
    contact_id_to_email = {}  # Intercom contact_id → email (fetched lazily)

    page_num = 0
    next_page_params = None

    while True:
        page_num += 1
        if next_page_params:
            body = {**search_body, "pagination": {**search_body["pagination"], **next_page_params}}
        else:
            body = search_body

        try:
            resp = intercom_request("POST", "conversations/search", body)
        except Exception as e:
            print(f"  ⚠ Intercom search error (page {page_num}): {e}")
            break

        conversations = resp.get("conversations", [])
        if not conversations:
            break

        for conv in conversations:
            stats  = conv.get("statistics") or {}
            closer = str(stats.get("last_closed_by_id") or "")
            if closer not in (ALEX_ADMIN_ID, AYA_ADMIN_ID):
                continue

            # Get the close date
            last_close_ts = stats.get("last_close_at")
            if not last_close_ts:
                continue
            try:
                close_date = date.fromtimestamp(last_close_ts)
            except (ValueError, OSError):
                continue

            # Extract contact email domain.
            # Conversation search embeds contact stubs — email may be in the stub
            # or we may need to look it up via the contacts API.
            contacts = (conv.get("contacts") or {}).get("contacts") or []
            for contact_stub in contacts:
                # Try the embedded email first
                email = (contact_stub.get("email") or "").lower().strip()

                # If not present, fetch the contact record (cached)
                if not email:
                    cid = contact_stub.get("id") or contact_stub.get("contact_id", "")
                    if cid:
                        if cid not in contact_id_to_email:
                            try:
                                c_resp = intercom_request("GET", f"contacts/{cid}")
                                contact_id_to_email[cid] = (
                                    c_resp.get("email") or ""
                                ).lower().strip()
                            except Exception:
                                contact_id_to_email[cid] = ""
                        email = contact_id_to_email[cid]

                if not email or "@" not in email:
                    continue
                domain = email.split("@")[1]
                if domain in GENERIC_DOMAINS:
                    continue
                # Keep the most recent date per domain
                if domain not in domain_last_date or close_date > domain_last_date[domain]:
                    domain_last_date[domain] = close_date

        # Pagination
        pagination = resp.get("pages") or {}
        next_page  = pagination.get("next") or {}
        if next_page.get("starting_after"):
            next_page_params = {"starting_after": next_page["starting_after"]}
        else:
            break

    if contact_id_to_email:
        print(f"  (fetched {len(contact_id_to_email)} contact records for email lookup)")

    return domain_last_date


# ── Step 3: Notion writes ─────────────────────────────────────────────────────

def patch_customer(page_id, combined_date, new_reason):
    """
    Write combined_date → 📞 Last Contact Date 🔒
    Write new_reason    → 💎 Reason for contact (None → clear)
    Single PATCH call.
    """
    reason_value = {"select": {"name": new_reason}} if new_reason else {"select": None}
    body = {
        "properties": {
            "📞 Last Contact Date 🔒": {
                "date": {"start": combined_date.isoformat()}
            },
            "💎 Reason for contact": reason_value,
        }
    }
    notion_request("PATCH", f"pages/{page_id}", body, version="2025-09-03")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 70)
    print("  sync_contact_reasons.py")
    if DRY_RUN:
        print("  *** DRY RUN — no writes ***")
    print("=" * 70)

    # 1. Fetch MCT active customers
    print("\n[1/4] Fetching active customers from Notion MCT...")
    customers = fetch_active_customers()
    print(f"  {len(customers)} active customers found")

    # 2. Fetch Intercom last-contact dates by domain
    print(f"\n[2/4] Fetching Intercom closed conversations (last {LOOKBACK_DAYS} days)...")
    intercom_dates = fetch_intercom_last_contact_by_domain()
    print(f"  {len(intercom_dates)} unique domains with recent Intercom contact")

    # 3. Compute combined dates and determine reasons
    print("\n[3/4] Computing combined dates and reasons...")
    print()
    hdr = (f"  {'Customer':<32} {'Meeting':<12} {'Intercom':<12} "
           f"{'Combined':<12} {'Days':<5} {'New Reason'}")
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    rows = []
    for c in sorted(customers, key=lambda x: x["name"].lower()):
        meeting_date  = c["meeting_date"]
        domain        = c["domain"]
        intercom_date = intercom_dates.get(domain)

        # Combined = max of both signals
        candidates = [d for d in [meeting_date, intercom_date] if d is not None]
        combined   = max(candidates) if candidates else None

        # Days since combined date
        days_since = (TODAY - combined).days if combined else None

        # Determine new reason
        cur_reason = c["current_reason"] or ""

        if cur_reason.startswith(BUG_FIXED_PREFIX):
            # Webhook-set — leave it alone
            new_reason = cur_reason
            reason_note = "(bug-fixed, kept)"
        elif combined and days_since is not None and days_since < RECENTLY_UPDATED_HOURS // 24:
            # Just contacted (within 48h) — clear
            new_reason  = None
            reason_note = "(recently contacted → clear)"
        elif combined is None or days_since is None:
            # No contact on record at all — only flag if Red health
            if c["health"] == "Red":
                new_reason  = REASON_AT_RISK
                reason_note = "(no contact, red health)"
            else:
                new_reason  = None
                reason_note = "(no contact, not red → ok)"
        elif c["health"] == "Red" and days_since >= RED_THRESHOLD_DAYS:
            new_reason  = REASON_AT_RISK
            reason_note = f"(red, {days_since}d)"
        else:
            new_reason  = None
            reason_note = f"({days_since}d, ok)"

        display_combined = combined.isoformat() if combined else "(none)"
        display_meeting  = meeting_date.isoformat() if meeting_date else "(none)"
        display_intercom = intercom_date.isoformat() if intercom_date else "(none)"
        display_days     = str(days_since) if days_since is not None else "—"
        display_reason   = new_reason or f"(clear) {reason_note}" if new_reason is None else f"{new_reason} {reason_note}"

        print(
            f"  {c['name']:<32} {display_meeting:<12} {display_intercom:<12} "
            f"{display_combined:<12} {display_days:<5} {display_reason}"
        )

        rows.append({**c,
                     "combined":   combined,
                     "new_reason": new_reason,
                     "reason_note": reason_note})

    # 4. Write to Notion
    if DRY_RUN:
        print()
        print("  *** DRY RUN — skipping Notion writes ***")
        print()
        return

    print(f"\n[4/4] Updating Notion (📞 Last Contact Date 🔒 + 💎 Reason for contact)...")
    updated = errors = skipped = 0

    for r in rows:
        combined   = r["combined"]
        new_reason = r["new_reason"]

        # If we have no combined date AND reason is unchanged, nothing to write
        if combined is None and new_reason == r["current_reason"]:
            skipped += 1
            continue

        try:
            if combined is not None:
                # Write both date and reason in one PATCH
                patch_customer(r["page_id"], combined, new_reason)
            else:
                # No date to write — only update reason
                reason_value = {"select": {"name": new_reason}} if new_reason else {"select": None}
                body = {"properties": {"💎 Reason for contact": reason_value}}
                notion_request("PATCH", f"pages/{r['page_id']}", body, version="2025-09-03")
            updated += 1
        except Exception as e:
            print(f"  ✗ Error updating {r['name']}: {e}")
            errors += 1

    print()
    print("=" * 70)
    print(f"  Done.  Updated: {updated}  |  Skipped (no combined date): {skipped}  |  Errors: {errors}")
    print("=" * 70)
    print()


if __name__ == "__main__":
    main()
