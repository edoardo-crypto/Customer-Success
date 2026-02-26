#!/usr/bin/env python3
"""
sync_next_checkin.py
---------------------
Computes and writes "📞 Next Scheduled Check-in" for every active customer
in Notion based on their Last Contact Date and Tier:

  Tier 1 → +14 days
  Tier 2 → +42 days
  Tier 3 → +84 days (also the default when tier is missing)

Rules applied before computing:
  1. If Last Contact Date is more than 6 months ago, cap it to 6 months ago
     (and update the field in Notion so it stays consistent).
  2. After computing next_checkin = capped_last_contact + interval,
     if the result is still before today → set to NEXT_MONDAY.
  3. If Last Contact Date is missing entirely → set to NEXT_MONDAY.

Usage:
  python3 sync_next_checkin.py
"""

import json
import urllib.request
from datetime import date, timedelta

# ── Config ────────────────────────────────────────────────────────────────────

NOTION_TOKEN          = "***REMOVED***"
NOTION_DATA_SOURCE_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

TIER_INTERVALS = {
    "Tier 1": 14,
    "Tier 2": 42,
    "Tier 3": 84,
}
DEFAULT_INTERVAL = 84

TODAY          = date.today()
SIX_MONTHS_AGO = TODAY - timedelta(days=180)
NEXT_MONDAY    = date(2026, 2, 23)


# ── Notion helpers ────────────────────────────────────────────────────────────

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


# ── Fetch active customers ────────────────────────────────────────────────────

def fetch_active_customers():
    customers = []
    cursor    = None

    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        resp = notion_request(
            "POST",
            f"data_sources/{NOTION_DATA_SOURCE_ID}/query",
            body,
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

            tier_val = (
                ((props.get("🏅 Tier") or {}).get("select") or {})
                .get("name", None)
            )

            last_contact_raw = (
                ((props.get("📞 Last Contact Date 🔒") or {}).get("date") or {})
                .get("start", None)
            )
            last_contact = None
            if last_contact_raw:
                try:
                    last_contact = date.fromisoformat(last_contact_raw[:10])
                except ValueError:
                    pass

            customers.append({
                "page_id":      page["id"],
                "name":         name,
                "tier":         tier_val,
                "last_contact": last_contact,
            })

        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break

    return customers


# ── Compute dates ─────────────────────────────────────────────────────────────

def compute_row(c):
    """
    Returns enriched dict with:
      effective_last  — the date we'll use for computation (may be capped)
      cap_last_contact — True if Last Contact Date needs to be updated in Notion
      next_date        — final Next Scheduled Check-in
      note             — human-readable explanation
    """
    last = c["last_contact"]
    tier = c["tier"]

    # Case 1: no last contact at all
    if last is None:
        return {**c,
                "effective_last":    None,
                "cap_last_contact":  False,
                "next_date":         NEXT_MONDAY,
                "note":              "no last contact → flagged"}

    # Case 2: last contact more than 6 months ago → cap it
    if last < SIX_MONTHS_AGO:
        effective = SIX_MONTHS_AGO
        cap       = True
    else:
        effective = last
        cap       = False

    # Compute candidate next check-in
    days      = TIER_INTERVALS.get(tier, DEFAULT_INTERVAL)
    next_date = effective + timedelta(days=days)

    # Case 3: result is still in the past → flag for next Monday
    if next_date < TODAY:
        note      = "overdue → flagged"
        next_date = NEXT_MONDAY
    else:
        note = "capped to 6 months ago" if cap else ""

    return {**c,
            "effective_last":   effective,
            "cap_last_contact": cap,
            "next_date":        next_date,
            "note":             note}


# ── Notion PATCH helpers ──────────────────────────────────────────────────────

def update_last_contact(page_id, contact_date):
    body = {"properties": {"📞 Last Contact Date 🔒": {"date": {"start": contact_date.isoformat()}}}}
    notion_request("PATCH", f"pages/{page_id}", body)


def update_next_checkin(page_id, next_date):
    body = {"properties": {"📞 Next Scheduled Check-in": {"date": {"start": next_date.isoformat()}}}}
    notion_request("PATCH", f"pages/{page_id}", body)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 80)
    print("  Notion: Populate Next Scheduled Check-in  (with overdue/6-month capping)")
    print("=" * 80)
    print(f"  Today: {TODAY}  |  6-months-ago cap: {SIX_MONTHS_AGO}  |  Fallback: {NEXT_MONDAY}")

    print("\n[1/3] Fetching active customers from Notion...")
    customers = fetch_active_customers()
    print(f"  Found {len(customers)} active customers")

    print("\n[2/3] Computing dates...")
    rows    = [compute_row(c) for c in customers]
    flagged = sum(1 for r in rows if r["next_date"] == NEXT_MONDAY)
    capped  = sum(1 for r in rows if r["cap_last_contact"])
    print(f"  Flagged → {NEXT_MONDAY}: {flagged}  |  Last-contact capped to 6 months: {capped}")

    print("\n[3/3] Updating Notion...")
    print()

    hdr = f"  {'Customer':<32} {'Tier':<8} {'Orig Last Contact':<19} {'Effective':<12} {'Next Check-in':<14} Note"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    updated = errors = 0
    for r in sorted(rows, key=lambda x: x["name"].lower()):
        orig_str = r["last_contact"].isoformat() if r["last_contact"] else "(none)"
        eff_str  = r["effective_last"].isoformat() if r.get("effective_last") else "(none)"
        tier_str = r["tier"] or "(none)"

        print(
            f"  {r['name']:<32} {tier_str:<8} {orig_str:<19} {eff_str:<12} "
            f"{r['next_date'].isoformat():<14} {r['note']}"
        )

        try:
            # If last contact was capped, write the capped date back to Notion
            if r["cap_last_contact"]:
                update_last_contact(r["page_id"], SIX_MONTHS_AGO)
            # Always write next check-in
            update_next_checkin(r["page_id"], r["next_date"])
            updated += 1
        except Exception as e:
            print(f"    ✗ Error updating {r['name']}: {e}")
            errors += 1

    print()
    print("=" * 80)
    print(f"  Done.  Updated: {updated}  |  Flagged → {NEXT_MONDAY}: {flagged}  "
          f"|  Last-contact capped: {capped}  |  Errors: {errors}")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
