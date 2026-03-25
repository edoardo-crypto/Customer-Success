"""
audit_mrr.py

Audits MCT MRR accuracy by searching Stripe by customer name instead of
relying on the stored Stripe Customer ID (which may be stale).

Usage:
    python3 audit_mrr.py           # print diff table only — no Notion writes
    python3 audit_mrr.py --apply   # print diff table, then patch Notion
"""

import sys
import json
import time
import requests

# ── CREDENTIALS ───────────────────────────────────────────────────────────────

creds_raw = open("Credentials.md").read()

def _extract_block(header):
    """Return first ```…``` block after the given ## header."""
    try:
        start = creds_raw.index(f"## {header}")
        block_start = creds_raw.index("```", start) + 3
        if creds_raw[block_start] not in ("\n", "\r"):
            block_start = creds_raw.index("\n", block_start) + 1
        block_end = creds_raw.index("```", block_start)
        return creds_raw[block_start:block_end].strip()
    except ValueError:
        raise RuntimeError(f"Could not find ## {header} block in Credentials.md")

def _extract_labeled_block(section_header, label):
    """Return first ```…``` block after `label` within `## section_header`."""
    section_start = creds_raw.index(f"## {section_header}")
    label_pos = creds_raw.index(label, section_start)
    block_start = creds_raw.index("```", label_pos) + 3
    if creds_raw[block_start] not in ("\n", "\r"):
        block_start = creds_raw.index("\n", block_start) + 1
    block_end = creds_raw.index("```", block_start)
    return creds_raw[block_start:block_end].strip()

STRIPE_KEY   = _extract_block("Stripe")
NOTION_TOKEN = _extract_block("Notion")

APPLY = "--apply" in sys.argv

MCT_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

NOTION_HDR = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type":  "application/json",
}


# ── HELPERS: property extraction ──────────────────────────────────────────────

def get_title(page, prop):
    p = page.get("properties", {}).get(prop)
    return "".join(t.get("plain_text", "") for t in (p or {}).get("title", []))

def get_rich_text(page, prop):
    p = page.get("properties", {}).get(prop)
    return "".join(t.get("plain_text", "") for t in (p or {}).get("rich_text", []))

def get_number(page, prop):
    p = page.get("properties", {}).get(prop)
    return p.get("number") if p else None

def get_select(page, prop):
    p = page.get("properties", {}).get(prop)
    return ((p or {}).get("select") or {}).get("name")


# ── NOTION: fetch all MCT rows ─────────────────────────────────────────────────

def fetch_all_mct():
    pages = []
    body  = {"page_size": 100}
    url   = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"
    print("🏢 Fetching MCT rows…")
    while True:
        r = requests.post(url, headers=NOTION_HDR, json=body, timeout=60)
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        body["start_cursor"] = data["next_cursor"]
    print(f"   → {len(pages)} total rows")
    return pages


# ── MRR COMPUTATION (mirrors the JS computeMrr in the n8n workflow) ────────────

def compute_mrr(subscription):
    """Sum all line items on a Stripe subscription → monthly MRR."""
    mrr = 0
    for item in subscription.get("items", {}).get("data", []):
        price          = item.get("price") or item.get("plan") or {}
        amount         = (
            price.get("unit_amount")
            or (item.get("plan") or {}).get("amount")
            or 0
        )
        recurring      = price.get("recurring") or {}
        interval       = recurring.get("interval") or (item.get("plan") or {}).get("interval") or "month"
        interval_count = recurring.get("interval_count") or (item.get("plan") or {}).get("interval_count") or 1
        qty            = item.get("quantity") or 1

        if interval == "month" and interval_count == 1:
            mrr += (amount * qty) / 100
        elif interval == "year" or (interval == "month" and interval_count == 12):
            mrr += (amount * qty) / 100 / 12
        elif interval == "month":
            mrr += (amount * qty) / 100 / interval_count
        # else: skip unknown intervals (e.g. one-time)

    return round(mrr, 2)


# ── STRIPE: search customer by name ──────────────────────────────────────────

def stripe_search_by_name(name):
    """
    Search Stripe customers by name. Returns list of customer dicts.
    Tries exact name first; on zero results retries with first two words.
    """
    def _search(query):
        r = requests.get(
            "https://api.stripe.com/v1/customers/search",
            auth=(STRIPE_KEY, ""),
            params={"query": query, "limit": 5},
            timeout=20,
        )
        r.raise_for_status()
        return r.json().get("data", [])

    results = _search(f'name:"{name}"')
    if results:
        return results, "exact"

    # Retry with first two words of the name
    words = name.split()
    if len(words) >= 2:
        partial = " ".join(words[:2])
        results = _search(f'name:"{partial}"')
        if results:
            return results, "partial"

    return [], "none"


# ── STRIPE: fetch active subscriptions for a customer ────────────────────────

def fetch_customer_subscriptions(cid):
    """Return list of active subscriptions for a given Stripe customer ID."""
    subs   = []
    params = {
        "customer":  cid,
        "status":    "active",
        "expand[]":  "data.items",
        "limit":     10,
    }
    r = requests.get(
        "https://api.stripe.com/v1/subscriptions",
        auth=(STRIPE_KEY, ""),
        params=params,
        timeout=20,
    )
    r.raise_for_status()
    subs.extend(r.json().get("data", []))
    return subs


# ── CORE LOGIC: resolve a single MCT row ─────────────────────────────────────

def resolve_row(company_name, stored_cid):
    """
    Search Stripe for `company_name`, pick the best match, compute MRR.

    Returns dict:
        found_cid   : str | None
        new_mrr     : float | None
        action      : 'ok' | 'MRR' | 'ID' | 'ID + MRR' | 'NOT FOUND' | 'REVIEW'
        note        : str  (human-readable detail)
    """
    candidates, match_type = stripe_search_by_name(company_name)

    if not candidates:
        return {"found_cid": None, "new_mrr": None, "action": "NOT FOUND", "note": "no Stripe customer found"}

    # Pick the best candidate
    if len(candidates) == 1:
        chosen = candidates[0]
        note   = f"{match_type} match"
    else:
        # Prefer the one whose ID matches the stored ID
        same_id = [c for c in candidates if c["id"] == stored_cid]
        if same_id:
            chosen = same_id[0]
            note   = f"{match_type} match, picked stored ID from {len(candidates)} results"
        else:
            # Pick the customer with the latest active subscription (most recent created)
            def _latest_sub_ts(c):
                subs = fetch_customer_subscriptions(c["id"])
                if not subs:
                    return 0
                return max(s.get("created", 0) for s in subs)

            scored = sorted(candidates, key=_latest_sub_ts, reverse=True)
            best   = scored[0]
            rest   = scored[1:]
            # If best has active subs and no other also has active subs → confident
            best_subs = fetch_customer_subscriptions(best["id"])
            rest_active = any(fetch_customer_subscriptions(c["id"]) for c in rest)
            if best_subs and not rest_active:
                chosen = best
                note   = f"{match_type} match, auto-picked (others have no active subs)"
            else:
                return {
                    "found_cid": None,
                    "new_mrr":   None,
                    "action":    "REVIEW",
                    "note":      f"{len(candidates)} candidates, cannot disambiguate: " +
                                 ", ".join(f"{c['id']} ({c.get('name','?')})" for c in candidates),
                }

    found_cid = chosen["id"]

    # Compute MRR for the found customer
    subs    = fetch_customer_subscriptions(found_cid)
    new_mrr = round(sum(compute_mrr(s) for s in subs), 2)

    return {"found_cid": found_cid, "new_mrr": new_mrr, "action": None, "note": note}


# ── NOTION: patch a page ──────────────────────────────────────────────────────

def patch_page(page_id, props):
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HDR,
        json={"properties": props},
        timeout=20,
    )
    r.raise_for_status()


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    if APPLY:
        print("⚠️  APPLY MODE — Notion will be updated after the audit table.\n")
    else:
        print("🔍 AUDIT MODE — read-only. Pass --apply to write corrections.\n")

    pages = fetch_all_mct()

    # Filter: only Active or Churning
    active_pages = []
    for page in pages:
        status = get_select(page, "💰 Billing Status")
        if status in ("Active", "Churning"):
            active_pages.append(page)

    print(f"\n🔎 Processing {len(active_pages)} Active/Churning rows…\n")

    rows = []

    for page in active_pages:
        company_name = get_title(page, "🏢 Company Name").strip()
        stored_cid   = get_rich_text(page, "🔗 Stripe Customer ID").strip()
        stored_mrr   = get_number(page, "💰 MRR")
        billing      = get_select(page, "💰 Billing Status") or ""
        page_id      = page["id"]

        print(f"  Checking {company_name!r}…", end=" ", flush=True)

        resolved = resolve_row(company_name, stored_cid)
        found_cid = resolved["found_cid"]
        new_mrr   = resolved["new_mrr"]

        # Determine action
        if resolved["action"] in ("NOT FOUND", "REVIEW"):
            action = resolved["action"]
        else:
            id_changed  = found_cid and found_cid != stored_cid
            mrr_changed = new_mrr is not None and new_mrr != (stored_mrr or 0)
            if id_changed and mrr_changed:
                action = "ID + MRR"
            elif id_changed:
                action = "ID"
            elif mrr_changed:
                action = "MRR"
            else:
                action = "ok"

        print(action)

        rows.append({
            "page_id":     page_id,
            "company":     company_name,
            "billing":     billing,
            "stored_cid":  stored_cid,
            "found_cid":   found_cid,
            "stored_mrr":  stored_mrr,
            "new_mrr":     new_mrr,
            "action":      action,
            "note":        resolved["note"],
        })

        # Be polite to Stripe rate limits
        time.sleep(0.15)

    # ── Print diff table ──────────────────────────────────────────────────────

    print()
    print("=" * 100)
    COL = "{:<32} {:<10} {:>10} {:>10}  {:<16}  {:<16}  {}"
    print(COL.format(
        "Company", "Billing", "Old MRR", "New MRR",
        "Old CID (last 8)", "New CID (last 8)", "Action"
    ))
    print("-" * 100)

    action_order = {"ID + MRR": 0, "MRR": 1, "ID": 2, "REVIEW": 3, "NOT FOUND": 4, "ok": 5}
    rows_sorted = sorted(rows, key=lambda r: (action_order.get(r["action"], 9), r["company"].lower()))

    for row in rows_sorted:
        old_mrr_str = f"€{row['stored_mrr']:,.2f}" if row["stored_mrr"] is not None else "—"
        new_mrr_str = f"€{row['new_mrr']:,.2f}"    if row["new_mrr"]    is not None else "—"
        old_cid_str = ("…" + row["stored_cid"][-8:]) if len(row["stored_cid"]) >= 8 else row["stored_cid"] or "—"
        new_cid_str = ("…" + row["found_cid"][-8:])  if row["found_cid"] and len(row["found_cid"]) >= 8 else (row["found_cid"] or "—")

        print(COL.format(
            row["company"][:32],
            row["billing"][:10],
            old_mrr_str,
            new_mrr_str,
            old_cid_str,
            new_cid_str,
            row["action"],
        ))

    print("=" * 100)

    needs_fix  = [r for r in rows if r["action"] not in ("ok", "NOT FOUND", "REVIEW")]
    not_found  = [r for r in rows if r["action"] == "NOT FOUND"]
    review     = [r for r in rows if r["action"] == "REVIEW"]
    ok_rows    = [r for r in rows if r["action"] == "ok"]

    print(f"\nSummary: {len(ok_rows)} ok | {len(needs_fix)} need fix | {len(not_found)} not found | {len(review)} need review")

    if not_found:
        print("\n⚠️  NOT FOUND (no active Stripe sub — may be manually managed or already canceled):")
        for r in not_found:
            print(f"   • {r['company']}")

    if review:
        print("\n⚠️  REVIEW (ambiguous — multiple Stripe customers, could not auto-pick):")
        for r in review:
            print(f"   • {r['company']}: {r['note']}")

    # ── Apply corrections ─────────────────────────────────────────────────────

    if not APPLY:
        print("\nRe-run with --apply to write corrections to Notion.")
        return

    if not needs_fix:
        print("\n✅ Nothing to fix.")
        return

    print(f"\n🔧 Applying {len(needs_fix)} correction(s) to Notion…\n")
    updated = 0
    failed  = 0

    for row in needs_fix:
        props = {}

        if row["action"] in ("ID", "ID + MRR"):
            props["🔗 Stripe Customer ID"] = {
                "rich_text": [{"text": {"content": row["found_cid"]}}]
            }

        if row["action"] in ("MRR", "ID + MRR"):
            props["💰 MRR"] = {"number": row["new_mrr"]}

        try:
            patch_page(row["page_id"], props)
            old_mrr_str = f"€{row['stored_mrr'] or 0:,.2f}"
            new_mrr_str = f"€{row['new_mrr']:,.2f}" if row["new_mrr"] is not None else "—"
            print(f"   ✅  {row['company']:<32}  MRR: {old_mrr_str} → {new_mrr_str}  [{row['action']}]")
            updated += 1
        except Exception as e:
            print(f"   ❌  {row['company']:<32}  ERROR: {e}")
            failed += 1

        time.sleep(0.3)  # stay well within Notion rate limits

    print(f"\n✅ Done — updated: {updated}, failed: {failed}")


if __name__ == "__main__":
    main()
