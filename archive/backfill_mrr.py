"""
backfill_mrr.py

One-time script: recomputes MRR for every active/churning Stripe subscription
by summing ALL line items (fixes previous single-plan-only bug in n8n sync),
then PATCHes any MCT rows where the stored value differs.

Usage:
    python3 backfill_mrr.py            # live run — updates Notion
    python3 backfill_mrr.py --dry-run  # prints diff table, no writes

Archive to archive/ after running.
"""

import sys
import json
import requests

# ── CREDENTIALS ───────────────────────────────────────────────────────────────

creds_raw = open("Credentials.md").read()

def _extract_block(header):
    """Return first ```...``` block after the given ## header."""
    try:
        start = creds_raw.index(f"## {header}")
        block_start = creds_raw.index("```", start) + 3
        # skip optional language tag on same line
        if creds_raw[block_start] not in ("\n", "\r"):
            block_start = creds_raw.index("\n", block_start) + 1
        block_end = creds_raw.index("```", block_start)
        return creds_raw[block_start:block_end].strip()
    except ValueError:
        raise RuntimeError(f"Could not find ## {header} block in Credentials.md")

STRIPE_KEY    = _extract_block("Stripe")
NOTION_TOKEN  = _extract_block("Notion")

DRY_RUN = "--dry-run" in sys.argv

MCT_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

NOTION_HDR = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type":  "application/json",
}

# ── MRR COMPUTATION ───────────────────────────────────────────────────────────

def compute_mrr(subscription):
    """Sum all line items on a Stripe subscription to get monthly MRR."""
    mrr = 0
    for item in subscription.get("items", {}).get("data", []):
        price = item.get("price") or item.get("plan") or {}
        amount = (
            price.get("unit_amount")
            or (item.get("plan") or {}).get("amount")
            or 0
        )
        recurring  = price.get("recurring") or {}
        interval       = recurring.get("interval") or (item.get("plan") or {}).get("interval") or "month"
        interval_count = recurring.get("interval_count") or (item.get("plan") or {}).get("interval_count") or 1
        qty = item.get("quantity") or 1

        if interval == "month" and interval_count == 1:
            mrr += (amount * qty) / 100
        elif interval == "year" or (interval == "month" and interval_count == 12):
            mrr += (amount * qty) / 100 / 12
        elif interval == "month":
            mrr += (amount * qty) / 100 / interval_count
        # else: skip unknown intervals

    return round(mrr, 2)


# ── STRIPE FETCH ──────────────────────────────────────────────────────────────

def fetch_stripe_subscriptions():
    """Fetch all active subscriptions (includes cancel_at_period_end=true)."""
    subs = []
    params = {
        "status":     "active",
        "expand[]":   ["data.items", "data.customer"],
        "limit":      100,
    }
    url = "https://api.stripe.com/v1/subscriptions"
    print("💳 Fetching Stripe subscriptions…")
    while True:
        r = requests.get(url, auth=(STRIPE_KEY, ""), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        subs.extend(data.get("data", []))
        if not data.get("has_more"):
            break
        params["starting_after"] = data["data"][-1]["id"]
    print(f"   → {len(subs)} active subscriptions")
    return subs


# ── NOTION FETCH ──────────────────────────────────────────────────────────────

def fetch_all_mct():
    """Fetch all MCT pages via data_sources query."""
    pages = []
    body  = {"page_size": 100}
    url   = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"
    print("🏢 Fetching MCT rows…")
    while True:
        r = requests.post(url, headers=NOTION_HDR, json=body, timeout=30)
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        body["start_cursor"] = data["next_cursor"]
    print(f"   → {len(pages)} MCT rows")
    return pages


def get_title(page, prop):
    p = page.get("properties", {}).get(prop)
    if not p:
        return ""
    return "".join(t.get("plain_text", "") for t in p.get("title", []))


def get_rich_text(page, prop):
    p = page.get("properties", {}).get(prop)
    if not p:
        return ""
    return "".join(t.get("plain_text", "") for t in p.get("rich_text", []))


def get_number(page, prop):
    p = page.get("properties", {}).get(prop)
    return p.get("number") if p else None


# ── PATCH ─────────────────────────────────────────────────────────────────────

def patch_mrr(page_id, mrr):
    """PATCH the 💰 MRR field on an MCT page."""
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HDR,
        json={"properties": {"💰 MRR": {"number": mrr}}},
        timeout=20,
    )
    r.raise_for_status()
    return r.status_code


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    subs  = fetch_stripe_subscriptions()
    pages = fetch_all_mct()

    # Build lookup: stripe_customer_id → MCT page
    mct_by_stripe = {}
    for page in pages:
        sid = get_rich_text(page, "🔗 Stripe Customer ID").strip()
        if sid:
            mct_by_stripe[sid] = page

    # Match subs → MCT and build diff table
    rows     = []
    matched  = 0
    no_match = 0

    for sub in subs:
        cust = sub.get("customer")
        if isinstance(cust, dict):
            stripe_cid = cust.get("id", "")
        else:
            stripe_cid = cust or ""

        page = mct_by_stripe.get(stripe_cid)
        if not page:
            no_match += 1
            continue

        matched += 1
        company   = get_title(page, "🏢 Company Name") or stripe_cid
        old_mrr   = get_number(page, "💰 MRR")
        new_mrr   = compute_mrr(sub)
        changed   = (old_mrr != new_mrr)

        rows.append({
            "page_id":    page["id"],
            "company":    company,
            "stripe_cid": stripe_cid,
            "old_mrr":    old_mrr,
            "new_mrr":    new_mrr,
            "changed":    changed,
        })

    # Sort: changed rows first, then by company name
    rows.sort(key=lambda r: (not r["changed"], r["company"].lower()))

    # Print table
    print()
    print(f"{'Company':<35} {'Old MRR':>10} {'New MRR':>10}  {'Status'}")
    print("-" * 72)
    for row in rows:
        old_str = f"${row['old_mrr']:,.2f}" if row["old_mrr"] is not None else "—"
        new_str = f"${row['new_mrr']:,.2f}"
        status  = "✏️  CHANGED" if row["changed"] else "  ok"
        print(f"{row['company']:<35} {old_str:>10} {new_str:>10}  {status}")

    # Summary
    changed_rows = [r for r in rows if r["changed"]]
    print()
    print(f"Matched: {matched}  |  No MCT match: {no_match}  |  Need update: {len(changed_rows)}")

    if not changed_rows:
        print("\n✅ All MRR values are already correct — nothing to update.")
        return

    if DRY_RUN:
        print("\n[DRY RUN] No changes written. Re-run without --dry-run to apply.")
        return

    # Apply patches
    print(f"\n🔧 Patching {len(changed_rows)} MCT row(s)…")
    updated = 0
    failed  = 0
    for row in changed_rows:
        try:
            patch_mrr(row["page_id"], row["new_mrr"])
            print(f"   ✅  {row['company']:35s}  ${row['old_mrr'] or 0:,.2f} → ${row['new_mrr']:,.2f}")
            updated += 1
        except Exception as e:
            print(f"   ❌  {row['company']:35s}  ERROR: {e}")
            failed += 1

    print(f"\n✅ Done — updated: {updated}, failed: {failed}")


if __name__ == "__main__":
    main()
