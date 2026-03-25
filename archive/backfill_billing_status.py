#!/usr/bin/env python3
"""
backfill_billing_status.py — One-time backfill for Billing Status = "Canceled"

For every Stripe subscription that is currently "canceled", this script finds
the matching Notion Master Customer Table row and sets:
  💰 Billing Status → "Canceled"

It does NOT touch 😢 Churn Date — that is handled by a Notion automation that
fires when Billing Status changes to "Canceled".

Usage:
  python3 backfill_billing_status.py            # live run
  python3 backfill_billing_status.py --dry-run  # preview only, no writes
"""

import sys
import time
import requests
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
STRIPE_KEY = creds.get("STRIPE_KEY")
NOTION_TOKEN = creds.get("NOTION_TOKEN")
NOTION_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

# ── HTTP headers ──────────────────────────────────────────────────────────────
stripe_headers = {
    "Authorization": f"Bearer {STRIPE_KEY}",
}
notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}


# ── Stripe: fetch all canceled subscriptions ─────────────────────────────────

def fetch_canceled_subscriptions():
    """
    Paginate through Stripe and collect all unique customer IDs
    that have at least one canceled subscription.
    """
    print("Fetching canceled subscriptions from Stripe...")
    customer_ids = []
    sub_count = 0
    params = {"status": "canceled", "limit": 100}

    while True:
        r = requests.get(
            "https://api.stripe.com/v1/subscriptions",
            headers=stripe_headers,
            params=params,
        )
        r.raise_for_status()
        data = r.json()
        subs = data.get("data", [])

        for sub in subs:
            sub_count += 1
            cid = sub.get("customer", "")
            if cid and cid not in customer_ids:
                customer_ids.append(cid)

        if not data.get("has_more"):
            break

        # Paginate: next page starts after the last object ID
        params["starting_after"] = subs[-1]["id"]
        time.sleep(0.2)  # gentle rate limiting

    print(f"  Found {sub_count} canceled subscriptions → {len(customer_ids)} unique customers")
    return customer_ids


# ── Notion: look up MCT row by Stripe Customer ID ────────────────────────────

def find_notion_row(stripe_customer_id):
    """
    Query MCT data source for a row whose '🔗 Stripe Customer ID' contains
    the given Stripe customer ID.

    Returns (page_id, current_billing_status) or (None, None) if not found.
    """
    r = requests.post(
        f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
        headers=notion_headers,
        json={
            "filter": {
                "property": "🔗 Stripe Customer ID",
                "rich_text": {"contains": stripe_customer_id},
            }
        },
    )
    if r.status_code != 200:
        print(f"  WARN: Notion query failed for {stripe_customer_id}: {r.status_code} — {r.text[:200]}")
        return None, None

    results = r.json().get("results", [])
    if not results:
        return None, None

    page = results[0]
    page_id = page["id"]
    props = page.get("properties", {})

    # Read current Billing Status value
    billing_prop = props.get("💰 Billing Status", {})
    current_status = ""
    if billing_prop.get("type") == "select" and billing_prop.get("select"):
        current_status = billing_prop["select"].get("name", "")
    elif billing_prop.get("select"):
        current_status = billing_prop["select"].get("name", "")

    return page_id, current_status


# ── Notion: PATCH Billing Status to "Canceled" ───────────────────────────────

def patch_billing_status(page_id):
    """
    Set 💰 Billing Status = "Canceled" on a Notion MCT page.
    """
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers,
        json={
            "properties": {
                "💰 Billing Status": {
                    "select": {"name": "Canceled"}
                }
            }
        },
    )
    if r.status_code not in (200, 201):
        print(f"  ERROR patching page {page_id}: {r.status_code} — {r.text[:300]}")
        return False
    return True


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv

    print("=" * 60)
    print("backfill_billing_status.py")
    if dry_run:
        print("MODE: DRY RUN — no Notion writes will be made")
    else:
        print("MODE: LIVE — will update Notion rows")
    print("=" * 60)

    # Step 1: all canceled Stripe customers
    customer_ids = fetch_canceled_subscriptions()

    # Counters
    not_in_notion = []
    already_canceled = []
    updated = []
    errors = []

    print(f"\nChecking {len(customer_ids)} Stripe customer IDs against Notion MCT...")
    print("─" * 60)

    for i, cid in enumerate(customer_ids, 1):
        print(f"[{i}/{len(customer_ids)}] {cid}", end="  ")

        # Step 2: find Notion row
        page_id, current_status = find_notion_row(cid)

        if page_id is None:
            print("→ NOT IN NOTION (no row)")
            not_in_notion.append(cid)
            time.sleep(0.3)
            continue

        if current_status == "Canceled":
            print(f"→ already Canceled (page {page_id[:8]}...) — skip")
            already_canceled.append(cid)
            time.sleep(0.3)
            continue

        # Step 3: update
        print(f"→ current='{current_status}' → will set Canceled (page {page_id[:8]}...)")

        if dry_run:
            updated.append({"cid": cid, "page_id": page_id, "was": current_status})
        else:
            ok = patch_billing_status(page_id)
            if ok:
                updated.append({"cid": cid, "page_id": page_id, "was": current_status})
                print(f"  ✓ Updated")
            else:
                errors.append(cid)

        time.sleep(0.5)  # Notion rate limit protection

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print("=" * 60)
    print(f"  Total Stripe canceled customers : {len(customer_ids)}")
    print(f"  Not found in Notion             : {len(not_in_notion)}")
    print(f"  Already Canceled (skipped)      : {len(already_canceled)}")
    print(f"  {'Would update' if dry_run else 'Updated'}                    : {len(updated)}")
    print(f"  Errors                          : {len(errors)}")

    if updated:
        print(f"\n{'Would update' if dry_run else 'Updated'} rows:")
        for u in updated:
            print(f"  • {u['cid']}  (was '{u['was']}')  page={u['page_id'][:8]}...")

    if not_in_notion:
        print(f"\nStripe customers with no Notion row:")
        for cid in not_in_notion:
            print(f"  • {cid}")

    if errors:
        print(f"\nFailed updates:")
        for cid in errors:
            print(f"  ✗ {cid}")
        sys.exit(1)

    if dry_run:
        print("\nDry run complete — re-run without --dry-run to apply changes.")


if __name__ == "__main__":
    main()
