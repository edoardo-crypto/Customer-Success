#!/usr/bin/env python3
"""
backfill_all_contacts.py — One-time backfill for "All contacts" rich_text field

Reads all active agent emails from ClickHouse's operator.agent table,
groups them by stripe_customer_id (via workspace_report_snapshot join),
then writes a comma-separated email list into the "All contacts" property
of each matching row in the Notion Master Customer Table.

Join chain:
  agent.organization_id → public_workspace_report_snapshot.org_id
                        → stripe_customer_id
                        → Notion "🔗 Stripe Customer ID"

Usage:
  python3 backfill_all_contacts.py            # live run
  python3 backfill_all_contacts.py --dry-run  # preview only, no writes
"""

import sys
import time
import json
import requests
from requests.auth import HTTPBasicAuth
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
CLICKHOUSE_URL        = os.environ.get("CLICKHOUSE_HOST", "").rstrip("/") + "/"
CLICKHOUSE_KEY_ID     = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_KEY_SECRET = os.environ.get("CLICKHOUSE_PASSWORD", "")

NOTION_TOKEN = creds.get("NOTION_TOKEN")
MCT_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"

# ── HTTP headers ──────────────────────────────────────────────────────────────
notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}

clickhouse_auth = HTTPBasicAuth(CLICKHOUSE_KEY_ID, CLICKHOUSE_KEY_SECRET)

# ── SQL query ─────────────────────────────────────────────────────────────────
SQL = """
SELECT
    w.stripe_customer_id,
    groupArray(DISTINCT a.email) AS emails
FROM operator.agent a
INNER JOIN (
    SELECT DISTINCT org_id, stripe_customer_id
    FROM operator.public_workspace_report_snapshot
    WHERE stripe_customer_id != ''
      AND org_id != ''
) w ON a.organization_id = w.org_id
WHERE a.status = 'active'
  AND a.email != ''
  AND a.email NOT LIKE 'deleted%'
GROUP BY w.stripe_customer_id
FORMAT JSON
""".strip()


# ── Step 1: Query ClickHouse ──────────────────────────────────────────────────

def fetch_clickhouse_emails():
    """
    Run the SQL query against ClickHouse and return a dict:
      { stripe_customer_id: [email1, email2, ...] }
    """
    print("Querying ClickHouse for active agent emails...")
    r = requests.post(
        CLICKHOUSE_URL,
        auth=clickhouse_auth,
        headers={"Content-Type": "text/plain"},
        data=SQL.encode("utf-8"),
        timeout=60,
    )
    if r.status_code != 200:
        print(f"  ERROR: ClickHouse returned {r.status_code}: {r.text[:500]}")
        sys.exit(1)

    response = r.json()
    rows = response.get("data", [])
    print(f"  ClickHouse returned {len(rows)} stripe_customer_id groups")

    result = {}
    for row in rows:
        sid = row.get("stripe_customer_id", "").strip()
        emails = row.get("emails", [])
        if sid and emails:
            result[sid] = sorted(emails)  # sort for deterministic output

    return result


# ── Step 2: Query Notion MCT (paginated) ─────────────────────────────────────

def fetch_notion_mct():
    """
    Pull all rows from the Master Customer Table using the data_sources API
    (required for multi-source MCT).

    Returns a dict: { stripe_customer_id: page_id }
    """
    print("Fetching all rows from Notion Master Customer Table...")
    stripe_to_page = {}
    cursor = None
    page_num = 0

    while True:
        page_num += 1
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        r = requests.post(
            f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query",
            headers=notion_headers,
            json=body,
        )
        if r.status_code != 200:
            print(f"  ERROR: Notion query page {page_num} failed: {r.status_code} — {r.text[:300]}")
            sys.exit(1)

        data = r.json()
        results = data.get("results", [])

        for page in results:
            page_id = page["id"]
            props = page.get("properties", {})

            # Extract "🔗 Stripe Customer ID" (rich_text property)
            stripe_prop = props.get("🔗 Stripe Customer ID", {})
            rt = stripe_prop.get("rich_text", [])
            stripe_id = rt[0]["plain_text"].strip() if rt else ""

            if stripe_id:
                stripe_to_page[stripe_id] = page_id

        print(f"  Page {page_num}: {len(results)} rows fetched (running total: {len(stripe_to_page)} with stripe ID)")

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
        time.sleep(0.3)

    print(f"  Total Notion rows with a Stripe Customer ID: {len(stripe_to_page)}")
    return stripe_to_page


# ── Step 3: PATCH Notion page ─────────────────────────────────────────────────

def patch_all_contacts(page_id, emails_str):
    """
    Write a comma-separated email list into the "All contacts" rich_text
    property on a Notion MCT page.
    """
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers,
        json={
            "properties": {
                "All contacts": {
                    "rich_text": [
                        {"text": {"content": emails_str}}
                    ]
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
    print("backfill_all_contacts.py")
    if dry_run:
        print("MODE: DRY RUN — no Notion writes will be made")
    else:
        print("MODE: LIVE — will update Notion rows")
    print("=" * 60)
    print()

    # Step 1: ClickHouse → { stripe_id: [emails] }
    ch_map = fetch_clickhouse_emails()
    print()

    # Step 2: Notion MCT → { stripe_id: page_id }
    notion_map = fetch_notion_mct()
    print()

    # Step 3: Match & PATCH
    matched = []
    not_matched = []
    updated = []
    errors = []

    for sid, emails in ch_map.items():
        if sid in notion_map:
            matched.append(sid)
        else:
            not_matched.append(sid)

    print(f"Matched {len(matched)} / {len(ch_map)} ClickHouse orgs to Notion rows")
    print(f"Not matched (no Notion row): {len(not_matched)}")
    print("─" * 60)

    for i, sid in enumerate(matched, 1):
        page_id = notion_map[sid]
        emails_str = ", ".join(ch_map[sid])
        short_pid = page_id[:8]

        print(f"[{i}/{len(matched)}] {sid}  ({len(ch_map[sid])} emails)  page={short_pid}...")

        if dry_run:
            print(f"  → Would write: {emails_str[:120]}{'...' if len(emails_str) > 120 else ''}")
            updated.append(sid)
        else:
            ok = patch_all_contacts(page_id, emails_str)
            if ok:
                updated.append(sid)
                print(f"  ✓ Updated: {emails_str[:80]}{'...' if len(emails_str) > 80 else ''}")
            else:
                errors.append(sid)

        time.sleep(0.2)  # Notion rate limit

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Total ClickHouse orgs found          : {len(ch_map)}")
    print(f"  Matched to Notion row                : {len(matched)}")
    print(f"  Not matched (no Notion row)          : {len(not_matched)}")
    print(f"  {'Would update' if dry_run else 'Updated'}                           : {len(updated)}")
    print(f"  Errors                               : {len(errors)}")

    if not_matched:
        print(f"\nStripe IDs with no Notion row ({len(not_matched)}):")
        for sid in sorted(not_matched):
            print(f"  • {sid}  ({len(ch_map[sid])} emails)")

    if errors:
        print(f"\nFailed updates:")
        for sid in errors:
            print(f"  ✗ {sid}")
        sys.exit(1)

    if dry_run:
        print("\nDry run complete — re-run without --dry-run to apply changes.")
    else:
        print("\nBackfill complete.")


if __name__ == "__main__":
    main()
