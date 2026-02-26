#!/usr/bin/env python3
"""
audit_all_contacts.py — Post-backfill completeness audit for "All contacts" field

After backfill_all_contacts.py ran (188/188 matched updated, 0 errors),
5 Notion rows were NOT updated:
  • 4 rows with a Stripe ID but no ClickHouse match (Group B)
  • 1 row with no Stripe ID at all (Group C)

This script identifies exactly which rows they are, diagnoses why each
Group B row had no ClickHouse data, and optionally fixes any that turn out
to have eligible emails (edge-case safety net).

Usage:
  python3 audit_all_contacts.py          # audit only
  python3 audit_all_contacts.py --fix    # audit + patch any fixable rows
"""

import sys
import time
import json
import requests
from requests.auth import HTTPBasicAuth

# ── Credentials ───────────────────────────────────────────────────────────────
CLICKHOUSE_URL        = "https://ua2wi80os4.eu-central-1.aws.clickhouse.cloud:8443/"
CLICKHOUSE_KEY_ID     = "default"
CLICKHOUSE_KEY_SECRET = "***REMOVED***"

NOTION_TOKEN = "***REMOVED***"
MCT_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"

# ── HTTP headers ──────────────────────────────────────────────────────────────
notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}

clickhouse_auth = HTTPBasicAuth(CLICKHOUSE_KEY_ID, CLICKHOUSE_KEY_SECRET)


# ── ClickHouse helpers ─────────────────────────────────────────────────────────

def run_clickhouse(sql):
    """Run a SQL query against ClickHouse and return the parsed JSON response."""
    r = requests.post(
        CLICKHOUSE_URL,
        auth=clickhouse_auth,
        headers={"Content-Type": "text/plain"},
        data=sql.encode("utf-8"),
        timeout=60,
    )
    if r.status_code != 200:
        raise RuntimeError(f"ClickHouse {r.status_code}: {r.text[:500]}")
    return r.json()


def diagnose_stripe_id(stripe_id):
    """
    For a given stripe_id, check how many agents exist, how many are active,
    and how many have eligible emails.  Returns a dict with the counts and a
    human-readable verdict.
    """
    sql = f"""
SELECT
    count()                                                          AS total_agents,
    countIf(status = 'active')                                       AS active_agents,
    countIf(status = 'active'
            AND email != ''
            AND email NOT LIKE 'deleted%')                           AS eligible_agents,
    groupArray(DISTINCT status)                                      AS statuses
FROM operator.agent a
INNER JOIN (
    SELECT DISTINCT org_id
    FROM operator.public_workspace_report_snapshot
    WHERE stripe_customer_id = '{stripe_id}'
      AND org_id != ''
) w ON a.organization_id = w.org_id
FORMAT JSON
""".strip()

    resp = run_clickhouse(sql)
    rows = resp.get("data", [])

    if not rows:
        # INNER JOIN returned nothing → no snapshot rows for this stripe_id
        return {
            "total_agents": 0,
            "active_agents": 0,
            "eligible_agents": 0,
            "statuses": [],
            "verdict": "No org linked in ClickHouse (no snapshot rows for this stripe_id)",
        }

    row = rows[0]
    total    = int(row.get("total_agents", 0))
    active   = int(row.get("active_agents", 0))
    eligible = int(row.get("eligible_agents", 0))
    statuses = row.get("statuses", [])

    if total == 0:
        verdict = "No org linked in ClickHouse (no snapshot rows for this stripe_id)"
    elif active == 0:
        verdict = "All agents inactive/deleted — no eligible emails (expected)"
    elif eligible == 0:
        verdict = "Active agents but all have empty/deleted emails (expected)"
    else:
        verdict = "BUG: eligible emails exist — should have been caught by backfill!"

    return {
        "total_agents": total,
        "active_agents": active,
        "eligible_agents": eligible,
        "statuses": statuses,
        "verdict": verdict,
    }


def fetch_emails_for_stripe_id(stripe_id):
    """
    Fetch the comma-separated email list for a single stripe_id
    (same logic as the original backfill SQL, scoped to one customer).
    """
    sql = f"""
SELECT
    groupArray(DISTINCT a.email) AS emails
FROM operator.agent a
INNER JOIN (
    SELECT DISTINCT org_id
    FROM operator.public_workspace_report_snapshot
    WHERE stripe_customer_id = '{stripe_id}'
      AND org_id != ''
) w ON a.organization_id = w.org_id
WHERE a.status = 'active'
  AND a.email != ''
  AND a.email NOT LIKE 'deleted%'
FORMAT JSON
""".strip()

    resp = run_clickhouse(sql)
    rows = resp.get("data", [])
    if not rows:
        return []
    emails = rows[0].get("emails", [])
    return sorted(emails)


# ── Notion helper ─────────────────────────────────────────────────────────────

def patch_all_contacts(page_id, emails_str):
    """
    Write a comma-separated email list into the "All contacts" rich_text
    property on a Notion MCT page.  (Verbatim from backfill_all_contacts.py)
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


# ── Step 1: Fetch Notion MCT rows ─────────────────────────────────────────────

def fetch_notion_mct_full():
    """
    Fetch all Notion MCT rows via the data_sources API (paginated).

    Returns a list of dicts, one per row:
      {
        "page_id":       str,
        "stripe_id":     str,   # "" if missing
        "all_contacts":  str,   # "" if not yet populated; joins all rich_text blocks
        "customer_name": str,   # title property value
      }
    """
    print("Fetching all rows from Notion Master Customer Table...")
    rows = []
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
            props   = page.get("properties", {})

            # Stripe Customer ID
            stripe_prop = props.get("🔗 Stripe Customer ID", {})
            rt_stripe   = stripe_prop.get("rich_text", [])
            stripe_id   = rt_stripe[0]["plain_text"].strip() if rt_stripe else ""

            # All contacts — join ALL rich_text blocks (Notion may split long text)
            contacts_prop = props.get("All contacts", {})
            rt_contacts   = contacts_prop.get("rich_text", [])
            all_contacts  = "".join(b["plain_text"] for b in rt_contacts).strip()

            # Customer name — find the title property
            customer_name = ""
            for prop_val in props.values():
                if prop_val.get("type") == "title":
                    title_arr = prop_val.get("title", [])
                    if title_arr:
                        customer_name = title_arr[0].get("plain_text", "").strip()
                    break

            rows.append({
                "page_id":       page_id,
                "stripe_id":     stripe_id,
                "all_contacts":  all_contacts,
                "customer_name": customer_name,
            })

        print(f"  Page {page_num}: {len(results)} rows (running total: {len(rows)})")

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
        time.sleep(0.3)

    print(f"  Total Notion rows fetched: {len(rows)}")
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    fix_mode = "--fix" in sys.argv

    print("=" * 68)
    print("  audit_all_contacts.py — Post-backfill audit")
    if fix_mode:
        print("  MODE: --fix enabled (will patch any fixable rows)")
    print("=" * 68)
    print()

    # ── Step 1: Fetch Notion rows ──────────────────────────────────────────────
    all_rows = fetch_notion_mct_full()
    print()

    # ── Step 2: Categorise ────────────────────────────────────────────────────
    group_a = []  # stripe_id set AND all_contacts non-empty → already updated
    group_b = []  # stripe_id set AND all_contacts empty    → the unknown 4
    group_c = []  # no stripe_id                            → the unknown 1

    for row in all_rows:
        if not row["stripe_id"]:
            group_c.append(row)
        elif row["all_contacts"]:
            group_a.append(row)
        else:
            group_b.append(row)

    # ── Step 3: Diagnose Group B via ClickHouse ────────────────────────────────
    print("Diagnosing Group B rows via ClickHouse...")
    diagnoses = []
    for row in group_b:
        sid = row["stripe_id"]
        print(f"  Querying ClickHouse for {sid} ...")
        diag = diagnose_stripe_id(sid)
        diagnoses.append((row, diag))
        time.sleep(0.2)
    print()

    # ── Step 4: Print structured report ───────────────────────────────────────
    print("=" * 68)
    print()

    # Group A
    print(f"=== Group A: All contacts populated ({len(group_a)} rows) ===")
    print(f"  {len(group_a)} rows updated correctly. Nothing to do.")
    print()

    # Group B
    print(f"=== Group B: Has Stripe ID but All contacts EMPTY ({len(group_b)} rows) ===")
    if not group_b:
        print("  (none)")
    else:
        for i, (row, diag) in enumerate(diagnoses, 1):
            name = row["customer_name"] or "(no name)"
            print(f"  [{i}] {name}")
            print(f"      Stripe ID  : {row['stripe_id']}")
            print(f"      Notion page: {row['page_id']}")
            print(f"      Diagnosis:")
            print(f"        Total agents    : {diag['total_agents']}")
            print(f"        Active agents   : {diag['active_agents']}")
            print(f"        Eligible agents : {diag['eligible_agents']}")
            print(f"        Statuses seen   : {diag['statuses']}")
            print(f"      Verdict: {diag['verdict']}")
            print()

    # Group C
    print(f"=== Group C: No Stripe ID ({len(group_c)} rows) ===")
    if not group_c:
        print("  (none)")
    else:
        for i, row in enumerate(group_c, 1):
            name = row["customer_name"] or "(no name)"
            print(f"  [{i}] {name}")
            print(f"      Notion page: {row['page_id']}")
            print(f"      (No Stripe ID — manually check if this is a real customer or test row)")
            print()

    # ── Step 5: Conclusion ────────────────────────────────────────────────────
    fixable = [(row, diag) for row, diag in diagnoses if diag["eligible_agents"] > 0]

    print("=== Conclusion ===")
    print(f"  Total Notion rows fetched          : {len(all_rows)}")
    print(f"  Group A — already populated        : {len(group_a)}")
    print(f"  Group B — has Stripe ID, no emails : {len(group_b)}")
    print(f"  Group C — no Stripe ID             : {len(group_c)}")
    print(f"  Fixable bugs (Group B eligible > 0): {len(fixable)}")

    if not fixable:
        print("  Backfill was complete for all eligible data.")
    else:
        print(f"  WARNING: {len(fixable)} row(s) have eligible emails and were NOT backfilled!")

    print()
    print("=" * 68)

    # ── Step 6: Optional --fix ────────────────────────────────────────────────
    if fix_mode and fixable:
        print()
        print(f"--fix: patching {len(fixable)} fixable row(s)...")
        for row, diag in fixable:
            sid      = row["stripe_id"]
            page_id  = row["page_id"]
            name     = row["customer_name"] or "(no name)"
            print(f"  Fetching emails for {sid} ({name})...")
            emails = fetch_emails_for_stripe_id(sid)
            if not emails:
                print(f"    WARNING: no emails returned by targeted query — skipping")
                continue
            emails_str = ", ".join(emails)
            print(f"    Patching {len(emails)} email(s): {emails_str[:80]}{'...' if len(emails_str) > 80 else ''}")
            ok = patch_all_contacts(page_id, emails_str)
            if ok:
                print(f"    ✓ Updated")
            else:
                print(f"    ✗ FAILED")
            time.sleep(0.2)

    elif fix_mode and not fixable:
        print()
        print("--fix: nothing to fix — all Group B rows have no eligible emails.")


if __name__ == "__main__":
    main()
