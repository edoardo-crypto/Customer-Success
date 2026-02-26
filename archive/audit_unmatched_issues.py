#!/usr/bin/env python3
"""
audit_unmatched_issues.py

Queries the Notion Issues Table for issues created on or after Feb 17, 2026
where the Customer relation is empty — i.e., issues that failed to match a
customer during the Domain blackout window (Feb 17–23, 2026).

Prints: created-at, issue title, source ID, reported-by, Notion page URL

Run this before and after triggering the Customer Issue Matcher to confirm
the unmatched count drops to zero.
"""

import time
import requests

# ── Credentials ────────────────────────────────────────────────────────────────
NOTION_TOKEN    = "***REMOVED***"
NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"

HEADERS = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type":   "application/json",
}

# Issues created on or after this date are in the potential blackout window
BLACKOUT_START = "2026-02-17"


# ── Query ──────────────────────────────────────────────────────────────────────

def query_unmatched_issues():
    """Return all Issues Table pages with no Customer relation, from Feb 17+."""
    issues      = []
    has_more    = True
    cursor      = None
    page_num    = 0

    while has_more:
        page_num += 1
        body = {
            "page_size": 100,
            "filter": {
                "and": [
                    {
                        "property": "Created At",
                        "date": {"on_or_after": BLACKOUT_START},
                    },
                    {
                        "property": "Customer",
                        "relation": {"is_empty": True},
                    },
                ]
            },
            "sorts": [{"property": "Created At", "direction": "ascending"}],
        }
        if cursor:
            body["start_cursor"] = cursor

        r = requests.post(
            f"https://api.notion.com/v1/databases/{NOTION_ISSUES_DB}/query",
            headers=HEADERS,
            json=body,
        )
        if r.status_code != 200:
            print(f"  ERROR {r.status_code}: {r.text[:400]}")
            raise RuntimeError("Notion query failed")

        data     = r.json()
        batch    = data.get("results", [])
        has_more = data.get("has_more", False)
        cursor   = data.get("next_cursor")

        issues.extend(batch)
        print(f"  Page {page_num}: {len(batch)} results  (running total: {len(issues)})")
        time.sleep(0.3)

    return issues


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_rich_text(prop):
    texts = prop.get("rich_text", [])
    return texts[0].get("plain_text", "") if texts else ""


def get_title(prop):
    texts = prop.get("title", [])
    return texts[0].get("plain_text", "") if texts else ""


def get_date(prop):
    d = prop.get("date") or {}
    return d.get("start", "")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    sep = "=" * 80

    print(f"\n{sep}")
    print(f"  Audit: Issues with no Customer match — created on or after {BLACKOUT_START}")
    print(sep)

    issues = query_unmatched_issues()
    total  = len(issues)

    print(f"\n  Total unmatched issues found: {total}")

    if total == 0:
        print()
        print("  No unmatched issues. Domain blackout had no lingering impact. ✓")
        print()
        return

    # ── Print table ────────────────────────────────────────────────────────────
    print()
    col_created   = 22
    col_title     = 48
    col_source_id = 20
    col_reported  = 35

    header = (
        f"  {'Created At':<{col_created}}"
        f"  {'Issue Title':<{col_title}}"
        f"  {'Source ID':<{col_source_id}}"
        f"  Reported By"
    )
    print(header)
    print(f"  {'-'*col_created}  {'-'*col_title}  {'-'*col_source_id}  {'-'*col_reported}")

    for page in issues:
        props = page.get("properties", {})

        created_at  = get_date(props.get("Created At", {}))
        title       = get_title(props.get("Issue Title", {}))[:col_title - 1]
        source_id   = get_rich_text(props.get("Source ID", {}))[:col_source_id - 1]
        reported_by = get_rich_text(props.get("Reported By", {}))[:col_reported - 1]

        print(
            f"  {created_at:<{col_created}}"
            f"  {title:<{col_title}}"
            f"  {source_id:<{col_source_id}}"
            f"  {reported_by}"
        )

    print()
    print(sep)
    print(f"  RESULT: {total} issue(s) have no Customer link since {BLACKOUT_START}")
    print()
    print("  Next steps:")
    print("  1. Go to n8n UI → workflow uGBGC3PzH9ajbwA6 (Customer Issue Matcher)")
    print("     Click 'Execute Workflow' to re-scan all open issues and re-link them")
    print("  2. Re-run this script — unmatched count should drop to 0")
    print("  3. Any issues that still show no Customer may have emails with generic")
    print("     domains (gmail.com etc.) or belong to customers not yet in MCT")
    print(sep)
    print()


if __name__ == "__main__":
    main()
