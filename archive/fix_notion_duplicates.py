#!/usr/bin/env python3
"""
fix_notion_duplicates.py

Archives:
  - Row 28: empty placeholder (no Source ID)
  - Row 46: manual backfill for conv 215473164094829 (created ~14:08), duplicate of Row 36
"""

import requests
import json

NOTION_TOKEN = "***REMOVED***"
ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"
TARGET_CONV_ID = "215473164094829"

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}


def query_issues_table():
    """Fetch all rows from the Issues Table, paginated."""
    url = f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query"
    rows = []
    cursor = None

    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        resp = requests.post(url, headers=NOTION_HEADERS, json=body)
        resp.raise_for_status()
        data = resp.json()

        rows.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return rows


def get_prop_text(page, prop_name):
    """Extract plain text from a Notion property."""
    props = page.get("properties", {})
    prop = props.get(prop_name, {})
    ptype = prop.get("type")

    if ptype == "title":
        items = prop.get("title", [])
        return "".join(t.get("plain_text", "") for t in items)
    elif ptype == "rich_text":
        items = prop.get("rich_text", [])
        return "".join(t.get("plain_text", "") for t in items)
    elif ptype == "number":
        return prop.get("number")
    elif ptype == "select":
        s = prop.get("select")
        return s.get("name") if s else None
    elif ptype == "date":
        d = prop.get("date")
        return d.get("start") if d else None
    elif ptype == "url":
        return prop.get("url")
    return None


def archive_page(page_id, reason):
    """Archive a Notion page by setting archived=true."""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    resp = requests.patch(url, headers=NOTION_HEADERS, json={"archived": True})
    if resp.status_code == 200:
        print(f"  ✓ Archived page {page_id} ({reason})")
    else:
        print(f"  ✗ Failed to archive {page_id}: {resp.status_code} {resp.text}")
    return resp.status_code == 200


def main():
    print("=== Step 1: Querying Notion Issues Table ===")
    rows = query_issues_table()
    print(f"  Total rows fetched: {len(rows)}")

    # Build a summary for inspection
    print("\n=== Row Summary ===")
    rows_with_conv = []
    empty_rows = []

    for page in rows:
        page_id = page["id"]
        created_time = page.get("created_time", "")
        source_id = get_prop_text(page, "Source ID")
        conv_id = get_prop_text(page, "Conversation ID")
        issue_type = get_prop_text(page, "Issue Type")

        is_empty = not source_id and not conv_id and not issue_type

        if is_empty:
            empty_rows.append(page)
            print(f"  [EMPTY] page_id={page_id} created={created_time}")
        elif conv_id == TARGET_CONV_ID:
            rows_with_conv.append(page)
            print(f"  [CONV {TARGET_CONV_ID}] page_id={page_id} created={created_time} source_id={source_id}")
        else:
            pass  # normal row, skip printing

    print(f"\n  Empty rows found: {len(empty_rows)}")
    print(f"  Rows for conv {TARGET_CONV_ID}: {len(rows_with_conv)}")

    # --- Archive empty rows (Row 28) ---
    print("\n=== Step 2: Archive Empty Placeholder Row(s) ===")
    if not empty_rows:
        print("  No empty rows found — nothing to archive.")
    else:
        for page in empty_rows:
            archive_page(page["id"], "empty placeholder (Row 28)")

    # --- Archive duplicate backfill (Row 46) ---
    print(f"\n=== Step 3: Archive Duplicate Backfill for Conv {TARGET_CONV_ID} ===")
    if len(rows_with_conv) <= 1:
        print(f"  Only {len(rows_with_conv)} row(s) for conv {TARGET_CONV_ID} — no duplicate to remove.")
    else:
        # Sort by created_time ascending; the first one is the earlier (backfill at 14:08)
        rows_sorted = sorted(rows_with_conv, key=lambda p: p.get("created_time", ""))
        print(f"  Found {len(rows_sorted)} rows. Archiving the earliest (backfill):")
        for page in rows_sorted:
            print(f"    page_id={page['id']} created={page.get('created_time')}")

        to_archive = rows_sorted[:-1]  # all but the last (latest = Row 36, the good one)
        to_keep = rows_sorted[-1]
        print(f"  Keeping (Row 36, workflow-created): {to_keep['id']} at {to_keep.get('created_time')}")

        for page in to_archive:
            archive_page(
                page["id"],
                f"duplicate backfill for conv {TARGET_CONV_ID} (Row 46)"
            )

    # --- Final verification ---
    print("\n=== Step 4: Verification Query ===")
    rows2 = query_issues_table()
    conv_rows = [
        p for p in rows2
        if get_prop_text(p, "Conversation ID") == TARGET_CONV_ID
    ]
    empty_rows2 = [
        p for p in rows2
        if not get_prop_text(p, "Source ID")
        and not get_prop_text(p, "Conversation ID")
        and not get_prop_text(p, "Issue Type")
    ]

    print(f"  Remaining rows for conv {TARGET_CONV_ID}: {len(conv_rows)}")
    for p in conv_rows:
        src = get_prop_text(p, "Source ID")
        print(f"    page_id={p['id']} source_id={src} created={p.get('created_time')}")

    print(f"  Remaining empty rows: {len(empty_rows2)}")

    if len(conv_rows) == 1 and len(empty_rows2) == 0:
        print("\n  ✓ Cleanup complete — 1 row for target conv, 0 empty rows.")
    else:
        print("\n  ⚠ Check results above — manual review may be needed.")


if __name__ == "__main__":
    main()
