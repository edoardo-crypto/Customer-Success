#!/usr/bin/env python3
"""
Clean up wrongly-logged Notion Issues rows caused by the OR-logic bug.

Targets rows that:
  - Were created today (2026-02-20)
  - Have blank Issue Type (meaning n8n couldn't match the value to a valid Notion option)
  - Source is "Intercom" (or source field is empty — most rows from this workflow)

These are conversations that Intercom closed but should have been blocked by the filter
(e.g. tagged "Not an Issue", internal @konvoai.com threads, calendar acceptances).

The script:
1. Queries the Notion Issues table for candidate rows
2. Prints each one so you can review
3. Archives (soft-deletes) them in Notion
"""

import json
import urllib.request
import urllib.error
from datetime import datetime, timezone
import creds

NOTION_TOKEN = creds.get("NOTION_TOKEN")
ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"
TODAY = "2026-02-20"


def notion_post(path, data):
    url = f"https://api.notion.com/v1{path}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def notion_patch(path, data):
    url = f"https://api.notion.com/v1{path}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        url, data=body, method="PATCH",
        headers={
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def get_prop(page, prop_name, prop_type):
    """Extract a property value from a Notion page."""
    props = page.get("properties", {})
    prop = props.get(prop_name, {})
    if prop_type == "select":
        sel = prop.get("select")
        return sel["name"] if sel else None
    elif prop_type == "title":
        rich = prop.get("title", [])
        return rich[0]["plain_text"] if rich else ""
    elif prop_type == "rich_text":
        rich = prop.get("rich_text", [])
        return rich[0]["plain_text"] if rich else ""
    elif prop_type == "date":
        d = prop.get("date")
        return d["start"] if d else None
    elif prop_type == "url":
        return prop.get("url")
    return None


def query_issues():
    """Query Notion Issues table for today's rows with blank Issue Type."""
    all_pages = []
    cursor = None

    while True:
        body = {
            "filter": {
                "timestamp": "created_time",
                "created_time": {
                    "on_or_after": f"{TODAY}T00:00:00.000Z"
                }
            },
            "page_size": 100,
        }
        if cursor:
            body["start_cursor"] = cursor

        result = notion_post(f"/databases/{ISSUES_DB_ID}/query", body)
        all_pages.extend(result.get("results", []))

        if not result.get("has_more"):
            break
        cursor = result.get("next_cursor")

    return all_pages


def main():
    print(f"Querying Notion Issues table for rows created on {TODAY}...")
    pages = query_issues()
    print(f"  Found {len(pages)} total rows created today\n")

    # Find rows with blank Issue Type
    bad_rows = []
    for page in pages:
        issue_type = get_prop(page, "Issue Type", "select")
        source = get_prop(page, "Source", "select")
        title = get_prop(page, "Conversation Title", "title") or get_prop(page, "Name", "title") or "(no title)"
        customer = get_prop(page, "Customer", "rich_text") or get_prop(page, "Company", "rich_text") or ""
        created = page.get("created_time", "")[:10]
        page_id = page["id"]

        # Target: blank Issue Type (failed to map → means original value wasn't a valid Notion option)
        if issue_type is None:
            bad_rows.append({
                "id": page_id,
                "title": title,
                "customer": customer,
                "source": source,
                "created": created,
                "issue_type": issue_type,
            })

    print(f"Found {len(bad_rows)} rows with blank Issue Type:\n")
    for i, row in enumerate(bad_rows, 1):
        print(f"  [{i}] {row['title']}")
        print(f"       Customer: {row['customer']}")
        print(f"       Source: {row['source']}  |  Issue Type: {row['issue_type']}  |  Created: {row['created']}")
        print(f"       Page ID: {row['id']}\n")

    if not bad_rows:
        print("Nothing to archive. All done.")
        return

    print(f"Archiving {len(bad_rows)} bad rows...")
    archived = 0
    errors = 0
    for row in bad_rows:
        try:
            notion_patch(f"/pages/{row['id']}", {"archived": True})
            print(f"  Archived: {row['title'][:60]}")
            archived += 1
        except urllib.error.HTTPError as e:
            print(f"  ERROR archiving {row['id']}: {e.code} {e.read().decode()}")
            errors += 1

    print(f"\nDone! Archived {archived} rows, {errors} errors.")
    print("These rows are now hidden from the Issues table (soft-deleted, recoverable).")


if __name__ == "__main__":
    main()
