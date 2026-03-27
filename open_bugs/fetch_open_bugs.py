#!/usr/bin/env python3
"""
fetch_open_bugs.py — Fetch all open/in-progress bugs from the Notion Issues Table.

Writes open_bugs_data.json with bug details grouped by category.
"""

import json
import os
import sys
from datetime import datetime, timezone

# Support both local (creds.py) and CI (env vars)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, ROOT_DIR)

try:
    import creds
    NOTION_TOKEN = creds.get("NOTION_TOKEN")
except Exception:
    NOTION_TOKEN = os.environ["NOTION_TOKEN"]

import requests

ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}


def query_all(body):
    """Paginated Notion query."""
    pages = []
    body = {**body, "page_size": 100}
    while True:
        r = requests.post(
            f"https://api.notion.com/v1/databases/{ISSUES_DB}/query",
            headers=HEADERS,
            json=body,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        body["start_cursor"] = data["next_cursor"]
    return pages


def extract_bug(page):
    """Extract relevant fields from a Notion issue page."""
    props = page.get("properties", {})

    def get_title(p):
        parts = p.get("title", [])
        return parts[0]["plain_text"] if parts else ""

    def get_select(p):
        s = p.get("select")
        return s["name"] if s else ""

    def get_text(p):
        parts = p.get("rich_text", [])
        return parts[0]["plain_text"] if parts else ""

    def get_date(p):
        d = p.get("date")
        return d["start"] if d else ""

    def get_url(p):
        return p.get("url") or ""

    def get_rollup_text(p):
        r = p.get("rollup", {})
        arr = r.get("array", [])
        if arr and arr[0].get("type") == "select" and arr[0].get("select"):
            return arr[0]["select"]["name"]
        return ""

    def get_relation_names(p):
        """Get related page titles (for Customer relation)."""
        return [r.get("id", "") for r in p.get("relation", [])]

    title = get_title(props.get("Issue Title", {}))
    category = get_select(props.get("Category", {}))
    severity = get_select(props.get("Severity", {}))
    status = get_select(props.get("Status", {}))
    created_at = get_date(props.get("Created At", {}))
    source_url = get_url(props.get("Source URL", {}))
    linear_url = get_url(props.get("Linear Ticket URL", {}))
    summary = get_text(props.get("Summary", {}))
    assigned_to = get_rollup_text(props.get("Assigned To", {}))
    customer_ids = get_relation_names(props.get("Customer", {}))
    page_id = page["id"]
    notion_url = f"https://notion.so/{page_id.replace('-', '')}"

    return {
        "id": page_id,
        "title": title,
        "category": category,
        "severity": severity,
        "status": status,
        "created_at": created_at,
        "source_url": source_url,
        "linear_url": linear_url,
        "summary": summary,
        "assigned_to": assigned_to,
        "customer_ids": customer_ids,
        "notion_url": notion_url,
    }


def fetch_customer_names(customer_ids):
    """Batch-fetch customer names from MCT."""
    names = {}
    for cid in customer_ids:
        if cid in names:
            continue
        try:
            r = requests.get(
                f"https://api.notion.com/v1/pages/{cid}",
                headers=HEADERS,
                timeout=15,
            )
            if r.status_code == 200:
                props = r.json().get("properties", {})
                title_prop = props.get("\U0001f3e2 Company Name", {})
                parts = title_prop.get("title", [])
                names[cid] = parts[0]["plain_text"] if parts else "Unknown"
            else:
                names[cid] = "Unknown"
        except Exception:
            names[cid] = "Unknown"
    return names


def main():
    print("Fetching open/in-progress bugs from Notion...")

    pages = query_all({
        "filter": {
            "and": [
                {"property": "Issue Type", "select": {"equals": "Bug"}},
                {
                    "or": [
                        {"property": "Status", "select": {"equals": "Open"}},
                        {"property": "Status", "select": {"equals": "In Progress"}},
                    ]
                },
            ]
        },
        "sorts": [{"property": "Created At", "direction": "descending"}],
    })

    print(f"  Found {len(pages)} open bugs")

    bugs = [extract_bug(p) for p in pages]

    # Collect all customer IDs and batch-fetch names
    all_customer_ids = set()
    for b in bugs:
        for cid in b["customer_ids"]:
            all_customer_ids.add(cid)

    print(f"  Fetching {len(all_customer_ids)} customer names...")
    customer_names = fetch_customer_names(all_customer_ids)

    # Replace customer IDs with names
    for b in bugs:
        names = [customer_names.get(cid, "Unknown") for cid in b["customer_ids"]]
        b["customer"] = names[0] if names else "Unknown"
        del b["customer_ids"]

    # Summary
    cats = {}
    for b in bugs:
        c = b["category"] or "Uncategorized"
        cats[c] = cats.get(c, 0) + 1
    for c, n in sorted(cats.items()):
        print(f"    {c}: {n}")

    output = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total": len(bugs),
        "bugs": bugs,
    }

    out_path = os.path.join(SCRIPT_DIR, "open_bugs_data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"  Wrote {out_path}")


if __name__ == "__main__":
    main()
