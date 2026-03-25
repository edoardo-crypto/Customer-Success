#!/usr/bin/env python3
"""
setup_cs_linked_dbs.py — Create 3 linked Notion databases for CS operations

Creates the following databases as children of the CS Operations Hub page
(same parent as the MCT), each with a relation property pointing back at
the Master Customer Table:

  1. CS Blockers & Next Actions  — Blocker, Next Action, Status, Customer
  2. CS Success Criteria          — Criterion, Confirmed at graduation, Customer
  3. CS To Dos                    — Task, Done, Notes, Customer

When the relation is created, Notion automatically adds a back-relation column
on every MCT row — no MCT writes needed.

Usage:
  python3 setup_cs_linked_dbs.py

After running:
  - Archive this script to archive/
  - Record the 3 new DB IDs in memory/MEMORY.md
"""

import requests
import creds

# ── Constants ─────────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
MCT_DB_ID    = "84feda19cfaf4c6e9500bf21d2aaafef"

HEADERS_V22 = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

HEADERS_V25 = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}


# ── Step 1: Find the MCT's parent page ───────────────────────────────────────

def get_mct_parent_page_id() -> str:
    """
    Fetch the MCT database metadata to discover which page it lives inside
    (the CS Operations Hub). We try 2022-06-28 first; fall back to 2025-09-03.
    """
    print("Step 1 — Looking up MCT parent page...")

    for headers, label in [(HEADERS_V22, "2022-06-28"), (HEADERS_V25, "2025-09-03")]:
        r = requests.get(
            f"https://api.notion.com/v1/databases/{MCT_DB_ID}",
            headers=headers,
        )
        if r.status_code == 200:
            parent = r.json().get("parent", {})
            page_id = parent.get("page_id") or parent.get("database_id")
            if page_id:
                print(f"  ✓ Found parent page ID: {page_id}  (using Notion-Version {label})")
                return page_id
            print(f"  WARN: 200 but no page_id in parent ({label}). Response: {r.json()}")
        else:
            print(f"  WARN: {r.status_code} with version {label} — {r.text[:200]}")

    raise RuntimeError("Could not determine MCT parent page ID from either API version.")


# ── Step 2: Create a database ─────────────────────────────────────────────────

def create_database(parent_page_id: str, title: str, properties: dict) -> dict:
    """
    Create a Notion database as a child of parent_page_id.
    Returns the full API response dict (includes id and url).
    """
    body = {
        "parent": {"type": "page_id", "page_id": parent_page_id},
        "title": [{"type": "text", "text": {"content": title}}],
        "properties": properties,
    }
    r = requests.post(
        "https://api.notion.com/v1/databases",
        headers=HEADERS_V25,
        json=body,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(
            f"Failed to create '{title}': {r.status_code} — {r.text[:400]}"
        )
    return r.json()


# ── Database property schemas ─────────────────────────────────────────────────

def blockers_properties() -> dict:
    return {
        "Blocker": {"title": {}},
        "Next Action": {"rich_text": {}},
        "Status": {
            "select": {
                "options": [
                    {"name": "Blocked",     "color": "red"},
                    {"name": "In Progress", "color": "yellow"},
                    {"name": "Resolved",    "color": "green"},
                ]
            }
        },
        "Customer": {
            "relation": {
                "database_id": MCT_DB_ID,
                "single_property": {},
            }
        },
    }


def success_criteria_properties() -> dict:
    return {
        "Criterion": {"title": {}},
        "Confirmed at graduation": {"checkbox": {}},
        "Customer": {
            "relation": {
                "database_id": MCT_DB_ID,
                "single_property": {},
            }
        },
    }


def todos_properties() -> dict:
    return {
        "Task": {"title": {}},
        "Done": {"checkbox": {}},
        "Notes": {"rich_text": {}},
        "Customer": {
            "relation": {
                "database_id": MCT_DB_ID,
                "single_property": {},
            }
        },
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("setup_cs_linked_dbs.py")
    print("Creating 3 linked CS databases in Notion")
    print("=" * 60)

    # Step 1: discover parent page
    parent_page_id = get_mct_parent_page_id()

    # Step 2: create the 3 databases
    databases = [
        ("CS Blockers & Next Actions", blockers_properties()),
        ("CS Success Criteria",        success_criteria_properties()),
        ("CS To Dos",                  todos_properties()),
    ]

    results = []
    for title, props in databases:
        print(f"\nStep 2 — Creating '{title}'...")
        db = create_database(parent_page_id, title, props)
        db_id  = db["id"]
        db_url = db.get("url", f"https://www.notion.so/{db_id.replace('-', '')}")
        print(f"  ✓ Created")
        print(f"    ID  : {db_id}")
        print(f"    URL : {db_url}")
        results.append({"title": title, "id": db_id, "url": db_url})

    # ── Final summary ─────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("DONE — 3 databases created successfully")
    print("=" * 60)
    for r in results:
        print(f"\n  {r['title']}")
        print(f"    ID  : {r['id']}")
        print(f"    URL : {r['url']}")

    print("""
Next steps:
  1. Open the MCT in Notion — 3 new relation columns should be visible.
  2. Optionally rename / pin them in the default MCT view.
  3. Archive this script: mv setup_cs_linked_dbs.py archive/
  4. Record the 3 DB IDs in memory/MEMORY.md (section: CS Linked Databases).
""")

    return results


if __name__ == "__main__":
    main()
