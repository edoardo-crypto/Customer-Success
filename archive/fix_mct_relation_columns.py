#!/usr/bin/env python3
"""
fix_mct_relation_columns.py — Add 3 relation properties directly to the MCT

Since the MCT is a multi-source database, Notion did not auto-create the
back-relation columns when we created the 3 CS databases. This script adds
them manually via PATCH /data_sources/{ds_id}.

Properties added:
  - 🚧 CS Blockers     → relation to CS Blockers & Next Actions
  - ✅ CS Criteria     → relation to CS Success Criteria
  - ✓ CS To Dos       → relation to CS To Dos

Usage: python3 fix_mct_relation_columns.py
"""

import requests
import creds

NOTION_TOKEN = creds.get("NOTION_TOKEN")
MCT_DB_ID    = "84feda19cfaf4c6e9500bf21d2aaafef"
MCT_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"

# The 3 databases we created
BLOCKERS_DB_ID  = "06fe1bb4-cfc2-4bdf-967c-cd633c8aa8c4"
CRITERIA_DB_ID  = "b938e808-b10b-4bbc-9dde-ec7a1c74b1ff"
TODOS_DB_ID     = "3092908d-088d-4726-8fe0-de3d72cf0ba3"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}

# Relations to add to the MCT — name → target database id
RELATIONS = [
    ("🚧 CS Blockers",  BLOCKERS_DB_ID),
    ("✅ CS Criteria",  CRITERIA_DB_ID),
    ("✓ CS To Dos",     TODOS_DB_ID),
]


def get_mct_properties():
    """
    Fetch current MCT schema and return property names.
    2025-09-03 omits 'properties' from GET /databases — use 2022-06-28 for
    verification won't work either (multi-source). So instead query one MCT
    page via data_sources and read its property keys.
    """
    r = requests.post(
        f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query",
        headers=HEADERS,
        json={"page_size": 1},
    )
    r.raise_for_status()
    results = r.json().get("results", [])
    if not results:
        return []
    return list(results[0].get("properties", {}).keys())


def add_relation_via_databases(prop_name: str, target_db_id: str) -> bool:
    """Try adding via PATCH /databases/{id} first (cleaner, no DANGER risk)."""
    r = requests.patch(
        f"https://api.notion.com/v1/databases/{MCT_DB_ID}",
        headers=HEADERS,
        json={
            "properties": {
                prop_name: {
                    "relation": {
                        "database_id": target_db_id,
                        "single_property": {},
                    }
                }
            }
        },
    )
    return r.status_code in (200, 201), r


def add_relation_via_data_sources(prop_name: str, target_db_id: str) -> bool:
    """
    Fallback: PATCH /data_sources/{ds_id}.
    Multi-source relations require 'data_source_id', not 'database_id'.
    Never include null values (risk of silent property corruption).
    """
    r = requests.patch(
        f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}",
        headers=HEADERS,
        json={
            "properties": {
                prop_name: {
                    "relation": {
                        "data_source_id": target_db_id,
                        "single_property": {},
                    }
                }
            }
        },
    )
    return r.status_code in (200, 201), r


def main():
    print("=" * 60)
    print("fix_mct_relation_columns.py")
    print("=" * 60)

    # Snapshot current properties to verify additions later
    print("\nStep 1 — Current MCT properties (checking for conflicts)...")
    existing = get_mct_properties()
    print(f"  {len(existing)} properties found")
    for rel_name, _ in RELATIONS:
        if rel_name in existing:
            print(f"  ⚠ '{rel_name}' already exists — will skip")

    print("\nStep 2 — Adding relation properties to MCT...")

    for prop_name, target_db_id in RELATIONS:
        if prop_name in existing:
            print(f"\n  [{prop_name}] already present — skip")
            continue

        print(f"\n  [{prop_name}]")
        print(f"    → target DB: {target_db_id}")

        # Try /databases first
        ok, r = add_relation_via_databases(prop_name, target_db_id)
        if ok:
            # Check it actually appeared (2025-09-03 can silently no-op)
            new_props = get_mct_properties()
            if prop_name in new_props:
                print(f"    ✓ Added via /databases endpoint")
                existing = new_props
                continue
            else:
                print(f"    WARN: /databases returned 200 but property not found — trying /data_sources")
        else:
            print(f"    WARN: /databases failed ({r.status_code}: {r.text[:150]}) — trying /data_sources")

        # Fallback: /data_sources
        ok2, r2 = add_relation_via_data_sources(prop_name, target_db_id)
        if ok2:
            new_props = get_mct_properties()
            if prop_name in new_props:
                print(f"    ✓ Added via /data_sources endpoint")
                existing = new_props
            else:
                print(f"    ERROR: /data_sources returned {r2.status_code} but property still not found")
                print(f"           Response: {r2.text[:300]}")
        else:
            print(f"    ERROR: /data_sources failed ({r2.status_code}): {r2.text[:300]}")

    # Final verification
    print("\nStep 3 — Verification...")
    final_props = get_mct_properties()
    all_ok = True
    for prop_name, _ in RELATIONS:
        if prop_name in final_props:
            print(f"  ✓ '{prop_name}' present in MCT")
        else:
            print(f"  ✗ '{prop_name}' NOT found in MCT")
            all_ok = False

    print(f"\n{'=' * 60}")
    if all_ok:
        print("SUCCESS — All 3 relation columns added to MCT")
        print("Open Notion MCT → Properties panel → scroll to bottom")
        print("to see the 3 new columns.")
    else:
        print("PARTIAL FAILURE — check errors above")
    print("=" * 60)


if __name__ == "__main__":
    main()
