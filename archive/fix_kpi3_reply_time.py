#!/usr/bin/env python3
"""
fix_kpi3_reply_time.py

1. Confirms SCORECARD_DB_ID by querying the W09 page parent.
2. Renames "Alex/Aya: Avg Reply Time" -> "Alex/Aya: Median Reply Time"
   in the Notion scorecard database schema (preserves existing data).
3. Tests that statistics.last_close_at is a valid Intercom search filter.

Run: python3 fix_kpi3_reply_time.py
"""

import requests
from datetime import date, datetime, timezone

NOTION_TOKEN   = "***REMOVED***"
INTERCOM_TOKEN = "***REMOVED***"

# W09 page — used to look up the parent DB ID
W09_PAGE_ID    = "311e418fd8c481b18552d12c067c1089"
# Known correct DB ID (confirmed Feb 25 2026 via confirm_scorecard_db())
KNOWN_DB_ID    = "311e418f-d8c4-810e-8b11-cdc50357e709"

std_headers = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}

intercom_headers = {
    "Authorization":    f"Bearer {INTERCOM_TOKEN}",
    "Intercom-Version": "2.11",
    "Accept":           "application/json",
    "Content-Type":     "application/json",
}


# ── Step 1: Confirm DB ID ──────────────────────────────────────────────────────

def confirm_scorecard_db():
    """Get the scorecard DB ID from the W09 Notion page parent."""
    url = f"https://api.notion.com/v1/pages/{W09_PAGE_ID}"
    r   = requests.get(url, headers=std_headers)
    if r.status_code != 200:
        print(f"  [warn] Could not fetch W09 page: HTTP {r.status_code} — using known constant")
        return KNOWN_DB_ID
    db_id = r.json().get("parent", {}).get("database_id", "")
    actual  = db_id.replace("-", "")
    known   = KNOWN_DB_ID.replace("-", "")
    if actual == known:
        print(f"  ✓ DB ID confirmed: {db_id}")
    else:
        print(f"  [warn] DB ID mismatch! known={KNOWN_DB_ID}, actual={db_id}")
        print(f"         Using actual value.")
    return db_id if db_id else KNOWN_DB_ID


# ── Step 2: Rename scorecard columns ──────────────────────────────────────────

def rename_columns(db_id):
    """PATCH the scorecard database to rename Avg → Median in column names."""
    url  = f"https://api.notion.com/v1/databases/{db_id}"
    body = {
        "properties": {
            "Alex: Avg Reply Time": {"name": "Alex: Median Reply Time"},
            "Aya: Avg Reply Time":  {"name": "Aya: Median Reply Time"},
        }
    }
    r = requests.patch(url, headers=std_headers, json=body)
    print(f"  PATCH /databases/{db_id}: HTTP {r.status_code}")
    if r.status_code == 200:
        print("  ✓ Columns renamed: 'Avg Reply Time' → 'Median Reply Time' (Alex & Aya)")
    else:
        print(f"  ERROR body: {r.text[:500]}")
        r.raise_for_status()


# ── Step 3: Verify Intercom statistics.last_close_at filter ───────────────────

def test_intercom_filter():
    """
    Call POST /conversations/search with statistics.last_close_at.
    Returns True if the filter is accepted (HTTP 200), False otherwise.
    """
    week_start_ts = int(datetime(2026, 2, 24, tzinfo=timezone.utc).timestamp())
    week_end_ts   = int(datetime(2026, 3,  2, tzinfo=timezone.utc).timestamp())

    url  = "https://api.intercom.io/conversations/search"
    body = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "open",                     "operator": "=",  "value": False},
                {"field": "statistics.last_close_at", "operator": ">",  "value": week_start_ts},
                {"field": "statistics.last_close_at", "operator": "<=", "value": week_end_ts},
            ],
        },
        "pagination": {"per_page": 1},
    }
    r = requests.post(url, headers=intercom_headers, json=body)
    print(f"  Intercom /conversations/search HTTP {r.status_code}")
    if r.status_code == 200:
        data        = r.json()
        total_count = data.get("total_count", 0)
        print(f"  ✓ Filter accepted — total_count={total_count}")
        return True
    else:
        print(f"  ERROR: {r.text[:400]}")
        print("  Filter may be invalid. Check Intercom API docs.")
        return False


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("fix_kpi3_reply_time.py")
    print("=" * 60)

    print("\n[1/3] Confirming scorecard DB ID from Notion …")
    db_id = confirm_scorecard_db()

    print(f"\n[2/3] Renaming reply-time columns in scorecard DB {db_id} …")
    rename_columns(db_id)

    print("\n[3/3] Testing statistics.last_close_at Intercom filter …")
    ok = test_intercom_filter()

    print("\n" + "=" * 60)
    if ok:
        print("DONE — all steps succeeded.")
    else:
        print("DONE — WARNING: Intercom filter test failed.")
        print("Consider using 'updated_at' as fallback in the scorecard builder.")
    print("=" * 60)

    print("\nNext steps:")
    print("  - build_weekly_scorecard.py  : filter already updated (statistics.last_close_at)")
    print("  - fetch_intercom_reply_time.py: filter already updated (statistics.last_close_at)")
    print("  - deploy_scorecard_builder_workflow.py : run to deploy KPIs 1-5 automation")


if __name__ == "__main__":
    main()
