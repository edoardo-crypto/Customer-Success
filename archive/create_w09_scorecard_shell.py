#!/usr/bin/env python3
"""
create_w09_scorecard_shell.py

Creates a W09 shell row in the Notion Scorecard DB so the
'📞 Weekly Customers Contacted Tracker' workflow has a row to update.

Week: "W09 2026"
Week Start: 2026-02-23 (Monday of ISO week 9)
All KPI columns: left empty (workflow/scorecard builder fills them)
"""

import requests
import json
import creds

NOTION_TOKEN = creds.get("NOTION_TOKEN")
SCORECARD_DB_ID = "311e418f-d8c4-810e-8b11-cdc50357e709"
W09_WEEK_START = "2026-02-23"
W09_WEEK_NAME = "W09 2026"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

SECTION = "=" * 65


def section(t):
    print(f"\n{SECTION}\n  {t}\n{SECTION}")


# ── Step 1: Check if row already exists ─────────────────────────────────────
section("STEP 1 — Check if W09 row already exists")
r = requests.post(
    f"https://api.notion.com/v1/databases/{SCORECARD_DB_ID}/query",
    headers=HEADERS,
    json={"filter": {"property": "Week Start", "date": {"equals": W09_WEEK_START}}},
)
r.raise_for_status()
existing = r.json().get("results", [])

if existing:
    page = existing[0]
    print(f"  Row already exists: {page['id']}")
    print(f"  archived={page.get('archived')}  in_trash={page.get('in_trash')}")
    print("  Nothing to create — exiting.")
    import sys; sys.exit(0)

print(f"  No row found for Week Start = {W09_WEEK_START} — will create one.")

# ── Step 2: Create the shell row ─────────────────────────────────────────────
section("STEP 2 — Create W09 shell row")

payload = {
    "parent": {"database_id": SCORECARD_DB_ID},
    "properties": {
        "Week": {
            "title": [{"type": "text", "text": {"content": W09_WEEK_NAME}}]
        },
        "Week Start": {
            "date": {"start": W09_WEEK_START}
        },
    },
}

r2 = requests.post(
    "https://api.notion.com/v1/pages",
    headers=HEADERS,
    json=payload,
)

if r2.status_code not in (200, 201):
    print(f"  ERROR: {r2.status_code} — {r2.text[:500]}")
    r2.raise_for_status()

page = r2.json()
print(f"  ✅ Created page ID: {page['id']}")
print(f"  URL: {page.get('url', '?')}")

# ── Step 3: Verify it's queryable ────────────────────────────────────────────
section("STEP 3 — Verify row is queryable")
r3 = requests.post(
    f"https://api.notion.com/v1/databases/{SCORECARD_DB_ID}/query",
    headers=HEADERS,
    json={"filter": {"property": "Week Start", "date": {"equals": W09_WEEK_START}}},
)
r3.raise_for_status()
results = r3.json().get("results", [])
print(f"  Query returned {len(results)} result(s)")
if results:
    p = results[0]
    props = p.get("properties", {})
    week_name = props.get("Week", {}).get("title", [{}])[0].get("plain_text", "?")
    week_start = props.get("Week Start", {}).get("date", {}).get("start", "?")
    alex = props.get("Alex: Customers Contacted", {}).get("number", None)
    aya = props.get("Aya: Customers Contacted", {}).get("number", None)
    print(f"  Week: {week_name}")
    print(f"  Week Start: {week_start}")
    print(f"  Alex: Customers Contacted = {alex}  (will be set by workflow)")
    print(f"  Aya: Customers Contacted  = {aya}  (will be set by workflow)")
    print(f"  ✅ Row is live and queryable")

print(f"""
{SECTION}
  DONE — W09 shell row created.

  Next step:
    1. Go to n8n UI → workflow 'iDA5BBJxsp0cmv2M'
    2. Click 'Execute Workflow'
    3. Verify:
       • Compute Week Bounds:  weekEnd = 2026-03-01  ✅
       • Guard node:           passes through (not empty)
       • Update Scorecard Row: HTTP 200
    4. Check the Notion W09 row has updated Alex/Aya counts.
{SECTION}
""")
