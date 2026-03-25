#!/usr/bin/env python3
"""
verify_customers_contacted.py

Post-patch verification for the 'Customers Contacted Tracker' workflow.
Checks:
  1. n8n workflow iDA5BBJxsp0cmv2M — is the Sunday-based week-bounds patch applied?
  2. Notion Scorecard DB — does an active (non-trashed) W09 row exist?
  3. Latest n8n execution — what did 'Compute Week Bounds' output?
"""

import json
import requests
from datetime import datetime, timezone, timedelta
import creds

# ── Constants ──────────────────────────────────────────────────────────────────
N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "iDA5BBJxsp0cmv2M"
IDEMPOTENCY_MARKER = "const sunday = new Date(monday);"

NOTION_TOKEN = creds.get("NOTION_TOKEN")
SCORECARD_DB_ID = "311e418f-d8c4-810e-8b11-cdc50357e709"
W09_WEEK_START = "2026-02-23"  # Monday of W09

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type": "application/json",
}
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

SECTION = "=" * 65


def section(title):
    print(f"\n{SECTION}")
    print(f"  {title}")
    print(SECTION)


# ── Check 1: n8n workflow patch ─────────────────────────────────────────────
section("CHECK 1 — n8n workflow: is Sunday-based patch applied?")

r = requests.get(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=N8N_HEADERS)
r.raise_for_status()
wf = r.json()
print(f"  Workflow: '{wf['name']}'  active={wf.get('active')}  nodes={len(wf['nodes'])}")

target_node = next((n for n in wf["nodes"] if n["name"] == "Compute Week Bounds"), None)
if not target_node:
    print("  ERROR: 'Compute Week Bounds' node not found!")
else:
    js = target_node.get("parameters", {}).get("jsCode", "")
    if IDEMPOTENCY_MARKER in js:
        print("  ✅ PATCH IS APPLIED — 'const sunday = new Date(monday);' found in JS")
        # Show what weekEnd line produces
        if "sunday.setUTCDate(monday.getUTCDate() + 6)" in js:
            print("  ✅ weekEnd = monday + 6 days (correct Sunday logic)")
    else:
        print("  ❌ PATCH NOT APPLIED — old code still running")
        print(f"  JS preview: {js[:120].strip()!r}…")


# ── Check 2: Latest n8n execution ───────────────────────────────────────────
section("CHECK 2 — Latest n8n execution details")

r2 = requests.get(
    f"{N8N_BASE}/api/v1/executions",
    headers=N8N_HEADERS,
    params={"workflowId": WORKFLOW_ID, "limit": 5, "includeData": "true"},
)
r2.raise_for_status()
executions = r2.json().get("data", [])

if not executions:
    print("  No executions found.")
else:
    for ex in executions[:3]:
        ex_id = ex.get("id")
        status = ex.get("status", "unknown")
        started = ex.get("startedAt", "?")
        finished = ex.get("stoppedAt", "?")
        print(f"\n  Execution #{ex_id}  status={status}  started={started}")

        # Try to find weekStart/weekEnd from the Compute Week Bounds node output
        run_data = ex.get("data", {}).get("resultData", {}).get("runData", {})
        cwb_data = run_data.get("Compute Week Bounds", [])
        if cwb_data:
            try:
                item = cwb_data[0]["data"]["main"][0][0]["json"]
                week_start = item.get("weekStart", "?")
                week_end = item.get("weekEnd", "?")
                print(f"    ↳ weekStart={week_start}  weekEnd={week_end}", end="")
                if week_end == "2026-03-01":
                    print("  ✅ CORRECT (Sunday of W09)")
                elif week_end:
                    print(f"  ⚠️  Expected 2026-03-01, got {week_end}")
                else:
                    print()
            except (KeyError, IndexError, TypeError):
                print("    ↳ Could not parse Compute Week Bounds output")
        else:
            print("    ↳ No 'Compute Week Bounds' output in execution data (may need includeData)")

        # Check Guard node outcome
        guard_data = run_data.get("Guard: Scorecard Row Found?", [])
        if guard_data:
            try:
                guard_items = guard_data[0]["data"]["main"][0]
                if guard_items:
                    print(f"    ↳ Guard node: passed {len(guard_items)} item(s) through → Notion update proceeded")
                else:
                    print("    ↳ Guard node: returned [] → Notion update was SKIPPED (no scorecard row found)")
            except (KeyError, IndexError, TypeError):
                pass

        # Alex/Aya counts
        for node_name in ["Count Contacts per CSM", "Match Intercom + Union + Count"]:
            count_data = run_data.get(node_name, [])
            if count_data:
                try:
                    items = count_data[0]["data"]["main"][0]
                    for itm in items:
                        j = itm.get("json", {})
                        alex = j.get("alexCount", j.get("alex_count", "?"))
                        aya = j.get("ayaCount", j.get("aya_count", "?"))
                        print(f"    ↳ {node_name}: alexCount={alex}  ayaCount={aya}")
                except (KeyError, IndexError, TypeError):
                    pass


# ── Check 3: Notion W09 scorecard row ──────────────────────────────────────
section("CHECK 3 — Notion Scorecard DB: W09 row (Week Start = 2026-02-23)")

notion_body = {
    "filter": {
        "property": "Week Start",
        "date": {"equals": W09_WEEK_START},
    }
}
r3 = requests.post(
    f"https://api.notion.com/v1/databases/{SCORECARD_DB_ID}/query",
    headers=NOTION_HEADERS,
    json=notion_body,
)

if r3.status_code != 200:
    print(f"  Notion query failed: {r3.status_code} — {r3.text[:400]}")
else:
    results = r3.json().get("results", [])
    print(f"  Notion returned {len(results)} result(s) for Week Start = {W09_WEEK_START}")

    if not results:
        print("  ⚠️  NO W09 ROW FOUND in Notion — workflow Guard node will cleanly stop")
        print("  Options:")
        print("    A) Restore the W09 page from trash in Notion UI, then re-run workflow")
        print("    B) Wait for the Scorecard Builder workflow to create a fresh shell on Monday")
    else:
        for page in results:
            page_id = page.get("id", "?")
            archived = page.get("archived", False)
            in_trash = page.get("in_trash", False)
            props = page.get("properties", {})

            # Week Start
            ws_prop = props.get("Week Start", {})
            ws_val = ws_prop.get("date", {})
            ws_start = ws_val.get("start", "?") if ws_val else "?"

            # Alex/Aya contacts
            alex_prop = props.get("Alex: Customers Contacted", {})
            alex_val = alex_prop.get("number", "?")
            aya_prop = props.get("Aya: Customers Contacted", {})
            aya_val = aya_prop.get("number", "?")

            # Status
            status_prop = props.get("Status", {})
            status_val = status_prop.get("status", {})
            status_name = status_val.get("name", "?") if status_val else "?"

            print(f"\n  Page ID: {page_id}")
            print(f"    Week Start: {ws_start}")
            print(f"    Alex Customers Contacted: {alex_val}")
            print(f"    Aya Customers Contacted:  {aya_val}")
            print(f"    Status: {status_name}")
            print(f"    archived={archived}  in_trash={in_trash}", end="")
            if in_trash:
                print("  ⚠️  PAGE IS IN TRASH — workflow won't update it")
            elif archived:
                print("  ⚠️  PAGE IS ARCHIVED")
            else:
                print("  ✅ Page is live")

# ── Summary ─────────────────────────────────────────────────────────────────
section("SUMMARY & NEXT STEPS")
print("""
  1. If CHECK 1 shows patch APPLIED → good. Proceed to step 2.
  2. If CHECK 2 shows latest execution used old weekEnd → you need to run
     the workflow once more from the n8n UI ('Execute Workflow' button).
  3. If CHECK 3 shows row IN TRASH → restore it from Notion trash, then
     re-run the workflow so it can update it with fresh counts.
  4. If CHECK 3 shows NO ROW → the Scorecard Builder should have created
     one at 06:00 Monday. You can also create it manually or wait until
     the shell workflow runs.
  5. After a successful run with the new code, verify:
       weekEnd = 2026-03-01  (Sunday of W09)
       Guard node passed through (not empty [])
       Alex/Aya counts are non-zero
""")
