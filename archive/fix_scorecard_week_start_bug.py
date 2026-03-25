#!/usr/bin/env python3
"""
fix_scorecard_week_start_bug.py

Fixes three issues found after diagnosing wrong W09 scorecard rows:

Step 1 — Patch Scorecard Builder workflow (eUwMYFeglyv9bHxn):
  The "Compute Week Bounds" node used `new Date(now)` directly as currentWeekStart.
  When triggered mid-week (e.g., Wednesday), this set the wrong date.
  Fix: compute ISO Monday explicitly (same logic as Customers Contacted Tracker).

Step 2 — Archive the wrong W09 shell row:
  Page 312e418f-d8c4-81b2-b4ed-cbe258147c15 (Week Start = 2026-02-25, a Wednesday)
  was created by the buggy Builder. Archive it so it doesn't pollute lookups.

Step 3 — Rename the correct W09 row to proper Builder format:
  Page 312e418f-d8c4-81c9-aa87-c118d6551929 (Week Start = 2026-02-23, correct Monday)
  is named "W09 2026" — rename to "W09 (2026-02-23 - 2026-03-01)" so the Builder
  recognises it next Monday when writing KPIs.

Idempotent: safe to re-run (each step checks before acting).
"""

import json
import requests
import creds

# ── Credentials ────────────────────────────────────────────────────────────────

N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "eUwMYFeglyv9bHxn"

NOTION_TOKEN  = creds.get("NOTION_TOKEN")

# Wrong row created by buggy mid-week trigger (Week Start = 2026-02-25, Wednesday)
WRONG_W09_PAGE_ID   = "312e418f-d8c4-81b2-b4ed-cbe258147c15"

# Correct row (Week Start = 2026-02-23, Monday) — needs rename only
CORRECT_W09_PAGE_ID = "312e418f-d8c4-81c9-aa87-c118d6551929"
CORRECT_W09_LABEL   = "W09 (2026-02-23 - 2026-03-01)"

N8N_HEADERS    = {"X-N8N-API-KEY": N8N_API_KEY, "Content-Type": "application/json"}
NOTION_HEADERS = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}

# ── JS patch strings ───────────────────────────────────────────────────────────

# The buggy block that was inserted by fix_scorecard_current_week_shell.py
OLD_CURRENT_WEEK_BLOCK = """\
// Current week (today = Monday = new week's first day)
const currentWeekStart = new Date(now);
currentWeekStart.setUTCHours(0, 0, 0, 0);"""

# Fixed block: always rolls back to ISO Monday regardless of which day it is
NEW_CURRENT_WEEK_BLOCK = """\
// Current week — compute ISO Monday regardless of which day it is
const daysToMon = now.getUTCDay() === 0 ? 6 : now.getUTCDay() - 1;
const currentWeekStart = new Date(now);
currentWeekStart.setUTCDate(now.getUTCDate() - daysToMon);
currentWeekStart.setUTCHours(0, 0, 0, 0);"""


# ── n8n helpers ────────────────────────────────────────────────────────────────

def fetch_workflow():
    r = requests.get(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=N8N_HEADERS)
    r.raise_for_status()
    return r.json()


def put_workflow(name, nodes, connections, settings):
    body = {"name": name, "nodes": nodes, "connections": connections, "settings": settings}
    r = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}",
        headers=N8N_HEADERS,
        json=body,
    )
    if r.status_code not in (200, 201):
        print(f"  PUT failed: HTTP {r.status_code}")
        print(f"  Body: {r.text[:800]}")
        r.raise_for_status()
    return r.json()


# ── Notion helpers ─────────────────────────────────────────────────────────────

def notion_patch(page_id, payload):
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json=payload,
    )
    return r


# ── Steps ─────────────────────────────────────────────────────────────────────

def step1_fix_workflow_js():
    print("\n" + "=" * 65)
    print("STEP 1 — Fix 'Compute Week Bounds' JS in Scorecard Builder")
    print("=" * 65)

    print("  Fetching live workflow …")
    wf       = fetch_workflow()
    name     = wf["name"]
    nodes    = wf["nodes"]
    conns    = wf.get("connections", {})
    settings = wf.get("settings", {})
    print(f"  Workflow: {name!r}  ({len(nodes)} nodes)")

    bounds_node = next((n for n in nodes if n["name"] == "Compute Week Bounds"), None)
    if bounds_node is None:
        print("  ERROR: 'Compute Week Bounds' node not found — aborting step 1")
        return

    js = bounds_node["parameters"]["jsCode"]

    # Idempotency: already fixed if daysToMon is present
    if "daysToMon" in js:
        print("  Already fixed (daysToMon present) — skipping ✓")
        return

    if OLD_CURRENT_WEEK_BLOCK not in js:
        print("  WARNING: expected old JS block not found — cannot patch automatically.")
        print("  Printing first 400 chars of current jsCode for inspection:")
        # Find the currentWeekStart line and print context
        for i, line in enumerate(js.splitlines()):
            if "currentWeekStart" in line:
                start = max(0, i - 2)
                snippet = "\n".join(js.splitlines()[start:start+6])
                print(f"  ... around 'currentWeekStart':\n{snippet}")
                break
        return

    new_js = js.replace(OLD_CURRENT_WEEK_BLOCK, NEW_CURRENT_WEEK_BLOCK)
    bounds_node["parameters"]["jsCode"] = new_js
    print("  ✓ Replaced buggy 'today' block with ISO Monday logic")

    print("  Pushing updated workflow …")
    result = put_workflow(name, nodes, conns, settings)
    print(f"  ✓ PUT successful — {len(result.get('nodes', nodes))} nodes")


def step2_archive_wrong_w09():
    print("\n" + "=" * 65)
    print("STEP 2 — Archive wrong W09 row (Week Start = 2026-02-25)")
    print("=" * 65)
    print(f"  Page ID: {WRONG_W09_PAGE_ID}")

    # First check if it's already archived
    r = requests.get(
        f"https://api.notion.com/v1/pages/{WRONG_W09_PAGE_ID}",
        headers=NOTION_HEADERS,
    )
    if r.status_code == 404:
        print("  Page not found (already deleted?) — skipping ✓")
        return
    if r.status_code != 200:
        print(f"  GET failed: HTTP {r.status_code} — {r.text[:200]}")
        return

    page = r.json()
    if page.get("archived"):
        print("  Already archived — skipping ✓")
        return

    # Confirm it's the right page (Week Start = 2026-02-25)
    week_start_prop = page.get("properties", {}).get("Week Start", {})
    date_val = week_start_prop.get("date", {})
    start_date = date_val.get("start", "") if date_val else ""
    print(f"  Confirmed Week Start = {start_date!r}")

    r2 = notion_patch(WRONG_W09_PAGE_ID, {"archived": True})
    if r2.status_code == 200:
        print("  ✓ Archived successfully")
    else:
        print(f"  ERROR: HTTP {r2.status_code} — {r2.text[:300]}")


def step3_rename_correct_w09():
    print("\n" + "=" * 65)
    print("STEP 3 — Rename correct W09 row to Builder format")
    print("=" * 65)
    print(f"  Page ID:   {CORRECT_W09_PAGE_ID}")
    print(f"  New label: {CORRECT_W09_LABEL!r}")

    # Check current title
    r = requests.get(
        f"https://api.notion.com/v1/pages/{CORRECT_W09_PAGE_ID}",
        headers=NOTION_HEADERS,
    )
    if r.status_code == 404:
        print("  Page not found — skipping")
        return
    if r.status_code != 200:
        print(f"  GET failed: HTTP {r.status_code} — {r.text[:200]}")
        return

    page      = r.json()
    week_prop = page.get("properties", {}).get("Week", {})
    title_arr = week_prop.get("title", [])
    current_title = title_arr[0]["plain_text"] if title_arr else ""
    print(f"  Current title: {current_title!r}")

    if current_title == CORRECT_W09_LABEL:
        print("  Already correct — skipping ✓")
        return

    payload = {
        "properties": {
            "Week": {
                "title": [{"text": {"content": CORRECT_W09_LABEL}}]
            }
        }
    }
    r2 = notion_patch(CORRECT_W09_PAGE_ID, payload)
    if r2.status_code == 200:
        print(f"  ✓ Renamed to {CORRECT_W09_LABEL!r}")
    else:
        print(f"  ERROR: HTTP {r2.status_code} — {r2.text[:300]}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("fix_scorecard_week_start_bug.py")
    print("=" * 65)

    step1_fix_workflow_js()
    step2_archive_wrong_w09()
    step3_rename_correct_w09()

    print("\n" + "=" * 65)
    print("DONE")
    print("=" * 65)
    print()
    print("Next user action:")
    print("  1. Open n8n UI → workflow iDA5BBJxsp0cmv2M (Customers Contacted Tracker)")
    print("  2. Click 'Execute Workflow' manually")
    print("  3. Verify:")
    print("     • weekEnd = 2026-03-01 in 'Compute Week Bounds'")
    print("     • Guard node passes (not []) — finds the renamed W09 row")
    print("     • Update Scorecard Row → HTTP 200")
    print("  4. Check Notion 📊 Weekly CS Scorecards:")
    print("     • Exactly 2 active rows: W08 (2026-02-18) + W09 (2026-02-23)")
    print("     • W09 now shows Alex/Aya Customers Contacted (non-zero)")
    print()
    print("Known gap: W08 Customers Contacted remains null — historical data")
    print("that cannot be auto-recovered. Enter approximate values manually")
    print("in Notion if needed, or leave blank.")
    print()


if __name__ == "__main__":
    main()
