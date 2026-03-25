#!/usr/bin/env python3
"""
fix_scorecard_current_week_shell.py

Patches live workflow eUwMYFeglyv9bHxn to add a parallel branch that:
  1. Checks if the CURRENT week's Notion scorecard row already exists
  2. Creates a blank shell row (Week title + Week Start only) if not

Problem solved:
  - Scorecard Builder runs Monday 06:00 → writes LAST week's row
  - Customers Contacted Tracker runs daily 07:00 → looks for CURRENT week's row
  - These are offset by one week → KPI 6 is always empty unless the row is pre-created

Fix: early Monday morning, Builder now also creates a shell row for the current week
so the Tracker can find and update it at 07:00.

Idempotent: re-running this script is safe.
"""

import json
import uuid
import requests
import creds

# ── Constants ──────────────────────────────────────────────────────────────────
N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")

WORKFLOW_ID      = "eUwMYFeglyv9bHxn"
SCORECARD_DB_ID  = "311e418f-d8c4-810e-8b11-cdc50357e709"
NOTION_CRED_ID   = "LH587kxanQCPcd9y"
NOTION_CRED_NAME = "Notion - Enrichment"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}


def uid():
    return str(uuid.uuid4())


# ── JS patch for "Compute Week Bounds" ────────────────────────────────────────
# We insert current-week variables before the existing return statement,
# then extend the return to also expose them.

OLD_RETURN = (
    "return [{ json: { weekStartStr, weekEndStr, weekStartTs, weekEndTs, weekLabel, intercomBody } }];"
)

CURRENT_WEEK_JS_ADDITION = """\

// Current week (today = Monday = new week's first day)
const currentWeekStart = new Date(now);
currentWeekStart.setUTCHours(0, 0, 0, 0);
const currentWeekStartStr = toDateStr(currentWeekStart);
const currentWeekNum = isoWeek(currentWeekStart);
const currentWeekEndDate = new Date(currentWeekStart);
currentWeekEndDate.setUTCDate(currentWeekStart.getUTCDate() + 6);
const currentWeekLabel = 'W' + String(currentWeekNum).padStart(2, '0')
    + ' (' + currentWeekStartStr + ' - ' + toDateStr(currentWeekEndDate) + ')';
const currentWeekQueryBody = JSON.stringify({
    filter: { property: 'Week Start', date: { equals: currentWeekStartStr } },
    page_size: 1,
});

"""

NEW_RETURN = (
    "return [{ json: {"
    " weekStartStr, weekEndStr, weekStartTs, weekEndTs, weekLabel, intercomBody,"
    " currentWeekStartStr, currentWeekLabel, currentWeekQueryBody"
    " } }];"
)


# ── Shell Needed? code node JS ────────────────────────────────────────────────
# Uses SCORECARD_DB_ID_PLACEHOLDER — replaced at build time.
# Plain triple-quoted string (no f-string) so JS braces are literal.

SHELL_NEEDED_JS_TEMPLATE = """\
const SCORECARD_DB_ID = 'SCORECARD_DB_ID_PLACEHOLDER';
const results = $input.first().json.results || [];
if (results.length > 0) {
    console.log('[shell-check] current week row already exists, skip');
    return [];   // stops execution cleanly — no create needed
}
const bounds = $('Compute Week Bounds').first().json;
const createBody = JSON.stringify({
    parent: { database_id: SCORECARD_DB_ID },
    properties: {
        'Week':       { title: [{ text: { content: bounds.currentWeekLabel } }] },
        'Week Start': { date:  { start: bounds.currentWeekStartStr } },
    },
});
console.log('[shell-check] creating shell row for ' + bounds.currentWeekLabel);
return [{ json: { createBody } }];
"""


# ── n8n credential / header helpers ───────────────────────────────────────────

def notion_cred():
    return {"httpHeaderAuth": {"id": NOTION_CRED_ID, "name": NOTION_CRED_NAME}}


def notion_auth():
    return {"authentication": "genericCredentialType", "genericAuthType": "httpHeaderAuth"}


def notion_header_v2():
    return {
        "sendHeaders": True,
        "headerParameters": {
            "parameters": [{"name": "Notion-Version", "value": "2022-06-28"}]
        },
    }


# ── n8n API helpers ────────────────────────────────────────────────────────────

def fetch_workflow():
    r = requests.get(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=N8N_HEADERS)
    r.raise_for_status()
    return r.json()


def put_workflow(wf_id, name, nodes, connections, settings):
    body = {
        "name":        name,
        "nodes":       nodes,
        "connections": connections,
        "settings":    settings,
    }
    r = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{wf_id}",
        headers=N8N_HEADERS,
        json=body,
    )
    if r.status_code not in (200, 201):
        print(f"  PUT failed: HTTP {r.status_code}")
        print(f"  Response: {r.text[:800]}")
        r.raise_for_status()
    return r.json()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("fix_scorecard_current_week_shell.py")
    print(f"Patching live workflow: {WORKFLOW_ID}")
    print("=" * 65)

    # ── Step 1: Fetch live workflow ───────────────────────────────────────────
    print("\n[1/5] Fetching live workflow …")
    wf       = fetch_workflow()
    name     = wf["name"]
    nodes    = wf["nodes"]
    conns    = wf.get("connections", {})
    settings = wf.get("settings", {})
    print(f"  Name: {name!r}")
    print(f"  Nodes ({len(nodes)}):")
    for n in nodes:
        print(f"    - {n['name']}")

    # ── Step 2: Patch "Compute Week Bounds" JS ────────────────────────────────
    print("\n[2/5] Patching 'Compute Week Bounds' JS …")
    bounds_node = next((n for n in nodes if n["name"] == "Compute Week Bounds"), None)
    if bounds_node is None:
        print("  ERROR: 'Compute Week Bounds' node not found!")
        return

    js = bounds_node["parameters"]["jsCode"]

    if "currentWeekStartStr" in js:
        print("  Already patched — skipping JS update (idempotent) ✓")
    else:
        if OLD_RETURN not in js:
            print("  ERROR: expected return statement not found in node JS!")
            print(f"  Looking for: {OLD_RETURN!r}")
            return
        js = js.replace(OLD_RETURN, CURRENT_WEEK_JS_ADDITION + NEW_RETURN)
        bounds_node["parameters"]["jsCode"] = js
        print("  ✓ Inserted current-week variables + updated return statement")

    # ── Step 3: Add 3 new nodes (if not already present) ─────────────────────
    print("\n[3/5] Adding new nodes …")
    existing_names = {n["name"] for n in nodes}

    if "Find Current Week Row" in existing_names:
        print("  New nodes already present — skipping (idempotent) ✓")
    else:
        shell_js = SHELL_NEEDED_JS_TEMPLATE.replace(
            "SCORECARD_DB_ID_PLACEHOLDER", SCORECARD_DB_ID
        )

        # Node A: Find Current Week Row
        node_find_current = {
            "id":          uid(),
            "name":        "Find Current Week Row",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [280, 500],
            "credentials": notion_cred(),
            "parameters": {
                **notion_auth(),
                "method":         "POST",
                "url":            f"https://api.notion.com/v1/databases/{SCORECARD_DB_ID}/query",
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                "body": "={{ $('Compute Week Bounds').first().json.currentWeekQueryBody }}",
                **notion_header_v2(),
                "options": {"continueOnFail": True},
            },
        }

        # Node B: Shell Needed? (code node — returns [] if row exists, createBody if not)
        node_shell_check = {
            "id":          uid(),
            "name":        "Shell Needed?",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    [560, 500],
            "parameters":  {"mode": "runOnceForAllItems", "jsCode": shell_js},
        }

        # Node C: Create Current Week Shell
        node_create_shell = {
            "id":          uid(),
            "name":        "Create Current Week Shell",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [840, 500],
            "credentials": notion_cred(),
            "parameters": {
                **notion_auth(),
                "method":         "POST",
                "url":            "https://api.notion.com/v1/pages",
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                "body":           "={{ $json.createBody }}",
                **notion_header_v2(),
                "options": {"continueOnFail": True},
            },
        }

        nodes.append(node_find_current)
        nodes.append(node_shell_check)
        nodes.append(node_create_shell)
        print("  ✓ Added: Find Current Week Row")
        print("  ✓ Added: Shell Needed?")
        print("  ✓ Added: Create Current Week Shell")

    # ── Step 4: Update connections ────────────────────────────────────────────
    print("\n[4/5] Updating connections …")

    # Check if already fanned out
    bounds_targets = []
    if "Compute Week Bounds" in conns:
        main0 = conns["Compute Week Bounds"].get("main", [[]])[0]
        bounds_targets = [c.get("node") for c in main0]

    if "Find Current Week Row" in bounds_targets:
        print("  Connections already updated — skipping (idempotent) ✓")
    else:
        # Preserve existing downstream target (should be "Fetch MCT Page 1")
        existing_target = "Fetch MCT Page 1"
        if bounds_targets:
            existing_target = bounds_targets[0]

        # Fan-out: Compute Week Bounds → both chains in parallel
        conns["Compute Week Bounds"] = {
            "main": [[
                {"node": existing_target,        "type": "main", "index": 0},
                {"node": "Find Current Week Row", "type": "main", "index": 0},
            ]]
        }
        # New linear shell branch
        conns["Find Current Week Row"] = {
            "main": [[{"node": "Shell Needed?",             "type": "main", "index": 0}]]
        }
        conns["Shell Needed?"] = {
            "main": [[{"node": "Create Current Week Shell", "type": "main", "index": 0}]]
        }
        print(f"  ✓ Compute Week Bounds now fans out to:")
        print(f"      • {existing_target}  (main KPI chain)")
        print(f"      • Find Current Week Row  (shell branch)")
        print(f"  ✓ Find Current Week Row → Shell Needed? → Create Current Week Shell")

    # ── Step 5: PUT updated workflow ─────────────────────────────────────────
    print("\n[5/5] Pushing updated workflow to n8n …")
    result         = put_workflow(WORKFLOW_ID, name, nodes, conns, settings)
    new_node_count = len(result.get("nodes", nodes))
    print(f"  ✓ PUT successful — workflow now has {new_node_count} nodes")

    print("\n" + "=" * 65)
    print("DONE — verification steps:")
    print("=" * 65)
    print()
    print("1. In n8n UI, manually execute the Scorecard Builder:")
    print(f"   {N8N_BASE}/workflow/{WORKFLOW_ID}")
    print()
    print("2. Check Notion '📊 Weekly CS Scorecards' — a new shell row for")
    print("   the CURRENT week should appear with 'Week' + 'Week Start' set,")
    print("   all KPI numbers blank.")
    print()
    print("3. Run the Builder again → 'Shell Needed?' should log")
    print("   '[shell-check] current week row already exists, skip'")
    print("   (idempotency check).")
    print()
    print("4. Toggle the Scorecard Builder ON in n8n UI to activate cron.")
    print()


if __name__ == "__main__":
    main()
