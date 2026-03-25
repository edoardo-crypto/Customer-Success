#!/usr/bin/env python3
"""
fix_stripe_notion_sync.py
=========================
1. Deactivate workflow Ai9Y3FWjqMtEhr57
2. Patch Schedule Trigger: cron → 30 9 * * *, timezone → Europe/Berlin
3. Patch Create New Row: Kickoff Date expression → DateTime.now().toFormat('yyyy-MM-dd')
4. PUT updated workflow
5. Reactivate workflow
6. PATCH Notion DB: update Journey Stage formula (adds Kickoff Date, fixes duplicate Testing Date)
"""

import json
import requests
import sys
import creds

# ── Config ──────────────────────────────────────────────────────────────────
N8N_API_KEY = creds.get("N8N_API_KEY")
N8N_BASE = "https://konvoai.app.n8n.cloud"
WORKFLOW_ID = "Ai9Y3FWjqMtEhr57"

NOTION_TOKEN = creds.get("NOTION_TOKEN")
NOTION_DB_ID = "84feda19cfaf4c6e9500bf21d2aaafef"

n8n_headers = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type": "application/json",
}
notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}

BACKUP_PATH = "/tmp/workflow_backup_stripe_sync.json"

# ── Journey Stage formula ────────────────────────────────────────────────────
# Fixes: duplicate Testing Date bug → now correctly includes Kickoff Date in the or()
JOURNEY_STAGE_FORMULA = (
    'if(!empty(prop("\U0001f622 Churn Date")),'
    'style("Graduated - Churned", "red_background"),'
    'if(!empty(prop("\U0001f680 Graduation Date")),'
    'style("Graduated - Nurturing", "green_background"),'
    'if(!empty(prop("\U0001f680 Go-Live Date")),'
    'style("Onboarding - Launched", "yellow_background"),'
    'if(or(!empty(prop("\U0001f680 Kickoff Date")), !empty(prop("\U0001f680 Testing Date"))),'
    'style("Onboarding - Pre-launch", "yellow_background"),'
    'style("Graduated - Nurturing", "green_background")'
    '))))'
)

def step(label):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")

def check(r, label):
    if r.status_code in (200, 201):
        print(f"  \u2713 {label} \u2192 {r.status_code}")
    else:
        print(f"  \u2717 {label} \u2192 {r.status_code}")
        print(f"    {r.text[:600]}")
        sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Load backup
step("1 · Load workflow backup")
with open(BACKUP_PATH) as f:
    wf = json.load(f)
print(f"  Loaded workflow '{wf['name']}' ({len(wf['nodes'])} nodes)")

# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Deactivate
step("2 · Deactivate workflow")
r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate", headers=n8n_headers)
check(r, "Deactivate")

# ─────────────────────────────────────────────────────────────────────────────
# Step 3 & 4: Patch nodes in the loaded workflow dict
step("3 · Patch Schedule Trigger and Create New Row nodes")

schedule_patched = False
kickoff_patched = False

for node in wf["nodes"]:

    # -- Schedule Trigger --
    if node["type"] == "n8n-nodes-base.scheduleTrigger":
        old_expr = node["parameters"]["rule"]["interval"][0].get("expression", "?")
        node["parameters"]["rule"]["interval"][0]["expression"] = "30 9 * * *"
        node["parameters"]["timezone"] = "Europe/Berlin"
        print(f"  Schedule '{node['name']}': '{old_expr}' -> '30 9 * * *', tz -> Europe/Berlin")
        schedule_patched = True

    # -- Create New Row (Kickoff Date) --
    if node["name"] == "Create New Row":
        body = node["parameters"]["jsonBody"]

        # Pattern in the decoded Python string (single quotes are literal):
        # \U0001f680 = rocket emoji 🚀
        OLD_KICKOFF = (
            "\U0001f680 Kickoff Date\":{\"date\":{\"start\":\""
            "{{ $('Transform Active Subs').item.json.contract_start }}\""
        )
        NEW_KICKOFF = (
            "\U0001f680 Kickoff Date\":{\"date\":{\"start\":\""
            "{{ DateTime.now().toFormat('yyyy-MM-dd') }}\""
        )

        if OLD_KICKOFF not in body:
            print(f"  ERROR: Kickoff Date pattern not found in Create New Row body")
            idx = body.find("Kickoff")
            print(f"  Body around 'Kickoff': {repr(body[max(0,idx-5):idx+100])}")
            sys.exit(1)

        node["parameters"]["jsonBody"] = body.replace(OLD_KICKOFF, NEW_KICKOFF, 1)
        print(f"  Create New Row: Kickoff Date -> DateTime.now().toFormat('yyyy-MM-dd') ✓")
        kickoff_patched = True

if not schedule_patched:
    print("  ERROR: Schedule Trigger node not found")
    sys.exit(1)
if not kickoff_patched:
    print("  ERROR: Create New Row node not found")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: PUT workflow (strip read-only fields the API won't accept)
step("4 · PUT updated workflow")
PUT_KEYS = {"name", "nodes", "connections", "settings"}
wf_put = {k: v for k, v in wf.items() if k in PUT_KEYS}
r = requests.put(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=n8n_headers, json=wf_put)
check(r, "PUT workflow")

# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Reactivate
step("5 · Reactivate workflow")
r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate", headers=n8n_headers)
check(r, "Activate")

# ─────────────────────────────────────────────────────────────────────────────
# Step 7: Update Notion Journey Stage formula
step("6 · Patch Notion Journey Stage formula")

# First, fetch the current formula to show what we're replacing
r_get = requests.get(
    f"https://api.notion.com/v1/databases/{NOTION_DB_ID}",
    headers=notion_headers,
)
if r_get.status_code == 200:
    db = r_get.json()
    props = db.get("properties", {})
    for prop_name, prop_val in props.items():
        if "Journey Stage" in prop_name:
            old_formula = prop_val.get("formula", {}).get("expression", "")
            print(f"  Current formula: {old_formula[:120]}...")
            break

payload = {
    "properties": {
        "\u2764\ufe0f Journey Stage": {
            "formula": {
                "expression": JOURNEY_STAGE_FORMULA
            }
        }
    }
}
r = requests.patch(
    f"https://api.notion.com/v1/databases/{NOTION_DB_ID}",
    headers=notion_headers,
    json=payload,
)
check(r, "Patch Notion DB formula")
print(f"  New formula: {JOURNEY_STAGE_FORMULA[:120]}...")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  ALL STEPS COMPLETE")
print("="*60)
print()
print("  IMPORTANT: Go to n8n UI and click 'Execute Workflow'")
print(f"  on '{wf['name']}' to sync Sherperex now.")
print("  (POST /run returns 405 on n8n Cloud - manual trigger only)")
print()
print("  Verify in Notion:")
print("  - Search 'Sherperex' -> row with Kickoff Date = 2026-02-19")
print("  - Journey Stage = 'Onboarding - Pre-launch'")
