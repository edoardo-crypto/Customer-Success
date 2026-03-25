#!/usr/bin/env python3
"""
fix_stripe_sync_paireditem_v2.py

Fixes the silent-drop bug in Stripe→Notion Sync the correct way:
- includeInputFields:true on the Notion Query node DID NOT carry Stripe fields
  through to the response items (n8n doesn't merge them at this point in the chain).
- Instead, use runOnceForAllItems mode in Prepare New Customer Payload and do
  index-based pairing between Transform Active Subs and Notion Query outputs,
  keeping only the rows where Notion returned 0 results (= new customer, no row yet).
"""

import json
import sys
import requests
import creds

N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "Ai9Y3FWjqMtEhr57"
BACKUP_PATH = "/tmp/workflow_backup_stripe_paireditem_v2.json"

n8n_headers = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

sep = "=" * 64

def step(label):
    print(f"\n{sep}\n  {label}\n{sep}")

def check(r, label):
    if r.status_code in (200, 201):
        print(f"  \u2713 {label} \u2192 {r.status_code}")
    else:
        print(f"  \u2717 {label} \u2192 {r.status_code}")
        print(f"    {r.text[:800]}")
        sys.exit(1)


# ── Step 1: Load workflow ──────────────────────────────────────────────────────
step("1 · Load workflow")
r = requests.get(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=n8n_headers)
check(r, "GET workflow")
workflow = r.json()
print(f"  Name: {workflow['name']}  |  Nodes: {len(workflow['nodes'])}")

with open(BACKUP_PATH, "w") as f:
    json.dump(workflow, f, indent=2)
print(f"  Backup: {BACKUP_PATH}")


# ── Step 2: Revert includeInputFields on Notion Query (wasn't helping) ─────────
step("2 · Revert Notion Query by Stripe ID — remove includeInputFields")

query_node = next(
    (n for n in workflow["nodes"] if "Notion Query by Stripe" in n.get("name", "")),
    None,
)
if not query_node:
    print("  \u2717 Could not find Notion Query by Stripe ID node")
    sys.exit(1)

query_node.setdefault("parameters", {})["options"] = {}
print(f"  options reset to {{}} on '{query_node['name']}'")


# ── Step 3: Patch Prepare New Customer Payload ────────────────────────────────
step("3 · Patch Prepare New Customer Payload — runOnceForAllItems + index pairing")

prepare_node = next(
    (n for n in workflow["nodes"] if "Prepare New Customer Payload" in n.get("name", "")),
    None,
)
if not prepare_node:
    print("  \u2717 Could not find Prepare New Customer Payload node")
    sys.exit(1)

print(f"  Found: '{prepare_node['name']}'")
print(f"  Current mode: {prepare_node.get('parameters', {}).get('mode', '(not set)')}")

# Switch to runOnceForAllItems and use index-based matching.
# Transform Active Subs and Notion Query run in the same order (1:1 per customer).
# We keep only indices where Notion returned 0 results = new customer.
NEW_JS_CODE = """\
// runOnceForAllItems — index-based pairing (no pairedItem dependency)
// Transform Active Subs[i] and Notion Query by Stripe ID[i] are 1:1 by position.
// Keep only rows where Notion returned 0 results = customer has no MCT row yet.
const transforms    = $('Transform Active Subs').all();
const notionResults = $('Notion Query by Stripe ID').all();

return transforms
    .map((item, idx) => {
        const notion   = notionResults[idx];
        const hasRow   = notion && notion.json.results && notion.json.results.length > 0;
        if (hasRow) return null;
        return { json: item.json };
    })
    .filter(item => item !== null);
"""

prepare_node.setdefault("parameters", {})["jsCode"] = NEW_JS_CODE
prepare_node["parameters"]["mode"] = "runOnceForAllItems"
print(f"  New mode: runOnceForAllItems")
print(f"  New code uses index-based pairing from Transform Active Subs + Notion Query")


# ── Step 4: Deactivate → PUT → Activate ───────────────────────────────────────
step("4 · Deactivate workflow")
r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate", headers=n8n_headers)
check(r, "Deactivate")

step("5 · PUT updated workflow")
put_payload = {
    "name":        workflow["name"],
    "nodes":       workflow["nodes"],
    "connections": workflow["connections"],
    "settings":    workflow.get("settings", {}),
}
r = requests.put(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=n8n_headers, json=put_payload)
check(r, "PUT workflow")

step("6 · Reactivate workflow")
r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate", headers=n8n_headers)
check(r, "Activate")

print(f"\n{sep}")
print("  DONE — Prepare New Customer Payload now uses index-based pairing")
print(sep)
print()
print("  What changed:")
print("  - Notion Query: includeInputFields reverted (wasn't merging Stripe fields)")
print("  - Prepare New Customer Payload: runOnceForAllItems + index match")
print("    transforms[i] paired with notionResults[i], keeps new customers only")
print()
print("  Backup saved to:", BACKUP_PATH)
print("  → Now re-run the workflow from n8n UI to verify")
print()
