#!/usr/bin/env python3
"""
fix_stripe_sync_paireditem_v3.py

Correct architectural fix for the Stripe→Notion Sync silent-drop bug.

Root cause:
  On the FALSE branch of IF-Row Exists, items carry only the Notion API
  response structure (object/results/next_cursor/...). The Stripe customer
  fields (stripe_customer_id, customer_name, domain, mrr, ...) are only
  available via pairedItem → Transform Active Subs, but that chain breaks
  in runOnceForAllItems Code nodes on a branch.

Fix:
  Insert a "Stamp Customer Fields" Code node (runOnceForEachItem) between
  Notion Query and IF-Row Exists, BEFORE any branching.
  While pairedItem is still intact, it reads both the Notion response AND
  the Stripe customer data and merges them into one item.
  All downstream nodes (true AND false branches) then have $json.stripe_customer_id
  etc. directly available — no more pairedItem lookups needed in the false branch.

Side effects on Update Existing Row:
  - $json.results[0].id still works  ✓
  - $('Transform Active Subs').item.json.* still works via pairedItem chain  ✓
    (runOnceForEachItem Code nodes preserve pairedItem automatically)

Prepare New Customer Payload: simplified to a passthrough.
"""

import json, sys, uuid, requests
import creds

N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "Ai9Y3FWjqMtEhr57"
BACKUP_PATH = "/tmp/workflow_backup_stripe_v3.json"

headers = {"X-N8N-API-KEY": N8N_API_KEY, "Content-Type": "application/json"}

sep = "=" * 64
def step(label): print(f"\n{sep}\n  {label}\n{sep}")
def check(r, label):
    if r.status_code in (200, 201):
        print(f"  \u2713 {label} \u2192 {r.status_code}")
    else:
        print(f"  \u2717 {label} \u2192 {r.status_code}\n    {r.text[:600]}")
        sys.exit(1)


# ── Step 1: Load workflow ──────────────────────────────────────────────────────
step("1 · Load workflow")
r = requests.get(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=headers)
check(r, "GET workflow")
wf = r.json()
print(f"  Name: {wf['name']}  |  Nodes: {len(wf['nodes'])}")
with open(BACKUP_PATH, "w") as f: json.dump(wf, f, indent=2)
print(f"  Backup: {BACKUP_PATH}")


# ── Step 2: Find existing nodes and their positions ───────────────────────────
step("2 · Locate Notion Query and IF nodes")

notion_query = next(n for n in wf["nodes"] if "Notion Query by Stripe" in n.get("name",""))
if_node      = next(n for n in wf["nodes"] if n.get("name") == "IF - Row Exists")
prepare_node = next(n for n in wf["nodes"] if "Prepare New Customer Payload" in n.get("name",""))

print(f"  Notion Query pos: {notion_query.get('position')}")
print(f"  IF Row Exists pos: {if_node.get('position')}")
print(f"  Prepare Payload pos: {prepare_node.get('position')}")

# Position Stamp node between Notion Query and IF Row Exists
nq_pos = notion_query["position"]
if_pos = if_node["position"]
stamp_pos = [
    int((nq_pos[0] + if_pos[0]) / 2),
    nq_pos[1]
]
print(f"  Stamp node will go at: {stamp_pos}")


# ── Step 3: Build the Stamp Customer Fields node ──────────────────────────────
step("3 · Build Stamp Customer Fields node")

STAMP_CODE = """\
// Merge Stripe customer fields into the Notion query response item.
// Runs once per item (runOnceForEachItem) — pairedItem chain stays intact.
// $('Transform Active Subs').item.json has the Stripe customer fields.
const stripeData = $('Transform Active Subs').item.json;
return { json: { ...$input.item.json, ...stripeData } };
"""

stamp_node = {
    "id":           str(uuid.uuid4()),
    "name":         "Stamp Customer Fields",
    "type":         "n8n-nodes-base.code",
    "typeVersion":  2,
    "position":     stamp_pos,
    "parameters": {
        "mode":    "runOnceForEachItem",
        "jsCode":  STAMP_CODE,
    },
}
print(f"  Stamp node id: {stamp_node['id']}")
print(f"  Code: merges Transform Active Subs fields into each Notion response item")


# ── Step 4: Simplify Prepare New Customer Payload (pure passthrough) ──────────
step("4 · Simplify Prepare New Customer Payload → passthrough")

PREPARE_CODE = """\
// Passthrough — Stripe fields are already on $json (stamped before IF).
// Create New Row reads $json.stripe_customer_id, $json.customer_name, etc.
return $input.all().map(item => ({ json: item.json }));
"""
prepare_node["parameters"]["jsCode"] = PREPARE_CODE
prepare_node["parameters"]["mode"]   = "runOnceForAllItems"
print(f"  Prepare simplified to passthrough")


# ── Step 5: Update connections ─────────────────────────────────────────────────
step("5 · Rewire connections")

conns = wf["connections"]

# Current: Notion Query → IF Row Exists (main[0])
# New:     Notion Query → Stamp → IF Row Exists

# 1. Change Notion Query's output to point to Stamp instead of IF
nq_name = notion_query["name"]
if nq_name in conns:
    for output_port in conns[nq_name].get("main", []):
        for i, link in enumerate(output_port):
            if link.get("node") == "IF - Row Exists":
                output_port[i]["node"] = stamp_node["name"]
                print(f"  Rewired: {nq_name} → {stamp_node['name']}")

# 2. Add Stamp → IF Row Exists connection
conns[stamp_node["name"]] = {
    "main": [
        [{"node": "IF - Row Exists", "type": "main", "index": 0}]
    ]
}
print(f"  Added: {stamp_node['name']} → IF - Row Exists")


# ── Step 6: Add Stamp node to the workflow ────────────────────────────────────
step("6 · Add Stamp node to workflow nodes list")
wf["nodes"].append(stamp_node)
print(f"  Total nodes now: {len(wf['nodes'])}")


# ── Step 7: Deactivate → PUT → Activate ───────────────────────────────────────
step("7 · Deactivate workflow")
r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate", headers=headers)
check(r, "Deactivate")

step("8 · PUT updated workflow")
put_payload = {
    "name":        wf["name"],
    "nodes":       wf["nodes"],
    "connections": wf["connections"],
    "settings":    wf.get("settings", {}),
}
r = requests.put(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=headers, json=put_payload)
check(r, "PUT workflow")

step("9 · Reactivate workflow")
r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate", headers=headers)
check(r, "Activate")


print(f"\n{sep}")
print("  DONE — Stamp Customer Fields node inserted")
print(sep)
print()
print("  Architecture now:")
print("  Transform Active Subs")
print("    → Notion Query by Stripe ID")
print("    → Stamp Customer Fields  (NEW — merges Stripe fields into each item)")
print("    → IF - Row Exists")
print("      true  → Update Existing Row  (unchanged — pairedItem chain preserved)")
print("      false → Prepare New Customer Payload (passthrough)")
print("               → Create New Row  (reads $json.stripe_customer_id etc.)")
print()
print("  Backup: ", BACKUP_PATH)
print("  → Run workflow from n8n UI to verify")
print()
