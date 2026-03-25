#!/usr/bin/env python3
"""
fix_customers_contacted_resilience.py

Inserts a guard Code node between "Find Scorecard Row" and "Update Scorecard Row"
in the "📞 Weekly Customers Contacted Tracker" workflow.

The guard node:
  - If no scorecard row was found (results empty) → returns [] → workflow stops cleanly
  - If found → extracts scorecard_page_id → "Update Scorecard Row" uses $json.scorecard_page_id

Also updates the "Update Scorecard Row" URL expression from:
  {{ 'https://api.notion.com/v1/pages/' + $('Find Scorecard Row').first().json.results[0].id }}
to:
  {{ 'https://api.notion.com/v1/pages/' + $json.scorecard_page_id }}
"""

import json
import uuid
import requests
import sys
import creds

# ── Constants ──────────────────────────────────────────────────────────────────
N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

WORKFLOW_NAME = "\U0001F4DE Weekly Customers Contacted Tracker"  # 📞

FIND_NODE_NAME   = "Find Scorecard Row"
GUARD_NODE_NAME  = "Guard: Scorecard Row Found?"
UPDATE_NODE_NAME = "Update Scorecard Row"

# Guard Code node JS
GUARD_SCORECARD_ROW_JS = """\
const results = $input.first().json.results || [];
if (results.length === 0) {
    console.warn('[customers-contacted] No scorecard row found for this week — skipping update.');
    return [];  // stops cleanly; Update Scorecard Row does not execute
}
return [{ json: { scorecard_page_id: results[0].id } }];
"""


def uid():
    return str(uuid.uuid4())


# ── n8n API helpers ────────────────────────────────────────────────────────────

def list_workflows():
    r = requests.get(f"{N8N_BASE}/api/v1/workflows", headers=N8N_HEADERS)
    r.raise_for_status()
    return r.json().get("data", [])


def get_workflow(wf_id):
    r = requests.get(f"{N8N_BASE}/api/v1/workflows/{wf_id}", headers=N8N_HEADERS)
    r.raise_for_status()
    return r.json()


def put_workflow(wf_id, wf_body):
    """PUT only accepts: name, nodes, connections, settings."""
    payload = {
        "name":        wf_body["name"],
        "nodes":       wf_body["nodes"],
        "connections": wf_body["connections"],
        "settings":    wf_body.get("settings", {}),
    }
    r = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{wf_id}",
        headers=N8N_HEADERS,
        json=payload,
    )
    if r.status_code not in (200, 201):
        print(f"  PUT failed: {r.status_code} — {r.text[:800]}")
        r.raise_for_status()
    return r.json()


# ── Core fix logic ─────────────────────────────────────────────────────────────

def apply_fix(wf):
    nodes       = wf["nodes"]
    connections = wf["connections"]

    # ── Locate relevant nodes ──────────────────────────────────────────────────
    find_node   = next((n for n in nodes if n["name"] == FIND_NODE_NAME),   None)
    update_node = next((n for n in nodes if n["name"] == UPDATE_NODE_NAME), None)

    if find_node is None:
        print(f"  ERROR: Node '{FIND_NODE_NAME}' not found.")
        sys.exit(1)
    if update_node is None:
        print(f"  ERROR: Node '{UPDATE_NODE_NAME}' not found.")
        sys.exit(1)

    # Check guard not already present
    if any(n["name"] == GUARD_NODE_NAME for n in nodes):
        print(f"  Guard node '{GUARD_NODE_NAME}' already exists — nothing to do.")
        sys.exit(0)

    print(f"  Found '{FIND_NODE_NAME}'  at position {find_node['position']}")
    print(f"  Found '{UPDATE_NODE_NAME}' at position {update_node['position']}")

    # ── Build guard node ───────────────────────────────────────────────────────
    # Position: halfway between Find and Update (or Find + 280 if they're far apart)
    find_x, find_y = find_node["position"]
    guard_x = find_x + 280
    guard_y = find_y

    guard_node = {
        "id":          uid(),
        "name":        GUARD_NODE_NAME,
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [guard_x, guard_y],
        "parameters": {
            "mode":   "runOnceForAllItems",
            "jsCode": GUARD_SCORECARD_ROW_JS,
        },
    }

    # ── Shift Update node rightward to make room ───────────────────────────────
    # Only shift if guard_x would overlap with update_node
    update_x, update_y = update_node["position"]
    if guard_x >= update_x:
        update_node["position"] = [guard_x + 280, update_y]
        print(f"  Shifted '{UPDATE_NODE_NAME}' to position {update_node['position']}")

    # ── Insert guard node into nodes list (after Find node) ───────────────────
    find_idx = next(i for i, n in enumerate(nodes) if n["name"] == FIND_NODE_NAME)
    nodes.insert(find_idx + 1, guard_node)
    print(f"  Inserted '{GUARD_NODE_NAME}' at position {guard_node['position']}")

    # ── Update "Update Scorecard Row" URL expression ───────────────────────────
    old_url = update_node["parameters"].get("url", "")
    new_url = "={{ 'https://api.notion.com/v1/pages/' + $json.scorecard_page_id }}"
    update_node["parameters"]["url"] = new_url
    print(f"  Updated '{UPDATE_NODE_NAME}' URL:")
    print(f"    old: {old_url}")
    print(f"    new: {new_url}")

    # ── Rewire connections ─────────────────────────────────────────────────────
    # Before: Find → Update
    # After:  Find → Guard → Update

    # 1. Find Scorecard Row → Guard (replaces Find → Update)
    connections[FIND_NODE_NAME] = {
        "main": [[{"node": GUARD_NODE_NAME, "type": "main", "index": 0}]]
    }

    # 2. Guard → Update
    connections[GUARD_NODE_NAME] = {
        "main": [[{"node": UPDATE_NODE_NAME, "type": "main", "index": 0}]]
    }

    print(f"  Rewired connections: {FIND_NODE_NAME} → {GUARD_NODE_NAME} → {UPDATE_NODE_NAME}")

    wf["nodes"]       = nodes
    wf["connections"] = connections
    return wf


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("fix_customers_contacted_resilience.py")
    print("=" * 65)

    # Step 1: Find the workflow
    print(f"\n[1/4] Looking for workflow '{WORKFLOW_NAME}' …")
    all_wfs = list_workflows()
    matches = [w for w in all_wfs if w.get("name") == WORKFLOW_NAME]

    if not matches:
        print(f"  ERROR: No workflow named '{WORKFLOW_NAME}' found.")
        print("  Available workflows:")
        for w in all_wfs:
            print(f"    ID={w['id']}  name={w['name']!r}")
        sys.exit(1)

    if len(matches) > 1:
        print(f"  WARNING: {len(matches)} workflows with this name — using most recent")
        # Sort by id (they're roughly chronological) — take last
        matches.sort(key=lambda w: w.get("id", ""))

    wf_meta = matches[-1]
    wf_id   = wf_meta["id"]
    print(f"  Found workflow ID={wf_id}  active={wf_meta.get('active')}")

    # Step 2: Fetch full workflow definition
    print(f"\n[2/4] Fetching full workflow definition for {wf_id} …")
    wf = get_workflow(wf_id)
    print(f"  Current nodes ({len(wf['nodes'])}):")
    for i, n in enumerate(wf["nodes"], 1):
        print(f"    {i:2}. {n['name']}")

    # Step 3: Apply fix
    print(f"\n[3/4] Applying fix …")
    wf = apply_fix(wf)
    print(f"  Updated nodes ({len(wf['nodes'])}):")
    for i, n in enumerate(wf["nodes"], 1):
        print(f"    {i:2}. {n['name']}")

    # Save for inspection
    save_path = "/tmp/customers_contacted_resilience_fix.json"
    with open(save_path, "w") as f:
        json.dump(wf, f, indent=2)
    print(f"  Saved to {save_path}")

    # Step 4: Push to n8n
    print(f"\n[4/4] Pushing updated workflow to n8n …")
    result = put_workflow(wf_id, wf)
    print(f"  PUT response: active={result.get('active')}")
    print(f"  Node count in response: {len(result.get('nodes', []))}")

    print()
    print("=" * 65)
    print("Fix applied successfully!")
    print("=" * 65)
    print()
    print("What changed:")
    print(f"  • Inserted guard node '{GUARD_NODE_NAME}' between")
    print(f"    '{FIND_NODE_NAME}' and '{UPDATE_NODE_NAME}'")
    print(f"  • Guard returns [] if no scorecard row found → workflow stops cleanly")
    print(f"  • '{UPDATE_NODE_NAME}' URL now reads $json.scorecard_page_id")
    print()
    print("Next steps:")
    print(f"  1. Open {N8N_BASE}/workflow/{wf_id}")
    print(f"  2. Verify 14 nodes are visible (was 13)")
    print(f"  3. Activate '📊 Weekly Scorecard Builder' (ID: eUwMYFeglyv9bHxn)")
    print(f"     and run it manually to create the W09 row")
    print(f"  4. Then run this workflow manually → it will find the W09 row")
    print(f"     and update the Customers Contacted counts")


if __name__ == "__main__":
    main()
