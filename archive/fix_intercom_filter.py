#!/usr/bin/env python3
"""
Fix Intercom → Classify → Notion Issues workflow (3AO3SRUK80rcOCgQ)

Changes:
1. Fix Filter: Not an Issue — remove cond-not-internal, add cs_severity != 'Not important'
2. Rewire FALSE branch of filter → End - Not an Issue (silent, no Slack alert)
3. Delete 15 engineering/Linear dead-code nodes
4. Delete 3 orphaned/alert nodes
5. Clean up all connections for deleted nodes
"""

import json
import urllib.request
import urllib.error
import creds

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "3AO3SRUK80rcOCgQ"

# Node IDs to delete (15 engineering/Linear + 3 orphaned/alert)
DELETE_IDS = {
    # Engineering/Linear dead-code path
    "1b020983-320d-43a8-981d-3939b74e378d",  # IF Engineering Required
    "5d92cb8a-8997-492d-989b-619629d61465",  # End - No Engineering Needed
    "765f594f-7ffd-4ce4-8470-266d35b4f5dc",  # Gather Context
    "fd9b2bb7-41c9-4337-a2dd-340257b055de",  # Notion - Check Duplicates
    "57e7d033-66f5-4f4f-9f33-9c3dff94292f",  # Process Dedup Results
    "fac043fc-26c5-4978-b295-9568cc14c161",  # IF Duplicate Found
    "ee122e29-f938-42d9-acb2-522b80c84342",  # End - Duplicate Skipped
    "63f28214-4d22-4c14-8281-2d16df1d92d6",  # Build Claude Ticket Payload
    "d7219d86-8cf9-4fbf-bc35-24266d455e4b",  # Claude - Generate Ticket
    "274a9e5e-20ca-4b50-ad99-38445e97b643",  # Parse Ticket + Build Linear
    "b3030509-1694-4e77-a532-ba7b895099de",  # Linear - Create Ticket
    "b7578476-fb60-4579-a278-fa3877991895",  # Build Write-back Payloads
    "4240762a-4992-4c63-94f9-330b41b4b1f3",  # Notion - Create Eng Ticket
    "57f23aa0-196c-42fd-9231-673c94cad5a2",  # Build Intercom Note
    "ede79c75-648c-4090-aa27-cd060229f128",  # Intercom - Post Internal Note
    # Orphaned/alert nodes
    "if-missing-class-001",                  # IF: Missing Classification
    "slack-missing-class-001",               # Slack: Missing Classification Alert
    "node-slack-blocked-alert",              # Slack: Blocked Conversation Alert
}


def api_get(path):
    url = f"{N8N_BASE}{path}"
    req = urllib.request.Request(url, headers={"X-N8N-API-KEY": N8N_API_KEY})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def api_put(path, data):
    url = f"{N8N_BASE}{path}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        url, data=body, method="PUT",
        headers={"X-N8N-API-KEY": N8N_API_KEY, "Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def main():
    print("Fetching workflow...")
    wf = api_get(f"/api/v1/workflows/{WORKFLOW_ID}")
    print(f"  Name: {wf['name']}")
    print(f"  Active: {wf['active']}")
    print(f"  Nodes: {len(wf['nodes'])}")

    # Build name→id and id→name maps for deleted nodes
    delete_names = set()
    for node in wf["nodes"]:
        if node["id"] in DELETE_IDS:
            delete_names.add(node["name"])

    print(f"\nNodes to delete ({len(delete_names)}):")
    for name in sorted(delete_names):
        print(f"  - {name}")

    # ── Change 1: Fix filter-not-issue-001 conditions ──────────────────────
    print("\nFixing Filter: Not an Issue conditions...")
    for node in wf["nodes"]:
        if node["id"] == "filter-not-issue-001":
            old_conditions = node["parameters"]["conditions"]["conditions"]
            # Remove cond-not-internal
            new_conditions = [c for c in old_conditions if c["id"] != "cond-not-internal"]
            # Add cs_severity != 'Not important'
            new_conditions.append({
                "id": "cond-severity-not-important",
                "leftValue": "={{ $json.cs_severity }}",
                "rightValue": "Not important",
                "operator": {
                    "type": "string",
                    "operation": "notEquals"
                }
            })
            node["parameters"]["conditions"]["conditions"] = new_conditions
            print(f"  Conditions now: {[c['id'] for c in new_conditions]}")
            break

    # ── Change 2: Rewire Filter FALSE branch → End - Not an Issue ──────────
    print("\nRewiring Filter: Not an Issue FALSE branch...")
    conns = wf["connections"]
    if "Filter: Not an Issue" in conns:
        # main[1] is FALSE branch
        conns["Filter: Not an Issue"]["main"][1] = [
            {"node": "End - Not an Issue", "type": "main", "index": 0}
        ]
        print("  FALSE branch → End - Not an Issue")

    # ── Change 3: Remove IF Engineering Required from Create/Update Issue ──
    print("\nRemoving IF Engineering Required connections from Create/Update Issue nodes...")
    for src_name in ["Notion - Create Issue", "Notion - Update Existing Issue"]:
        if src_name in conns:
            for output_idx, output_list in enumerate(conns[src_name].get("main", [])):
                before = len(output_list)
                conns[src_name]["main"][output_idx] = [
                    conn for conn in output_list
                    if conn["node"] != "IF Engineering Required"
                ]
                after = len(conns[src_name]["main"][output_idx])
                if before != after:
                    print(f"  {src_name}: removed {before - after} connection(s) to IF Engineering Required")

    # ── Change 4: Remove all deleted nodes ────────────────────────────────
    print("\nRemoving deleted nodes from node list...")
    before_count = len(wf["nodes"])
    wf["nodes"] = [n for n in wf["nodes"] if n["id"] not in DELETE_IDS]
    after_count = len(wf["nodes"])
    print(f"  Nodes: {before_count} → {after_count}")

    # ── Change 5: Remove deleted nodes from connections map ───────────────
    print("\nCleaning connections map...")
    # Remove source entries for deleted nodes
    for name in list(conns.keys()):
        if name in delete_names:
            del conns[name]
            print(f"  Removed source: {name}")

    # Remove target references to deleted nodes in all remaining connections
    for src_name, src_conns in conns.items():
        for output_idx, output_list in enumerate(src_conns.get("main", [])):
            before = len(output_list)
            src_conns["main"][output_idx] = [
                conn for conn in output_list
                if conn["node"] not in delete_names
            ]
            after = len(src_conns["main"][output_idx])
            if before != after:
                print(f"  {src_name}[{output_idx}]: removed {before - after} ref(s) to deleted nodes")

    # ── PUT the cleaned workflow ───────────────────────────────────────────
    print(f"\nPushing updated workflow ({len(wf['nodes'])} nodes)...")
    payload = {
        "name": wf["name"],
        "nodes": wf["nodes"],
        "connections": wf["connections"],
        "settings": wf.get("settings", {}),
    }

    result = api_put(f"/api/v1/workflows/{WORKFLOW_ID}", payload)
    print(f"  Result name: {result.get('name')}")
    print(f"  Result active: {result.get('active')}")
    print(f"  Result node count: {len(result.get('nodes', []))}")
    print("\nDone!")


if __name__ == "__main__":
    main()
