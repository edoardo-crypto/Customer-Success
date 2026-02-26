#!/usr/bin/env python3
"""
fix_filter_and_dedup.py

Fixes the "Filter: Not an Issue" node in workflow 3AO3SRUK80rcOCgQ:
  - Corrects "No Issue" → "Not an Issue" in the notEquals condition
  - Ensures all 5 conditions are present with correct logic (AND / all)
"""

import requests
import json
import copy

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = "***REMOVED***"
WORKFLOW_ID = "3AO3SRUK80rcOCgQ"
FILTER_NODE_NAME = "Filter: Not an Issue"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type": "application/json",
}

# Desired filter conditions (5 conditions, AND logic)
NEW_CONDITIONS = {
    "options": {},
    "conditions": {
        "options": {
            "caseSensitive": True,
            "leftValue": "",
            "typeValidation": "strict"
        },
        "conditions": [
            {
                "id": "cond-1",
                "leftValue": "={{ $json.issue_type }}",
                "rightValue": "Not an Issue",
                "operator": {
                    "type": "string",
                    "operation": "notEquals",
                    "name": "filter.operator.notEquals"
                }
            },
            {
                "id": "cond-2",
                "leftValue": "={{ $json.issue_type }}",
                "rightValue": "",
                "operator": {
                    "type": "string",
                    "operation": "notEmpty",
                    "name": "filter.operator.notEmpty",
                    "singleValue": True
                }
            },
            {
                "id": "cond-3",
                "leftValue": "={{ $json.cs_severity }}",
                "rightValue": "",
                "operator": {
                    "type": "string",
                    "operation": "notEmpty",
                    "name": "filter.operator.notEmpty",
                    "singleValue": True
                }
            },
            {
                "id": "cond-4",
                "leftValue": "={{ $json.cs_severity }}",
                "rightValue": "Not important",
                "operator": {
                    "type": "string",
                    "operation": "notEquals",
                    "name": "filter.operator.notEquals"
                }
            },
            {
                "id": "cond-5",
                "leftValue": "={{ $json.issue_description }}",
                "rightValue": "",
                "operator": {
                    "type": "string",
                    "operation": "notEmpty",
                    "name": "filter.operator.notEmpty",
                    "singleValue": True
                }
            }
        ],
        "combineOperation": "all"
    },
    "combineOperation": "all"
}


def fetch_workflow():
    url = f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}"
    resp = requests.get(url, headers=N8N_HEADERS)
    resp.raise_for_status()
    return resp.json()


def put_workflow(workflow):
    url = f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}"
    # Only allowed fields in PUT body
    body = {
        "name": workflow["name"],
        "nodes": workflow["nodes"],
        "connections": workflow["connections"],
        "settings": workflow.get("settings", {}),
    }
    resp = requests.put(url, headers=N8N_HEADERS, json=body)
    if resp.status_code not in (200, 204):
        print(f"  ✗ PUT failed: {resp.status_code} {resp.text}")
        resp.raise_for_status()
    return resp.json() if resp.text else {}


def activate_workflow():
    url = f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate"
    resp = requests.post(url, headers=N8N_HEADERS)
    if resp.status_code in (200, 204):
        print("  ✓ Workflow activated")
    else:
        print(f"  ⚠ Activation response: {resp.status_code} {resp.text}")


def find_filter_node(nodes):
    for i, node in enumerate(nodes):
        if node.get("name") == FILTER_NODE_NAME:
            return i, node
    return None, None


def print_conditions(conditions_obj, label):
    """Print current conditions for inspection."""
    conds = conditions_obj.get("conditions", {})
    if isinstance(conds, dict):
        items = conds.get("conditions", [])
        combine = conds.get("combineOperation", "?")
    else:
        items = conds
        combine = conditions_obj.get("combineOperation", "?")

    print(f"  [{label}] combineOperation={combine}, conditions={len(items)}:")
    for c in items:
        op = c.get("operator", {}).get("operation", "?")
        left = c.get("leftValue", "")
        right = c.get("rightValue", "")
        print(f"    {left} [{op}] {right!r}")


def main():
    print("=== Step 1: Fetch Workflow ===")
    workflow = fetch_workflow()
    print(f"  Name: {workflow['name']}")
    print(f"  Active: {workflow.get('active')}")
    print(f"  Nodes: {len(workflow['nodes'])}")

    print(f"\n=== Step 2: Find '{FILTER_NODE_NAME}' Node ===")
    idx, filter_node = find_filter_node(workflow["nodes"])
    if filter_node is None:
        print(f"  ✗ Node '{FILTER_NODE_NAME}' not found! Available nodes:")
        for n in workflow["nodes"]:
            print(f"    - {n['name']} ({n['type']})")
        return

    print(f"  ✓ Found at index {idx}")
    print(f"  Type: {filter_node['type']}")

    # Show current conditions
    current_params = filter_node.get("parameters", {})
    print("\n  Current parameters:")
    print_conditions(current_params, "CURRENT")

    print("\n=== Step 3: Apply New Filter Conditions ===")
    updated_workflow = copy.deepcopy(workflow)
    updated_node = updated_workflow["nodes"][idx]

    # Replace the parameters wholesale
    updated_node["parameters"] = NEW_CONDITIONS

    print("  New parameters:")
    print_conditions(NEW_CONDITIONS, "NEW")

    print("\n=== Step 4: PUT Updated Workflow ===")
    result = put_workflow(updated_workflow)
    print(f"  ✓ PUT succeeded — workflow updated")

    # Verify the node was updated
    print("\n=== Step 5: Verify Filter Node ===")
    refreshed = fetch_workflow()
    _, refreshed_node = find_filter_node(refreshed["nodes"])
    if refreshed_node:
        refreshed_params = refreshed_node.get("parameters", {})
        print_conditions(refreshed_params, "AFTER UPDATE")

        # Check the key condition
        conds_obj = refreshed_params.get("conditions", {})
        if isinstance(conds_obj, dict):
            cond_list = conds_obj.get("conditions", [])
        else:
            cond_list = conds_obj

        fixed = any(
            c.get("operator", {}).get("operation") == "notEquals"
            and c.get("rightValue") == "Not an Issue"
            for c in cond_list
        )
        if fixed:
            print("\n  ✓ Condition 'notEquals Not an Issue' is correctly set")
        else:
            print("\n  ⚠ Could not confirm the fix — review conditions above")
    else:
        print("  ✗ Could not re-fetch filter node for verification")

    print("\n=== Step 6: Reactivate Workflow ===")
    was_active = workflow.get("active", False)
    if was_active:
        activate_workflow()
    else:
        print(f"  Workflow was inactive — skipping reactivation")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
