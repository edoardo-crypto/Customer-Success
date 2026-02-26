#!/usr/bin/env python3
"""
Fix the AND/OR bug in the Intercom → Classify → Notion Issues filter.

The Filter: Not an Issue node uses the old field name `combineOperation: "all"`
which n8n v2 silently ignores, causing it to fall back to OR logic.
The fix is to replace it with `combinator: "and"` (the field n8n v2 actually reads)
and remove the duplicate outer `combineOperation` parameter.

Workflow: 3AO3SRUK80rcOCgQ
Node:     filter-not-issue-001  (Filter: Not an Issue)
"""

import json
import urllib.request

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = "***REMOVED***"
WORKFLOW_ID = "3AO3SRUK80rcOCgQ"


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


def api_post(path, data=None):
    url = f"{N8N_BASE}{path}"
    body = json.dumps(data).encode() if data else b"{}"
    req = urllib.request.Request(
        url, data=body, method="POST",
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

    fixed = False
    for node in wf["nodes"]:
        if node["id"] == "filter-not-issue-001":
            params = node["parameters"]
            conditions_block = params["conditions"]

            # Show current state
            old_inner = conditions_block.get("combineOperation", "(not set)")
            old_combinator = conditions_block.get("combinator", "(not set)")
            outer = params.get("combineOperation", "(not set)")
            print(f"\n  Current inner combineOperation: {old_inner}")
            print(f"  Current inner combinator: {old_combinator}")
            print(f"  Current outer combineOperation: {outer}")
            print(f"  Conditions count: {len(conditions_block.get('conditions', []))}")

            # Fix 1: Replace combineOperation with combinator inside conditions block
            if "combineOperation" in conditions_block:
                del conditions_block["combineOperation"]
            conditions_block["combinator"] = "and"

            # Fix 2: Remove duplicate outer combineOperation
            if "combineOperation" in params:
                del params["combineOperation"]

            print(f"\n  After fix:")
            print(f"    inner combinator: {conditions_block.get('combinator')}")
            print(f"    outer combineOperation: {params.get('combineOperation', '(removed)')}")
            fixed = True
            break

    if not fixed:
        print("\nERROR: filter-not-issue-001 node not found!")
        return

    # Push the updated workflow back
    print(f"\nPushing updated workflow...")
    payload = {
        "name": wf["name"],
        "nodes": wf["nodes"],
        "connections": wf["connections"],
        "settings": wf.get("settings", {}),
    }
    result = api_put(f"/api/v1/workflows/{WORKFLOW_ID}", payload)
    print(f"  PUT result — name: {result.get('name')}, active: {result.get('active')}, nodes: {len(result.get('nodes', []))}")

    # Re-activate if it was active
    if wf.get("active"):
        print("\nRe-activating workflow...")
        act = api_post(f"/api/v1/workflows/{WORKFLOW_ID}/activate")
        print(f"  Activation result — active: {act.get('active')}")
    else:
        print("\nWorkflow was inactive — skipping re-activation.")

    print("\nDone! The filter will now use AND logic: all 5 conditions must pass.")
    print("Only conversations with a valid Issue Type, Severity, and Description will reach Notion.")


if __name__ == "__main__":
    main()
