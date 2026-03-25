#!/usr/bin/env python3
"""
revert_aten_filter.py
Restores "Filter: Not an Issue" node to 5 conditions in workflow 3AO3SRUK80rcOCgQ.
Then reports last 10 executions.
"""

import json
import requests
import sys
import creds

N8N_BASE = "https://konvoai.app.n8n.cloud"
API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "3AO3SRUK80rcOCgQ"
BACKUP_PATH = "/tmp/workflow_backup_aten_revert.json"

HEADERS = {
    "X-N8N-API-KEY": API_KEY,
    "Content-Type": "application/json",
}


def get_workflow():
    url = f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()


def deactivate_workflow():
    url = f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate"
    r = requests.post(url, headers=HEADERS)
    if r.status_code not in (200, 204):
        print(f"  WARNING: deactivate returned {r.status_code}: {r.text[:200]}")
    else:
        print(f"  Deactivated (status {r.status_code})")


def activate_workflow():
    url = f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate"
    r = requests.post(url, headers=HEADERS)
    if r.status_code not in (200, 204):
        print(f"  WARNING: activate returned {r.status_code}: {r.text[:200]}")
    else:
        print(f"  Activated (status {r.status_code})")


def put_workflow(wf_data):
    url = f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}"
    payload = {
        "name": wf_data["name"],
        "nodes": wf_data["nodes"],
        "connections": wf_data["connections"],
        "settings": wf_data.get("settings", {}),
    }
    r = requests.put(url, headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()


# ── 5-condition definition ──────────────────────────────────────────────────
FIVE_CONDITIONS = [
    {
        "id": "cond-not-nai",
        "leftValue": "={{ $json.issue_type }}",
        "rightValue": "No Issue",
        "operator": {
            "type": "string",
            "operation": "notEquals",
            "name": "filter.operator.notEquals",
        },
    },
    {
        "id": "cond-has-issue-type",
        "leftValue": "={{ $json.issue_type }}",
        "rightValue": "",
        "operator": {
            "type": "string",
            "operation": "notEmpty",
            "name": "filter.operator.notEmpty",
            "singleValue": True,
        },
    },
    {
        "id": "check-severity-752f2d65",
        "leftValue": "={{ $json.cs_severity }}",
        "rightValue": "",
        "operator": {
            "type": "string",
            "operation": "notEmpty",
            "name": "filter.operator.notEmpty",
            "singleValue": True,
        },
    },
    {
        "id": "check-description-5394ebb6",
        "leftValue": "={{ $json.issue_description }}",
        "rightValue": "",
        "operator": {
            "type": "string",
            "operation": "notEmpty",
            "name": "filter.operator.notEmpty",
            "singleValue": True,
        },
    },
    {
        "id": "cond-severity-not-important",
        "leftValue": "={{ $json.cs_severity }}",
        "rightValue": "Not important",
        "operator": {
            "type": "string",
            "operation": "notEquals",
            "name": "filter.operator.notEquals",
        },
    },
]


def find_filter_node(nodes):
    for node in nodes:
        if node.get("name") == "Filter: Not an Issue":
            return node
    return None


def print_current_conditions(node):
    params = node.get("parameters", {})
    conds = params.get("conditions", {})
    existing = conds.get("conditions", [])
    print(f"  Current condition count: {len(existing)}")
    for c in existing:
        op = c.get("operator", {}).get("operation", "?")
        left = c.get("leftValue", "?")
        right = c.get("rightValue", "")
        print(f"    [{c.get('id','?')}] {left} {op} '{right}'")


def apply_five_conditions(node):
    """Overwrite the conditions.conditions array; keep combinator=and."""
    params = node.setdefault("parameters", {})
    conds = params.setdefault("conditions", {})
    conds["conditions"] = FIVE_CONDITIONS
    conds["combinator"] = "and"
    # Keep options if present
    if "options" not in conds:
        conds["options"] = {}


def fetch_executions():
    url = f"{N8N_BASE}/api/v1/executions"
    params = {
        "workflowId": WORKFLOW_ID,
        "limit": 10,
        "includeData": True,
    }
    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code != 200:
        print(f"  WARNING: executions returned {r.status_code}: {r.text[:300]}")
        return []
    data = r.json()
    return data.get("data", [])


def summarise_executions(executions):
    print("\n── Last 10 Executions ──────────────────────────────────────────────────────")
    if not executions:
        print("  (no executions found)")
        return

    for ex in executions:
        ex_id = ex.get("id")
        status = ex.get("status", "?")
        finished = ex.get("stoppedAt") or ex.get("finishedAt") or "?"

        # Try to extract filter branch + field values from execution data
        run_data = ex.get("data", {}) or {}
        result_data = run_data.get("resultData", {}) or {}
        run_by_node = result_data.get("runData", {}) or {}

        # Get values from "Parse AI Summary" node output if available
        issue_type = cs_severity = issue_description = "n/a"
        parse_runs = run_by_node.get("Parse AI Summary", [])
        if parse_runs:
            try:
                items = parse_runs[0].get("data", {}).get("main", [[]])[0]
                if items:
                    jdata = items[0].get("json", {})
                    issue_type = jdata.get("issue_type", "n/a")
                    cs_severity = jdata.get("cs_severity", "n/a")
                    issue_description = str(jdata.get("issue_description", "n/a"))[:60]
            except Exception:
                pass

        # Determine which branch the filter took
        filter_branch = "unknown"
        filter_runs = run_by_node.get("Filter: Not an Issue", [])
        if filter_runs:
            try:
                main_outputs = filter_runs[0].get("data", {}).get("main", [])
                # main[0] = true branch, main[1] = false branch
                true_items = main_outputs[0] if len(main_outputs) > 0 else []
                false_items = main_outputs[1] if len(main_outputs) > 1 else []
                if true_items:
                    filter_branch = "TRUE (logged)"
                elif false_items:
                    filter_branch = "FALSE (dropped)"
                else:
                    filter_branch = "no output"
            except Exception:
                filter_branch = "parse error"

        print(f"\n  Execution {ex_id}")
        print(f"    status:      {status}")
        print(f"    finished:    {finished}")
        print(f"    filter:      {filter_branch}")
        print(f"    issue_type:  {issue_type}")
        print(f"    severity:    {cs_severity}")
        print(f"    description: {issue_description}")


# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=== Step 1: GET workflow ===")
    wf = get_workflow()
    print(f"  Name: {wf['name']}")
    print(f"  Active: {wf.get('active')}")
    print(f"  Nodes: {len(wf['nodes'])}")

    print(f"\n=== Step 2: Backup to {BACKUP_PATH} ===")
    with open(BACKUP_PATH, "w") as f:
        json.dump(wf, f, indent=2)
    print(f"  Saved.")

    print("\n=== Step 3: Find 'Filter: Not an Issue' node ===")
    filter_node = find_filter_node(wf["nodes"])
    if not filter_node:
        print("  ERROR: Node not found — aborting.")
        sys.exit(1)
    print(f"  Found node id={filter_node.get('id')}")
    print_current_conditions(filter_node)

    print("\n=== Step 4: Deactivate workflow ===")
    deactivate_workflow()

    print("\n=== Step 5: Apply 5 conditions ===")
    apply_five_conditions(filter_node)
    print_current_conditions(filter_node)

    print("\n=== Step 6: PUT workflow ===")
    updated = put_workflow(wf)
    print(f"  PUT successful, name={updated.get('name')}")

    # Verify in the returned data
    returned_filter = find_filter_node(updated.get("nodes", []))
    if returned_filter:
        returned_count = len(
            returned_filter.get("parameters", {})
            .get("conditions", {})
            .get("conditions", [])
        )
        print(f"  Returned filter condition count: {returned_count}")
    else:
        print("  WARNING: filter node not found in returned workflow")

    print("\n=== Step 7: Activate workflow ===")
    activate_workflow()

    print("\n=== Step 8: Fetch last 10 executions ===")
    executions = fetch_executions()
    summarise_executions(executions)

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
