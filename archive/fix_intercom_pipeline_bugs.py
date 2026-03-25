#!/usr/bin/env python3
"""
fix_intercom_pipeline_bugs.py — Fix 3 bugs in workflow 3AO3SRUK80rcOCgQ

Bug 2: Remove "Not important" severity filter from Filter: Not an Issue
       → Was silently dropping real issues when CS set Severity = "Not important".
         "Not important" is a valid, intentional low-priority classification.

Bug 3: Add 'New Feature Request' to valid issue types in Build Notion Issue Payload
       → Intercom sends capital-R "New Feature Request" but the node only accepted
         lowercase-r "New feature request", leaving Issue Type blank in Notion.

Bug 4: Fix issue_type = 'No Issue' → 'Not an Issue' in Extract Intercom Data
       → Inconsistency: the filter checks != "Not an Issue" but the extract node
         was setting 'No Issue' for internal senders, creating a mismatch.
         Also updates the filter condition to consistently check "Not an Issue".
"""

import json
import requests
import sys
import creds

# ── Config ───────────────────────────────────────────────────────────────────
N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "3AO3SRUK80rcOCgQ"

# Node identifiers
FILTER_NODE_NAME = "Filter: Not an Issue"
EXTRACT_NODE_ID = "15aba94b-06d4-460f-9989-ad589f7bdee3"
BUILD_PAYLOAD_NODE_ID = "ebaf8800-06f9-458f-92c9-60ae532839ec"

HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type": "application/json",
}

# ── Desired final filter conditions (4 conditions, AND) ──────────────────────
# Removes Bug 2 (cond-severity-not-important) and fixes Bug 4 (No Issue → Not an Issue)
FINAL_FILTER_CONDITIONS = [
    {
        "id": "cond-not-nai",
        "leftValue": "={{ $json.issue_type }}",
        "rightValue": "Not an Issue",  # Bug 4 fix: was "No Issue"
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
    # NOTE: cond-severity-not-important is intentionally excluded (Bug 2 fix)
]


# ── n8n API helpers ──────────────────────────────────────────────────────────

def fetch_workflow():
    r = requests.get(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=HEADERS)
    r.raise_for_status()
    return r.json()


def put_workflow(wf):
    payload = {
        "name": wf["name"],
        "nodes": wf["nodes"],
        "connections": wf["connections"],
        "settings": wf.get("settings", {}),
    }
    r = requests.put(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=HEADERS, json=payload)
    if r.status_code not in (200, 204):
        print(f"  PUT failed: {r.status_code} — {r.text[:500]}")
        r.raise_for_status()
    return r.json() if r.text else {}


def deactivate():
    r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate", headers=HEADERS)
    if r.status_code in (200, 204):
        print("  Deactivated.")
    else:
        print(f"  Deactivate warning: {r.status_code} (continuing)")


def activate():
    r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate", headers=HEADERS)
    if r.status_code in (200, 204):
        print("  Activated.")
    else:
        print(f"  Activate warning: {r.status_code} — {r.text[:200]}")


# ── Bug fixes ────────────────────────────────────────────────────────────────

def fix_filter_node(nodes):
    """Bug 2 + Bug 4: Replace filter conditions — remove severity exclusion,
    fix 'No Issue' → 'Not an Issue' in the notEquals check."""
    for node in nodes:
        if node.get("name") == FILTER_NODE_NAME:
            params = node.setdefault("parameters", {})
            conds = params.setdefault("conditions", {})

            # Show current state
            current = conds.get("conditions", [])
            print(f"  Current conditions ({len(current)}):")
            for c in current:
                op = c.get("operator", {}).get("operation", "?")
                print(f"    [{c.get('id', '?')}] {c.get('leftValue', '')} {op} {c.get('rightValue', '')!r}")

            # Replace with clean 4-condition set
            conds["conditions"] = FINAL_FILTER_CONDITIONS
            conds["combinator"] = "and"
            # Remove legacy field if present
            conds.pop("combineOperation", None)
            params.pop("combineOperation", None)

            print(f"\n  New conditions ({len(FINAL_FILTER_CONDITIONS)}):")
            for c in FINAL_FILTER_CONDITIONS:
                op = c.get("operator", {}).get("operation", "?")
                print(f"    [{c.get('id', '?')}] {c.get('leftValue', '')} {op} {c.get('rightValue', '')!r}")
            return True

    print(f"  ERROR: Node '{FILTER_NODE_NAME}' not found!")
    return False


def fix_extract_node(nodes):
    """Bug 4: Replace 'No Issue' with 'Not an Issue' in Extract Intercom Data."""
    for node in nodes:
        if node.get("id") == EXTRACT_NODE_ID:
            js = node["parameters"].get("jsCode", "")

            # Count occurrences to verify
            count = js.count("'No Issue'")
            if count == 0:
                # Try double-quote variant
                count = js.count('"No Issue"')
                if count == 0:
                    print(f"  WARNING: 'No Issue' not found in Extract node — may already be fixed")
                    return True
                old = '"No Issue"'
                new = '"Not an Issue"'
            else:
                old = "'No Issue'"
                new = "'Not an Issue'"

            js_fixed = js.replace(old, new)
            node["parameters"]["jsCode"] = js_fixed
            print(f"  Replaced {count} occurrence(s) of {old!r} → {new!r}")
            return True

    print(f"  ERROR: Extract node ({EXTRACT_NODE_ID}) not found!")
    return False


def fix_build_payload_node(nodes):
    """Bug 3: Add 'New Feature Request' (capital R) to valid issue types."""
    for node in nodes:
        if node.get("id") == BUILD_PAYLOAD_NODE_ID:
            js = node["parameters"].get("jsCode", "")

            # Check if already fixed
            if "'New Feature Request'" in js or '"New Feature Request"' in js:
                print("  Already contains 'New Feature Request' — skipping (already fixed)")
                return True

            # Find the valid types line and add the capital-R variant
            # The current line is: ['Bug', 'Config Issue', 'New feature request', 'Feature improvement']
            old_types = "'New feature request'"
            new_types = "'New feature request', 'New Feature Request'"

            if old_types in js:
                js_fixed = js.replace(old_types, new_types, 1)
                node["parameters"]["jsCode"] = js_fixed
                print(f"  Added 'New Feature Request' to valid types list")
                return True

            # Try double-quote variant
            old_types_dq = '"New feature request"'
            new_types_dq = '"New feature request", "New Feature Request"'
            if old_types_dq in js:
                js_fixed = js.replace(old_types_dq, new_types_dq, 1)
                node["parameters"]["jsCode"] = js_fixed
                print(f"  Added 'New Feature Request' to valid types list (double-quote variant)")
                return True

            print(f"  WARNING: Could not find 'New feature request' in Build Payload node JS")
            print(f"  Showing JS snippet for manual inspection:")
            # Print lines around issue type handling
            for i, line in enumerate(js.splitlines()):
                if "issue" in line.lower() and "type" in line.lower():
                    print(f"    L{i+1}: {line}")
            return False

    print(f"  ERROR: Build Payload node ({BUILD_PAYLOAD_NODE_ID}) not found!")
    return False


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("fix_intercom_pipeline_bugs.py")
    print("=" * 60)

    # 1. Fetch
    print("\n[1/6] Fetching workflow...")
    wf = fetch_workflow()
    print(f"  Name: {wf['name']}")
    print(f"  Active: {wf.get('active')}")
    print(f"  Nodes: {len(wf['nodes'])}")

    # 2. Backup
    backup_path = "/tmp/intercom_pipeline_backup_bugs.json"
    with open(backup_path, "w") as f:
        json.dump(wf, f, indent=2)
    print(f"  Backed up to {backup_path}")

    # 3. Deactivate
    print("\n[2/6] Deactivating...")
    if wf.get("active"):
        deactivate()
    else:
        print("  Already inactive.")

    # 4. Apply fixes
    print("\n[3/6] Fixing Filter: Not an Issue (Bug 2 + Bug 4 partial)...")
    ok_filter = fix_filter_node(wf["nodes"])

    print("\n[4/6] Fixing Extract Intercom Data (Bug 4)...")
    ok_extract = fix_extract_node(wf["nodes"])

    print("\n[5/6] Fixing Build Notion Issue Payload (Bug 3)...")
    ok_payload = fix_build_payload_node(wf["nodes"])

    if not all([ok_filter, ok_extract, ok_payload]):
        print("\nERROR: One or more fixes failed — aborting PUT")
        sys.exit(1)

    # 5. PUT
    print("\n[5/6] Pushing updated workflow...")
    result = put_workflow(wf)
    print(f"  PUT OK — name={result.get('name')}, nodes={len(result.get('nodes', []))}")

    # Verify filter
    returned_filter = next(
        (n for n in result.get("nodes", []) if n.get("name") == FILTER_NODE_NAME), None
    )
    if returned_filter:
        returned_conds = (
            returned_filter.get("parameters", {})
            .get("conditions", {})
            .get("conditions", [])
        )
        print(f"  Verified: filter has {len(returned_conds)} conditions")
        has_severity_exclusion = any(
            c.get("rightValue") == "Not important" for c in returned_conds
        )
        if has_severity_exclusion:
            print("  WARNING: 'Not important' exclusion still present in filter!")
        else:
            print("  ✓ 'Not important' exclusion successfully removed")

    # 6. Reactivate
    print("\n[6/6] Reactivating...")
    activate()

    # Summary
    print("\n" + "=" * 60)
    print("DONE — Changes applied:")
    print("  Bug 2: Removed cs_severity != 'Not important' filter condition")
    print("         (conversations with Not important severity now pass through)")
    print("  Bug 3: Added 'New Feature Request' to valid issue types")
    print("         (capital-R variant from Intercom now maps correctly)")
    print("  Bug 4: Fixed 'No Issue' → 'Not an Issue' in filter + extract node")
    print("         (consistent labeling for internal/rejected conversations)")
    print("=" * 60)
    print()
    print("Next steps:")
    print("  1. Run backfill_missed_issues.py to log the 5 missed conversations")
    print("  2. Run deploy_intercom_catchup.py to set up the 30-min catch-up poll")
    print("  3. Activate the catch-up workflow in the n8n UI after deployment")


if __name__ == "__main__":
    main()
