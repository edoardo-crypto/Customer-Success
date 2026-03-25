#!/usr/bin/env python3
"""Two targeted fixes for workflow xdVkUh6YCtcuW8QM (Linear Issue State → Notion Status Sync).

Fix 1 — 'Is Bug?' condition (Bug 1)
  OLD: checks results[0].properties['Issue Type']?.select?.name === "Bug"
       (fails silently when ENG-400 has multiple Notion rows and row[0] isn't a Bug)
  NEW: $('Search Notion Issue').first().json.results.some(r => r.properties['Issue Type']?.select?.name === 'Bug')
       (true if ANY result has Issue Type == Bug)
  IMPORTANT: n8n IF typeVersion 1 boolean conditions require operation="equal" + value2=true
             (not operation="true" which throws "compareOperationFunctions[op] is not a function")

Fix 2 — Guard: Customer ID? (Bug 2)
  Inserts a Code node between 'Is Bug?' (true branch) and 'Update MCT: Bug Fixed'.
  Returns [] (stop) if customerId is null, preventing a silent PATCH to /pages/null.
  (Idempotent: skips node insertion if already present.)
"""

import json
import ssl
import uuid
import urllib.request
import urllib.error
import creds

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "xdVkUh6YCtcuW8QM"

ctx = ssl.create_default_context()


def n8n(method, path, body=None):
    url = f"{N8N_BASE}/api/v1{path}"
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"X-N8N-API-KEY": N8N_API_KEY, "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, context=ctx) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        print(f"  HTTP {e.code}: {body_text}")
        raise


def main():
    # ── Step 1: Fetch current workflow ──────────────────────────────────────
    print(f"Step 1/4 — Fetching workflow {WORKFLOW_ID}...")
    wf = n8n("GET", f"/workflows/{WORKFLOW_ID}")
    print(f"  Fetched: name='{wf['name']}'  nodes={len(wf['nodes'])}  active={wf.get('active')}")

    nodes = wf["nodes"]
    connections = wf["connections"]
    existing_names = {n["name"] for n in nodes}

    # ── Step 2: Fix 'Is Bug?' condition ─────────────────────────────────────
    print("Step 2/4 — Patching 'Is Bug?' condition to use .some()...")
    is_bug_node = next((n for n in nodes if n["name"] == "Is Bug?"), None)
    if not is_bug_node:
        print("  ERROR: 'Is Bug?' node not found")
        raise SystemExit(1)

    # n8n IF typeVersion 1: boolean conditions use operation="equal" + value2=True
    # (operation="true" throws "compareOperationFunctions[op] is not a function")
    is_bug_node["parameters"]["conditions"] = {
        "boolean": [
            {
                "value1": (
                    "={{ $('Search Notion Issue').first().json.results"
                    ".some(r => r.properties['Issue Type']?.select?.name === 'Bug') }}"
                ),
                "operation": "equal",
                "value2": True,
            }
        ]
    }
    print("  Updated 'Is Bug?' to boolean .some() condition with operation=equal, value2=true")

    # ── Step 3: Insert Guard node (if not already present) ───────────────────
    if "Guard: Customer ID?" in existing_names:
        print("Step 3/4 — 'Guard: Customer ID?' already exists, skipping insertion")
    else:
        print("Step 3/4 — Inserting 'Guard: Customer ID?' Code node...")

        is_bug_x, is_bug_y = is_bug_node["position"]

        # Shift Update MCT: Bug Fixed right to make room for guard
        update_mct_node = next((n for n in nodes if n["name"] == "Update MCT: Bug Fixed"), None)
        if not update_mct_node:
            print("  ERROR: 'Update MCT: Bug Fixed' node not found")
            raise SystemExit(1)
        update_mct_node["position"] = [is_bug_x + 500, is_bug_y]

        guard_node = {
            "id": str(uuid.uuid4()),
            "name": "Guard: Customer ID?",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [is_bug_x + 250, is_bug_y],
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": (
                    "const { customerId } = $('Collapse to One').first().json;\n"
                    "if (!customerId) return [];\n"
                    "return [{ json: $('Collapse to One').first().json }];\n"
                ),
            },
        }
        nodes.append(guard_node)
        print(f"  Guard node id={guard_node['id']}  position={guard_node['position']}")

        # Re-wire: Is Bug? [true] → Guard → Update MCT: Bug Fixed
        connections["Is Bug?"] = {
            "main": [
                [{"node": "Guard: Customer ID?", "type": "main", "index": 0}],  # true
                [],  # false → silent stop
            ]
        }
        connections["Guard: Customer ID?"] = {
            "main": [[{"node": "Update MCT: Bug Fixed", "type": "main", "index": 0}]]
        }
        print("  Wired: Is Bug? [true] → Guard: Customer ID? → Update MCT: Bug Fixed")

    print(f"  Workflow has {len(nodes)} nodes")

    # ── Step 4: PUT + activate ────────────────────────────────────────────────
    print("Step 4a/4 — Uploading updated workflow...")
    put_body = {
        "name": wf["name"],
        "nodes": nodes,
        "connections": connections,
        "settings": wf.get("settings", {}),
    }
    result = n8n("PUT", f"/workflows/{WORKFLOW_ID}", put_body)
    print(f"  PUT ok: id={result['id']}  updatedAt={result.get('updatedAt', 'n/a')}")

    print("Step 4b/4 — Re-activating workflow...")
    n8n("POST", f"/workflows/{WORKFLOW_ID}/activate")
    print("  Activated")

    print()
    print("=" * 60)
    print(f"  Workflow: {N8N_BASE}/workflow/{WORKFLOW_ID}")
    print("=" * 60)
    print()
    print("Verification:")
    print("  1. Run test_bug_resolved_webhook.py — happy path must pass")
    print("  2. Execution should show Guard: Customer ID? with 1 item out")
    print("  3. Update MCT: Bug Fixed should show success")


if __name__ == "__main__":
    main()
