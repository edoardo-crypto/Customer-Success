#!/usr/bin/env python3
"""
fix_intercom_catchup_json_body.py — Fix invalid syntax in Intercom: Search Recent Closed

Root cause: the jsonBody expression contains '}}' inside the n8n template {{ ... }}.
n8n's tmpl parser closes the template at the FIRST '}}' it finds — even inside
object literals — causing ExpressionExtensionError: invalid syntax before any node runs.

Offender: ...,"pagination": {"per_page": 150}}) }}
                                              ^^
These two braces close the pagination object + root JSON object inside JSON.stringify(),
but n8n sees '}}' and closes the template early.

Fix: switch from JSON.stringify({...}) to string concatenation.
The two closing braces are emitted as separate single-character string literals:
  '...,{"per_page":150}' + '}' + '}'
so '}}' never appears inside the template source.
"""

import time
import requests
import creds

N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "J1l8oI22H26f9iM5"
NODE_NAME   = "Intercom: Search Recent Closed"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

# The fixed n8n expression.
#
# Result JSON (what Intercom receives):
#   {"query":{"operator":"AND","value":[
#     {"field":"state","operator":"=","value":"closed"},
#     {"field":"updated_at","operator":">","value":<unix_1h_ago>}
#   ]},"sort":{"field":"updated_at","order":"descending"},"pagination":{"per_page":150}}
#
# Key: the final '}}' in that JSON is produced by  '}' + '}'  — two separate
# single-char strings — so the token '}}' never appears inside the template source.
FIXED_JSON_BODY = (
    "={{ "
    """'{"query":{"operator":"AND","value":[{"field":"state","operator":"=","value":"closed"},"""
    """{"field":"updated_at","operator":">","value":'"""
    " + (Math.floor(Date.now()/1000) - 3600) + "
    """'}]},"sort":{"field":"updated_at","order":"descending"},"pagination":{"per_page":150}'"""
    " + '}' + '}'"
    " }}"
)


def get_workflow():
    r = requests.get(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=N8N_HEADERS)
    r.raise_for_status()
    return r.json()


def put_workflow(wf):
    payload = {
        "name":        wf["name"],
        "nodes":       wf["nodes"],
        "connections": wf["connections"],
        "settings":    wf.get("settings", {}),
    }
    r = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}",
        headers=N8N_HEADERS,
        json=payload,
    )
    if r.status_code not in (200, 201):
        print(f"  PUT failed {r.status_code}: {r.text[:500]}")
        r.raise_for_status()
    return r.json()


def deactivate():
    r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate",
                      headers=N8N_HEADERS)
    print(f"  Deactivate: {r.status_code}")


def activate():
    r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate",
                      headers=N8N_HEADERS)
    print(f"  Activate: {r.status_code}")


def main():
    print("=" * 65)
    print("fix_intercom_catchup_json_body.py")
    print(f"Workflow: {WORKFLOW_ID}")
    print("=" * 65)

    # Sanity-check: no '}}' inside the template (between ={{ and the final }})
    inner = FIXED_JSON_BODY[4:-3]   # strip '={{ ' prefix and ' }}' suffix
    if "}}" in inner:
        print(f"  BUG IN SCRIPT: '}}' still appears inside expression!")
        print(f"  inner={inner}")
        return
    print(f"\nFixed expression (verified no '}}' inside template):")
    print(f"  {FIXED_JSON_BODY}")

    # 1. Fetch workflow
    print("\n[1/4] Fetching workflow...")
    wf = get_workflow()
    nodes = wf["nodes"]
    print(f"  Nodes: {len(nodes)}, active: {wf.get('active')}")

    # 2. Find the target node
    target = next((n for n in nodes if n["name"] == NODE_NAME), None)
    if not target:
        print(f"\n  ERROR: Node '{NODE_NAME}' not found.")
        print("  Available nodes:", [n["name"] for n in nodes])
        return

    old_body = target["parameters"].get("jsonBody", "")
    print(f"\n[2/4] Current jsonBody (first 100 chars):\n  {old_body[:100]}")
    print(f"  Contains '}}': {'}}' in old_body}")

    # 3. Deactivate + patch + PUT
    print("\n[3/4] Deactivating and pushing fix...")
    deactivate()
    time.sleep(1)

    target["parameters"]["jsonBody"] = FIXED_JSON_BODY
    result = put_workflow(wf)
    print(f"  PUT OK — {len(result.get('nodes', []))} nodes confirmed")

    # Verify the change was stored
    updated_node = next((n for n in result["nodes"] if n["name"] == NODE_NAME), None)
    if updated_node:
        stored = updated_node["parameters"].get("jsonBody", "")
        print(f"  Stored expression (first 100 chars): {stored[:100]}")
    else:
        print("  WARNING: could not find node in PUT response to verify")

    # 4. Reactivate
    print("\n[4/4] Reactivating...")
    time.sleep(1)
    activate()

    print()
    print("=" * 65)
    print("Fix applied! Workflow should now run on the next 30-min tick.")
    print("=" * 65)
    print()
    print("What changed:")
    print(f"  Node '{NODE_NAME}'")
    print("  jsonBody: replaced JSON.stringify({{...}}) with string building")
    print("  to avoid '}}' inside the n8n template expression.")
    print()
    print("IMPORTANT: Toggle INACTIVE → ACTIVE in the n8n UI to re-register")
    print("the schedule trigger after any PUT.")
    print(f"  -> {N8N_BASE}/workflow/{WORKFLOW_ID}")


if __name__ == "__main__":
    main()
