#!/usr/bin/env python3
"""
fix_last_contact_sync_field.py
-------------------------------
Updates the n8n "📞 Daily Last Contact Date Sync" workflow (veEIgePuCQ0z9jYr)
to write "📅 Last Meeting Date 🔒" instead of "📞 Last Contact Date 🔒"
in its Build Patch List node.

Background:
  The workflow originally tracked "combined last contact" (GCal + Intercom).
  Now 📅 Last Meeting Date 🔒 is the GCal-only field, and 📞 Last Contact Date 🔒
  is reserved for the combined signal computed by sync_contact_reasons.py.

What this script does:
  1. Fetches the workflow JSON from n8n
  2. Finds "Build Patch List" — the node that builds the Notion PATCH body
  3. Replaces the property key in the patchBody JS from
       📞 Last Contact Date 🔒  →  📅 Last Meeting Date 🔒
  4. PUTs the modified workflow back
  5. Reactivates it

IMPORTANT: After running, toggle the workflow INACTIVE → ACTIVE in the n8n UI
to re-register the schedule trigger.
"""

import time
import requests
import creds

# ── Constants ──────────────────────────────────────────────────────────────────
N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "veEIgePuCQ0z9jYr"
NODE_NAME   = "Build Patch List"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

# Old property (📞 = \uD83D\uDCDE, 🔒 = \uD83D\uDD12)
OLD_PROP = "\\uD83D\\uDCDE Last Contact Date \\uD83D\\uDD12"

# New property (📅 = \uD83D\uDCC5, 🔒 = \uD83D\uDD12)
NEW_PROP = "\\uD83D\\uDCC5 Last Meeting Date \\uD83D\\uDD12"


# ── n8n helpers ────────────────────────────────────────────────────────────────

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
    r = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate",
        headers=N8N_HEADERS,
    )
    print(f"  Deactivate: HTTP {r.status_code}")


def activate():
    r = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate",
        headers=N8N_HEADERS,
    )
    print(f"  Activate: HTTP {r.status_code}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("fix_last_contact_sync_field.py")
    print(f"Workflow: {WORKFLOW_ID}")
    print("=" * 65)

    # 1. Fetch workflow
    print("\n[1/4] Fetching workflow...")
    wf    = get_workflow()
    nodes = wf["nodes"]
    print(f"  Nodes: {len(nodes)}, active: {wf.get('active')}")

    # 2. Find target node
    target = next((n for n in nodes if n["name"] == NODE_NAME), None)
    if not target:
        print(f"\n  ERROR: Node '{NODE_NAME}' not found.")
        print("  Available nodes:", [n["name"] for n in nodes])
        return

    js_code = target["parameters"].get("jsCode", "")
    print(f"\n[2/4] Node '{NODE_NAME}' found.")
    print(f"  Contains OLD_PROP: {OLD_PROP in js_code}")
    print(f"  Contains NEW_PROP: {NEW_PROP in js_code}")

    if NEW_PROP in js_code and OLD_PROP not in js_code:
        print("  Already updated — nothing to do.")
        return

    if OLD_PROP not in js_code:
        print(f"\n  WARNING: Expected string '{OLD_PROP}' not found in jsCode.")
        print("  The workflow may have a different structure. Aborting.")
        # Print first 400 chars of jsCode for inspection
        print(f"  jsCode preview:\n{js_code[:400]}")
        return

    # 3. Deactivate + patch + PUT
    print("\n[3/4] Deactivating and applying fix...")
    deactivate()
    time.sleep(1)

    new_js = js_code.replace(OLD_PROP, NEW_PROP)
    target["parameters"]["jsCode"] = new_js

    result    = put_workflow(wf)
    result_nodes = result.get("nodes", [])
    print(f"  PUT OK — {len(result_nodes)} nodes confirmed")

    # Verify
    updated_node = next((n for n in result_nodes if n["name"] == NODE_NAME), None)
    if updated_node:
        stored = updated_node["parameters"].get("jsCode", "")
        print(f"  NEW_PROP in stored code: {NEW_PROP in stored}")
        print(f"  OLD_PROP still present:  {OLD_PROP in stored}")
    else:
        print("  WARNING: could not find node in PUT response to verify")

    # 4. Reactivate
    print("\n[4/4] Reactivating...")
    time.sleep(1)
    activate()

    print()
    print("=" * 65)
    print("Fix applied!")
    print("=" * 65)
    print()
    print("What changed:")
    print(f"  Node '{NODE_NAME}'")
    print(f"  patchBody property key:")
    print(f"    FROM: {OLD_PROP}")
    print(f"      TO: {NEW_PROP}")
    print()
    print("IMPORTANT: Toggle INACTIVE → ACTIVE in n8n UI to re-register schedule:")
    print(f"  -> {N8N_BASE}/workflow/{WORKFLOW_ID}")
    print()


if __name__ == "__main__":
    main()
