#!/usr/bin/env python3
"""
fix_intercom_catchup_domain_prefix.py — Add domain-prefix fallback to company lookup node

Updates the "Notion: Find Customer by Company" node in workflow J1l8oI22H26f9iM5
so that when Intercom's company name is empty, it falls back to searching MCT by
the email domain prefix (e.g. "grippadel.com" → search MCT for "grippadel").

Search term priority in the updated node:
  1. conv.companies[0].name  (if >= 4 chars)       ← already there
  2. email domain prefix      (if non-generic, >= 4) ← NEW
  3. "SKIP_NO_MATCH__zz99"                           ← fallback no-op

This is a single-node jsonBody expression change — no structural changes to the
workflow (no new nodes, no connection changes).
"""

import time
import requests
import sys

# ── Config ────────────────────────────────────────────────────────────────────
N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = (
    "***REMOVED_JWT***"
    ".eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJp"
    "c3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleH"
    "AiOjE3NzMyNzAwMDB9.X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)

WORKFLOW_ID        = "J1l8oI22H26f9iM5"
TARGET_NODE_NAME   = "Notion: Find Customer by Company"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

# New jsonBody expression: try company_name first, then domain prefix, then skip.
# Structure unchanged from before: JSON.stringify({"filter": {"property": ..., "title": {"contains": VALUE}}})
# The same }}}) }} pattern as before is intentional — works on this n8n instance.
NEW_JSON_BODY = (
    '={{ JSON.stringify({'
    '"filter": {'
    '"property": "title",'
    '"title": {"contains": '
    '($json.company_name && $json.company_name.length >= 4) '
    '? $json.company_name '
    ': (($json.email_domain '
    '&& !["gmail.com","yahoo.com","hotmail.com","outlook.com","icloud.com","protonmail.com"].includes($json.email_domain) '
    '&& $json.email_domain.split(".")[0].length >= 4) '
    '? $json.email_domain.split(".")[0] '
    ': "SKIP_NO_MATCH__zz99")'
    '}'
    '}'
    '}) }}'
)


# ── n8n API helpers ───────────────────────────────────────────────────────────

def get_workflow():
    r = requests.get(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}",
        headers=N8N_HEADERS,
    )
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
        print(f"  PUT failed: {r.status_code}")
        print(f"  Response: {r.text[:600]}")
        r.raise_for_status()
    return r.json()


def deactivate():
    r = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate",
        headers=N8N_HEADERS,
    )
    if r.status_code in (200, 204):
        print("  Deactivated")
    else:
        print(f"  Deactivate returned {r.status_code} (may already be inactive)")


def activate():
    r = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate",
        headers=N8N_HEADERS,
    )
    if r.status_code in (200, 204):
        print("  Activated")
    else:
        print(f"  Activate returned {r.status_code}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("fix_intercom_catchup_domain_prefix.py")
    print(f"Workflow: {WORKFLOW_ID}")
    print("=" * 65)

    # 1. Fetch current workflow
    print("\n[1/4] Fetching current workflow...")
    wf = get_workflow()
    print(f"  Name:   {wf['name']}")
    print(f"  Nodes:  {len(wf['nodes'])}")
    print(f"  Active: {wf.get('active')}")

    nodes        = wf["nodes"]
    node_by_name = {n["name"]: n for n in nodes}

    # 2. Safety checks
    print("\n[2/4] Validating node structure...")
    if TARGET_NODE_NAME not in node_by_name:
        print(f"  ERROR: Node '{TARGET_NODE_NAME}' not found.")
        print(f"  Present nodes: {list(node_by_name.keys())}")
        sys.exit(1)
    print(f"  ✓ '{TARGET_NODE_NAME}' found")

    # Guard: check if already patched (domain prefix expression already present)
    target_node  = node_by_name[TARGET_NODE_NAME]
    current_body = target_node.get("parameters", {}).get("jsonBody", "")
    if "email_domain.split" in current_body:
        print("\n  Already patched — 'email_domain.split' found in jsonBody.")
        print("  Aborting to avoid double-patch.")
        sys.exit(0)

    print(f"\n  Current jsonBody (first 120 chars): {current_body[:120]}")
    print(f"\n  New jsonBody (first 120 chars):     {NEW_JSON_BODY[:120]}")

    # 3. Deactivate
    print("\n[3/4] Deactivating workflow...")
    deactivate()
    time.sleep(1)

    # 4. Patch the node
    print("\n[4/4] Patching node jsonBody and pushing to n8n...")
    target_node["parameters"]["jsonBody"] = NEW_JSON_BODY
    result    = put_workflow(wf)
    got_nodes = len(result.get("nodes", []))
    print(f"  PUT OK — {got_nodes} nodes confirmed")

    # Re-activate
    print("\nReactivating workflow...")
    time.sleep(1)
    activate()

    print()
    print("=" * 65)
    print("Done! Domain-prefix fallback added to company lookup node.")
    print("=" * 65)
    print()
    print("What changed:")
    print(f"  Node '{TARGET_NODE_NAME}' jsonBody updated.")
    print("  Search term priority:")
    print("    1. conv.companies[0].name  (if >= 4 chars)  ← was already there")
    print("    2. email domain prefix      (if non-generic, >= 4 chars)  ← NEW")
    print("    3. 'SKIP_NO_MATCH__zz99'                    ← fallback no-op")
    print()
    print("IMPORTANT: Toggle INACTIVE → ACTIVE in the n8n UI to re-register")
    print("the schedule trigger after any PUT.")
    print(f"  → {N8N_BASE}/workflow/{WORKFLOW_ID}")


if __name__ == "__main__":
    main()
