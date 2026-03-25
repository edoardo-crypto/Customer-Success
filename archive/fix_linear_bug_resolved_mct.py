#!/usr/bin/env python3
"""Update MCT 'Reason for contact' when a Bug-type Linear issue is resolved.

Appends 2 new nodes after "Send Slack Alert" in workflow xdVkUh6YCtcuW8QM:
  1. Is Bug?               (IF)           — passes only when Issue Type == 'Bug'
  2. Update MCT: Bug Fixed (HTTP Request) — PATCHes MCT page to set
                                            '💎 Reason for contact' = 'Bug fixed! 🎉 - check Issues DB'

No existing nodes are modified.
"""

import json
import ssl
import urllib.request
import urllib.error
import creds

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "xdVkUh6YCtcuW8QM"
NOTION_TOKEN = creds.get("NOTION_TOKEN")

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


# Static JSON body for the Notion PATCH — plain string, not an n8n expression.
# emoji: 💎 = \U0001f48e, 🎉 = \U0001f389
MCT_PATCH_BODY = json.dumps(
    {
        "properties": {
            "\U0001f48e Reason for contact": {
                "select": {"name": "Bug fixed! \U0001f389 - check Issues DB"}
            }
        }
    },
    ensure_ascii=False,
)

# ---------------------------------------------------------------------------
# New nodes — positions overwritten dynamically below
# ---------------------------------------------------------------------------
NEW_NODES = [
    # ── Node A: IF — Is Bug? ───────────────────────────────────────────────
    {
        "id": "nA-is-bug",
        "name": "Is Bug?",
        "type": "n8n-nodes-base.if",
        "typeVersion": 1,
        "position": [0, 0],
        "parameters": {
            "conditions": {
                "string": [
                    {
                        # optional-chaining guards against issues with no type set
                        "value1": "={{ $('Search Notion Issue').first().json.results[0].properties['Issue Type']?.select?.name }}",
                        "operation": "equal",
                        "value2": "Bug",
                    }
                ]
            }
        },
    },
    # ── Node B: HTTP Request — Update MCT: Bug Fixed ───────────────────────
    # continueOnFail: true so a null customerId is a silent skip, not a crash.
    {
        "id": "nB-update-mct-bug-fixed",
        "name": "Update MCT: Bug Fixed",
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4,
        "position": [0, 0],
        "continueOnFail": True,
        "parameters": {
            "method": "PATCH",
            "url": "={{ 'https://api.notion.com/v1/pages/' + $('Collapse to One').first().json.customerId }}",
            "sendHeaders": True,
            "headerParameters": {
                "parameters": [
                    {"name": "Authorization", "value": f"Bearer {NOTION_TOKEN}"},
                    # MCT is a multi-source database — must use 2025-09-03
                    {"name": "Notion-Version", "value": "2025-09-03"},
                ]
            },
            "sendBody": True,
            "contentType": "raw",
            "rawContentType": "application/json",
            "body": MCT_PATCH_BODY,
        },
    },
]

# Connections to add.
# "Send Slack Alert" is currently a terminal node, so we just create its outgoing entry.
NEW_CONNECTIONS = {
    "Send Slack Alert": {
        "main": [[{"node": "Is Bug?", "type": "main", "index": 0}]]
    },
    "Is Bug?": {
        "main": [
            [{"node": "Update MCT: Bug Fixed", "type": "main", "index": 0}],  # true
            [],  # false → silent stop (non-Bug issues do nothing)
        ]
    },
}


def main():
    # ── Step 1: Fetch current workflow ──────────────────────────────────────
    print(f"Step 1/4 — Fetching workflow {WORKFLOW_ID}...")
    wf = n8n("GET", f"/workflows/{WORKFLOW_ID}")
    print(f"  + Fetched  name='{wf['name']}'  nodes={len(wf['nodes'])}  active={wf.get('active')}")

    nodes = wf["nodes"]
    connections = wf["connections"]

    # Idempotency guard — abort if our nodes already exist
    existing_names = {node["name"] for node in nodes}
    for new_node in NEW_NODES:
        if new_node["name"] in existing_names:
            print(f"  ! Node '{new_node['name']}' already exists — workflow already patched. Exiting.")
            return

    # ── Step 2: Resolve positions relative to "Send Slack Alert" ────────────
    slack_node = next((n for n in nodes if n["name"] == "Send Slack Alert"), None)
    if not slack_node:
        print("  ERROR: 'Send Slack Alert' node not found in workflow")
        raise SystemExit(1)

    base_x, base_y = slack_node["position"]
    print(f"  'Send Slack Alert' at position [{base_x}, {base_y}]")

    NEW_NODES[0]["position"] = [base_x + 250, base_y]   # Is Bug?
    NEW_NODES[1]["position"] = [base_x + 500, base_y]   # Update MCT: Bug Fixed

    # ── Step 3: Merge nodes + connections ───────────────────────────────────
    print("Step 2/4 — Appending 2 new nodes and their connections...")
    nodes.extend(NEW_NODES)
    connections.update(NEW_CONNECTIONS)
    print(f"  + Workflow now has {len(nodes)} nodes")

    # ── Step 4: PUT updated workflow ─────────────────────────────────────────
    print("Step 3/4 — Uploading updated workflow...")
    put_body = {
        "name": wf["name"],
        "nodes": nodes,
        "connections": connections,
        "settings": wf.get("settings", {}),
    }
    result = n8n("PUT", f"/workflows/{WORKFLOW_ID}", put_body)
    print(f"  + Updated  id={result['id']}  updatedAt={result.get('updatedAt', 'n/a')}")

    # ── Step 5: Re-activate ───────────────────────────────────────────────────
    print("Step 4/4 — Re-activating workflow...")
    n8n("POST", f"/workflows/{WORKFLOW_ID}/activate")
    print("  + Activated")

    print()
    print("=" * 60)
    print(f"  Workflow : {N8N_BASE}/workflow/{WORKFLOW_ID}")
    print("=" * 60)
    print()
    print("Verification steps:")
    print("  1. In Linear, resolve a Bug-type issue linked to a customer")
    print("     → MCT '\U0001f48e Reason for contact' should become 'Bug fixed! \U0001f389 - check Issues DB'")
    print("  2. Resolve a non-Bug issue → MCT column should NOT change")
    print("  3. Bug with no Customer relation → continueOnFail silently skips")


if __name__ == "__main__":
    main()
