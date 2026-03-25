#!/usr/bin/env python3
"""Extend Linear→Notion sync workflow with a Slack resolved alert.

Appends 3 new nodes after "Update Notion Page" in workflow ce8BpceG04fjgOCz:
  1. Is Resolved?      (IF)           — passes only when notionStatus == 'Resolved'
  2. Get Customer Name (HTTP Request) — fetches the Customer page from Notion
  3. Send Slack Alert  (HTTP Request) — posts to #customer-success-core (C0AGZDTUND6)
"""

import json
import ssl
import urllib.request
import urllib.error
import creds

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "ce8BpceG04fjgOCz"
NOTION_TOKEN = creds.get("NOTION_TOKEN")
SLACK_CRED_ID = "IMuEGtYutmUKwCqY"
SLACK_CRED_NAME = "Slack Bot for Alerts"
SLACK_CHANNEL = "C0AGZDTUND6"

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


# ---------------------------------------------------------------------------
# Slack body expression (JavaScript evaluated by n8n at runtime)
# Apostrophes escaped as \' inside JS single-quoted strings.
# Customer name and Issue Title pulled from earlier nodes via $('NodeName').
# continueOnFail on Get Customer Name means: if Customer relation is empty,
# that node errors but the workflow continues; optional-chaining + || fallback
# handles the undefined gracefully in the Slack text.
# ---------------------------------------------------------------------------
SLACK_BODY = (
    "={{ JSON.stringify({ "
    "channel: 'C0AGZDTUND6', "
    "text: '\\uD83C\\uDF89 *' + "
    "(($('Get Customer Name').item.json?.properties?.Name?.title?.[0]?.plain_text) || 'A customer') + "
    "'\\'s issue regarding *\\\"' + "
    "(($('Search Notion Issue').item.json?.results?.[0]?.properties?.['Issue Title']?.title?.[0]?.plain_text) || 'an issue') + "
    "'\\\"* has been resolved! Let\\'s contact them right now to let them know! \\uD83D\\uDCDE', "
    "unfurl_links: false "
    "}) }}"
)

NOTION_GET_HEADERS = {
    "parameters": [
        {"name": "Authorization", "value": f"Bearer {NOTION_TOKEN}"},
        {"name": "Notion-Version", "value": "2022-06-28"},
    ]
}

# New nodes to append — positions are set dynamically relative to "Update Notion Page"
NEW_NODES = [
    # ── Node 6: IF — Is Resolved? ──────────────────────────────────────────
    {
        "id": "n6",
        "name": "Is Resolved?",
        "type": "n8n-nodes-base.if",
        "typeVersion": 1,
        "position": [0, 0],  # overwritten below
        "parameters": {
            "conditions": {
                "string": [
                    {
                        "value1": "={{ $('Parse & Map State').item.json.notionStatus }}",
                        "operation": "equal",
                        "value2": "Resolved",
                    }
                ]
            }
        },
    },
    # ── Node 7: HTTP Request — Get Customer Name ───────────────────────────
    # continueOnFail: true so that if Customer relation is empty (URL malformed)
    # the workflow still proceeds to send the Slack message with "A customer" fallback.
    {
        "id": "n7",
        "name": "Get Customer Name",
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4,
        "position": [0, 0],  # overwritten below
        "continueOnFail": True,
        "parameters": {
            "method": "GET",
            "url": "={{ 'https://api.notion.com/v1/pages/' + $('Search Notion Issue').item.json.results[0].properties.Customer.relation[0].id }}",
            "sendHeaders": True,
            "headerParameters": NOTION_GET_HEADERS,
        },
    },
    # ── Node 8: HTTP Request — Send Slack Alert ────────────────────────────
    {
        "id": "n8",
        "name": "Send Slack Alert",
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4,
        "position": [0, 0],  # overwritten below
        "credentials": {
            "httpHeaderAuth": {
                "id": SLACK_CRED_ID,
                "name": SLACK_CRED_NAME,
            }
        },
        "parameters": {
            "authentication": "genericCredentialType",
            "genericAuthType": "httpHeaderAuth",
            "method": "POST",
            "url": "https://slack.com/api/chat.postMessage",
            "sendBody": True,
            "contentType": "raw",
            "rawContentType": "application/json; charset=utf-8",
            "body": SLACK_BODY,
        },
    },
]

# New connections to merge into the existing connections dict
NEW_CONNECTIONS = {
    "Update Notion Page": {
        "main": [[{"node": "Is Resolved?", "type": "main", "index": 0}]]
    },
    "Is Resolved?": {
        "main": [
            [{"node": "Get Customer Name", "type": "main", "index": 0}],  # true
            [],  # false → silent stop (In Progress transitions do nothing)
        ]
    },
    "Get Customer Name": {
        "main": [[{"node": "Send Slack Alert", "type": "main", "index": 0}]]
    },
}


def main():
    # ── Step 1: Fetch current workflow ──────────────────────────────────────
    print(f"Step 1/4 — Fetching workflow {WORKFLOW_ID}...")
    wf = n8n("GET", f"/workflows/{WORKFLOW_ID}")
    print(f"  + Fetched  name='{wf['name']}'  nodes={len(wf['nodes'])}  active={wf.get('active')}")

    nodes = wf["nodes"]
    connections = wf["connections"]

    # Idempotency guard — abort if any of our nodes already exist
    existing_names = {node["name"] for node in nodes}
    for new_node in NEW_NODES:
        if new_node["name"] in existing_names:
            print(f"  ! Node '{new_node['name']}' already exists — workflow already patched. Exiting.")
            return

    # ── Step 2: Resolve positions relative to "Update Notion Page" ──────────
    update_node = next((n for n in nodes if n["name"] == "Update Notion Page"), None)
    if not update_node:
        print("  ERROR: 'Update Notion Page' node not found in workflow")
        raise SystemExit(1)

    base_x, base_y = update_node["position"]
    print(f"  'Update Notion Page' at position [{base_x}, {base_y}]")

    NEW_NODES[0]["position"] = [base_x + 250, base_y]   # Is Resolved?
    NEW_NODES[1]["position"] = [base_x + 500, base_y]   # Get Customer Name
    NEW_NODES[2]["position"] = [base_x + 750, base_y]   # Send Slack Alert

    # ── Step 3: Merge nodes + connections ───────────────────────────────────
    print("Step 2/4 — Appending 3 new nodes and their connections...")
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
    print(f"  Slack ch : #{SLACK_CHANNEL}")
    print("=" * 60)
    print()
    print("Verification steps:")
    print("  1. In Linear, resolve an issue with a linked Notion row (Customer set)")
    print("     → #customer-success-core should receive the alert within ~5s")
    print("  2. Change a Linear issue to In Progress — NO Slack message should be sent")
    print("  3. Resolve an issue with no Customer relation set")
    print("     → message still sends with fallback 'A customer'")


if __name__ == "__main__":
    main()
