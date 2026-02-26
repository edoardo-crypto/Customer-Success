#!/usr/bin/env python3
"""Deploy Linear Issue State → Notion Issue Status Sync workflow to n8n.

Architecture (5 nodes):
  Linear Webhook → Parse & Map State → Search Notion Issue → Page Found? → Update Notion Page
"""

import json
import ssl
import urllib.request
import urllib.error

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = (
    "***REMOVED_JWT***"
    ".eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJpc3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleHAiOjE3NzMyNzAwMDB9"
    ".X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)
NOTION_TOKEN = "***REMOVED***"
NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"

ctx = ssl.create_default_context()


def n8n(method, path, body=None):
    url = f"{N8N_BASE}/api/v1{path}"
    data = json.dumps(body).encode() if body is not None else None
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
# Code node — Parse & Map State
# Guards: non-issue-update, non-state-change, irrelevant state types
# Outputs one item with searchBody + patchBodyStr (pre-stringified JSON)
# ---------------------------------------------------------------------------
PARSE_CODE = r"""
const items = $input.all();
const body = items[0].json;

const { action, type, data, updatedFrom } = body;

// Only handle issue state-change updates
if (action !== 'update' || type !== 'Issue') return [];
if (!updatedFrom || !updatedFrom.stateId) return [];
if (!data || !data.identifier) return [];

const stateType = data.state?.type;
if (stateType !== 'started' && stateType !== 'completed') return [];

const identifier  = data.identifier;   // e.g. "KON-42"
const notionStatus = stateType === 'started' ? 'In Progress' : 'Resolved';

// Pre-stringify bodies to avoid n8n expression-escaping issues
const searchBody = JSON.stringify({
  filter: { property: 'Linear Ticket URL', url: { contains: identifier } },
  page_size: 1,
});

const props = { Status: { select: { name: notionStatus } } };
if (notionStatus === 'Resolved') {
  props['Resolved At'] = { date: { start: new Date().toISOString() } };
}
const patchBodyStr = JSON.stringify({ properties: props });

return [{ json: { identifier, notionStatus, searchBody, patchBodyStr } }];
""".strip()

# ---------------------------------------------------------------------------
# Workflow definition
# ---------------------------------------------------------------------------
NOTION_AUTH_HEADERS = {
    "parameters": [
        {"name": "Authorization", "value": f"Bearer {NOTION_TOKEN}"},
        {"name": "Notion-Version", "value": "2022-06-28"},
        {"name": "Content-Type",   "value": "application/json"},
    ]
}

workflow = {
    "name": "Linear Issue State → Notion Status Sync",
    "settings": {
        "executionOrder": "v1",
    },
    "nodes": [
        # ── Node 1: Webhook ─────────────────────────────────────────────────
        {
            "id": "n1",
            "name": "Linear Webhook",
            "type": "n8n-nodes-base.webhook",
            "typeVersion": 1,
            "position": [250, 300],
            "parameters": {
                "httpMethod": "POST",
                "path": "linear-issue-sync",
                "responseMode": "onReceived",
                "options": {},
            },
        },
        # ── Node 2: Code ─────────────────────────────────────────────────────
        {
            "id": "n2",
            "name": "Parse & Map State",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [500, 300],
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": PARSE_CODE,
            },
        },
        # ── Node 3: HTTP Request — search ────────────────────────────────────
        {
            "id": "n3",
            "name": "Search Notion Issue",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position": [750, 300],
            "parameters": {
                "method": "POST",
                "url": f"https://api.notion.com/v1/databases/{NOTION_ISSUES_DB}/query",
                "sendHeaders": True,
                "headerParameters": NOTION_AUTH_HEADERS,
                "sendBody": True,
                "contentType": "raw",
                "rawContentType": "application/json",
                "body": "={{ $json.searchBody }}",
            },
        },
        # ── Node 4: IF — page found? ──────────────────────────────────────────
        {
            "id": "n4",
            "name": "Page Found?",
            "type": "n8n-nodes-base.if",
            "typeVersion": 1,
            "position": [1000, 300],
            "parameters": {
                "conditions": {
                    "number": [
                        {
                            "value1": "={{ $json.results.length }}",
                            "operation": "larger",
                            "value2": 0,
                        }
                    ]
                }
            },
        },
        # ── Node 5: HTTP Request — update ────────────────────────────────────
        {
            "id": "n5",
            "name": "Update Notion Page",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position": [1250, 300],
            "parameters": {
                "method": "PATCH",
                "url": "={{ 'https://api.notion.com/v1/pages/' + $json.results[0].id }}",
                "sendHeaders": True,
                "headerParameters": NOTION_AUTH_HEADERS,
                "sendBody": True,
                "contentType": "raw",
                "rawContentType": "application/json",
                "body": "={{ $('Parse & Map State').first().json.patchBodyStr }}",
            },
        },
    ],
    "connections": {
        "Linear Webhook": {
            "main": [[{"node": "Parse & Map State",  "type": "main", "index": 0}]]
        },
        "Parse & Map State": {
            "main": [[{"node": "Search Notion Issue", "type": "main", "index": 0}]]
        },
        "Search Notion Issue": {
            "main": [[{"node": "Page Found?",         "type": "main", "index": 0}]]
        },
        "Page Found?": {
            "main": [
                [{"node": "Update Notion Page", "type": "main", "index": 0}],
                [],   # false branch → silent stop
            ]
        },
    },
}


def main():
    print("Step 1/2 — Creating workflow…")
    result = n8n("POST", "/workflows", workflow)
    wf_id = result["id"]
    print(f"  ✓ Created  id={wf_id}")

    print("Step 2/2 — Activating workflow…")
    n8n("POST", f"/workflows/{wf_id}/activate")
    print("  ✓ Activated")

    print()
    print("=" * 60)
    print(f"  Workflow URL : {N8N_BASE}/workflow/{wf_id}")
    print(f"  Webhook URL  : {N8N_BASE}/webhook/linear-issue-sync")
    print("=" * 60)
    print()
    print("NEXT STEP — Register webhook in Linear Developer Hub:")
    print(f"  URL    : {N8N_BASE}/webhook/linear-issue-sync")
    print("  Events : Issue update (state change)")
    print()
    print("State mapping:")
    print("  started   → Notion 'In Progress'")
    print("  completed → Notion 'Resolved' + sets 'Resolved At' = now")
    print("  all others → ignored (pipeline stops at Code node)")


if __name__ == "__main__":
    main()
