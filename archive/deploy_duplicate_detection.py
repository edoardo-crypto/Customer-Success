#!/usr/bin/env python3
"""Deploy Duplicate Detection workflow to n8n."""

import json
import urllib.request
import urllib.error
import ssl
import sys
import creds

# ── Configuration ────────────────────────────────────────────────────────────

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
NOTION_API_KEY = creds.get("NOTION_TOKEN")
LINEAR_API_KEY = creds.get("LINEAR_TOKEN")
ANTHROPIC_API_KEY = creds.get("ANTHROPIC_API_KEY")
NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"

# ── Helpers ──────────────────────────────────────────────────────────────────

ctx = ssl.create_default_context()

def n8n_request(method, path, body=None):
    """Make an API request to n8n."""
    url = f"{N8N_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-N8N-API-KEY", N8N_API_KEY)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode() if e.fp else ""
        print(f"  HTTP {e.code} for {method} {path}: {body_text}")
        raise


def notion_request(method, path, body=None):
    """Make an API request to Notion."""
    url = f"https://api.notion.com/v1{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {NOTION_API_KEY}")
    req.add_header("Notion-Version", "2022-06-28")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, context=ctx) as resp:
        return json.loads(resp.read().decode())


# ── Step 1: Create credentials ──────────────────────────────────────────────

print("=" * 60)
print("Step 1: Creating n8n credentials")
print("=" * 60)

credentials_spec = [
    {
        "name": "Notion - Duplicate Detection",
        "type": "httpHeaderAuth",
        "data": {"name": "Authorization", "value": f"Bearer {NOTION_API_KEY}"},
    },
    {
        "name": "Linear - Duplicate Detection",
        "type": "httpHeaderAuth",
        "data": {"name": "Authorization", "value": LINEAR_API_KEY},
    },
    {
        "name": "Anthropic - Duplicate Detection",
        "type": "httpHeaderAuth",
        "data": {"name": "x-api-key", "value": ANTHROPIC_API_KEY},
    },
]

cred_ids = {}  # name → id

# First, list existing credentials so we can reuse if already created
existing_creds = {}
try:
    resp = n8n_request("GET", "/api/v1/credentials")
    for c in resp.get("data", []):
        existing_creds[c["name"]] = c["id"]
    print(f"  Found {len(existing_creds)} existing credentials")
except Exception as e:
    print(f"  Warning: could not list existing credentials: {e}")

for spec in credentials_spec:
    name = spec["name"]
    if name in existing_creds:
        cred_ids[name] = existing_creds[name]
        print(f"  ✓ Reusing existing credential: {name} (id={cred_ids[name]})")
        continue

    print(f"  Creating credential: {name} ...")
    try:
        result = n8n_request("POST", "/api/v1/credentials", spec)
        cred_ids[name] = result["id"]
        print(f"  ✓ Created: {name} (id={cred_ids[name]})")
    except urllib.error.HTTPError as e:
        error_body = ""
        try:
            error_body = e.read().decode() if hasattr(e, 'read') else ""
        except:
            pass
        print(f"  Creation failed (HTTP {e.code}): {error_body}")
        # Try to list credentials with cursor-based pagination or filter
        print(f"  Trying to find existing credential by name...")
        try:
            # n8n cloud may support filter param
            filter_url = f"/api/v1/credentials?type=httpHeaderAuth"
            resp = n8n_request("GET", filter_url)
            for c in resp.get("data", []):
                if c["name"] == name:
                    cred_ids[name] = c["id"]
                    print(f"  ✓ Found existing: {name} (id={cred_ids[name]})")
                    break
        except Exception as e2:
            print(f"  Could not list credentials: {e2}")

        if name not in cred_ids:
            print(f"  FATAL: Could not create or find credential '{name}'")
            sys.exit(1)

notion_cred_id = cred_ids["Notion - Duplicate Detection"]
linear_cred_id = cred_ids["Linear - Duplicate Detection"]
anthropic_cred_id = cred_ids["Anthropic - Duplicate Detection"]

print(f"\n  Credential IDs:")
print(f"    Notion:    {notion_cred_id}")
print(f"    Linear:    {linear_cred_id}")
print(f"    Anthropic: {anthropic_cred_id}")

# ── Step 2: Create workflow ─────────────────────────────────────────────────

print("\n" + "=" * 60)
print("Step 2: Creating workflow")
print("=" * 60)

workflow_json = {
    "name": "Duplicate Detection: Issues → Linear",
    "nodes": [
        {
            "parameters": {
                "rule": {
                    "interval": [{"field": "minutes", "minutesInterval": 15}]
                }
            },
            "id": "node-schedule",
            "name": "Every 15 Minutes",
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.2,
            "position": [0, 0],
        },
        {
            "parameters": {
                "method": "POST",
                "url": f"https://api.notion.com/v1/databases/{NOTION_ISSUES_DB}/query",
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [{"name": "Notion-Version", "value": "2022-06-28"}]
                },
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": json.dumps(
                    {
                        "filter": {
                            "and": [
                                {
                                    "property": "Duplicate Status",
                                    "select": {"is_empty": True},
                                },
                                {
                                    "property": "Issue Title",
                                    "title": {"is_not_empty": True},
                                },
                            ]
                        },
                        "page_size": 10,
                    }
                ),
                "options": {},
            },
            "id": "node-fetch-issues",
            "name": "Fetch Unchecked Issues",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [240, 0],
            "credentials": {
                "httpHeaderAuth": {
                    "id": str(notion_cred_id),
                    "name": "Notion - Duplicate Detection",
                }
            },
        },
        {
            "parameters": {
                "conditions": {
                    "options": {
                        "caseSensitive": True,
                        "leftValue": "",
                        "typeValidation": "strict",
                    },
                    "conditions": [
                        {
                            "id": "cond-has-results",
                            "leftValue": "={{ $json.results.length }}",
                            "rightValue": 0,
                            "operator": {"type": "number", "operation": "gt"},
                        }
                    ],
                    "combinator": "and",
                },
                "options": {},
            },
            "id": "node-if-issues",
            "name": "Has Unchecked Issues?",
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.2,
            "position": [480, 0],
        },
        {
            "parameters": {
                "jsCode": "const results = $input.first().json.results;\nreturn results.map(page => {\n  const getTitle = (p) => p?.title?.map(t => t.plain_text).join('') || '';\n  const getText = (p) => p?.rich_text?.map(t => t.plain_text).join('') || '';\n  const getSelect = (p) => p?.select?.name || '';\n  return {\n    json: {\n      page_id: page.id,\n      issue_title: getTitle(page.properties['Issue Title']),\n      summary: getText(page.properties['Summary']),\n      category: getSelect(page.properties['Category']),\n      issue_type: getSelect(page.properties['Issue Type'])\n    }\n  };\n});"
            },
            "id": "node-split",
            "name": "Split Issues",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [720, -100],
        },
        {
            "parameters": {
                "method": "POST",
                "url": "https://api.linear.app/graphql",
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": '{"query":"{ issues(filter: { team: { id: { in: [\\"6d529180-e6ca-4940-aca0-9a479270f662\\", \\"3433e177-0b08-4ee2-86d8-59933fcf59db\\"] } }, state: { type: { nin: [\\"completed\\", \\"canceled\\"] } } }, first: 200) { nodes { id identifier title description url state { name } team { name } } } }"}',
                "options": {},
            },
            "id": "node-linear",
            "name": "Fetch Open Linear Tickets",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [960, -100],
            "credentials": {
                "httpHeaderAuth": {
                    "id": str(linear_cred_id),
                    "name": "Linear - Duplicate Detection",
                }
            },
        },
        {
            "parameters": {
                "jsCode": "const issue = $('Split Issues').item.json;\nconst tickets = $input.first().json.data?.issues?.nodes || [];\n\nif (tickets.length === 0) {\n  return [{ json: { page_id: issue.page_id, skip: true, duplicate_status: 'New', potential_duplicate_url: null } }];\n}\n\nconst ticketsList = tickets.map((t, i) => {\n  const desc = (t.description || '').substring(0, 200).replace(/\\n/g, ' ');\n  return `${i+1}. [${t.identifier}] ${t.title}\\n   Team: ${t.team.name} | Status: ${t.state.name}\\n   URL: ${t.url}${desc ? '\\n   Desc: ' + desc : ''}`;\n}).join('\\n\\n');\n\nreturn [{ json: {\n  page_id: issue.page_id,\n  issue_title: issue.issue_title,\n  summary: issue.summary,\n  category: issue.category,\n  issue_type: issue.issue_type,\n  linear_tickets_formatted: ticketsList,\n  ticket_count: tickets.length,\n  skip: false\n} }];"
            },
            "id": "node-prepare",
            "name": "Prepare Claude Prompt",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1200, -100],
        },
        {
            "parameters": {
                "conditions": {
                    "options": {
                        "caseSensitive": True,
                        "leftValue": "",
                        "typeValidation": "strict",
                    },
                    "conditions": [
                        {
                            "id": "cond-not-skip",
                            "leftValue": "={{ $json.skip }}",
                            "rightValue": True,
                            "operator": {"type": "boolean", "operation": "notEqual"},
                        }
                    ],
                    "combinator": "and",
                },
                "options": {},
            },
            "id": "node-if-tickets",
            "name": "Has Linear Tickets?",
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.2,
            "position": [1440, -100],
        },
        {
            "parameters": {
                "method": "POST",
                "url": "https://api.anthropic.com/v1/messages",
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {"name": "anthropic-version", "value": "2023-06-01"},
                        {"name": "content-type", "value": "application/json"},
                    ]
                },
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": """={{ JSON.stringify({ model: 'claude-sonnet-4-5-20250929', max_tokens: 500, messages: [{ role: 'user', content: `You are a duplicate issue detector for KonvoAI (ecommerce AI platform). Compare the NEW ISSUE against EXISTING LINEAR TICKETS.

A match means the same underlying problem, bug, or feature request. Consider:
- Same technical component or integration
- Same symptom or error
- Same customer workflow impacted
- Same feature requested

Do NOT match issues that merely share a broad category (e.g. two unrelated Shopify bugs are not duplicates).

NEW ISSUE:
Title: ${$json.issue_title}
Summary: ${$json.summary || 'N/A'}
Category: ${$json.category || 'N/A'}
Type: ${$json.issue_type || 'N/A'}

EXISTING TICKETS (${$json.ticket_count}):
${$json.linear_tickets_formatted}

Respond with ONLY this JSON (no markdown, no explanation):
{"is_duplicate":true/false,"confidence":0-100,"matched_ticket_url":"url or null","matched_ticket_id":"id or null","reasoning":"one sentence"}

Set is_duplicate=true only if confidence >= 75.` }] }) }}""",
                "options": {"timeout": 30000},
            },
            "id": "node-claude",
            "name": "Claude Semantic Match",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [1680, -200],
            "credentials": {
                "httpHeaderAuth": {
                    "id": str(anthropic_cred_id),
                    "name": "Anthropic - Duplicate Detection",
                }
            },
        },
        {
            "parameters": {
                "jsCode": "const resp = $input.first().json;\nconst pageId = $('Prepare Claude Prompt').item.json.page_id;\nlet result;\ntry {\n  const text = resp.content[0].text.trim().replace(/^```json\\n?/, '').replace(/\\n?```$/, '').trim();\n  result = JSON.parse(text);\n} catch (e) {\n  result = { is_duplicate: false, confidence: 0, matched_ticket_url: null, matched_ticket_id: null, reasoning: 'Parse error: ' + e.message };\n}\nreturn [{ json: {\n  page_id: pageId,\n  duplicate_status: result.is_duplicate ? 'Likely Duplicate' : 'New',\n  potential_duplicate_url: result.matched_ticket_url || null,\n  confidence: result.confidence || 0,\n  reasoning: result.reasoning || ''\n} }];"
            },
            "id": "node-parse",
            "name": "Parse Claude Response",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1920, -200],
        },
        {
            "parameters": {
                "method": "PATCH",
                "url": "=https://api.notion.com/v1/pages/{{ $json.page_id }}",
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [{"name": "Notion-Version", "value": "2022-06-28"}]
                },
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": "={{ JSON.stringify({ properties: Object.assign({ 'Duplicate Status': { select: { name: $json.duplicate_status } } }, $json.potential_duplicate_url ? { 'Potential Duplicate URL': { url: $json.potential_duplicate_url } } : {}) }) }}",
                "options": {},
            },
            "id": "node-update-match",
            "name": "Update Notion (Result)",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [2160, -200],
            "credentials": {
                "httpHeaderAuth": {
                    "id": str(notion_cred_id),
                    "name": "Notion - Duplicate Detection",
                }
            },
        },
        {
            "parameters": {
                "method": "PATCH",
                "url": "=https://api.notion.com/v1/pages/{{ $json.page_id }}",
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [{"name": "Notion-Version", "value": "2022-06-28"}]
                },
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": '{ "properties": { "Duplicate Status": { "select": { "name": "New" } } } }',
                "options": {},
            },
            "id": "node-update-new",
            "name": "Update Notion (New)",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [1680, 0],
            "credentials": {
                "httpHeaderAuth": {
                    "id": str(notion_cred_id),
                    "name": "Notion - Duplicate Detection",
                }
            },
        },
    ],
    "connections": {
        "Every 15 Minutes": {
            "main": [
                [{"node": "Fetch Unchecked Issues", "type": "main", "index": 0}]
            ]
        },
        "Fetch Unchecked Issues": {
            "main": [
                [{"node": "Has Unchecked Issues?", "type": "main", "index": 0}]
            ]
        },
        "Has Unchecked Issues?": {
            "main": [[{"node": "Split Issues", "type": "main", "index": 0}]]
        },
        "Split Issues": {
            "main": [
                [{"node": "Fetch Open Linear Tickets", "type": "main", "index": 0}]
            ]
        },
        "Fetch Open Linear Tickets": {
            "main": [
                [{"node": "Prepare Claude Prompt", "type": "main", "index": 0}]
            ]
        },
        "Prepare Claude Prompt": {
            "main": [
                [{"node": "Has Linear Tickets?", "type": "main", "index": 0}]
            ]
        },
        "Has Linear Tickets?": {
            "main": [
                [{"node": "Claude Semantic Match", "type": "main", "index": 0}],
                [{"node": "Update Notion (New)", "type": "main", "index": 0}],
            ]
        },
        "Claude Semantic Match": {
            "main": [
                [{"node": "Parse Claude Response", "type": "main", "index": 0}]
            ]
        },
        "Parse Claude Response": {
            "main": [
                [{"node": "Update Notion (Result)", "type": "main", "index": 0}]
            ]
        },
    },
    "settings": {"executionOrder": "v1", "saveManualExecutions": True},
}

try:
    result = n8n_request("POST", "/api/v1/workflows", workflow_json)
    workflow_id = result["id"]
    print(f"  ✓ Workflow created: id={workflow_id}")
except urllib.error.HTTPError as e:
    print(f"  FATAL: Could not create workflow")
    sys.exit(1)

# ── Step 3: Activate workflow ────────────────────────────────────────────────

print("\n" + "=" * 60)
print("Step 3: Activating workflow")
print("=" * 60)

try:
    n8n_request("PATCH", f"/api/v1/workflows/{workflow_id}", {"active": True})
    print(f"  ✓ Workflow activated")
    workflow_active = True
except Exception as e:
    print(f"  Warning: Activation failed: {e}")
    print("  Workflow created but inactive. Open it in n8n UI to verify credentials and activate.")
    workflow_active = False

# ── Step 4: Verify ──────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("Step 4: Verification")
print("=" * 60)

workflow_url = f"{N8N_BASE}/workflow/{workflow_id}"
print(f"  Workflow URL: {workflow_url}")

# Count unchecked issues in Notion
try:
    notion_resp = notion_request(
        "POST",
        f"/databases/{NOTION_ISSUES_DB}/query",
        {
            "filter": {
                "and": [
                    {"property": "Duplicate Status", "select": {"is_empty": True}},
                    {"property": "Issue Title", "title": {"is_not_empty": True}},
                ]
            },
            "page_size": 100,
        },
    )
    unchecked_count = len(notion_resp.get("results", []))
    print(f"  Unchecked issues in Notion: {unchecked_count}")
except Exception as e:
    unchecked_count = "unknown"
    print(f"  Warning: Could not query Notion: {e}")

# ── Summary ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("DEPLOYMENT SUMMARY")
print("=" * 60)
print(f"  Workflow ID:       {workflow_id}")
print(f"  Workflow URL:      {workflow_url}")
print(f"  Workflow active:   {workflow_active}")
print(f"  Credentials:       3 (Notion, Linear, Anthropic)")
print(f"    Notion ID:       {notion_cred_id}")
print(f"    Linear ID:       {linear_cred_id}")
print(f"    Anthropic ID:    {anthropic_cred_id}")
print(f"  Unchecked issues:  {unchecked_count}")
print(f"\n  Next run will process up to 10 unchecked issues.")
print("=" * 60)
