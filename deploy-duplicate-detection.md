# Task: Deploy Duplicate Detection Workflow to n8n

You are deploying an n8n workflow for KonvoAI that checks new Notion issues against open Linear tickets using Claude for semantic duplicate matching. Execute every step below. Do not ask questions. If a step fails, log the error and continue.

## Environment

- n8n cloud: `https://konvoai.app.n8n.cloud`
- n8n API key: read from env var `N8N_API_KEY` (fail if missing)
- The workflow uses three external APIs (Notion, Linear, Anthropic). These are called via HTTP Request nodes with Header Auth credentials stored in n8n. You will create the credentials via the n8n API.

## Required API keys

Read these from environment variables. Fail with a clear message if any are missing:
- `N8N_API_KEY`
- `NOTION_API_KEY` (Notion integration token, starts with `ntn_` or `secret_`)
- `LINEAR_API_KEY` (Linear personal API key)
- `ANTHROPIC_API_KEY`

## Constants

```
NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"
LINEAR_CS_TEAM_ID = "6d529180-e6ca-4940-aca0-9a479270f662"
LINEAR_ENG_TEAM_ID = "3433e177-0b08-4ee2-86d8-59933fcf59db"
N8N_BASE = "https://konvoai.app.n8n.cloud"
```

## Step 1: Create n8n credentials

Create three Header Auth credentials via the n8n REST API (`POST /api/v1/credentials`). Save each returned `id` for use in the workflow JSON.

### Credential A: Notion

```json
{
  "name": "Notion - Duplicate Detection",
  "type": "httpHeaderAuth",
  "data": {
    "name": "Authorization",
    "value": "Bearer <NOTION_API_KEY>"
  }
}
```

### Credential B: Linear

```json
{
  "name": "Linear - Duplicate Detection",
  "type": "httpHeaderAuth",
  "data": {
    "name": "Authorization",
    "value": "<LINEAR_API_KEY>"
  }
}
```

### Credential C: Anthropic

```json
{
  "name": "Anthropic - Duplicate Detection",
  "type": "httpHeaderAuth",
  "data": {
    "name": "x-api-key",
    "value": "<ANTHROPIC_API_KEY>"
  }
}
```

If credential creation fails because one already exists, list credentials (`GET /api/v1/credentials`) and find the matching one by name. Use its ID.

## Step 2: Create the workflow

`POST /api/v1/workflows` with the JSON below. Replace all `__PLACEHOLDER__` values with the credential IDs from Step 1.

The workflow has 10 nodes:

### Node graph

```
Schedule (15 min)
  → Fetch Unchecked Issues (Notion query)
    → Has Issues? (IF)
      → TRUE: Split Issues (Code)
        → Fetch Open Linear Tickets (HTTP/GraphQL)
          → Prepare Claude Prompt (Code)
            → Has Tickets? (IF)
              → TRUE: Claude Semantic Match (HTTP)
                → Parse Response (Code)
                  → Update Notion with result (HTTP PATCH)
              → FALSE: Update Notion as "New" (HTTP PATCH)
      → FALSE: (stop)
```

### Complete workflow JSON

```json
{
  "name": "Duplicate Detection: Issues → Linear",
  "nodes": [
    {
      "parameters": {
        "rule": {
          "interval": [{ "field": "minutes", "minutesInterval": 15 }]
        }
      },
      "id": "node-schedule",
      "name": "Every 15 Minutes",
      "type": "n8n-nodes-base.scheduleTrigger",
      "typeVersion": 1.2,
      "position": [0, 0]
    },
    {
      "parameters": {
        "method": "POST",
        "url": "https://api.notion.com/v1/databases/bd1ed48de20e426f8bebeb8e700d19d8/query",
        "authentication": "genericCredentialType",
        "genericAuthType": "httpHeaderAuth",
        "sendHeaders": true,
        "headerParameters": {
          "parameters": [{ "name": "Notion-Version", "value": "2022-06-28" }]
        },
        "sendBody": true,
        "specifyBody": "json",
        "jsonBody": "{\n  \"filter\": {\n    \"and\": [\n      { \"property\": \"Duplicate Status\", \"select\": { \"is_empty\": true } },\n      { \"property\": \"Issue Title\", \"title\": { \"is_not_empty\": true } }\n    ]\n  },\n  \"page_size\": 10\n}",
        "options": {}
      },
      "id": "node-fetch-issues",
      "name": "Fetch Unchecked Issues",
      "type": "n8n-nodes-base.httpRequest",
      "typeVersion": 4.2,
      "position": [240, 0],
      "credentials": {
        "httpHeaderAuth": { "id": "__NOTION_CRED_ID__", "name": "Notion - Duplicate Detection" }
      }
    },
    {
      "parameters": {
        "conditions": {
          "options": { "caseSensitive": true, "leftValue": "", "typeValidation": "strict" },
          "conditions": [{
            "id": "cond-has-results",
            "leftValue": "={{ $json.results.length }}",
            "rightValue": 0,
            "operator": { "type": "number", "operation": "gt" }
          }],
          "combinator": "and"
        },
        "options": {}
      },
      "id": "node-if-issues",
      "name": "Has Unchecked Issues?",
      "type": "n8n-nodes-base.if",
      "typeVersion": 2.2,
      "position": [480, 0]
    },
    {
      "parameters": {
        "jsCode": "const results = $input.first().json.results;\nreturn results.map(page => {\n  const getTitle = (p) => p?.title?.map(t => t.plain_text).join('') || '';\n  const getText = (p) => p?.rich_text?.map(t => t.plain_text).join('') || '';\n  const getSelect = (p) => p?.select?.name || '';\n  return {\n    json: {\n      page_id: page.id,\n      issue_title: getTitle(page.properties['Issue Title']),\n      summary: getText(page.properties['Summary']),\n      category: getSelect(page.properties['Category']),\n      issue_type: getSelect(page.properties['Issue Type'])\n    }\n  };\n});"
      },
      "id": "node-split",
      "name": "Split Issues",
      "type": "n8n-nodes-base.code",
      "typeVersion": 2,
      "position": [720, -100]
    },
    {
      "parameters": {
        "method": "POST",
        "url": "https://api.linear.app/graphql",
        "authentication": "genericCredentialType",
        "genericAuthType": "httpHeaderAuth",
        "sendBody": true,
        "specifyBody": "json",
        "jsonBody": "{\"query\":\"{ issues(filter: { team: { id: { in: [\\\"6d529180-e6ca-4940-aca0-9a479270f662\\\", \\\"3433e177-0b08-4ee2-86d8-59933fcf59db\\\"] } }, state: { type: { nin: [\\\"completed\\\", \\\"canceled\\\"] } } }, first: 200) { nodes { id identifier title description url state { name } team { name } } } }\"}",
        "options": {}
      },
      "id": "node-linear",
      "name": "Fetch Open Linear Tickets",
      "type": "n8n-nodes-base.httpRequest",
      "typeVersion": 4.2,
      "position": [960, -100],
      "credentials": {
        "httpHeaderAuth": { "id": "__LINEAR_CRED_ID__", "name": "Linear - Duplicate Detection" }
      }
    },
    {
      "parameters": {
        "jsCode": "const issue = $('Split Issues').item.json;\nconst tickets = $input.first().json.data?.issues?.nodes || [];\n\nif (tickets.length === 0) {\n  return [{ json: { page_id: issue.page_id, skip: true, duplicate_status: 'New', potential_duplicate_url: null } }];\n}\n\nconst ticketsList = tickets.map((t, i) => {\n  const desc = (t.description || '').substring(0, 200).replace(/\\n/g, ' ');\n  return `${i+1}. [${t.identifier}] ${t.title}\\n   Team: ${t.team.name} | Status: ${t.state.name}\\n   URL: ${t.url}${desc ? '\\n   Desc: ' + desc : ''}`;\n}).join('\\n\\n');\n\nreturn [{ json: {\n  page_id: issue.page_id,\n  issue_title: issue.issue_title,\n  summary: issue.summary,\n  category: issue.category,\n  issue_type: issue.issue_type,\n  linear_tickets_formatted: ticketsList,\n  ticket_count: tickets.length,\n  skip: false\n} }];"
      },
      "id": "node-prepare",
      "name": "Prepare Claude Prompt",
      "type": "n8n-nodes-base.code",
      "typeVersion": 2,
      "position": [1200, -100]
    },
    {
      "parameters": {
        "conditions": {
          "options": { "caseSensitive": true, "leftValue": "", "typeValidation": "strict" },
          "conditions": [{
            "id": "cond-not-skip",
            "leftValue": "={{ $json.skip }}",
            "rightValue": true,
            "operator": { "type": "boolean", "operation": "notEqual" }
          }],
          "combinator": "and"
        },
        "options": {}
      },
      "id": "node-if-tickets",
      "name": "Has Linear Tickets?",
      "type": "n8n-nodes-base.if",
      "typeVersion": 2.2,
      "position": [1440, -100]
    },
    {
      "parameters": {
        "method": "POST",
        "url": "https://api.anthropic.com/v1/messages",
        "authentication": "genericCredentialType",
        "genericAuthType": "httpHeaderAuth",
        "sendHeaders": true,
        "headerParameters": {
          "parameters": [
            { "name": "anthropic-version", "value": "2023-06-01" },
            { "name": "content-type", "value": "application/json" }
          ]
        },
        "sendBody": true,
        "specifyBody": "json",
        "jsonBody": "={{ JSON.stringify({ model: 'claude-sonnet-4-5-20250929', max_tokens: 500, messages: [{ role: 'user', content: `You are a duplicate issue detector for KonvoAI (ecommerce AI platform). Compare the NEW ISSUE against EXISTING LINEAR TICKETS.\n\nA match means the same underlying problem, bug, or feature request. Consider:\n- Same technical component or integration\n- Same symptom or error\n- Same customer workflow impacted\n- Same feature requested\n\nDo NOT match issues that merely share a broad category (e.g. two unrelated Shopify bugs are not duplicates).\n\nNEW ISSUE:\nTitle: ${$json.issue_title}\nSummary: ${$json.summary || 'N/A'}\nCategory: ${$json.category || 'N/A'}\nType: ${$json.issue_type || 'N/A'}\n\nEXISTING TICKETS (${$json.ticket_count}):\n${$json.linear_tickets_formatted}\n\nRespond with ONLY this JSON (no markdown, no explanation):\n{\"is_duplicate\":true/false,\"confidence\":0-100,\"matched_ticket_url\":\"url or null\",\"matched_ticket_id\":\"id or null\",\"reasoning\":\"one sentence\"}\n\nSet is_duplicate=true only if confidence >= 75.` }] }) }}",
        "options": { "timeout": 30000 }
      },
      "id": "node-claude",
      "name": "Claude Semantic Match",
      "type": "n8n-nodes-base.httpRequest",
      "typeVersion": 4.2,
      "position": [1680, -200],
      "credentials": {
        "httpHeaderAuth": { "id": "__ANTHROPIC_CRED_ID__", "name": "Anthropic - Duplicate Detection" }
      }
    },
    {
      "parameters": {
        "jsCode": "const resp = $input.first().json;\nconst pageId = $('Prepare Claude Prompt').item.json.page_id;\nlet result;\ntry {\n  const text = resp.content[0].text.trim().replace(/^```json\\n?/, '').replace(/\\n?```$/, '').trim();\n  result = JSON.parse(text);\n} catch (e) {\n  result = { is_duplicate: false, confidence: 0, matched_ticket_url: null, matched_ticket_id: null, reasoning: 'Parse error: ' + e.message };\n}\nreturn [{ json: {\n  page_id: pageId,\n  duplicate_status: result.is_duplicate ? 'Likely Duplicate' : 'New',\n  potential_duplicate_url: result.matched_ticket_url || null,\n  confidence: result.confidence || 0,\n  reasoning: result.reasoning || ''\n} }];"
      },
      "id": "node-parse",
      "name": "Parse Claude Response",
      "type": "n8n-nodes-base.code",
      "typeVersion": 2,
      "position": [1920, -200]
    },
    {
      "parameters": {
        "method": "PATCH",
        "url": "=https://api.notion.com/v1/pages/{{ $json.page_id }}",
        "authentication": "genericCredentialType",
        "genericAuthType": "httpHeaderAuth",
        "sendHeaders": true,
        "headerParameters": {
          "parameters": [{ "name": "Notion-Version", "value": "2022-06-28" }]
        },
        "sendBody": true,
        "specifyBody": "json",
        "jsonBody": "={{ JSON.stringify({ properties: Object.assign({ 'Duplicate Status': { select: { name: $json.duplicate_status } } }, $json.potential_duplicate_url ? { 'Potential Duplicate URL': { url: $json.potential_duplicate_url } } : {}) }) }}",
        "options": {}
      },
      "id": "node-update-match",
      "name": "Update Notion (Result)",
      "type": "n8n-nodes-base.httpRequest",
      "typeVersion": 4.2,
      "position": [2160, -200],
      "credentials": {
        "httpHeaderAuth": { "id": "__NOTION_CRED_ID__", "name": "Notion - Duplicate Detection" }
      }
    },
    {
      "parameters": {
        "method": "PATCH",
        "url": "=https://api.notion.com/v1/pages/{{ $json.page_id }}",
        "authentication": "genericCredentialType",
        "genericAuthType": "httpHeaderAuth",
        "sendHeaders": true,
        "headerParameters": {
          "parameters": [{ "name": "Notion-Version", "value": "2022-06-28" }]
        },
        "sendBody": true,
        "specifyBody": "json",
        "jsonBody": "{ \"properties\": { \"Duplicate Status\": { \"select\": { \"name\": \"New\" } } } }",
        "options": {}
      },
      "id": "node-update-new",
      "name": "Update Notion (New)",
      "type": "n8n-nodes-base.httpRequest",
      "typeVersion": 4.2,
      "position": [1680, 0],
      "credentials": {
        "httpHeaderAuth": { "id": "__NOTION_CRED_ID__", "name": "Notion - Duplicate Detection" }
      }
    }
  ],
  "connections": {
    "Every 15 Minutes": { "main": [[{ "node": "Fetch Unchecked Issues", "type": "main", "index": 0 }]] },
    "Fetch Unchecked Issues": { "main": [[{ "node": "Has Unchecked Issues?", "type": "main", "index": 0 }]] },
    "Has Unchecked Issues?": { "main": [[{ "node": "Split Issues", "type": "main", "index": 0 }]] },
    "Split Issues": { "main": [[{ "node": "Fetch Open Linear Tickets", "type": "main", "index": 0 }]] },
    "Fetch Open Linear Tickets": { "main": [[{ "node": "Prepare Claude Prompt", "type": "main", "index": 0 }]] },
    "Prepare Claude Prompt": { "main": [[{ "node": "Has Linear Tickets?", "type": "main", "index": 0 }]] },
    "Has Linear Tickets?": { "main": [[{ "node": "Claude Semantic Match", "type": "main", "index": 0 }], [{ "node": "Update Notion (New)", "type": "main", "index": 0 }]] },
    "Claude Semantic Match": { "main": [[{ "node": "Parse Claude Response", "type": "main", "index": 0 }]] },
    "Parse Claude Response": { "main": [[{ "node": "Update Notion (Result)", "type": "main", "index": 0 }]] }
  },
  "settings": { "executionOrder": "v1", "saveManualExecutions": true },
  "active": false
}
```

### Placeholder replacement

Before sending the workflow JSON to the API, replace these placeholders with the credential IDs from Step 1:
- `__NOTION_CRED_ID__` → ID from Credential A
- `__LINEAR_CRED_ID__` → ID from Credential B  
- `__ANTHROPIC_CRED_ID__` → ID from Credential C

## Step 3: Activate the workflow

`PATCH /api/v1/workflows/<workflow_id>` with `{"active": true}`.

If activation fails (usually a credential verification issue), log the error and print: "Workflow created but inactive. Open it in n8n UI to verify credentials and activate."

## Step 4: Verify

1. Print the workflow URL: `https://konvoai.app.n8n.cloud/workflow/<workflow_id>`
2. Query the Notion Issues DB to count how many rows have empty "Duplicate Status" (these will be processed on the next run)
3. Print a summary:
   - Workflow ID
   - Workflow URL  
   - Number of credentials created
   - Number of unchecked issues that will be processed

## Error handling

- If a credential already exists by name, reuse its ID (don't fail)
- If the n8n API returns errors, print the full response body for debugging
- All HTTP calls to n8n API use header `X-N8N-API-KEY: <N8N_API_KEY>`
- n8n credential creation endpoint: `POST <N8N_BASE>/api/v1/credentials`
- n8n workflow creation endpoint: `POST <N8N_BASE>/api/v1/workflows`
- n8n workflow update endpoint: `PATCH <N8N_BASE>/api/v1/workflows/<id>`
- n8n list credentials endpoint: `GET <N8N_BASE>/api/v1/credentials`
