#!/usr/bin/env python3
"""Update the AI Resolution Rate workflow to add Notion pagination."""

import json
import urllib.request
import urllib.error
import ssl

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = "***REMOVED***"
NOTION_API_KEY = "***REMOVED***"
WORKFLOW_ID = "hNUbhJ1oQlUOAQ4T"
NOTION_DB_ID = "84feda19cfaf4c6e9500bf21d2aaafef"
NOTION_CRED_ID = "EEKGIOhGYmOQmoCb"
CLICKHOUSE_CRED_ID = "eln0c5CfecjgK91H"
CLICKHOUSE_HOST = "https://ua2wi80os4.eu-central-1.aws.clickhouse.cloud:8443/"

ctx = ssl.create_default_context()


def n8n_request(method, path, body=None):
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


# -- Deactivate first -------------------------------------------------------
print("Deactivating workflow...")
try:
    n8n_request("POST", f"/api/v1/workflows/{WORKFLOW_ID}/deactivate")
    print("  Deactivated")
except Exception as e:
    print(f"  Warning: {e}")

# -- Build updated workflow --------------------------------------------------

ch_query = (
    "SELECT stripe_customer_id, ai_resolution_rate "
    "FROM operator.public_workspace_report_snapshot "
    "FORMAT JSON"
)

notion_query_body = json.dumps({
    "page_size": 100,
    "filter": {
        "property": "\U0001f517 Stripe Customer ID",
        "rich_text": {"is_not_empty": True}
    }
})

# Updated match code: flattens multiple paginated Notion response items
match_code = """// Get ClickHouse rows (FORMAT JSON returns {meta, data, rows, statistics})
const chResponse = $('Query ClickHouse').first().json;
const chRows = chResponse.data || [];

// Flatten all Notion pages from paginated responses
// Each input item is a full API response: { results: [...], has_more, next_cursor }
const allNotionPages = [];
for (const item of $input.all()) {
  const results = item.json.results || [];
  allNotionPages.push(...results);
}

// Build map: stripe_customer_id -> Notion page ID
const stripeToPageId = {};
for (const page of allNotionPages) {
  const stripeProp = page.properties['\\u{1F517} Stripe Customer ID'];
  let stripeId = null;
  if (stripeProp) {
    if (stripeProp.rich_text && stripeProp.rich_text.length > 0) {
      stripeId = stripeProp.rich_text[0].plain_text;
    } else if (stripeProp.title && stripeProp.title.length > 0) {
      stripeId = stripeProp.title[0].plain_text;
    } else if (typeof stripeProp === 'string') {
      stripeId = stripeProp;
    }
  }
  if (stripeId) stripeToPageId[stripeId.trim()] = page.id;
}

// Match and prepare updates
const updates = [];
let matched = 0, unmatched = 0;

for (const row of chRows) {
  const stripeId = (row.stripe_customer_id || '').trim();
  const rate = parseFloat(row.ai_resolution_rate);
  if (!stripeId || isNaN(rate)) continue;

  const pageId = stripeToPageId[stripeId];
  if (pageId) {
    updates.push({ json: { pageId, rate: rate / 100 } });
    matched++;
  } else {
    unmatched++;
  }
}

console.log(`Matched: ${matched}, Unmatched: ${unmatched}, Notion pages: ${allNotionPages.length}, CH rows: ${chRows.length}`);

if (updates.length === 0) {
  return [{ json: { done: true, message: `No matches. CH: ${chRows.length}, Notion: ${allNotionPages.length}` } }];
}

return updates;"""

delay_code = """await new Promise(resolve => setTimeout(resolve, 350));
return $input.all();"""

workflow_body = {
    "name": "Daily AI Resolution Rate: ClickHouse \u2192 Notion",
    "nodes": [
        {
            "parameters": {
                "rule": {
                    "interval": [
                        {"field": "cronExpression", "expression": "0 4 * * *"}
                    ]
                }
            },
            "id": "node-trigger",
            "name": "Daily 4AM Trigger",
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.2,
            "position": [0, 300],
        },
        {
            "parameters": {
                "method": "POST",
                "url": CLICKHOUSE_HOST,
                "authentication": "genericCredentialType",
                "genericAuthType": "httpBasicAuth",
                "sendBody": True,
                "specifyBody": "string",
                "body": ch_query,
                "options": {"timeout": 30000},
            },
            "id": "node-clickhouse",
            "name": "Query ClickHouse",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [260, 300],
            "credentials": {
                "httpBasicAuth": {
                    "id": CLICKHOUSE_CRED_ID,
                    "name": "ClickHouse - AI Resolution Sync",
                }
            },
        },
        {
            "parameters": {
                "method": "POST",
                "url": f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {"name": "Notion-Version", "value": "2022-06-28"}
                    ]
                },
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": notion_query_body,
                "options": {
                    "pagination": {
                        "paginationMode": "updateAParameterInEachRequest",
                        "parameters": {
                            "parameters": [
                                {
                                    "type": "body",
                                    "name": "start_cursor",
                                    "value": "={{ $response.body.next_cursor }}"
                                }
                            ]
                        },
                        "paginationCompleteWhen": "responseContainsExpression",
                        "completeExpression": "={{ $response.body.has_more === false }}",
                        "limitPagesFetched": False,
                        "maxRequests": 20,
                    }
                },
            },
            "id": "node-notion-query",
            "name": "Fetch Notion Pages",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [520, 300],
            "credentials": {
                "httpHeaderAuth": {
                    "id": NOTION_CRED_ID,
                    "name": "Notion - AI Resolution Sync",
                }
            },
        },
        {
            "parameters": {"jsCode": match_code},
            "id": "node-match",
            "name": "Match & Build Updates",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [780, 300],
        },
        {
            "parameters": {"batchSize": 1, "options": {}},
            "id": "node-batch",
            "name": "Batch Updates",
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [1040, 300],
        },
        {
            "parameters": {"jsCode": delay_code},
            "id": "node-delay",
            "name": "Rate Limit Delay",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1300, 300],
        },
        {
            "parameters": {
                "method": "PATCH",
                "url": "=https://api.notion.com/v1/pages/{{ $json.pageId }}",
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {"name": "Notion-Version", "value": "2022-06-28"}
                    ]
                },
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": "={{ JSON.stringify({ properties: { '\U0001f916 AI Resolution Rate': { number: $json.rate } } }) }}",
                "options": {},
            },
            "id": "node-update",
            "name": "Update Notion Page",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [1560, 300],
            "credentials": {
                "httpHeaderAuth": {
                    "id": NOTION_CRED_ID,
                    "name": "Notion - AI Resolution Sync",
                }
            },
        },
    ],
    "connections": {
        "Daily 4AM Trigger": {
            "main": [[{"node": "Query ClickHouse", "type": "main", "index": 0}]]
        },
        "Query ClickHouse": {
            "main": [[{"node": "Fetch Notion Pages", "type": "main", "index": 0}]]
        },
        "Fetch Notion Pages": {
            "main": [[{"node": "Match & Build Updates", "type": "main", "index": 0}]]
        },
        "Match & Build Updates": {
            "main": [[{"node": "Batch Updates", "type": "main", "index": 0}]]
        },
        "Batch Updates": {
            "main": [
                [{"node": "Rate Limit Delay", "type": "main", "index": 0}],
                []
            ]
        },
        "Rate Limit Delay": {
            "main": [[{"node": "Update Notion Page", "type": "main", "index": 0}]]
        },
        "Update Notion Page": {
            "main": [[{"node": "Batch Updates", "type": "main", "index": 0}]]
        },
    },
    "settings": {"executionOrder": "v1", "saveManualExecutions": True},
}

print("Updating workflow with Notion pagination...")
try:
    result = n8n_request("PUT", f"/api/v1/workflows/{WORKFLOW_ID}", workflow_body)
    print(f"  Updated: id={result['id']}")
except urllib.error.HTTPError:
    print("  FATAL: Could not update workflow")
    raise

# -- Reactivate -------------------------------------------------------------
print("Reactivating workflow...")
try:
    n8n_request("POST", f"/api/v1/workflows/{WORKFLOW_ID}/activate")
    print("  Activated")
except Exception as e:
    print(f"  Warning: {e}")

print("\nDone. Notion pagination added (up to 20 pages / 2000 customers).")
print(f"Workflow: {N8N_BASE}/workflow/{WORKFLOW_ID}")
