#!/usr/bin/env python3
"""Deploy ClickHouse -> Notion AI Resolution Rate sync workflow to n8n.

Flow: Schedule (4AM) -> ClickHouse query -> Notion query -> Match -> Batch update
"""

import json
import urllib.request
import urllib.error
import ssl
import sys

# -- Configuration -----------------------------------------------------------

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = "***REMOVED***"
NOTION_API_KEY = "***REMOVED***"

NOTION_DB_ID = "84feda19cfaf4c6e9500bf21d2aaafef"
CLICKHOUSE_HOST = "https://ua2wi80os4.eu-central-1.aws.clickhouse.cloud:8443/"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "REPLACE_WITH_CLICKHOUSE_DB_PASSWORD"

# -- Helpers -----------------------------------------------------------------

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


# -- Step 1: Create credentials ---------------------------------------------

print("=" * 60)
print("Step 1: Creating n8n credentials")
print("=" * 60)

credentials_spec = [
    {
        "name": "Notion - AI Resolution Sync",
        "type": "httpHeaderAuth",
        "data": {"name": "Authorization", "value": f"Bearer {NOTION_API_KEY}"},
    },
    {
        "name": "ClickHouse - AI Resolution Sync",
        "type": "httpBasicAuth",
        "data": {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD},
    },
]

cred_ids = {}

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
        print(f"  Reusing existing credential: {name} (id={cred_ids[name]})")
        continue

    print(f"  Creating credential: {name} ...")
    try:
        result = n8n_request("POST", "/api/v1/credentials", spec)
        cred_ids[name] = result["id"]
        print(f"  Created: {name} (id={cred_ids[name]})")
    except urllib.error.HTTPError:
        try:
            resp = n8n_request("GET", f"/api/v1/credentials?type={spec['type']}")
            for c in resp.get("data", []):
                if c["name"] == name:
                    cred_ids[name] = c["id"]
                    print(f"  Found existing: {name} (id={cred_ids[name]})")
                    break
        except Exception as e2:
            print(f"  Could not list credentials: {e2}")

        if name not in cred_ids:
            print(f"  FATAL: Could not create or find credential '{name}'")
            sys.exit(1)

notion_cred_id = cred_ids["Notion - AI Resolution Sync"]
clickhouse_cred_id = cred_ids["ClickHouse - AI Resolution Sync"]

print(f"\n  Credential IDs:")
print(f"    Notion:     {notion_cred_id}")
print(f"    ClickHouse: {clickhouse_cred_id}")

# -- Step 2: Build and create workflow ---------------------------------------

print("\n" + "=" * 60)
print("Step 2: Creating workflow")
print("=" * 60)

# ClickHouse SQL query - get all rows inserted in the last 25 hours.
# Since the table is a daily snapshot (one row per customer per day), a 25-hour
# window reliably captures the most recent snapshot for every customer, with a
# 1-hour buffer for pipeline timing drift.
ch_query = (
    "SELECT * "
    "FROM operator.public_workspace_report_snapshot "
    "WHERE created_at > now() - INTERVAL 25 HOUR "
    "FORMAT JSON"
)

# Notion query body - filter for pages that have a Stripe Customer ID
notion_query_body = json.dumps({
    "page_size": 100,
    "filter": {
        "property": "\U0001f517 Stripe Customer ID",
        "rich_text": {"is_not_empty": True}
    }
})

# Code node: Match ClickHouse rows to Notion pages by Stripe Customer ID
match_code = """// Get ClickHouse rows (FORMAT JSON returns {meta, data, rows, statistics})
const chResponse = $('Query ClickHouse').first().json;
const chRows = chResponse.data || [];

// Get Notion pages from the query response
const notionResponse = $input.first().json;
const notionPages = notionResponse.results || [];

// Build map: stripe_customer_id -> Notion page ID
const stripeToPageId = {};
for (const page of notionPages) {
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
  if (stripeId) {
    stripeToPageId[stripeId.trim()] = page.id;
  }
}

// Match and prepare update items
const updates = [];
let matched = 0;
let unmatched = 0;

for (const row of chRows) {
  const stripeId = (row.stripe_customer_id || '').trim();
  const rate = parseFloat(row.ai_resolution_rate);
  if (!stripeId || isNaN(rate)) continue;

  const pageId = stripeToPageId[stripeId];
  if (pageId) {
    updates.push({
      json: {
        pageId: pageId,
        rate: rate / 100  // ClickHouse stores 0-100, Notion expects 0-1
      }
    });
    matched++;
  } else {
    unmatched++;
  }
}

// Log summary
console.log(`Matched: ${matched}, Unmatched: ${unmatched}, Notion pages: ${notionPages.length}, CH rows: ${chRows.length}`);

if (notionResponse.has_more) {
  console.log('WARNING: Notion has more pages. Consider adding pagination.');
}

if (updates.length === 0) {
  return [{ json: { done: true, message: `No matches. CH rows: ${chRows.length}, Notion pages: ${notionPages.length}` } }];
}

return updates;"""

# Delay code for rate limiting (Notion allows 3 req/sec)
delay_code = """// 350ms delay between Notion API updates to respect rate limit
await new Promise(resolve => setTimeout(resolve, 350));
return $input.all();"""

# -- Workflow JSON -----------------------------------------------------------

workflow_json = {
    "name": "Daily AI Resolution Rate: ClickHouse \u2192 Notion",
    "nodes": [
        # 1. Schedule Trigger - daily at 4:00 AM
        {
            "parameters": {
                "rule": {
                    "interval": [
                        {
                            "field": "cronExpression",
                            "expression": "0 4 * * *"
                        }
                    ]
                }
            },
            "id": "node-trigger",
            "name": "Daily 4AM Trigger",
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.2,
            "position": [0, 300],
        },

        # 2. Query ClickHouse via HTTPS interface
        {
            "parameters": {
                "method": "POST",
                "url": CLICKHOUSE_HOST,
                "authentication": "genericCredentialType",
                "genericAuthType": "httpBasicAuth",
                "sendBody": True,
                "specifyBody": "string",
                "body": ch_query,
                "options": {
                    "timeout": 30000,
                },
            },
            "id": "node-clickhouse",
            "name": "Query ClickHouse",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [260, 300],
            "credentials": {
                "httpBasicAuth": {
                    "id": str(clickhouse_cred_id),
                    "name": "ClickHouse - AI Resolution Sync",
                }
            },
        },

        # 3. Query Notion Master Customer Table
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
                "options": {},
            },
            "id": "node-notion-query",
            "name": "Fetch Notion Pages",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [520, 300],
            "credentials": {
                "httpHeaderAuth": {
                    "id": str(notion_cred_id),
                    "name": "Notion - AI Resolution Sync",
                }
            },
        },

        # 4. Match ClickHouse rows to Notion pages
        {
            "parameters": {
                "jsCode": match_code,
            },
            "id": "node-match",
            "name": "Match & Build Updates",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [780, 300],
        },

        # 5. Split into batches of 1 for rate-limited updates
        {
            "parameters": {
                "batchSize": 1,
                "options": {}
            },
            "id": "node-batch",
            "name": "Batch Updates",
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [1040, 300],
        },

        # 6. Rate limit delay (350ms)
        {
            "parameters": {
                "jsCode": delay_code,
            },
            "id": "node-delay",
            "name": "Rate Limit Delay",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1300, 300],
        },

        # 7. Update Notion page with AI Resolution Rate
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
                    "id": str(notion_cred_id),
                    "name": "Notion - AI Resolution Sync",
                }
            },
        },
    ],

    "connections": {
        "Daily 4AM Trigger": {
            "main": [
                [{"node": "Query ClickHouse", "type": "main", "index": 0}]
            ]
        },
        "Query ClickHouse": {
            "main": [
                [{"node": "Fetch Notion Pages", "type": "main", "index": 0}]
            ]
        },
        "Fetch Notion Pages": {
            "main": [
                [{"node": "Match & Build Updates", "type": "main", "index": 0}]
            ]
        },
        "Match & Build Updates": {
            "main": [
                [{"node": "Batch Updates", "type": "main", "index": 0}]
            ]
        },
        "Batch Updates": {
            "main": [
                [{"node": "Rate Limit Delay", "type": "main", "index": 0}],
                []
            ]
        },
        "Rate Limit Delay": {
            "main": [
                [{"node": "Update Notion Page", "type": "main", "index": 0}]
            ]
        },
        "Update Notion Page": {
            "main": [
                [{"node": "Batch Updates", "type": "main", "index": 0}]
            ]
        },
    },

    "settings": {"executionOrder": "v1", "saveManualExecutions": True},
}

try:
    result = n8n_request("POST", "/api/v1/workflows", workflow_json)
    workflow_id = result["id"]
    print(f"  Workflow created: id={workflow_id}")
except urllib.error.HTTPError:
    print("  FATAL: Could not create workflow")
    sys.exit(1)

# -- Step 3: Activate workflow -----------------------------------------------

print("\n" + "=" * 60)
print("Step 3: Activating workflow")
print("=" * 60)

workflow_active = False
try:
    n8n_request("POST", f"/api/v1/workflows/{workflow_id}/activate")
    print("  Workflow activated")
    workflow_active = True
except Exception as e:
    print(f"  Warning: Activation failed: {e}")
    print("  This is expected - ClickHouse password is a placeholder.")

# -- Summary -----------------------------------------------------------------

print("\n" + "=" * 60)
print("DEPLOYMENT SUMMARY")
print("=" * 60)

workflow_url = f"{N8N_BASE}/workflow/{workflow_id}"
print(f"  Workflow ID:       {workflow_id}")
print(f"  Workflow URL:      {workflow_url}")
print(f"  Workflow active:   {workflow_active}")
print(f"  Credentials:       Notion (id={notion_cred_id}), ClickHouse (id={clickhouse_cred_id})")
print()
print("  NEXT STEPS:")
print("  1. Get ClickHouse 'default' user DB password from your engineer")
print(f"  2. Update credential in n8n UI: {N8N_BASE}/credentials/{clickhouse_cred_id}")
print("  3. Open workflow in n8n UI and test manually")
print("  4. Spot-check 3-5 customers in Notion")
print("  5. Activate the workflow for daily 4 AM runs")
print("=" * 60)
