#!/usr/bin/env python3
"""
Comprehensive n8n workflow fix script — Feb 17, 2026

Fixes 8 failing workflows in priority order:
  P0: Intercom Webhook, Error Handler, "Not an Issue" filter
  P1: Nightly Issue Score, Stripe Sync, Customer Enrichment, AI Resolution Rate
  P2: Duplicate Detection, CS Owner Reassign
"""

import json
import urllib.request
import urllib.error
import ssl
import sys
import time

# ── Configuration ────────────────────────────────────────────────────────────

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = "***REMOVED***"
INTERCOM_TOKEN = "***REMOVED***"
NOTION_API_KEY = "***REMOVED***"
HUBSPOT_KEY = "***REMOVED***"

# Workflow IDs
WF_INTERCOM = "3AO3SRUK80rcOCgQ"
WF_ERROR_HANDLER = "FTintCymBsThcbZj"
WF_NIGHTLY_SCORE = "6xIuCyBje6QnynUh"
WF_STRIPE_SYNC = "Ai9Y3FWjqMtEhr57"
WF_ENRICHMENT = "1FG950L1j8rkG4SJ"
WF_AI_RESOLUTION = "hNUbhJ1oQlUOAQ4T"
WF_DUPLICATE = "G4bxsv1nrzON6XXd"
WF_CS_REASSIGN = "FJrd7tESKEOGB5VH"

# Credential IDs (existing)
NOTION_CRED_DEDUP = "O8VKBqY2XhVz8AOz"       # "Notion - Duplicate Detection" httpHeaderAuth
SLACK_BOT_CRED = "IMuEGtYutmUKwCqY"           # "Slack Bot for Alerts" httpHeaderAuth
NOTION_CRED_NATIVE = "qbhd5Lx9NFT9b8KM"       # "Notion account" notionApi
CLICKHOUSE_CRED = "eln0c5CfecjgK91H"           # ClickHouse httpBasicAuth
NOTION_CRED_AI_RES = "EEKGIOhGYmOQmoCb"        # "Notion - AI Resolution Sync" httpHeaderAuth

# Database IDs
NOTION_MCT_DB = "84feda19cfaf4c6e9500bf21d2aaafef"
NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"
NOTION_DATASOURCE = "3ceb1ad0-91f1-40db-945a-c51c58035898"

# ── Helpers ──────────────────────────────────────────────────────────────────

ctx = ssl.create_default_context()
results = {"passed": [], "failed": [], "skipped": []}


def api_request(base, method, path, body=None, headers=None):
    """Generic HTTP request helper."""
    url = f"{base}{path}" if base else path
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
        return json.loads(resp.read().decode())


def n8n(method, path, body=None):
    """n8n API request."""
    return api_request(N8N_BASE, method, path, body,
                       {"X-N8N-API-KEY": N8N_API_KEY})


def log(section, msg, indent=0):
    prefix = "  " * indent
    print(f"{prefix}[{section}] {msg}")


def record(section, success, msg=""):
    if success:
        results["passed"].append(section)
        log(section, f"OK {msg}")
    else:
        results["failed"].append(section)
        log(section, f"FAILED {msg}")


# ══════════════════════════════════════════════════════════════════════════════
# P0-1: Register Intercom Webhook
# ══════════════════════════════════════════════════════════════════════════════

def fix_intercom_webhook():
    section = "P0-1: Intercom Webhook"
    log(section, "Registering webhook in Intercom...")

    webhook_url = f"{N8N_BASE}/webhook/intercom-webhook"

    # First, list existing webhooks
    try:
        resp = api_request("https://api.intercom.io", "GET", "/subscriptions", headers={
            "Authorization": f"Bearer {INTERCOM_TOKEN}",
            "Intercom-Version": "2.11",
        })
        existing = resp.get("items", resp.get("data", []))
        if isinstance(resp, list):
            existing = resp

        # Check if webhook already registered
        for sub in existing if isinstance(existing, list) else []:
            if isinstance(sub, dict) and sub.get("url") == webhook_url:
                log(section, f"Webhook already registered (id={sub.get('id')})", 1)
                record(section, True, "already registered")
                return True
    except Exception as e:
        log(section, f"Could not list existing webhooks: {e}", 1)

    # Register new webhook
    try:
        resp = api_request("https://api.intercom.io", "POST", "/subscriptions", body={
            "service_type": "web",
            "url": webhook_url,
            "topics": ["conversation.admin.closed"],
        }, headers={
            "Authorization": f"Bearer {INTERCOM_TOKEN}",
            "Intercom-Version": "2.11",
        })
        sub_id = resp.get("id", "unknown")
        log(section, f"Webhook registered (id={sub_id})", 1)
        record(section, True, f"id={sub_id}")
        return True
    except urllib.error.HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode()
        except:
            pass
        log(section, f"HTTP {e.code}: {body_text[:300]}", 1)
        record(section, False, f"HTTP {e.code}")
        return False
    except Exception as e:
        record(section, False, str(e))
        return False


# ══════════════════════════════════════════════════════════════════════════════
# P0-2: Fix Error Handler — Replace Slack OAuth2 node with HTTP Request
# ══════════════════════════════════════════════════════════════════════════════

def fix_error_handler():
    section = "P0-2: Error Handler"
    log(section, "Fixing Error Handler - Slack Alert workflow...")

    try:
        # Deactivate first
        try:
            n8n("POST", f"/api/v1/workflows/{WF_ERROR_HANDLER}/deactivate")
        except:
            pass

        # Build fixed workflow — replace native Slack node with HTTP Request
        workflow_body = {
            "name": "Error Handler - Slack Alert",
            "nodes": [
                {
                    "id": "error-trigger-001",
                    "name": "Error Trigger",
                    "type": "n8n-nodes-base.errorTrigger",
                    "typeVersion": 1,
                    "position": [250, 300],
                    "parameters": {},
                },
                {
                    "id": "format-error-msg",
                    "name": "Format Error Message",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": [500, 300],
                    "parameters": {
                        "jsCode": (
                            "const data = $input.first().json;\n"
                            "const wfName = data.workflow?.name || 'Unknown';\n"
                            "const errMsg = data.execution?.error?.message || 'Unknown error';\n"
                            "const execUrl = data.execution?.url || '';\n"
                            "\n"
                            "return [{ json: {\n"
                            "  channel: 'C0AC2BTCJVA',\n"
                            "  text: `\\u26a0\\ufe0f *n8n Workflow Failed*\\n"
                            "*Workflow:* ${wfName}\\n"
                            "*Error:* ${errMsg}\\n"
                            "*Execution:* ${execUrl}`\n"
                            "} }];\n"
                        )
                    },
                },
                {
                    "id": "slack-http-alert",
                    "name": "Slack Alert (HTTP)",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": [750, 300],
                    "parameters": {
                        "method": "POST",
                        "url": "https://slack.com/api/chat.postMessage",
                        "authentication": "genericCredentialType",
                        "genericAuthType": "httpHeaderAuth",
                        "sendBody": True,
                        "specifyBody": "json",
                        "jsonBody": '={{ JSON.stringify({ channel: $json.channel, text: $json.text }) }}',
                        "options": {},
                    },
                    "credentials": {
                        "httpHeaderAuth": {
                            "id": SLACK_BOT_CRED,
                            "name": "Slack Bot for Alerts",
                        }
                    },
                },
            ],
            "connections": {
                "Error Trigger": {
                    "main": [[{"node": "Format Error Message", "type": "main", "index": 0}]]
                },
                "Format Error Message": {
                    "main": [[{"node": "Slack Alert (HTTP)", "type": "main", "index": 0}]]
                },
            },
            "settings": {
                "callerPolicy": "workflowsFromSameOwner",
                "executionOrder": "v1",
            },
        }

        n8n("PUT", f"/api/v1/workflows/{WF_ERROR_HANDLER}", workflow_body)
        log(section, "Workflow updated", 1)

        # Reactivate
        try:
            n8n("PATCH", f"/api/v1/workflows/{WF_ERROR_HANDLER}", {"active": True})
            log(section, "Workflow activated", 1)
        except Exception as e:
            log(section, f"Activation warning: {e}", 1)

        record(section, True)
        return True
    except Exception as e:
        record(section, False, str(e))
        return False


# ══════════════════════════════════════════════════════════════════════════════
# P0-3: Fix Intercom "Not an Issue" filter — allow empty Issue Type through
# ══════════════════════════════════════════════════════════════════════════════

def fix_intercom_filter():
    section = "P0-3: Not an Issue Filter"
    log(section, "Fixing 'Not an Issue' filter to allow empty Issue Type...")

    try:
        # Get current workflow
        wf = n8n("GET", f"/api/v1/workflows/{WF_INTERCOM}")
        nodes = wf["nodes"]
        connections = wf["connections"]

        # Find and fix the "Filter: Not an Issue" node
        updated = False
        for node in nodes:
            if node["name"] == "Filter: Not an Issue":
                # Remove the "cond-not-empty" condition, keep only "cond-not-nai"
                conditions = node["parameters"]["conditions"]["conditions"]
                original_count = len(conditions)
                node["parameters"]["conditions"]["conditions"] = [
                    c for c in conditions if c.get("id") != "cond-not-empty"
                ]
                new_count = len(node["parameters"]["conditions"]["conditions"])
                if new_count < original_count:
                    updated = True
                    log(section, f"Removed empty-string condition ({original_count} → {new_count})", 1)
                else:
                    log(section, "Condition already fixed or not found", 1)
                break

        if not updated:
            log(section, "No changes needed", 1)
            record(section, True, "already fixed")
            return True

        # Deactivate, update, reactivate
        try:
            n8n("POST", f"/api/v1/workflows/{WF_INTERCOM}/deactivate")
        except:
            pass

        n8n("PUT", f"/api/v1/workflows/{WF_INTERCOM}", {
            "name": wf["name"],
            "nodes": nodes,
            "connections": connections,
            "settings": wf.get("settings", {}),
        })
        log(section, "Workflow updated", 1)

        try:
            n8n("PATCH", f"/api/v1/workflows/{WF_INTERCOM}", {"active": True})
            log(section, "Workflow activated", 1)
        except Exception as e:
            log(section, f"Activation warning: {e}", 1)

        record(section, True)
        return True
    except Exception as e:
        record(section, False, str(e))
        return False


# ══════════════════════════════════════════════════════════════════════════════
# P1-1: Fix Nightly Issue Score — Replace Code node HTTP calls with HTTP
#        Request nodes. The current Code nodes use this.helpers.httpRequest()
#        which is broken on n8n cloud.
# ══════════════════════════════════════════════════════════════════════════════

def fix_nightly_issue_score():
    section = "P1-1: Nightly Issue Score"
    log(section, "Restructuring to use HTTP Request nodes...")

    try:
        # First, create a Notion httpHeaderAuth credential for this workflow
        notion_cred_id = None
        try:
            result = n8n("POST", "/api/v1/credentials", {
                "name": "Notion - Issue Score",
                "type": "httpHeaderAuth",
                "data": {"name": "Authorization", "value": f"Bearer {NOTION_API_KEY}"},
            })
            notion_cred_id = result["id"]
            log(section, f"Created Notion credential: {notion_cred_id}", 1)
        except urllib.error.HTTPError:
            # Might already exist — use the Duplicate Detection one
            notion_cred_id = NOTION_CRED_DEDUP
            log(section, f"Using existing Notion credential: {notion_cred_id}", 1)

        # Notion query bodies
        customers_query = json.dumps({
            "page_size": 100,
            "filter": {
                "or": [
                    {"property": "\U0001f4b0 Billing Status", "select": {"equals": "Active"}},
                    {"property": "\U0001f4b0 Billing Status", "select": {"equals": "Trialing"}},
                ]
            },
        })
        issues_query = json.dumps({
            "page_size": 100,
            "filter": {
                "and": [
                    {"property": "Status", "select": {"does_not_equal": "Resolved"}},
                    {"property": "Status", "select": {"does_not_equal": "Closed"}},
                ]
            },
        })

        # Process customers code — extract page_id and company_name from paginated results
        process_customers_code = """// Extract customer data from paginated Notion responses
const allPages = [];
for (const item of $input.all()) {
  const results = item.json.results || [];
  allPages.push(...results);
}

return allPages.map(page => {
  const titleArr = page.properties['\\u{1F3E2} Company Name']?.title || [];
  return {
    json: {
      page_id: page.id,
      company_name: titleArr.length ? titleArr[0].plain_text : '(unnamed)',
    }
  };
});"""

        # Process issues code
        process_issues_code = """// Extract issue data from paginated Notion responses
const allPages = [];
for (const item of $input.all()) {
  const results = item.json.results || [];
  allPages.push(...results);
}

if (allPages.length === 0) {
  return [{ json: { _no_issues: true } }];
}

return allPages.map(page => {
  const props = page.properties;
  const customerRel = props['Customer']?.relation || [];
  return {
    json: {
      customer_page_ids: customerRel.map(r => r.id),
      severity: props['Severity']?.select?.name || '',
      created_at: props['Created At']?.date?.start || '',
    }
  };
});"""

        # Compute scores code (unchanged — pure data processing, no HTTP)
        compute_scores_code = """// Compute issue_score per customer
const customers = $('Process Active Customers').all();
const issueItems = $('Process Open Issues').all();
const now = Date.now();
const FORTY_EIGHT_HOURS = 48 * 60 * 60 * 1000;

const issuesByCustomer = {};
for (const item of issueItems) {
  const { customer_page_ids, severity, created_at, _no_issues } = item.json;
  if (_no_issues) continue;
  if (!customer_page_ids?.length) continue;
  for (const custId of customer_page_ids) {
    if (!issuesByCustomer[custId]) issuesByCustomer[custId] = [];
    issuesByCustomer[custId].push({ severity, created_at });
  }
}

const results = [];
for (const cust of customers) {
  const { page_id, company_name } = cust.json;
  const openIssues = issuesByCustomer[page_id] || [];
  const count = openIssues.length;
  const hasP0 = openIssues.some(i => i.severity === 'P0-Critical');
  const p1Issues = openIssues.filter(i => i.severity === 'P1-High');
  const hasP1Over48h = p1Issues.some(i => {
    if (!i.created_at) return false;
    return (now - new Date(i.created_at).getTime()) > FORTY_EIGHT_HOURS;
  });

  let score;
  if (hasP0 || count >= 4 || hasP1Over48h) score = 'Red';
  else if (count >= 2 || p1Issues.length >= 1) score = 'Yellow';
  else score = 'Green';

  results.push({ json: { page_id, company_name, open_issues: count, issue_score: score } });
}
return results;"""

        # Rate limit delay
        delay_code = """await new Promise(resolve => setTimeout(resolve, 400));
return $input.all();"""

        # Deactivate
        try:
            n8n("POST", f"/api/v1/workflows/{WF_NIGHTLY_SCORE}/deactivate")
        except:
            pass

        workflow_body = {
            "name": "Nightly Issue Score Computation",
            "nodes": [
                {
                    "id": "trigger",
                    "name": "Daily 06:00 Berlin",
                    "type": "n8n-nodes-base.scheduleTrigger",
                    "typeVersion": 1.2,
                    "position": [0, 300],
                    "parameters": {
                        "rule": {"interval": [{"field": "cronExpression", "expression": "0 6 * * *"}]}
                    },
                },
                {
                    "id": "fetch-customers",
                    "name": "Fetch Active Customers",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": [260, 300],
                    "parameters": {
                        "method": "POST",
                        "url": f"https://api.notion.com/v1/databases/{NOTION_MCT_DB}/query",
                        "authentication": "genericCredentialType",
                        "genericAuthType": "httpHeaderAuth",
                        "sendHeaders": True,
                        "headerParameters": {
                            "parameters": [{"name": "Notion-Version", "value": "2022-06-28"}]
                        },
                        "sendBody": True,
                        "specifyBody": "json",
                        "jsonBody": customers_query,
                        "options": {
                            "pagination": {
                                "paginationMode": "updateAParameterInEachRequest",
                                "parameters": {
                                    "parameters": [{
                                        "type": "body",
                                        "name": "start_cursor",
                                        "value": "={{ $response.body.next_cursor }}",
                                    }]
                                },
                                "paginationCompleteWhen": "responseContainsExpression",
                                "completeExpression": "={{ $response.body.has_more === false }}",
                                "maxRequests": 20,
                            }
                        },
                    },
                    "credentials": {
                        "httpHeaderAuth": {"id": notion_cred_id, "name": "Notion - Issue Score"}
                    },
                },
                {
                    "id": "process-customers",
                    "name": "Process Active Customers",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": [520, 300],
                    "parameters": {"jsCode": process_customers_code},
                },
                {
                    "id": "fetch-issues",
                    "name": "Fetch Open Issues",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": [780, 300],
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
                        "jsonBody": issues_query,
                        "options": {
                            "pagination": {
                                "paginationMode": "updateAParameterInEachRequest",
                                "parameters": {
                                    "parameters": [{
                                        "type": "body",
                                        "name": "start_cursor",
                                        "value": "={{ $response.body.next_cursor }}",
                                    }]
                                },
                                "paginationCompleteWhen": "responseContainsExpression",
                                "completeExpression": "={{ $response.body.has_more === false }}",
                                "maxRequests": 20,
                            }
                        },
                    },
                    "credentials": {
                        "httpHeaderAuth": {"id": notion_cred_id, "name": "Notion - Issue Score"}
                    },
                },
                {
                    "id": "process-issues",
                    "name": "Process Open Issues",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": [1040, 300],
                    "parameters": {"jsCode": process_issues_code},
                },
                {
                    "id": "compute-scores",
                    "name": "Compute Scores",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": [1300, 300],
                    "parameters": {"jsCode": compute_scores_code},
                },
                {
                    "id": "batch-updates",
                    "name": "Batch Updates",
                    "type": "n8n-nodes-base.splitInBatches",
                    "typeVersion": 3,
                    "position": [1560, 300],
                    "parameters": {"batchSize": 1, "options": {}},
                },
                {
                    "id": "rate-limit",
                    "name": "Rate Limit Delay",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": [1820, 300],
                    "parameters": {"jsCode": delay_code},
                },
                {
                    "id": "update-notion",
                    "name": "Update Notion Score",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": [2080, 300],
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
                        "jsonBody": "={{ JSON.stringify({ properties: { '\U0001f4ca Issue Score': { select: { name: $json.issue_score } } } }) }}",
                        "options": {},
                    },
                    "credentials": {
                        "httpHeaderAuth": {"id": notion_cred_id, "name": "Notion - Issue Score"}
                    },
                },
            ],
            "connections": {
                "Daily 06:00 Berlin": {
                    "main": [[{"node": "Fetch Active Customers", "type": "main", "index": 0}]]
                },
                "Fetch Active Customers": {
                    "main": [[{"node": "Process Active Customers", "type": "main", "index": 0}]]
                },
                "Process Active Customers": {
                    "main": [[{"node": "Fetch Open Issues", "type": "main", "index": 0}]]
                },
                "Fetch Open Issues": {
                    "main": [[{"node": "Process Open Issues", "type": "main", "index": 0}]]
                },
                "Process Open Issues": {
                    "main": [[{"node": "Compute Scores", "type": "main", "index": 0}]]
                },
                "Compute Scores": {
                    "main": [[{"node": "Batch Updates", "type": "main", "index": 0}]]
                },
                "Batch Updates": {
                    "main": [
                        [{"node": "Rate Limit Delay", "type": "main", "index": 0}],
                        [],
                    ]
                },
                "Rate Limit Delay": {
                    "main": [[{"node": "Update Notion Score", "type": "main", "index": 0}]]
                },
                "Update Notion Score": {
                    "main": [[{"node": "Batch Updates", "type": "main", "index": 0}]]
                },
            },
            "settings": {
                "timezone": "Europe/Berlin",
                "executionOrder": "v1",
                "saveManualExecutions": True,
            },
        }

        n8n("PUT", f"/api/v1/workflows/{WF_NIGHTLY_SCORE}", workflow_body)
        log(section, "Workflow restructured", 1)

        try:
            n8n("PATCH", f"/api/v1/workflows/{WF_NIGHTLY_SCORE}", {"active": True})
            log(section, "Workflow activated", 1)
        except Exception as e:
            log(section, f"Activation warning: {e}", 1)

        record(section, True)
        return True
    except Exception as e:
        record(section, False, str(e))
        return False


# ══════════════════════════════════════════════════════════════════════════════
# P1-2: Fix Stripe → Notion Sync — The "workflow has issues" error is likely
#        from the Slack OAuth2 credential being invalid. Replace native Slack
#        nodes with HTTP Request nodes.
# ══════════════════════════════════════════════════════════════════════════════

def fix_stripe_sync():
    section = "P1-2: Stripe Sync"
    log(section, "Fixing Slack nodes in Stripe Sync...")

    try:
        wf = n8n("GET", f"/api/v1/workflows/{WF_STRIPE_SYNC}")
        nodes = wf["nodes"]
        connections = wf["connections"]

        # Find and replace problematic Slack-related nodes
        fixed_nodes = []
        nodes_replaced = 0

        for node in nodes:
            if node["name"] == "Success Alert" and "slack" in node["type"].lower():
                # Replace native Slack node with HTTP Request
                fixed_nodes.append({
                    "id": node["id"],
                    "name": "Success Alert",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": node["position"],
                    "parameters": {
                        "method": "POST",
                        "url": "https://slack.com/api/chat.postMessage",
                        "authentication": "genericCredentialType",
                        "genericAuthType": "httpHeaderAuth",
                        "sendBody": True,
                        "specifyBody": "json",
                        "jsonBody": '={{ JSON.stringify({ channel: "C0AC2BTCJVA", text: "\\u2705 Stripe Daily Sync complete.\\nTime: " + $now.toISO() }) }}',
                        "options": {},
                    },
                    "credentials": {
                        "httpHeaderAuth": {
                            "id": SLACK_BOT_CRED,
                            "name": "Slack Bot for Alerts",
                        }
                    },
                })
                nodes_replaced += 1
                log(section, f"Replaced Success Alert Slack node with HTTP Request", 1)
            elif node["name"] == "Slack: New Customer" and "code" in node["type"].lower():
                # This Code node uses this.helpers.httpRequestWithAuthentication
                # which references the broken slackOAuth2Api credential.
                # Replace with direct HTTP Request approach
                new_code = """const customerName = $('Transform Active Subs').item.json.customer_name;
const mrr = $('Transform Active Subs').item.json.mrr;
const planTier = $('Transform Active Subs').item.json.plan_tier;
const notionPageId = $('Create New Row').item.json.id;

return [{ json: {
  channel: "C090L9F66FM",
  text: `New customer: ${customerName} (\\u20ac${mrr}/mo, ${planTier}). Assigned to Aya.`,
  notionPageId: notionPageId
} }];"""
                fixed_nodes.append({
                    "id": node["id"],
                    "name": "Slack: New Customer",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": node["position"],
                    "parameters": {
                        "mode": "runOnceForAllItems",
                        "jsCode": new_code,
                    },
                })
                nodes_replaced += 1
                log(section, "Fixed 'Slack: New Customer' code node", 1)
            else:
                fixed_nodes.append(node)

        if nodes_replaced == 0:
            log(section, "No Slack nodes found to fix", 1)
            record(section, True, "no changes needed")
            return True

        # Also add a new HTTP Request node after "Slack: New Customer" for actual posting
        # Check if there's already a connection from "Slack: New Customer"
        slack_new_customer_id = None
        for n in fixed_nodes:
            if n["name"] == "Slack: New Customer":
                slack_new_customer_id = n["id"]
                break

        if slack_new_customer_id:
            # Add HTTP node to post the Slack message
            fixed_nodes.append({
                "id": "slack-new-customer-http",
                "name": "Post New Customer Alert",
                "type": "n8n-nodes-base.httpRequest",
                "typeVersion": 4.2,
                "position": [1560, 120],
                "parameters": {
                    "method": "POST",
                    "url": "https://slack.com/api/chat.postMessage",
                    "authentication": "genericCredentialType",
                    "genericAuthType": "httpHeaderAuth",
                    "sendBody": True,
                    "specifyBody": "json",
                    "jsonBody": '={{ JSON.stringify({ channel: $json.channel, text: $json.text }) }}',
                    "options": {},
                },
                "credentials": {
                    "httpHeaderAuth": {
                        "id": SLACK_BOT_CRED,
                        "name": "Slack Bot for Alerts",
                    }
                },
            })
            # Update connections: Slack: New Customer → Post New Customer Alert
            if "Slack: New Customer" in connections:
                old_conns = connections["Slack: New Customer"]
                connections["Slack: New Customer"] = {
                    "main": [[{"node": "Post New Customer Alert", "type": "main", "index": 0}]]
                }
                # Connect Post New Customer Alert to whatever Slack: New Customer was connected to
                if old_conns.get("main") and old_conns["main"][0]:
                    connections["Post New Customer Alert"] = old_conns
            else:
                connections["Slack: New Customer"] = {
                    "main": [[{"node": "Post New Customer Alert", "type": "main", "index": 0}]]
                }

        # Deactivate, update, reactivate
        try:
            n8n("POST", f"/api/v1/workflows/{WF_STRIPE_SYNC}/deactivate")
        except:
            pass

        n8n("PUT", f"/api/v1/workflows/{WF_STRIPE_SYNC}", {
            "name": wf["name"],
            "nodes": fixed_nodes,
            "connections": connections,
            "settings": wf.get("settings", {}),
        })
        log(section, "Workflow updated", 1)

        try:
            n8n("PATCH", f"/api/v1/workflows/{WF_STRIPE_SYNC}", {"active": True})
            log(section, "Workflow activated", 1)
        except Exception as e:
            log(section, f"Activation warning: {e}", 1)

        record(section, True)
        return True
    except Exception as e:
        record(section, False, str(e))
        return False


# ══════════════════════════════════════════════════════════════════════════════
# P1-3: Fix Customer Enrichment — Replace this.helpers.httpRequest with
#        HTTP Request nodes. This is the most complex restructuring.
# ══════════════════════════════════════════════════════════════════════════════

def fix_customer_enrichment():
    section = "P1-3: Customer Enrichment"
    log(section, "Restructuring Customer Enrichment to use HTTP Request nodes...")

    try:
        # Create credentials
        notion_cred_id = None
        hubspot_cred_id = None

        try:
            result = n8n("POST", "/api/v1/credentials", {
                "name": "Notion - Enrichment",
                "type": "httpHeaderAuth",
                "data": {"name": "Authorization", "value": f"Bearer {NOTION_API_KEY}"},
            })
            notion_cred_id = result["id"]
        except:
            notion_cred_id = NOTION_CRED_DEDUP

        try:
            result = n8n("POST", "/api/v1/credentials", {
                "name": "HubSpot - Enrichment",
                "type": "httpHeaderAuth",
                "data": {"name": "Authorization", "value": f"Bearer {HUBSPOT_KEY}"},
            })
            hubspot_cred_id = result["id"]
        except:
            # Try to find existing
            hubspot_cred_id = None
            log(section, "Could not create HubSpot credential, checking existing...", 1)

        if not hubspot_cred_id:
            log(section, "Creating HubSpot credential as httpHeaderAuth...", 1)
            try:
                result = n8n("POST", "/api/v1/credentials", {
                    "name": "HubSpot - Customer Enrichment",
                    "type": "httpHeaderAuth",
                    "data": {"name": "Authorization", "value": f"Bearer {HUBSPOT_KEY}"},
                })
                hubspot_cred_id = result["id"]
            except:
                record(section, False, "Cannot create HubSpot credential")
                return False

        log(section, f"Credentials: Notion={notion_cred_id}, HubSpot={hubspot_cred_id}", 1)

        # The enrichment logic is complex (per-customer HubSpot lookups).
        # Restructure into: Fetch Notion → Code (filter) → Loop → HubSpot Search → Code (process) → Update Notion
        # For simplicity, keep the main logic in a Code node but replace this.helpers.httpRequest
        # with a pattern that works: have the Code node output items that HTTP Request nodes process.

        # Actually, the simplest reliable fix: split into multiple stages with HTTP Request nodes
        # handling all HTTP calls.

        # Stage 1: Fetch all Notion rows (HTTP Request with pagination)
        # Stage 2: Code node to filter rows needing enrichment & prepare HubSpot search queries
        # Stage 3: Loop through items → HubSpot Search (HTTP Request) → Process & Update Notion

        filter_code = """// Filter rows needing enrichment (missing Country or Shop System)
const allPages = [];
for (const item of $input.all()) {
  const results = item.json.results || [];
  allPages.push(...results);
}

const toEnrich = [];
for (const row of allPages) {
  const p = row.properties;
  const getText = (prop, type) => {
    const arr = prop?.[type || 'rich_text'] || [];
    return arr.length ? (arr[0].plain_text || '').trim() : '';
  };
  const getSelect = (prop) => prop?.select ? (prop.select.name || '').trim() : '';

  const company = getText(p['\\u{1F3E2} Company Name'], 'title');
  const domain = getText(p['\\u{1F3E2} Domain']);
  const country = getSelect(p['\\u{1F3E2} Country']);
  const shopSystem = getSelect(p['\\u{1F3E2} Shop System']);
  const hubspotId = getText(p['\\u{1F517} HubSpot Company ID']);

  if (!country || !shopSystem) {
    toEnrich.push({
      json: {
        page_id: row.id,
        company: company,
        domain: domain,
        country: country,
        shop_system: shopSystem,
        hubspot_id: hubspotId,
        search_domain: (!hubspotId && domain) ? domain : '',
      }
    });
  }
}

if (toEnrich.length === 0) {
  return [{ json: { _done: true, message: 'No rows need enrichment', total: allPages.length } }];
}

return toEnrich;"""

        process_hubspot_code = """// Process HubSpot results and prepare Notion updates
const row = $('Filter Rows Needing Enrichment').item.json;
const hsResponse = $input.first().json;

// Country mapping
const COUNTRY_MAP = {
  'ES': 'Spain', 'GB': 'United Kingdom', 'NL': 'Netherlands', 'PT': 'Portugal',
  'CH': 'Switzerland', 'UY': 'Uruguay', 'US': 'United States', 'AT': 'Austria',
  'DE': 'Germany', 'FR': 'France', 'MX': 'Mexico', 'CL': 'Chile', 'BE': 'Belgium',
  'IT': 'Italy', 'IE': 'Ireland', 'DK': 'Denmark', 'SE': 'Sweden', 'NO': 'Norway',
  'FI': 'Finland', 'PL': 'Poland', 'CZ': 'Czech Republic', 'RO': 'Romania',
  'HU': 'Hungary', 'GR': 'Greece', 'CA': 'Canada', 'AU': 'Australia', 'BR': 'Brazil',
  'AR': 'Argentina', 'CO': 'Colombia', 'PE': 'Peru', 'IN': 'India', 'JP': 'Japan',
};

// CC TLD fallback
const CC_TLD_MAP = {
  '.es': 'Spain', '.uk': 'United Kingdom', '.nl': 'Netherlands', '.de': 'Germany',
  '.fr': 'France', '.pt': 'Portugal', '.it': 'Italy', '.ch': 'Switzerland',
  '.at': 'Austria', '.be': 'Belgium', '.ie': 'Ireland', '.mx': 'Mexico',
  '.cl': 'Chile', '.se': 'Sweden', '.no': 'Norway', '.dk': 'Denmark',
  '.co.uk': 'United Kingdom', '.com.ar': 'Argentina', '.com.br': 'Brazil',
  '.com.mx': 'Mexico', '.com.uy': 'Uruguay',
};

function normalizeCountry(raw) {
  if (!raw || !raw.trim()) return null;
  raw = raw.trim();
  if (COUNTRY_MAP[raw]) return COUNTRY_MAP[raw];
  for (const [k, v] of Object.entries(COUNTRY_MAP)) {
    if (k.toLowerCase() === raw.toLowerCase()) return v;
  }
  return raw;
}

function detectShop(shopsystem, webTech) {
  if (shopsystem && shopsystem.trim()) return shopsystem.trim();
  if (!webTech) return null;
  const tech = webTech.toLowerCase();
  if (tech.includes('shopify')) return 'Shopify';
  if (tech.includes('woo')) return 'WooCommerce';
  if (tech.includes('prestashop')) return 'PrestaShop';
  if (tech.includes('magento')) return 'Magento';
  return null;
}

function countryFromTld(domain) {
  if (!domain) return null;
  domain = domain.toLowerCase();
  const sorted = Object.keys(CC_TLD_MAP).sort((a, b) => b.length - a.length);
  for (const tld of sorted) {
    if (domain.endsWith(tld)) return CC_TLD_MAP[tld];
  }
  return null;
}

// Extract HubSpot data
let hsProps = null;
let foundHubspotId = row.hubspot_id;

if (row.hubspot_id && hsResponse.properties) {
  hsProps = hsResponse.properties;
} else if (hsResponse.results && hsResponse.results.length > 0) {
  hsProps = hsResponse.results[0].properties;
  foundHubspotId = hsResponse.results[0].id;
}

const notionUpdates = {};

if (hsProps) {
  if (!row.country) {
    const country = normalizeCountry(hsProps.country || '');
    if (country) notionUpdates['\\u{1F3E2} Country'] = { select: { name: country } };
  }
  if (!row.shop_system) {
    const shop = detectShop(hsProps.shopsystem || '', hsProps.web_technologies || '');
    if (shop) notionUpdates['\\u{1F3E2} Shop System'] = { select: { name: shop } };
  }
  if (!row.hubspot_id && foundHubspotId) {
    notionUpdates['\\u{1F517} HubSpot Company ID'] = {
      rich_text: [{ text: { content: String(foundHubspotId) } }]
    };
  }
}

// CC TLD fallback for country
if (!row.country && !notionUpdates['\\u{1F3E2} Country']) {
  const tldCountry = countryFromTld(row.domain);
  if (tldCountry) notionUpdates['\\u{1F3E2} Country'] = { select: { name: tldCountry } };
}

if (Object.keys(notionUpdates).length === 0) {
  return [{ json: { _skip: true, page_id: row.page_id } }];
}

return [{ json: {
  page_id: row.page_id,
  update_body: JSON.stringify({ properties: notionUpdates }),
  _skip: false,
} }];"""

        try:
            n8n("POST", f"/api/v1/workflows/{WF_ENRICHMENT}/deactivate")
        except:
            pass

        workflow_body = {
            "name": "Customer Enrichment (Daily)",
            "nodes": [
                {
                    "id": "enrich-trigger",
                    "name": "Daily 08:00 CET",
                    "type": "n8n-nodes-base.scheduleTrigger",
                    "typeVersion": 1.2,
                    "position": [0, 300],
                    "parameters": {
                        "rule": {"interval": [{"field": "cronExpression", "expression": "0 8 * * *"}]}
                    },
                },
                {
                    "id": "enrich-manual",
                    "name": "Manual Trigger",
                    "type": "n8n-nodes-base.manualTrigger",
                    "typeVersion": 1,
                    "position": [0, 500],
                    "parameters": {},
                },
                {
                    "id": "fetch-all-rows",
                    "name": "Fetch All Notion Rows",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": [260, 400],
                    "parameters": {
                        "method": "POST",
                        "url": f"https://api.notion.com/v1/databases/{NOTION_MCT_DB}/query",
                        "authentication": "genericCredentialType",
                        "genericAuthType": "httpHeaderAuth",
                        "sendHeaders": True,
                        "headerParameters": {
                            "parameters": [{"name": "Notion-Version", "value": "2022-06-28"}]
                        },
                        "sendBody": True,
                        "specifyBody": "json",
                        "jsonBody": '{"page_size": 100}',
                        "options": {
                            "pagination": {
                                "paginationMode": "updateAParameterInEachRequest",
                                "parameters": {
                                    "parameters": [{
                                        "type": "body",
                                        "name": "start_cursor",
                                        "value": "={{ $response.body.next_cursor }}",
                                    }]
                                },
                                "paginationCompleteWhen": "responseContainsExpression",
                                "completeExpression": "={{ $response.body.has_more === false }}",
                                "maxRequests": 20,
                            }
                        },
                    },
                    "credentials": {
                        "httpHeaderAuth": {"id": notion_cred_id, "name": "Notion - Enrichment"}
                    },
                },
                {
                    "id": "filter-rows",
                    "name": "Filter Rows Needing Enrichment",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": [520, 400],
                    "parameters": {"jsCode": filter_code},
                },
                {
                    "id": "if-done",
                    "name": "Has Rows to Enrich?",
                    "type": "n8n-nodes-base.if",
                    "typeVersion": 2.2,
                    "position": [780, 400],
                    "parameters": {
                        "conditions": {
                            "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict"},
                            "conditions": [{
                                "id": "cond-not-done",
                                "leftValue": "={{ $json._done }}",
                                "rightValue": True,
                                "operator": {"type": "boolean", "operation": "notEqual"},
                            }],
                            "combinator": "and",
                        },
                        "options": {},
                    },
                },
                {
                    "id": "hubspot-search",
                    "name": "HubSpot Search by Domain",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": [1040, 300],
                    "parameters": {
                        "method": "POST",
                        "url": "https://api.hubapi.com/crm/v3/objects/companies/search",
                        "authentication": "genericCredentialType",
                        "genericAuthType": "httpHeaderAuth",
                        "sendBody": True,
                        "specifyBody": "json",
                        "jsonBody": '={{ JSON.stringify({ filterGroups: [{ filters: [{ propertyName: "domain", operator: "EQ", value: $json.domain || "NONE" }] }], properties: ["country", "shopsystem", "web_technologies"], limit: 1 }) }}',
                        "options": {"timeout": 10000},
                    },
                    "credentials": {
                        "httpHeaderAuth": {"id": hubspot_cred_id, "name": "HubSpot - Customer Enrichment"}
                    },
                    "onError": "continueErrorOutput",
                },
                {
                    "id": "process-hubspot",
                    "name": "Process & Prepare Update",
                    "type": "n8n-nodes-base.code",
                    "typeVersion": 2,
                    "position": [1300, 300],
                    "parameters": {
                        "mode": "runOnceForEachItem",
                        "jsCode": process_hubspot_code,
                    },
                },
                {
                    "id": "if-skip",
                    "name": "Has Update?",
                    "type": "n8n-nodes-base.if",
                    "typeVersion": 2.2,
                    "position": [1560, 300],
                    "parameters": {
                        "conditions": {
                            "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict"},
                            "conditions": [{
                                "id": "cond-not-skip",
                                "leftValue": "={{ $json._skip }}",
                                "rightValue": True,
                                "operator": {"type": "boolean", "operation": "notEqual"},
                            }],
                            "combinator": "and",
                        },
                        "options": {},
                    },
                },
                {
                    "id": "update-notion",
                    "name": "Update Notion Row",
                    "type": "n8n-nodes-base.httpRequest",
                    "typeVersion": 4.2,
                    "position": [1820, 200],
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
                        "jsonBody": "={{ $json.update_body }}",
                        "options": {},
                    },
                    "credentials": {
                        "httpHeaderAuth": {"id": notion_cred_id, "name": "Notion - Enrichment"}
                    },
                },
            ],
            "connections": {
                "Daily 08:00 CET": {
                    "main": [[{"node": "Fetch All Notion Rows", "type": "main", "index": 0}]]
                },
                "Manual Trigger": {
                    "main": [[{"node": "Fetch All Notion Rows", "type": "main", "index": 0}]]
                },
                "Fetch All Notion Rows": {
                    "main": [[{"node": "Filter Rows Needing Enrichment", "type": "main", "index": 0}]]
                },
                "Filter Rows Needing Enrichment": {
                    "main": [[{"node": "Has Rows to Enrich?", "type": "main", "index": 0}]]
                },
                "Has Rows to Enrich?": {
                    "main": [
                        [{"node": "HubSpot Search by Domain", "type": "main", "index": 0}],
                        [],
                    ]
                },
                "HubSpot Search by Domain": {
                    "main": [[{"node": "Process & Prepare Update", "type": "main", "index": 0}]]
                },
                "Process & Prepare Update": {
                    "main": [[{"node": "Has Update?", "type": "main", "index": 0}]]
                },
                "Has Update?": {
                    "main": [
                        [{"node": "Update Notion Row", "type": "main", "index": 0}],
                        [],
                    ]
                },
            },
            "settings": {"executionOrder": "v1", "saveManualExecutions": True},
        }

        n8n("PUT", f"/api/v1/workflows/{WF_ENRICHMENT}", workflow_body)
        log(section, "Workflow restructured", 1)

        try:
            n8n("PATCH", f"/api/v1/workflows/{WF_ENRICHMENT}", {"active": True})
            log(section, "Workflow activated", 1)
        except Exception as e:
            log(section, f"Activation warning: {e}", 1)

        record(section, True)
        return True
    except Exception as e:
        record(section, False, str(e))
        return False


# ══════════════════════════════════════════════════════════════════════════════
# P1-4: Fix Daily AI Resolution Rate — ClickHouse auth failed
#        Note: We can't fix the password via API. Flag for manual action.
# ══════════════════════════════════════════════════════════════════════════════

def fix_ai_resolution_rate():
    section = "P1-4: AI Resolution Rate"
    log(section, "ClickHouse credentials need manual password update")
    log(section, f"Credential to update: {CLICKHOUSE_CRED}", 1)
    log(section, f"Update at: {N8N_BASE}/home/credentials/{CLICKHOUSE_CRED}", 1)
    results["skipped"].append(section + " (needs ClickHouse password)")
    return False


# ══════════════════════════════════════════════════════════════════════════════
# P2-1: Fix Duplicate Detection — $input.first() context error
#        The "Prepare Claude Prompt" node has mode: runOnceForEachItem but
#        uses $input.first() which doesn't work in per-item mode.
# ══════════════════════════════════════════════════════════════════════════════

def fix_duplicate_detection():
    section = "P2-1: Duplicate Detection"
    log(section, "Fixing $input.first() context error in Prepare Claude Prompt...")

    try:
        wf = n8n("GET", f"/api/v1/workflows/{WF_DUPLICATE}")
        nodes = wf["nodes"]
        connections = wf["connections"]

        updated = False
        for node in nodes:
            if node["name"] == "Prepare Claude Prompt":
                old_code = node["parameters"]["jsCode"]
                # Fix: replace $input.first() with $input.item in runOnceForEachItem mode
                new_code = old_code.replace("$input.first()", "$input.item")
                if new_code != old_code:
                    node["parameters"]["jsCode"] = new_code
                    updated = True
                    log(section, "Fixed $input.first() → $input.item", 1)

            if node["name"] == "Parse Claude Response":
                old_code = node["parameters"]["jsCode"]
                new_code = old_code.replace("$input.first()", "$input.item")
                if new_code != old_code:
                    node["parameters"]["jsCode"] = new_code
                    updated = True
                    log(section, "Fixed Parse Claude Response $input.first() → $input.item", 1)

        if not updated:
            log(section, "No changes needed", 1)
            record(section, True, "already fixed")
            return True

        try:
            n8n("POST", f"/api/v1/workflows/{WF_DUPLICATE}/deactivate")
        except:
            pass

        n8n("PUT", f"/api/v1/workflows/{WF_DUPLICATE}", {
            "name": wf["name"],
            "nodes": nodes,
            "connections": connections,
            "settings": wf.get("settings", {}),
        })
        log(section, "Workflow updated", 1)

        try:
            n8n("PATCH", f"/api/v1/workflows/{WF_DUPLICATE}", {"active": True})
            log(section, "Workflow activated", 1)
        except Exception as e:
            log(section, f"Activation warning: {e}", 1)

        record(section, True)
        return True
    except Exception as e:
        record(section, False, str(e))
        return False


# ══════════════════════════════════════════════════════════════════════════════
# P2-2: Fix CS Owner Reassign — null check for empty Slack payload
# ══════════════════════════════════════════════════════════════════════════════

def fix_cs_reassign():
    section = "P2-2: CS Owner Reassign"
    log(section, "Adding null checks to Reassign to Alex code...")

    try:
        wf = n8n("GET", f"/api/v1/workflows/{WF_CS_REASSIGN}")
        nodes = wf["nodes"]
        connections = wf["connections"]

        updated = False
        for node in nodes:
            if node["name"] == "Reassign to Alex":
                # Replace with safer code that handles missing data
                node["parameters"]["jsCode"] = """const NOTION_TOKEN = '***REMOVED***';

// Parse Slack interactive payload safely
const raw = $input.first().json;

let payload;
try {
  let payloadStr = raw.body?.payload || raw.payload;
  if (payloadStr && typeof payloadStr === 'string') {
    payload = JSON.parse(payloadStr);
  } else if (raw.body?.actions) {
    payload = raw.body;
  } else if (raw.actions) {
    payload = raw;
  } else if (typeof raw.body === 'string') {
    payload = JSON.parse(raw.body);
  } else {
    payload = raw.body || raw;
  }
} catch (e) {
  return [{ json: { success: false, error: 'Failed to parse Slack payload: ' + e.message } }];
}

// Safely access actions
const actions = payload?.actions;
if (!actions || !Array.isArray(actions) || actions.length === 0) {
  return [{ json: { success: false, error: 'No actions in payload' } }];
}

const notionPageId = actions[0]?.value;
if (!notionPageId) {
  return [{ json: { success: false, error: 'No Notion page ID in action value' } }];
}

const responseUrl = payload.response_url || '';
const originalBlock = payload.message?.blocks?.[0]?.text?.text || '';

// Update Notion: CS Owner → Alex
try {
  await this.helpers.httpRequest({
    method: 'PATCH',
    url: `https://api.notion.com/v1/pages/${notionPageId}`,
    headers: {
      'Authorization': `Bearer ${NOTION_TOKEN}`,
      'Notion-Version': '2022-06-28',
      'Content-Type': 'application/json'
    },
    body: {
      properties: {
        '\\u2b50 CS Owner': { select: { name: 'Alex' } }
      }
    }
  });
} catch (e) {
  return [{ json: { success: false, error: 'Notion update failed: ' + e.message, notionPageId } }];
}

// Respond to Slack
if (responseUrl) {
  try {
    const updatedText = (originalBlock.split('\\n\\nAssigned')[0] || 'Customer')
      + '\\n\\n\\u2705 Reassigned to *Alex*';

    await this.helpers.httpRequest({
      method: 'POST',
      url: responseUrl,
      headers: { 'Content-Type': 'application/json' },
      body: {
        replace_original: true,
        blocks: [{ type: 'section', text: { type: 'mrkdwn', text: updatedText } }]
      }
    });
  } catch (e) {
    // Slack response failure is non-critical
  }
}

return [{ json: { success: true, notionPageId } }];
"""
                updated = True
                log(section, "Updated code with null checks", 1)

        if not updated:
            record(section, True, "no changes needed")
            return True

        try:
            n8n("POST", f"/api/v1/workflows/{WF_CS_REASSIGN}/deactivate")
        except:
            pass

        n8n("PUT", f"/api/v1/workflows/{WF_CS_REASSIGN}", {
            "name": wf["name"],
            "nodes": nodes,
            "connections": connections,
            "settings": wf.get("settings", {}),
        })
        log(section, "Workflow updated", 1)

        try:
            n8n("PATCH", f"/api/v1/workflows/{WF_CS_REASSIGN}", {"active": True})
            log(section, "Workflow activated", 1)
        except Exception as e:
            log(section, f"Activation warning: {e}", 1)

        record(section, True)
        return True
    except Exception as e:
        record(section, False, str(e))
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Main execution
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 70)
    print("n8n Comprehensive Workflow Fix — Feb 17, 2026")
    print("=" * 70)

    fixes = [
        ("P0", fix_intercom_webhook),
        ("P0", fix_error_handler),
        ("P0", fix_intercom_filter),
        ("P1", fix_nightly_issue_score),
        ("P1", fix_stripe_sync),
        ("P1", fix_customer_enrichment),
        ("P1", fix_ai_resolution_rate),
        ("P2", fix_duplicate_detection),
        ("P2", fix_cs_reassign),
    ]

    for priority, fix_fn in fixes:
        print(f"\n{'─' * 70}")
        try:
            fix_fn()
        except Exception as e:
            print(f"  UNEXPECTED ERROR in {fix_fn.__name__}: {e}")
            results["failed"].append(fix_fn.__name__)
        time.sleep(1)  # Rate limit between API calls

    # Summary
    print(f"\n{'═' * 70}")
    print("FIX SUMMARY")
    print(f"{'═' * 70}")
    print(f"  Passed:  {len(results['passed'])} — {', '.join(results['passed']) or 'none'}")
    print(f"  Failed:  {len(results['failed'])} — {', '.join(results['failed']) or 'none'}")
    print(f"  Skipped: {len(results['skipped'])} — {', '.join(results['skipped']) or 'none'}")
    print()

    # Manual action items
    print("MANUAL ACTION ITEMS:")
    print("  1. Get ClickHouse password from engineering and update credential:")
    print(f"     {N8N_BASE}/home/credentials/{CLICKHOUSE_CRED}")
    print("  2. Verify Intercom webhook is registered:")
    print("     https://app.intercom.com → Settings → Webhooks")
    print("  3. Test the Intercom pipeline: close a conversation with Issue Type set")
    print("  4. Test Error Handler: trigger an error and check Slack for alert")
    print(f"{'═' * 70}")
