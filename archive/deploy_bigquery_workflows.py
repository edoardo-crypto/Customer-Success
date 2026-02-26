#!/usr/bin/env python3
"""
Phase 1: Create 'Backfill - ClickHouse to BigQuery' (manual trigger, no activation).
Phase 2: Fix weekly workflow Jlmx2An3mRolraS3:
  - Schedule trigger → cron '0 4 * * 1' (Monday 04:00)
  - SQL → add 4 missing columns
  - Code node → full 20-column parse/cast
  - Settings → timezone: Europe/Berlin
  - Re-activate
"""

import json
import urllib.request
import urllib.error
import ssl
import sys

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = (
    "***REMOVED_JWT***"
    ".eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJpc3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleHAiOjE3NzMyNzAwMDB9"
    ".X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)

CLICKHOUSE_URL = "https://ua2wi80os4.eu-central-1.aws.clickhouse.cloud:8443/"
CLICKHOUSE_CRED_ID   = "kionhtTQSKGgcIYt"
CLICKHOUSE_CRED_NAME = "ClickHouse - AI Resolution Sync"

BIGQUERY_CRED_ID   = "o2dKwUuc5DSzmiou"
BIGQUERY_CRED_NAME = "Google BigQuery Service Account"

WEEKLY_WORKFLOW_ID = "Jlmx2An3mRolraS3"

# ---------------------------------------------------------------------------
# SQL Queries (FORMAT JSON — proven to work; Code node extracts .data[])
# ---------------------------------------------------------------------------

_SELECT_COLS = (
    "stripe_customer_id, org_id, workspace_id, "
    "toMonday(toDate(created_at)) AS week_start, "
    "avg(ai_resolution_rate) AS avg_ai_resolution_rate, "
    "avg(ai_sessions_total) AS avg_ai_sessions_total, "
    "avg(ai_sessions_count) AS avg_ai_sessions_count, "
    "avg(ai_sessions_resolved) AS avg_ai_sessions_resolved, "
    "avg(ai_sessions_unresolved) AS avg_ai_sessions_unresolved, "
    "avg(active_skills_count) AS avg_active_skills_count, "
    "avg(active_processes_count) AS avg_active_processes_count, "
    "avg(custom_replies_count) AS avg_custom_replies_count, "
    "avg(channels_connected_count) AS avg_channels_connected_count, "
    "avg(channels_with_ai_count) AS avg_channels_with_ai_count, "
    "avg(test_scenarios_count) AS avg_test_scenarios_count, "
    "avg(open_tickets_count) AS avg_open_tickets_count, "
    "avg(messages_sent24h) AS avg_messages_sent24h, "
    "avg(messages_received24h) AS avg_messages_received24h, "
    "count() AS data_points"
)
_FROM = "FROM operator.public_workspace_report_snapshot"
_GROUP = "GROUP BY stripe_customer_id, org_id, workspace_id, week_start FORMAT JSON"

BACKFILL_SQL = f"SELECT {_SELECT_COLS} {_FROM} {_GROUP}"

WEEKLY_SQL = (
    f"SELECT {_SELECT_COLS} {_FROM} "
    "WHERE toDate(created_at) >= toMonday(today()) - 7 "
    "AND toDate(created_at) < toMonday(today()) "
    f"{_GROUP}"
)

# ---------------------------------------------------------------------------
# Code node JS — shared between backfill and weekly
# All ClickHouse numeric values arrive as strings; explicit casts required.
# ---------------------------------------------------------------------------

PARSE_CAST_CODE = """\
const response = $input.first().json;
const rows = response.data || [];

if (!Array.isArray(rows) || rows.length === 0) {
  throw new Error('ClickHouse returned no data.');
}

const now = new Date().toISOString();
return rows.map(row => ({
  json: {
    stripe_customer_id:           String(row.stripe_customer_id),
    org_id:                       String(row.org_id),
    workspace_id:                 String(row.workspace_id),
    week_start:                   String(row.week_start),
    avg_ai_resolution_rate:       parseFloat(row.avg_ai_resolution_rate)       || 0,
    avg_ai_sessions_total:        parseFloat(row.avg_ai_sessions_total)        || 0,
    avg_ai_sessions_count:        parseFloat(row.avg_ai_sessions_count)        || 0,
    avg_ai_sessions_resolved:     parseFloat(row.avg_ai_sessions_resolved)     || 0,
    avg_ai_sessions_unresolved:   parseFloat(row.avg_ai_sessions_unresolved)   || 0,
    avg_active_skills_count:      parseFloat(row.avg_active_skills_count)      || 0,
    avg_active_processes_count:   parseFloat(row.avg_active_processes_count)   || 0,
    avg_custom_replies_count:     parseFloat(row.avg_custom_replies_count)     || 0,
    avg_channels_connected_count: parseFloat(row.avg_channels_connected_count) || 0,
    avg_channels_with_ai_count:   parseFloat(row.avg_channels_with_ai_count)   || 0,
    avg_test_scenarios_count:     parseFloat(row.avg_test_scenarios_count)     || 0,
    avg_open_tickets_count:       parseFloat(row.avg_open_tickets_count)       || 0,
    avg_messages_sent24h:         parseFloat(row.avg_messages_sent24h)         || 0,
    avg_messages_received24h:     parseFloat(row.avg_messages_received24h)     || 0,
    data_points:                  parseInt(row.data_points, 10)                || 0,
    ingested_at:                  now,
  }
}));
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
        print(f"  HTTP {e.code} for {method} {path}: {body_text[:500]}")
        raise


def clickhouse_node(node_id, name, sql, position):
    return {
        "parameters": {
            "method": "POST",
            "url": CLICKHOUSE_URL,
            "authentication": "genericCredentialType",
            "genericAuthType": "httpBasicAuth",
            "sendBody": True,
            "contentType": "raw",
            "rawContentType": "text/plain",
            "body": sql,
            "options": {"timeout": 60000},
        },
        "id": node_id,
        "name": name,
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position": position,
        "credentials": {
            "httpBasicAuth": {
                "id": CLICKHOUSE_CRED_ID,
                "name": CLICKHOUSE_CRED_NAME,
            }
        },
    }


def code_node(node_id, name, code, position):
    return {
        "parameters": {
            "mode": "runOnceForAllItems",
            "jsCode": code,
        },
        "id": node_id,
        "name": name,
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": position,
    }


def bigquery_node(node_id, name, position):
    return {
        "parameters": {
            "authentication": "serviceAccount",
            "projectId": {"__rl": True, "value": "konvoai-n8n", "mode": "id"},
            "datasetId": {"__rl": True, "value": "konvoai_analytics", "mode": "id"},
            "tableId":   {"__rl": True, "value": "customer_kpis_weekly", "mode": "id"},
            "columns":   {"mappingMode": "autoMapInputData", "value": {}},
            "options":   {},
        },
        "id": node_id,
        "name": name,
        "type": "n8n-nodes-base.googleBigQuery",
        "typeVersion": 2,
        "position": position,
        "credentials": {
            "googleApi": {
                "id": BIGQUERY_CRED_ID,
                "name": BIGQUERY_CRED_NAME,
            }
        },
    }


# ===========================================================================
# Phase 1 — Create Backfill Workflow
# ===========================================================================

print("=" * 60)
print("Phase 1: Create 'Backfill - ClickHouse to BigQuery'")
print("=" * 60)

backfill_wf = {
    "name": "Backfill - ClickHouse to BigQuery",
    "nodes": [
        {
            "parameters": {},
            "id": "backfill-trigger",
            "name": "Manual Trigger",
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "position": [0, 300],
        },
        clickhouse_node("backfill-ch", "Query ClickHouse", BACKFILL_SQL, [260, 300]),
        code_node("backfill-code", "Parse, Cast & Timestamp", PARSE_CAST_CODE, [520, 300]),
        bigquery_node("backfill-bq", "Insert to BigQuery", [780, 300]),
    ],
    "connections": {
        "Manual Trigger": {
            "main": [[{"node": "Query ClickHouse", "type": "main", "index": 0}]]
        },
        "Query ClickHouse": {
            "main": [[{"node": "Parse, Cast & Timestamp", "type": "main", "index": 0}]]
        },
        "Parse, Cast & Timestamp": {
            "main": [[{"node": "Insert to BigQuery", "type": "main", "index": 0}]]
        },
    },
    "settings": {"executionOrder": "v1"},
}

try:
    result = n8n_request("POST", "/api/v1/workflows", backfill_wf)
    backfill_id = result["id"]
    print(f"  Created: id={backfill_id}")
    print(f"  URL: {N8N_BASE}/workflow/{backfill_id}")
    print("  NOT activating (manual trigger — run from UI)")
except urllib.error.HTTPError:
    print("  FATAL: Could not create backfill workflow")
    sys.exit(1)


# ===========================================================================
# Phase 2 — Fix Weekly Workflow Jlmx2An3mRolraS3
# ===========================================================================

print()
print("=" * 60)
print(f"Phase 2: Fix Weekly Workflow {WEEKLY_WORKFLOW_ID}")
print("=" * 60)

# --- Fetch ---
print("  Fetching current workflow...")
try:
    wf = n8n_request("GET", f"/api/v1/workflows/{WEEKLY_WORKFLOW_ID}")
except urllib.error.HTTPError:
    print("  FATAL: Could not fetch weekly workflow")
    sys.exit(1)

print(f"  Name: {wf['name']}")
print(f"  Nodes ({len(wf['nodes'])}):")
for n in wf["nodes"]:
    print(f"    [{n['type'].split('.')[-1]}] '{n['name']}' id={n['id']}")

# --- Deactivate ---
try:
    n8n_request("POST", f"/api/v1/workflows/{WEEKLY_WORKFLOW_ID}/deactivate")
    print("  Deactivated for patching")
except Exception:
    print("  (Skipping deactivate — may already be inactive)")

# --- Apply fixes ---
nodes = wf["nodes"]
fixed = {"schedule": False, "clickhouse": False, "code": False, "bigquery": False}

for node in nodes:
    ntype = node["type"]

    # Fix 1: Schedule Trigger → cron 0 4 * * 1
    if ntype == "n8n-nodes-base.scheduleTrigger":
        print(f"  [Fix 1] Schedule Trigger '{node['name']}'")
        node["parameters"] = {
            "rule": {
                "interval": [{
                    "field": "cronExpression",
                    "expression": "0 4 * * 1",
                }]
            }
        }
        print("    → cron: 0 4 * * 1  (Monday 04:00 Europe/Berlin)")
        fixed["schedule"] = True

    # Fix 2+3: ClickHouse HTTP Request → updated SQL + correct format
    elif ntype == "n8n-nodes-base.httpRequest":
        print(f"  [Fix 2] HTTP Request node '{node['name']}'")
        node["parameters"]["body"] = WEEKLY_SQL
        node["parameters"]["contentType"] = "raw"
        node["parameters"]["rawContentType"] = "text/plain"
        node["parameters"]["sendBody"] = True
        node["parameters"]["options"] = {"timeout": 60000}
        node["credentials"] = {
            "httpBasicAuth": {
                "id": CLICKHOUSE_CRED_ID,
                "name": CLICKHOUSE_CRED_NAME,
            }
        }
        print("    → SQL updated (all 18 metric columns + FORMAT JSON)")
        print("    → contentType: raw, timeout: 60s")
        fixed["clickhouse"] = True

    # Fix 3: Code node → full 20-column parse/cast
    elif ntype == "n8n-nodes-base.code":
        print(f"  [Fix 3] Code node '{node['name']}'")
        node["parameters"]["jsCode"] = PARSE_CAST_CODE
        node["parameters"]["mode"] = "runOnceForAllItems"
        print("    → Full 20-column parse/cast code (runOnceForAllItems)")
        fixed["code"] = True

    # Fix 4: BigQuery → ensure correct credential key
    elif ntype == "n8n-nodes-base.googleBigQuery":
        print(f"  [Fix 4] BigQuery node '{node['name']}'")
        node["credentials"] = {
            "googleApi": {
                "id": BIGQUERY_CRED_ID,
                "name": BIGQUERY_CRED_NAME,
            }
        }
        print("    → credential key: googleApi (service account)")
        fixed["bigquery"] = True

for key, ok in fixed.items():
    if not ok:
        print(f"  WARNING: Did not find/fix node type '{key}'")

# Fix 4: Workflow settings — add timezone
fixed_wf = {
    "name": wf["name"],
    "nodes": nodes,
    "connections": wf["connections"],
    "settings": {
        "timezone": "Europe/Berlin",
        "executionOrder": "v1",
    },
}

# --- PUT ---
print("  Sending PUT to update workflow...")
try:
    n8n_request("PUT", f"/api/v1/workflows/{WEEKLY_WORKFLOW_ID}", fixed_wf)
    print("  Workflow updated")
except urllib.error.HTTPError:
    print("  FATAL: Could not PUT weekly workflow")
    sys.exit(1)

# --- Re-activate ---
print("  Re-activating workflow...")
activated = False
try:
    n8n_request("POST", f"/api/v1/workflows/{WEEKLY_WORKFLOW_ID}/activate")
    print("  Activated")
    activated = True
except Exception as e:
    print(f"  Warning: activation failed: {e}")


# ===========================================================================
# Summary
# ===========================================================================

print()
print("=" * 60)
print("DEPLOYMENT COMPLETE")
print("=" * 60)
print()
print("  Backfill workflow:")
print(f"    ID:  {backfill_id}")
print(f"    URL: {N8N_BASE}/workflow/{backfill_id}")
print("    → Open URL → click 'Execute Workflow' to run backfill")
print("    → Expect ~10k+ rows from ClickHouse, then BigQuery insert")
print()
print("  Weekly workflow:")
print(f"    ID:  {WEEKLY_WORKFLOW_ID}")
print(f"    URL: {N8N_BASE}/workflow/{WEEKLY_WORKFLOW_ID}")
print(f"    Active: {activated}")
print("    Schedule: Monday 04:00 Europe/Berlin (cron: 0 4 * * 1)")
print()
print("  Verification:")
print("  1. Backfill → Execute Workflow → green nodes = success")
print("     If BigQuery 403: check credential type is googleApi (service account)")
print("  2. Weekly → confirm schedule shows 'Monday 4:00 Europe/Berlin'")
print("     confirm SQL has all 18 metric columns")
print("=" * 60)
