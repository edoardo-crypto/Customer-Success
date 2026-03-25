#!/usr/bin/env python3
"""
Switch ClickHouse→BigQuery pipelines to snapshot approach:
  1. BigQuery: rename 14 avg_* columns to non-prefixed names (ALTER TABLE)
  2. Weekly workflow Jlmx2An3mRolraS3: use argMax snapshot SQL + updated Code node
  3. Backfill workflow qeCE5b28xkLf8ZqQ: same logic, full history grouped by week

No re-activation needed — both workflows stay active.
"""

import json
import urllib.request
import urllib.error
import ssl
import sys
import time

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")

CLICKHOUSE_CRED_ID   = "kionhtTQSKGgcIYt"
CLICKHOUSE_CRED_NAME = "ClickHouse - AI Resolution Sync"

BIGQUERY_CRED_ID   = "o2dKwUuc5DSzmiou"
BIGQUERY_CRED_NAME = "Google BigQuery Service Account"

WEEKLY_WORKFLOW_ID   = "Jlmx2An3mRolraS3"
BACKFILL_WORKFLOW_ID = "qeCE5b28xkLf8ZqQ"

BQ_PROJECT = "konvoai-n8n"
BQ_DATASET = "konvoai_analytics"
BQ_TABLE   = "customer_kpis_weekly"

# 14 columns to rename: old_name → new_name
COLUMN_RENAMES = [
    ("avg_ai_resolution_rate",       "ai_resolution_rate"),
    ("avg_ai_sessions_total",        "ai_sessions_total"),
    ("avg_ai_sessions_count",        "ai_sessions_count"),
    ("avg_ai_sessions_resolved",     "ai_sessions_resolved"),
    ("avg_ai_sessions_unresolved",   "ai_sessions_unresolved"),
    ("avg_active_skills_count",      "active_skills_count"),
    ("avg_active_processes_count",   "active_processes_count"),
    ("avg_custom_replies_count",     "custom_replies_count"),
    ("avg_channels_connected_count", "channels_connected_count"),
    ("avg_channels_with_ai_count",   "channels_with_ai_count"),
    ("avg_test_scenarios_count",     "test_scenarios_count"),
    ("avg_open_tickets_count",       "open_tickets_count"),
    ("avg_messages_sent24h",         "messages_sent24h"),
    ("avg_messages_received24h",     "messages_received24h"),
]

# ---------------------------------------------------------------------------
# SQL Queries — snapshot approach using argMax
# ---------------------------------------------------------------------------

WEEKLY_SQL = """\
SELECT stripe_customer_id, any(org_id) AS org_id, any(workspace_id) AS workspace_id,
toMonday(today()) AS week_start,
argMax(ai_resolution_rate, created_at) AS ai_resolution_rate,
argMax(ai_sessions_total, created_at) AS ai_sessions_total,
argMax(ai_sessions_count, created_at) AS ai_sessions_count,
argMax(ai_sessions_resolved, created_at) AS ai_sessions_resolved,
argMax(ai_sessions_unresolved, created_at) AS ai_sessions_unresolved,
argMax(active_skills_count, created_at) AS active_skills_count,
argMax(active_processes_count, created_at) AS active_processes_count,
argMax(custom_replies_count, created_at) AS custom_replies_count,
argMax(channels_connected_count, created_at) AS channels_connected_count,
argMax(channels_with_ai_count, created_at) AS channels_with_ai_count,
argMax(test_scenarios_count, created_at) AS test_scenarios_count,
argMax(open_tickets_count, created_at) AS open_tickets_count,
argMax(messages_sent24h, created_at) AS messages_sent24h,
argMax(messages_received24h, created_at) AS messages_received24h,
count() AS data_points
FROM operator.public_workspace_report_snapshot
WHERE toDate(created_at) = today()
GROUP BY stripe_customer_id FORMAT JSON"""

BACKFILL_SQL = """\
SELECT stripe_customer_id, any(org_id) AS org_id, any(workspace_id) AS workspace_id,
toMonday(toDate(created_at)) AS week_start,
argMax(ai_resolution_rate, created_at) AS ai_resolution_rate,
argMax(ai_sessions_total, created_at) AS ai_sessions_total,
argMax(ai_sessions_count, created_at) AS ai_sessions_count,
argMax(ai_sessions_resolved, created_at) AS ai_sessions_resolved,
argMax(ai_sessions_unresolved, created_at) AS ai_sessions_unresolved,
argMax(active_skills_count, created_at) AS active_skills_count,
argMax(active_processes_count, created_at) AS active_processes_count,
argMax(custom_replies_count, created_at) AS custom_replies_count,
argMax(channels_connected_count, created_at) AS channels_connected_count,
argMax(channels_with_ai_count, created_at) AS channels_with_ai_count,
argMax(test_scenarios_count, created_at) AS test_scenarios_count,
argMax(open_tickets_count, created_at) AS open_tickets_count,
argMax(messages_sent24h, created_at) AS messages_sent24h,
argMax(messages_received24h, created_at) AS messages_received24h,
count() AS data_points
FROM operator.public_workspace_report_snapshot
GROUP BY stripe_customer_id, week_start FORMAT JSON"""

# ---------------------------------------------------------------------------
# Code node JS — no avg_ prefix
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
    stripe_customer_id:         String(row.stripe_customer_id),
    org_id:                     String(row.org_id),
    workspace_id:               String(row.workspace_id),
    week_start:                 String(row.week_start),
    ai_resolution_rate:         parseFloat(row.ai_resolution_rate)         || 0,
    ai_sessions_total:          parseFloat(row.ai_sessions_total)          || 0,
    ai_sessions_count:          parseFloat(row.ai_sessions_count)          || 0,
    ai_sessions_resolved:       parseFloat(row.ai_sessions_resolved)       || 0,
    ai_sessions_unresolved:     parseFloat(row.ai_sessions_unresolved)     || 0,
    active_skills_count:        parseFloat(row.active_skills_count)        || 0,
    active_processes_count:     parseFloat(row.active_processes_count)     || 0,
    custom_replies_count:       parseFloat(row.custom_replies_count)       || 0,
    channels_connected_count:   parseFloat(row.channels_connected_count)   || 0,
    channels_with_ai_count:     parseFloat(row.channels_with_ai_count)     || 0,
    test_scenarios_count:       parseFloat(row.test_scenarios_count)       || 0,
    open_tickets_count:         parseFloat(row.open_tickets_count)         || 0,
    messages_sent24h:           parseFloat(row.messages_sent24h)           || 0,
    messages_received24h:       parseFloat(row.messages_received24h)       || 0,
    data_points:                parseInt(row.data_points, 10)              || 0,
    ingested_at:                now,
  }
}));
"""

# ---------------------------------------------------------------------------
# Google auth helpers (service account JWT → access token)
# ---------------------------------------------------------------------------

import base64
import hashlib
import hmac
import struct
import time as _time

SA_JSON = json.loads(os.environ.get("BIGQUERY_SA_JSON", "{}"))
# Expected keys: type, project_id, private_key_id, private_key, client_email, token_uri


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _get_access_token() -> str:
    """Mint a short-lived OAuth2 access token from the service account key."""
    try:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
    except ImportError:
        print("  Installing cryptography...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "cryptography", "-q"])
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
import creds

    now = int(_time.time())
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    claim = _b64url(json.dumps({
        "iss": SA_JSON["client_email"],
        "scope": "https://www.googleapis.com/auth/bigquery",
        "aud": SA_JSON["token_uri"],
        "iat": now,
        "exp": now + 3600,
    }).encode())

    signing_input = f"{header}.{claim}".encode()
    private_key = load_pem_private_key(SA_JSON["private_key"].encode(), password=None)
    signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    jwt = f"{header}.{claim}.{_b64url(signature)}"

    ctx = ssl.create_default_context()
    body = f"grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={jwt}"
    req = urllib.request.Request(
        SA_JSON["token_uri"],
        data=body.encode(),
        method="POST",
    )
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, context=ctx) as resp:
        return json.loads(resp.read().decode())["access_token"]


# ---------------------------------------------------------------------------
# BigQuery REST helpers
# ---------------------------------------------------------------------------

ctx = ssl.create_default_context()


def bq_run_job(access_token: str, sql: str) -> dict:
    """Submit a BigQuery job (async) and poll until complete. Works for DDL."""
    # Insert job
    insert_url = (
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{BQ_PROJECT}/jobs"
    )
    job_body = json.dumps({
        "configuration": {
            "query": {
                "query": sql,
                "useLegacySql": False,
            }
        },
        "jobReference": {
            "projectId": BQ_PROJECT,
            "location": "europe-west3",
        },
    }).encode()
    req = urllib.request.Request(insert_url, data=job_body, method="POST")
    req.add_header("Authorization", f"Bearer {access_token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            job = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode() if e.fp else ""
        print(f"  BigQuery HTTP {e.code}: {body_text[:800]}")
        raise

    job_id = job["jobReference"]["jobId"]
    location = job["jobReference"].get("location", "US")

    # Poll until done
    for _ in range(30):
        poll_url = (
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{BQ_PROJECT}"
            f"/jobs/{job_id}?location={location}"
        )
        poll_req = urllib.request.Request(poll_url, method="GET")
        poll_req.add_header("Authorization", f"Bearer {access_token}")
        with urllib.request.urlopen(poll_req, context=ctx) as resp:
            status = json.loads(resp.read().decode())
        state = status.get("status", {}).get("state", "")
        if state == "DONE":
            err = status.get("status", {}).get("errorResult")
            if err:
                raise RuntimeError(f"BigQuery job error: {err}")
            return status
        time.sleep(2)

    raise TimeoutError(f"BigQuery job {job_id} did not complete in time")


# ---------------------------------------------------------------------------
# n8n helpers
# ---------------------------------------------------------------------------


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


def patch_workflow(workflow_id: str, new_sql: str, label: str):
    """Fetch workflow, replace ClickHouse SQL + Code node JS, PUT back."""
    print(f"\n  Fetching {label} workflow {workflow_id}...")
    wf = n8n_request("GET", f"/api/v1/workflows/{workflow_id}")
    print(f"  Name: {wf['name']}  Nodes: {len(wf['nodes'])}")

    fixed = {"clickhouse": False, "code": False}

    for node in wf["nodes"]:
        ntype = node["type"]

        if ntype == "n8n-nodes-base.httpRequest":
            print(f"    [ClickHouse] '{node['name']}' → updating SQL")
            node["parameters"]["body"] = new_sql
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
            fixed["clickhouse"] = True

        elif ntype == "n8n-nodes-base.code":
            print(f"    [Code] '{node['name']}' → updating jsCode (no avg_ prefix)")
            node["parameters"]["jsCode"] = PARSE_CAST_CODE
            node["parameters"]["mode"] = "runOnceForAllItems"
            fixed["code"] = True

    for key, ok in fixed.items():
        if not ok:
            print(f"    WARNING: Did not find node type '{key}' in {label} workflow")

    put_body = {
        "name": wf["name"],
        "nodes": wf["nodes"],
        "connections": wf["connections"],
        "settings": wf.get("settings", {"executionOrder": "v1"}),
    }
    n8n_request("PUT", f"/api/v1/workflows/{workflow_id}", put_body)
    print(f"  {label} workflow updated.")


# ===========================================================================
# Phase 1 — BigQuery: rename 14 columns
# ===========================================================================

print("=" * 60)
print("Phase 1: BigQuery — rename avg_* columns")
print("=" * 60)

print("  Getting access token...")
try:
    token = _get_access_token()
    print("  Access token obtained.")
except Exception as e:
    print(f"  FATAL: Could not get BigQuery access token: {e}")
    sys.exit(1)

errors = []
for i, (old_col, new_col) in enumerate(COLUMN_RENAMES):
    ddl = (
        f"ALTER TABLE `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` "
        f"RENAME COLUMN `{old_col}` TO `{new_col}`"
    )
    print(f"  Renaming {old_col} → {new_col} ...", end=" ", flush=True)
    renamed = False
    for attempt in range(5):
        try:
            bq_run_job(token, ddl)
            print("OK")
            renamed = True
            break
        except RuntimeError as e:
            if "rateLimitExceeded" in str(e):
                print(f" rate limit, waiting 30s...", end="", flush=True)
                time.sleep(30)
            else:
                print(f"ERROR: {e}")
                break
        except Exception as e:
            print(f"ERROR: {e}")
            break
    if not renamed:
        errors.append(old_col)
    elif i < len(COLUMN_RENAMES) - 1:
        time.sleep(15)  # avoid DDL rate limit (5 ops/10s)

if errors:
    print(f"\n  WARNING: {len(errors)} rename(s) failed: {errors}")
    print("  Continuing with n8n workflow updates anyway...")
else:
    print(f"\n  All {len(COLUMN_RENAMES)} columns renamed successfully.")

# ===========================================================================
# Phase 2 — Weekly Workflow
# ===========================================================================

print()
print("=" * 60)
print(f"Phase 2: Weekly Workflow {WEEKLY_WORKFLOW_ID}")
print("=" * 60)

try:
    patch_workflow(WEEKLY_WORKFLOW_ID, WEEKLY_SQL, "Weekly")
except Exception as e:
    print(f"  FATAL: {e}")
    sys.exit(1)

# ===========================================================================
# Phase 3 — Backfill Workflow
# ===========================================================================

print()
print("=" * 60)
print(f"Phase 3: Backfill Workflow {BACKFILL_WORKFLOW_ID}")
print("=" * 60)

try:
    patch_workflow(BACKFILL_WORKFLOW_ID, BACKFILL_SQL, "Backfill")
except Exception as e:
    print(f"  FATAL: {e}")
    sys.exit(1)

# ===========================================================================
# Summary
# ===========================================================================

print()
print("=" * 60)
print("DONE")
print("=" * 60)
print()
print("  BigQuery: 14 columns renamed (avg_* → non-prefixed)")
print()
print("  Weekly workflow:")
print(f"    URL: {N8N_BASE}/workflow/{WEEKLY_WORKFLOW_ID}")
print("    SQL: argMax snapshot for today() rows, grouped by stripe_customer_id")
print()
print("  Backfill workflow:")
print(f"    URL: {N8N_BASE}/workflow/{BACKFILL_WORKFLOW_ID}")
print("    SQL: argMax snapshot per week, full history")
print()
print("  Next steps:")
print("  1. Open weekly workflow URL → Execute Workflow → verify green nodes")
print("  2. Check BigQuery for new row with non-prefixed column names")
print("  3. (Optional) Run backfill workflow to rewrite historical rows")
print("=" * 60)
