#!/usr/bin/env python3
"""
Test script: sends a synthetic Linear 'issue resolved' webhook
for a Bug-type issue to verify the Is Bug? → Update MCT: Bug Fixed flow.
"""
import requests
import json
import time
import creds

NOTION_TOKEN = creds.get("NOTION_TOKEN")
N8N_API_KEY  = creds.get("N8N_API_KEY")
N8N_BASE     = "https://konvoai.app.n8n.cloud"
WEBHOOK_URL  = "https://konvoai.app.n8n.cloud/webhook/linear-issue-sync"
WORKFLOW_ID  = "xdVkUh6YCtcuW8QM"
ISSUES_DB    = "bd1ed48de20e426f8bebeb8e700d19d8"

# ── Step 1: find a Bug issue with a Customer relation and a Linear URL ──────────

print("=== Step 1: Query Notion Issues Table for a Bug issue ===")

notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

query_payload = {
    "filter": {
        "and": [
            {
                "property": "Issue Type",
                "select": {"equals": "Bug"}
            },
            {
                "property": "Customer",
                "relation": {"is_not_empty": True}
            },
            {
                "property": "Linear Ticket URL",
                "url": {"is_not_empty": True}
            }
        ]
    },
    "page_size": 5
}

resp = requests.post(
    f"https://api.notion.com/v1/databases/{ISSUES_DB}/query",
    headers=notion_headers,
    json=query_payload
)
resp.raise_for_status()
results = resp.json().get("results", [])

if not results:
    print("ERROR: No Bug issues with Customer + Linear URL found in Issues Table.")
    exit(1)

# Pick the first one
issue_page = results[0]
page_id    = issue_page["id"]
props      = issue_page["properties"]

# Extract title
title_parts = props.get("Issue Title", {}).get("title", [])
title = title_parts[0]["plain_text"] if title_parts else "Test Bug Issue"

# Extract Linear URL → identifier (e.g. ENG-555)
linear_url = props.get("Linear Ticket URL", {}).get("url", "")
identifier = ""
if linear_url:
    # URL format: https://linear.app/{org}/issue/{IDENTIFIER}/{slug}
    parts = linear_url.rstrip("/").split("/")
    # Find "issue" and take the next segment
    if "issue" in parts:
        idx = parts.index("issue")
        if idx + 1 < len(parts):
            identifier = parts[idx + 1]

# Extract Customer relation
customer_relations = props.get("Customer", {}).get("relation", [])
customer_page_id = customer_relations[0]["id"] if customer_relations else None

print(f"  Page ID  : {page_id}")
print(f"  Title    : {title}")
print(f"  Linear URL: {linear_url}")
print(f"  Identifier: {identifier}")
print(f"  Customer page ID: {customer_page_id}")

if not identifier:
    print("ERROR: Could not extract Linear identifier from URL.")
    exit(1)

# ── Step 2: Send synthetic webhook ──────────────────────────────────────────────

print("\n=== Step 2: POST synthetic Linear webhook ===")

webhook_payload = {
    "action": "update",
    "type": "Issue",
    "data": {
        "identifier": identifier,
        "state": {"name": "Done", "type": "completed"},
        "title": title,
        "url": linear_url,
    },
    # Required by the workflow's guard: proves the state actually changed
    "updatedFrom": {
        "stateId": "previous-state-id-synthetic-test"
    }
}

print(f"  Payload: {json.dumps(webhook_payload, indent=2)}")

wh_resp = requests.post(WEBHOOK_URL, json=webhook_payload)
print(f"  Response status : {wh_resp.status_code}")
print(f"  Response body   : {wh_resp.text[:500]}")

if wh_resp.status_code not in (200, 201):
    print("WARNING: Webhook returned non-2xx — check n8n for details.")

# ── Step 3: Poll n8n for the latest execution ────────────────────────────────────

print("\n=== Step 3: Check n8n execution log (waiting 8s for workflow to finish) ===")
time.sleep(8)

n8n_headers = {"X-N8N-API-KEY": N8N_API_KEY}
exec_resp = requests.get(
    f"{N8N_BASE}/api/v1/executions",
    headers=n8n_headers,
    params={"workflowId": WORKFLOW_ID, "limit": 1}
)
exec_resp.raise_for_status()
exec_data = exec_resp.json()

executions = exec_data.get("data", [])
if not executions:
    print("No executions found for workflow.")
else:
    ex = executions[0]
    exec_id     = ex.get("id")
    exec_status = ex.get("status")
    exec_start  = ex.get("startedAt")
    exec_end    = ex.get("stoppedAt")
    print(f"  Execution ID : {exec_id}")
    print(f"  Status       : {exec_status}")
    print(f"  Started      : {exec_start}")
    print(f"  Stopped      : {exec_end}")

    # Fetch full execution details for node-level status
    detail_resp = requests.get(
        f"{N8N_BASE}/api/v1/executions/{exec_id}",
        headers=n8n_headers
    )
    if detail_resp.status_code == 200:
        detail = detail_resp.json()
        run_data = detail.get("data", {}).get("resultData", {}).get("runData", {})
        print("\n  Node-level results:")
        for node_name, node_runs in run_data.items():
            for run in node_runs:
                error = run.get("error")
                status_msg = "ERROR: " + str(error) if error else "OK"
                print(f"    [{status_msg}] {node_name}")
    else:
        print(f"  Could not fetch execution details: {detail_resp.status_code}")

# ── Step 4: Check the MCT row ───────────────────────────────────────────────────

if customer_page_id:
    print(f"\n=== Step 4: Check MCT row for customer {customer_page_id} ===")
    time.sleep(2)

    # Use Notion-Version 2025-09-03 for MCT pages
    mct_headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2025-09-03",
    }
    mct_resp = requests.get(
        f"https://api.notion.com/v1/pages/{customer_page_id}",
        headers=mct_headers
    )
    if mct_resp.status_code == 200:
        mct_page = mct_resp.json()
        mct_props = mct_page.get("properties", {})

        reason_prop = mct_props.get("💎 Reason for contact", {})
        rtype = reason_prop.get("type", "")
        if rtype == "select":
            reason_val = reason_prop.get("select", {})
            reason_val = reason_val.get("name", "") if reason_val else ""
        elif rtype == "rich_text":
            rt = reason_prop.get("rich_text", [])
            reason_val = rt[0]["plain_text"] if rt else ""
        else:
            reason_val = str(reason_prop)

        print(f"  💎 Reason for contact = {repr(reason_val)}")
        expected = "Bug fixed! 🎉 - check Issues DB"
        if expected in reason_val:
            print("  ✓ MCT correctly updated!")
        else:
            print(f"  ✗ MCT value does not match expected: {repr(expected)}")
            print(f"    All properties keys: {list(mct_props.keys())[:20]}")
    else:
        print(f"  Could not fetch MCT page: {mct_resp.status_code} {mct_resp.text[:300]}")
else:
    print("\nSkipping Step 4 — no customer_page_id found.")

print("\n=== Done ===")
