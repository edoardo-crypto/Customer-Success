#!/usr/bin/env python3
"""
fix_backfill_sql.py
Update the ClickHouse query in backfill workflow qeCE5b28xkLf8ZqQ
to GROUP BY (stripe_customer_id, week_start) only, using any() for org_id/workspace_id.
"""

import json
import urllib.request
import urllib.error
import creds

N8N_BASE = "https://konvoai.app.n8n.cloud"
API_KEY  = creds.get("N8N_API_KEY")
WORKFLOW_ID = "qeCE5b28xkLf8ZqQ"

NEW_SQL = (
    "SELECT stripe_customer_id, any(org_id) AS org_id, any(workspace_id) AS workspace_id, "
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
    "count() AS data_points "
    "FROM operator.public_workspace_report_snapshot "
    "GROUP BY stripe_customer_id, week_start "
    "FORMAT JSON"
)

HEADERS = {
    "X-N8N-API-KEY": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def api(method, path, body=None):
    url = f"{N8N_BASE}/api/v1{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=HEADERS, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} {e.reason}: {e.read().decode()}")
        raise


# 1. Fetch workflow
print(f"Fetching workflow {WORKFLOW_ID}...")
wf = api("GET", f"/workflows/{WORKFLOW_ID}")
nodes = wf.get("nodes", [])

# 2. Find ClickHouse HTTP Request node
target = None
for node in nodes:
    name_lower = node.get("name", "").lower()
    node_type = node.get("type", "")
    if node_type == "n8n-nodes-base.httpRequest" and "clickhouse" in name_lower:
        target = node
        break

if target is None:
    # Fallback: look for any httpRequest node with SQL-like body
    for node in nodes:
        if node.get("type") == "n8n-nodes-base.httpRequest":
            body_val = (
                node.get("parameters", {}).get("body", "") or
                node.get("parameters", {}).get("jsonBody", "") or
                node.get("parameters", {}).get("rawBody", "")
            )
            if "stripe_customer_id" in str(body_val) or "FORMAT JSON" in str(body_val):
                target = node
                break

if target is None:
    print("ERROR: Could not find ClickHouse HTTP Request node. Node list:")
    for n in nodes:
        print(f"  [{n.get('type')}] {n.get('name')}")
    raise SystemExit(1)

print(f"Found node: '{target['name']}' (type={target['type']})")
print(f"Old body snippet: {str(target.get('parameters', {}).get('body', ''))[:120]}...")

# 3. Update the body parameter
target["parameters"]["body"] = NEW_SQL
print(f"New body set.")

# 4. PUT workflow back — only send fields accepted by n8n API
put_body = {
    "name": wf["name"],
    "nodes": wf["nodes"],
    "connections": wf["connections"],
    "settings": wf.get("settings", {}),
    "staticData": wf.get("staticData"),
}
print("Pushing updated workflow...")
result = api("PUT", f"/workflows/{WORKFLOW_ID}", put_body)
print(f"Success — workflow '{result.get('name')}' updated (active={result.get('active')}).")
print()
print("Next steps:")
print(f"  1. Open https://konvoai.app.n8n.cloud/workflow/{WORKFLOW_ID}")
print("  2. Click 'Execute Workflow' (manual trigger)")
print("  3. Check ClickHouse node output — each (stripe_customer_id, week_start) should appear once")
