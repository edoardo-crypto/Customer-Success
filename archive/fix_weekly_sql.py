#!/usr/bin/env python3
"""
Fix Weekly ClickHouse SQL — apply same 2-column GROUP BY (per-customer) as backfill.
Workflow: Jlmx2An3mRolraS3
"""

import json
import requests

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = "***REMOVED***"
WORKFLOW_ID = "Jlmx2An3mRolraS3"

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
    "WHERE toDate(created_at) >= toMonday(today()) - 7 AND toDate(created_at) < toMonday(today()) "
    "GROUP BY stripe_customer_id, week_start "
    "FORMAT JSON"
)

headers = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type": "application/json",
}


def main():
    # 1. GET workflow
    url = f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    workflow = resp.json()
    print(f"Fetched workflow: {workflow.get('name', WORKFLOW_ID)}")

    nodes = workflow.get("nodes", [])

    # 2. Find the httpRequest node (Query ClickHouse)
    target_node = None
    for node in nodes:
        if node.get("type") == "n8n-nodes-base.httpRequest":
            target_node = node
            print(f"Found httpRequest node: '{node.get('name')}' (id={node.get('id')})")
            break

    if target_node is None:
        raise RuntimeError("Could not find httpRequest node in workflow!")

    # Show old SQL for reference
    old_body = target_node.get("parameters", {}).get("body", "<not found>")
    print(f"\nOLD SQL (first 200 chars):\n{old_body[:200]}\n")

    # 3. Replace body with new SQL
    target_node["parameters"]["body"] = NEW_SQL
    print(f"NEW SQL (first 200 chars):\n{NEW_SQL[:200]}\n")

    # 4. PUT workflow back — only send fields accepted by n8n API
    payload = {
        "name": workflow["name"],
        "nodes": workflow["nodes"],
        "connections": workflow["connections"],
        "settings": workflow.get("settings", {}),
        "staticData": workflow.get("staticData"),
    }
    put_resp = requests.put(url, headers=headers, json=payload)
    if not put_resp.ok:
        print(f"PUT failed {put_resp.status_code}: {put_resp.text[:1000]}")
        put_resp.raise_for_status()
    updated = put_resp.json()
    print(f"Workflow updated successfully. Active: {updated.get('active')}")
    print("Done.")


if __name__ == "__main__":
    main()
