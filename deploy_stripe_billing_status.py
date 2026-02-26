#!/usr/bin/env python3
"""
deploy_stripe_billing_status.py — Deploy "Stripe Churn: Update Billing Status" workflow

What it does:
  When Stripe fires customer.subscription.updated with status = "canceled",
  this workflow immediately sets 💰 Billing Status = "Canceled" on the matching
  Notion Master Customer Table row. No-ops on any other status transition.

The daily Stripe sync (Ai9Y3FWjqMtEhr57) already does this at 09:30 CET — this
workflow makes the update happen within seconds instead of up to 24 hours.

Workflow (5 nodes):
  1. Webhook         — receives Stripe POST to /webhook/stripe-billing-status
  2. IF: Canceled?   — guards: only proceed if data.object.status === "canceled"
  3. Find Customer   — queries MCT data_sources by 🔗 Stripe Customer ID
  4. IF: Found?      — guards: only proceed if at least one row matched
  5. Update Notion   — PATCHes the page: 💰 Billing Status = "Canceled"

After deployment:
  1. Open the workflow URL printed below → toggle Active in the n8n UI
  2. Register the webhook URL in Stripe Dashboard → Developers → Webhooks
     Event to listen for: customer.subscription.updated
"""

import json
import ssl
import uuid
import urllib.request
import urllib.error
import sys

# ── Config ────────────────────────────────────────────────────────────────────
N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = (
    "***REMOVED_JWT***"
    ".eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJp"
    "c3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleH"
    "AiOjE3NzMyNzAwMDB9.X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)
NOTION_TOKEN = "***REMOVED***"
NOTION_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"  # MCT multi-source data source

WORKFLOW_NAME = "Stripe Churn: Update Billing Status"
WEBHOOK_PATH  = "stripe-billing-status"
WEBHOOK_ID    = str(uuid.uuid4())  # required by n8n for webhook route registration

ctx = ssl.create_default_context()


# ── n8n API helper ─────────────────────────────────────────────────────────────

def n8n(method, path, body=None):
    url = f"{N8N_BASE}/api/v1{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"X-N8N-API-KEY": N8N_API_KEY, "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, context=ctx) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        print(f"  HTTP {e.code}: {body_text[:500]}")
        raise


# ── Workflow definition ────────────────────────────────────────────────────────

NOTION_HEADERS_2025 = {
    "parameters": [
        {"name": "Authorization",  "value": f"Bearer {NOTION_TOKEN}"},
        {"name": "Notion-Version", "value": "2025-09-03"},
        {"name": "Content-Type",   "value": "application/json"},
    ]
}

# Static patch body — Billing Status is always "Canceled" for this workflow
PATCH_BODY = json.dumps({
    "properties": {
        "\U0001f4b0 Billing Status": {
            "select": {"name": "Canceled"}
        }
    }
})

# MCT query: filter where 🔗 Stripe Customer ID contains the customer ID from event
# Payload is at $json.body (webhook typeVersion 2 wraps the raw body there)
MCT_QUERY_BODY = (
    '={{ JSON.stringify({'
    '"filter": {'
    '"property": "\\ud83d\\udd17 Stripe Customer ID",'
    '"rich_text": {"contains": $json.body.data.object.customer}'
    '},'
    '"page_size": 1'
    '}) }}'
)

workflow = {
    "name": WORKFLOW_NAME,
    "settings": {
        "executionOrder": "v1",
        "saveManualExecutions": True,
    },
    "nodes": [
        # ── 1. Stripe Webhook ────────────────────────────────────────────────
        {
            "id":          "n1",
            "name":        "Stripe Webhook",
            "type":        "n8n-nodes-base.webhook",
            "typeVersion": 2,
            "position":    [250, 300],
            "webhookId":   WEBHOOK_ID,
            "parameters": {
                "httpMethod":   "POST",
                "path":         WEBHOOK_PATH,
                "responseMode": "onReceived",
                "options":      {},
            },
        },

        # ── 2. IF: Is status === "canceled"? ─────────────────────────────────
        # Stripe sends cancel_at_period_end=true before the sub actually cancels;
        # we only act when status literally becomes "canceled".
        {
            "id":          "n2",
            "name":        "IF: Is Canceled?",
            "type":        "n8n-nodes-base.if",
            "typeVersion": 1,
            "position":    [500, 300],
            "parameters": {
                "conditions": {
                    "string": [
                        {
                            "value1":    "={{ $json.body.data.object.status }}",
                            "operation": "equal",
                            "value2":    "canceled",
                        }
                    ]
                }
            },
        },

        # ── 3. Find Customer in MCT ──────────────────────────────────────────
        {
            "id":          "n3",
            "name":        "Find Customer in MCT",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [750, 300],
            "parameters": {
                "method":          "POST",
                "url":             f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
                "sendHeaders":     True,
                "headerParameters": NOTION_HEADERS_2025,
                "sendBody":        True,
                "specifyBody":     "json",
                "jsonBody":        MCT_QUERY_BODY,
                "options":         {},
            },
        },

        # ── 4. IF: Customer row found? ───────────────────────────────────────
        {
            "id":          "n4",
            "name":        "IF: Customer Found?",
            "type":        "n8n-nodes-base.if",
            "typeVersion": 1,
            "position":    [1000, 300],
            "parameters": {
                "conditions": {
                    "number": [
                        {
                            "value1":    "={{ $json.results.length }}",
                            "operation": "larger",
                            "value2":    0,
                        }
                    ]
                }
            },
        },

        # ── 5. Update Notion Page: set Billing Status = "Canceled" ───────────
        {
            "id":          "n5",
            "name":        "Update Notion Page",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [1250, 300],
            "parameters": {
                "method":      "PATCH",
                "url":         "={{ 'https://api.notion.com/v1/pages/' + $json.results[0].id }}",
                "sendHeaders": True,
                "headerParameters": NOTION_HEADERS_2025,
                "sendBody":    True,
                "contentType": "raw",
                "rawContentType": "application/json",
                "body":        PATCH_BODY,
                "options":     {},
            },
        },
    ],

    "connections": {
        "Stripe Webhook": {
            "main": [[{"node": "IF: Is Canceled?", "type": "main", "index": 0}]]
        },
        "IF: Is Canceled?": {
            "main": [
                [{"node": "Find Customer in MCT", "type": "main", "index": 0}],
                [],   # false branch — not a cancellation, do nothing
            ]
        },
        "Find Customer in MCT": {
            "main": [[{"node": "IF: Customer Found?", "type": "main", "index": 0}]]
        },
        "IF: Customer Found?": {
            "main": [
                [{"node": "Update Notion Page", "type": "main", "index": 0}],
                [],   # false branch — no matching row in MCT, do nothing
            ]
        },
    },
}


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("deploy_stripe_billing_status.py")
    print(f"Deploying: {WORKFLOW_NAME!r}")
    print("=" * 60)

    # Check for duplicate name, prompt before deleting
    print("\n[1/3] Checking for existing workflow with same name...")
    existing = n8n("GET", "/workflows").get("data", [])
    duplicates = [w for w in existing if w.get("name") == WORKFLOW_NAME]
    if duplicates:
        print(f"  Found {len(duplicates)} existing workflow(s):")
        for d in duplicates:
            print(f"    id={d['id']}  active={d.get('active')}")
        answer = input("  Delete and redeploy? [y/N]: ").strip().lower()
        if answer != "y":
            print("  Aborting.")
            sys.exit(0)
        for d in duplicates:
            n8n("DELETE", f"/workflows/{d['id']}")
            print(f"  Deleted {d['id']}")

    # Save workflow JSON locally for inspection
    with open("/tmp/stripe_billing_status_workflow.json", "w") as f:
        json.dump(workflow, f, indent=2)
    print("\n[2/3] Workflow JSON saved to /tmp/stripe_billing_status_workflow.json")
    print(f"  Nodes: {len(workflow['nodes'])}")
    for i, node in enumerate(workflow["nodes"], 1):
        print(f"    {i}. {node['name']}")

    # Create in n8n
    print("\n[3/3] Creating workflow in n8n...")
    result = n8n("POST", "/workflows", workflow)
    wf_id = result.get("id", "?")
    print(f"  Created workflow id: {wf_id}")
    print(f"  Active: {result.get('active')}")

    # Instructions
    print()
    print("=" * 60)
    print("NEXT STEPS (both required before this workflow does anything)")
    print("=" * 60)
    print()
    print("Step 1 — Activate in n8n UI:")
    print(f"  Open:  {N8N_BASE}/workflow/{wf_id}")
    print("  Toggle the 'Active' switch in the top-right corner.")
    print("  (API activation alone does NOT register the webhook route.)")
    print()
    print("Step 2 — Register webhook in Stripe:")
    print("  Go to: Stripe Dashboard → Developers → Webhooks → Add endpoint")
    print(f"  URL:   {N8N_BASE}/webhook/{WEBHOOK_PATH}")
    print("  Event: customer.subscription.updated")
    print()
    print("What happens once live:")
    print("  • Stripe cancels a subscription → fires customer.subscription.updated")
    print("  • Workflow checks: is status 'canceled'? (ignores cancel_at_period_end)")
    print("  • Finds the Notion MCT row by 🔗 Stripe Customer ID")
    print("  • Sets 💰 Billing Status = 'Canceled' instantly (vs. up to 24h delay)")
    print()
    print(f"Webhook ID embedded in node: {WEBHOOK_ID}")


if __name__ == "__main__":
    main()
