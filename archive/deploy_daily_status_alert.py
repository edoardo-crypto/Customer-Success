#!/usr/bin/env python3
"""
deploy_daily_status_alert.py

Deploys "Daily Status Alert" n8n workflow.

Schedule: 8pm daily (Europe/Berlin) — 20:00 CET/CEST.

What it does:
  1. Fetches count of open Intercom conversations (state = open)
  2. Fetches open bugs in Notion Issues DB with no Linear ticket URL
  3. Builds a Slack summary message with both counts
  4. Posts to #customer-success-core via Slack incoming webhook

6 nodes (linear chain):
  1  Schedule: 8pm Daily         scheduleTrigger
  2  Fetch Intercom Open         httpRequest  (POST /conversations/search)
  3  Count Non-Internal          code         (runOnceForAllItems, filters @konvoai.com)
  4  Fetch Open Bugs No Linear   httpRequest  (POST /databases/{id}/query)
  5  Build Summary               code         (runOnceForAllItems)
  6  Send Slack                  httpRequest  (POST to incoming webhook)

After deploy: try API activation; if it fails, toggle ON in n8n UI.
"""

import json
import uuid
import requests
import sys
import creds

# ── Constants ──────────────────────────────────────────────────────────────────
N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")

INTERCOM_TOKEN   = creds.get("INTERCOM_TOKEN")
NOTION_CRED_ID   = "LH587kxanQCPcd9y"
NOTION_CRED_NAME = "Notion - Enrichment"
ISSUES_DB_ID     = "bd1ed48de20e426f8bebeb8e700d19d8"
SLACK_WEBHOOK_URL = creds.get("SLACK_WEBHOOK_CS")

WORKFLOW_NAME = "Daily Status Alert"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}


def uid():
    return str(uuid.uuid4())


def notion_cred():
    return {"httpHeaderAuth": {"id": NOTION_CRED_ID, "name": NOTION_CRED_NAME}}


def notion_auth():
    return {"authentication": "genericCredentialType", "genericAuthType": "httpHeaderAuth"}


# ── JS code block ───────────────────────────────────────────────────────────────
# Unicode escapes for emojis (n8n JS engine resolves them at runtime):
#   \\uD83D\\uDEA8 = 🚨  (sirens)    — U+1F6A8 (surrogate pair)
#   \\uD83D\\uDCEC = 📬  (mailbox)   — U+1F4EC
#   \\uD83D\\uDC1B = 🐛  (bug)       — U+1F41B
#   \\u2014        = —   (em dash)   — U+2014 (BMP, single unit)
#
# Uses $('Fetch Intercom Open') to reach the node two steps back.
# $input is the output of "Fetch Open Bugs No Linear" (the immediately preceding node).

BUILD_SUMMARY_JS = """\
const intercomTotal = $('Count Non-Internal').first().json.count ?? 0;
const bugCount      = ($input.first().json.results || []).length;

const today = new Date().toLocaleDateString('en-GB', {
  day: 'numeric', month: 'long', year: 'numeric'
});

const text = [
  '\\uD83D\\uDEA8 *Daily Status Check \\u2014 ' + today + '*',
  '',
  '\\uD83D\\uDCEC Open Intercom Conversations: *' + intercomTotal + '*',
  '\\uD83D\\uDC1B Open Bugs Without Linear Ticket: *' + bugCount + '*',
].join('\\n');

return [{ json: { text } }];
"""

COUNT_INTERNAL_JS = """\
const conversations = ($input.first().json.conversations) || [];
const external = conversations.filter(c => {
  // Check source author email (always present — person who started the conversation)
  const authorEmail = ((c.source && c.source.author && c.source.author.email) || '').toLowerCase();
  // Also check contact list if emails are populated (varies by Intercom API version)
  const contacts = (c.contacts && c.contacts.contacts) || [];
  const contactEmails = contacts.map(ct => (ct.email || '').toLowerCase()).filter(Boolean);
  const allEmails = [authorEmail, ...contactEmails].filter(Boolean);
  // Keep conversation unless ALL known emails are @konvoai.com
  if (allEmails.length === 0) return true;
  return !allEmails.every(e => e.includes('konvoai.com'));
});
return [{ json: { count: external.length } }];
"""


# ── Build workflow ─────────────────────────────────────────────────────────────

def build_workflow():
    """Build the 6-node Daily Status Alert workflow JSON."""

    y_main = 300
    xs     = [i * 280 for i in range(6)]

    # ── 1. Schedule Trigger ───────────────────────────────────────────────────
    node_schedule = {
        "id":          uid(),
        "name":        "Schedule: 8pm Daily",
        "type":        "n8n-nodes-base.scheduleTrigger",
        "typeVersion": 1.2,
        "position":    [xs[0], y_main],
        "parameters": {
            "rule": {
                "interval": [
                    {"field": "cronExpression", "expression": "0 20 * * *"}
                ]
            },
            "timezone": "Europe/Berlin",
        },
    }

    # ── 2. Fetch Intercom Open ─────────────────────────────────────────────────
    # POST /conversations/search with state=open, per_page=150 (Intercom max).
    # Full conversation objects are returned so Count Non-Internal can post-filter
    # by email. Note: if >150 open conversations exist, count may slightly undercount.
    node_intercom = {
        "id":          uid(),
        "name":        "Fetch Intercom Open",
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [xs[1], y_main],
        "parameters": {
            "method":  "POST",
            "url":     "https://api.intercom.io/conversations/search",
            "sendHeaders": True,
            "headerParameters": {
                "parameters": [
                    {"name": "Authorization",    "value": f"Bearer {INTERCOM_TOKEN}"},
                    {"name": "Intercom-Version", "value": "2.11"},
                    {"name": "Accept",           "value": "application/json"},
                    {"name": "Content-Type",     "value": "application/json"},
                ]
            },
            "sendBody":       True,
            "contentType":    "raw",
            "rawContentType": "application/json",
            # Static body — no n8n template expressions needed
            "body": json.dumps({
                "query": {"field": "state", "operator": "=", "value": "open"},
                "pagination": {"per_page": 150},
            }),
            "options": {},
        },
    }

    # ── 3. Count Non-Internal ─────────────────────────────────────────────────
    # Post-filter: exclude conversations where all known emails are @konvoai.com.
    # Intercom's search API does not support contact.email as a predicate field —
    # filtering must be done client-side after fetching the full response.
    node_filter = {
        "id":          uid(),
        "name":        "Count Non-Internal",
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [xs[2], y_main],
        "parameters":  {"mode": "runOnceForAllItems", "jsCode": COUNT_INTERNAL_JS},
    }

    # ── 4. Fetch Open Bugs No Linear ──────────────────────────────────────────
    # Issues DB uses standard Notion API (not data_sources) + version 2022-06-28.
    # Filter: Issue Type = Bug AND Status = Open AND Linear Ticket URL is_empty
    #         AND Severity IN (Urgent, Important)  — excludes "Not Urgent" bugs.
    # page_size=100 is enough — we count results.length from the single page.
    node_bugs = {
        "id":          uid(),
        "name":        "Fetch Open Bugs No Linear",
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [xs[3], y_main],
        "credentials": notion_cred(),
        "parameters": {
            **notion_auth(),
            "method":         "POST",
            "url":            f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query",
            "sendBody":       True,
            "contentType":    "raw",
            "rawContentType": "application/json",
            # Static body — all values hardcoded, no template expressions
            "body": json.dumps({
                "filter": {
                    "and": [
                        {"property": "Issue Type",        "select": {"equals": "Bug"}},
                        {"property": "Status",            "select": {"equals": "Open"}},
                        {"property": "Linear Ticket URL", "url":    {"is_empty": True}},
                        {"or": [
                            {"property": "Severity", "select": {"equals": "Urgent"}},
                            {"property": "Severity", "select": {"equals": "Important"}},
                        ]},
                        {"property": "Assigned To", "rollup": {"any": {"people": {"is_not_empty": True}}}},
                    ]
                },
                "page_size": 100,
            }),
            "sendHeaders": True,
            "headerParameters": {
                "parameters": [
                    {"name": "Notion-Version", "value": "2022-06-28"},
                ]
            },
            "options": {},
        },
    }

    # ── 5. Build Summary ──────────────────────────────────────────────────────
    # runOnceForAllItems: $input = Fetch Open Bugs No Linear output.
    # References Count Non-Internal by name to get filtered Intercom count.
    node_summary = {
        "id":          uid(),
        "name":        "Build Summary",
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [xs[4], y_main],
        "parameters":  {"mode": "runOnceForAllItems", "jsCode": BUILD_SUMMARY_JS},
    }

    # ── 6. Send Slack ─────────────────────────────────────────────────────────
    # Slack incoming webhook — no auth credential needed, just POST JSON.
    # Body expression: ={{ JSON.stringify({ text: $json.text }) }}
    # The sequence inside the expression is `})` (not `}}`) so no parser collision.
    node_slack = {
        "id":          uid(),
        "name":        "Send Slack",
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [xs[5], y_main],
        "parameters": {
            "method":  "POST",
            "url":     SLACK_WEBHOOK_URL,
            "sendHeaders": True,
            "headerParameters": {
                "parameters": [
                    {"name": "Content-Type", "value": "application/json"},
                ]
            },
            "sendBody":       True,
            "contentType":    "raw",
            "rawContentType": "application/json",
            # $json.text is a string; JSON.stringify wraps it in {"text":"..."}
            # `})` inside the expression is NOT `}}`, so no n8n template parser issue.
            "body":  "={{ JSON.stringify({ text: $json.text }) }}",
            "options": {},
        },
    }

    # ── Connections (linear chain) ─────────────────────────────────────────────
    nodes = [node_schedule, node_intercom, node_filter, node_bugs, node_summary, node_slack]
    connections = {}
    for i in range(len(nodes) - 1):
        connections[nodes[i]["name"]] = {
            "main": [[{"node": nodes[i + 1]["name"], "type": "main", "index": 0}]]
        }

    return {
        "name":  WORKFLOW_NAME,
        "nodes": nodes,
        "connections": connections,
        "settings": {
            "executionOrder": "v1",
            "saveManualExecutions": True,
            "timezone": "Europe/Berlin",
        },
    }


# ── n8n API helpers ────────────────────────────────────────────────────────────

def list_workflows():
    r = requests.get(f"{N8N_BASE}/api/v1/workflows", headers=N8N_HEADERS)
    r.raise_for_status()
    return r.json().get("data", [])


def create_workflow(wf_body):
    r = requests.post(f"{N8N_BASE}/api/v1/workflows", headers=N8N_HEADERS, json=wf_body)
    if r.status_code not in (200, 201):
        print(f"  CREATE failed: {r.status_code} — {r.text[:600]}")
        r.raise_for_status()
    return r.json()


def delete_workflow(wf_id):
    r = requests.delete(f"{N8N_BASE}/api/v1/workflows/{wf_id}", headers=N8N_HEADERS)
    print(f"  Deleted workflow {wf_id} (HTTP {r.status_code})")


def activate_workflow(wf_id):
    r = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{wf_id}/activate",
        headers=N8N_HEADERS,
    )
    return r.status_code, r.text[:200]


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("deploy_daily_status_alert.py")
    print(f"Deploying: {WORKFLOW_NAME!r}")
    print("=" * 65)

    # Step 1: Check for existing workflow with same name
    print(f"\n[1/3] Checking for existing workflow named {WORKFLOW_NAME!r} …")
    existing   = list_workflows()
    duplicates = [w for w in existing if w.get("name") == WORKFLOW_NAME]
    if duplicates:
        print(f"  Found {len(duplicates)} existing workflow(s):")
        for d in duplicates:
            print(f"    ID={d['id']}  active={d.get('active')}")
        answer = input("  Delete and redeploy? [y/N]: ").strip().lower()
        if answer == "y":
            for d in duplicates:
                delete_workflow(d["id"])
        else:
            print("  Aborting.")
            sys.exit(0)
    else:
        print("  No existing workflow found — creating fresh.")

    # Step 2: Build workflow JSON
    print("\n[2/3] Building workflow JSON …")
    wf_body = build_workflow()
    print(f"  Nodes ({len(wf_body['nodes'])}):")
    for i, node in enumerate(wf_body["nodes"], 1):
        print(f"    {i:2}. {node['name']}")

    save_path = "/tmp/daily_status_alert_workflow.json"
    with open(save_path, "w") as f:
        json.dump(wf_body, f, indent=2)
    print(f"  Saved to {save_path}")

    # Step 3: Create in n8n + activate
    print("\n[3/3] Creating workflow in n8n …")
    result = create_workflow(wf_body)
    wf_id  = result.get("id", "?")
    print(f"  ✓ Created  ID={wf_id}  active={result.get('active')}")

    print("      Attempting API activation (schedule trigger) …")
    status, body = activate_workflow(wf_id)
    if status == 200:
        print(f"  ✓ Activated via API (HTTP 200)")
        activated = True
    else:
        print(f"  [warn] Activation returned HTTP {status}: {body}")
        activated = False

    # Final summary
    print("\n" + "=" * 65)
    print("Deployment complete.")
    print("=" * 65)
    print()
    print(f"  Workflow ID : {wf_id}")
    print(f"  Schedule    : 8pm daily  (Europe/Berlin — 20:00 CET/CEST)")
    print(f"  Activated   : {'yes (via API)' if activated else 'NO — manual toggle required'}")
    print()

    if not activated:
        print("  >>> Manual activation required:")
        print(f"      1. Open:  {N8N_BASE}/workflow/{wf_id}")
        print("      2. Click the 'Active' toggle (top-right) → confirm it turns green.")
        print()

    print("Verification steps:")
    print(f"  1. Open: {N8N_BASE}/workflow/{wf_id}")
    print("  2. Click 'Test workflow' → check #customer-success-core for the Slack message.")
    print("  3. Cross-check Intercom: log in → Conversations → filter state=open.")
    print("  4. Cross-check Notion: Issues DB → filter Bug + Open + no Linear URL.")
    print()
    print("Expected Slack message format:")
    print("  \U0001F6A8 Daily Status Check \u2014 25 February 2026")
    print("  \U0001F4EC Open Intercom Conversations: *12*")
    print("  \U0001F41B Open Bugs Without Linear Ticket: *3*")
    print()
    print(f"  Workflow JSON: {save_path}")


if __name__ == "__main__":
    main()
