#!/usr/bin/env python3
"""Deploy 'Log Customer Contact' workflow to n8n.

Slack-triggered webhook that lets Alex/Aya log a customer contact:
  - Updates the '📞 Last Contact Date' field in Master Customer Table
  - Creates a row in the CS Actions DB

Flow:
  Webhook  POST /webhook/log-contact
    → Code 'Extract Input'           (normalize user_id → performer name,
                                      pre-build MCT search body)
    → HTTP 'Search MCT'              (title-contains query)
    → Code 'Evaluate Match'          (count results, pre-build patch + create bodies)
    → IF   'Exactly One Match?'
        ├─[true]  HTTP 'Update Last Contact Date'
        │           → HTTP 'Create CS Action Row'
        │               → HTTP 'Slack Confirm'
        └─[false] HTTP 'Slack Clarify'

REQUIRED AFTER DEPLOY:
  1. Note the webhook URL printed at the end of this script.
  2. Toggle the workflow ON in the n8n UI — webhooks only register via UI toggle.
  3. Create a Slack Workflow Builder shortcut that POSTs:
       { "customer_name": "...", "user_id": "...",
         "channel_id": "...", "performed_by": "..." }
     to the webhook URL.
  4. Verify that 'Performed By' select options in CS Actions DB
     match exactly 'Alex' and 'Aya' (check in Notion if unsure).
"""

import json
import ssl
import uuid
import urllib.request
import urllib.error
import creds

# ── Constants ──────────────────────────────────────────────────────────────────
N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")

MCT_DS_ID        = "3ceb1ad0-91f1-40db-945a-c51c58035898"
CS_ACTIONS_DB    = "e0d1057c6d24405cb99cb35c2fae8ad6"
NOTION_CRED_ID   = "LH587kxanQCPcd9y"
NOTION_CRED_NAME = "Notion - Enrichment"
SLACK_CRED_ID    = "IMuEGtYutmUKwCqY"
SLACK_CRED_NAME  = "Slack Bot for Alerts"
DEFAULT_CHANNEL  = "C0AGZDTUND6"    # #customer-success-core

ALEX_USER_ID  = "U0781C7B3UM"
AYA_USER_ID   = "U08US7UFH62"

WEBHOOK_ID    = str(uuid.uuid4())   # new UUID every deploy
WEBHOOK_PATH  = "log-contact"

MCT_URL = "https://api.notion.com/v1/data_sources/" + MCT_DS_ID + "/query"

ctx = ssl.create_default_context()


def n8n(method, path, body=None):
    url = N8N_BASE + "/api/v1" + path
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"X-N8N-API-KEY": N8N_API_KEY, "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, context=ctx) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print("  HTTP " + str(e.code) + ": " + e.read().decode())
        raise


def notion_cred():
    return {"httpHeaderAuth": {"id": NOTION_CRED_ID, "name": NOTION_CRED_NAME}}


def slack_cred():
    return {"httpHeaderAuth": {"id": SLACK_CRED_ID, "name": SLACK_CRED_NAME}}


def notion_auth():
    return {"authentication": "genericCredentialType", "genericAuthType": "httpHeaderAuth"}


def notion_header_v3():
    """Notion-Version 2025-09-03 — required for MCT (multi-source DB)."""
    return {
        "sendHeaders": True,
        "headerParameters": {
            "parameters": [{"name": "Notion-Version", "value": "2025-09-03"}]
        },
    }


def notion_header_v2():
    """Notion-Version 2022-06-28 — for standard DBs (CS Actions)."""
    return {
        "sendHeaders": True,
        "headerParameters": {
            "parameters": [{"name": "Notion-Version", "value": "2022-06-28"}]
        },
    }


# ── JS: Extract Input ──────────────────────────────────────────────────────────
# Plain triple-quoted string (no f-string) to avoid brace-escaping issues.
# Constants are injected via .replace() after the string is defined.
# searchBody is pre-built here to avoid a nested }} pattern in the HTTP body
# expression that would confuse n8n's template parser.
_EXTRACT_INPUT_TEMPLATE = """\
const body = $input.first().json.body || $input.first().json;
const userId = body.user_id || '';
const performed_by = userId === 'ALEX_UID' ? 'Alex'
                   : userId === 'AYA_UID'  ? 'Aya'
                   : (body.performed_by || 'Unknown').split(' ')[0];
const customer_name = (body.customer_name || '').trim();

// Pre-build the search body here to avoid }} in the HTTP node body expression.
// filter: { property: "...", title: { contains: X } } would produce }}
// when serialised inside an n8n template expression.
const searchFilter = {property: '\\uD83C\\uDFE2 Company Name', title: {contains: customer_name}};
const searchBody = JSON.stringify({page_size: 10, filter: searchFilter});

return [{json: {
  customer_name,
  user_id:    userId,
  channel_id: body.channel_id || 'DEFAULT_CH',
  performed_by,
  searchBody,
}}];
"""

EXTRACT_INPUT_JS = (
    _EXTRACT_INPUT_TEMPLATE
    .replace("ALEX_UID",   ALEX_USER_ID)
    .replace("AYA_UID",    AYA_USER_ID)
    .replace("DEFAULT_CH", DEFAULT_CHANNEL)
)


# ── JS: Evaluate Match ────────────────────────────────────────────────────────
# Also pre-builds patchBody and createBody to avoid }} patterns in HTTP nodes.
# The nested property objects ( {date: {start: X}} etc.) would otherwise create
# }} sequences that n8n's template parser closes early.
_EVALUATE_MATCH_TEMPLATE = """\
const results = $input.first().json.results || [];
const inp = $('Extract Input').first().json;

const matches = results.map(r => ({
  id:   r.id,
  name: r.properties['\\uD83C\\uDFE2 Company Name']?.title?.[0]?.plain_text || '?',
}));

const page_id      = matches.length === 1 ? matches[0].id   : null;
const company_name = matches.length === 1 ? matches[0].name : null;
const today        = new Date().toISOString().slice(0, 10);

// Pre-build PATCH body for Update Last Contact Date.
// Builds the nested object programmatically — no }} in the resulting JSON string.
let patchBody = null;
if (page_id) {
  const dateProps = {};
  dateProps['\\uD83D\\uDCDE Last Contact Date \\uD83D\\uDD12'] = {date: {start: today}};
  patchBody = JSON.stringify({properties: dateProps});
}

// Pre-build POST body for Create CS Action Row.
let createBody = null;
if (page_id) {
  const props = {};
  props['Action Title'] = {title: [{text: {content: 'Check-in with ' + company_name}}]};
  props['Action Type']  = {select: {name: 'Check-in'}};
  props['Date']         = {date: {start: today}};
  props['Performed By'] = {select: {name: inp.performed_by}};
  props['Customer']     = {relation: [{id: page_id}]};
  createBody = JSON.stringify({
    parent:     {database_id: 'CS_ACTIONS_DB_PLACEHOLDER'},
    properties: props,
  });
}

return [{json: {
  match_count:    matches.length,
  page_id,
  company_name,
  matches,
  channel_id:     inp.channel_id,
  performed_by:   inp.performed_by,
  customer_query: inp.customer_name,
  patchBody,
  createBody,
}}];
"""

EVALUATE_MATCH_JS = _EVALUATE_MATCH_TEMPLATE.replace(
    "CS_ACTIONS_DB_PLACEHOLDER", CS_ACTIONS_DB
)


def build_workflow():
    nodes = [
        # ── 1. Webhook ────────────────────────────────────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Webhook",
            "type":        "n8n-nodes-base.webhook",
            "typeVersion": 2,
            "position":    [0, 0],
            "webhookId":   WEBHOOK_ID,
            "parameters": {
                "path":         WEBHOOK_PATH,
                "httpMethod":   "POST",
                "responseMode": "responseNode",
            },
        },
        # ── 2. Code: Extract Input ────────────────────────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Extract Input",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    [260, 0],
            "parameters": {
                "mode":   "runOnceForAllItems",
                "jsCode": EXTRACT_INPUT_JS,
            },
        },
        # ── 3. HTTP: Search MCT ───────────────────────────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Search MCT",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position":    [520, 0],
            "credentials": notion_cred(),
            "parameters": {
                **notion_auth(),
                "method":         "POST",
                "url":            MCT_URL,
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                # Plain field reference — no }} risk
                "body":           "={{ $('Extract Input').first().json.searchBody }}",
                **notion_header_v3(),
            },
        },
        # ── 4. Code: Evaluate Match ───────────────────────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Evaluate Match",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    [780, 0],
            "parameters": {
                "mode":   "runOnceForAllItems",
                "jsCode": EVALUATE_MATCH_JS,
            },
        },
        # ── 5. IF: Exactly One Match? ─────────────────────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Exactly One Match?",
            "type":        "n8n-nodes-base.if",
            "typeVersion": 1,
            "position":    [1040, 0],
            "parameters": {
                "conditions": {
                    "number": [{
                        "value1":    "={{ $json.match_count }}",
                        "operation": "equal",
                        "value2":    1,
                    }]
                }
            },
        },
        # ── 6. HTTP: Update Last Contact Date (true branch) ───────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Update Last Contact Date",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position":    [1300, -140],
            "credentials": notion_cred(),
            "parameters": {
                **notion_auth(),
                "method":         "PATCH",
                "url":            "={{ \"https://api.notion.com/v1/pages/\" + $json.page_id }}",
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                # Plain field reference — patchBody was pre-built in Evaluate Match
                "body":           "={{ $json.patchBody }}",
                **notion_header_v3(),
            },
        },
        # ── 7. HTTP: Create CS Action Row ─────────────────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Create CS Action Row",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position":    [1560, -140],
            "credentials": notion_cred(),
            "parameters": {
                **notion_auth(),
                "method":         "POST",
                "url":            "https://api.notion.com/v1/pages",
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                # Plain field reference — createBody was pre-built in Evaluate Match
                "body":           "={{ $('Evaluate Match').first().json.createBody }}",
                **notion_header_v2(),
            },
        },
        # ── 8. HTTP: Slack Confirm (true branch end) ──────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Slack Confirm",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position":    [1820, -140],
            "credentials": slack_cred(),
            "parameters": {
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "method":         "POST",
                "url":            "https://slack.com/api/chat.postMessage",
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json; charset=utf-8",
                # Outer object has single-depth braces — no }} inside the expression
                "body": (
                    "={{ JSON.stringify({"
                    " channel: $('Evaluate Match').first().json.channel_id,"
                    " text: \"\\u2705 Logged contact with *\""
                    " + $('Evaluate Match').first().json.company_name"
                    " + \"* \\u2014 last_contact_date updated to today.\","
                    " unfurl_links: false"
                    " }) }}"
                ),
            },
        },
        # ── 9. HTTP: Slack Clarify (false branch) ─────────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Slack Clarify",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position":    [1300, 140],
            "credentials": slack_cred(),
            "parameters": {
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "method":         "POST",
                "url":            "https://slack.com/api/chat.postMessage",
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json; charset=utf-8",
                # Multi-branch JS expression.
                # Brace audit: if/else blocks close with single }, JSON.stringify
                # object closes with } then ) then ; — no }} anywhere inside.
                "body": (
                    "={{\n"
                    "  const d = $json;\n"
                    "  let msg;\n"
                    "  if (d.match_count === 0) {\n"
                    "    msg = \"\\u2753 No customer found matching *\""
                    " + d.customer_query + \"*. Check the name and try again.\";\n"
                    "  } else {\n"
                    "    const list = d.matches.map(m => \"\\u2022 \" + m.name).join(\"\\n\");\n"
                    "    msg = \"\\u2753 Found \" + d.match_count"
                    " + \" customers matching *\" + d.customer_query + \"*:\\n\""
                    " + list + \"\\nPlease be more specific.\";\n"
                    "  }\n"
                    "  JSON.stringify({ channel: d.channel_id,"
                    " text: msg, unfurl_links: false });\n"
                    "}}"
                ),
            },
        },
    ]

    connections = {
        "Webhook": {
            "main": [[{"node": "Extract Input", "type": "main", "index": 0}]]
        },
        "Extract Input": {
            "main": [[{"node": "Search MCT", "type": "main", "index": 0}]]
        },
        "Search MCT": {
            "main": [[{"node": "Evaluate Match", "type": "main", "index": 0}]]
        },
        "Evaluate Match": {
            "main": [[{"node": "Exactly One Match?", "type": "main", "index": 0}]]
        },
        "Exactly One Match?": {
            "main": [
                [{"node": "Update Last Contact Date", "type": "main", "index": 0}],  # true
                [{"node": "Slack Clarify",            "type": "main", "index": 0}],  # false
            ]
        },
        "Update Last Contact Date": {
            "main": [[{"node": "Create CS Action Row", "type": "main", "index": 0}]]
        },
        "Create CS Action Row": {
            "main": [[{"node": "Slack Confirm", "type": "main", "index": 0}]]
        },
    }

    return nodes, connections


def main():
    nodes, connections = build_workflow()

    wf_body = {
        "name": "Log Customer Contact",
        "nodes": nodes,
        "connections": connections,
        "settings": {"executionOrder": "v1"},
    }

    print("Step 1/2 — Creating workflow...")
    result = n8n("POST", "/workflows", wf_body)
    wf_id = result["id"]
    print("  + Created  id=" + wf_id + "  name='" + result["name"] + "'")

    print("Step 2/2 — Setting active=true via API...")
    try:
        n8n("POST", "/workflows/" + wf_id + "/activate")
        print("  + API activate succeeded")
    except Exception:
        print("  ! Activate call failed — toggle manually in UI (normal for webhooks)")

    webhook_url = N8N_BASE + "/webhook/" + WEBHOOK_PATH

    print()
    print("=" * 60)
    print("  Workflow   : " + N8N_BASE + "/workflow/" + wf_id)
    print("  Webhook ID : " + WEBHOOK_ID)
    print("  Webhook URL: " + webhook_url)
    print("=" * 60)
    print()
    print("REQUIRED: Toggle the workflow ON in n8n UI to register the webhook.")
    print()
    print("Test with:")
    print("  curl -X POST " + webhook_url + " \\")
    print("    -H 'Content-Type: application/json' \\")
    print("    -d '{\"customer_name\":\"Tienda\",\"user_id\":\"" + ALEX_USER_ID + "\",")
    print("         \"channel_id\":\"" + DEFAULT_CHANNEL + "\",\"performed_by\":\"Alex\"}'")
    print()
    print("Expected:")
    print("  - Slack confirm in #customer-success-core")
    print("  - MCT '\\uD83D\\uDCDE Last Contact Date' updated to today")
    print("  - New row in CS Actions DB")
    print()
    print("Slack Workflow Builder POST body:")
    print('  {"customer_name": "{{form.customer_name}}",')
    print('   "user_id":       "{{trigger.user.id}}",')
    print('   "channel_id":    "{{trigger.channel.id}}",')
    print('   "performed_by":  "{{trigger.user.display_name}}"}')


if __name__ == "__main__":
    main()
