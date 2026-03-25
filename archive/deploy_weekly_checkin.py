#!/usr/bin/env python3
"""Deploy 'Weekly Check-in Reminder' workflow to n8n.

Flow (every Monday 08:00 Europe/Berlin):
  Schedule Trigger
    → HTTP 'Fetch MCT Page 1'    (POST data_sources query, page_size=100)
    → HTTP 'Fetch MCT Page 2'    (same, with next_cursor if present)
    → Code 'Filter & Group'      (dedup + filter 21-28d + group by CS owner)
    → IF 'Any Matches?'          ($json.total > 0)
       └─[true] HTTP 'Send Slack'  (chat.postMessage → #customer-success-core)
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
NOTION_CRED_ID   = "LH587kxanQCPcd9y"
NOTION_CRED_NAME = "Notion - Enrichment"
SLACK_CRED_ID    = "IMuEGtYutmUKwCqY"
SLACK_CRED_NAME  = "Slack Bot for Alerts"
SLACK_CHANNEL    = "C0AGZDTUND6"   # #customer-success-core — change here if needed

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
    return {
        "sendHeaders": True,
        "headerParameters": {
            "parameters": [{"name": "Notion-Version", "value": "2025-09-03"}]
        },
    }


# ── JS for the "Filter & Group" Code node ─────────────────────────────────────
# Written as a plain triple-quoted string (no f-string) to avoid brace confusion.
# Unicode escapes are intentional: \\uXXXX → stored as \uXXXX in JSON → JS
# interprets as the actual emoji character when evaluating the source code.
FILTER_GROUP_JS = """\
const page1 = $('Fetch MCT Page 1').first().json.results || [];
const page2 = $input.first().json.results || [];

// Deduplicate across pages
const seen = new Set();
const all = [...page1, ...page2].filter(r => {
  if (seen.has(r.id)) return false;
  seen.add(r.id);
  return true;
});

// Keep only Active customers approaching the 28-day no-contact threshold
const toContact = all.filter(r => {
  const p       = r.properties || {};
  const days    = p['\\uD83D\\uDCDE Days Since Last Contact']?.formula?.number;
  const stage   = p['\\u2764\\uFE0F Journey Stage']?.formula?.string || '';
  const billing = p['\\uD83D\\uDCB0 Billing Status']?.select?.name  || '';
  return days >= 21 && days <= 28
      && billing === 'Active'
      && !stage.includes('Churned');
});

const alex = [], aya = [];
for (const r of toContact) {
  const p     = r.properties || {};
  const owner = p['\\u2B50 CS Owner']?.select?.name || '';
  const entry = {
    name:  p['\\uD83C\\uDFE2 Company Name']?.title?.[0]?.plain_text || '?',
    days:  p['\\uD83D\\uDCDE Days Since Last Contact']?.formula?.number,
    stage: p['\\u2764\\uFE0F Journey Stage']?.formula?.string || '',
    tier:  p['\\uD83D\\uDCB0 Plan Tier']?.select?.name || '',
  };
  if (owner === 'Alex') alex.push(entry);
  else if (owner === 'Aya') aya.push(entry);
}

const today = new Date().toLocaleDateString('en-GB', {
  day: 'numeric', month: 'long', year: 'numeric'
});

const fmt = (name, items) => {
  if (!items.length) return '';
  const rows = items.map(c =>
    '\\u2022 ' + c.name + ' \\u2014 ' + c.days + 'd, ' + c.stage + (c.tier ? ', ' + c.tier : '')
  ).join('\\n');
  return '\\n*' + name + ' (' + items.length + ' customer' + (items.length > 1 ? 's' : '') + ' approaching 28-day threshold):*\\n' + rows;
};

const text = [
  '\\uD83D\\uDCDE *Weekly Check-in Reminder \\u2014 ' + today + '*',
  fmt('Alex', alex),
  fmt('Aya', aya),
  '\\n_Act before 28 days to keep these accounts on track._',
].filter(Boolean).join('\\n');

return [{ json: { alex, aya, total: toContact.length, slack_text: text } }];
"""


def build_workflow():
    nodes = [
        # ── 1. Schedule Trigger ───────────────────────────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Schedule Trigger",
            "type":        "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1,
            "position":    [0, 0],
            "parameters": {
                "rule": {
                    "interval": [{"field": "cronExpression", "expression": "0 8 * * 1"}]
                },
                "timezone": "Europe/Berlin",
            },
        },
        # ── 2. Fetch MCT Page 1 ───────────────────────────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Fetch MCT Page 1",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position":    [280, 0],
            "credentials": notion_cred(),
            "parameters": {
                **notion_auth(),
                "method":         "POST",
                "url":            MCT_URL,
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                "body":           '{"page_size": 100}',
                **notion_header_v3(),
            },
        },
        # ── 3. Fetch MCT Page 2 ───────────────────────────────────────────────
        # continueOnFail: true guards against edge case where next_cursor is null
        # (single-page result — the expression evaluates to {page_size: 100} only)
        {
            "id":             str(uuid.uuid4()),
            "name":           "Fetch MCT Page 2",
            "type":           "n8n-nodes-base.httpRequest",
            "typeVersion":    4,
            "position":       [560, 0],
            "continueOnFail": True,
            "credentials":    notion_cred(),
            "parameters": {
                **notion_auth(),
                "method":         "POST",
                "url":            MCT_URL,
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                # The ternary inside spreads a cursor key only when present.
                # Braces: { "page_size": 100, ...( ? {"start_cursor": X} : {} ) }
                # No }} pattern inside the expression — safe for n8n template parser.
                "body": (
                    "={{ JSON.stringify({ \"page_size\": 100,"
                    " ...($('Fetch MCT Page 1').item.json.next_cursor"
                    " ? {\"start_cursor\": $('Fetch MCT Page 1').item.json.next_cursor}"
                    " : {}) }) }}"
                ),
                **notion_header_v3(),
            },
        },
        # ── 4. Code: Filter & Group ───────────────────────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Filter & Group",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    [840, 0],
            "parameters": {
                "mode":   "runOnceForAllItems",
                "jsCode": FILTER_GROUP_JS,
            },
        },
        # ── 5. IF: Any Matches? ───────────────────────────────────────────────
        {
            "id":          str(uuid.uuid4()),
            "name":        "Any Matches?",
            "type":        "n8n-nodes-base.if",
            "typeVersion": 1,
            "position":    [1120, 0],
            "parameters": {
                "conditions": {
                    "number": [{
                        "value1":    "={{ $json.total }}",
                        "operation": "larger",
                        "value2":    0,
                    }]
                }
            },
        },
        # ── 6. HTTP: Send Slack (true branch) ─────────────────────────────────
        # slack_text is a pre-built string from Filter & Group — plain field
        # reference means no nested }} in this expression.
        {
            "id":          str(uuid.uuid4()),
            "name":        "Send Slack",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position":    [1400, -120],
            "credentials": slack_cred(),
            "parameters": {
                "authentication": "genericCredentialType",
                "genericAuthType": "httpHeaderAuth",
                "method":         "POST",
                "url":            "https://slack.com/api/chat.postMessage",
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json; charset=utf-8",
                "body": (
                    '={{ JSON.stringify({ channel: "' + SLACK_CHANNEL + '",'
                    " text: $json.slack_text, unfurl_links: false }) }}"
                ),
            },
        },
    ]

    connections = {
        "Schedule Trigger": {
            "main": [[{"node": "Fetch MCT Page 1", "type": "main", "index": 0}]]
        },
        "Fetch MCT Page 1": {
            "main": [[{"node": "Fetch MCT Page 2", "type": "main", "index": 0}]]
        },
        "Fetch MCT Page 2": {
            "main": [[{"node": "Filter & Group", "type": "main", "index": 0}]]
        },
        "Filter & Group": {
            "main": [[{"node": "Any Matches?", "type": "main", "index": 0}]]
        },
        "Any Matches?": {
            "main": [
                [{"node": "Send Slack", "type": "main", "index": 0}],  # true
                [],                                                      # false → stop
            ]
        },
    }

    return nodes, connections


def main():
    nodes, connections = build_workflow()

    wf_body = {
        "name": "Weekly Check-in Reminder",
        "nodes": nodes,
        "connections": connections,
        "settings": {"timezone": "Europe/Berlin", "executionOrder": "v1"},
    }

    print("Step 1/2 — Creating workflow...")
    result = n8n("POST", "/workflows", wf_body)
    wf_id = result["id"]
    print("  + Created  id=" + wf_id + "  name='" + result["name"] + "'")

    print("Step 2/2 — Activating (Schedule Trigger — no UI toggle required)...")
    n8n("POST", "/workflows/" + wf_id + "/activate")
    print("  + Activated")

    print()
    print("=" * 60)
    print("  Workflow : " + N8N_BASE + "/workflow/" + wf_id)
    print("  Schedule : Every Monday 08:00 Europe/Berlin")
    print("  Slack ch : " + SLACK_CHANNEL + "  (#customer-success-core)")
    print("=" * 60)
    print()
    print("Verification:")
    print("  1. Open workflow in n8n UI → click 'Test workflow'")
    print("     If any MCT customers are 21-28d without contact,")
    print("     a message appears in #customer-success-core.")
    print("  2. No matches → IF node stops silently (correct).")


if __name__ == "__main__":
    main()
