#!/usr/bin/env python3
"""
fix_intercom_catchup_ai_search.py — Add Claude web-search fallback to Intercom catchup

Modifies workflow J1l8oI22H26f9iM5 (Intercom Catch-up Polling) to call Claude
with the web_search tool when both email-domain AND company-name lookups fail to
find a customer in MCT.  Claude searches LinkedIn/Google to identify the company
the contact works for, then searches MCT by that company name.

Changes (13 → 18 nodes):
  1. Patch "Merge: Customer Result" JS — emit conv_email + conv_name downstream
  2. Insert "IF: Customer Found?"     — routes matched vs unmatched
  3. FALSE branch: Claude web search → Stamp AI company → Notion search → Format
  4. TRUE  branch: direct pass-through to Build Notion Payload (unchanged)
  5. Both branches converge at "Build Notion Payload" (unchanged)
  6. Shift "Build Notion Payload" and "Notion: Create Issue" right by +1500px
"""

import json
import uuid
import time
import requests
import sys

# ── Config ────────────────────────────────────────────────────────────────────
N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = (
    "***REMOVED_JWT***"
    ".eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJp"
    "c3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleH"
    "AiOjE3NzMyNzAwMDB9.X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)
NOTION_TOKEN = "***REMOVED***"
NOTION_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"
ANTHROPIC_KEY = (
    "***REMOVED***"
    "CxzATZqnMZonZicxgwR2LlsWw-446IlgAA"
)

WORKFLOW_ID = "J1l8oI22H26f9iM5"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

# ── Node name constants ───────────────────────────────────────────────────────
MERGE_NODE         = "Merge: Customer Result"
IF_NODE            = "IF: Customer Found?"
CLAUDE_AI_NODE     = "Claude: Find Company via Web Search"
STAMP_NODE         = "Stamp: AI Company"
NOTION_AI_NODE     = "Notion: Find Customer by AI Company"
FORMAT_NODE        = "Format: AI Customer Result"
BUILD_PAYLOAD_NODE = "Build Notion Payload"
CREATE_ISSUE_NODE  = "Notion: Create Issue"

# ── Strings to patch in Merge: Customer Result ────────────────────────────────
# The current Merge node outputs { results: [...] }.
# We add conv_email + conv_name so the FALSE branch can pass them to Claude.
OLD_MERGE_PUSH = (
    "results.push({ json: { results: customerResults } });"
)
NEW_MERGE_PUSH = (
    "results.push({ json: { results: customerResults, "
    "conv_email: convData.user_email || '', "
    "conv_name: convData.user_name || '' } });"
)

# ── Code node JS strings ──────────────────────────────────────────────────────

STAMP_AI_COMPANY_JS = """\
// Extract company name from Claude's web-search response.
// Outputs { ai_company: "<name>" } or a placeholder that won't match any MCT row.

const content = $input.item.json.content || [];
let company = "SKIP_NO_MATCH__zz99";

for (const block of content) {
    if (
        block.type === "text" &&
        block.text &&
        block.text.trim().toUpperCase() !== "UNKNOWN" &&
        block.text.trim().length >= 2
    ) {
        company = block.text.trim();
        break;
    }
}

console.log(`[catchup-ai] Claude identified company: ${company}`);
return [{ json: { ai_company: company } }];
"""

FORMAT_AI_RESULT_JS = """\
// Normalise Notion query results from AI company lookup.
// Outputs { results: [...] } — same shape as "Merge: Customer Result" —
// so "Build Notion Payload" requires no changes.

const items = $input.all();
return items.map(item => ({ json: { results: item.json.results || [] } }));
"""


def uid():
    return str(uuid.uuid4())


# ── n8n API helpers ──────────────────────────────────────────────────────────

def get_workflow():
    r = requests.get(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}",
        headers=N8N_HEADERS,
    )
    r.raise_for_status()
    return r.json()


def put_workflow(wf):
    """PUT updated workflow — only name/nodes/connections/settings accepted."""
    payload = {
        "name":        wf["name"],
        "nodes":       wf["nodes"],
        "connections": wf["connections"],
        "settings":    wf.get("settings", {}),
    }
    r = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}",
        headers=N8N_HEADERS,
        json=payload,
    )
    if r.status_code not in (200, 201):
        print(f"  PUT failed: {r.status_code}")
        print(f"  Response: {r.text[:600]}")
        r.raise_for_status()
    return r.json()


def deactivate():
    r = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate",
        headers=N8N_HEADERS,
    )
    if r.status_code in (200, 204):
        print("  Deactivated")
    else:
        print(f"  Deactivate returned {r.status_code} (may already be inactive)")


def activate():
    r = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate",
        headers=N8N_HEADERS,
    )
    if r.status_code in (200, 204):
        print("  Activated")
    else:
        print(f"  Activate returned {r.status_code}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("fix_intercom_catchup_ai_search.py")
    print(f"Workflow: {WORKFLOW_ID}")
    print("=" * 65)

    # 1. Fetch current workflow
    print("\n[1/6] Fetching current workflow...")
    wf = get_workflow()
    print(f"  Name:   {wf['name']}")
    print(f"  Nodes:  {len(wf['nodes'])}")
    print(f"  Active: {wf.get('active')}")

    nodes        = wf["nodes"]
    connections  = wf["connections"]
    node_by_name = {n["name"]: n for n in nodes}

    # 2. Safety checks
    print("\n[2/6] Validating node structure...")
    for required in [MERGE_NODE, BUILD_PAYLOAD_NODE, CREATE_ISSUE_NODE]:
        if required not in node_by_name:
            print(f"  ERROR: Expected node '{required}' not found.")
            print(f"  Present nodes: {list(node_by_name.keys())}")
            sys.exit(1)
        print(f"  ✓ '{required}'")

    # Guard: don't double-patch
    if IF_NODE in node_by_name:
        print(f"\n  '{IF_NODE}' already exists — script was already applied.")
        print("  Aborting to avoid double-patch.")
        sys.exit(0)

    # 3. Deactivate
    print("\n[3/6] Deactivating workflow...")
    deactivate()
    time.sleep(1)

    # 4. Patch "Merge: Customer Result" to emit conv_email + conv_name
    print("\n[4/6] Patching 'Merge: Customer Result' node...")
    merge_node = node_by_name[MERGE_NODE]
    js = merge_node["parameters"]["jsCode"]

    if OLD_MERGE_PUSH not in js:
        print("  ERROR: Expected push line not found in Merge node JS.")
        print("  The workflow may have been modified manually.")
        print(f"  Snippet (last 300 chars):\n{js[-300:]}")
        print("  Aborting — manual inspection required.")
        sys.exit(1)

    merge_node["parameters"]["jsCode"] = js.replace(OLD_MERGE_PUSH, NEW_MERGE_PUSH)
    print("  ✓ Patched: conv_email + conv_name added to Merge output")

    # 5. Insert 5 new nodes + shift Build/Create positions
    print("\n[5/6] Building modified node list...")

    # Read current positions
    mx, my    = node_by_name[MERGE_NODE]["position"]
    bx, by    = node_by_name[BUILD_PAYLOAD_NODE]["position"]
    crx, cry  = node_by_name[CREATE_ISSUE_NODE]["position"]

    # Shift Build Payload and Create Issue right to make room
    node_by_name[BUILD_PAYLOAD_NODE]["position"] = [bx  + 1500, by]
    node_by_name[CREATE_ISSUE_NODE]["position"]  = [crx + 1500, cry]

    y_false = my + 300  # FALSE branch drops visually below the main line

    # ── New node 1: IF: Customer Found? ──────────────────────────────────────
    if_node = {
        "id":          uid(),
        "name":        IF_NODE,
        "type":        "n8n-nodes-base.if",
        "typeVersion": 1,
        "position":    [mx + 250, my],
        "parameters": {
            "conditions": {
                "boolean": [
                    {
                        "value1":    "={{ $json.results && $json.results.length > 0 }}",
                        "operation": "equal",
                        "value2":    True,
                    }
                ]
            }
        },
    }

    # ── New node 2: Claude: Find Company via Web Search ───────────────────────
    # Body expression: avoid }}  inside strings (n8n tmpl parser issue).
    # All }} here are JavaScript object-closing braces, not string literals — safe.
    ai_search_body = (
        '={{ JSON.stringify({'
        '"model": "claude-haiku-4-5-20251001",'
        '"max_tokens": 50,'
        '"tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}],'
        '"messages": [{"role": "user", "content":'
        '"What company does " + ($json.conv_name || $json.conv_email || "this person") + '
        '" work for? Their email is " + ($json.conv_email || "unknown") + '
        '". Reply with ONLY the company name, nothing else. If you cannot find it, reply with exactly: UNKNOWN"'
        '}]}) }}'
    )
    claude_ai_node = {
        "id":          uid(),
        "name":        CLAUDE_AI_NODE,
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [mx + 500, y_false],
        "parameters": {
            "method": "POST",
            "url":    "https://api.anthropic.com/v1/messages",
            "sendHeaders": True,
            "headerParameters": {
                "parameters": [
                    {"name": "x-api-key",        "value": ANTHROPIC_KEY},
                    {"name": "anthropic-version", "value": "2023-06-01"},
                    {"name": "anthropic-beta",    "value": "web-search-2025-03-05"},
                    {"name": "Content-Type",      "value": "application/json"},
                ]
            },
            "sendBody":    True,
            "specifyBody": "json",
            "jsonBody":    ai_search_body,
            "options":     {"continueOnFail": True},
        },
    }

    # ── New node 3: Stamp: AI Company ─────────────────────────────────────────
    stamp_node = {
        "id":          uid(),
        "name":        STAMP_NODE,
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [mx + 750, y_false],
        "parameters": {
            "mode":   "runOnceForEachItem",
            "jsCode": STAMP_AI_COMPANY_JS,
        },
    }

    # ── New node 4: Notion: Find Customer by AI Company ───────────────────────
    # Stamp always outputs a value (real company or SKIP_NO_MATCH__zz99),
    # so no extra guard is needed inside the body.
    notion_ai_body = (
        '={{ JSON.stringify({'
        '"filter": {"property": "title", "title": {"contains": $json.ai_company}}'
        '}) }}'
    )
    notion_ai_node = {
        "id":          uid(),
        "name":        NOTION_AI_NODE,
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [mx + 1000, y_false],
        "parameters": {
            "method": "POST",
            "url":    f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
            "sendHeaders": True,
            "headerParameters": {
                "parameters": [
                    {"name": "Authorization",  "value": f"Bearer {NOTION_TOKEN}"},
                    {"name": "Notion-Version", "value": "2025-09-03"},
                ]
            },
            "sendBody":    True,
            "specifyBody": "json",
            "jsonBody":    notion_ai_body,
            "options":     {"continueOnFail": True},
        },
    }

    # ── New node 5: Format: AI Customer Result ────────────────────────────────
    format_node = {
        "id":          uid(),
        "name":        FORMAT_NODE,
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [mx + 1250, y_false],
        "parameters": {
            "mode":   "runOnceForAllItems",
            "jsCode": FORMAT_AI_RESULT_JS,
        },
    }

    nodes.extend([if_node, claude_ai_node, stamp_node, notion_ai_node, format_node])

    # 6. Rebuild connections
    print("\n[6/6] Rebuilding connections...")

    new_connections = {}

    for src_name, conn_data in connections.items():
        if src_name == MERGE_NODE:
            continue  # overridden below
        new_connections[src_name] = conn_data

    # Merge: Customer Result → IF: Customer Found?
    new_connections[MERGE_NODE] = {
        "main": [[{"node": IF_NODE, "type": "main", "index": 0}]]
    }

    # IF: Customer Found?
    #   TRUE  (main[0]) → Build Notion Payload   (fast path: customer already found)
    #   FALSE (main[1]) → Claude: Find Company   (slow path: ask AI)
    new_connections[IF_NODE] = {
        "main": [
            [{"node": BUILD_PAYLOAD_NODE, "type": "main", "index": 0}],  # TRUE
            [{"node": CLAUDE_AI_NODE,     "type": "main", "index": 0}],  # FALSE
        ]
    }

    # FALSE branch chain
    new_connections[CLAUDE_AI_NODE] = {
        "main": [[{"node": STAMP_NODE, "type": "main", "index": 0}]]
    }
    new_connections[STAMP_NODE] = {
        "main": [[{"node": NOTION_AI_NODE, "type": "main", "index": 0}]]
    }
    new_connections[NOTION_AI_NODE] = {
        "main": [[{"node": FORMAT_NODE, "type": "main", "index": 0}]]
    }
    new_connections[FORMAT_NODE] = {
        "main": [[{"node": BUILD_PAYLOAD_NODE, "type": "main", "index": 0}]]
    }

    wf["nodes"]       = nodes
    wf["connections"] = new_connections

    # Print summary
    new_node_names = {IF_NODE, CLAUDE_AI_NODE, STAMP_NODE, NOTION_AI_NODE, FORMAT_NODE}
    print(f"\nFinal node list ({len(nodes)} nodes):")
    for i, n in enumerate(nodes, 1):
        if n["name"] in new_node_names:
            marker = " ← NEW"
        elif n["name"] == MERGE_NODE:
            marker = " ← PATCHED"
        elif n["name"] in (BUILD_PAYLOAD_NODE, CREATE_ISSUE_NODE):
            marker = " ← SHIFTED +1500px"
        else:
            marker = ""
        print(f"  {i:2}. {n['name']}{marker}")

    # PUT updated workflow
    print("\nPushing to n8n...")
    result = put_workflow(wf)
    got_nodes = len(result.get("nodes", []))
    print(f"  PUT OK — {got_nodes} nodes confirmed")

    # Re-activate
    print("\nReactivating workflow...")
    time.sleep(1)
    activate()

    print()
    print("=" * 65)
    print("Done! Claude web-search fallback added to Intercom catchup workflow.")
    print("=" * 65)
    print()
    print("What changed:")
    print("  1. 'Merge: Customer Result' now emits conv_email + conv_name so")
    print("     they survive into the FALSE branch for Claude to use")
    print("  2. New: 'IF: Customer Found?' routes matched items directly to")
    print("     Build Notion Payload (TRUE) and unmatched items to Claude (FALSE)")
    print("  3. New FALSE branch:")
    print("     • 'Claude: Find Company via Web Search' — calls Anthropic API")
    print("       with web_search tool to identify the person's employer")
    print("     • 'Stamp: AI Company' — extracts company name from Claude response")
    print("     • 'Notion: Find Customer by AI Company' — MCT title search")
    print("     • 'Format: AI Customer Result' — normalises output shape")
    print("  4. 'Build Notion Payload' and 'Notion: Create Issue' UNCHANGED")
    print()
    print("IMPORTANT: Toggle INACTIVE → ACTIVE in the n8n UI to re-register")
    print("the schedule trigger after any PUT.")
    print(f"  → {N8N_BASE}/workflow/{WORKFLOW_ID}")


if __name__ == "__main__":
    main()
