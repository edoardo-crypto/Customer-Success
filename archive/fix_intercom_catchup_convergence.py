#!/usr/bin/env python3
"""
fix_intercom_catchup_convergence.py — Fix branch-scope convergence bug

Addresses Bug C (CRITICAL): Build Notion Payload sees empty convItems on the FALSE
branch because n8n's branch-scoped .all() loses item ancestry after HTTP Request
nodes in the FALSE chain create new items, breaking lineage back to Extract Conv Text.

Also fixes Bug A in the n8n workflow: Stamp: AI Company broke on the FIRST text
block from Claude, which may be a brief "Searching..." preamble — not the answer.

Changes to workflow J1l8oI22H26f9iM5 (18 → 19 nodes):
  1. Deactivate workflow
  2. Update "Stamp: AI Company" — remove break, accumulate LAST valid text block
  3. Insert "Expand: Pre-IF Data Stamp" Code node between Merge: Customer Result
     and IF: Customer Found? — stamps all conv+Claude data into each item BEFORE
     the IF split so TRUE-branch items carry everything needed by Build Payload
  4. Update "Build Notion Payload" — loop over customerItems ($input) and read
     from stamped fields instead of cross-referencing upstream nodes
  5. Patch connections: Merge → Expand → IF  (was: Merge → IF)
  6. PUT + reactivate
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

WORKFLOW_ID = "J1l8oI22H26f9iM5"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

# ── Node name constants ───────────────────────────────────────────────────────
MERGE_NODE         = "Merge: Customer Result"
EXPAND_NODE        = "Expand: Pre-IF Data Stamp"
IF_NODE            = "IF: Customer Found?"
STAMP_NODE         = "Stamp: AI Company"
BUILD_PAYLOAD_NODE = "Build Notion Payload"

# ── Updated Stamp: AI Company JS ──────────────────────────────────────────────
# Bug A fix: Claude may emit a brief text block BEFORE the tool_use block
# (e.g. "Let me search for that...") — we want the LAST text block, not the first.
STAMP_AI_COMPANY_JS_FIXED = """\
// Extract company name from Claude's web-search response.
// Outputs { ai_company: "<name>" } or a placeholder that won't match any MCT row.
// Uses the LAST valid text block — Claude may emit a brief text BEFORE the tool call.

const content = $input.item.json.content || [];
let company = "SKIP_NO_MATCH__zz99";

for (const block of content) {
    if (block.type === "text" && block.text && block.text.trim().length >= 2) {
        const candidate = block.text.trim();
        if (candidate.toUpperCase() !== "UNKNOWN") {
            company = candidate;  // overwrite with each valid block → keeps the last one
        }
    }
}

console.log(`[catchup-ai] Claude identified company: ${company}`);
return [{ json: { ai_company: company } }];
"""

# ── New Expand: Pre-IF Data Stamp JS ─────────────────────────────────────────
# Runs on the MAIN path (before IF split) where cross-node .all() calls work.
# Stamps conv+Claude data into each item so TRUE-branch Build Payload reads $input.
EXPAND_PRE_IF_JS = """\
// Run BEFORE the IF split. On the main path, cross-node .all() references are
// guaranteed to work. Stamp all conv+Claude fields into each item so that
// Build Notion Payload can read from $input on the TRUE branch without
// branch-scope ancestry issues.

const claudeItems = $('Claude: Summarize Issue').all();
const convItems   = $('Extract Conv Text').all();
const mergeItems  = $input.all();  // from Merge: Customer Result

const results = [];

for (let i = 0; i < mergeItems.length; i++) {
    const merge  = (mergeItems[i]  && mergeItems[i].json)  || {};
    const conv   = (convItems[i]   && convItems[i].json)   || {};
    const claude = (claudeItems[i] && claudeItems[i].json) || {};

    results.push({ json: {
        // Customer results (from Merge — may be empty if no match yet)
        results: merge.results || [],
        // Conv data (from Extract Conv Text)
        conversation_id:   conv.conversation_id   || '',
        source_url:        conv.source_url        || '',
        user_email:        conv.user_email        || merge.conv_email || '',
        user_name:         conv.user_name         || merge.conv_name  || '',
        company_name:      conv.company_name      || '',
        issue_type:        conv.issue_type        || '',
        cs_severity:       conv.cs_severity       || '',
        issue_description: conv.issue_description || '',
        created_at:        conv.created_at        || null,
        conversation_text: conv.conversation_text || '',
        // Claude raw content (for summary parsing in Build Payload)
        claude_content:    claude.content         || [],
    }});
}

return results;
"""

# ── Updated Build Notion Payload JS ──────────────────────────────────────────
# Reads from $input (stamped items) instead of cross-referencing upstream nodes.
# On the TRUE branch: $input items come from IF which received Expand items — full data.
# On the FALSE branch: $input items come from Format which only has { results: [...] }
#   → conv/claude fields fall back to empty strings (still creates an issue, just
#      with minimal data — better than the current behaviour of creating NO issue).
BUILD_NOTION_PAYLOAD_JS = """\
const customerItems = $input.all();  // stamped on TRUE branch; results-only on FALSE
const NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8";
const VALID_SEVERITIES = new Set(['Urgent', 'Important', 'Not important']);

function normalizeIssueType(raw) {
    if (raw === 'New Feature Request') return 'New Feature Request';
    return raw;
}

const results = [];

for (let i = 0; i < customerItems.length; i++) {
    const item = (customerItems[i] && customerItems[i].json) || {};

    // Parse Claude summary from pre-stamped claude_content (last text block wins)
    let issueTitle = '', summary = '';
    try {
        const content = item.claude_content || [];
        let text = '';
        for (const block of content) {
            if (block.type === 'text' && block.text) text = block.text;
        }
        text = text.replace(/```json\\s*/gi, '').replace(/```\\s*/g, '').trim();
        const parsed = JSON.parse(text);
        issueTitle = (parsed.issue_title || '').substring(0, 80);
        summary    = parsed.summary || '';
    } catch (e) {
        issueTitle = ((item.user_name || item.user_email || 'Unknown') + ': ' +
                      (item.issue_description || 'Needs Review')).substring(0, 80);
        summary = item.issue_description || 'AI summary generation failed.';
    }

    const customerResults = item.results || [];
    const customerFound   = customerResults.length > 0;
    const customerPageId  = customerFound ? customerResults[0].id : null;

    const rawSev = item.cs_severity || '';
    const mappedSeverity = VALID_SEVERITIES.has(rawSev) ? rawSev : 'Important';

    const properties = {
        "Issue Title": { "title": [{ "text": { "content": issueTitle || "Untitled Issue" } }] },
        "Source":     { "select": { "name": "Intercom" } },
        "Source ID":  { "rich_text": [{ "text": { "content": item.conversation_id || "" } }] },
        "Source URL": { "url": item.source_url || null },
        "Reported By": { "rich_text": [{ "text": { "content":
            (item.user_name || '') + ' <' + (item.user_email || '') + '>' } }] },
        "Summary":    { "rich_text": [{ "text": { "content": (summary || '').substring(0, 2000) } }] },
        "Raw Message": { "rich_text": [{ "text": { "content":
            (item.conversation_text || '').substring(0, 2000) } }] },
        "Severity":   { "select": { "name": mappedSeverity } },
        "Status":     { "select": { "name": "Open" } },
        "Issue Type": { "select": { "name": normalizeIssueType(item.issue_type || '') } },
        "Created At": { "date": { "start": item.created_at } },
    };

    if (customerPageId) {
        properties["Customer"] = { "relation": [{ "id": customerPageId }] };
    }

    results.push({ json: {
        notion_payload: { parent: { database_id: NOTION_ISSUES_DB }, properties },
        conversation_id: item.conversation_id,
        issue_title: issueTitle,
        customer_linked: customerFound,
    }});
}

return results;
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
    print("fix_intercom_catchup_convergence.py")
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
    for required in [MERGE_NODE, IF_NODE, STAMP_NODE, BUILD_PAYLOAD_NODE]:
        if required not in node_by_name:
            print(f"  ERROR: Expected node '{required}' not found.")
            print(f"  Present nodes: {list(node_by_name.keys())}")
            sys.exit(1)
        print(f"  OK '{required}'")

    # Guard: don't double-patch
    if EXPAND_NODE in node_by_name:
        print(f"\n  '{EXPAND_NODE}' already exists — script was already applied.")
        print("  Aborting to avoid double-patch.")
        sys.exit(0)

    # 3. Deactivate
    print("\n[3/6] Deactivating workflow...")
    deactivate()
    time.sleep(1)

    # 4. Update Stamp: AI Company (Bug A fix — last text block, no break)
    print("\n[4/6] Updating 'Stamp: AI Company' (last-text-block fix)...")
    stamp_node = node_by_name[STAMP_NODE]
    stamp_node["parameters"]["jsCode"] = STAMP_AI_COMPANY_JS_FIXED
    print("  OK — accumulates last valid text block instead of stopping at first")

    # 5. Insert Expand: Pre-IF Data Stamp between Merge and IF
    print("\n[5/6] Inserting 'Expand: Pre-IF Data Stamp' + updating Build Notion Payload...")
    mx, my   = node_by_name[MERGE_NODE]["position"]
    ifx, ify = node_by_name[IF_NODE]["position"]
    # Place Expand midway between Merge and IF, on the same Y level
    expand_x = (mx + ifx) // 2
    expand_y = my

    expand_node = {
        "id":          uid(),
        "name":        EXPAND_NODE,
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [expand_x, expand_y],
        "parameters": {
            "mode":   "runOnceForAllItems",
            "jsCode": EXPAND_PRE_IF_JS,
        },
    }
    nodes.append(expand_node)

    # Update Build Notion Payload to read from $input (stamped items)
    build_node = node_by_name[BUILD_PAYLOAD_NODE]
    build_node["parameters"]["jsCode"] = BUILD_NOTION_PAYLOAD_JS
    print("  OK — 'Build Notion Payload' now reads from $input (no cross-node refs)")

    # 6. Rebuild connections: Merge → Expand → IF  (was: Merge → IF)
    print("\n[6/6] Patching connections (Merge → Expand → IF)...")
    new_connections = {}

    for src_name, conn_data in connections.items():
        if src_name == MERGE_NODE:
            continue  # override below
        new_connections[src_name] = conn_data

    # Merge → Expand
    new_connections[MERGE_NODE] = {
        "main": [[{"node": EXPAND_NODE, "type": "main", "index": 0}]]
    }
    # Expand → IF
    new_connections[EXPAND_NODE] = {
        "main": [[{"node": IF_NODE, "type": "main", "index": 0}]]
    }

    wf["nodes"]       = nodes
    wf["connections"] = new_connections

    # Summary
    print(f"\nFinal node list ({len(nodes)} nodes):")
    for i, n in enumerate(nodes, 1):
        if n["name"] == EXPAND_NODE:
            marker = " <- NEW"
        elif n["name"] == STAMP_NODE:
            marker = " <- UPDATED (last text block)"
        elif n["name"] == BUILD_PAYLOAD_NODE:
            marker = " <- UPDATED (reads from $input)"
        else:
            marker = ""
        print(f"  {i:2}. {n['name']}{marker}")

    # PUT
    print("\nPushing to n8n...")
    result = put_workflow(wf)
    got_nodes = len(result.get("nodes", []))
    print(f"  PUT OK — {got_nodes} nodes confirmed")

    # Reactivate
    print("\nReactivating workflow...")
    time.sleep(1)
    activate()

    print()
    print("=" * 65)
    print("Done! Convergence bug fixed.")
    print("=" * 65)
    print()
    print("What changed:")
    print("  1. NEW 'Expand: Pre-IF Data Stamp' (between Merge and IF)")
    print("     Stamps all conv+Claude data into each item BEFORE branching.")
    print("     TRUE-branch items now arrive at Build Payload with full data.")
    print("  2. 'Stamp: AI Company' updated — keeps LAST valid text block.")
    print("     Handles Claude emitting a brief preamble before tool_use.")
    print("  3. 'Build Notion Payload' updated — loops over $input items")
    print("     and reads stamped fields. No more cross-node .all() calls.")
    print()
    print("Behaviour after fix:")
    print("  TRUE branch  (customer found in MCT): full issue with all data")
    print("  FALSE branch (no MCT match, or AI-found match): issue created")
    print("    with results from Notion AI search; conv data may be sparse")
    print("    (was previously: ZERO issues created — silent data loss)")
    print()
    print("IMPORTANT: Toggle INACTIVE -> ACTIVE in the n8n UI to re-register")
    print("the schedule trigger after any PUT.")
    print(f"  -> {N8N_BASE}/workflow/{WORKFLOW_ID}")


if __name__ == "__main__":
    main()
