#!/usr/bin/env python3
"""
add_intercom_category_classification.py — Feb 26, 2026

Adds category auto-classification to both Intercom n8n workflows:

1. Pipeline v2 (3AO3SRUK80rcOCgQ):
   Extends the existing Claude "Summarize Issue" call to also return "category"
   in the same JSON — zero extra API calls, zero new nodes.
   Patches 3 nodes by ID: Claude node (prompt), Parse node (extraction), Build node (guard).

2. Catchup Poll (J1l8oI22H26f9iM5):
   Inserts a new "Claude: Classify Issue" HTTP Request node between
   "Extract Conv Text" and its downstream target. Patches "Expand: Pre-IF Data Stamp"
   to read the category, and "Build Notion Payload" to set it on the Notion page.

Both workflows are fully idempotent — safe to re-run.
"""

import json
import uuid
import time
import requests
import sys
import creds

# ── Config ────────────────────────────────────────────────────────────────────
N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
ANTHROPIC_KEY = creds.get("ANTHROPIC_API_KEY")

PIPELINE_ID = "3AO3SRUK80rcOCgQ"
CATCHUP_ID  = "J1l8oI22H26f9iM5"

# Pipeline v2 node IDs (stable across edits)
CLAUDE_NODE_ID = "f481203b-43de-4ce9-87b8-bf576d0d3617"
PARSE_NODE_ID  = "f55665bc-4fbb-4419-af85-eb1c4191c0b8"
BUILD_NODE_ID  = "ebaf8800-06f9-458f-92c9-60ae532839ec"

HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

# ── n8n API helpers ───────────────────────────────────────────────────────────

def get_workflow(wf_id):
    r = requests.get(f"{N8N_BASE}/api/v1/workflows/{wf_id}", headers=HEADERS)
    r.raise_for_status()
    return r.json()


def put_workflow(wf_id, wf):
    """PUT updated workflow — only name/nodes/connections/settings accepted."""
    payload = {
        "name":        wf["name"],
        "nodes":       wf["nodes"],
        "connections": wf["connections"],
        "settings":    wf.get("settings", {}),
    }
    r = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{wf_id}",
        headers=HEADERS,
        json=payload,
    )
    if r.status_code not in (200, 201):
        print(f"  PUT failed {r.status_code}: {r.text[:500]}")
        r.raise_for_status()
    return r.json()


def deactivate(wf_id):
    r = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{wf_id}/deactivate",
        headers=HEADERS,
    )
    print(f"  deactivate → {r.status_code}")


def activate(wf_id):
    r = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{wf_id}/activate",
        headers=HEADERS,
    )
    print(f"  activate   → {r.status_code}")


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline v2 patches
# ═══════════════════════════════════════════════════════════════════════════════

def patch_claude_node(node):
    """
    Extend the Claude prompt so it returns 'category' alongside issue_title/summary.

    The jsonBody is an n8n expression string. After requests.json() parses the API
    response, the Python string contains literal \\n (backslash+n) and \\" (backslash+
    dquote) exactly as they appear inside a JSON string value — NOT as escape sequences.
    """
    body = node["parameters"]["jsonBody"]

    # Old tail in the Python string (backslash chars are real backslashes here):
    OLD_TAIL = (
        'Response format (JSON only):\\n'
        '{\\"issue_title\\": \\"<max 80 chars>\\", \\"summary\\": \\"<2-3 sentences>\\"}"}]}) }}'
    )
    NEW_TAIL = (
        'Response format (JSON only):\\n'
        '{\\"issue_title\\": \\"<max 80 chars>\\", \\"summary\\": \\"<2-3 sentences>\\", '
        '\\"category\\": \\"<one of: Feature request, AI Behavior, Integration, '
        'Platform & UI, Billing & Account>\\"}'
        '\\n\\nCategory rules:\\n'
        '- Feature request: customer asking for new features or improvements\\n'
        '- AI Behavior: AI quality, responses, or conversation flow\\n'
        '- Integration: third-party tool connections (Shopify, WhatsApp, CRMs)\\n'
        '- Platform & UI: dashboard bugs or usability issues\\n'
        '- Billing & Account: payments, access, or account management\\n'
        'Pick the single best-matching category."}]}) }}'
    )

    idx = body.rfind(OLD_TAIL)
    if idx == -1:
        print(f"  DEBUG — last 400 chars of Claude jsonBody:\n{body[-400:]!r}")
        raise ValueError(
            "Could not find old prompt tail in Claude node jsonBody. "
            "See DEBUG output above."
        )

    node["parameters"]["jsonBody"] = body[:idx] + NEW_TAIL
    return node


def patch_parse_node(node):
    """
    After JSON.parse(text), extract parsed.category and validate it against
    the five allowed values. Surface category in the returned item.
    """
    js = node["parameters"]["jsCode"]

    # Idempotency guard
    if "VALID_CATEGORIES" in js:
        print("  Parse node already has VALID_CATEGORIES — skipping (idempotent)")
        return node

    # ── Insert category extraction before the outer "const extractData" line ──
    # The catch block also has "const extractData" but with 4-space indent.
    # The outer declaration has no leading spaces after the newline, so we
    # can distinguish it by including the following "const notionResult" line.
    OUTER_ANCHOR = '\nconst extractData = $("Extract Intercom Data").item.json;\nconst notionResult'
    idx = js.find(OUTER_ANCHOR)
    if idx == -1:
        # Fallback: rfind the plain line (picks the last = outer occurrence)
        OUTER_ANCHOR = '\nconst extractData = $("Extract Intercom Data").item.json;'
        idx = js.rfind(OUTER_ANCHOR)
        if idx == -1:
            print(f"  DEBUG — parse JS middle section:\n{js[400:900]!r}")
            raise ValueError("Could not find outer extractData anchor in Parse AI Summary JS")

    CATEGORY_BLOCK = (
        "\n"
        "const VALID_CATEGORIES = new Set([\n"
        "    'Feature request', 'AI Behavior', 'Integration',\n"
        "    'Platform & UI', 'Billing & Account'\n"
        "]);\n"
        "const rawCategory = (parsed.category || '').trim();\n"
        "const category = VALID_CATEGORIES.has(rawCategory) ? rawCategory : '';\n"
    )
    js = js[:idx] + CATEGORY_BLOCK + js[idx:]

    # ── Add category to the return object (as last field after needs_classification) ──
    # Try with trailing semicolon first (raw string ends the module with };)
    OLD_RETURN = "        needs_classification: needsClassification\n    }\n}];"
    NEW_RETURN = "        needs_classification: needsClassification,\n        category\n    }\n}];"
    if OLD_RETURN not in js:
        OLD_RETURN = "        needs_classification: needsClassification\n    }\n}]"
        NEW_RETURN = "        needs_classification: needsClassification,\n        category\n    }\n}]"
        if OLD_RETURN not in js:
            print(f"  DEBUG — tail of parse JS:\n{js[-300:]!r}")
            raise ValueError("Could not find return object anchor in Parse AI Summary JS")

    js = js.replace(OLD_RETURN, NEW_RETURN, 1)
    node["parameters"]["jsCode"] = js
    return node


def patch_build_node_pipeline(node):
    """Replace the 'no longer set by Claude' comment with an active category guard."""
    js = node["parameters"]["jsCode"]

    OLD_COMMENT = (
        "// Category and Root Cause: no longer set by Claude\n"
        "// CS team fills these manually in Notion"
    )
    NEW_CODE = (
        "// Category: auto-classified by Claude\n"
        "if (d.category) {\n"
        "    properties[\"Category\"] = { \"select\": { \"name\": d.category } };\n"
        "}"
    )

    if OLD_COMMENT not in js:
        if 'properties["Category"]' in js:
            print("  Build node already has Category guard — skipping (idempotent)")
            return node
        idx = js.find("Category")
        if idx != -1:
            print(f"  DEBUG — context around 'Category':\n{js[max(0,idx-30):idx+100]!r}")
        raise ValueError("Could not find category comment in Build Notion Issue Payload JS")

    js = js.replace(OLD_COMMENT, NEW_CODE, 1)
    node["parameters"]["jsCode"] = js
    return node


def run_pipeline_v2():
    print("\n" + "=" * 65)
    print(f"[1/2] Pipeline v2 — {PIPELINE_ID}")
    print("=" * 65)

    print("\n  Fetching workflow...")
    wf = get_workflow(PIPELINE_ID)
    print(f"  Name: {wf['name']}   Nodes: {len(wf['nodes'])}")

    # Idempotency: check if Parse node already has category logic
    node_by_id = {n.get("id", ""): n for n in wf["nodes"]}
    parse_js = node_by_id.get(PARSE_NODE_ID, {}).get("parameters", {}).get("jsCode", "")
    if "VALID_CATEGORIES" in parse_js:
        print("\n  Category logic already present — Pipeline v2 already patched. Skipping.")
        return

    print("\n  Deactivating...")
    deactivate(PIPELINE_ID)
    time.sleep(1)

    print("\n  Patching 3 nodes...")
    patched = {"claude": False, "parse": False, "build": False}

    for node in wf["nodes"]:
        nid = node.get("id", "")
        if nid == CLAUDE_NODE_ID:
            patch_claude_node(node)
            patched["claude"] = True
            print("  ✓ Claude node — prompt extended to return category")
        elif nid == PARSE_NODE_ID:
            patch_parse_node(node)
            patched["parse"] = True
            print("  ✓ Parse node — category extraction + validation added")
        elif nid == BUILD_NODE_ID:
            patch_build_node_pipeline(node)
            patched["build"] = True
            print("  ✓ Build node — Category property guard added")

    missing = [k for k, v in patched.items() if not v]
    if missing:
        raise ValueError(f"Nodes not found in Pipeline v2: {missing}")

    print(f"\n  Pushing {len(wf['nodes'])} nodes to n8n...")
    result = put_workflow(PIPELINE_ID, wf)
    print(f"  PUT OK — {len(result.get('nodes', []))} nodes confirmed")

    print("\n  Activating via API (webhook still needs manual UI re-toggle)...")
    activate(PIPELINE_ID)

    print(f"\n  ✓ Pipeline v2 done.")
    print(f"  !! IMPORTANT: toggle INACTIVE → ACTIVE in n8n UI to re-register webhook")
    print(f"     https://konvoai.app.n8n.cloud/workflow/{PIPELINE_ID}")


# ═══════════════════════════════════════════════════════════════════════════════
# Catchup Poll patches
# ═══════════════════════════════════════════════════════════════════════════════

# n8n expression for the Claude classify HTTP body.
# }}-safety: the ONLY }} in this string is the final " }}" which closes the n8n
# template. No }} appears anywhere inside the JSON.stringify() body.
CLASSIFY_BODY_EXPR = (
    '={{ JSON.stringify({'
    '"model": "claude-haiku-4-5-20251001",'
    '"max_tokens": 100,'
    '"messages": [{"role": "user", "content": '
    '"Classify this Konvo AI customer support issue into exactly ONE category.\\n\\n'
    'Valid categories:\\n'
    '- Feature request: new features or improvements\\n'
    '- AI Behavior: AI quality, responses, conversation flow\\n'
    '- Integration: third-party tool connections (Shopify, WhatsApp, CRMs)\\n'
    '- Platform & UI: dashboard bugs or usability issues\\n'
    '- Billing & Account: payments, access, account management\\n\\n'
    'Issue type: " + ($json.issue_type || "unknown") + "\\n'
    'Description: " + ($json.issue_description || "no description") + "\\n\\n'
    'Reply with ONLY the exact category name, nothing else."'
    '}]}) }}'
)


def insert_classify_node(wf):
    """
    Insert 'Claude: Classify Issue' between 'Extract Conv Text' and its current target.
    Returns True if inserted, False if already present (idempotent).
    """
    nodes        = wf["nodes"]
    connections  = wf["connections"]
    node_by_name = {n["name"]: n for n in nodes}

    EXTRACT_NODE  = "Extract Conv Text"
    CLASSIFY_NODE = "Claude: Classify Issue"

    if EXTRACT_NODE not in node_by_name:
        raise ValueError(f"Node '{EXTRACT_NODE}' not found in catchup workflow")

    if CLASSIFY_NODE in node_by_name:
        print(f"  '{CLASSIFY_NODE}' already exists — skipping (idempotent)")
        return False

    # Find current downstream target of Extract Conv Text
    extract_conns = connections.get(EXTRACT_NODE, {}).get("main", [[]])
    if not extract_conns or not extract_conns[0]:
        raise ValueError(f"No outgoing connections found from '{EXTRACT_NODE}'")
    old_target = extract_conns[0][0]["node"]
    print(f"  Extract Conv Text currently connects to: '{old_target}'")

    # Position: midpoint between Extract and its old target
    ex, ey = node_by_name[EXTRACT_NODE]["position"]
    tx, ty = node_by_name[old_target]["position"] if old_target in node_by_name else (ex + 400, ey)
    mid_x  = (ex + tx) // 2
    mid_y  = ey

    classify_node = {
        "id":          str(uuid.uuid4()),
        "name":        CLASSIFY_NODE,
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [mid_x, mid_y],
        "parameters": {
            "method":        "POST",
            "url":           "https://api.anthropic.com/v1/messages",
            "sendHeaders":   True,
            "headerParameters": {
                "parameters": [
                    {"name": "x-api-key",        "value": ANTHROPIC_KEY},
                    {"name": "anthropic-version", "value": "2023-06-01"},
                    {"name": "Content-Type",      "value": "application/json"},
                ]
            },
            "sendBody":    True,
            "specifyBody": "json",
            "jsonBody":    CLASSIFY_BODY_EXPR,
            "options":     {"continueOnFail": True},
        },
    }
    nodes.append(classify_node)

    # Rewire: Extract → Classify → old_target
    connections[EXTRACT_NODE] = {
        "main": [[{"node": CLASSIFY_NODE, "type": "main", "index": 0}]]
    }
    connections[CLASSIFY_NODE] = {
        "main": [[{"node": old_target, "type": "main", "index": 0}]]
    }

    print(f"  Inserted '{CLASSIFY_NODE}' at position [{mid_x}, {mid_y}]")
    print(f"  Rewired: {EXTRACT_NODE} → {CLASSIFY_NODE} → {old_target}")
    return True


def patch_expand_node(wf):
    """
    Patch 'Expand: Pre-IF Data Stamp' to:
      1. Declare claudeClassItems alongside claudeItems
      2. Compute category from the classify response (last text block wins)
      3. Stamp category onto each item
    """
    EXPAND_NODE  = "Expand: Pre-IF Data Stamp"
    node_by_name = {n["name"]: n for n in wf["nodes"]}

    if EXPAND_NODE not in node_by_name:
        raise ValueError(f"Node '{EXPAND_NODE}' not found in catchup workflow")

    node = node_by_name[EXPAND_NODE]
    js   = node["parameters"]["jsCode"]

    if "claudeClassItems" in js:
        print(f"  Expand node already has claudeClassItems — skipping (idempotent)")
        return

    # ── Step 1: add claudeClassItems declaration ──────────────────────────────
    OLD_CLAUDE_DECL = "const claudeItems = $('Claude: Summarize Issue').all();"
    NEW_CLAUDE_DECL = (
        "const claudeItems      = $('Claude: Summarize Issue').all();\n"
        "const claudeClassItems = $('Claude: Classify Issue').all();"
    )
    if OLD_CLAUDE_DECL not in js:
        print(f"  DEBUG — first 300 chars of Expand JS:\n{js[:300]!r}")
        raise ValueError(f"Anchor '{OLD_CLAUDE_DECL}' not found in Expand node JS")
    js = js.replace(OLD_CLAUDE_DECL, NEW_CLAUDE_DECL, 1)

    # ── Step 2: compute category inside the for-loop body ─────────────────────
    # Anchor is the "const claude = ..." line (4-space indent, inside the loop)
    OLD_CLAUDE_VAR = "    const claude = (claudeItems[i] && claudeItems[i].json) || {};"
    NEW_CLAUDE_VAR = (
        "    const claude    = (claudeItems[i]      && claudeItems[i].json)      || {};\n"
        "    const classItem = (claudeClassItems[i] && claudeClassItems[i].json) || {};\n"
        "    let rawCategory = '';\n"
        "    try {\n"
        "        for (const block of (classItem.content || [])) {\n"
        "            if (block.type === 'text' && block.text) rawCategory = block.text.trim();\n"
        "        }\n"
        "    } catch(e) {}\n"
        "    const VALID_CATS = new Set([\n"
        "        'Feature request', 'AI Behavior', 'Integration',\n"
        "        'Platform & UI', 'Billing & Account'\n"
        "    ]);\n"
        "    const category = VALID_CATS.has(rawCategory) ? rawCategory : '';"
    )
    if OLD_CLAUDE_VAR not in js:
        print(f"  DEBUG — Expand JS loop area:\n{js[js.find('for (let i'):js.find('for (let i')+500]!r}")
        raise ValueError("Loop anchor 'const claude = (claudeItems[i]...)' not found in Expand JS")
    js = js.replace(OLD_CLAUDE_VAR, NEW_CLAUDE_VAR, 1)

    # ── Step 3: add category field to the stamped item ────────────────────────
    # Anchor: the claude_content line inside results.push({ json: { ... }})
    OLD_CONTENT = "        claude_content:    claude.content         || [],"
    NEW_CONTENT = (
        "        claude_content:    claude.content         || [],\n"
        "        category:          category,"
    )
    if OLD_CONTENT not in js:
        # Flexible fallback: find the line by prefix and append after it
        idx = js.find("        claude_content:")
        if idx == -1:
            print(f"  DEBUG — Expand JS tail:\n{js[-400:]!r}")
            raise ValueError("'claude_content:' line not found in Expand node JS")
        eol = js.find("\n", idx)
        js = js[:eol] + "\n        category:          category," + js[eol:]
    else:
        js = js.replace(OLD_CONTENT, NEW_CONTENT, 1)

    node["parameters"]["jsCode"] = js
    print("  ✓ Expand node — claudeClassItems + category extraction added")


def patch_build_node_catchup(wf):
    """
    Patch 'Build Notion Payload' to set the Category property when category is non-empty.
    The guard is inserted right after the properties object closes.
    """
    BUILD_NODE   = "Build Notion Payload"
    node_by_name = {n["name"]: n for n in wf["nodes"]}

    if BUILD_NODE not in node_by_name:
        raise ValueError(f"Node '{BUILD_NODE}' not found in catchup workflow")

    node = node_by_name[BUILD_NODE]
    js   = node["parameters"]["jsCode"]

    if 'properties["Category"]' in js:
        print(f"  Build Notion Payload already has Category — skipping (idempotent)")
        return

    # Anchor: the end of the properties object literal (Issue Type + Created At + };)
    OLD_PROPS_END = (
        "        \"Issue Type\": { \"select\": { \"name\": normalizeIssueType(item.issue_type || '') } },\n"
        "        \"Created At\": { \"date\": { \"start\": item.created_at } },\n"
        "    };"
    )
    NEW_PROPS_END = (
        "        \"Issue Type\": { \"select\": { \"name\": normalizeIssueType(item.issue_type || '') } },\n"
        "        \"Created At\": { \"date\": { \"start\": item.created_at } },\n"
        "    };\n"
        "\n"
        "    if (item.category) {\n"
        "        properties[\"Category\"] = { \"select\": { \"name\": item.category } };\n"
        "    }"
    )

    if OLD_PROPS_END not in js:
        idx = js.find('"Issue Type"')
        if idx != -1:
            print(f"  DEBUG — Issue Type context:\n{js[max(0,idx-10):idx+250]!r}")
        raise ValueError("Could not find properties end anchor in Build Notion Payload JS")

    js = js.replace(OLD_PROPS_END, NEW_PROPS_END, 1)
    node["parameters"]["jsCode"] = js
    print("  ✓ Build Notion Payload — Category guard added")


def run_catchup():
    print("\n" + "=" * 65)
    print(f"[2/2] Catchup Poll — {CATCHUP_ID}")
    print("=" * 65)

    print("\n  Fetching workflow...")
    wf = get_workflow(CATCHUP_ID)
    print(f"  Name: {wf['name']}   Nodes: {len(wf['nodes'])}")

    print("\n  Deactivating...")
    deactivate(CATCHUP_ID)
    time.sleep(1)

    print("\n  Inserting 'Claude: Classify Issue' node...")
    insert_classify_node(wf)

    print("\n  Patching Expand + Build Payload nodes...")
    patch_expand_node(wf)
    patch_build_node_catchup(wf)

    print(f"\n  Pushing {len(wf['nodes'])} nodes to n8n...")
    result = put_workflow(CATCHUP_ID, wf)
    print(f"  PUT OK — {len(result.get('nodes', []))} nodes confirmed")

    print("\n  Activating via API...")
    activate(CATCHUP_ID)

    print(f"\n  ✓ Catchup done.")
    print(f"  !! IMPORTANT: toggle INACTIVE → ACTIVE in n8n UI to re-register schedule")
    print(f"     https://konvoai.app.n8n.cloud/workflow/{CATCHUP_ID}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("add_intercom_category_classification.py")
    print("=" * 65)

    run_pipeline_v2()
    run_catchup()

    print("\n" + "=" * 65)
    print("ALL DONE")
    print("=" * 65)
    print()
    print("Manual steps required in the n8n UI:")
    print()
    print("1. Pipeline v2 — toggle INACTIVE → ACTIVE")
    print("   (re-registers the Intercom webhook)")
    print(f"   https://konvoai.app.n8n.cloud/workflow/{PIPELINE_ID}")
    print()
    print("2. Catchup Poll — toggle INACTIVE → ACTIVE")
    print("   (re-registers the schedule trigger)")
    print(f"   https://konvoai.app.n8n.cloud/workflow/{CATCHUP_ID}")
    print()
    print("Verification:")
    print("  Close a billing/account Intercom conversation")
    print("  → Notion issue should appear within seconds with Category = 'Billing & Account'")
    print("  Wait up to 30 min for catchup run — newly created issues should have Category set")
    print("  (blank Category = Claude failed or unknown value — issue still created normally)")


if __name__ == "__main__":
    main()
