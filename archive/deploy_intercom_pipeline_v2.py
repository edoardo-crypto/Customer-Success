#!/usr/bin/env python3
"""
Deploy Intercom → Notion Issues Pipeline v2 — Feb 17, 2026

Redesigns the Intercom webhook workflow to:
1. Stop using Claude for classification (CS team classifies manually in Intercom)
2. Use Claude only for title + summary generation
3. Pass through untagged conversations as "Needs Classification"
4. Filter internal conversations (@konvoai.com, automated senders)
5. Keep filtering explicit "Not an Issue"

Modifies 6 nodes in workflow 3AO3SRUK80rcOCgQ by ID.
"""

import json
import urllib.request
import urllib.error
import ssl
import sys
import time
import creds

# ── Config ──────────────────────────────────────────────────────────────────
N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "3AO3SRUK80rcOCgQ"

# Node IDs to modify
NODE_IDS = {
    "extract":       "15aba94b-06d4-460f-9989-ad589f7bdee3",
    "filter_not":    "filter-not-issue-001",
    "claude":        "f481203b-43de-4ce9-87b8-bf576d0d3617",
    "parse":         "f55665bc-4fbb-4419-af85-eb1c4191c0b8",
    "build_payload": "ebaf8800-06f9-458f-92c9-60ae532839ec",
    "if_failed":     "if-claude-failed-001",
}

try:
    ctx = ssl.create_default_context()
except Exception:
    ctx = None


def log(msg, indent=0):
    prefix = "  " * indent
    print(f"{prefix}{msg}")


def n8n(method, path, body=None):
    url = f"{N8N_BASE}/api/v1{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-N8N-API-KEY", N8N_API_KEY)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=30)
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode() if e.fp else ""
        log(f"  HTTP {e.code}: {body_text[:500]}")
        raise


# ── Node Modifications ──────────────────────────────────────────────────────

def modify_extract_intercom_data(node):
    """Node 1: Add tags and is_internal extraction."""
    js = node["parameters"]["jsCode"]

    # Add tags and is_internal extraction before the return statement
    new_extraction = """
// Extract Intercom tags
const tags = (convo.tags?.tags || convo.tags || []).map(t => t.name || t);
const is_internal = email_domain === 'konvoai.com' ||
    ['linear.app', 'klaviyo.com', 'hubspot.com', 'intercom.com'].includes(email_domain);
"""

    # Insert before the return statement
    return_idx = js.rfind("return [{")
    if return_idx == -1:
        raise ValueError("Could not find 'return [{' in Extract Intercom Data")

    js = js[:return_idx] + new_extraction + "\n" + js[return_idx:]

    # Add tags and is_internal to the return object
    js = js.replace(
        "        priority\n    }",
        "        priority,\n        tags,\n        is_internal\n    }"
    )

    node["parameters"]["jsCode"] = js
    return node


def modify_filter_not_issue(node):
    """Node 2: Replace empty issue_type condition with is_internal filter."""
    node["parameters"]["conditions"] = {
        "options": {
            "caseSensitive": True,
            "leftValue": "",
            "typeValidation": "strict"
        },
        "conditions": [
            {
                "id": "cond-not-nai",
                "leftValue": "={{ $json.issue_type }}",
                "rightValue": "Not an Issue",
                "operator": {
                    "type": "string",
                    "operation": "notEquals"
                }
            },
            {
                "id": "cond-not-internal",
                "leftValue": "={{ $json.is_internal }}",
                "rightValue": True,
                "operator": {
                    "type": "boolean",
                    "operation": "notTrue"
                }
            }
        ],
        "combinator": "and"
    }
    return node


def modify_claude_classify(node):
    """Node 3: Change prompt from classification to title + summary only."""
    # Build the new prompt as an n8n expression
    prompt_expr = (
        "={{ JSON.stringify({\"model\": \"claude-sonnet-4-5-20250929\", \"max_tokens\": 500, "
        "\"messages\": [{\"role\": \"user\", \"content\": "
        "\"You are a customer support issue summarizer for Konvo AI, a B2B SaaS platform that provides AI-powered sales assistants for e-commerce businesses.\\n\\n"
        "A CS manager has closed an Intercom conversation. Your job is to:\\n"
        "1. Generate a clean, descriptive ISSUE TITLE (max 80 chars, in English)\\n"
        "2. Write a brief SUMMARY of the issue (2-3 sentences, in English)\\n\\n"
        "Customer: \" + $(\"Extract Intercom Data\").item.json.user_name + \" (\" + $(\"Extract Intercom Data\").item.json.user_email + \")\\n"
        "Company: \" + $(\"Extract Intercom Data\").item.json.company_name + \"\\n\\n"
        "CS Manager Description: \" + ($(\"Extract Intercom Data\").item.json.issue_description || \"None provided\") + \"\\n\\n"
        "Full Conversation Thread:\\n\" + ($(\"Extract Intercom Data\").item.json.conversation_text || \"No conversation data available\") + \"\\n\\n"
        "ISSUE TITLE rules: Max 80 chars, start with customer or company name, always in English regardless of conversation language. If no conversation data, use the CS Manager Description.\\n\\n"
        "SUMMARY rules: 2-3 sentences describing the core issue, in English. If no data available, say 'No conversation data available - manual review needed.'\\n\\n"
        "Response format (JSON only):\\n"
        "{\\\"issue_title\\\": \\\"<max 80 chars>\\\", \\\"summary\\\": \\\"<2-3 sentences>\\\"}\""
        "}]}) }}"
    )

    node["parameters"]["jsonBody"] = prompt_expr
    # Rename node to reflect new purpose
    node["name"] = "Claude - Summarize Issue"
    return node


def modify_parse_classification(node):
    """Node 4: Simplify to handle title + summary, add needs_classification flag."""
    new_js = r"""// Parse Claude API response — title + summary only (v2)
const claudeResponse = $input.first().json;
let parsed;
let title_generated = true;

try {
    let text = claudeResponse.content[0].text || '';
    text = text.replace(/```json\s*/gi, '').replace(/```\s*/g, '').trim();
    parsed = JSON.parse(text);
    parsed.issue_title = (parsed.issue_title || '').substring(0, 80);
    parsed.summary = parsed.summary || '';
} catch (e) {
    title_generated = false;
    const extractData = $("Extract Intercom Data").item.json;
    parsed = {
        issue_title: `${extractData.user_name || extractData.user_email || 'Unknown'}: ${extractData.issue_description || 'Needs Review'}`.substring(0, 80),
        summary: extractData.issue_description || 'AI summary generation failed - manual review needed.'
    };
}

const extractData = $("Extract Intercom Data").item.json;
const notionResult = $("Notion - Find Customer").item.json;
const customerResults = notionResult.results || [];
const customerFound = customerResults.length > 0;
const customerPageId = customerFound ? customerResults[0].id : null;
let csOwner = '';
if (customerFound) {
    const props = customerResults[0].properties || {};
    csOwner = props['\u2b50 CS Owner']?.select?.name || '';
}

const validSeverities = ['Critical', 'Medium', 'Minor'];
const sev = extractData.cs_severity || '';
const mappedSeverity = validSeverities.includes(sev) ? sev : 'Medium';

// Flag conversations with no Issue Type as needing classification
const needsClassification = !extractData.issue_type || extractData.issue_type.trim() === '';

return [{
    json: {
        ...extractData,
        issue_title: parsed.issue_title,
        summary: parsed.summary,
        title_generated,
        mapped_severity: mappedSeverity,
        customer_found: customerFound,
        customer_page_id: customerPageId,
        cs_owner: csOwner,
        needs_classification: needsClassification
    }
}];"""

    node["parameters"]["jsCode"] = new_js
    # Rename node to reflect new purpose
    node["name"] = "Parse AI Summary"
    return node


def modify_build_payload(node):
    """Node 5: Use Claude summary, flag unclassified, remove category/root_cause."""
    new_js = r"""// Build Notion page payload (v2: manual classification, Claude summary)
const d = $input.first().json;
const isUpdate = d.issue_exists === true;
const existingPageId = d.existing_page_id || null;

const issueType = d.issue_type || '';
const needsClassification = d.needs_classification === true;

const properties = {
    "Issue Title": {
        "title": [{ "text": { "content": d.issue_title || "Untitled Issue" } }]
    },
    "Source": {
        "select": { "name": "Intercom" }
    },
    "Source ID": {
        "rich_text": [{ "text": { "content": d.conversation_id || "" } }]
    },
    "Source URL": {
        "url": d.source_url || null
    },
    "Reported By": {
        "rich_text": [{ "text": { "content": (d.user_name || "") + " <" + (d.user_email || "") + ">" } }]
    },
    "Summary": {
        "rich_text": [{ "text": { "content": (d.summary || d.issue_description || d.message_body || "").substring(0, 2000) } }]
    },
    "Raw Message": {
        "rich_text": [{ "text": { "content": (d.conversation_text || d.message_body || "").substring(0, 2000) } }]
    },
    "Severity": {
        "select": { "name": d.mapped_severity || "Medium" }
    },
    "Status": {
        "select": { "name": "Open" }
    }
};

// Issue Type: use manual classification, or flag for review
if (needsClassification) {
    properties["Issue Type"] = { "select": { "name": "Needs Classification" } };
} else if (['Bug', 'Config Issue', 'New feature request', 'Feature improvement'].includes(issueType)) {
    properties["Issue Type"] = { "select": { "name": issueType } };
}

// Category and Root Cause: no longer set by Claude
// CS team fills these manually in Notion

// Add customer relation only if a match was found
if (d.customer_found && d.customer_page_id) {
    properties["Customer"] = {
        "relation": [{ "id": d.customer_page_id }]
    };
}

// Only set Created At on new issues
if (!isUpdate) {
    properties["Created At"] = {
        "date": { "start": d.created_at }
    };
}

// For updates: don't overwrite Status
if (isUpdate) {
    delete properties["Status"];
}

// Build the appropriate payload
if (isUpdate) {
    return [{
        json: {
            ...d,
            notion_payload: { properties: properties },
            update_payload: { properties: properties },
            is_update: true
        }
    }];
} else {
    return [{
        json: {
            ...d,
            notion_payload: {
                parent: { database_id: "bd1ed48de20e426f8bebeb8e700d19d8" },
                properties: properties
            },
            is_update: false
        }
    }];
}"""

    node["parameters"]["jsCode"] = new_js
    return node


def modify_if_claude_failed(node):
    """Node 6: Change condition from classification_success to title_generated."""
    node["parameters"]["conditions"] = {
        "options": {
            "caseSensitive": True,
            "leftValue": "",
            "typeValidation": "strict"
        },
        "conditions": [
            {
                "id": "cond-title-failed",
                "leftValue": "={{ $json.title_generated }}",
                "rightValue": True,
                "operator": {
                    "type": "boolean",
                    "operation": "false"
                }
            }
        ],
        "combinator": "and"
    }
    # Rename to reflect new purpose
    node["name"] = "IF: Title Generation Failed"
    return node


# ── Connection Updates ──────────────────────────────────────────────────────

def update_connections(connections):
    """Update connections to reflect renamed nodes."""
    renames = {
        "Claude - Classify Issue": "Claude - Summarize Issue",
        "Parse AI Classification": "Parse AI Summary",
        "IF: Claude Failed": "IF: Title Generation Failed",
    }

    new_connections = {}
    for source_name, outputs in connections.items():
        # Rename source node if needed
        new_source = renames.get(source_name, source_name)

        # Rename target nodes in connections
        new_outputs = {}
        for output_key, branches in outputs.items():
            new_branches = []
            for branch in branches:
                new_branch = []
                for conn in branch:
                    new_conn = dict(conn)
                    new_conn["node"] = renames.get(conn["node"], conn["node"])
                    new_branch.append(new_conn)
                new_branches.append(new_branch)
            new_outputs[output_key] = new_branches
        new_connections[new_source] = new_outputs

    return new_connections


def update_slack_alert_message(nodes):
    """Update the Slack alert node to reference title generation instead of classification."""
    for node in nodes:
        if node.get("id") == "slack-class-failed-001":
            old_body = node["parameters"].get("jsonBody", "")
            new_body = old_body.replace(
                "Claude Classification Failed",
                "Claude Title Generation Failed"
            ).replace(
                "Issue created with empty Category/Root Cause",
                "Issue created with fallback title"
            )
            node["parameters"]["jsonBody"] = new_body
            node["name"] = "Slack: Title Generation Failed Alert"
            log("Updated Slack alert node text and name", 2)
            return True
    return False


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    log("=" * 60)
    log("Intercom → Notion Issues Pipeline v2 Deployment")
    log("=" * 60)
    log("")

    # 1. Fetch current workflow
    log("[1/5] Fetching workflow...")
    wf = n8n("GET", f"/workflows/{WORKFLOW_ID}")
    log(f"  Got workflow: {wf['name']} ({len(wf['nodes'])} nodes)", 1)

    # Save backup
    with open("/tmp/intercom_workflow_backup.json", "w") as f:
        json.dump(wf, f, indent=2)
    log("  Saved backup to /tmp/intercom_workflow_backup.json", 1)

    # 2. Deactivate
    log("[2/5] Deactivating workflow...")
    try:
        n8n("POST", f"/workflows/{WORKFLOW_ID}/deactivate")
        log("  Deactivated", 1)
    except Exception as e:
        log(f"  Deactivation warning (continuing): {e}", 1)

    # 3. Modify nodes
    log("[3/5] Modifying nodes...")
    nodes = wf["nodes"]
    modified = set()

    modifiers = {
        NODE_IDS["extract"]:       ("Extract Intercom Data", modify_extract_intercom_data),
        NODE_IDS["filter_not"]:    ("Filter: Not an Issue", modify_filter_not_issue),
        NODE_IDS["claude"]:        ("Claude - Classify Issue", modify_claude_classify),
        NODE_IDS["parse"]:         ("Parse AI Classification", modify_parse_classification),
        NODE_IDS["build_payload"]: ("Build Notion Issue Payload", modify_build_payload),
        NODE_IDS["if_failed"]:     ("IF: Claude Failed", modify_if_claude_failed),
    }

    for node in nodes:
        nid = node.get("id", "")
        if nid in modifiers:
            name, modifier_fn = modifiers[nid]
            try:
                modifier_fn(node)
                log(f"  ✓ Modified: {name} ({nid[:8]}...)", 1)
                modified.add(nid)
            except Exception as e:
                log(f"  ✗ FAILED: {name} — {e}", 1)
                sys.exit(1)

    # Update Slack alert node
    update_slack_alert_message(nodes)

    # Verify all nodes found
    missing = set(NODE_IDS.values()) - modified
    if missing:
        log(f"\n  ✗ ERROR: Could not find nodes: {missing}")
        sys.exit(1)

    log(f"  All {len(modified)} nodes modified successfully", 1)

    # 4. Update connections for renamed nodes
    log("[4/5] Updating connections for renamed nodes...")
    connections = wf.get("connections", {})
    wf["connections"] = update_connections(connections)

    # Also rename in the Slack alert connections
    renames = {
        "Slack: Classification Failed Alert": "Slack: Title Generation Failed Alert",
    }
    final_connections = {}
    for source_name, outputs in wf["connections"].items():
        new_source = renames.get(source_name, source_name)
        new_outputs = {}
        for output_key, branches in outputs.items():
            new_branches = []
            for branch in branches:
                new_branch = []
                for conn in branch:
                    new_conn = dict(conn)
                    new_conn["node"] = renames.get(conn["node"], conn["node"])
                    new_branch.append(new_conn)
                new_branches.append(new_branch)
            new_outputs[output_key] = new_branches
        final_connections[new_source] = new_outputs
    wf["connections"] = final_connections
    log("  Connections updated", 1)

    # Build PUT body
    put_body = {
        "name": wf["name"],
        "nodes": wf["nodes"],
        "connections": wf["connections"],
        "settings": wf.get("settings", {}),
    }

    # Save the modified workflow for inspection
    with open("/tmp/intercom_workflow_v2.json", "w") as f:
        json.dump(put_body, f, indent=2)
    log("  Saved modified workflow to /tmp/intercom_workflow_v2.json", 1)

    # PUT the updated workflow
    log("  Pushing updated workflow to n8n...")
    result = n8n("PUT", f"/workflows/{WORKFLOW_ID}", put_body)
    log(f"  ✓ Workflow updated: {result.get('name', '?')}", 1)

    # 5. Reactivate
    log("[5/5] Reactivating workflow...")
    try:
        n8n("POST", f"/workflows/{WORKFLOW_ID}/activate")
        log("  ✓ Workflow activated", 1)
    except Exception as e:
        log(f"  Activation error: {e}", 1)
        log("  Trying alternative activation method...", 1)
        try:
            n8n("PATCH", f"/workflows/{WORKFLOW_ID}", {"active": True})
            log("  ✓ Workflow activated (via PATCH)", 1)
        except Exception as e2:
            log(f"  ✗ Could not activate: {e2}", 1)
            log("  Please activate manually in the n8n UI", 1)

    # Summary
    log("")
    log("=" * 60)
    log("DEPLOYMENT COMPLETE")
    log("=" * 60)
    log("")
    log("Changes made:")
    log("  1. Extract Intercom Data — added tags + is_internal extraction")
    log("  2. Filter: Not an Issue — added is_internal filter, removed empty issue_type block")
    log("  3. Claude - Summarize Issue — prompt now generates title + summary only")
    log("  4. Parse AI Summary — simplified to handle title/summary + needs_classification")
    log("  5. Build Notion Issue Payload — uses Claude summary, flags unclassified, no category/root_cause")
    log("  6. IF: Title Generation Failed — checks title_generated instead of classification_success")
    log("")
    log("Verification steps:")
    log("  1. Close Intercom conversation WITH Issue Type → should create Notion issue with manual type")
    log("  2. Close Intercom conversation WITHOUT Issue Type → should create with 'Needs Classification'")
    log("  3. Close conversation from @konvoai.com → should be filtered out")
    log("  4. Close conversation with 'Not an Issue' → should still be filtered out")
    log("  5. No more 'Unable to classify' junk entries")


if __name__ == "__main__":
    main()
