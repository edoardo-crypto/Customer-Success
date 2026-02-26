#!/usr/bin/env python3
"""
deploy_intercom_catchup.py — Deploy Intercom catch-up polling workflow

Problem solved: When CS agents update Issue Type / Severity / Description AFTER
closing a conversation, no new webhook fires. This poll runs every 30 minutes,
finds recently-updated closed conversations with all 3 required fields set,
deduplicates against the Notion Issues table, and creates new issues as needed.

Workflow (11 nodes):
  1. Schedule: every 30 min
  2. Intercom: search recently updated closed conversations (last 1 hour)
  3. Code: filter qualifying conversations (all 3 fields set, valid type)
  4. Notion: dedup check per conversation (Source ID exists?)
  5. Code: merge dedup result + conv data, drop duplicates
  6. Intercom: get full conversation for text extraction
  7. Code: extract conversation text, merge with original data
  8. Claude: generate issue title + summary
  9. Notion: find customer in MCT by email domain
 10. Code: parse Claude response + build Notion payload (with/without customer)
 11. Notion: create issue page
"""

import json
import uuid
import requests
import sys

# ── Config ───────────────────────────────────────────────────────────────────
N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = (
    "***REMOVED_JWT***"
    ".eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJp"
    "c3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleH"
    "AiOjE3NzMyNzAwMDB9.X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)

INTERCOM_TOKEN = "***REMOVED***"
NOTION_TOKEN = "***REMOVED***"
ANTHROPIC_KEY = (
    "***REMOVED***"
    "CxzATZqnMZonZicxgwR2LlsWw-446IlgAA"
)

NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"
NOTION_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"  # MCT data source

WORKFLOW_NAME = "Intercom Catch-up Polling"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type": "application/json",
}


def uid():
    return str(uuid.uuid4())


# ── Node JS code strings ──────────────────────────────────────────────────────

# Node 3: Filter conversations with all 3 required fields set
FILTER_QUALIFYING_JS = r"""// Filter Intercom search results to only conversations with all 3 required
// fields set: valid Issue Type, non-empty Severity, non-empty Issue Description.
// Emits one item per qualifying conversation.

const responseBody = $input.first().json;

// Intercom search response wraps conversations in a 'conversations' array
const conversations = responseBody.conversations || [];

const VALID_ISSUE_TYPES = new Set([
    'Bug',
    'Config Issue',
    'Feature improvement',
    'New feature request',
    'New Feature Request',
]);

const GENERIC_DOMAINS = new Set([
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
    'icloud.com', 'protonmail.com', 'live.com', 'me.com',
]);

const results = [];

for (const conv of conversations) {
    const attrs = conv.custom_attributes || {};

    // Intercom CDA field names (case-sensitive keys from the API)
    const issueType = attrs['Issue Type'] || attrs['cs_issue_type'] || '';
    const severity  = attrs['Severity']   || attrs['cs_severity']   || '';
    const issueDesc = attrs['Issue Description'] || attrs['issue_description'] || '';

    // Skip conversations explicitly marked "Not an Issue"
    if (issueType === 'Not an Issue' || issueType === 'No Issue') continue;

    // Skip if missing any required field
    if (!VALID_ISSUE_TYPES.has(issueType)) continue;
    if (!severity) continue;
    if (!issueDesc) continue;

    // Extract contact info (search API returns contacts reference, not full objects)
    const contacts = (conv.contacts && conv.contacts.contacts) || [];
    const firstContact = contacts[0] || {};
    const userEmail = firstContact.email || '';
    const userName  = firstContact.name  || '';
    const emailDomain = userEmail.includes('@')
        ? userEmail.split('@')[1].toLowerCase()
        : '';

    // Company
    const companies = (conv.companies && conv.companies.companies) || [];
    const companyName = (companies[0] && companies[0].name) || '';

    // Timestamps
    const createdTs = conv.created_at || 0;
    const createdAt = createdTs
        ? new Date(createdTs * 1000).toISOString()
        : new Date().toISOString();

    const convId = String(conv.id || '');

    results.push({
        json: {
            conversation_id: convId,
            source_url: `https://app.intercom.com/a/inbox/o0lp6qsb/inbox/conversation/${convId}`,
            user_name: userName,
            user_email: userEmail,
            company_name: companyName,
            email_domain: emailDomain,
            is_generic_domain: GENERIC_DOMAINS.has(emailDomain),
            issue_type: issueType,
            cs_severity: severity,
            issue_description: issueDesc,
            created_at: createdAt,
        }
    });
}

// Log summary for debugging
console.log(`[catchup] Total conversations: ${conversations.length}, Qualifying: ${results.length}`);

return results;
"""

# Node 5: Merge dedup result with conv data, drop duplicates
MERGE_DEDUP_JS = r"""// Merge Notion dedup results with the original conversation data.
// Items that already have a Notion entry are dropped here.
// Uses index-based pairing between dedup results and filter node output.

const dedupItems  = $input.all();   // from Notion: Check Duplicate
const filterItems = $('Filter: Qualifying Conversations').all();  // original conv data

const results = [];

for (let i = 0; i < dedupItems.length; i++) {
    const dedupJson = dedupItems[i].json || {};
    const convData  = (filterItems[i] && filterItems[i].json) || {};

    // Notion query response: { results: [...], has_more: false }
    const existingCount = (dedupJson.results || []).length;

    if (existingCount > 0) {
        // Already logged in Notion — skip
        console.log(`[catchup] Skip ${convData.conversation_id} — already in Notion`);
        continue;
    }

    // Not in Notion — keep for processing
    console.log(`[catchup] Queue ${convData.conversation_id} for issue creation`);
    results.push({ json: { ...convData } });
}

return results;
"""

# Node 7: Extract conversation text from full Intercom response + merge with data
EXTRACT_TEXT_JS = r"""// Extract conversation thread text from the full Intercom API response.
// Merges with the conversation metadata from the Filter node (via index pairing).

const intercomItems = $input.all();       // full conv responses from Intercom
const priorItems    = $('Merge: Drop Duplicates').all();  // conv metadata

const results = [];

for (let i = 0; i < intercomItems.length; i++) {
    const convJson  = intercomItems[i].json || {};
    const convData  = (priorItems[i] && priorItems[i].json) || {};

    // Build conversation text from all parts
    const messages = [];

    // Opening message
    const source = convJson.source || {};
    if (source.body) {
        const body = source.body.replace(/<[^>]+>/g, '').trim();
        if (body) {
            const author = source.author || {};
            const name = author.name || author.email || 'User';
            messages.push(`${name}: ${body}`);
        }
    }

    // Conversation parts
    const parts = (convJson.conversation_parts && convJson.conversation_parts.conversation_parts) || [];
    for (const part of parts) {
        if (!['comment', 'note', 'reply'].includes(part.part_type)) continue;
        if (!part.body) continue;
        const body = part.body.replace(/<[^>]+>/g, '').trim();
        if (!body) continue;
        const author = part.author || {};
        const name = author.name || author.email || 'Agent';
        messages.push(`${name}: ${body}`);
    }

    const conversationText = messages.join('\n\n');

    // Enrich contact info from full conv response (search API may have limited data)
    const fullContacts = (convJson.contacts && convJson.contacts.contacts) || [];
    const firstFull = fullContacts[0] || {};
    const enrichedEmail = firstFull.email || convData.user_email || '';
    const enrichedName  = firstFull.name  || convData.user_name  || '';
    const enrichedDomain = enrichedEmail.includes('@')
        ? enrichedEmail.split('@')[1].toLowerCase()
        : convData.email_domain || '';

    results.push({
        json: {
            ...convData,
            user_email: enrichedEmail || convData.user_email,
            user_name:  enrichedName  || convData.user_name,
            email_domain: enrichedDomain || convData.email_domain,
            conversation_text: conversationText,
        }
    });
}

return results;
"""

# Node 10: Parse Claude response + find customer result + build final Notion payload
BUILD_PAYLOAD_JS = """// Parse Claude response + merge customer lookup result → build Notion page payload.
// Uses index-based pairing across Claude results, conv data, and customer results.

const claudeItems    = $('Claude: Summarize Issue').all();
const convItems      = $('Extract Conv Text').all();
const customerItems  = $input.all();  // from Notion: Find Customer

const NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8";

const VALID_SEVERITIES = new Set(['Urgent', 'Important', 'Not important']);

// Normalize issue type — Intercom capital-R variant maps to its own select option
// (Notion Issues table has both "New feature request" and "New Feature Request" added)
function normalizeIssueType(raw) {
    if (raw === 'New Feature Request') return 'New Feature Request';
    return raw;  // all other types pass through unchanged
}

const results = [];

for (let i = 0; i < convItems.length; i++) {
    const claudeJson   = (claudeItems[i]   && claudeItems[i].json)   || {};
    const convData     = (convItems[i]      && convItems[i].json)     || {};
    const customerJson = (customerItems[i]  && customerItems[i].json) || {};

    // Parse Claude response
    let issueTitle = '';
    let summary    = '';
    try {
        let text = (claudeJson.content && claudeJson.content[0] && claudeJson.content[0].text) || '';
        text = text.replace(/```json\\s*/gi, '').replace(/```\\s*/g, '').trim();
        const parsed = JSON.parse(text);
        issueTitle = (parsed.issue_title || '').substring(0, 80);
        summary    = parsed.summary || '';
    } catch (e) {
        issueTitle = (
            (convData.user_name || convData.user_email || 'Unknown') +
            ': ' + (convData.issue_description || 'Needs Review')
        ).substring(0, 80);
        summary = convData.issue_description || 'AI summary generation failed.';
    }

    // Customer relation
    const customerResults = customerJson.results || [];
    const customerFound   = customerResults.length > 0;
    const customerPageId  = customerFound ? customerResults[0].id : null;

    // Severity mapping
    const rawSev = convData.cs_severity || '';
    const mappedSeverity = VALID_SEVERITIES.has(rawSev) ? rawSev : 'Important';

    // Build Notion properties
    const properties = {
        "Issue Title": {
            "title": [{ "text": { "content": issueTitle || "Untitled Issue" } }]
        },
        "Source": { "select": { "name": "Intercom" } },
        "Source ID": {
            "rich_text": [{ "text": { "content": convData.conversation_id || "" } }]
        },
        "Source URL": { "url": convData.source_url || null },
        "Reported By": {
            "rich_text": [{
                "text": {
                    "content": (convData.user_name || '') + ' <' + (convData.user_email || '') + '>'
                }
            }]
        },
        "Summary": {
            "rich_text": [{
                "text": {
                    "content": (summary || '').substring(0, 2000)
                }
            }]
        },
        "Raw Message": {
            "rich_text": [{
                "text": {
                    "content": (convData.conversation_text || '').substring(0, 2000)
                }
            }]
        },
        "Severity": { "select": { "name": mappedSeverity } },
        "Status":   { "select": { "name": "Open" } },
        "Issue Type": {
            "select": { "name": normalizeIssueType(convData.issue_type || '') }
        },
        "Created At": { "date": { "start": convData.created_at } },
    };

    if (customerPageId) {
        properties["Customer"] = { "relation": [{ "id": customerPageId }] };
    }

    results.push({
        json: {
            notion_payload: {
                parent: { database_id: NOTION_ISSUES_DB },
                properties: properties,
            },
            conversation_id: convData.conversation_id,
            issue_title: issueTitle,
            customer_linked: customerFound,
        }
    });
}

return results;
"""


def build_workflow():
    """Build the complete n8n workflow JSON."""

    # ── Node positions (left to right, then down for branches) ───────────────
    x = [0, 250, 500, 750, 1000, 1250, 1500, 1750, 2000, 2250, 2500]
    y_main = 300

    # Claude prompt expression — uses $json fields from previous node (Extract Conv Text)
    claude_prompt_expr = (
        '={{ JSON.stringify({'
        '"model": "claude-sonnet-4-5-20250929",'
        '"max_tokens": 500,'
        '"messages": [{'
        '"role": "user",'
        '"content": '
        '"You are a customer support issue summarizer for Konvo AI, a B2B SaaS platform that provides AI-powered sales assistants for e-commerce businesses.\\\\n\\\\n'
        'A CS manager has closed an Intercom conversation. Your job is to:\\\\n'
        '1. Generate a clean, descriptive ISSUE TITLE (max 80 chars, in English)\\\\n'
        '2. Write a brief SUMMARY of the issue (2-3 sentences, in English)\\\\n\\\\n'
        'Customer: " + ($json.user_name || $json.user_email || "Unknown") + " (" + ($json.user_email || "") + ")\\\\n'
        'Company: " + ($json.company_name || "Unknown") + "\\\\n\\\\n'
        'CS Manager Description: " + ($json.issue_description || "None provided") + "\\\\n\\\\n'
        'Full Conversation Thread:\\\\n" + ($json.conversation_text || "No conversation data available") + "\\\\n\\\\n'
        'ISSUE TITLE rules: Max 80 chars, start with customer or company name, always in English.\\\\n\\\\n'
        'SUMMARY rules: 2-3 sentences describing the core issue, in English.\\\\n\\\\n'
        'Response format (JSON only):\\\\n{\\\\\\"issue_title\\\\\\": \\\\\\"<max 80 chars>\\\\\\", \\\\\\"summary\\\\\\": \\\\\\"<2-3 sentences>\\\\\\"}"'
        '}]}) }}'
    )

    # MCT customer lookup body — searches by email domain
    customer_lookup_body = (
        '={{ JSON.stringify({'
        '"filter": {'
        '"property": "\\ud83c\\udfe2 Domain",'
        '"rich_text": {"contains": ($json.email_domain && !["gmail.com","yahoo.com","hotmail.com","outlook.com","icloud.com","protonmail.com"].includes($json.email_domain)) ? $json.email_domain : "SKIP_NO_MATCH_PLACEHOLDER"}'
        '}'
        '}) }}'
    )

    nodes = [
        # 1. Schedule Trigger — every 30 minutes
        {
            "id": uid(),
            "name": "Schedule: Every 30 Minutes",
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.2,
            "position": [x[0], y_main],
            "parameters": {
                "rule": {
                    "interval": [
                        {"field": "minutes", "minutesInterval": 30}
                    ]
                }
            },
        },

        # 2. Intercom search — closed conversations updated in last 1 hour
        {
            "id": uid(),
            "name": "Intercom: Search Recent Closed",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [x[1], y_main],
            "parameters": {
                "method": "POST",
                "url": "https://api.intercom.io/conversations/search",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {"name": "Authorization", "value": f"Bearer {INTERCOM_TOKEN}"},
                        {"name": "Accept", "value": "application/json"},
                    ]
                },
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": (
                    '={{ JSON.stringify({'
                    '"query": {"operator": "AND", "value": ['
                    '{"field": "state", "operator": "=", "value": "closed"},'
                    '{"field": "updated_at", "operator": ">", "value": Math.floor(Date.now()/1000) - 3600}'
                    ']},'
                    '"sort": {"field": "updated_at", "order": "descending"},'
                    '"pagination": {"per_page": 150}'
                    '}) }}'
                ),
                "options": {},
            },
        },

        # 3. Filter: keep only conversations with all 3 required fields set
        {
            "id": uid(),
            "name": "Filter: Qualifying Conversations",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [x[2], y_main],
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": FILTER_QUALIFYING_JS,
            },
        },

        # 4. Notion dedup — check if Source ID already exists in Issues table
        {
            "id": uid(),
            "name": "Notion: Check Duplicate",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [x[3], y_main],
            "parameters": {
                "method": "POST",
                "url": f"https://api.notion.com/v1/databases/{NOTION_ISSUES_DB}/query",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {"name": "Authorization", "value": f"Bearer {NOTION_TOKEN}"},
                        {"name": "Notion-Version", "value": "2022-06-28"},
                    ]
                },
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": (
                    '={{ JSON.stringify({'
                    '"filter": {"property": "Source ID", "rich_text": {"equals": $json.conversation_id}},'
                    '"page_size": 1'
                    '}) }}'
                ),
                "options": {},
            },
        },

        # 5. Merge dedup result with conv data + drop duplicates
        {
            "id": uid(),
            "name": "Merge: Drop Duplicates",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [x[4], y_main],
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": MERGE_DEDUP_JS,
            },
        },

        # 6. Intercom: get full conversation for thread text
        {
            "id": uid(),
            "name": "Intercom: Get Full Conversation",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [x[5], y_main],
            "parameters": {
                "method": "GET",
                "url": "={{ `https://api.intercom.io/conversations/${$json.conversation_id}` }}",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {"name": "Authorization", "value": f"Bearer {INTERCOM_TOKEN}"},
                        {"name": "Accept", "value": "application/json"},
                    ]
                },
                "options": {},
            },
        },

        # 7. Extract conversation text + merge with metadata
        {
            "id": uid(),
            "name": "Extract Conv Text",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [x[6], y_main],
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": EXTRACT_TEXT_JS,
            },
        },

        # 8. Claude: generate issue title + summary
        {
            "id": uid(),
            "name": "Claude: Summarize Issue",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [x[7], y_main],
            "parameters": {
                "method": "POST",
                "url": "https://api.anthropic.com/v1/messages",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {"name": "x-api-key", "value": ANTHROPIC_KEY},
                        {"name": "anthropic-version", "value": "2023-06-01"},
                        {"name": "Content-Type", "value": "application/json"},
                    ]
                },
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": claude_prompt_expr,
                "options": {},
            },
        },

        # 9. Notion: find customer in MCT by email domain
        {
            "id": uid(),
            "name": "Notion: Find Customer",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [x[8], y_main],
            "parameters": {
                "method": "POST",
                "url": f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {"name": "Authorization", "value": f"Bearer {NOTION_TOKEN}"},
                        {"name": "Notion-Version", "value": "2025-09-03"},
                    ]
                },
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": customer_lookup_body,
                "options": {"continueOnFail": True},
            },
        },

        # 10. Build final Notion payload (parse Claude + customer relation)
        {
            "id": uid(),
            "name": "Build Notion Payload",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [x[9], y_main],
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": BUILD_PAYLOAD_JS,
            },
        },

        # 11. Notion: create issue page
        {
            "id": uid(),
            "name": "Notion: Create Issue",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [x[10], y_main],
            "parameters": {
                "method": "POST",
                "url": "https://api.notion.com/v1/pages",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {"name": "Authorization", "value": f"Bearer {NOTION_TOKEN}"},
                        {"name": "Notion-Version", "value": "2022-06-28"},
                    ]
                },
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": "={{ JSON.stringify($json.notion_payload) }}",
                "options": {},
            },
        },
    ]

    # Build connection map: each node connects to the next
    node_names = [n["name"] for n in nodes]
    connections = {}
    for i in range(len(node_names) - 1):
        connections[node_names[i]] = {
            "main": [
                [{"node": node_names[i + 1], "type": "main", "index": 0}]
            ]
        }

    return {
        "name": WORKFLOW_NAME,
        "nodes": nodes,
        "connections": connections,
        "settings": {
            "executionOrder": "v1",
            "saveManualExecutions": True,
            "callerPolicy": "workflowsFromSameOwner",
            "errorWorkflow": "",
        },
    }


# ── n8n API helpers ──────────────────────────────────────────────────────────

def list_workflows():
    r = requests.get(f"{N8N_BASE}/api/v1/workflows", headers=N8N_HEADERS)
    r.raise_for_status()
    return r.json().get("data", [])


def create_workflow(wf_body):
    r = requests.post(f"{N8N_BASE}/api/v1/workflows", headers=N8N_HEADERS, json=wf_body)
    if r.status_code not in (200, 201):
        print(f"  CREATE failed: {r.status_code} — {r.text[:500]}")
        r.raise_for_status()
    return r.json()


def delete_workflow(wf_id):
    r = requests.delete(f"{N8N_BASE}/api/v1/workflows/{wf_id}", headers=N8N_HEADERS)
    if r.status_code in (200, 204):
        print(f"  Deleted workflow {wf_id}")
    else:
        print(f"  Delete warning: {r.status_code}")


def save_workflow_json(wf, path):
    with open(path, "w") as f:
        json.dump(wf, f, indent=2)
    print(f"  Saved to {path}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("deploy_intercom_catchup.py")
    print(f"Deploying: {WORKFLOW_NAME!r}")
    print("=" * 60)

    # 1. Check for existing workflow with same name
    print("\n[1/4] Checking for existing workflow with same name...")
    existing = list_workflows()
    duplicates = [w for w in existing if w.get("name") == WORKFLOW_NAME]
    if duplicates:
        print(f"  Found {len(duplicates)} existing workflow(s) with this name:")
        for d in duplicates:
            print(f"    ID={d['id']}  active={d.get('active')}")
        answer = input("  Delete and redeploy? [y/N]: ").strip().lower()
        if answer == "y":
            for d in duplicates:
                delete_workflow(d["id"])
        else:
            print("  Aborting — existing workflow not deleted")
            sys.exit(0)

    # 2. Build workflow JSON
    print("\n[2/4] Building workflow JSON...")
    wf_body = build_workflow()
    print(f"  Nodes: {len(wf_body['nodes'])}")
    for i, node in enumerate(wf_body["nodes"], 1):
        print(f"    {i:2}. {node['name']}")

    # Save for inspection
    save_workflow_json(wf_body, "/tmp/intercom_catchup_workflow.json")

    # 3. Create workflow in n8n
    print("\n[3/4] Creating workflow in n8n...")
    result = create_workflow(wf_body)
    wf_id = result.get("id", "?")
    print(f"  Created workflow ID: {wf_id}")
    print(f"  Active: {result.get('active')}")

    # 4. Summary + activation instructions
    print("\n[4/4] Deployment complete.")
    print()
    print("=" * 60)
    print("IMPORTANT: Manual step required")
    print("=" * 60)
    print()
    print("Webhooks and scheduled triggers only register when you toggle")
    print("the workflow ACTIVE in the n8n UI.")
    print()
    print("To activate:")
    print(f"  1. Open: {N8N_BASE}/workflow/{wf_id}")
    print("  2. Click the 'Active' toggle in the top-right corner")
    print("  3. Confirm the workflow appears as active (green)")
    print()
    print("What this workflow does every 30 minutes:")
    print("  • Queries Intercom for closed conversations updated in the last hour")
    print("  • Filters to only those with Issue Type + Severity + Description set")
    print("  • Deduplicates against the Notion Issues table (by Source ID)")
    print("  • For new issues: calls Claude for title/summary, finds customer,")
    print("    creates a Notion Issues row — identical to the webhook pipeline")
    print()
    print("This catches post-close attribute changes that the webhook misses.")
    print()
    print(f"Workflow JSON saved to: /tmp/intercom_catchup_workflow.json")


if __name__ == "__main__":
    main()
