#!/usr/bin/env python3
"""
fix_aten_filter.py — Fix ATEN workflow filter + backfill 2 missing Notion rows

Part 1: Update "Filter: Not an Issue" to keep only 2 conditions:
  1. issue_type NOT EQUALS "No Issue"
  2. issue_type NOT EMPTY

Part 2: Backfill 2 missing Notion issue rows for:
  - Conversation 215473148118694 (Miriam Hoyos, Feature Improvement Request)
  - Conversation 215473164094829 (almacosmeticauy@gmail.com, Bug)
"""

import json
import time
import sys
import requests
from datetime import datetime, timezone
import creds

# ── Credentials ──────────────────────────────────────────────────────────────
N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "3AO3SRUK80rcOCgQ"

INTERCOM_TOKEN = creds.get("INTERCOM_TOKEN")
NOTION_TOKEN = creds.get("NOTION_TOKEN")
ANTHROPIC_KEY = creds.get("ANTHROPIC_API_KEY")

NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"
NOTION_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

n8n_headers = {"X-N8N-API-KEY": N8N_KEY, "Content-Type": "application/json"}
notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
notion_headers_v2025 = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}
intercom_headers = {
    "Authorization": f"Bearer {INTERCOM_TOKEN}",
    "Accept": "application/json",
}
anthropic_headers = {
    "x-api-key": ANTHROPIC_KEY,
    "anthropic-version": "2023-06-01",
    "Content-Type": "application/json",
}

# ── Backfill targets ──────────────────────────────────────────────────────────
BACKFILL_CONVERSATIONS = [
    {
        "id": "215473148118694",
        "issue_type": "Feature Improvement Request",
        "email_domain": "northdeco.com",
    },
    {
        "id": "215473164094829",
        "issue_type": "Bug",
        "email_domain": None,  # gmail.com — will not match customer
    },
]


# ═════════════════════════════════════════════════════════════════════════════
# PART 1 — Fix the "Filter: Not an Issue" node
# ═════════════════════════════════════════════════════════════════════════════

def fix_filter_node():
    print("\n=== PART 1: Fix 'Filter: Not an Issue' node ===")

    # Step 1 — GET workflow
    print(f"  Fetching workflow {WORKFLOW_ID}...")
    r = requests.get(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=n8n_headers)
    r.raise_for_status()
    workflow = r.json()

    # Step 2 — Backup
    backup_path = f"/tmp/workflow_backup_aten_filter.json"
    with open(backup_path, "w") as f:
        json.dump(workflow, f, indent=2)
    print(f"  Backed up to {backup_path}")

    # Step 3 — Deactivate
    if workflow.get("active"):
        print("  Deactivating workflow...")
        da = requests.post(
            f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate",
            headers=n8n_headers,
        )
        da.raise_for_status()
        print("  Deactivated.")
    else:
        print("  Workflow already inactive.")

    # Step 4 — Find and patch the filter node
    nodes = workflow["nodes"]
    filter_node = next(
        (n for n in nodes if n.get("name") == "Filter: Not an Issue"), None
    )
    if not filter_node:
        print("  ERROR: 'Filter: Not an Issue' node not found!")
        sys.exit(1)

    print(f"  Found node id={filter_node['id']}")
    current_conditions = filter_node["parameters"]["conditions"]["conditions"]
    print(f"  Current condition count: {len(current_conditions)}")

    # Keep only the first 2 conditions (NOT EQUALS "No Issue" and NOT EMPTY)
    # Both have stable IDs from the fetched data
    new_conditions = [
        {
            "id": "cond-not-nai",
            "leftValue": "={{ $json.issue_type }}",
            "rightValue": "No Issue",
            "operator": {"type": "string", "operation": "notEquals"},
        },
        {
            "id": "cond-has-issue-type",
            "leftValue": "={{ $json.issue_type }}",
            "rightValue": "",
            "operator": {"type": "string", "operation": "notEmpty"},
        },
    ]
    filter_node["parameters"]["conditions"]["conditions"] = new_conditions
    print(f"  New condition count: {len(new_conditions)}")

    # Step 5 — PUT workflow (only allowed keys)
    put_body = {
        "name": workflow["name"],
        "nodes": workflow["nodes"],
        "connections": workflow["connections"],
        "settings": workflow.get("settings", {}),
    }
    print("  PUTting updated workflow...")
    put_r = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}",
        headers=n8n_headers,
        json=put_body,
    )
    if put_r.status_code != 200:
        print(f"  PUT failed: {put_r.status_code} — {put_r.text[:400]}")
        sys.exit(1)
    print("  PUT succeeded.")

    # Step 6 — Activate
    print("  Activating workflow...")
    act = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate",
        headers=n8n_headers,
    )
    act.raise_for_status()
    print("  Workflow activated.")

    print("  Part 1 DONE.\n")


# ═════════════════════════════════════════════════════════════════════════════
# PART 2 — Backfill 2 missing Notion rows
# ═════════════════════════════════════════════════════════════════════════════

def get_intercom_conversation(conv_id):
    """Fetch full Intercom conversation including parts and contacts."""
    print(f"  Fetching Intercom conversation {conv_id}...")
    r = requests.get(
        f"https://api.intercom.io/conversations/{conv_id}",
        headers=intercom_headers,
    )
    r.raise_for_status()
    return r.json()


def extract_conversation_data(conv, issue_type):
    """Extract fields matching workflow's 'Extract Intercom Data' node logic."""
    # Contacts
    contacts = conv.get("contacts", {}).get("contacts", [])
    user_name = ""
    user_email = ""
    if contacts:
        c = contacts[0]
        user_name = c.get("name", "") or ""
        # Fetch full contact to get email
        contact_id = c.get("id", "")
        if contact_id:
            try:
                cr = requests.get(
                    f"https://api.intercom.io/contacts/{contact_id}",
                    headers=intercom_headers,
                )
                if cr.status_code == 200:
                    contact_data = cr.json()
                    user_email = contact_data.get("email", "") or ""
                    if not user_name:
                        user_name = contact_data.get("name", "") or ""
            except Exception as e:
                print(f"    Warning: could not fetch contact details: {e}")

    # Company
    company_name = ""
    companies = conv.get("companies", {})
    if isinstance(companies, dict):
        company_list = companies.get("companies", [])
        if company_list:
            company_name = company_list[0].get("name", "") or ""

    # Custom attributes (cs_severity, issue_description from CDA)
    custom_attrs = conv.get("custom_attributes", {}) or {}
    cs_severity = custom_attrs.get("cs_severity", "") or ""
    issue_description = custom_attrs.get("issue_description", "") or ""

    # Conversation text: compile all parts
    parts = conv.get("conversation_parts", {}).get("conversation_parts", [])
    messages = []

    # First message
    first_part = conv.get("source", {})
    if first_part:
        author = first_part.get("author", {})
        author_name = author.get("name", "") or author.get("email", "") or "User"
        body = first_part.get("body", "") or ""
        # Strip HTML tags crudely
        body = body.replace("<br>", "\n").replace("<br/>", "\n")
        import re
        body = re.sub(r"<[^>]+>", "", body).strip()
        if body:
            messages.append(f"{author_name}: {body}")

    for part in parts:
        if part.get("part_type") in ("comment", "note", "reply", "conversation_rating"):
            author = part.get("author", {})
            author_name = author.get("name", "") or author.get("email", "") or "Agent"
            body = part.get("body", "") or ""
            import re
            body = body.replace("<br>", "\n").replace("<br/>", "\n")
            body = re.sub(r"<[^>]+>", "", body).strip()
            if body:
                messages.append(f"{author_name}: {body}")

    conversation_text = "\n\n".join(messages) if messages else ""

    # Created at
    created_ts = conv.get("created_at", 0) or 0
    created_at = datetime.fromtimestamp(created_ts, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    ) if created_ts else datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Derive email domain
    email_domain = ""
    if user_email and "@" in user_email:
        email_domain = user_email.split("@")[1].lower()

    conv_id = str(conv.get("id", ""))

    return {
        "conversation_id": conv_id,
        "source_url": f"https://app.intercom.com/a/inbox/o0lp6qsb/inbox/conversation/{conv_id}",
        "user_name": user_name,
        "user_email": user_email,
        "company_name": company_name,
        "email_domain": email_domain,
        "issue_type": issue_type,
        "cs_severity": cs_severity,
        "issue_description": issue_description,
        "conversation_text": conversation_text,
        "created_at": created_at,
    }


def call_claude_summarize(data):
    """Call Claude to generate issue_title + summary."""
    print(f"  Calling Claude for conversation {data['conversation_id']}...")

    prompt = (
        "You are a customer support issue summarizer for Konvo AI, a B2B SaaS platform "
        "that provides AI-powered sales assistants for e-commerce businesses.\n\n"
        "A CS manager has closed an Intercom conversation. Your job is to:\n"
        "1. Generate a clean, descriptive ISSUE TITLE (max 80 chars, in English)\n"
        "2. Write a brief SUMMARY of the issue (2-3 sentences, in English)\n\n"
        f"Customer: {data['user_name']} ({data['user_email']})\n"
        f"Company: {data['company_name']}\n\n"
        f"CS Manager Description: {data['issue_description'] or 'None provided'}\n\n"
        f"Full Conversation Thread:\n{data['conversation_text'] or 'No conversation data available'}\n\n"
        "ISSUE TITLE rules: Max 80 chars, start with customer or company name, always in English "
        "regardless of conversation language. If no conversation data, use the CS Manager Description.\n\n"
        "SUMMARY rules: 2-3 sentences describing the core issue, in English. If no data available, "
        "say 'No conversation data available - manual review needed.'\n\n"
        'Response format (JSON only):\n{"issue_title": "<max 80 chars>", "summary": "<2-3 sentences>"}'
    )

    payload = {
        "model": "claude-sonnet-4-5-20250929",
        "max_tokens": 500,
        "messages": [{"role": "user", "content": prompt}],
    }

    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=anthropic_headers,
        json=payload,
    )
    r.raise_for_status()
    resp = r.json()

    text = resp["content"][0]["text"].strip()
    # Strip markdown code fences if present
    text = text.replace("```json", "").replace("```", "").strip()

    try:
        parsed = json.loads(text)
        issue_title = (parsed.get("issue_title") or "")[:80]
        summary = parsed.get("summary") or ""
    except Exception:
        issue_title = f"{data['user_name'] or data['user_email'] or 'Unknown'}: {data['issue_description'] or 'Needs Review'}"[:80]
        summary = data["issue_description"] or "AI summary generation failed - manual review needed."

    print(f"    Title: {issue_title}")
    return issue_title, summary


def find_customer_by_domain(domain):
    """Query Notion Master Customer Table by domain using data_sources API."""
    if not domain or domain in ("gmail.com", "yahoo.com", "hotmail.com", "outlook.com"):
        print(f"  Skipping customer lookup for generic domain: {domain}")
        return None, None

    print(f"  Looking up customer by domain: {domain}...")
    r = requests.post(
        f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
        headers=notion_headers_v2025,
        json={
            "filter": {
                "property": "🏢 Domain",
                "rich_text": {"contains": domain},
            }
        },
    )
    r.raise_for_status()
    results = r.json().get("results", [])

    if not results:
        print(f"  No customer found for domain {domain}")
        return None, None

    page_id = results[0]["id"]
    props = results[0].get("properties", {})
    cs_owner = ""
    owner_prop = props.get("⭐ CS Owner", {})
    if owner_prop.get("select"):
        cs_owner = owner_prop["select"].get("name", "")
    print(f"  Found customer: page_id={page_id}, cs_owner={cs_owner}")
    return page_id, cs_owner


def build_notion_payload(data, issue_title, summary, customer_page_id):
    """Build Notion Issues Table page creation payload."""
    valid_severities = ["Urgent", "Important", "Not important"]
    sev = data.get("cs_severity", "")
    mapped_severity = sev if sev in valid_severities else "Important"

    properties = {
        "Issue Title": {
            "title": [{"text": {"content": issue_title or "Untitled Issue"}}]
        },
        "Source": {"select": {"name": "Intercom"}},
        "Source ID": {
            "rich_text": [{"text": {"content": data["conversation_id"]}}]
        },
        "Source URL": {"url": data["source_url"]},
        "Reported By": {
            "rich_text": [
                {
                    "text": {
                        "content": f"{data['user_name']} <{data['user_email']}>"
                    }
                }
            ]
        },
        "Summary": {
            "rich_text": [
                {"text": {"content": (summary or data.get("issue_description") or "")[:2000]}}
            ]
        },
        "Raw Message": {
            "rich_text": [
                {
                    "text": {
                        "content": (data.get("conversation_text") or "")[:2000]
                    }
                }
            ]
        },
        "Severity": {"select": {"name": mapped_severity}},
        "Status": {"select": {"name": "Open"}},
        "Issue Type": {"select": {"name": data["issue_type"]}},
        "Created At": {"date": {"start": data["created_at"]}},
    }

    if customer_page_id:
        properties["Customer"] = {"relation": [{"id": customer_page_id}]}

    return {
        "parent": {"database_id": NOTION_ISSUES_DB},
        "properties": properties,
    }


def create_notion_issue(payload):
    """POST to Notion to create the issue page."""
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_headers,
        json=payload,
    )
    if r.status_code not in (200, 201):
        print(f"  ERROR creating Notion page: {r.status_code} — {r.text[:500]}")
        return None
    page = r.json()
    print(f"  Created Notion page: {page['id']}")
    return page["id"]


def backfill_conversations():
    print("\n=== PART 2: Backfill 2 missing Notion rows ===")

    for target in BACKFILL_CONVERSATIONS:
        conv_id = target["id"]
        issue_type = target["issue_type"]
        print(f"\n--- Processing conversation {conv_id} (type: {issue_type}) ---")

        # a. Fetch Intercom conversation
        conv = get_intercom_conversation(conv_id)
        data = extract_conversation_data(conv, issue_type)

        print(f"  User: {data['user_name']} ({data['user_email']})")
        print(f"  Domain: {data['email_domain']}")
        print(f"  Created at: {data['created_at']}")
        print(f"  Conversation text length: {len(data['conversation_text'])} chars")

        # b. Call Claude for title + summary
        issue_title, summary = call_claude_summarize(data)

        # c. Find customer in Notion
        customer_page_id, cs_owner = find_customer_by_domain(data["email_domain"])

        # d. Build payload and create Notion row
        payload = build_notion_payload(data, issue_title, summary, customer_page_id)
        page_id = create_notion_issue(payload)

        if page_id:
            print(f"  SUCCESS: Notion row created — https://notion.so/{page_id.replace('-', '')}")
        else:
            print(f"  FAILED: Could not create Notion row for conversation {conv_id}")

        time.sleep(1)  # brief pause between API calls

    print("\n  Part 2 DONE.\n")


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=== fix_aten_filter.py ===")
    fix_filter_node()
    backfill_conversations()
    print("\n=== ALL DONE ===")
