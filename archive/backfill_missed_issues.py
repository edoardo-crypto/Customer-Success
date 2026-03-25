#!/usr/bin/env python3
"""
backfill_missed_issues.py — One-time backfill for 5 conversations missed Feb 23

Root causes:
  - 215473213477942: Post-close attribute changes (webhook fired before CS updated attrs)
  - 215473211569900: Severity "Not important" was wrongly filtered out
  - 215473210256993: Severity "Not important" was wrongly filtered out
  - 215473208362849: Severity "Not important" was wrongly filtered out
  - 215473198746105: Severity "Not important" was wrongly filtered out

For each conversation:
  1. Fetch full data from Intercom API
  2. Check Notion Issues table for existing Source ID (dedup)
  3. Call Claude for issue title + summary
  4. Look up customer in Master Customer Table by email domain
  5. Create Notion Issues table row
"""

import json
import re
import time
import sys
import requests
from datetime import datetime, timezone
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
INTERCOM_TOKEN = creds.get("INTERCOM_TOKEN")
NOTION_TOKEN = creds.get("NOTION_TOKEN")
ANTHROPIC_KEY = creds.get("ANTHROPIC_API_KEY")

NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"
NOTION_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"  # MCT data source

# Generic email domains that won't match a business customer
GENERIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "icloud.com", "protonmail.com", "live.com", "me.com",
}

intercom_headers = {
    "Authorization": f"Bearer {INTERCOM_TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}
notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
notion_headers_mct = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}
anthropic_headers = {
    "x-api-key": ANTHROPIC_KEY,
    "anthropic-version": "2023-06-01",
    "Content-Type": "application/json",
}

# ── Conversations to backfill ─────────────────────────────────────────────────
# Issue type and severity are overrides from Intercom CDA (confirmed correct)
BACKFILL_CONVERSATIONS = [
    {
        "id": "215473213477942",
        "issue_type": "Bug",
        "cs_severity": "Important",
        "note": "Post-close attribute change — webhook fired before attrs were updated",
    },
    {
        "id": "215473211569900",
        "issue_type": "Bug",
        "cs_severity": "Not important",
        "note": "Dropped by severity filter",
    },
    {
        "id": "215473210256993",
        "issue_type": "Config Issue",
        "cs_severity": "Not important",
        "note": "Dropped by severity filter",
    },
    {
        "id": "215473208362849",
        "issue_type": "Config Issue",
        "cs_severity": "Not important",
        "note": "Dropped by severity filter",
    },
    {
        "id": "215473198746105",
        "issue_type": "New Feature Request",
        "cs_severity": "Not important",
        "note": "Dropped by severity filter",
    },
]


# ── Intercom helpers ──────────────────────────────────────────────────────────

def get_intercom_conversation(conv_id):
    """Fetch full Intercom conversation with all parts."""
    print(f"  Fetching Intercom conversation {conv_id}...")
    r = requests.get(
        f"https://api.intercom.io/conversations/{conv_id}",
        headers=intercom_headers,
    )
    r.raise_for_status()
    return r.json()


def get_intercom_contact(contact_id):
    """Fetch contact details to get email."""
    r = requests.get(
        f"https://api.intercom.io/contacts/{contact_id}",
        headers=intercom_headers,
    )
    if r.status_code == 200:
        return r.json()
    return {}


def extract_conversation_data(conv, override_issue_type, override_severity):
    """Extract conversation fields, applying overrides for issue type and severity."""
    # Contacts
    contacts = conv.get("contacts", {}).get("contacts", [])
    user_name = ""
    user_email = ""
    if contacts:
        c = contacts[0]
        user_name = c.get("name", "") or ""
        contact_id = c.get("id", "")
        if contact_id:
            contact_data = get_intercom_contact(contact_id)
            user_email = contact_data.get("email", "") or ""
            if not user_name:
                user_name = contact_data.get("name", "") or ""

    # Company
    company_name = ""
    companies = conv.get("companies", {})
    if isinstance(companies, dict):
        company_list = companies.get("companies", [])
        if company_list:
            company_name = company_list[0].get("name", "") or ""

    # Custom attributes — use Intercom CDA values for description
    custom_attrs = conv.get("custom_attributes", {}) or {}
    # Override issue type and severity with confirmed values from investigation
    issue_type = override_issue_type
    cs_severity = override_severity
    # Use the description from Intercom (should be set after post-close changes)
    issue_description = (
        custom_attrs.get("issue_description")
        or custom_attrs.get("Issue Description")
        or ""
    )

    # Conversation text: compile all message parts
    parts = conv.get("conversation_parts", {}).get("conversation_parts", [])
    messages = []

    # First message from source
    source = conv.get("source", {})
    if source:
        body = source.get("body", "") or ""
        body = body.replace("<br>", "\n").replace("<br/>", "\n")
        body = re.sub(r"<[^>]+>", "", body).strip()
        if body:
            author = source.get("author", {})
            author_name = author.get("name", "") or author.get("email", "") or "User"
            messages.append(f"{author_name}: {body}")

    for part in parts:
        if part.get("part_type") in ("comment", "note", "reply"):
            body = part.get("body", "") or ""
            if not body:
                continue
            body = body.replace("<br>", "\n").replace("<br/>", "\n")
            body = re.sub(r"<[^>]+>", "", body).strip()
            if body:
                author = part.get("author", {})
                author_name = author.get("name", "") or author.get("email", "") or "Agent"
                messages.append(f"{author_name}: {body}")

    conversation_text = "\n\n".join(messages)

    # Created timestamp
    created_ts = conv.get("created_at", 0) or 0
    created_at = (
        datetime.fromtimestamp(created_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        if created_ts
        else datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    )

    # Email domain
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


# ── Notion dedup check ────────────────────────────────────────────────────────

def check_notion_duplicate(conv_id):
    """Check if a Notion issue already exists for this conversation ID."""
    r = requests.post(
        f"https://api.notion.com/v1/databases/{NOTION_ISSUES_DB}/query",
        headers=notion_headers,
        json={
            "filter": {
                "property": "Source ID",
                "rich_text": {"equals": conv_id},
            },
            "page_size": 1,
        },
    )
    r.raise_for_status()
    results = r.json().get("results", [])
    return len(results) > 0, results[0]["id"] if results else None


# ── Claude summarization ──────────────────────────────────────────────────────

def call_claude(data):
    """Call Claude to generate issue title + summary."""
    print(f"  Calling Claude for {data['conversation_id']}...")

    prompt = (
        "You are a customer support issue summarizer for Konvo AI, a B2B SaaS platform "
        "that provides AI-powered sales assistants for e-commerce businesses.\n\n"
        "A CS manager has closed an Intercom conversation. Your job is to:\n"
        "1. Generate a clean, descriptive ISSUE TITLE (max 80 chars, in English)\n"
        "2. Write a brief SUMMARY of the issue (2-3 sentences, in English)\n\n"
        f"Customer: {data['user_name']} ({data['user_email']})\n"
        f"Company: {data['company_name']}\n\n"
        f"CS Manager Description: {data['issue_description'] or 'None provided'}\n\n"
        f"Full Conversation Thread:\n"
        f"{data['conversation_text'] or 'No conversation data available'}\n\n"
        "ISSUE TITLE rules: Max 80 chars, start with customer or company name, "
        "always in English regardless of conversation language. "
        "If no conversation data, use the CS Manager Description.\n\n"
        "SUMMARY rules: 2-3 sentences describing the core issue, in English. "
        "If no data available, say 'No conversation data available - manual review needed.'\n\n"
        'Response format (JSON only):\n'
        '{"issue_title": "<max 80 chars>", "summary": "<2-3 sentences>"}'
    )

    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=anthropic_headers,
        json={
            "model": "claude-sonnet-4-5-20250929",
            "max_tokens": 500,
            "messages": [{"role": "user", "content": prompt}],
        },
    )
    r.raise_for_status()

    text = r.json()["content"][0]["text"].strip()
    text = text.replace("```json", "").replace("```", "").strip()

    try:
        parsed = json.loads(text)
        issue_title = (parsed.get("issue_title") or "")[:80]
        summary = parsed.get("summary") or ""
    except Exception:
        issue_title = (
            f"{data['user_name'] or data['user_email'] or 'Unknown'}: "
            f"{data['issue_description'] or 'Needs Review'}"
        )[:80]
        summary = data["issue_description"] or "AI summary generation failed."

    print(f"    Title: {issue_title}")
    return issue_title, summary


# ── Notion customer lookup ────────────────────────────────────────────────────

def find_customer_by_domain(domain):
    """Query MCT by email domain. Returns (page_id, cs_owner) or (None, None)."""
    if not domain or domain in GENERIC_DOMAINS:
        print(f"  Skipping customer lookup — generic domain: {domain or '(empty)'}")
        return None, None

    print(f"  Looking up customer by domain: {domain}...")
    r = requests.post(
        f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
        headers=notion_headers_mct,
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
    print(f"  Found customer: {page_id[:8]}... cs_owner={cs_owner!r}")
    return page_id, cs_owner


# ── Notion issue creation ─────────────────────────────────────────────────────

def build_notion_payload(data, issue_title, summary, customer_page_id):
    """Build the Notion Issues table page creation payload."""
    # Notion Issues table valid severities (as of Feb 2026 rename)
    valid_severities = ["Urgent", "Important", "Not important"]
    sev = data.get("cs_severity", "")
    mapped_severity = sev if sev in valid_severities else "Important"

    # Normalize issue type (handle capital-R variant)
    issue_type_map = {
        "New Feature Request": "New Feature Request",
        "New feature request": "New feature request",
        "Bug": "Bug",
        "Config Issue": "Config Issue",
        "Feature improvement": "Feature improvement",
    }
    notion_issue_type = issue_type_map.get(data["issue_type"], data["issue_type"])

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
                {"text": {"content": (data.get("conversation_text") or "")[:2000]}}
            ]
        },
        "Severity": {"select": {"name": mapped_severity}},
        "Status": {"select": {"name": "Open"}},
        "Issue Type": {"select": {"name": notion_issue_type}},
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
    page_id = page["id"]
    print(f"  Created Notion page: https://notion.so/{page_id.replace('-', '')}")
    return page_id


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("backfill_missed_issues.py — Backfilling 5 missed conversations")
    print("=" * 60)

    results = []

    for target in BACKFILL_CONVERSATIONS:
        conv_id = target["id"]
        print(f"\n{'─' * 50}")
        print(f"Conversation {conv_id}")
        print(f"  Type: {target['issue_type']}  Severity: {target['cs_severity']}")
        print(f"  Note: {target['note']}")

        # a. Dedup check
        print(f"  Checking Notion for existing entry...")
        already_exists, existing_id = check_notion_duplicate(conv_id)
        if already_exists:
            print(f"  SKIP — already in Notion: {existing_id}")
            results.append({"conv_id": conv_id, "status": "skipped", "reason": "already_exists"})
            continue

        # b. Fetch Intercom conversation
        try:
            conv = get_intercom_conversation(conv_id)
        except Exception as e:
            print(f"  ERROR fetching Intercom conversation: {e}")
            results.append({"conv_id": conv_id, "status": "error", "reason": str(e)})
            continue

        data = extract_conversation_data(conv, target["issue_type"], target["cs_severity"])
        print(f"  User: {data['user_name']} ({data['user_email']})")
        print(f"  Domain: {data['email_domain']}")
        print(f"  Issue description: {(data['issue_description'] or '(empty)')[:80]}")
        print(f"  Conv text: {len(data['conversation_text'])} chars")

        # c. Claude title + summary
        try:
            issue_title, summary = call_claude(data)
        except Exception as e:
            print(f"  Claude error: {e}")
            issue_title = (
                f"{data['user_name'] or data['user_email'] or 'Unknown'}: "
                f"{data['issue_description'] or 'Needs Review'}"
            )[:80]
            summary = data["issue_description"] or ""

        # d. Customer lookup
        customer_page_id, cs_owner = find_customer_by_domain(data["email_domain"])

        # e. Build payload + create Notion issue
        payload = build_notion_payload(data, issue_title, summary, customer_page_id)
        page_id = create_notion_issue(payload)

        if page_id:
            results.append({
                "conv_id": conv_id,
                "status": "created",
                "page_id": page_id,
                "issue_type": target["issue_type"],
                "severity": target["cs_severity"],
                "customer": customer_page_id is not None,
            })
        else:
            results.append({"conv_id": conv_id, "status": "error", "reason": "notion_create_failed"})

        time.sleep(1.5)  # Rate limit protection

    # Summary
    print(f"\n{'=' * 60}")
    print("BACKFILL SUMMARY")
    print("=" * 60)
    created = [r for r in results if r["status"] == "created"]
    skipped = [r for r in results if r["status"] == "skipped"]
    errors = [r for r in results if r["status"] == "error"]

    print(f"  Created:  {len(created)}")
    print(f"  Skipped:  {len(skipped)} (already in Notion)")
    print(f"  Errors:   {len(errors)}")

    for r in created:
        cust = "✓ linked" if r.get("customer") else "⚠ no match"
        print(f"  ✓ {r['conv_id']} — {r['issue_type']} / {r['severity']} — customer: {cust}")

    for r in errors:
        print(f"  ✗ {r['conv_id']} — {r.get('reason', 'unknown')}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
