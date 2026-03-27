#!/usr/bin/env python3
"""
classify_untagged_issues.py — Classify issues without a Category in Notion.

Queries the Issues Table for bugs with empty Category, calls Claude Sonnet
to classify them (AI Agent / Inbox / WhatsApp Marketing / Integration),
and writes the result back to Notion.

Run daily via GitHub Actions or manually.
"""

import json
import os
import sys
import time
import requests

try:
    import creds
    NOTION_TOKEN = creds.get("NOTION_TOKEN")
    ANTHROPIC_KEY = creds.get("ANTHROPIC_API_KEY")
except Exception:
    NOTION_TOKEN = os.environ["NOTION_TOKEN"]
    ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]

ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"
VALID_CATEGORIES = ["AI Agent", "Inbox", "WhatsApp Marketing", "Integration"]
BATCH_SIZE = 20

notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

CLASSIFY_PROMPT = """\
Classify each customer-support issue into exactly one category.

DECISION FLOWCHART — follow this order:
1. Is it about a FLOW or BROADCAST? → WhatsApp Marketing
2. Is it about an EXTERNAL TOOL not working with Konvo (Shopify, Gorgias, Zendesk, \
Klaviyo, Outlook, WhatsApp/Instagram channel connection, files from external channel \
not appearing, customer data from external tools not showing)? → Integration
3. Is it about the AI's behavior, responses, or an AI-specific feature (product recs, \
order lookup, handover, OTP handled by AI, AI not responding, AI response quality, \
playground, personas)? → AI Agent
4. Everything else about Konvo's own platform UI/inbox/messaging → Inbox

VALID CATEGORIES (use these exact strings):
- AI Agent          → AI recommending wrong products, AI can't find orders, \
handover/transfer failures, AI wrong language, OTP not identified by AI, \
AI turned off/not responding, AI giving wrong info, AI-specific features (playground, personas)
- Inbox             → inbox slow/not loading, messages missing/duplicated/expired, \
notifications not updating, search bar issues, UI glitches, snooze bugs, conversation display
- WhatsApp Marketing → ALL broadcast issues (not sending, errors, variables, media), \
ALL flow issues (stopping, misfiring, wrong triggers, sent to wrong person, opt-out flows)
- Integration       → Gorgias/Zendesk issues, Shopify/WooCommerce sync, Klaviyo data sync, \
email/Outlook/Instagram/WhatsApp channel connection, files from external channels not appearing, \
customer data from external tools not showing, OTP/SMS delivery from provider

DISAMBIGUATION:
- ALL flow and broadcast bugs → WhatsApp Marketing (no exceptions, even AI opt-out flows)
- Files/attachments from external channels not appearing in Konvo → Integration (NOT Inbox)
- Customer data from Shopify/Klaviyo not showing in Konvo → Integration (NOT Inbox)
- Gorgias/Zendesk limited functionality → Integration (NOT Inbox)
- "AI" in title but platform feature broken (not AI-specific) → Inbox
- OTP from SMS provider failing → Integration; OTP not identified by AI → AI Agent

ISSUES:
{issues_json}

Return ONLY a JSON object mapping each "id" to its "category".
Example: {{"abc": "AI Agent", "def": "Integration"}}
Use the exact strings above. No other text."""


def query_unclassified():
    """Find all issues with empty Category."""
    pages = []
    body = {
        "filter": {
            "property": "Category",
            "select": {"is_empty": True},
        },
        "page_size": 100,
    }
    start_cursor = None
    while True:
        if start_cursor:
            body["start_cursor"] = start_cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{ISSUES_DB}/query",
            headers=notion_headers, json=body, timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        if data.get("has_more") and data.get("next_cursor"):
            start_cursor = data["next_cursor"]
        else:
            break
    return pages


def extract_text(page):
    """Get title + summary for classification."""
    props = page.get("properties", {})
    title_parts = props.get("Issue Title", {}).get("title", [])
    title = title_parts[0]["plain_text"] if title_parts else ""
    summary_parts = props.get("Summary", {}).get("rich_text", [])
    summary = summary_parts[0]["plain_text"] if summary_parts else ""
    return (title + " | " + summary)[:500]


def classify_batch(batch):
    """Call Claude Sonnet to classify a batch. Returns {id: category}."""
    issues_json = json.dumps(
        [{"id": item["id"], "text": item["text"]} for item in batch],
        ensure_ascii=False,
    )
    prompt = CLASSIFY_PROMPT.format(issues_json=issues_json)

    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-6",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    r.raise_for_status()

    text = ""
    for block in r.json().get("content", []):
        if block.get("type") == "text":
            text = block.get("text", "")

    text = text.replace("```json", "").replace("```", "").strip()
    parsed = json.loads(text)

    validated = {}
    for pid, cat in parsed.items():
        if cat in VALID_CATEGORIES:
            validated[pid] = cat
        else:
            print(f"  Unknown category '{cat}' for {pid} — skipping")
    return validated


def patch_category(page_id, category):
    """Write Category back to Notion."""
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers,
        json={"properties": {"Category": {"select": {"name": category}}}},
        timeout=20,
    )
    return r.status_code == 200


def main():
    print("Classifying untagged issues...")

    pages = query_unclassified()
    items = []
    for p in pages:
        text = extract_text(p)
        if text.strip() and text.strip() != "|":
            items.append({"id": p["id"], "text": text})

    print(f"  Found {len(items)} issues without Category")

    if not items:
        print("  Nothing to classify.")
        return

    total_ok = 0
    total_err = 0

    for i in range(0, len(items), BATCH_SIZE):
        batch = items[i:i + BATCH_SIZE]
        print(f"  Batch {i // BATCH_SIZE + 1} ({len(batch)} issues)...")

        try:
            results = classify_batch(batch)
        except Exception as e:
            print(f"  Claude error: {e}")
            total_err += len(batch)
            continue

        for pid, cat in results.items():
            ok = patch_category(pid, cat)
            if ok:
                total_ok += 1
                print(f"    {pid[:8]}... → {cat}")
            else:
                total_err += 1

        time.sleep(1)

    print(f"\nDone. Classified: {total_ok}, Errors: {total_err}")
    if total_err:
        sys.exit(1)


if __name__ == "__main__":
    main()
