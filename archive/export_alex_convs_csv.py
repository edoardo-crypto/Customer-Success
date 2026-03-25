#!/usr/bin/env python3
"""
Exports Alex's conversations from last Friday + this week as a clean CSV.
Columns: Client Name, Short Description (AI summary), Intercom Link
"""

import csv
import re
import anthropic
import requests
from datetime import date, datetime, timezone
import creds

INTERCOM_TOKEN  = creds.get("INTERCOM_TOKEN")
ALEX_ADMIN_ID   = "7484673"
INTERCOM_APP_ID = "o0lp6qsb"
INTERCOM_API    = "https://api.intercom.io"

NOTION_TOKEN = creds.get("NOTION_TOKEN")
MCT_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"

WEEK_START = date(2026, 2, 20)   # last Friday
WEEK_END   = date(2026, 3, 2)

OUTPUT_FILE = "/Users/edoardopelli/Downloads/alex_convs_fri_w09.csv"

headers = {
    "Authorization":    f"Bearer {INTERCOM_TOKEN}",
    "Intercom-Version": "2.11",
    "Accept":           "application/json",
    "Content-Type":     "application/json",
}

ai = anthropic.Anthropic(api_key=creds.get("ANTHROPIC_API_KEY"))


def _to_unix(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def fetch_alex_conversations():
    url = f"{INTERCOM_API}/conversations/search"
    query = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "created_at", "operator": ">=", "value": _to_unix(WEEK_START)},
                {"field": "created_at", "operator": "<=", "value": _to_unix(WEEK_END)},
            ],
        },
        "pagination": {"per_page": 150},
    }
    all_convs = []
    cursor = None
    while True:
        if cursor:
            query["pagination"]["starting_after"] = cursor
        elif "starting_after" in query["pagination"]:
            del query["pagination"]["starting_after"]
        r = requests.post(url, headers=headers, json=query)
        r.raise_for_status()
        data = r.json()
        batch = data.get("conversations", [])
        all_convs.extend(batch)
        pages = data.get("pages", {})
        next_page = pages.get("next", {})
        cursor = next_page.get("starting_after") if isinstance(next_page, dict) else None
        if not cursor or not batch:
            break
    return [c for c in all_convs
            if str((c.get("statistics") or {}).get("last_closed_by_id", "")) == ALEX_ADMIN_ID]


def fetch_mct_domains():
    """Return {domain: company_name} for all MCT rows that have a domain set."""
    notion_headers = {
        "Authorization":    f"Bearer {NOTION_TOKEN}",
        "Notion-Version":   "2025-09-03",
        "Content-Type":     "application/json",
    }
    url = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"
    domain_map = {}
    cursor = None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        r = requests.post(url, headers=notion_headers, json=body)
        r.raise_for_status()
        data = r.json()
        for page in data.get("results", []):
            props = page.get("properties", {})
            try:
                domain = props["🏢 Domain"]["rich_text"][0]["plain_text"].strip().lower()
            except (KeyError, IndexError):
                domain = ""
            try:
                company = props["🏢 Company Name"]["title"][0]["plain_text"].strip()
            except (KeyError, IndexError):
                company = ""
            if domain and company:
                domain_map[domain] = company
        if data.get("has_more"):
            cursor = data.get("next_cursor")
        else:
            break
    return domain_map


def get_contact_info(contact_id):
    """Return (name, email) for an Intercom contact."""
    r = requests.get(f"{INTERCOM_API}/contacts/{contact_id}", headers=headers)
    if r.status_code != 200:
        return None, None
    d = r.json()
    name  = (d.get("name") or "").strip()
    email = (d.get("email") or "").strip()
    return name or None, email or None


def clean_html(text):
    """Strip HTML tags and collapse whitespace."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


CALENDAR_NOISE_RE = re.compile(
    r"^(Aceptad[ao]|Acceptad[ao]|Accepted|Declined|Tentative|Rechazado)[:\s]",
    re.IGNORECASE,
)


def is_calendar_noise(conv):
    source = conv.get("source") or {}
    subject = clean_html(source.get("subject") or "")
    return bool(CALENDAR_NOISE_RE.match(subject))


def fetch_full_conversation(conv_id):
    """Return a flat list of (author_type, text) tuples for the whole thread."""
    r = requests.get(
        f"{INTERCOM_API}/conversations/{conv_id}",
        headers=headers,
        params={"display_as": "plaintext"},
    )
    if r.status_code != 200:
        return []
    data = r.json()
    messages = []

    # Opening message
    source = data.get("source") or {}
    opening = clean_html(source.get("body") or "")
    if opening:
        author_type = (source.get("author") or {}).get("type", "user")
        messages.append((author_type, opening[:800]))

    # Subsequent parts — only real messages, skip assignments/notes/etc.
    parts = (data.get("conversation_parts") or {}).get("conversation_parts", [])
    for part in parts:
        body = clean_html(part.get("body") or "")
        if not body:
            continue
        author_type = (part.get("author") or {}).get("type", "unknown")
        messages.append((author_type, body[:800]))

    return messages


def summarize_conversation(conv_id, company):
    """Fetch full thread and return a 1-2 sentence AI summary."""
    messages = fetch_full_conversation(conv_id)
    if not messages:
        return "—"

    # Build a readable transcript (cap at 30 turns to avoid token bloat)
    lines = []
    for author_type, text in messages[:30]:
        label = "Customer" if author_type == "user" else "Support"
        lines.append(f"{label}: {text}")
    transcript = "\n\n".join(lines)

    resp = ai.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=120,
        messages=[{
            "role": "user",
            "content": (
                f"This is a customer support conversation for {company}. "
                "Reply with 1-2 plain sentences only (no headers, no bullet points, no markdown): "
                "what did the customer need, and what was the outcome?\n\n"
                f"{transcript}"
            ),
        }],
    )
    return resp.content[0].text.strip()


def main():
    print("Fetching MCT domains from Notion …")
    domain_map = fetch_mct_domains()
    print(f"Loaded {len(domain_map)} known customer domains\n")

    print(f"Fetching Alex's conversations from {WEEK_START} …")
    convs = fetch_alex_conversations()
    print(f"Found {len(convs)} conversations attributed to Alex\n")

    rows = []
    skipped = 0
    for i, c in enumerate(convs, 1):
        conv_id = c.get("id", "")
        link = f"https://app.intercom.com/a/inbox/{INTERCOM_APP_ID}/inbox/conversation/{conv_id}"

        contact_id = None
        try:
            contact_id = c["contacts"]["contacts"][0]["id"]
        except (KeyError, IndexError):
            pass

        name, email = None, None
        if contact_id:
            print(f"  [{i}/{len(convs)}] Fetching contact {contact_id} …")
            name, email = get_contact_info(contact_id)

        # Domain-filter: skip if not a known customer
        domain = email.split("@")[1].lower() if email and "@" in email else ""
        company = domain_map.get(domain)
        if not company:
            print(f"         → SKIP (domain={domain or 'unknown'})")
            skipped += 1
            continue

        if is_calendar_noise(c):
            print(f"         → SKIP calendar noise")
            skipped += 1
            continue

        print(f"         → {company} — summarizing …")
        desc = summarize_conversation(conv_id, company)
        print(f"            {desc[:80]}")

        rows.append([company, desc, link])

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Client Name", "Short Description", "Intercom Link"])
        writer.writerows(rows)

    print(f"\n{len(rows)} real-customer rows kept, {skipped} automated/unknown skipped.")
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
