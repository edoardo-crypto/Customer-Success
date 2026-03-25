#!/usr/bin/env python3
"""
intercom_engagement_report.py
------------------------------
Fetches all Intercom conversations since Dec 1, 2025,
matches them to customers in the Notion MCT by email domain,
and produces a CSV: company_name, domain, billing_status,
                    conversation_count, unique_contacts, contact_emails
"""

import json
import csv
import sys
import time
import requests
import re
from datetime import datetime, timezone
from pathlib import Path
import creds

# ── Credentials ──────────────────────────────────────────────────────────────
INTERCOM_TOKEN = creds.get("INTERCOM_TOKEN")
NOTION_TOKEN   = creds.get("NOTION_TOKEN")
NOTION_DS_ID   = "3ceb1ad0-91f1-40db-945a-c51c58035898"

# Since Dec 1, 2025 00:00 UTC
SINCE_TS = int(datetime(2025, 12, 1, tzinfo=timezone.utc).timestamp())

# Free email providers to exclude from domain matching
GENERIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "aol.com", "protonmail.com", "live.com", "msn.com", "me.com",
    "googlemail.com", "ymail.com", "mail.com"
}

INTERCOM_HDRS = {
    "Authorization": f"Bearer {INTERCOM_TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Intercom-Version": "2.11",
}

NOTION_HDRS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}


# ── Notion: fetch all MCT customers ─────────────────────────────────────────

def fetch_mct_customers():
    customers = []
    cursor = None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        resp = requests.post(
            f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
            headers=NOTION_HDRS, json=body, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        for page in data.get("results", []):
            props = page.get("properties", {})

            name_prop = props.get("🏢 Company Name", props.get("Company Name", {}))
            name = "".join(t.get("plain_text", "") for t in name_prop.get("title", [])).strip()
            if not name:
                continue

            billing_prop = props.get("💰 Billing Status", {})
            billing = (billing_prop.get("select") or {}).get("name", "")

            domain_prop = props.get("🏢 Domain", props.get("Domain", {}))
            domain = "".join(t.get("plain_text", "") for t in domain_prop.get("rich_text", [])).strip().lower()
            domain = re.sub(r"^https?://", "", domain)
            domain = re.sub(r"^www\.", "", domain)
            domain = domain.rstrip("/")

            customers.append({"name": name, "domain": domain, "billing_status": billing})

        if data.get("has_more"):
            cursor = data.get("next_cursor")
        else:
            break

    return customers


# ── Intercom: fetch all conversations since SINCE_TS ──────────────────────

def fetch_all_conversations():
    """Search all conversations with created_at > SINCE_TS. Returns list of conv objects."""
    conversations = []
    starting_after = None

    print(f"  Fetching conversations (created after Dec 1 2025)...")
    while True:
        body = {
            "query": {
                "operator": "AND",
                "value": [
                    {"field": "created_at", "operator": ">", "value": SINCE_TS}
                ]
            },
            "pagination": {"per_page": 150}
        }
        if starting_after:
            body["pagination"]["starting_after"] = starting_after

        resp = requests.post(
            "https://api.intercom.io/conversations/search",
            headers=INTERCOM_HDRS, json=body, timeout=60
        )
        if not resp.ok:
            print(f"  ERROR {resp.status_code}: {resp.text[:300]}")
            break

        data = resp.json()
        items = data.get("conversations", [])
        conversations.extend(items)
        print(f"    Fetched {len(items)} → total {len(conversations)}")

        pages = data.get("pages", {})
        next_cur = pages.get("next", {})
        if isinstance(next_cur, dict):
            starting_after = next_cur.get("starting_after")
        else:
            starting_after = None

        if not starting_after:
            break

    print(f"  Total conversations: {len(conversations)}")
    return conversations


# ── Intercom: fetch contact by ID to get email ───────────────────────────

def fetch_contact(contact_id):
    resp = requests.get(
        f"https://api.intercom.io/contacts/{contact_id}",
        headers=INTERCOM_HDRS, timeout=30
    )
    if resp.ok:
        return resp.json()
    return {}


def batch_fetch_contacts(contact_ids):
    """Fetch multiple contacts; returns {id: email}."""
    result = {}
    ids = list(contact_ids)
    print(f"  Fetching {len(ids)} unique contacts for email lookup...")
    for i, cid in enumerate(ids):
        if i > 0 and i % 50 == 0:
            print(f"    {i}/{len(ids)} contacts fetched...")
            time.sleep(0.2)  # gentle rate limiting
        c = fetch_contact(cid)
        email = c.get("email", "")
        if email:
            result[cid] = email.lower()
    return result


# ── Build domain → conversations map ─────────────────────────────────────

def build_domain_map(conversations):
    """
    Extract emails from conversations (source.author + contacts).
    Returns {domain: {"conv_ids": set, "emails": set, "contact_ids": set}}
    Also returns set of contact IDs that we still need to look up.
    """
    domain_map = {}
    unfetched_contact_ids = set()

    for conv in conversations:
        conv_id = conv.get("id")
        emails_this_conv = set()
        contact_ids_this_conv = set()

        # 1. source.author (the person who opened the conversation)
        source_author = conv.get("source", {}).get("author", {})
        author_email = source_author.get("email", "")
        if author_email:
            emails_this_conv.add(author_email.lower())

        # 2. contacts list (newer API versions include email here)
        for cref in conv.get("contacts", {}).get("contacts", []):
            email = cref.get("email", "")
            cid   = cref.get("id", "")
            if email:
                emails_this_conv.add(email.lower())
            elif cid:
                contact_ids_this_conv.add(cid)

        # 3. Any email found in first_contact_reply
        fcr_email = conv.get("first_contact_reply", {})
        if isinstance(fcr_email, dict):
            e = fcr_email.get("author", {}).get("email", "")
            if e:
                emails_this_conv.add(e.lower())

        for email in emails_this_conv:
            if "@" not in email:
                continue
            domain = email.split("@")[-1].lower()
            if domain in GENERIC_DOMAINS:
                continue
            if domain not in domain_map:
                domain_map[domain] = {"conv_ids": set(), "emails": set(), "contact_ids": set()}
            domain_map[domain]["conv_ids"].add(conv_id)
            domain_map[domain]["emails"].add(email)

        # Remember contact IDs we don't have emails for yet
        unfetched_contact_ids.update(contact_ids_this_conv)

    return domain_map, unfetched_contact_ids


def enrich_domain_map_with_contacts(domain_map, contact_id_email_map, conversations):
    """
    For contacts we fetched separately (email was missing from conv object),
    re-scan conversations and add their emails to domain_map.
    """
    # Build contact_id → domain lookup from what we fetched
    contact_domain = {}
    for cid, email in contact_id_email_map.items():
        if "@" in email:
            domain = email.split("@")[-1].lower()
            if domain not in GENERIC_DOMAINS:
                contact_domain[cid] = (domain, email)

    # Re-scan conversations
    for conv in conversations:
        conv_id = conv.get("id")
        for cref in conv.get("contacts", {}).get("contacts", []):
            cid = cref.get("id", "")
            if cid in contact_domain:
                domain, email = contact_domain[cid]
                if domain not in domain_map:
                    domain_map[domain] = {"conv_ids": set(), "emails": set(), "contact_ids": set()}
                domain_map[domain]["conv_ids"].add(conv_id)
                domain_map[domain]["emails"].add(email)


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 60)
    print("  Intercom Engagement Report (since Dec 1, 2025)")
    print("=" * 60)
    print()

    # 1. MCT customers
    print("[1/5] Fetching customers from Notion MCT...")
    customers = fetch_mct_customers()
    print(f"  Total customers: {len(customers)}")
    active = [c for c in customers if c["billing_status"] == "Active"]
    churning = [c for c in customers if c["billing_status"] == "Churning"]
    print(f"  Active: {len(active)} | Churning: {len(churning)}")
    print()

    # 2. All Intercom conversations since Dec 1
    print("[2/5] Fetching all Intercom conversations since Dec 1, 2025...")
    conversations = fetch_all_conversations()
    print()

    # 3. Build initial domain map from conversation data
    print("[3/5] Extracting domains from conversation data...")
    domain_map, unfetched_ids = build_domain_map(conversations)
    print(f"  Domains found with emails in conv data: {len(domain_map)}")
    print(f"  Contact IDs that need separate lookup: {len(unfetched_ids)}")
    print()

    # 4. Fetch missing contacts
    if unfetched_ids:
        print("[4/5] Fetching remaining contacts for email lookup...")
        contact_email_map = batch_fetch_contacts(unfetched_ids)
        enrich_domain_map_with_contacts(domain_map, contact_email_map, conversations)
        print(f"  Domain map after enrichment: {len(domain_map)} domains")
    else:
        print("[4/5] No additional contact lookups needed.")
    print()

    # 5. Match to MCT and write CSV
    print("[5/5] Matching domains to MCT and writing CSV...")

    rows = []
    for customer in customers:
        domain = customer["domain"]
        match = domain_map.get(domain, {})
        conv_count = len(match.get("conv_ids", set()))
        emails = sorted(match.get("emails", set()))
        unique_contacts = len(set(e.split("@")[0] for e in emails))  # rough count

        rows.append({
            "company_name": customer["name"],
            "domain": domain or "(no domain)",
            "billing_status": customer["billing_status"],
            "conversations_since_dec_2025": conv_count,
            "unique_contact_emails": len(emails),
            "contact_emails": " | ".join(emails),
        })

    # Sort: billing status order (Active → Churning → Canceled), then conv count desc
    order = {"Active": 0, "Churning": 1, "Canceled": 2, "": 3}
    rows.sort(key=lambda x: (order.get(x["billing_status"], 3), -x["conversations_since_dec_2025"]))

    output_path = Path("/Users/edoardopelli/projects/Customer Success/intercom_engagement.csv")
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "company_name", "domain", "billing_status",
            "conversations_since_dec_2025", "unique_contact_emails", "contact_emails"
        ])
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Written: {output_path}")
    print()

    # Summary
    active_with = [r for r in rows if r["billing_status"] == "Active" and r["conversations_since_dec_2025"] > 0]
    active_without = [r for r in rows if r["billing_status"] == "Active" and r["conversations_since_dec_2025"] == 0]

    print("=" * 60)
    print(f"  Active customers WITH conversations (Dec-now):    {len(active_with)}")
    print(f"  Active customers WITHOUT conversations (Dec-now): {len(active_without)}")
    print()
    print("  Top 15 most active:")
    print(f"  {'Company':<35} {'Status':<10} {'Convs':>5}  Domain")
    print(f"  {'-'*35} {'-'*10} {'-'*5}  {'-'*30}")
    for r in rows[:15]:
        print(f"  {r['company_name']:<35} {r['billing_status']:<10} {r['conversations_since_dec_2025']:>5}  {r['domain']}")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
