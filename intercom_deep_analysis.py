#!/usr/bin/env python3
"""
intercom_deep_analysis.py
--------------------------
For customers with 1–5 Intercom conversations (Dec 1 2025 – now),
classifies each conversation as genuine / outbound / no_reply / bot
to identify who is truly not engaging.

Input:  intercom_engagement.csv  (already generated)
Output: intercom_low_engagement_analysis.csv
"""

import csv
import html
import re
import time
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
INTERCOM_TOKEN = creds.get("INTERCOM_TOKEN")

SINCE_TS = int(datetime(2025, 12, 1, tzinfo=timezone.utc).timestamp())

GENERIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "aol.com", "protonmail.com", "live.com", "msn.com", "me.com",
    "googlemail.com", "ymail.com", "mail.com",
}

HEADERS = {
    "Authorization": f"Bearer {INTERCOM_TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Intercom-Version": "2.11",
}

INPUT_CSV  = Path("/Users/edoardopelli/projects/Customer Success/intercom_engagement.csv")
OUTPUT_CSV = Path("/Users/edoardopelli/projects/Customer Success/intercom_low_engagement_analysis.csv")


# ── Helpers ───────────────────────────────────────────────────────────────────

def strip_html(text: str, max_len: int = 120) -> str:
    """Remove HTML tags and truncate."""
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len]


def extract_email_domain(email: str) -> Optional[str]:
    """Return lowercased domain or None if generic/invalid."""
    if not email or "@" not in email:
        return None
    domain = email.split("@")[-1].lower()
    if domain in GENERIC_DOMAINS:
        return None
    return domain


def get_domains_from_conv(conv: dict) -> set[str]:
    """Extract all email domains found in a conversation summary object."""
    domains = set()

    # source author
    author_email = conv.get("source", {}).get("author", {}).get("email", "")
    d = extract_email_domain(author_email)
    if d:
        domains.add(d)

    # contacts list
    for cref in conv.get("contacts", {}).get("contacts", []):
        email = cref.get("email", "")
        d = extract_email_domain(email)
        if d:
            domains.add(d)

    # first_contact_reply author
    fcr = conv.get("first_contact_reply")
    if isinstance(fcr, dict):
        e = fcr.get("author", {}).get("email", "")
        d = extract_email_domain(e)
        if d:
            domains.add(d)

    return domains


# ── Step 1: load CSV, filter 1–5 conversations ───────────────────────────────

def load_low_engagement_customers() -> list[dict]:
    rows = []
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            n = int(row["conversations_since_dec_2025"])
            if 1 <= n <= 5:
                rows.append({
                    "company_name":   row["company_name"],
                    "domain":         row["domain"].lower().strip(),
                    "billing_status": row["billing_status"],
                    "total_conversations": n,
                })
    return rows


# ── Step 2: fetch all conversations since Dec 1 (paginated) ──────────────────

def fetch_all_conversations() -> list[dict]:
    convs = []
    starting_after = None
    print("  Fetching conversations from Intercom (Dec 1 2025 – now)...")
    while True:
        body: dict = {
            "query": {
                "operator": "AND",
                "value": [{"field": "created_at", "operator": ">", "value": SINCE_TS}],
            },
            "pagination": {"per_page": 150},
        }
        if starting_after:
            body["pagination"]["starting_after"] = starting_after

        resp = requests.post(
            "https://api.intercom.io/conversations/search",
            headers=HEADERS, json=body, timeout=60,
        )
        if not resp.ok:
            print(f"  ERROR {resp.status_code}: {resp.text[:300]}")
            break

        data = resp.json()
        items = data.get("conversations", [])
        convs.extend(items)
        print(f"    page fetched: {len(items)} → total {len(convs)}")

        pages = data.get("pages", {})
        nxt = pages.get("next", {})
        starting_after = nxt.get("starting_after") if isinstance(nxt, dict) else None
        if not starting_after:
            break

    print(f"  Total conversations fetched: {len(convs)}")
    return convs


# ── Step 3: filter to target domains ─────────────────────────────────────────

def filter_conversations(convs: list[dict], target_domains: set[str]) -> dict[str, list[str]]:
    """Returns {domain: [conv_id, ...]} for conversations matching target domains."""
    domain_convs: dict[str, list[str]] = {d: [] for d in target_domains}
    for conv in convs:
        conv_id = conv.get("id")
        domains = get_domains_from_conv(conv)
        for d in domains:
            if d in domain_convs:
                domain_convs[d].append(conv_id)
    return domain_convs


# ── Step 4: fetch full conversation details ───────────────────────────────────

def fetch_conversation_detail(conv_id: str) -> dict:
    resp = requests.get(
        f"https://api.intercom.io/conversations/{conv_id}",
        headers=HEADERS, timeout=30,
    )
    if resp.ok:
        return resp.json()
    print(f"    WARN: GET /conversations/{conv_id} → {resp.status_code}")
    return {}


# ── Step 5: classify a single conversation ───────────────────────────────────

def classify_conversation(detail: dict) -> str:
    """
    Rules:
    - "genuine"   : source.author.type in ("user", "lead")
    - "bot"       : source.author.type == "bot"
    - "outbound"  : source.author.type == "admin" AND first_contact_reply is not null
    - "no_reply"  : source.author.type == "admin" AND first_contact_reply is null
    """
    author_type = detail.get("source", {}).get("author", {}).get("type", "")
    has_reply = detail.get("first_contact_reply") is not None

    if author_type in ("user", "lead"):
        return "genuine"
    if author_type == "bot":
        return "bot"
    if author_type == "admin":
        return "outbound" if has_reply else "no_reply"
    return "no_reply"  # fallback for unknown/empty


def build_conv_detail_str(conv_id: str, detail: dict, classification: str) -> str:
    """Format: conv_id:author_type:classification:subject_snippet"""
    author_type = detail.get("source", {}).get("author", {}).get("type", "?")
    subject = strip_html(
        detail.get("source", {}).get("subject", "")
        or detail.get("source", {}).get("body", ""),
        max_len=80,
    )
    # sanitise pipe chars in subject
    subject = subject.replace("|", "/")
    return f"{conv_id}:{author_type}:{classification}:{subject}"


# ── Step 6: roll up engagement label ─────────────────────────────────────────

def engagement_label(genuine: int, total: int) -> str:
    if genuine == 0:
        return "Not Engaging"
    if genuine == total:
        return "Engaging"
    return "Partially Engaging"


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print()
    print("=" * 65)
    print("  Intercom Deep Engagement Analysis")
    print("=" * 65)
    print()

    # 1. Load low-engagement customers
    print("[1/5] Loading low-engagement customers from CSV (1–5 conversations)...")
    customers = load_low_engagement_customers()
    print(f"  Qualifying customers: {len(customers)}")
    target_domains = {c["domain"] for c in customers if c["domain"] not in ("", "(no domain)")}
    print(f"  Unique domains to match: {len(target_domains)}")
    print()

    # 2. Fetch all conversations
    print("[2/5] Fetching all conversations from Intercom...")
    all_convs = fetch_all_conversations()
    print()

    # 3. Filter to target domains
    print("[3/5] Filtering conversations to target domains...")
    domain_conv_ids = filter_conversations(all_convs, target_domains)
    total_to_fetch = sum(len(v) for v in domain_conv_ids.values())
    print(f"  Conversations matched to low-engagement customers: {total_to_fetch}")
    print()

    # 4. Fetch full details
    print("[4/5] Fetching full conversation details (0.2s delay each)...")
    # Deduplicate: a conv_id might match multiple domains (e.g. same email domain)
    all_conv_ids = set(cid for ids in domain_conv_ids.values() for cid in ids)
    conv_details: dict[str, dict] = {}
    for i, conv_id in enumerate(all_conv_ids):
        if i > 0 and i % 20 == 0:
            print(f"    {i}/{len(all_conv_ids)} fetched...")
        conv_details[conv_id] = fetch_conversation_detail(conv_id)
        time.sleep(0.2)
    print(f"  Fetched {len(conv_details)} full conversation details")
    print()

    # 5. Build output rows
    print("[5/5] Classifying and building output...")
    output_rows = []

    for customer in customers:
        domain = customer["domain"]
        conv_ids = domain_conv_ids.get(domain, [])

        counts = {"genuine": 0, "outbound": 0, "no_reply": 0, "bot": 0}
        details_strs = []

        for cid in conv_ids:
            detail = conv_details.get(cid, {})
            if not detail:
                classification = "no_reply"
            else:
                classification = classify_conversation(detail)
            counts[classification] += 1
            details_strs.append(build_conv_detail_str(cid, detail, classification))

        actual_total = sum(counts.values())
        label = engagement_label(counts["genuine"], actual_total) if actual_total > 0 else "No Data"

        output_rows.append({
            "company_name":        customer["company_name"],
            "domain":              domain,
            "billing_status":      customer["billing_status"],
            "total_conversations": customer["total_conversations"],
            "genuine":             counts["genuine"],
            "outbound":            counts["outbound"],
            "no_reply":            counts["no_reply"],
            "bot":                 counts["bot"],
            "engagement_label":    label,
            "conversation_details": " | ".join(details_strs),
        })

    # Sort: engagement label order, then company name
    label_order = {"Not Engaging": 0, "Partially Engaging": 1, "Engaging": 2, "No Data": 3}
    output_rows.sort(key=lambda r: (label_order.get(r["engagement_label"], 9), r["company_name"]))

    # Write CSV
    fieldnames = [
        "company_name", "domain", "billing_status", "total_conversations",
        "genuine", "outbound", "no_reply", "bot",
        "engagement_label", "conversation_details",
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)
    print(f"  Written: {OUTPUT_CSV}")
    print()

    # Summary table
    not_eng     = [r for r in output_rows if r["engagement_label"] == "Not Engaging"]
    partial_eng = [r for r in output_rows if r["engagement_label"] == "Partially Engaging"]
    engaging    = [r for r in output_rows if r["engagement_label"] == "Engaging"]
    no_data     = [r for r in output_rows if r["engagement_label"] == "No Data"]

    print("=" * 65)
    print(f"  Not Engaging:        {len(not_eng):>3}  (0 genuine conversations)")
    print(f"  Partially Engaging:  {len(partial_eng):>3}  (some genuine, some automated)")
    print(f"  Engaging:            {len(engaging):>3}  (all conversations are genuine)")
    print(f"  No Data (unmatched): {len(no_data):>3}")
    print()
    print(f"  {'Company':<36} {'Status':<10} {'Convs':>5}  {'Gen':>3}  {'Out':>3}  {'NRp':>3}  {'Bot':>3}  Label")
    print(f"  {'-'*36} {'-'*10} {'-'*5}  {'-'*3}  {'-'*3}  {'-'*3}  {'-'*3}  {'-'*18}")
    for r in output_rows:
        print(
            f"  {r['company_name']:<36.36} {r['billing_status']:<10} "
            f"{r['total_conversations']:>5}  {r['genuine']:>3}  {r['outbound']:>3}  "
            f"{r['no_reply']:>3}  {r['bot']:>3}  {r['engagement_label']}"
        )
    print("=" * 65)
    print()


if __name__ == "__main__":
    main()
