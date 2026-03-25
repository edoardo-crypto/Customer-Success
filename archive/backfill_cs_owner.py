#!/usr/bin/env python3
"""
backfill_cs_owner.py — Backfill CS Owner on existing Issues with empty Customer relation

For every issue in the Notion Issues table that has no Customer linked,
tries to match a customer using a 3-strategy waterfall:

  Strategy 1: Intercom Company ID (best — ID-based, no ambiguity)
    For Intercom issues: fetch the conversation, extract companies[0].id,
    look up MCT by "🔗 Intercom Company ID"

  Strategy 2: Main Contact Email exact match (medium — email-based)
    Query MCT by "👤 Main Contact Email" contains the full email address

  Strategy 3: Domain match (fallback — current approach)
    Extract domain from email, query MCT by "🏢 Domain" contains domain

Also verifies that today's recent issues already have a Customer set (workflow health check).

Usage:
  python3 backfill_cs_owner.py          # dry-run: show what WOULD be patched
  python3 backfill_cs_owner.py --apply  # actually patch
"""

import re
import sys
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
NOTION_TOKEN    = creds.get("NOTION_TOKEN")
INTERCOM_TOKEN  = creds.get("INTERCOM_TOKEN")

ISSUES_DB_ID   = "bd1ed48de20e426f8bebeb8e700d19d8"   # Issues Table
DS_ID          = "3ceb1ad0-91f1-40db-945a-c51c58035898"  # Master Customer Table data source

# Skip these domains — they are internal or generic free-mail providers
SKIP_DOMAINS = {
    "konvoai.com",
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "icloud.com", "me.com", "mac.com", "live.com",
    "aol.com", "msn.com", "protonmail.com", "proton.me",
    "yopmail.com", "mailinator.com", "guerrillamail.com",
}

# ── Headers ───────────────────────────────────────────────────────────────────
notion_v2022 = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
notion_v2025 = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}
intercom_headers = {
    "Authorization": f"Bearer {INTERCOM_TOKEN}",
    "Accept": "application/json",
    "Intercom-Version": "2.11",
}

# ── Dry-run flag ──────────────────────────────────────────────────────────────
DRY_RUN = "--apply" not in sys.argv


# ═════════════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════════════

def extract_email_and_domain(reported_by: str) -> tuple:
    """
    Parse full email and domain from Reported By text.
    Supported formats:
      "Alice Smith <alice@company.com>"
      "alice@company.com"
    Returns (email, domain) or (None, None) if unparseable / should be skipped.
    Domain is None (but email returned) if the domain is in SKIP_DOMAINS.
    """
    if not reported_by:
        return None, None

    # Try to pull email from angle brackets first
    match = re.search(r"<(.+?)>", reported_by)
    if match:
        email = match.group(1).strip()
    elif "@" in reported_by:
        email = reported_by.strip()
    else:
        return None, None

    if "@" not in email:
        return None, None

    domain = email.split("@")[-1].lower()
    if domain in SKIP_DOMAINS:
        return email, None  # return email but skip domain-based matching
    return email, domain


def get_rich_text_value(prop: dict) -> str:
    """Extract plain text from a Notion rich_text property."""
    parts = prop.get("rich_text", [])
    return "".join(p.get("plain_text", "") for p in parts).strip()


def get_title_value(prop: dict) -> str:
    """Extract plain text from a Notion title property."""
    parts = prop.get("title", [])
    return "".join(p.get("plain_text", "") for p in parts).strip()


def extract_cs_owner(props: dict) -> str:
    """Extract CS Owner select value from a Notion page properties dict."""
    owner_prop = props.get("⭐ CS Owner", {})
    if owner_prop.get("select"):
        return owner_prop["select"].get("name", "")
    return ""


# ═════════════════════════════════════════════════════════════════════════════
# Step 1 — Fetch all Issues with empty Customer relation
# ═════════════════════════════════════════════════════════════════════════════

def fetch_issues_without_customer() -> list[dict]:
    """
    Query the Issues Table for pages with no Customer relation.
    Paginates automatically.
    """
    print("Fetching issues with empty Customer relation...")
    url = f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query"
    payload = {
        "filter": {
            "property": "Customer",
            "relation": {"is_empty": True},
        },
        "page_size": 100,
    }

    issues = []
    cursor = None
    page_num = 0

    while True:
        page_num += 1
        if cursor:
            payload["start_cursor"] = cursor

        resp = requests.post(url, headers=notion_v2022, json=payload)
        if resp.status_code != 200:
            print(f"  ERROR fetching issues (page {page_num}): {resp.status_code} — {resp.text[:300]}")
            sys.exit(1)

        data = resp.json()
        results = data.get("results", [])
        print(f"  Page {page_num}: {len(results)} issues fetched")
        issues.extend(results)

        if data.get("has_more") and data.get("next_cursor"):
            cursor = data["next_cursor"]
        else:
            break

    print(f"  Total issues with empty Customer: {len(issues)}\n")
    return issues


def parse_issue(page: dict) -> dict:
    """Extract the fields we care about from a raw Notion page object."""
    props = page.get("properties", {})

    reported_by = get_rich_text_value(props.get("Reported By", {}))
    source_id   = get_rich_text_value(props.get("Source ID", {}))
    title       = get_title_value(props.get("Issue Title", {}))
    source_name = (props.get("Source", {}).get("select") or {}).get("name", "")

    email, domain = extract_email_and_domain(reported_by)

    return {
        "page_id":     page["id"],
        "title":       title,
        "reported_by": reported_by,
        "source":      source_name,
        "source_id":   source_id,
        "email":       email,
        "domain":      domain,
    }


# ═════════════════════════════════════════════════════════════════════════════
# Step 2a — Strategy 1: Intercom Company ID lookup
# ═════════════════════════════════════════════════════════════════════════════

_intercom_conv_cache: dict[str, Optional[str]] = {}  # source_id → intercom_company_id

def get_intercom_company_id(source_id: str) -> Optional[str]:
    """
    Fetch the Intercom conversation, return the first company ID or None.
    Results are cached to avoid repeated API calls.
    """
    if source_id in _intercom_conv_cache:
        return _intercom_conv_cache[source_id]

    resp = requests.get(
        f"https://api.intercom.io/conversations/{source_id}",
        headers=intercom_headers,
    )
    if resp.status_code != 200:
        _intercom_conv_cache[source_id] = None
        return None

    conv = resp.json()
    companies = conv.get("companies", {}).get("companies", [])
    company_id = companies[0].get("id") if companies else None
    _intercom_conv_cache[source_id] = company_id
    return company_id


_intercom_id_cache: dict[str, tuple] = {}  # intercom_company_id → (page_id, cs_owner)

def find_customer_by_intercom_id(intercom_id: str) -> tuple:
    """
    Query Master Customer Table by "🔗 Intercom Company ID" contains intercom_id.
    Returns (customer_page_id, cs_owner) or (None, None).
    Results are cached.
    """
    if intercom_id in _intercom_id_cache:
        return _intercom_id_cache[intercom_id]

    resp = requests.post(
        f"https://api.notion.com/v1/data_sources/{DS_ID}/query",
        headers=notion_v2025,
        json={
            "filter": {
                "property": "🔗 Intercom Company ID",
                "rich_text": {"contains": intercom_id},
            }
        },
    )
    if resp.status_code != 200:
        print(f"    WARNING: intercom_id lookup failed for {intercom_id}: {resp.status_code} — {resp.text[:200]}")
        _intercom_id_cache[intercom_id] = (None, None)
        return None, None

    results = resp.json().get("results", [])
    if not results:
        _intercom_id_cache[intercom_id] = (None, None)
        return None, None

    page_id  = results[0]["id"]
    cs_owner = extract_cs_owner(results[0].get("properties", {}))
    _intercom_id_cache[intercom_id] = (page_id, cs_owner)
    return page_id, cs_owner


# ═════════════════════════════════════════════════════════════════════════════
# Step 2b — Strategy 2: Main Contact Email exact match
# ═════════════════════════════════════════════════════════════════════════════

_email_cache: dict[str, tuple] = {}  # email → (page_id, cs_owner)

def find_customer_by_email(email: str) -> tuple:
    """
    Query Master Customer Table by "👤 Main Contact Email" contains email.
    Returns (customer_page_id, cs_owner) or (None, None).
    Results are cached.
    """
    if email in _email_cache:
        return _email_cache[email]

    resp = requests.post(
        f"https://api.notion.com/v1/data_sources/{DS_ID}/query",
        headers=notion_v2025,
        json={
            "filter": {
                "property": "👤 Main Contact Email",
                "email": {"equals": email},
            }
        },
    )
    if resp.status_code != 200:
        print(f"    WARNING: email lookup failed for {email}: {resp.status_code} — {resp.text[:200]}")
        _email_cache[email] = (None, None)
        return None, None

    results = resp.json().get("results", [])
    if not results:
        _email_cache[email] = (None, None)
        return None, None

    page_id  = results[0]["id"]
    cs_owner = extract_cs_owner(results[0].get("properties", {}))
    _email_cache[email] = (page_id, cs_owner)
    return page_id, cs_owner


# ═════════════════════════════════════════════════════════════════════════════
# Step 2c — Strategy 3: Domain match (original approach)
# ═════════════════════════════════════════════════════════════════════════════

_domain_cache: dict[str, tuple] = {}  # domain → (page_id, cs_owner)

def find_customer_by_domain(domain: str) -> tuple:
    """
    Query Master Customer Table for a page whose Domain contains `domain`.
    Returns (customer_page_id, cs_owner) or (None, None).
    Results are cached.
    """
    if domain in _domain_cache:
        return _domain_cache[domain]

    resp = requests.post(
        f"https://api.notion.com/v1/data_sources/{DS_ID}/query",
        headers=notion_v2025,
        json={
            "filter": {
                "property": "👤 Main Contact Email",
                "email": {"contains": "@" + domain},
            }
        },
    )
    if resp.status_code != 200:
        print(f"    WARNING: domain lookup failed for {domain}: {resp.status_code} — {resp.text[:200]}")
        _domain_cache[domain] = (None, None)
        return None, None

    results = resp.json().get("results", [])
    if not results:
        _domain_cache[domain] = (None, None)
        return None, None

    page_id  = results[0]["id"]
    cs_owner = extract_cs_owner(results[0].get("properties", {}))
    _domain_cache[domain] = (page_id, cs_owner)
    return page_id, cs_owner


# ═════════════════════════════════════════════════════════════════════════════
# Step 2 — Waterfall: try all 3 strategies in order
# ═════════════════════════════════════════════════════════════════════════════

def match_customer(issue: dict) -> tuple:
    """
    Try to find a matching MCT customer using 3 strategies in order.
    Returns (customer_page_id, cs_owner, matched_by) where matched_by is
    one of "intercom_id", "email", "domain", or None.
    """
    # ── Strategy 1: Intercom Company ID ───────────────────────────────────────
    if issue["source"] == "Intercom" and issue["source_id"]:
        intercom_company_id = get_intercom_company_id(issue["source_id"])
        time.sleep(0.1)  # rate-limit courtesy for Intercom API
        if intercom_company_id:
            page_id, cs_owner = find_customer_by_intercom_id(intercom_company_id)
            time.sleep(0.15)
            if page_id:
                return page_id, cs_owner, "intercom_id"

    # ── Strategy 2: Email exact match ─────────────────────────────────────────
    if issue["email"]:
        page_id, cs_owner = find_customer_by_email(issue["email"])
        time.sleep(0.15)
        if page_id:
            return page_id, cs_owner, "email"

    # ── Strategy 3: Domain match ──────────────────────────────────────────────
    if issue["domain"]:
        page_id, cs_owner = find_customer_by_domain(issue["domain"])
        time.sleep(0.15)
        if page_id:
            return page_id, cs_owner, "domain"

    return None, None, None


# ═════════════════════════════════════════════════════════════════════════════
# Step 3 — PATCH the issue with the Customer relation
# ═════════════════════════════════════════════════════════════════════════════

def patch_issue_customer(page_id: str, customer_page_id: str) -> bool:
    """Set the Customer relation on the given Issues page. Returns True on success."""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            "Customer": {
                "relation": [{"id": customer_page_id}]
            }
        }
    }

    resp = requests.patch(url, headers=notion_v2022, json=payload)
    if resp.status_code == 200:
        return True
    print(f"    PATCH failed for page {page_id}: {resp.status_code} — {resp.text[:200]}")
    return False


# ═════════════════════════════════════════════════════════════════════════════
# Step 4 — Verification: check today's recent issues have Customer set
# ═════════════════════════════════════════════════════════════════════════════

def verify_recent_issues():
    """
    Quick sanity check: fetch issues created in the last 24h and show
    how many already have a Customer linked (workflow health check).
    """
    print("\n=== VERIFICATION: Issues from last 24h ===")
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

    url = f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query"
    payload = {
        "filter": {
            "property": "Created At",
            "date": {"on_or_after": cutoff},
        },
        "page_size": 50,
        "sorts": [{"property": "Created At", "direction": "descending"}],
    }

    resp = requests.post(url, headers=notion_v2022, json=payload)
    if resp.status_code != 200:
        print(f"  WARNING: could not fetch recent issues: {resp.status_code}")
        return

    results = resp.json().get("results", [])
    print(f"  Issues created in last 24h: {len(results)}")

    with_customer    = 0
    without_customer = 0

    for page in results:
        props = page.get("properties", {})
        customer_rel = props.get("Customer", {}).get("relation", [])
        title = get_title_value(props.get("Issue Title", {}))
        has_customer = len(customer_rel) > 0
        if has_customer:
            with_customer += 1
        else:
            without_customer += 1
            print(f"    ✗ No customer: {title or page['id']}")

    print(f"  ✓ With customer:    {with_customer}")
    print(f"  ✗ Without customer: {without_customer}")

    if without_customer == 0:
        print("  Workflow is correctly linking all recent issues.")
    else:
        print("  WARNING: some recent issues are missing Customer — check the Intercom pipeline.")


# ═════════════════════════════════════════════════════════════════════════════
# Main backfill loop
# ═════════════════════════════════════════════════════════════════════════════

def main():
    mode_label = "DRY RUN (pass --apply to actually patch)" if DRY_RUN else "LIVE — will PATCH Notion"
    print(f"=== CS OWNER BACKFILL === ({mode_label})\n")

    # ── 1. Fetch all issues with empty Customer ────────────────────────────────
    raw_issues = fetch_issues_without_customer()
    issues     = [parse_issue(p) for p in raw_issues]

    total = len(issues)

    # ── 2. Process each issue ─────────────────────────────────────────────────
    matched_patched: list[dict] = []
    no_match:        list[dict] = []   # all 3 strategies failed
    no_email:        list[dict] = []   # no email in Reported By at all

    owner_counts:    dict[str, int] = {}
    strategy_counts: dict[str, int] = {}

    for issue in issues:
        # Skip entirely if no email at all (can't try any strategy)
        if not issue["email"] and not (issue["source"] == "Intercom" and issue["source_id"]):
            no_email.append(issue)
            continue

        customer_page_id, cs_owner, matched_by = match_customer(issue)

        if customer_page_id is None:
            no_match.append(issue)
            continue

        # We have a match — patch (unless dry-run)
        if not DRY_RUN:
            success = patch_issue_customer(issue["page_id"], customer_page_id)
            time.sleep(0.2)
            if not success:
                no_match.append({**issue, "patch_error": True})
                continue

        matched_patched.append({**issue, "cs_owner": cs_owner, "matched_by": matched_by})
        owner_counts[cs_owner]       = owner_counts.get(cs_owner, 0) + 1
        strategy_counts[matched_by]  = strategy_counts.get(matched_by, 0) + 1

    # ── 3. Print summary ──────────────────────────────────────────────────────
    print("\n" + "═" * 65)
    print("=== CS OWNER BACKFILL — SUMMARY ===")
    print("═" * 65)
    print(f"Total Issues with empty Customer:  {total}")
    print()

    owner_breakdown    = ", ".join(f"{k}: {v}" for k, v in sorted(owner_counts.items()))
    strategy_breakdown = ", ".join(f"{k}: {v}" for k, v in sorted(strategy_counts.items()))
    action = "patched" if not DRY_RUN else "would be patched"
    print(f"  ✓ Matched + {action}:           {len(matched_patched)}"
          + (f"  ({owner_breakdown})" if owner_breakdown else ""))
    if strategy_breakdown:
        print(f"      Match strategies used: {strategy_breakdown}")
    print(f"  ✗ No email/source in Reported By:  {len(no_email)}")
    print(f"  ✗ All strategies failed:           {len(no_match)}")
    print()

    if no_email:
        print("─── No email / skipped ───")
        for i in no_email:
            print(f"  [{i['source_id'] or i['page_id'][:8]}] {i['title'][:60]:60s}  | {i['reported_by'][:50]}")

    if no_match:
        print("\n─── Unmatched (need manual assignment or add company to MCT) ───")
        seen_domains: set[str] = set()
        for i in no_match:
            dom = i.get("domain") or "?"
            src = i.get("source_id") or i["page_id"][:8]
            print(f"  [{src}] domain={dom:30s} | {i['title'][:50]}")
            if dom != "?":
                seen_domains.add(dom)

        if seen_domains:
            print(f"\n  Unique unmatched domains: {sorted(seen_domains)}")
            print("\n  Add these companies to the Master Customer Table,")
            print("  then re-run with --apply to link their issues.")

    if matched_patched and DRY_RUN:
        print(f"\n─── Issues that WOULD be patched (first 30) ───")
        for i in matched_patched[:30]:
            dom = i.get("domain") or i.get("email", "")[:30]
            print(f"  ✓ [{i['matched_by']:12s}] owner={i.get('cs_owner','?'):6s}  domain/id={dom:30s} | {i['title'][:40]}")

    print()
    if DRY_RUN:
        print("DRY RUN complete — no changes made. Re-run with --apply to patch Notion.")
    else:
        print("Backfill complete.")

    # ── 4. Verify recent issues ───────────────────────────────────────────────
    verify_recent_issues()

    print("\n=== DONE ===")


if __name__ == "__main__":
    main()
