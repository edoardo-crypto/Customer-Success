#!/usr/bin/env python3
"""
fix_maagencia_customer_link.py — Link "ma agencia" issues to Aguas do Soprano

Two-step fix:
  1. Add maagencia.com to the 🏢 Domain field on the Aguas do Soprano MCT row
     so future Intercom Catch-up Poller runs auto-link new issues.
  2. Patch all existing Issues Table records whose "Reported By" contains
     "maagencia.com" and whose Customer relation is empty → set Customer
     to Aguas do Soprano.

Usage:
  python3 fix_maagencia_customer_link.py          # dry-run (no writes)
  python3 fix_maagencia_customer_link.py --apply  # actually patch
"""

import sys
import time
import requests
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")

# ── IDs ───────────────────────────────────────────────────────────────────────
ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"
MCT_DB_ID    = "84feda19cfaf4c6e9500bf21d2aaafef"
DS_ID        = "3ceb1ad0-91f1-40db-945a-c51c58035898"

MAAGENCIA_DOMAIN = "maagencia.com"

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

DRY_RUN = "--apply" not in sys.argv


# ── Step 1: Find Aguas do Soprano in MCT ──────────────────────────────────────
def find_aguas_do_soprano():
    print("Step 1 — Searching MCT for 'Aguas do Soprano'...")
    url = f"https://api.notion.com/v1/data_sources/{DS_ID}/query"
    payload = {
        "filter": {
            "property": "🏢 Company Name",
            "title": {"contains": "aguas"}
        }
    }
    r = requests.post(url, headers=notion_v2025, json=payload)
    r.raise_for_status()
    results = r.json().get("results", [])

    if not results:
        print("  ERROR: No MCT row found matching 'aguas'. Aborting.")
        sys.exit(1)

    page = results[0]
    page_id = page["id"]
    props = page.get("properties", {})

    # Extract current domain value
    domain_prop = props.get("🏢 Domain", {})
    rich_text_items = domain_prop.get("rich_text", [])
    current_domain = "".join(t.get("plain_text", "") for t in rich_text_items)

    # Extract company name for confirmation
    name_prop = props.get("🏢 Company Name", {})
    name_items = name_prop.get("title", [])
    company_name = "".join(t.get("plain_text", "") for t in name_items)

    print(f"  Found: '{company_name}' (page_id={page_id})")
    print(f"  Current 🏢 Domain value: '{current_domain}'")
    return page_id, current_domain


# ── Step 2: Add maagencia.com domain to MCT row ───────────────────────────────
def update_mct_domain(page_id: str, current_domain: str):
    print(f"\nStep 2 — Updating 🏢 Domain on MCT page...")

    if MAAGENCIA_DOMAIN in current_domain:
        print(f"  '{MAAGENCIA_DOMAIN}' already present in domain field — skipping.")
        return

    # Append with comma separator if there's already a value
    if current_domain.strip():
        new_domain = current_domain.rstrip(", ") + f", {MAAGENCIA_DOMAIN}"
    else:
        new_domain = MAAGENCIA_DOMAIN

    print(f"  New domain value will be: '{new_domain}'")

    if DRY_RUN:
        print("  [DRY-RUN] Would PATCH MCT page domain — skipped.")
        return

    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            "🏢 Domain": {
                "rich_text": [{"type": "text", "text": {"content": new_domain}}]
            }
        }
    }
    r = requests.patch(url, headers=notion_v2025, json=payload)
    r.raise_for_status()
    print(f"  Domain updated successfully.")


# ── Step 3: Find unlinked ma agencia issues ───────────────────────────────────
def find_unlinked_issues():
    print(f"\nStep 3 — Querying Issues Table for unlinked '{MAAGENCIA_DOMAIN}' issues...")
    url = f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query"
    payload = {
        "filter": {
            "and": [
                {
                    "property": "Reported By",
                    "rich_text": {"contains": MAAGENCIA_DOMAIN}
                },
                {
                    "property": "Customer",
                    "relation": {"is_empty": True}
                }
            ]
        }
    }

    issues = []
    cursor = None
    while True:
        if cursor:
            payload["start_cursor"] = cursor
        r = requests.post(url, headers=notion_v2022, json=payload)
        r.raise_for_status()
        data = r.json()
        issues.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    print(f"  Found {len(issues)} unlinked issue(s).")
    return issues


# ── Step 4: Patch each issue with Customer relation ───────────────────────────
def link_issues(issues: list, aguas_page_id: str):
    print(f"\nStep 4 — Linking issues to Aguas do Soprano...")

    if not issues:
        print("  Nothing to patch.")
        return

    for issue in issues:
        issue_id = issue["id"]
        props = issue.get("properties", {})

        # Extract title for display
        title_items = props.get("Issue Title", {}).get("title", [])
        title = "".join(t.get("plain_text", "") for t in title_items) or "(no title)"

        reported_by_items = props.get("Reported By", {}).get("rich_text", [])
        reported_by = "".join(t.get("plain_text", "") for t in reported_by_items)

        print(f"  Issue: '{title}' (reported_by={reported_by})")

        if DRY_RUN:
            print(f"    [DRY-RUN] Would link → Aguas do Soprano")
            continue

        url = f"https://api.notion.com/v1/pages/{issue_id}"
        payload = {
            "properties": {
                "Customer": {
                    "relation": [{"id": aguas_page_id}]
                }
            }
        }
        r = requests.patch(url, headers=notion_v2022, json=payload)
        r.raise_for_status()
        print(f"    Linked → Aguas do Soprano")
        time.sleep(0.3)  # gentle rate-limit buffer


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    mode = "DRY-RUN" if DRY_RUN else "APPLY"
    print(f"=== fix_maagencia_customer_link.py [{mode}] ===\n")

    aguas_page_id, current_domain = find_aguas_do_soprano()
    update_mct_domain(aguas_page_id, current_domain)
    issues = find_unlinked_issues()
    link_issues(issues, aguas_page_id)

    print("\nDone.")
    if DRY_RUN:
        print("Re-run with --apply to actually make changes.")


if __name__ == "__main__":
    main()
