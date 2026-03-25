#!/usr/bin/env python3
"""
audit_fix_unmatched_issues.py — Find and fix Notion Issues with no Customer relation

Usage:
  python3 audit_fix_unmatched_issues.py           # dry-run (default, safe)
  python3 audit_fix_unmatched_issues.py --apply   # commit fixes to Notion

Root cause being addressed:
  The pipeline matches Intercom conversations to MCT using only email domain.
  Many contacts write from personal emails (gmail/yahoo), so domain lookup is skipped.
  But Intercom associates contacts with a *company* even when no work email is present.
  This company name was previously ignored — now we use it as a fallback.

Steps:
  A. Find all Notion Issues with empty Customer relation
  B. Fetch Intercom data (email, company name) for each
  C. Build MCT lookup: domain dict + company-name dict (all rows, in-memory)
  D. Match each issue: domain first → company name fallback → unresolved
  E. Print report (always)
  F. PATCH Customer relation on matched issues (--apply only)
"""

import sys
import time
import requests
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
INTERCOM_TOKEN = creds.get("INTERCOM_TOKEN")
NOTION_TOKEN   = creds.get("NOTION_TOKEN")

NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"
NOTION_DS_ID     = "3ceb1ad0-91f1-40db-945a-c51c58035898"  # MCT data source

GENERIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "icloud.com", "protonmail.com", "live.com", "me.com",
}

STRIPE_TOKEN = creds.get("STRIPE_KEY")

ANTHROPIC_KEY = creds.get("ANTHROPIC_API_KEY")

intercom_headers = {
    "Authorization": f"Bearer {INTERCOM_TOKEN}",
    "Accept": "application/json",
}
notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",   # Issues table is NOT multi-source
    "Content-Type": "application/json",
}
notion_headers_mct = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",   # MCT is multi-source → requires 2025-09-03
    "Content-Type": "application/json",
}


# ── Step A: Find unmatched Issues ─────────────────────────────────────────────

def get_unmatched_issues():
    """Query Notion Issues table for pages whose Customer relation is empty."""
    print("Step A: Querying Notion Issues for empty Customer relation...")
    issues = []
    cursor = None

    while True:
        body = {
            "filter": {
                "property": "Customer",
                "relation": {"is_empty": True},
            },
            "page_size": 100,
        }
        if cursor:
            body["start_cursor"] = cursor

        r = requests.post(
            f"https://api.notion.com/v1/databases/{NOTION_ISSUES_DB}/query",
            headers=notion_headers,
            json=body,
        )
        r.raise_for_status()
        data = r.json()

        for page_obj in data.get("results", []):
            page_id = page_obj["id"]
            props   = page_obj.get("properties", {})

            # Issue title
            title_items = props.get("Issue Title", {}).get("title", [])
            title = "".join(t.get("plain_text", "") for t in title_items)

            # Source ID = Intercom conversation ID
            source_id_texts = props.get("Source ID", {}).get("rich_text", [])
            source_id = "".join(t.get("plain_text", "") for t in source_id_texts)

            # Source type (we only process Intercom)
            source_select = props.get("Source", {}).get("select") or {}
            source = source_select.get("name", "")

            issues.append({
                "page_id":   page_id,
                "title":     title,
                "source_id": source_id,
                "source":    source,
            })

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
        time.sleep(0.3)

    print(f"  Found {len(issues)} unmatched issues")
    return issues


# ── Step B: Fetch Intercom data ───────────────────────────────────────────────

def fetch_intercom_data(source_id):
    """
    Fetch full Intercom conversation and return (user_email, user_name, company_name).
    Returns (None, None, None) if conversation is unavailable.
    """
    try:
        r = requests.get(
            f"https://api.intercom.io/conversations/{source_id}?display_as=plaintext",
            headers=intercom_headers,
            timeout=15,
        )
        if r.status_code == 404:
            return None, None, None   # conversation deleted
        r.raise_for_status()
        conv = r.json()

        # Contact — search API may return a reference with no email
        contacts   = conv.get("contacts", {}).get("contacts", [])
        user_email = ""
        user_name  = ""
        if contacts:
            c          = contacts[0]
            user_email = c.get("email", "") or ""
            user_name  = c.get("name",  "") or ""
            # Follow the contact link if email is missing
            if not user_email:
                cid = c.get("id", "")
                if cid:
                    cr = requests.get(
                        f"https://api.intercom.io/contacts/{cid}",
                        headers=intercom_headers,
                        timeout=10,
                    )
                    if cr.status_code == 200:
                        cd         = cr.json()
                        user_email = cd.get("email", "") or ""
                        user_name  = user_name or (cd.get("name", "") or "")

        # Company — present even when contact uses a personal email
        company_name = ""
        companies_data = conv.get("companies", {})
        if isinstance(companies_data, dict):
            company_list = companies_data.get("companies", [])
            if company_list:
                company_name = company_list[0].get("name", "") or ""

        return user_email, user_name, company_name

    except Exception as e:
        print(f"    WARNING: Could not fetch Intercom conv {source_id}: {e}")
        return None, None, None


# ── Step C: Build MCT lookup ──────────────────────────────────────────────────

def build_mct_lookup():
    """
    Load all MCT pages and build two lookup dicts:
      domain_map:  { domain_str  → page_id }
      title_map:   { title_lower → page_id }
    """
    print("Step C: Building MCT lookup (domain + company name)...")
    domain_map = {}
    title_map  = {}
    cursor     = None
    total      = 0

    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        r = requests.post(
            f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
            headers=notion_headers_mct,
            json=body,
        )
        r.raise_for_status()
        data = r.json()

        for page_obj in data.get("results", []):
            page_id = page_obj["id"]
            props   = page_obj.get("properties", {})

            # Title — find whichever property has type "title"
            mct_title = ""
            for prop_val in props.values():
                if prop_val.get("type") == "title":
                    items     = prop_val.get("title", [])
                    mct_title = "".join(t.get("plain_text", "") for t in items).strip()
                    break

            # Domain
            domain_texts = props.get("🏢 Domain", {}).get("rich_text", [])
            domain_val   = "".join(t.get("plain_text", "") for t in domain_texts).strip().lower()

            if domain_val:
                domain_map[domain_val] = page_id
            if mct_title:
                title_map[mct_title.lower()] = page_id

            total += 1

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
        time.sleep(0.3)

    print(f"  Loaded {total} MCT rows — {len(domain_map)} with domain, {len(title_map)} with name")
    return domain_map, title_map


# ── Step D: Match each issue ──────────────────────────────────────────────────

def fetch_stripe_customer_name(user_email):
    """Look up customer name in Stripe by email. Returns name string or ''."""
    try:
        r = requests.get(
            "https://api.stripe.com/v1/customers",
            params={"email": user_email, "limit": 1},
            headers={"Authorization": f"Bearer {STRIPE_TOKEN}"},
            timeout=10,
        )
        if r.status_code != 200:
            return ""
        data = r.json().get("data", [])
        if not data:
            return ""
        cust = data[0]
        return cust.get("name", "") or cust.get("description", "") or ""
    except Exception as e:
        print(f"    WARNING: Stripe lookup failed for {user_email}: {e}")
        return ""


def fetch_company_via_claude(user_email, user_name):
    """Ask Claude (with web search) to identify the company for this person."""
    try:
        prompt = (
            f"What company does {user_name or user_email} work for? "
            f"Their email is {user_email}. "
            "Reply with ONLY the company name, nothing else. "
            "If you cannot find it, reply with exactly: UNKNOWN"
        )
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":        ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "anthropic-beta":    "web-search-2025-03-05",
                "Content-Type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 50,
                "tools": [{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}],
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        if r.status_code != 200:
            print(f"    WARNING: Claude web search returned {r.status_code} for {user_email}")
            return ""
        last_text = ""
        for block in (r.json().get("content") or []):
            if block.get("type") == "text":
                last_text = block.get("text", "").strip()
        return "" if not last_text or last_text.upper() == "UNKNOWN" else last_text
    except Exception as e:
        print(f"    WARNING: Claude web search failed for {user_email}: {e}")
        return ""


def match_issue(user_email, user_name, company_name, issue_title, domain_map, title_map):
    """
    Try in order:
      1. Domain match (skip generic personal-email domains)
      2. Company-name substring match (>= 4 chars, case-insensitive)
      3. Domain-prefix title match (strip TLD, compare against MCT titles with spaces removed)
      4. Issue title prefix match (split on ' - ', left side vs MCT titles)
      5. Stripe email lookup (only for generic emails)
      6. Claude web search (last resort — AI looks up person on LinkedIn/Google)
    Returns (match_type, page_id) or (None, None).
    """
    domain = ""
    is_generic = True
    if user_email and "@" in user_email:
        domain = user_email.split("@")[1].lower()
        is_generic = domain in GENERIC_DOMAINS

    # 1. Domain match
    if domain and not is_generic and domain in domain_map:
        return "domain", domain_map[domain]

    # 2. Company-name match
    if company_name and len(company_name) >= 4:
        company_lower = company_name.lower()
        for mct_title_lower, page_id in title_map.items():
            if company_lower in mct_title_lower or mct_title_lower in company_lower:
                return "company", page_id

    # 3. Domain-prefix title match
    # e.g. "grippadel.com" → "grippadel" → matches MCT "Grippadel" (spaces stripped)
    if domain and not is_generic:
        prefix = domain.split(".")[0].lower()
        if len(prefix) >= 4:
            for mct_title_lower, page_id in title_map.items():
                mct_nospace = mct_title_lower.replace(" ", "")
                if prefix in mct_nospace or mct_nospace in prefix:
                    return "domain_prefix", page_id

    # 4. Issue title prefix match
    # e.g. "La Valenciana Calzados - Flows stopping..." → "La Valenciana Calzados"
    if issue_title and " - " in issue_title:
        left = issue_title.split(" - ")[0].strip()
        if len(left) >= 4 and "://" not in left:
            left_lower = left.lower()

            # 4a. Full prefix substring match
            for mct_title_lower, page_id in title_map.items():
                if left_lower in mct_title_lower or mct_title_lower in left_lower:
                    return "title_prefix", page_id

            # 4b. Significant-word match: extract long words (>=7 chars) from the
            # prefix and check if any appears in an MCT title.
            # e.g. "La Valenciana Calzados" → "valenciana" → matches "La Valenciana 1950"
            sig_words = [w for w in left_lower.split() if len(w) >= 7]
            for word in sig_words:
                for mct_title_lower, page_id in title_map.items():
                    if word in mct_title_lower:
                        return "title_word", page_id

    # 5. Stripe email lookup (only for generic/personal email domains)
    if user_email and is_generic:
        stripe_name = fetch_stripe_customer_name(user_email)
        if stripe_name and len(stripe_name) >= 4:
            stripe_lower = stripe_name.lower()
            for mct_title_lower, page_id in title_map.items():
                if stripe_lower in mct_title_lower or mct_title_lower in stripe_lower:
                    return "stripe", page_id

    # 6. Claude web search (last resort — AI looks up person on LinkedIn/Google)
    if user_email:
        ai_company = fetch_company_via_claude(user_email, user_name)
        if ai_company and len(ai_company) >= 4:
            ai_lower = ai_company.lower()
            for mct_title_lower, page_id in title_map.items():
                if ai_lower in mct_title_lower or mct_title_lower in ai_lower:
                    return "ai_search", page_id

    return None, None


# ── Step F: Apply fix ─────────────────────────────────────────────────────────

def patch_customer_relation(issue_page_id, customer_page_id):
    """PATCH the Customer relation on an Issues table page."""
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{issue_page_id}",
        headers=notion_headers,
        json={
            "properties": {
                "Customer": {
                    "relation": [{"id": customer_page_id}]
                }
            }
        },
    )
    if r.status_code not in (200, 201):
        print(f"    ERROR PATCH {issue_page_id}: {r.status_code} — {r.text[:300]}")
        return False
    return True


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    apply = "--apply" in sys.argv
    mode  = "APPLY" if apply else "DRY-RUN"

    print("=" * 70)
    print(f"audit_fix_unmatched_issues.py  [{mode}]")
    print("=" * 70)

    # ── A. Get all unmatched issues ───────────────────────────────────────────
    issues = get_unmatched_issues()
    if not issues:
        print("\nNo unmatched issues found. Nothing to do.")
        return

    # ── B. Fetch Intercom data ────────────────────────────────────────────────
    print(f"\nStep B: Fetching Intercom data for {len(issues)} issues...")
    intercom_issues = [i for i in issues if i["source_id"]]
    no_source       = [i for i in issues if not i["source_id"]]

    print(f"  {len(intercom_issues)} have a Source ID (Intercom) — fetching...")
    print(f"  {len(no_source)} have no Source ID — will be unresolved")

    for issue in intercom_issues:
        sid = issue["source_id"]
        email, name, company = fetch_intercom_data(sid)
        issue["user_email"]   = email   or ""
        issue["user_name"]    = name    or ""
        issue["company_name"] = company or ""
        time.sleep(0.5)   # Intercom rate-limit protection

    for issue in no_source:
        issue["user_email"]   = ""
        issue["user_name"]    = ""
        issue["company_name"] = ""

    # ── C. Build MCT lookup ───────────────────────────────────────────────────
    print()
    domain_map, title_map = build_mct_lookup()

    # ── D. Match each issue ───────────────────────────────────────────────────
    print("\nStep D: Matching issues to MCT customers...")
    matched    = []
    unresolved = []

    for issue in issues:
        match_type, customer_page_id = match_issue(
            issue["user_email"], issue["user_name"], issue["company_name"], issue["title"],
            domain_map, title_map
        )
        if match_type:
            # Reverse-look up the MCT title for display
            mct_title = next(
                (k for k, v in title_map.items() if v == customer_page_id),
                customer_page_id[:8]
            )
            issue.update({
                "match_type":        match_type,
                "customer_page_id":  customer_page_id,
                "mct_title":         mct_title,
            })
            matched.append(issue)
        else:
            issue.update({"match_type": None, "customer_page_id": None})
            unresolved.append(issue)

    # ── E. Report ─────────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("REPORT")
    print("=" * 70)
    print(f"MATCHED (can fix):  {len(matched):3d} issues")
    print(f"UNRESOLVED:         {len(unresolved):3d} issues — no data to match on")

    if matched:
        print()
        print("Matched issues:")
        hdr = f"  {'Issue Title':<45}  {'Company':22}  {'Via':<8}  MCT Customer"
        print(hdr)
        print(f"  {'-'*45}  {'-'*22}  {'-'*8}  {'-'*30}")
        for iss in matched:
            title   = (iss["title"]        or "(no title)")[:44]
            company = (iss["company_name"] or "(no company)")[:21]
            mtype   = iss["match_type"]
            mct     = iss["mct_title"][:30]
            print(f"  {title:<45}  {company:<22}  {mtype:<8}  {mct}")

    if unresolved:
        print()
        print("Unresolved issues (cannot match):")
        for iss in unresolved:
            email   = iss.get("user_email",   "") or "(no email)"
            company = iss.get("company_name", "") or "(no company)"
            sid     = iss.get("source_id",    "") or "(no source ID)"
            title   = (iss["title"] or "(no title)")[:45]
            print(f"  • {title}  email={email}  company={company}  sid={sid}")

    # ── F. Apply ──────────────────────────────────────────────────────────────
    if not apply:
        print()
        print("Dry-run complete — no changes made.")
        print("Re-run with --apply to commit the Customer relation patches to Notion.")
        return

    if not matched:
        print("\nNothing to apply.")
        return

    print()
    print("=" * 70)
    print("APPLYING FIXES")
    print("=" * 70)

    ok   = 0
    fail = 0
    for iss in matched:
        label = (iss["title"] or "(no title)")[:45]
        cust  = iss["mct_title"][:30]
        print(f"  PATCH [{iss['match_type']}] {label!r}  →  {cust!r}")
        if patch_customer_relation(iss["page_id"], iss["customer_page_id"]):
            print("    OK")
            ok += 1
        else:
            fail += 1
        time.sleep(0.5)   # Notion rate-limit protection

    print()
    print(f"Result: {ok} patched, {fail} failed")
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
