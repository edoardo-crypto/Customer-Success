"""
fill_point_of_contact.py
------------------------
One-shot enrichment: fills the "Point of Contact" email field on every MCT
row that currently has it blank, using three strategies in priority order:

  1. hs-company   — HubSpot company search by domain → first associated contact
  2. hs-sequence  — Pre-loaded sequence enrollments → match by email domain
  3. all-contacts — Regex-extract first email from MCT "All Contacts" field

Rows that already have a value are left untouched.
Supports --dry-run: prints what would be written, makes no Notion PATCHes.

Usage:
    python3 fill_point_of_contact.py [--dry-run]
"""

import re
import sys
import time
import requests
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
NOTION_TOKEN  = creds.get("NOTION_TOKEN")
HUBSPOT_TOKEN = creds.get("HUBSPOT_TOKEN")

MCT_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

SEQUENCE_IDS = [
    "769600699",  # Alex — EN
    "769600701",  # Alex — ES
    "769600702",  # Aya — EN
    "769557714",  # Aya — ES
]

GENERIC_DOMAINS = {
    "gmail.com", "hotmail.com", "outlook.com", "yahoo.com", "yahoo.es",
    "icloud.com", "live.com", "msn.com", "me.com", "googlemail.com",
}

# ── API headers ───────────────────────────────────────────────────────────────
NOTION_HDR = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type":   "application/json",
}
HUBSPOT_HDR = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type":  "application/json",
}

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# ── Notion property helpers ───────────────────────────────────────────────────

def _prop(page, name):
    return page.get("properties", {}).get(name, {})

def prop_title(page, name):
    items = _prop(page, name).get("title", [])
    return items[0]["plain_text"] if items else ""

def prop_select(page, name):
    sel = _prop(page, name).get("select")
    return sel["name"] if sel else ""

def prop_rich_text(page, name):
    """Return first block only (used for single-value fields like Domain)."""
    items = _prop(page, name).get("rich_text", [])
    return items[0]["plain_text"] if items else ""

def prop_rich_text_full(page, name):
    """Concatenate ALL rich_text blocks (used for multi-line fields like All Contacts)."""
    items = _prop(page, name).get("rich_text", [])
    return "".join(b.get("plain_text", "") for b in items)

def prop_email_or_text(page, name):
    """Handle both email-type and rich_text-type properties."""
    prop = _prop(page, name)
    if "email" in prop:
        return prop["email"] or ""
    items = prop.get("rich_text", [])
    return items[0]["plain_text"] if items else ""

# ── Notion: fetch ALL MCT rows (paginated) ────────────────────────────────────

def fetch_all_mct():
    url     = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"
    payload = {"page_size": 100}
    rows, cursor = [], None
    while True:
        if cursor:
            payload["start_cursor"] = cursor
        resp = requests.post(url, headers=NOTION_HDR, json=payload)
        resp.raise_for_status()
        data = resp.json()
        rows.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
        time.sleep(0.3)
    return rows

def parse_mct_row(page):
    return {
        "page_id":      page["id"],
        "company":      prop_title(page,           "🏢 Company Name"),
        "domain":       prop_rich_text(page,       "🏢 Domain"),
        "poc":          prop_email_or_text(page,   "Point of contact"),
        "all_contacts": prop_rich_text_full(page,  "All contacts"),
    }

# ── Strategy 1: HubSpot company → contacts ────────────────────────────────────

def search_company_by_domain(domain):
    if not domain:
        return None
    clean = domain.replace("www.", "").strip().lower()
    url = "https://api.hubapi.com/crm/v3/objects/companies/search"
    payload = {
        "filterGroups": [{"filters": [
            {"propertyName": "domain", "operator": "EQ", "value": clean}
        ]}],
        "properties": ["domain", "name"],
        "limit": 1,
    }
    resp = requests.post(url, headers=HUBSPOT_HDR, json=payload)
    if not resp.ok:
        return None
    results = resp.json().get("results", [])
    return results[0] if results else None

def get_company_contact_ids(company_id):
    url = (f"https://api.hubapi.com/crm/v3/objects/companies"
           f"/{company_id}/associations/contacts")
    resp = requests.get(url, headers=HUBSPOT_HDR)
    if not resp.ok:
        return []
    return [r["id"] for r in resp.json().get("results", [])]

def get_contact_email(contact_id):
    url    = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    params = {"properties": "email,company"}
    resp   = requests.get(url, headers=HUBSPOT_HDR, params=params)
    if not resp.ok:
        return None, None
    props = resp.json().get("properties", {})
    return props.get("email") or "", props.get("company") or ""

def strategy_hs_company(domain):
    if not domain:
        return None
    hs_co = search_company_by_domain(domain)
    if not hs_co:
        return None
    contact_ids = get_company_contact_ids(hs_co["id"])
    if not contact_ids:
        return None
    email, _ = get_contact_email(contact_ids[0])
    return email or None

# ── Strategy 2: HubSpot sequence enrollments ──────────────────────────────────

def load_sequence_enrollments():
    """
    Returns a dict: email_domain -> email  (best-effort; generic domains excluded)
    and a list of all (email, hs_company_name) for fallback company-name matching.
    """
    domain_map   = {}   # email-domain → email
    all_contacts = []   # (email, hs_company_name)

    for seq_id in SEQUENCE_IDS:
        after = None
        while True:
            url    = f"https://api.hubapi.com/automation/v4/sequences/{seq_id}/enrollments"
            params = {"limit": 100}
            if after:
                params["after"] = after
            resp = requests.get(url, headers=HUBSPOT_HDR, params=params)
            if not resp.ok:
                print(f"  [WARN] sequences/{seq_id}/enrollments → {resp.status_code}")
                break
            data     = resp.json()
            contacts = data.get("results", [])

            # Batch-fetch emails for all contact IDs in this page
            contact_ids = [str(c["contactId"]) for c in contacts if c.get("contactId")]
            if contact_ids:
                emails_map = batch_fetch_emails(contact_ids)
                for cid, (email, company_name) in emails_map.items():
                    if email:
                        all_contacts.append((email, company_name))
                        dom = email.split("@")[-1].lower()
                        if dom not in GENERIC_DOMAINS:
                            domain_map.setdefault(dom, email)

            paging = data.get("paging", {}).get("next", {})
            after  = paging.get("after")
            if not after:
                break
            time.sleep(0.2)

    return domain_map, all_contacts

def batch_fetch_emails(contact_ids):
    """Returns {contact_id: (email, company_name)} via batch read."""
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/read"
    payload = {
        "inputs":     [{"id": cid} for cid in contact_ids],
        "properties": ["email", "company"],
    }
    resp = requests.post(url, headers=HUBSPOT_HDR, json=payload)
    if not resp.ok:
        return {}
    out = {}
    for obj in resp.json().get("results", []):
        props   = obj.get("properties", {})
        out[obj["id"]] = (props.get("email") or "", props.get("company") or "")
    return out

def strategy_hs_sequence(domain, company_name, domain_map, all_contacts):
    """
    Primary: match by email domain.
    Fallback: if email domain is generic, check if hs_company_name matches MCT name.
    """
    if not domain and not company_name:
        return None

    # Primary: direct domain match
    if domain:
        clean = domain.replace("www.", "").strip().lower()
        if clean in domain_map:
            return domain_map[clean]

    # Fallback: generic-domain contacts whose company name matches
    if company_name:
        cn_lower = company_name.lower()
        for email, hs_company in all_contacts:
            dom = email.split("@")[-1].lower()
            if dom in GENERIC_DOMAINS and hs_company:
                if hs_company.lower() in cn_lower or cn_lower in hs_company.lower():
                    return email

    return None

# ── Strategy 3: MCT "All Contacts" regex parse ───────────────────────────────

def strategy_all_contacts(all_contacts_text):
    emails = EMAIL_RE.findall(all_contacts_text)
    return emails[0] if emails else None

# ── Write back to Notion ──────────────────────────────────────────────────────

def patch_notion_poc(page_id, email):
    url = f"https://api.notion.com/v1/pages/{page_id}"

    # Try email type first
    body = {"properties": {"Point of contact": {"email": email}}}
    resp = requests.patch(url, headers=NOTION_HDR, json=body)
    if resp.ok:
        return True

    # Fallback: rich_text
    body = {"properties": {"Point of contact": {"rich_text": [{"text": {"content": email}}]}}}
    resp = requests.patch(url, headers=NOTION_HDR, json=body)
    if not resp.ok:
        print(f"  [ERROR] PATCH failed {resp.status_code}: {resp.text[:120]}")
        return False
    return True

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("DRY RUN — no Notion PATCHes will be made\n")

    # ── Pre-load sequence enrollments ─────────────────────────────────────────
    print("Loading HubSpot sequence enrollments…")
    domain_map, seq_all_contacts = load_sequence_enrollments()
    print(f"  → {len(domain_map)} unique email domains from {len(seq_all_contacts)} enrolled contacts\n")

    # ── Fetch MCT ─────────────────────────────────────────────────────────────
    print("Fetching all MCT rows from Notion…")
    pages = fetch_all_mct()
    rows  = [parse_mct_row(p) for p in pages]
    print(f"  → {len(rows)} rows total\n")

    # ── Counters ──────────────────────────────────────────────────────────────
    already_filled = 0
    filled = {"hs-company": [], "hs-sequence": [], "all-contacts": []}
    still_empty    = []

    # ── Table header ──────────────────────────────────────────────────────────
    print(f"{'Customer':<35} {'Strategy':<16} {'Email'}")
    print(f"{'-'*35} {'-'*16} {'-'*40}")

    for row in rows:
        company = row["company"] or "(no name)"
        poc     = row["poc"]

        # Already filled — skip
        if poc:
            already_filled += 1
            continue

        domain = row["domain"]
        email  = None
        strategy = None

        # Strategy 1: HubSpot company → contacts
        email = strategy_hs_company(domain)
        if email:
            strategy = "hs-company"
        time.sleep(0.15)

        # Strategy 2: HubSpot sequence enrollments
        if not email:
            email = strategy_hs_sequence(domain, company, domain_map, seq_all_contacts)
            if email:
                strategy = "hs-sequence"

        # Strategy 3: MCT "All Contacts" field
        if not email:
            email = strategy_all_contacts(row["all_contacts"])
            if email:
                strategy = "all-contacts"

        # Print row
        c_trunc = company[:34]
        if email:
            print(f"{c_trunc:<35} {strategy:<16} {email}")
            filled[strategy].append(company)
            if not dry_run:
                patch_notion_poc(row["page_id"], email)
                time.sleep(0.2)
        else:
            print(f"{c_trunc:<35} {'(none)':<16} —")
            still_empty.append(company)

    # ── Summary ───────────────────────────────────────────────────────────────
    total_filled = sum(len(v) for v in filled.values())
    print()
    print("── Summary ──────────────────────────────────────────────────────")
    print(f"  Already filled (skipped):  {already_filled}")
    for strat, names in filled.items():
        print(f"  Filled by {strat:<14}: {len(names)}")
    print(f"  Still empty:               {len(still_empty)}")

    if still_empty:
        print("\nStill empty:")
        for name in still_empty:
            print(f"  — {name}")

    if dry_run:
        print("\n[DRY RUN] No changes were made.")


if __name__ == "__main__":
    main()
