"""
find_phone_gaps.py
------------------
For every MCT company (193 total), find phone numbers in HubSpot that are
NOT currently in the MCT `DM - Phone Number` / `Oper - Phone Number` fields.

Lookup strategy:
  Tier 1 (~75 companies): MCT has DM/Oper email set → batch-read from HubSpot
  Tier 2 (~100 companies): MCT has a real domain, no emails → domain search
  Tier 3 (~18 companies):  Gmail/hotmail email set → EQ search fallback

Output: phone_gaps.json (never writes back to Notion)

Usage:
    python3 find_phone_gaps.py
"""

import re
import time
import json
import requests
import creds

# ── Credentials ──────────────────────────────────────────────────────────────
NOTION_TOKEN  = creds.get("NOTION_TOKEN")
HUBSPOT_TOKEN = creds.get("HUBSPOT_TOKEN")
DS_ID         = "3ceb1ad0-91f1-40db-945a-c51c58035898"

NOTION_HDR = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}
HS_HDR = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

HS_PROPS = ["email", "firstname", "lastname", "jobtitle", "phone", "mobilephone"]

GMAIL_DOMAINS = {"gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "live.com"}

# Job title signals for inferring DM vs Oper from domain search results
DM_SIGNALS = [
    "ceo", "founder", "co-founder", "cofounder", "owner", "director",
    "cto", "coo", "president", "managing", "gerente", "fundador",
    "propietari", "director general", "head of", "chief",
]
OPER_SIGNALS = [
    "ecommerce", "e-commerce", "operations", "marketing", "cx",
    "customer experience", "digital", "commercial", "tienda", "manager",
    "responsable", "coordinador",
]

OUTPUT_PATH = "/Users/edoardopelli/projects/Customer Success/phone_gaps.json"


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize_phone(phone):
    """Strip non-digits, return last 9 digits for comparison."""
    digits = re.sub(r"\D", "", str(phone))
    return digits[-9:] if len(digits) >= 9 else digits


def contact_phone(props):
    """Return whichever phone field has a value (phone preferred over mobile)."""
    return props.get("phone") or props.get("mobilephone") or ""


def contact_name(props):
    fn = props.get("firstname") or ""
    ln = props.get("lastname") or ""
    full = f"{fn} {ln}".strip()
    return full if full else (props.get("email") or "").split("@")[0]


def is_dm_title(title):
    t = (title or "").lower()
    return any(sig in t for sig in DM_SIGNALS)


def is_oper_title(title):
    t = (title or "").lower()
    return any(sig in t for sig in OPER_SIGNALS)


def is_gmail_domain(domain):
    return any(g in (domain or "") for g in GMAIL_DOMAINS)


# ── Notion ────────────────────────────────────────────────────────────────────

def fetch_all_mct():
    pages, cursor = [], None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/data_sources/{DS_ID}/query",
            headers=NOTION_HDR, json=body,
        )
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


def get_prop(page, name, ptype):
    prop = page.get("properties", {}).get(name, {})
    if ptype == "title":
        return "".join(x.get("plain_text", "") for x in prop.get("title", []))
    if ptype == "phone_number":
        return prop.get("phone_number") or ""
    if ptype == "rich_text":
        items = prop.get("rich_text", [])
        return items[0].get("plain_text", "") if items else ""
    if ptype == "email":
        return prop.get("email") or ""
    return ""


# ── HubSpot ───────────────────────────────────────────────────────────────────

def batch_read_by_email(emails):
    """
    Batch-read HubSpot contacts by email address.
    Returns dict: lowercase_email -> properties dict.
    Chunks into groups of 100 (API limit).
    """
    if not emails:
        return {}
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/read"
    result = {}
    emails = list(emails)
    for i in range(0, len(emails), 100):
        chunk = emails[i : i + 100]
        body = {
            "inputs": [{"id": e} for e in chunk],
            "idProperty": "email",
            "properties": HS_PROPS,
        }
        r = requests.post(url, headers=HS_HDR, json=body)
        if r.ok:
            for c in r.json().get("results", []):
                props = c.get("properties", {})
                em = (props.get("email") or "").lower()
                if em:
                    result[em] = props
        time.sleep(0.2)
    return result


def search_by_domain(domain):
    """Search HubSpot for all contacts whose email contains @domain."""
    domain = domain.replace("www.", "").strip().lower()
    if not domain:
        return []
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": "email", "operator": "CONTAINS_TOKEN", "value": f"@{domain}"}
        ]}],
        "properties": HS_PROPS,
        "limit": 50,
    }
    r = requests.post(url, headers=HS_HDR, json=body)
    if not r.ok:
        return []
    return r.json().get("results", [])


def search_by_exact_email(email):
    """Exact-match email search (for gmail/hotmail fallback)."""
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": "email", "operator": "EQ", "value": email}
        ]}],
        "properties": HS_PROPS,
        "limit": 5,
    }
    r = requests.post(url, headers=HS_HDR, json=body)
    if not r.ok:
        return []
    return r.json().get("results", [])


# ── Gap detection ─────────────────────────────────────────────────────────────

def make_gap(company, page_id, role, hs_props, mct_phone, tier):
    """
    Compare HubSpot phone vs MCT phone and return a gap dict, or None if no gap.
    """
    hs_phone = contact_phone(hs_props)
    if not hs_phone:
        return None  # HubSpot has no phone → nothing to offer

    hs_norm  = normalize_phone(hs_phone)
    mct_norm = normalize_phone(mct_phone) if mct_phone else ""

    if mct_norm and hs_norm == mct_norm:
        return None  # Already in sync

    return {
        "company":     company,
        "page_id":     page_id,
        "role":        role,
        "hs_name":     contact_name(hs_props),
        "hs_email":    hs_props.get("email") or "",
        "hs_title":    hs_props.get("jobtitle") or "",
        "hs_phone":    hs_phone,
        "mct_phone":   mct_phone,
        "status":      "MISSING" if not mct_phone else "DIFFERENT",
        "lookup_tier": tier,
    }


def best_dm(contacts):
    """Best DM candidate from domain-search results (Tier 2)."""
    with_title = [c for c in contacts if is_dm_title(c["properties"].get("jobtitle") or "")]
    pool = with_title if with_title else contacts
    with_phone = [c for c in pool if contact_phone(c["properties"])]
    return (with_phone or pool)[0] if pool else None


def best_oper(contacts, exclude_email=None):
    """Best Oper candidate from domain-search results, excluding the DM."""
    filtered = [c for c in contacts
                if c["properties"].get("email") != exclude_email]
    with_title = [c for c in filtered if is_oper_title(c["properties"].get("jobtitle") or "")]
    pool = with_title if with_title else filtered
    with_phone = [c for c in pool if contact_phone(c["properties"])]
    return (with_phone or pool)[0] if pool else None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Fetching all MCT pages...")
    mct_pages = fetch_all_mct()
    print(f"  {len(mct_pages)} pages loaded\n")

    # Parse every MCT row
    companies = []
    for p in mct_pages:
        domain_raw = get_prop(p, "🏢 Domain", "rich_text")
        domain = domain_raw.split(",")[0].strip().lower() if domain_raw else ""
        companies.append({
            "page_id":  p["id"],
            "company":  get_prop(p, "🏢 Company Name", "title"),
            "domain":   domain,
            "dm_email": get_prop(p, "DM - Point of contact", "email").strip().lower(),
            "op_email": get_prop(p, "Oper - Point of contact", "email").strip().lower(),
            "dm_phone": get_prop(p, "DM - Phone Number", "phone_number"),
            "op_phone": get_prop(p, "Oper - Phone Number", "phone_number"),
        })

    # ── Tier 1 — batch-read all known DM/Oper emails from HubSpot ────────────
    tier1_emails = set()
    for co in companies:
        if co["dm_email"]:
            tier1_emails.add(co["dm_email"])
        if co["op_email"]:
            tier1_emails.add(co["op_email"])

    print(f"Tier 1: batch-reading {len(tier1_emails)} unique emails from HubSpot...")
    hs_by_email = batch_read_by_email(tier1_emails)
    print(f"  {len(hs_by_email)} contacts found in HubSpot\n")

    gaps = []
    counts = {"tier1": 0, "tier2": 0, "tier3": 0, "skipped": 0}

    for i, co in enumerate(companies):
        company  = co["company"]
        page_id  = co["page_id"]
        domain   = co["domain"]
        dm_email = co["dm_email"]
        op_email = co["op_email"]
        dm_phone = co["dm_phone"]
        op_phone = co["op_phone"]

        if dm_email or op_email:
            # ── Tier 1: emails known, use batch-read results ──────────────────
            # Tier 3 fallback for gmail/hotmail not found in batch read
            def resolve_contact(email):
                if not email:
                    return None
                if email in hs_by_email:
                    return hs_by_email[email]
                if is_gmail_domain(email.split("@")[-1] if "@" in email else ""):
                    results = search_by_exact_email(email)
                    time.sleep(0.2)
                    if results:
                        props = results[0]["properties"]
                        hs_by_email[email] = props  # cache for dedup
                        return props
                return None

            dm_props = resolve_contact(dm_email)
            op_props = resolve_contact(op_email) if op_email != dm_email else dm_props

            if dm_email and dm_props:
                tier = 3 if is_gmail_domain(dm_email.split("@")[-1] if "@" in dm_email else "") else 1
                g = make_gap(company, page_id, "DM", dm_props, dm_phone, tier)
                if g:
                    gaps.append(g)

            if op_email and op_props:
                tier = 3 if is_gmail_domain(op_email.split("@")[-1] if "@" in op_email else "") else 1
                # Only add Oper gap if either email differs from DM or phone slot differs
                if op_email != dm_email or op_phone != dm_phone:
                    g = make_gap(company, page_id, "Oper", op_props, op_phone, tier)
                    if g:
                        gaps.append(g)

            counts["tier1"] += 1

        elif domain and not is_gmail_domain(domain):
            # ── Tier 2: no emails set, search by domain ───────────────────────
            counts["tier2"] += 1
            contacts = search_by_domain(domain)
            time.sleep(0.2)

            if contacts:
                dm_c  = best_dm(contacts)
                dm_em = dm_c["properties"].get("email") if dm_c else None
                op_c  = best_oper(contacts, exclude_email=dm_em)

                if dm_c:
                    g = make_gap(company, page_id, "DM", dm_c["properties"], dm_phone, 2)
                    if g:
                        gaps.append(g)

                if op_c and op_c is not dm_c:
                    g = make_gap(company, page_id, "Oper", op_c["properties"], op_phone, 2)
                    if g:
                        gaps.append(g)

        else:
            # No email, no searchable domain — nothing we can do
            counts["skipped"] += 1

        if (i + 1) % 25 == 0:
            print(f"  [{i+1}/{len(companies)}] gaps so far: {len(gaps)}")

    # ── Write JSON output ─────────────────────────────────────────────────────
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(gaps, f, indent=2, ensure_ascii=False)
    print(f"\nWrote {len(gaps)} gaps → {OUTPUT_PATH}")

    # ── Human-readable summary ────────────────────────────────────────────────
    missing   = [g for g in gaps if g["status"] == "MISSING"]
    different = [g for g in gaps if g["status"] == "DIFFERENT"]

    print(f"\n{'='*80}")
    print("PHONE GAPS SUMMARY")
    print(f"{'='*80}")
    print(f"Tier 1 (email known):   {counts['tier1']} companies")
    print(f"Tier 2 (domain search): {counts['tier2']} companies")
    print(f"Skipped (no data):      {counts['skipped']} companies")
    print()
    print(f"MISSING:   {len(missing)}  (MCT field empty, HubSpot has a phone)")
    print(f"DIFFERENT: {len(different)}  (MCT has phone but HubSpot has different)")
    print(f"TOTAL GAPS:{len(gaps)}")
    print()

    # Group by company for readable output
    by_company: dict = {}
    for g in gaps:
        by_company.setdefault(g["company"], []).append(g)

    hdr = f"{'Company':<36} {'Role':<5} {'Status':<10} {'T':<2} {'HS Name':<22} {'HS Phone':<15} HS Title"
    print(hdr)
    print("-" * 115)
    for company in sorted(by_company):
        for g in by_company[company]:
            print(
                f"{company[:35]:<36} {g['role']:<5} {g['status']:<10} "
                f"{g['lookup_tier']:<2} {g['hs_name'][:21]:<22} "
                f"{g['hs_phone']:<15} {g['hs_title'][:35]}"
            )

    print(f"\n{len(gaps)} total gaps across {len(by_company)} companies.")


if __name__ == "__main__":
    main()
