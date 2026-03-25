"""
find_dm_contacts.py
-------------------
For each Active/Churning MCT company where no DM email is set (no_dm_email=True),
search multiple sources to find the Decision Maker contact.

Sources tried in order:
  1. oper_promoted   — Oper's HubSpot jobtitle contains DM keywords → promote them
  2. hubspot_domain  — Search HubSpot contacts by @domain, pick best DM-titled match
  3. intercom        — Search Intercom contacts by @domain email pattern
  4. stripe          — Pull billing email from Stripe customer record

Output: dm_research_results.json + dry-run table printed to stdout

Usage:
    python3 find_dm_contacts.py
"""

import json
import re
import time
import requests
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
NOTION_TOKEN   = creds.get("NOTION_TOKEN")
DS_ID          = "3ceb1ad0-91f1-40db-945a-c51c58035898"
HUBSPOT_TOKEN  = creds.get("HUBSPOT_TOKEN")
INTERCOM_TOKEN = creds.get("INTERCOM_TOKEN")

OUTPUT_PATH = "/Users/edoardopelli/projects/Customer Success/dm_research_results.json"

NOTION_HDR = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}
HS_HDR = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}
INTERCOM_HDR = {
    "Authorization": f"Bearer {INTERCOM_TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

HS_PROPS = ["email", "firstname", "lastname", "jobtitle", "phone", "mobilephone"]

# DM signal keywords in job titles
DM_SIGNALS = [
    "ceo", "founder", "co-founder", "cofounder", "owner", "director",
    "cto", "coo", "president", "managing", "gerente", "fundador",
    "propietari", "director general", "head of", "chief",
]

# Generic/role emails that indicate LOW confidence
GENERIC_PREFIXES = {
    "info", "admin", "hola", "hello", "pedidos", "marketing", "press",
    "contact", "sales", "soporte", "support", "web", "almacen", "export",
    "data", "tienda", "shop", "orders", "ayuda", "contacto", "store",
}

FREE_DOMAINS = {"gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "live.com"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_free_domain(domain):
    return any(g in (domain or "").lower() for g in FREE_DOMAINS)


def is_generic_email(email):
    if not email or "@" not in email:
        return False
    prefix = email.split("@")[0].lower()
    return prefix in GENERIC_PREFIXES


def is_dm_title(title):
    t = (title or "").lower()
    return any(sig in t for sig in DM_SIGNALS)


def contact_name(props):
    fn = (props.get("firstname") or "").strip()
    ln = (props.get("lastname") or "").strip()
    full = f"{fn} {ln}".strip()
    return full if full else (props.get("email") or "").split("@")[0]


def load_stripe_key():
    """Parse Stripe secret key from Credentials.md."""
    try:
        text = open("Credentials.md").read()
        # Look for sk_live_... or sk_test_... pattern
        m = re.search(r"([rs]k_(?:live|test)_[A-Za-z0-9]+)", text)
        if m:
            return m.group(1)
    except FileNotFoundError:
        pass
    return None


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
    if ptype == "select":
        sel = prop.get("select")
        return sel.get("name", "") if sel else ""
    return ""


# ── HubSpot ───────────────────────────────────────────────────────────────────

def batch_read_by_email(emails):
    """Batch-read HubSpot contacts by email. Returns dict: lower_email → props."""
    if not emails:
        return {}
    url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/read"
    result = {}
    emails = list(emails)
    for i in range(0, len(emails), 100):
        chunk = emails[i: i + 100]
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


def best_dm_from_contacts(contacts, exclude_email=None):
    """
    From a list of HubSpot/Intercom-style contact dicts with 'properties',
    return the best DM candidate (excluding oper_email).
    Priority: explicit DM title > named individual > generic email.
    """
    exc = (exclude_email or "").lower()
    pool = [c for c in contacts if (c.get("properties", {}).get("email") or "").lower() != exc]
    if not pool:
        return None

    # Prefer DM-titled contacts
    dm_titled = [c for c in pool if is_dm_title(c["properties"].get("jobtitle") or "")]
    if dm_titled:
        return dm_titled[0]

    # Then prefer non-generic emails
    non_generic = [c for c in pool if not is_generic_email(c["properties"].get("email") or "")]
    if non_generic:
        return non_generic[0]

    return pool[0]


def classify_confidence(props, source):
    """Return HIGH / MEDIUM / LOW based on what we know about the contact."""
    title = props.get("jobtitle") or ""
    email = props.get("email") or ""
    name  = contact_name(props)

    if is_dm_title(title):
        return "HIGH"
    if source == "stripe":
        return "MEDIUM"
    if is_generic_email(email):
        return "LOW"
    # Named person found (has a real name derived from firstname/lastname, not just email prefix)
    fn = (props.get("firstname") or "").strip()
    ln = (props.get("lastname") or "").strip()
    if fn or ln:
        return "MEDIUM"
    return "LOW"


# ── Intercom ──────────────────────────────────────────────────────────────────

def search_intercom_by_domain(domain):
    """Search Intercom contacts whose email contains @domain."""
    domain = domain.replace("www.", "").strip().lower()
    if not domain:
        return []
    url = "https://api.intercom.io/contacts/search"
    body = {
        "query": {
            "field": "email",
            "operator": "CONTAINS",
            "value": f"@{domain}",
        },
        "pagination": {"per_page": 50},
    }
    try:
        r = requests.post(url, headers=INTERCOM_HDR, json=body, timeout=10)
        if not r.ok:
            return []
        raw = r.json().get("data", [])
        # Normalise to same shape as HubSpot (wrap in properties dict)
        result = []
        for c in raw:
            email = (c.get("email") or "").lower()
            if not email:
                continue
            result.append({"properties": {
                "email":     email,
                "firstname": c.get("name", "").split()[0] if c.get("name") else "",
                "lastname":  " ".join(c.get("name", "").split()[1:]) if c.get("name") else "",
                "jobtitle":  c.get("role") or "",
                "phone":     c.get("phone") or "",
                "mobilephone": "",
            }})
        return result
    except Exception:
        return []


# ── Stripe ────────────────────────────────────────────────────────────────────

def get_stripe_billing_email(stripe_customer_id, stripe_key):
    """Return (email, name) from Stripe customer object, or ('', '')."""
    if not stripe_customer_id or not stripe_key:
        return "", ""
    url = f"https://api.stripe.com/v1/customers/{stripe_customer_id}"
    try:
        r = requests.get(url, auth=(stripe_key, ""), timeout=10)
        if not r.ok:
            return "", ""
        data = r.json()
        return (data.get("email") or ""), (data.get("name") or "")
    except Exception:
        return "", ""


# ── Core research logic ───────────────────────────────────────────────────────

def research_company(co, hs_oper_props, stripe_key):
    """
    Try all 4 sources for one company and return a result dict.
    co = {company, page_id, billing_status, domain, oper_email, stripe_customer_id}
    hs_oper_props = HubSpot properties dict for oper_email (may be None)
    """
    company    = co["company"]
    page_id    = co["page_id"]
    domain     = co["domain"]
    oper_email = (co["oper_email"] or "").lower()

    result = {
        "company":         company,
        "page_id":         page_id,
        "billing_status":  co["billing_status"],
        "domain":          domain,
        "oper_email":      oper_email,
        "oper_title_hs":   "",
        "oper_is_dm":      False,
        "found_dm_email":  "",
        "found_dm_name":   "",
        "found_dm_title":  "",
        "found_dm_phone":  "",
        "source":          "none",
        "confidence":      "",
        "action":          "LINKEDIN_NEEDED",
        "notes":           "",
    }

    # ── Source 1: oper promoted ───────────────────────────────────────────────
    if hs_oper_props:
        title = hs_oper_props.get("jobtitle") or ""
        result["oper_title_hs"] = title
        if is_dm_title(title):
            result["oper_is_dm"] = True
            result["found_dm_email"] = oper_email
            result["found_dm_name"]  = contact_name(hs_oper_props)
            result["found_dm_title"] = title
            result["found_dm_phone"] = (hs_oper_props.get("phone") or
                                        hs_oper_props.get("mobilephone") or "")
            result["source"]     = "oper_promoted"
            result["confidence"] = "HIGH"
            result["action"]     = "SET_DM_EMAIL"
            result["notes"]      = "Oper contact has DM-level job title — promote to DM slot"
            return result

    # ── Source 2: HubSpot domain search ──────────────────────────────────────
    is_free = is_free_domain(domain)
    if domain and not is_free:
        hs_contacts = search_by_domain(domain)
        time.sleep(0.2)
        dm_c = best_dm_from_contacts(hs_contacts, exclude_email=oper_email)
        if dm_c:
            props = dm_c["properties"]
            em    = props.get("email") or ""
            result["found_dm_email"] = em
            result["found_dm_name"]  = contact_name(props)
            result["found_dm_title"] = props.get("jobtitle") or ""
            result["found_dm_phone"] = props.get("phone") or props.get("mobilephone") or ""
            result["source"]         = "hubspot_domain"
            result["confidence"]     = classify_confidence(props, "hubspot_domain")
            result["action"]         = "SET_DM_EMAIL"
            return result

    # ── Source 3: Intercom contacts search ────────────────────────────────────
    if domain and not is_free:
        ic_contacts = search_intercom_by_domain(domain)
        time.sleep(0.2)
        dm_c = best_dm_from_contacts(ic_contacts, exclude_email=oper_email)
        if dm_c:
            props = dm_c["properties"]
            em    = props.get("email") or ""
            result["found_dm_email"] = em
            result["found_dm_name"]  = contact_name(props)
            result["found_dm_title"] = props.get("jobtitle") or ""
            result["found_dm_phone"] = props.get("phone") or ""
            result["source"]         = "intercom"
            result["confidence"]     = classify_confidence(props, "intercom")
            result["action"]         = "SET_DM_EMAIL"
            return result

    # ── Source 4: Stripe billing email ────────────────────────────────────────
    stripe_id = co.get("stripe_customer_id") or ""
    if stripe_id and stripe_key:
        s_email, s_name = get_stripe_billing_email(stripe_id, stripe_key)
        time.sleep(0.3)
        if s_email and s_email.lower() != oper_email:
            fake_props = {
                "email":     s_email,
                "firstname": s_name.split()[0] if s_name else "",
                "lastname":  " ".join(s_name.split()[1:]) if s_name else "",
                "jobtitle":  "",
                "phone":     "",
                "mobilephone": "",
            }
            result["found_dm_email"] = s_email
            result["found_dm_name"]  = s_name or s_email.split("@")[0]
            result["found_dm_title"] = ""
            result["found_dm_phone"] = ""
            result["source"]         = "stripe"
            result["confidence"]     = classify_confidence(fake_props, "stripe")
            result["action"]         = "SET_DM_EMAIL"
            result["notes"]          = "Billing email from Stripe — verify role before use"
            return result

    # ── Unresolved ────────────────────────────────────────────────────────────
    if is_free:
        result["notes"] = "Free-domain email (gmail/hotmail) — domain search not useful; LinkedIn required"
    else:
        result["notes"] = "No DM found in HubSpot, Intercom, or Stripe"
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("find_dm_contacts.py — DM email research for MCT companies")
    print("=" * 70)

    # ── Load Stripe key ───────────────────────────────────────────────────────
    stripe_key = load_stripe_key()
    if stripe_key:
        print(f"Stripe key loaded: {stripe_key[:12]}...")
    else:
        print("WARNING: Stripe key not found in Credentials.md — source 4 disabled")

    # ── Fetch MCT ─────────────────────────────────────────────────────────────
    print("\nFetching all MCT pages...")
    all_pages = fetch_all_mct()
    print(f"  {len(all_pages)} pages loaded")

    # ── Filter to target companies ────────────────────────────────────────────
    targets = []
    for p in all_pages:
        billing = get_prop(p, "💰 Billing Status", "select")
        if billing not in ("Active", "Churning"):
            continue
        dm_email   = get_prop(p, "DM - Point of contact", "email").strip().lower()
        oper_email = get_prop(p, "Oper - Point of contact", "email").strip().lower()
        if dm_email:
            continue  # Already has a DM email
        domain_raw = get_prop(p, "🏢 Domain", "rich_text")
        domain = domain_raw.split(",")[0].strip().lower() if domain_raw else ""
        stripe_id = get_prop(p, "🔗 Stripe Customer ID", "rich_text").strip()
        targets.append({
            "company":            get_prop(p, "🏢 Company Name", "title"),
            "page_id":            p["id"],
            "billing_status":     billing,
            "domain":             domain,
            "oper_email":         oper_email,
            "stripe_customer_id": stripe_id,
        })

    print(f"  {len(targets)} Active/Churning companies without DM email\n")

    # ── Batch-read Oper emails from HubSpot ───────────────────────────────────
    oper_emails = {co["oper_email"] for co in targets if co["oper_email"]
                   and not is_free_domain(co["oper_email"].split("@")[-1])}
    print(f"Batch-reading {len(oper_emails)} Oper emails from HubSpot...")
    hs_by_email = batch_read_by_email(oper_emails)
    print(f"  {len(hs_by_email)} contacts found\n")

    # ── Research each company ─────────────────────────────────────────────────
    results = []
    for i, co in enumerate(targets):
        company = co["company"]
        oper_em = co["oper_email"].lower()
        hs_oper = hs_by_email.get(oper_em)

        print(f"[{i+1:2}/{len(targets)}] {company[:50]}...")
        res = research_company(co, hs_oper, stripe_key)
        results.append(res)

        src  = res["source"]
        conf = res["confidence"] or "—"
        em   = res["found_dm_email"] or "LINKEDIN_NEEDED"
        print(f"         → source={src:<18} conf={conf:<7} email={em}")

    # ── Write JSON ────────────────────────────────────────────────────────────
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nWrote {len(results)} records → {OUTPUT_PATH}")

    # ── Dry-run table ─────────────────────────────────────────────────────────
    found    = [r for r in results if r["action"] == "SET_DM_EMAIL"]
    linkedin = [r for r in results if r["action"] == "LINKEDIN_NEEDED"]

    print(f"\n{'='*115}")
    print("DRY-RUN TABLE — would write these DM emails to MCT")
    print(f"{'='*115}")
    hdr = (f"{'Company':<45} {'Status':<10} {'Source':<18} {'Conf':<7} "
           f"{'Found DM Email':<35} Found DM Name")
    print(hdr)
    print("-" * 115)
    for r in sorted(results, key=lambda x: (x["action"], x["company"])):
        em_col   = r["found_dm_email"] or "—"
        name_col = r["found_dm_name"]  or "—"
        conf_col = r["confidence"]     or "—"
        print(
            f"{r['company'][:44]:<45} {r['billing_status']:<10} "
            f"{r['source']:<18} {conf_col:<7} {em_col[:34]:<35} {name_col}"
        )

    # ── LinkedIn needed list ──────────────────────────────────────────────────
    if linkedin:
        print(f"\n{'='*70}")
        print(f"LINKEDIN NEEDED ({len(linkedin)} companies)")
        print(f"{'='*70}")
        for r in linkedin:
            print(f"  • {r['company']:<50} domain={r['domain']}  notes: {r['notes']}")

    # ── Summary ───────────────────────────────────────────────────────────────
    by_source = {}
    for r in results:
        s = r["source"]
        by_source[s] = by_source.get(s, 0) + 1

    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"Total companies researched : {len(results)}")
    print(f"SET_DM_EMAIL               : {len(found)}")
    print(f"LINKEDIN_NEEDED            : {len(linkedin)}")
    print()
    print("By source:")
    for src, cnt in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"  {src:<20} {cnt}")

    by_conf = {}
    for r in found:
        c = r["confidence"]
        by_conf[c] = by_conf.get(c, 0) + 1
    print()
    print("By confidence (SET_DM_EMAIL only):")
    for conf, cnt in sorted(by_conf.items()):
        print(f"  {conf:<8} {cnt}")


if __name__ == "__main__":
    main()
