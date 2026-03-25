"""
match_phones_to_contacts.py
---------------------------
For each MCT company that has a phone number, search HubSpot contacts at the
same domain and try to match the phone number to a named contact.

Output: a report of (company, phone, matched_name, matched_email, matched_title)
and whether the phone looks like a DM or Oper number.

Usage:
    python3 match_phones_to_contacts.py
"""

import re
import time
import requests
import creds

NOTION_TOKEN  = creds.get("NOTION_TOKEN")
HUBSPOT_TOKEN = creds.get("HUBSPOT_TOKEN")
DS_ID         = "3ceb1ad0-91f1-40db-945a-c51c58035898"

NOTION_HDR = {"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2025-09-03", "Content-Type": "application/json"}
HS_HDR     = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}

# Job title keywords that signal a decision maker
DM_SIGNALS = ["ceo", "founder", "co-founder", "cofounder", "owner", "director",
               "cto", "coo", "president", "managing", "gerente", "ceo", "fundador",
               "propietari", "director general", "head of", "chief"]

GENERIC_EMAILS = {"info@", "admin@", "hola@", "hello@", "pedidos@", "marketing@",
                  "press@", "contact@", "sales@", "soporte@", "support@", "web@",
                  "almacen@", "export@", "data@", "tienda@"}


def normalize_phone(phone):
    """Strip all non-digits, then return last 9 digits for comparison."""
    digits = re.sub(r"\D", "", str(phone))
    return digits[-9:] if len(digits) >= 9 else digits


def fetch_all_mct():
    pages, cursor = [], None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        r = requests.post(f"https://api.notion.com/v1/data_sources/{DS_ID}/query", headers=NOTION_HDR, json=body)
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
        return "".join(x.get("plain_text","") for x in prop.get("title", []))
    if ptype == "phone_number":
        return prop.get("phone_number") or ""
    if ptype == "rich_text":
        items = prop.get("rich_text", [])
        return items[0].get("plain_text","") if items else ""
    if ptype == "email":
        return prop.get("email") or ""
    return ""


def search_hs_contacts_by_domain(domain):
    """Search HubSpot for all contacts whose email contains @domain."""
    domain = domain.replace("www.", "").strip().lower()
    if not domain:
        return []
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": "email", "operator": "CONTAINS_TOKEN", "value": f"@{domain}"}
        ]}],
        "properties": ["email", "firstname", "lastname", "jobtitle", "phone", "mobilephone"],
        "limit": 50,
    }
    r = requests.post(url, headers=HS_HDR, json=body)
    if not r.ok:
        return []
    return r.json().get("results", [])


def is_dm(title, email):
    if not title:
        return False
    t = title.lower()
    return any(sig in t for sig in DM_SIGNALS)


def contact_name(props):
    fn = props.get("firstname") or ""
    ln = props.get("lastname") or ""
    name = f"{fn} {ln}".strip()
    return name if name else props.get("email","").split("@")[0]


def match_phone(target_norm, contacts):
    """Find contact whose phone matches target_norm (last 9 digits)."""
    if not target_norm:
        return None
    for c in contacts:
        props = c.get("properties", {})
        for ph_field in ["phone", "mobilephone"]:
            ph = props.get(ph_field) or ""
            if ph and normalize_phone(ph) == target_norm:
                return c
    return None


def main():
    print("Fetching MCT pages...")
    pages = fetch_all_mct()
    print(f"  {len(pages)} pages loaded\n")

    rows = []
    for p in pages:
        name   = get_prop(p, "🏢 Company Name", "title")
        domain = get_prop(p, "🏢 Domain", "rich_text")
        dm_ph  = get_prop(p, "DM - Phone Number", "phone_number")
        op_ph  = get_prop(p, "Oper - Phone Number", "phone_number")
        dm_em  = get_prop(p, "DM - Point of contact", "email")
        op_em  = get_prop(p, "Oper - Point of contact", "email")
        if dm_ph or op_ph:
            rows.append({
                "page_id": p["id"],
                "company": name,
                "domain": domain.split(",")[0].strip(),   # take first domain if multiple
                "dm_phone": dm_ph,
                "oper_phone": op_ph,
                "dm_email": dm_em,
                "oper_email": op_em,
            })

    print(f"Companies with phone numbers: {len(rows)}\n")

    results = []
    no_match = []

    for i, row in enumerate(rows):
        company   = row["company"]
        domain    = row["domain"]
        dm_ph     = row["dm_phone"]
        op_ph     = row["oper_phone"]

        # The phone we need to identify (currently everything is in oper field)
        phones_to_check = {}
        if dm_ph:
            phones_to_check["DM"] = dm_ph
        if op_ph:
            phones_to_check["Oper"] = op_ph

        # Search HubSpot contacts for this domain
        contacts = []
        if domain and "gmail" not in domain and "hotmail" not in domain:
            contacts = search_hs_contacts_by_domain(domain)
            time.sleep(0.2)

        for slot, phone in phones_to_check.items():
            phone_norm = normalize_phone(phone)
            matched = match_phone(phone_norm, contacts)

            if matched:
                props = matched.get("properties", {})
                name_str  = contact_name(props)
                email_str = props.get("email","")
                title_str = props.get("jobtitle") or ""
                role = "DM" if is_dm(title_str, email_str) else "Oper"
                results.append({
                    "company": company,
                    "slot": slot,
                    "phone": phone,
                    "matched_name": name_str,
                    "matched_email": email_str,
                    "matched_title": title_str,
                    "inferred_role": role,
                })
            else:
                # Try to infer from DM/Oper emails we already know
                role_guess = "unknown"
                name_guess = ""
                email_guess = ""
                if row["dm_email"] and slot == "DM":
                    email_guess = row["dm_email"]
                    role_guess = "DM"
                elif row["oper_email"] and slot == "Oper":
                    email_guess = row["oper_email"]
                    role_guess = "Oper"
                no_match.append({
                    "company": company,
                    "slot": slot,
                    "phone": phone,
                    "domain": domain,
                    "email_guess": email_guess,
                    "role_guess": role_guess,
                })

        if (i+1) % 20 == 0:
            print(f"  Processed {i+1}/{len(rows)}...")

    # ── Print results ──────────────────────────────────────────────────────────
    print("\n" + "="*100)
    print("MATCHED — phone linked to a named HubSpot contact")
    print("="*100)
    print(f"{'Company':<38} {'Phone':<20} {'Name':<25} {'Email':<35} {'Title':<30} Role")
    print("-"*160)
    for r in results:
        print(f"{r['company'][:37]:<38} {r['phone']:<20} {r['matched_name'][:24]:<25} "
              f"{r['matched_email'][:34]:<35} {r['matched_title'][:29]:<30} {r['inferred_role']}")

    print(f"\n\n{'='*100}")
    print("UNMATCHED — phone not found in HubSpot contacts (attributed by email association)")
    print("="*100)
    print(f"{'Company':<38} {'Phone':<20} {'Domain':<25} Email guess")
    print("-"*100)
    for r in no_match:
        print(f"{r['company'][:37]:<38} {r['phone']:<20} {r['domain'][:24]:<25} {r['email_guess']}")

    print(f"\nSummary: {len(results)} matched, {len(no_match)} unmatched")


if __name__ == "__main__":
    main()
