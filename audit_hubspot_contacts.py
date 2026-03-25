"""
audit_hubspot_contacts.py
Comprehensive audit of all MCT companies for HubSpot outreach sequence readiness.

For each MCT company, checks 4 contact slots:
  - DM email / DM phone
  - Oper email / Oper phone
And verifies which emails exist as contacts in HubSpot.

Output: contact_audit.json + printed summary.

    python3 audit_hubspot_contacts.py
"""

import json
import time
import requests
import creds

# ── Credentials (hardcoded) ───────────────────────────────────────────────────
NOTION_TOKEN  = creds.get("NOTION_TOKEN")
DS_ID         = "3ceb1ad0-91f1-40db-945a-c51c58035898"
NOTION_VER    = "2025-09-03"
HUBSPOT_TOKEN = creds.get("HUBSPOT_TOKEN")

OUTPUT_JSON = "/Users/edoardopelli/projects/Customer Success/contact_audit.json"

# ── API helpers ───────────────────────────────────────────────────────────────

def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VER,
        "Content-Type": "application/json",
    }

def hubspot_headers():
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }

# ── Notion property extractors ────────────────────────────────────────────────

def extract_text(prop):
    """Extract plain text from rich_text or title property."""
    if not prop:
        return ""
    t = prop.get("type", "")
    if t in ("rich_text", "title"):
        parts = prop.get(t, [])
        return "".join(p.get("plain_text", "") for p in parts).strip()
    return ""

def extract_select(prop):
    if not prop:
        return ""
    sel = prop.get("select")
    return sel.get("name", "") if sel else ""

def extract_email(prop):
    if not prop:
        return ""
    return (prop.get("email") or "").strip().lower()

def extract_phone(prop):
    if not prop:
        return ""
    return (prop.get("phone_number") or "").strip()


# ── Step 1: Fetch all MCT pages ───────────────────────────────────────────────

def fetch_all_mct_pages():
    """Paginate through the MCT data source — no filter, all 193 companies."""
    url = f"https://api.notion.com/v1/data_sources/{DS_ID}/query"
    pages = []
    cursor = None
    page_num = 0

    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        resp = requests.post(url, headers=notion_headers(), json=body)
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("results", [])
        pages.extend(batch)
        page_num += 1
        print(f"  Fetched page {page_num}: {len(batch)} rows (total so far: {len(pages)})")

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
        time.sleep(0.2)

    return pages


# ── Step 2: Parse each MCT page ───────────────────────────────────────────────

def parse_mct_page(page):
    props   = page.get("properties", {})
    page_id = page.get("id", "")

    company    = extract_text(props.get("🏢 Company Name"))
    domain     = extract_text(props.get("🏢 Domain"))
    billing    = extract_select(props.get("💰 Billing Status"))
    dm_email   = extract_email(props.get("DM - Point of contact"))
    oper_email = extract_email(props.get("Oper - Point of contact"))
    dm_phone   = extract_phone(props.get("DM - Phone Number"))
    oper_phone = extract_phone(props.get("Oper - Phone Number"))

    return {
        "company":        company,
        "page_id":        page_id,
        "domain":         domain,
        "billing_status": billing,
        "dm_email":       dm_email,
        "dm_phone_mct":   dm_phone,
        "dm_in_hs":       False,
        "dm_phone_hs":    "",
        "oper_email":     oper_email,
        "oper_phone_mct": oper_phone,
        "oper_in_hs":     False,
        "oper_phone_hs":  "",
        "dm_eq_oper":     False,  # computed later
    }


# ── Step 3: Batch-lookup emails in HubSpot ────────────────────────────────────

HS_BATCH_URL = "https://api.hubapi.com/crm/v3/objects/contacts/batch/read"
HS_PROPERTIES = [
    "email", "firstname", "lastname", "jobtitle",
    "phone", "mobilephone", "hs_email_optout", "unsubscribedfromall",
]

def batch_lookup_hubspot(emails):
    """
    Query HubSpot for a list of emails in chunks of 100.
    Returns dict: {email_lower: properties_dict or None}
    """
    results = {}
    unique_emails = sorted({e for e in emails if e})
    total = len(unique_emails)

    for i in range(0, total, 100):
        chunk = unique_emails[i:i + 100]
        body = {
            "idProperty": "email",
            "inputs": [{"id": e} for e in chunk],
            "properties": HS_PROPERTIES,
        }
        resp = requests.post(HS_BATCH_URL, headers=hubspot_headers(), json=body)

        if resp.status_code not in (200, 207):
            print(f"  [WARN] HubSpot batch error {resp.status_code}: {resp.text[:200]}")
            for e in chunk:
                results[e] = None
            time.sleep(0.2)
            continue

        data = resp.json()
        found = {}
        for contact in data.get("results", []):
            email_val = (contact.get("properties", {}).get("email") or "").strip().lower()
            if email_val:
                found[email_val] = contact.get("properties", {})

        errors = data.get("errors", [])
        if errors:
            # Errors mean those IDs were not found — treat as missing
            pass

        for e in chunk:
            results[e] = found.get(e)  # None if not found

        found_count = sum(1 for v in found.values() if v is not None)
        print(f"  HubSpot chunk {i // 100 + 1}: {len(chunk)} queried, {found_count} found")
        time.sleep(0.2)

    return results


def hs_best_phone(contact_props):
    """Return the first non-empty phone from a HubSpot contact properties dict."""
    if not contact_props:
        return ""
    return (
        (contact_props.get("phone") or "").strip()
        or (contact_props.get("mobilephone") or "").strip()
    )


# ── Step 4: Compute flags ─────────────────────────────────────────────────────

def add_flags(row):
    row["dm_eq_oper"]     = bool(row["dm_email"] and row["dm_email"] == row["oper_email"])
    row["no_dm_email"]    = not row["dm_email"]
    row["no_oper_email"]  = not row["oper_email"]
    row["dm_not_in_hs"]   = bool(row["dm_email"]) and not row["dm_in_hs"]
    row["oper_not_in_hs"] = bool(row["oper_email"]) and not row["oper_in_hs"]
    row["no_dm_phone"]    = not row["dm_phone_mct"] and not row["dm_phone_hs"]
    # Oper phone is only a gap when DM and Oper are distinct contacts
    row["no_oper_phone"]  = (
        not row["dm_eq_oper"]
        and not row["oper_phone_mct"]
        and not row["oper_phone_hs"]
    )
    return row


# ── Step 5: Print summary ─────────────────────────────────────────────────────

def print_section(title, items, detail_fn=None):
    bar = "=" * 62
    print(f"\n{bar}")
    print(f"  {title}  ({len(items)} companies)")
    print(bar)
    for r in sorted(items, key=lambda x: x["company"].lower()):
        detail = f"  [{detail_fn(r)}]" if detail_fn else ""
        print(f"  {r['company']}{detail}")


def print_summary(rows):
    no_dm_email    = [r for r in rows if r["no_dm_email"]]
    dm_not_in_hs   = [r for r in rows if r["dm_not_in_hs"]]
    no_oper_email  = [r for r in rows if r["no_oper_email"]]
    oper_not_in_hs = [r for r in rows if r["oper_not_in_hs"]]
    no_dm_phone    = [r for r in rows if r["no_dm_phone"]]
    no_oper_phone  = [r for r in rows if r["no_oper_phone"]]

    # Sequence ready: DM email exists AND is found in HubSpot
    seq_ready = [r for r in rows if r["dm_email"] and r["dm_in_hs"]]

    # Fully equipped: DM + Oper each have email + HubSpot presence + phone
    fully_eq = []
    for r in rows:
        dm_ok   = r["dm_email"] and r["dm_in_hs"] and not r["no_dm_phone"]
        oper_ok = (
            r["oper_email"] and r["oper_in_hs"]
            and (r["dm_eq_oper"] or not r["no_oper_phone"])
        )
        if dm_ok and oper_ok:
            fully_eq.append(r)

    print_section(
        "1. Companies with NO DM email",
        no_dm_email,
        lambda r: r["billing_status"] or "—",
    )
    print_section(
        "2. Companies with DM email but NOT in HubSpot",
        dm_not_in_hs,
        lambda r: r["dm_email"],
    )
    print_section(
        "3. Companies with NO Oper email",
        no_oper_email,
        lambda r: r["billing_status"] or "—",
    )
    print_section(
        "4. Companies with Oper email but NOT in HubSpot",
        oper_not_in_hs,
        lambda r: r["oper_email"],
    )
    print_section(
        "5. Companies missing DM phone (MCT + HubSpot both empty)",
        no_dm_phone,
        lambda r: f"dm_email={r['dm_email'] or 'NONE'}",
    )
    print_section(
        "6. Companies missing Oper phone (excl. DM=Oper single-contact)",
        no_oper_phone,
        lambda r: f"oper_email={r['oper_email'] or 'NONE'}",
    )

    bar = "=" * 62
    print(f"\n{bar}")
    print(f"  TOTALS")
    print(bar)
    print(f"  Total companies audited:                        {len(rows)}")
    print(f"  Single-contact (DM == Oper email):              {sum(1 for r in rows if r['dm_eq_oper'])}")
    print(f"")
    print(f"  SEQUENCE READY  (DM email set + in HubSpot):   {len(seq_ready)}")
    print(f"  FULLY EQUIPPED  (DM+Oper: email+phone+HS):     {len(fully_eq)}")
    print(bar)

    # Sequence-ready by billing status
    print("\n  Sequence-ready breakdown by billing status:")
    by_status = {}
    for r in seq_ready:
        s = r["billing_status"] or "Unknown"
        by_status[s] = by_status.get(s, 0) + 1
    for s, c in sorted(by_status.items()):
        print(f"    {s:<20} {c}")

    # Quick gap overview
    print(f"\n  Gap overview (out of {len(rows)} total):")
    print(f"    no_dm_email:    {len(no_dm_email)}")
    print(f"    dm_not_in_hs:   {len(dm_not_in_hs)}")
    print(f"    no_oper_email:  {len(no_oper_email)}")
    print(f"    oper_not_in_hs: {len(oper_not_in_hs)}")
    print(f"    no_dm_phone:    {len(no_dm_phone)}")
    print(f"    no_oper_phone:  {len(no_oper_phone)}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=== MCT → HubSpot Contact Audit ===\n")

    # 1. Fetch all MCT pages (no billing filter — all 193)
    print("Step 1: Fetching all MCT pages from Notion (no filter)...")
    pages = fetch_all_mct_pages()
    print(f"  Total MCT pages retrieved: {len(pages)}\n")

    # 2. Parse
    print("Step 2: Parsing MCT properties...")
    rows = [parse_mct_page(p) for p in pages]
    rows = [r for r in rows if r["company"]]  # drop ghost rows
    print(f"  Parsed {len(rows)} named companies\n")

    # 3. Collect unique emails
    print("Step 3: Collecting unique emails for HubSpot lookup...")
    all_emails = set()
    for r in rows:
        if r["dm_email"]:
            all_emails.add(r["dm_email"])
        if r["oper_email"]:
            all_emails.add(r["oper_email"])
    print(f"  Unique emails to look up: {len(all_emails)}\n")

    # 4. Batch-read HubSpot
    print("Step 4: Batch-reading HubSpot contacts (chunks of 100)...")
    hs_map = batch_lookup_hubspot(list(all_emails))
    hs_found = sum(1 for v in hs_map.values() if v is not None)
    print(f"  HubSpot lookup done: {hs_found}/{len(all_emails)} emails found in HubSpot\n")

    # 5. Enrich rows + compute flags
    print("Step 5: Enriching rows and computing flags...")
    for r in rows:
        if r["dm_email"]:
            contact = hs_map.get(r["dm_email"])
            r["dm_in_hs"]    = contact is not None
            r["dm_phone_hs"] = hs_best_phone(contact) if contact else ""
        if r["oper_email"]:
            contact = hs_map.get(r["oper_email"])
            r["oper_in_hs"]    = contact is not None
            r["oper_phone_hs"] = hs_best_phone(contact) if contact else ""
        add_flags(r)
    print(f"  Flags computed for {len(rows)} rows\n")

    # 6. Write JSON
    print(f"Step 6: Writing {OUTPUT_JSON}...")
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    print(f"  Done ({len(rows)} rows)\n")

    # 7. Summary
    print("=" * 62)
    print("  Step 7: ANALYSIS SUMMARY")
    print("=" * 62)
    print_summary(rows)


if __name__ == "__main__":
    main()
