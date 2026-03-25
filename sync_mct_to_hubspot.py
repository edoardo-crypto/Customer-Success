"""
sync_mct_to_hubspot.py
----------------------
Phase 2 of the Customer Reactivation Outreach Campaign.

Reads hubspot_audit.csv and creates a HubSpot contact for every row
flagged gap=not_in_hubspot. Also finds-or-creates the matching HubSpot
company (by domain) and associates the two.

After this script runs, rows are re-flagged as gap=no_phone — CS managers
should add phone numbers directly in HubSpot before sequences start.

    python3 sync_mct_to_hubspot.py
"""

import csv
import re
import sys
import time
import requests
from pathlib import Path
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
HUBSPOT_TOKEN = creds.get("HUBSPOT_TOKEN")

SCRIPT_DIR = Path(__file__).parent
AUDIT_FILE = SCRIPT_DIR / "hubspot_audit.csv"

HUBSPOT_HDR = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type":  "application/json",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_name(email):
    """Best-effort name split: john.doe@corp.com → ('John', 'Doe')."""
    local = email.split("@")[0]
    parts = [p for p in re.split(r"[._\-+]", local) if p.isalpha()]
    firstname = parts[0].title() if parts else ""
    lastname  = parts[-1].title() if len(parts) > 1 else ""
    return firstname, lastname

def clean_domain(domain):
    return domain.replace("www.", "").strip().lower() if domain else ""

# ── HubSpot: company ──────────────────────────────────────────────────────────

def find_company(domain):
    d = clean_domain(domain)
    if not d:
        return None
    url = "https://api.hubapi.com/crm/v3/objects/companies/search"
    payload = {
        "filterGroups": [{"filters": [
            {"propertyName": "domain", "operator": "EQ", "value": d}
        ]}],
        "properties": ["domain", "name"],
        "limit": 1,
    }
    resp = requests.post(url, headers=HUBSPOT_HDR, json=payload)
    results = resp.json().get("results", []) if resp.ok else []
    return results[0]["id"] if results else None

def create_company(name, domain):
    url = "https://api.hubapi.com/crm/v3/objects/companies"
    payload = {"properties": {"name": name, "domain": clean_domain(domain)}}
    resp = requests.post(url, headers=HUBSPOT_HDR, json=payload)
    resp.raise_for_status()
    return resp.json()["id"]

# ── HubSpot: contact ──────────────────────────────────────────────────────────

def create_contact(email, firstname, lastname, company_name):
    """Create contact; if 409 (duplicate), extract the existing ID from the error."""
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    payload = {"properties": {
        "email":     email,
        "firstname": firstname,
        "lastname":  lastname,
        "company":   company_name,
    }}
    resp = requests.post(url, headers=HUBSPOT_HDR, json=payload)
    if resp.status_code == 409:
        # HubSpot returns: "Contact already exists. Existing ID: 12345"
        msg   = resp.json().get("message", "")
        match = re.search(r"ID:\s*(\d+)", msg)
        if match:
            return match.group(1), "existing"
        raise ValueError(f"Contact 409 but no ID in response: {resp.text[:200]}")
    resp.raise_for_status()
    return resp.json()["id"], "created"

def associate_contact_company(contact_id, company_id):
    url = (f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
           f"/associations/companies/{company_id}/contact_to_company")
    resp = requests.put(url, headers=HUBSPOT_HDR)
    return resp.ok

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not AUDIT_FILE.exists():
        print(f"ERROR: {AUDIT_FILE} not found. Run audit_hubspot_contacts.py first.")
        sys.exit(1)

    with open(AUDIT_FILE, newline="") as f:
        rows = list(csv.DictReader(f))

    to_create = [r for r in rows if r["gap"] == "not_in_hubspot"]
    print(f"Total rows in CSV:  {len(rows)}")
    print(f"Contacts to create: {len(to_create)}\n")

    created  = 0
    existing = 0
    errors   = 0

    for i, row in enumerate(rows):
        if row["gap"] != "not_in_hubspot":
            continue

        email   = row["email"]
        company = row["company_name"]
        domain  = row["domain"]
        idx     = i + 1

        print(f"[{idx}] {company}")

        if not email:
            print("  SKIP — no email in MCT (set gap=no_email)")
            row["gap"] = "no_email"
            errors += 1
            continue

        firstname, lastname = parse_name(email)
        print(f"  email={email}  name={firstname} {lastname}")

        # Find or create company
        company_id = find_company(domain)
        if company_id:
            print(f"  company found: {company_id}")
        elif company:
            try:
                company_id = create_company(company, domain)
                print(f"  company created: {company_id}")
            except Exception as e:
                print(f"  company create FAILED: {e}")
                company_id = None

        # Create (or recover) contact
        try:
            contact_id, outcome = create_contact(email, firstname, lastname, company)
            print(f"  contact {outcome}: {contact_id}")
            if outcome == "created":
                created += 1
            else:
                existing += 1
        except Exception as e:
            print(f"  contact create FAILED: {e}")
            errors += 1
            continue

        # Associate contact ↔ company
        if company_id:
            ok = associate_contact_company(contact_id, company_id)
            print(f"  association: {'OK' if ok else 'FAILED'}")

        # Update the row in memory
        row["hubspot_contact_id"] = contact_id
        row["hubspot_company_id"] = company_id or ""
        row["gap"] = "no_phone"   # created, but still needs a phone number

        time.sleep(0.3)

    # Rewrite CSV with updated IDs
    fieldnames = list(rows[0].keys())
    with open(AUDIT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("\n── Done ──────────────────────────────────────────────────")
    print(f"  Contacts created:  {created}")
    print(f"  Already existed:   {existing}")
    print(f"  Errors / skipped:  {errors}")
    print(f"  CSV updated:       {AUDIT_FILE}")
    print()
    print("Next steps:")
    print("  1. CS managers open HubSpot and add phone numbers for 'no_phone' rows")
    print("  2. Alex & Aya create their sequences in HubSpot UI (Sales → Sequences)")
    print("  3. Paste sequence IDs + user IDs into enroll_hubspot_sequence.py")
    print("  4. Run enroll_hubspot_sequence.py")


if __name__ == "__main__":
    main()
