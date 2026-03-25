"""
enrich_gap_contacts.py
----------------------
Enriches the 41 customers in hubspot_audit.csv that have gap=no_email or
gap=no_phone, using the existing RB2B → Clay → HubSpot pipeline.

Pipeline:
  1. Build enrichment CSV from gap rows
  2. POST it to the RB2B form endpoint (triggers n8n → Clay → HubSpot)
  3. Wait for Clay async enrichment (default 10 min)
  4. Re-scan HubSpot per company domain, pull enriched contacts
  5. Update hubspot_audit.csv in-place

Usage:
  python3 enrich_gap_contacts.py             # full run (trigger + wait + scan)
  python3 enrich_gap_contacts.py --no-wait   # trigger now, skip wait
  python3 enrich_gap_contacts.py --scan-only # skip trigger, only re-scan HubSpot
  python3 enrich_gap_contacts.py --wait 300  # custom wait in seconds
"""

import argparse
import csv
import io
import sys
import time
import requests
from pathlib import Path
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
HUBSPOT_TOKEN = creds.get("HUBSPOT_TOKEN")

SCRIPT_DIR  = Path(__file__).parent
AUDIT_FILE  = SCRIPT_DIR / "hubspot_audit.csv"

RB2B_FORM_URL = "https://konvoai.app.n8n.cloud/form/rb2b-form-upload"

HUBSPOT_HDR = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type":  "application/json",
}

GAP_TYPES = {"no_email", "no_phone"}

# ── HubSpot helpers ────────────────────────────────────────────────────────────

def search_company_by_domain(domain):
    clean = domain.replace("www.", "").strip().lower()
    url   = "https://api.hubapi.com/crm/v3/objects/companies/search"
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
    url  = (f"https://api.hubapi.com/crm/v3/objects/companies"
            f"/{company_id}/associations/contacts")
    resp = requests.get(url, headers=HUBSPOT_HDR)
    if not resp.ok:
        return []
    return [r["id"] for r in resp.json().get("results", [])]


def get_contact_details(contact_id):
    url    = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    params = {"properties": "email,phone,mobilephone,firstname,lastname"}
    resp   = requests.get(url, headers=HUBSPOT_HDR, params=params)
    return resp.json() if resp.ok else None


def best_contact(contacts):
    """
    Given a list of contact dicts, pick the best one.
    Prefer contacts with both email AND phone; then email-only; then phone-only.
    """
    if not contacts:
        return None
    has_both  = [c for c in contacts if c.get("email") and c.get("phone")]
    has_email = [c for c in contacts if c.get("email")]
    if has_both:
        return has_both[0]
    if has_email:
        return has_email[0]
    return contacts[0]


def scan_company(domain, old_contact_id):
    """
    Look up a company by domain in HubSpot, fetch all its contacts,
    and return a dict with the best contact's data.
    Returns None if nothing found.
    """
    hs_co = search_company_by_domain(domain)
    if not hs_co:
        return None

    company_id = hs_co["id"]
    cids       = get_company_contact_ids(company_id)
    if not cids:
        return None

    contacts = []
    for cid in cids[:10]:          # cap at 10 contacts per company
        c = get_contact_details(cid)
        if not c:
            continue
        props = c.get("properties", {})
        contacts.append({
            "id":    c.get("id", ""),
            "email": props.get("email") or "",
            "phone": props.get("phone") or props.get("mobilephone") or "",
            "firstname": props.get("firstname") or "",
            "lastname":  props.get("lastname") or "",
        })
        time.sleep(0.1)

    winner = best_contact(contacts)
    if not winner:
        return None

    if old_contact_id and winner["id"] != old_contact_id:
        print(f"    [NOTE] new contact {winner['id']} differs from old {old_contact_id}")

    return {
        "hubspot_company_id": company_id,
        "hubspot_contact_id": winner["id"],
        "email":  winner["email"],
        "phone":  winner["phone"],
    }

# ── Step 1: build enrichment CSV ───────────────────────────────────────────────

def build_enrichment_csv(rows):
    """
    Filter gap rows, build the CSV string for the RB2B form.
    Returns (csv_string, submitted_rows, skipped_rows).
    """
    buf      = io.StringIO()
    writer   = csv.writer(buf)
    writer.writerow(["Company Website", "Company Name", "Company Industry"])

    submitted = []
    skipped   = []

    for r in rows:
        if r["gap"] not in GAP_TYPES:
            continue

        domain = r.get("domain", "").strip()

        # Skip if no domain
        if not domain:
            skipped.append((r["company_name"], "no domain"))
            continue

        # Skip if it's a generic/personal domain
        if domain.lower() in ("gmail.com", "hotmail.com", "yahoo.com"):
            skipped.append((r["company_name"], f"generic domain ({domain})"))
            continue

        # Skip if multiple domains (contains comma)
        if "," in domain:
            skipped.append((r["company_name"], f"multiple domains ({domain})"))
            continue

        writer.writerow([domain, r["company_name"], "E-commerce"])
        submitted.append(r)

    return buf.getvalue(), submitted, skipped

# ── Step 2: POST to RB2B form ──────────────────────────────────────────────────

def post_to_rb2b_form(csv_string, dry_run=False):
    if dry_run:
        print("[DRY RUN] Would POST CSV to RB2B form endpoint.")
        return

    csv_bytes = csv_string.encode("utf-8")
    files     = {
        "RB2B CSV File": ("enrichment.csv", csv_bytes, "text/csv"),
    }
    print(f"Submitting CSV to RB2B form endpoint…")
    resp = requests.post(RB2B_FORM_URL, files=files, timeout=30)
    if resp.ok:
        print(f"  → Submitted OK (HTTP {resp.status_code})")
    else:
        print(f"  → WARN: HTTP {resp.status_code}: {resp.text[:200]}")

# ── Step 3: wait ──────────────────────────────────────────────────────────────

def wait_for_enrichment(seconds):
    print(f"\nWaiting {seconds}s for Clay enrichment to complete…")
    step = 30
    elapsed = 0
    while elapsed < seconds:
        remaining = seconds - elapsed
        bar_len   = 40
        filled    = int(bar_len * elapsed / seconds)
        bar       = "█" * filled + "░" * (bar_len - filled)
        sys.stdout.write(f"\r  [{bar}] {elapsed}s / {seconds}s  ({remaining}s left) ")
        sys.stdout.flush()
        time.sleep(min(step, remaining))
        elapsed += step
    sys.stdout.write(f"\r  [{'█' * 40}] {seconds}s / {seconds}s  (done)          \n")
    print()

# ── Step 4+5: re-scan HubSpot and update CSV ──────────────────────────────────

def rescan_and_update(rows, gap_rows):
    """
    For each gap row, re-scan HubSpot, update the row dict if enrichment found data.
    Returns summary counts.
    """
    resolved_full  = 0   # now have email + phone
    resolved_phone = 0   # had email, now have phone
    resolved_email = 0   # had nothing, now have email (still no phone)
    still_missing  = 0

    for r in rows:
        if r["gap"] not in GAP_TYPES:
            continue

        domain = r.get("domain", "").strip()
        if not domain or "," in domain or domain.lower() in ("gmail.com",):
            still_missing += 1
            continue

        company_name = r.get("company_name", "")
        old_gap      = r["gap"]
        old_contact  = r.get("hubspot_contact_id", "")

        print(f"  Scanning {company_name} ({domain})…")
        result = scan_company(domain, old_contact)
        time.sleep(0.2)

        if not result:
            print(f"    → not found in HubSpot")
            still_missing += 1
            continue

        new_email = result["email"]
        new_phone = result["phone"]
        new_cid   = result["hubspot_contact_id"]
        new_coid  = result["hubspot_company_id"]

        if new_email and new_phone:
            r["email"]              = new_email
            r["phone"]              = new_phone
            r["hubspot_contact_id"] = new_cid
            r["hubspot_company_id"] = new_coid
            r["gap"]                = "none"
            print(f"    → RESOLVED  email={new_email}  phone={new_phone}")
            resolved_full += 1

        elif new_email:
            r["email"]              = new_email
            r["hubspot_contact_id"] = new_cid
            r["hubspot_company_id"] = new_coid
            r["gap"]                = "no_phone"
            print(f"    → email only: {new_email}")
            resolved_email += 1

        elif new_phone and old_gap == "no_phone":
            # Had contact, still no email — but maybe phone improved
            r["phone"]              = new_phone
            r["hubspot_contact_id"] = new_cid
            r["hubspot_company_id"] = new_coid
            r["gap"]                = "none"
            print(f"    → phone found: {new_phone}")
            resolved_phone += 1

        else:
            print(f"    → still no useful data (cid={new_cid})")
            still_missing += 1

    return {
        "resolved_full":  resolved_full,
        "resolved_email": resolved_email,
        "resolved_phone": resolved_phone,
        "still_missing":  still_missing,
    }

# ── Write CSV back ─────────────────────────────────────────────────────────────

def write_audit_csv(rows):
    fieldnames = [
        "company_name", "cs_owner", "mrr", "billing_status",
        "email", "phone", "hubspot_contact_id", "hubspot_company_id",
        "gap", "domain", "all_contacts", "notion_page_id",
    ]
    with open(AUDIT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nUpdated → {AUDIT_FILE}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enrich gap contacts via RB2B → Clay pipeline")
    parser.add_argument("--no-wait",   action="store_true", help="Trigger pipeline but skip wait")
    parser.add_argument("--scan-only", action="store_true", help="Skip trigger, only re-scan HubSpot")
    parser.add_argument("--wait",      type=int, default=600, help="Seconds to wait (default 600)")
    args = parser.parse_args()

    # Load CSV
    with open(AUDIT_FILE, newline="") as f:
        rows = list(csv.DictReader(f))

    gap_rows = [r for r in rows if r.get("gap") in GAP_TYPES]
    print(f"Gap rows found: {len(gap_rows)}  "
          f"(no_email={sum(1 for r in gap_rows if r['gap']=='no_email')}, "
          f"no_phone={sum(1 for r in gap_rows if r['gap']=='no_phone')})\n")

    # ── Step 1 ─────────────────────────────────────────────────────────────────
    csv_string, submitted, skipped = build_enrichment_csv(rows)

    if skipped:
        print("Skipped (will need manual lookup):")
        for name, reason in skipped:
            print(f"  • {name}: {reason}")
        print()

    print(f"Rows to submit to Clay: {len(submitted)}")

    if not args.scan_only:
        # ── Step 2 ─────────────────────────────────────────────────────────────
        post_to_rb2b_form(csv_string)

        # ── Step 3 ─────────────────────────────────────────────────────────────
        if not args.no_wait:
            wait_for_enrichment(args.wait)
        else:
            print("\n--no-wait set: skipping wait. Run with --scan-only later to re-scan.\n")
            return

    # ── Step 4 ─────────────────────────────────────────────────────────────────
    print("Re-scanning HubSpot for enriched contacts…")
    summary = rescan_and_update(rows, gap_rows)

    # ── Step 5 ─────────────────────────────────────────────────────────────────
    write_audit_csv(rows)

    # Summary
    from collections import Counter
    gap_counts = Counter(r["gap"] for r in rows)
    print("\n── Summary ───────────────────────────────────────────────────────")
    print(f"Total customers:   {len(rows)}")
    print(f"  gap=none:        {gap_counts.get('none', 0)}")
    print(f"  gap=no_phone:    {gap_counts.get('no_phone', 0)}")
    print(f"  gap=no_email:    {gap_counts.get('no_email', 0)}")
    print()
    print("Enrichment results (this run):")
    print(f"  fully resolved:  {summary['resolved_full']}")
    print(f"  email added:     {summary['resolved_email']}")
    print(f"  phone added:     {summary['resolved_phone']}")
    print(f"  still missing:   {summary['still_missing']}")

    if gap_counts.get("no_email", 0) + gap_counts.get("no_phone", 0) > 0:
        print("\nRemaining gaps need manual lookup or a second Clay enrichment pass.")


if __name__ == "__main__":
    main()
