"""
enroll_meeting_no.py
--------------------
Enroll all MCT companies where 'Meeting Scheduled = No'
(Active or Churning billing status) into their HubSpot sequence.

Enrolls BOTH DM and Oper contacts when they are different people.
If DM == Oper (same email) only one enrollment is made.

Steps:
  1. Fetch MCT, filter Meeting Scheduled = No AND Active/Churning
  2. For each company: collect DM email + Oper email
  3. Batch-read all emails from HubSpot → get contact_ids
  4. Get language from hubspot_audit.csv (fallback: ES)
  5. Enroll each contact in sequence (cs_owner × language)
     Order: Churning first → Active; within each group MRR desc

Usage:
    python3 enroll_meeting_no.py           # dry-run (prints table, no enroll)
    python3 enroll_meeting_no.py --enroll  # actually enroll
"""

import csv
import sys
import time
import requests
from datetime import datetime, timezone
from pathlib import Path
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
NOTION_TOKEN  = creds.get("NOTION_TOKEN")
DS_ID         = "3ceb1ad0-91f1-40db-945a-c51c58035898"
HUBSPOT_TOKEN = creds.get("HUBSPOT_TOKEN")

# ── Sequence config ───────────────────────────────────────────────────────────
ALEX_EN_SEQUENCE_ID = "769600699"
ALEX_ES_SEQUENCE_ID = "769600701"
AYA_EN_SEQUENCE_ID  = "769600702"
AYA_ES_SEQUENCE_ID  = "769557714"

ALEX_SENDER_EMAIL = "alex@konvoai.com"
AYA_SENDER_EMAIL  = "aya@konvoai.com"
ALEX_USER_ID      = "71846567"
AYA_USER_ID       = "29582160"

SEQUENCE_MAP = {
    ("Alex", "EN"): ALEX_EN_SEQUENCE_ID,
    ("Alex", "ES"): ALEX_ES_SEQUENCE_ID,
    ("Aya",  "EN"): AYA_EN_SEQUENCE_ID,
    ("Aya",  "ES"): AYA_ES_SEQUENCE_ID,
}
SENDER_MAP  = {"Alex": ALEX_SENDER_EMAIL, "Aya": AYA_SENDER_EMAIL}
USER_ID_MAP = {"Alex": ALEX_USER_ID,      "Aya": AYA_USER_ID}

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
AUDIT_CSV  = SCRIPT_DIR / "hubspot_audit.csv"
LOG_FILE   = SCRIPT_DIR / "enrollment_meeting_no_log.csv"

# ── Headers ───────────────────────────────────────────────────────────────────
NOTION_HDR = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}
HS_HDR = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}


# ── Notion helpers ────────────────────────────────────────────────────────────

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
    if ptype == "rich_text":
        items = prop.get("rich_text", [])
        return items[0].get("plain_text", "") if items else ""
    if ptype == "email":
        return (prop.get("email") or "").strip().lower()
    if ptype == "phone_number":
        return prop.get("phone_number") or ""
    if ptype == "select":
        sel = prop.get("select")
        return sel.get("name", "") if sel else ""
    if ptype == "number":
        return prop.get("number") or 0
    return ""


# ── Language map ──────────────────────────────────────────────────────────────

def load_language_map():
    lang_by_page  = {}
    lang_by_email = {}
    if not AUDIT_CSV.exists():
        print(f"  [WARN] {AUDIT_CSV} not found — defaulting all to ES")
        return lang_by_page, lang_by_email
    with open(AUDIT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            lang = (row.get("language") or "ES").strip().upper() or "ES"
            pid  = (row.get("notion_page_id") or "").strip()
            em   = (row.get("email") or "").strip().lower()
            if pid:
                lang_by_page[pid] = lang
            if em:
                lang_by_email[em] = lang
    return lang_by_page, lang_by_email


# ── HubSpot helpers ───────────────────────────────────────────────────────────

def batch_read_hs(emails):
    """Returns {email_lower: {"id": ..., "properties": ...}} for found contacts."""
    result = {}
    emails = [e for e in emails if e]
    for i in range(0, len(emails), 100):
        chunk = emails[i:i+100]
        body = {
            "idProperty": "email",
            "inputs": [{"id": e} for e in chunk],
            "properties": ["email", "firstname", "lastname", "phone", "mobilephone"],
        }
        r = requests.post(
            "https://api.hubapi.com/crm/v3/objects/contacts/batch/read",
            headers=HS_HDR, json=body,
        )
        if r.ok:
            for c in r.json().get("results", []):
                em = (c.get("properties", {}).get("email") or "").lower()
                if em:
                    result[em] = {"id": c["id"], "properties": c.get("properties", {})}
        time.sleep(0.2)
    return result


def enroll_contact(contact_id, sequence_id, sender_email, user_id):
    url = "https://api.hubapi.com/automation/v4/sequences/enrollments"
    payload = {
        "sequenceId":  sequence_id,
        "contactId":   contact_id,
        "senderEmail": sender_email,
    }
    resp = requests.post(url, headers=HS_HDR, json=payload, params={"userId": user_id})
    return resp.status_code, resp.text


# ── Sort key ──────────────────────────────────────────────────────────────────

def sort_key(row):
    rank = {"Churning": 0, "Active": 1}.get(row["billing_status"], 2)
    return (rank, -float(row.get("mrr") or 0))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    dry_run = "--enroll" not in sys.argv

    print("=== Enroll 'Meeting Scheduled = No' — DM + Oper ===\n")
    if dry_run:
        print("  DRY-RUN mode — pass --enroll to actually enroll\n")

    # 1. Fetch MCT
    print("Fetching MCT pages...")
    pages = fetch_all_mct()
    print(f"  {len(pages)} pages loaded\n")

    # 2. Filter Meeting Scheduled ≠ Yes, Active/Churning
    companies = []
    for p in pages:
        billing = get_prop(p, "💰 Billing Status", "select")
        if billing not in ("Active", "Churning"):
            continue
        if get_prop(p, "Meeting Scheduled", "select") == "Yes":
            continue
        companies.append({
            "page_id":        p["id"],
            "company":        get_prop(p, "🏢 Company Name",          "title"),
            "dm_email":       get_prop(p, "DM - Point of contact",    "email"),
            "op_email":       get_prop(p, "Oper - Point of contact",  "email"),
            "cs_owner":       get_prop(p, "⭐ CS Owner",              "select"),
            "mrr":            get_prop(p, "💰 MRR",                   "number"),
            "billing_status": billing,
        })
    print(f"  Companies (Meeting=No, Active/Churning): {len(companies)}\n")

    # 3. Load language map
    lang_by_page, lang_by_email = load_language_map()

    # 4. Collect all unique emails to look up
    all_emails = set()
    for c in companies:
        if c["dm_email"]:
            all_emails.add(c["dm_email"])
        if c["op_email"]:
            all_emails.add(c["op_email"])
    print(f"Batch-reading {len(all_emails)} emails from HubSpot...")
    hs_map = batch_read_hs(list(all_emails))
    print(f"  Found {len(hs_map)} contacts in HubSpot\n")

    # 5. Build per-contact enrollment rows
    # Each entry = one contact to enroll (DM or Oper)
    enrollable = []
    skipped    = []

    for c in companies:
        company  = c["company"]
        page_id  = c["page_id"]
        cs_owner = c["cs_owner"]
        billing  = c["billing_status"]
        mrr      = c["mrr"]

        # Language for this company
        lang = lang_by_page.get(page_id) or lang_by_email.get(c["dm_email"]) or "ES"
        if lang not in ("EN", "ES"):
            lang = "ES"

        sequence_id  = SEQUENCE_MAP.get((cs_owner, lang), "")
        sender_email = SENDER_MAP.get(cs_owner, "")
        user_id      = USER_ID_MAP.get(cs_owner, "")

        if not sequence_id:
            skipped.append({"company": company, "role": "both",
                            "email": "", "reason": f"no_sequence_{cs_owner}_{lang}"})
            continue

        # --- DM contact ---
        dm_added = False
        if c["dm_email"]:
            hs = hs_map.get(c["dm_email"])
            if hs:
                enrollable.append({
                    "company":        company,
                    "role":           "DM",
                    "email":          c["dm_email"],
                    "contact_id":     hs["id"],
                    "cs_owner":       cs_owner,
                    "language":       lang,
                    "billing_status": billing,
                    "mrr":            mrr,
                    "sequence_id":    sequence_id,
                    "sender_email":   sender_email,
                    "user_id":        user_id,
                })
                dm_added = True
            else:
                skipped.append({"company": company, "role": "DM",
                                "email": c["dm_email"], "reason": "not_in_hubspot"})
        else:
            skipped.append({"company": company, "role": "DM",
                            "email": "", "reason": "no_dm_email"})

        # --- Oper contact (only if different from DM) ---
        if c["op_email"] and c["op_email"] != c["dm_email"]:
            hs = hs_map.get(c["op_email"])
            if hs:
                enrollable.append({
                    "company":        company,
                    "role":           "Oper",
                    "email":          c["op_email"],
                    "contact_id":     hs["id"],
                    "cs_owner":       cs_owner,
                    "language":       lang,
                    "billing_status": billing,
                    "mrr":            mrr,
                    "sequence_id":    sequence_id,
                    "sender_email":   sender_email,
                    "user_id":        user_id,
                })
            else:
                skipped.append({"company": company, "role": "Oper",
                                "email": c["op_email"], "reason": "not_in_hubspot"})

    # Sort: Churning → Active, then MRR desc (keep DM before Oper for same company via stable sort)
    enrollable.sort(key=sort_key)

    # 6. Print plan
    print(f"{'='*100}")
    print(f"  ENROLLMENT PLAN  ({len(enrollable)} contacts | {len(skipped)} skipped)")
    print(f"{'='*100}")
    print(f"  {'#':<4} {'Status':<10} {'MRR':>6}  {'Owner':<5} {'Lang':<4} {'Role':<5} Company  /  Email")
    print(f"  {'-'*4} {'-'*10} {'-'*6}  {'-'*5} {'-'*4} {'-'*5} {'-'*60}")
    for i, row in enumerate(enrollable, 1):
        print(f"  {i:<4} {row['billing_status']:<10} {float(row['mrr'] or 0):>6.0f}  "
              f"{row['cs_owner']:<5} {row['language']:<4} {row['role']:<5} "
              f"{row['company'][:40]}  {row['email']}")

    if skipped:
        print(f"\n  SKIPPED ({len(skipped)}):")
        for s in sorted(skipped, key=lambda x: x["company"]):
            print(f"    {s['company']:<48} role={s['role']:<5} reason={s['reason']}  {s['email']}")

    print(f"\n  Sequence breakdown:")
    for owner in ("Alex", "Aya"):
        for lang in ("EN", "ES"):
            n = sum(1 for r in enrollable if r["cs_owner"] == owner and r["language"] == lang)
            seq_id = SEQUENCE_MAP.get((owner, lang), "—")
            if n:
                print(f"    {owner} — {lang}: {n:>3} contacts  → sequence {seq_id}")

    if dry_run:
        print("\n  [DRY RUN] No enrollments made. Re-run with --enroll to proceed.")
        return

    # 7. Enroll
    print(f"\n{'='*100}")
    print(f"  ENROLLING {len(enrollable)} contacts...")
    print(f"{'='*100}")

    log_rows = []
    enrolled = 0
    errors   = 0

    for i, row in enumerate(enrollable, 1):
        status_code, body = enroll_contact(
            row["contact_id"], row["sequence_id"],
            row["sender_email"], row["user_id"],
        )
        now = datetime.now(timezone.utc).isoformat()

        if status_code in (200, 201, 204):
            status = "enrolled"
            error  = ""
            enrolled += 1
            print(f"  [{i}/{len(enrollable)}] {row['company']} [{row['role']}] "
                  f"({row['cs_owner']}/{row['language']}, {row['billing_status']}) → enrolled")
        else:
            status = "error"
            error  = body[:300]
            errors += 1
            print(f"  [{i}/{len(enrollable)}] {row['company']} [{row['role']}] "
                  f"({row['cs_owner']}/{row['language']}) → ERROR {status_code}: {error[:80]}")

        log_rows.append({
            "company":        row["company"],
            "role":           row["role"],
            "contact_id":     row["contact_id"],
            "email":          row["email"],
            "cs_owner":       row["cs_owner"],
            "language":       row["language"],
            "billing_status": row["billing_status"],
            "mrr":            row["mrr"],
            "enrolled_at":    now,
            "status":         status,
            "error":          error,
        })

        time.sleep(0.5)

    # Write log
    fieldnames = ["company", "role", "contact_id", "email", "cs_owner", "language",
                  "billing_status", "mrr", "enrolled_at", "status", "error"]
    with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(log_rows)

    print(f"\n  ── Summary ──────────────────────────────────────────")
    print(f"  Enrolled: {enrolled}")
    print(f"  Errors:   {errors}")
    print(f"  Log:      {LOG_FILE}")
    print(f"\n  Verify in HubSpot → Sales → Sequences → open each sequence → Enrolled tab")


if __name__ == "__main__":
    main()
