"""
enroll_hubspot_sequence.py
--------------------------
Phase 4 of the Customer Reactivation Outreach Campaign.

Reads hubspot_audit.csv and bulk-enrolls each contact into the HubSpot
sequence matched to their CS owner AND language (EN / ES / CA).

Enrollment order: Churning customers first, then Active — within each
group sorted by MRR descending so highest-value customers are contacted
earliest.

BEFORE RUNNING:
  1. Run tag_languages.py to add/confirm the `language` column in the CSV
  2. Alex creates 2 sequences in HubSpot (Sales Hub → Sequences → Create):
       "Alex — EN"  /  "Alex — ES"
  3. Aya creates 2 sequences:
       "Aya — EN"   /  "Aya — ES"
  4. Each sequence needs: connected inbox, meeting link in email, correct copy
  5. Copy each sequence's ID from the URL (the number after /sequences/)
  6. Fill in the 4 constants below
  7. Run: python3 enroll_hubspot_sequence.py
"""

import csv
import sys
import time
import requests
from datetime import datetime, timezone
from pathlib import Path
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
HUBSPOT_TOKEN = creds.get("HUBSPOT_TOKEN")

# ── FILL THESE IN BEFORE RUNNING ─────────────────────────────────────────────
# Sequence IDs: HubSpot → Sales → Sequences → open sequence → number in URL
# Sender emails must match the connected inbox in HubSpot for each user
ALEX_EN_SEQUENCE_ID = "769600699"   # "Alex — EN"
ALEX_ES_SEQUENCE_ID = "769600701"   # "Alex — ES"

AYA_EN_SEQUENCE_ID  = "769600702"   # "Aya — EN"
AYA_ES_SEQUENCE_ID  = "769557714"   # "Aya — ES"

ALEX_SENDER_EMAIL = "alex@konvoai.com"
AYA_SENDER_EMAIL  = "aya@konvoai.com"

ALEX_USER_ID = "71846567"
AYA_USER_ID  = "29582160"
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
AUDIT_FILE = SCRIPT_DIR / "hubspot_audit.csv"
LOG_FILE   = SCRIPT_DIR / "enrollment_log.csv"

HUBSPOT_HDR = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type":  "application/json",
}

# ── Companies excluded from outreach (Notion MCT "Not immediate catchup" = ✓) ──
EXCLUDED_COMPANIES = {
    "APRILPLANTS",
    "Aguas do Paraño",
    "Alhamas Artesania",
    "Ancestra",
    "Andrés Marín",
    "Atlas Flowers Limited",
    "BAYMO THE LABEL",
    "Bad Habits",
    "Blanca Jewels",
    "Brikum",
    "CHOCOLATES TORRAS, S.A.",
    "CRU E NU",
    "Calzados Pablo SLU",
    "Daedo International",
    "Deeply Europe",
    "ECOMDELSUR SRL",
    "ELISA RIVERA SLU",
    "EMPORIUM AOVE",
    "Electrotodo",
    "Endor Technologies",
    "Evercore Europe",
    "FITPLANET BRAND SL",
    "FLYSURF BRAND SL",
    "Farmaciasdirect",
    "Find Your Everest",
    "Flor de Madre Ltd",
    "GIMAGUAS STUDIO SL",
    "Gabis",
    "IBG Illice Brands Group",
    "Indian Ocean Consulting, S.L.",
    "Joyería José Luis Romero SL",
    "Labei Cosmetics",
    "Lion Hair",
    "Live Out Solutions SL",
    "Lovely Story",
    "MERCAJEANS GROUP, S. L.",
    "MUN FERMENTS SL",
    "Matcha Jeans",
    "MindTravelerBcn",
    "Moma Bikes",
    "NATURAL SMART BEAUTY SL",
    "Naïve",
    "Nomade Nation SL",
    "Northdeco",
    "OFF TV MEDIA GROUP SL",
    "OFFVIEW SL",
    "Odisei Music",
    "Old School Spain",
    "Orvis UK",
    "PAR Y ESCALA SL",
    "PARFUMS NOX",
    "PAYS D,OC, S.L.",
    "PICSIL",
    "Platadepalo",
    "Platanomelón",
    "Pott Candles",
    "Productos Curly",
    "Santa Teresa Gourmet",
    "Sepiia",
    "Sherperex SL",
    "Simuero",
    "Small Beer",
    "Tattoox",
    "The Cool Bottles Company SL",
    "The Stage Ventures SL",
    "Tienda Bass",
    "Tienda Carpfishing",
    "UNISA Shoes & Accessories",
    "UO ESTUDIO SL",
    "Unikare S.L.",
    "VALMAS GROUP LIMITED",
    "VITALBRANDS",
    "Vinkova Leotards",
}

BILLING_RANK = {"Churning": 0, "Active": 1}

# ── Sort key: Churning before Active, then MRR descending ────────────────────

def sort_key(row):
    status_rank = BILLING_RANK.get(row.get("billing_status", ""), 99)
    mrr         = -float(row.get("mrr") or 0)
    return (status_rank, mrr)


# ── Pick sequence ID based on owner + language ────────────────────────────────

SEQUENCE_MAP = {
    ("Alex", "EN"): lambda: ALEX_EN_SEQUENCE_ID,
    ("Alex", "ES"): lambda: ALEX_ES_SEQUENCE_ID,
    ("Aya",  "EN"): lambda: AYA_EN_SEQUENCE_ID,
    ("Aya",  "ES"): lambda: AYA_ES_SEQUENCE_ID,
}

SENDER_MAP = {
    "Alex": ALEX_SENDER_EMAIL,
    "Aya":  AYA_SENDER_EMAIL,
}

USER_ID_MAP = {
    "Alex": ALEX_USER_ID,
    "Aya":  AYA_USER_ID,
}


def get_sequence_id(owner: str, language: str) -> str:
    getter = SEQUENCE_MAP.get((owner, language))
    return getter() if getter else ""


# ── HubSpot: enroll one contact ───────────────────────────────────────────────

def enroll(contact_id: str, sequence_id: str, sender_email: str, user_id: str):
    url = "https://api.hubapi.com/automation/v4/sequences/enrollments"
    payload = {
        "sequenceId":  sequence_id,
        "contactId":   contact_id,
        "senderEmail": sender_email,
    }
    resp = requests.post(url, headers=HUBSPOT_HDR, json=payload, params={"userId": user_id})
    return resp.status_code, resp.text


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Guard: all 6 sequence IDs must be set
    all_ids = {
        "ALEX_EN_SEQUENCE_ID": ALEX_EN_SEQUENCE_ID,
        "ALEX_ES_SEQUENCE_ID": ALEX_ES_SEQUENCE_ID,
        "AYA_EN_SEQUENCE_ID":  AYA_EN_SEQUENCE_ID,
        "AYA_ES_SEQUENCE_ID":  AYA_ES_SEQUENCE_ID,
    }
    missing = [name for name, val in all_ids.items() if not val]
    if missing:
        print("ERROR: The following sequence IDs are not set:")
        for name in missing:
            print(f"  {name}")
        print()
        print("Steps:")
        print("  1. Alex creates 2 sequences in HubSpot:")
        print("       'Alex — EN'  /  'Alex — ES'  /  'Alex — CA'")
        print("  2. Aya creates 3 sequences:")
        print("       'Aya — EN'   /  'Aya — ES'   /  'Aya — CA'")
        print("  3. Copy each ID from the URL (number after /sequences/)")
        print("  4. Fill them in at the top of this script, then re-run.")
        sys.exit(1)

    if not AUDIT_FILE.exists():
        print(f"ERROR: {AUDIT_FILE} not found. Run audit_hubspot_contacts.py first.")
        sys.exit(1)

    with open(AUDIT_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if "language" not in (rows[0].keys() if rows else []):
        print("ERROR: `language` column not found in hubspot_audit.csv.")
        print("Run tag_languages.py first, then re-run this script.")
        sys.exit(1)

    # Filter to enrollable: must have a HubSpot contact ID and not be excluded
    enrollable = [
        r for r in rows
        if r.get("hubspot_contact_id")
        and r.get("gap") != "not_in_hubspot"
        and r.get("company_name", "") not in EXCLUDED_COMPANIES
    ]
    excluded_count = sum(1 for r in rows if r.get("company_name", "") in EXCLUDED_COMPANIES)
    skipped_count = len(rows) - len(enrollable) - excluded_count

    print(f"Total rows:        {len(rows)}")
    print(f"Excluded (MCT):    {excluded_count}")
    print(f"Skipped (no ID):   {skipped_count}")
    print(f"To enroll:         {len(enrollable)}")
    print()

    # ── Language breakdown before enrolling ───────────────────────────────────
    print("Language breakdown:")
    for owner in ("Alex", "Aya"):
        own = [r for r in enrollable if r.get("cs_owner") == owner]
        for lang in ("EN", "ES", "CA"):
            n = sum(1 for r in own if r.get("language") == lang)
            seq_id = get_sequence_id(owner, lang)
            print(f"  {owner} — {lang}: {n:>3} contacts  →  sequence {seq_id}")
    print()

    enrollable.sort(key=sort_key)

    # Print the enrollment queue for operator review
    print("Enrollment order preview (first 15):")
    print(f"  {'Status':<10}  {'MRR':>6}  {'Owner':<5}  {'Lang':<4}  Company")
    print(f"  {'-'*10}  {'-'*6}  {'-'*5}  {'-'*4}  {'-'*40}")
    for r in enrollable[:15]:
        mrr_str = f"{float(r.get('mrr') or 0):>6.0f}"
        print(
            f"  {r.get('billing_status',''):10}  {mrr_str}  "
            f"{r.get('cs_owner',''):5}  {r.get('language','?'):4}  "
            f"{r.get('company_name','')}"
        )
    if len(enrollable) > 15:
        print(f"  … and {len(enrollable) - 15} more")
    print()

    log_rows = []
    enrolled = 0
    errors   = 0

    for i, row in enumerate(enrollable, 1):
        owner      = row.get("cs_owner", "")
        language   = row.get("language", "ES")
        company    = row.get("company_name", "")
        contact_id = row.get("hubspot_contact_id", "")

        sequence_id  = get_sequence_id(owner, language)
        sender_email = SENDER_MAP.get(owner, "")
        user_id      = USER_ID_MAP.get(owner, "")

        if not sequence_id or not sender_email:
            print(f"[{i}/{len(enrollable)}] {company} — SKIP (unknown owner={owner!r} or lang={language!r})")
            log_rows.append({
                "company":        company,
                "contact_id":     contact_id,
                "owner":          owner,
                "language":       language,
                "billing_status": row.get("billing_status", ""),
                "mrr":            row.get("mrr", ""),
                "enrolled_at":    "",
                "status":         "skipped",
                "error":          f"no sequence for owner={owner!r} lang={language!r}",
            })
            continue

        status_code, body = enroll(contact_id, sequence_id, sender_email, user_id)
        now = datetime.now(timezone.utc).isoformat()

        if status_code in (200, 201, 204):
            status = "enrolled"
            error  = ""
            enrolled += 1
            print(
                f"[{i}/{len(enrollable)}] {company} "
                f"({owner}/{language}, {row.get('billing_status','')}) → enrolled"
            )
        else:
            status = "error"
            error  = body[:300]
            errors += 1
            print(
                f"[{i}/{len(enrollable)}] {company} ({owner}/{language})"
                f" → ERROR {status_code}: {error[:80]}"
            )

        log_rows.append({
            "company":        company,
            "contact_id":     contact_id,
            "owner":          owner,
            "language":       language,
            "billing_status": row.get("billing_status", ""),
            "mrr":            row.get("mrr", ""),
            "enrolled_at":    now,
            "status":         status,
            "error":          error,
        })

        time.sleep(0.5)   # HubSpot sequences API: ~10 req/s limit, be conservative

    # Write log
    fieldnames = ["company", "contact_id", "owner", "language", "billing_status",
                  "mrr", "enrolled_at", "status", "error"]
    with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(log_rows)

    print()
    print("── Summary ──────────────────────────────────────────────────")
    print(f"  Enrolled:  {enrolled}")
    print(f"  Errors:    {errors}")
    print(f"  Log:       {LOG_FILE}")
    print()
    print("Verify in HubSpot: Sales → Sequences → open each sequence → Enrolled tab")


if __name__ == "__main__":
    main()
