"""
update_phone_dm_oper.py
-----------------------
Reads phone_gaps.json and writes HubSpot phone numbers back to the MCT.

  MISSING  → fills an empty MCT phone field
  DIFFERENT → overwrites MCT value with HubSpot's (fresher source)

Bad-phone guard: skips entries where the phone, after stripping non-digits,
has fewer than 7 digits or is "0".

Usage:
    python3 update_phone_dm_oper.py [--dry-run]
"""

import re
import sys
import time
import json
import requests
import creds

# ── Credentials ──────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
NOTION_VER   = "2025-09-03"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VER,
    "Content-Type": "application/json",
}

GAPS_FILE = "/Users/edoardopelli/projects/Customer Success/phone_gaps.json"

FIELD_MAP = {
    "DM":   "DM - Phone Number",
    "Oper": "Oper - Phone Number",
}


def is_valid_phone(phone_str):
    """Return True if phone has at least 7 digits and is not literally '0'."""
    digits = re.sub(r"\D", "", str(phone_str))
    if len(digits) < 7:
        return False
    if digits == "0":
        return False
    return True


def patch_phone(page_id, field_name, phone_value):
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=HEADERS,
        json={"properties": {field_name: {"phone_number": phone_value}}},
    )
    return r.status_code


def main():
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("DRY RUN — no Notion PATCHes will be made\n")

    with open(GAPS_FILE, encoding="utf-8") as f:
        gaps = json.load(f)

    print(f"Loaded {len(gaps)} gaps from phone_gaps.json\n")

    updated = skipped_bad = errors = 0
    different_log = []

    for g in gaps:
        company   = g["company"]
        page_id   = g["page_id"]
        role      = g["role"]
        hs_phone  = g["hs_phone"]
        mct_phone = g["mct_phone"]
        status    = g["status"]
        field     = FIELD_MAP[role]

        # Guard: skip phones that look like placeholders
        if not is_valid_phone(hs_phone):
            print(f"  [SKIP-BAD]  {company} / {role}  phone={repr(hs_phone)}")
            skipped_bad += 1
            continue

        label = f"{company} / {role}"

        if status == "DIFFERENT":
            different_log.append(
                f"  {company[:42]:<44} {role:<5}  {mct_phone}  →  {hs_phone}"
            )

        if dry_run:
            if status == "MISSING":
                print(f"  [DRY-MISS]  {label}  ← {hs_phone}  ({g['hs_name']})")
            else:
                print(f"  [DRY-DIFF]  {label}  OLD={mct_phone}  NEW={hs_phone}")
            updated += 1
            continue

        http_status = patch_phone(page_id, field, hs_phone)
        if http_status == 200:
            if status == "MISSING":
                print(f"  [OK]   {label}  ← {hs_phone}")
            else:
                print(f"  [OK]   {label}  OLD={mct_phone}  →  {hs_phone}")
            updated += 1
        else:
            print(f"  [ERR]  {label}  HTTP {http_status}")
            errors += 1

        time.sleep(0.35)

    print(f"\n{'='*70}")
    print(f"Done: {updated} written, {skipped_bad} skipped (bad phone), {errors} errors")

    if different_log:
        print(f"\nOVERWRITES ({len(different_log)} DIFFERENT cases):")
        for line in different_log:
            print(line)


if __name__ == "__main__":
    main()
