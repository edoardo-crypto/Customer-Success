"""
create_alex_call_tasks.py
--------------------------
Creates 5 call tasks in HubSpot for each of Alex's 92 contacts
(the ones that couldn't be enrolled in sequences due to no connected inbox).

Schedule: Mon–Fri next week (March 9–13 2026), 9:00 AM CET each day.
Alex works through the list each day and skips contacts who already booked.

For each contact: 5 tasks created, one per day, each associated to the
HubSpot contact record. Phone number included in task body.

Usage:
    python3 create_alex_call_tasks.py           # dry-run
    python3 create_alex_call_tasks.py --create  # actually create tasks
"""

import csv
import sys
import time
import requests
from datetime import datetime, timezone
from pathlib import Path
import creds

HUBSPOT_TOKEN = creds.get("HUBSPOT_TOKEN")
ALEX_OWNER_ID = "887821747"  # CRM owner ID (different from user ID 71846567)

HS_HDR = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

SCRIPT_DIR = Path(__file__).parent
LOG_FILE   = SCRIPT_DIR / "enrollment_meeting_no_log.csv"

# Mon–Fri next week, 9:00 AM CET (UTC+1)
# 9:00 CET = 08:00 UTC
CALL_DAYS = [
    datetime(2026, 3,  9, 8, 0, 0, tzinfo=timezone.utc),  # Mon
    datetime(2026, 3, 10, 8, 0, 0, tzinfo=timezone.utc),  # Tue
    datetime(2026, 3, 11, 8, 0, 0, tzinfo=timezone.utc),  # Wed
    datetime(2026, 3, 12, 8, 0, 0, tzinfo=timezone.utc),  # Thu
    datetime(2026, 3, 13, 8, 0, 0, tzinfo=timezone.utc),  # Fri
]

DAY_LABELS = ["Day 1/5 — Mon", "Day 2/5 — Tue", "Day 3/5 — Wed",
              "Day 4/5 — Thu", "Day 5/5 — Fri"]


# ── Load Alex's contacts from enrollment log ──────────────────────────────────

def load_alex_contacts():
    """Return list of {company, role, contact_id, email, billing_status, mrr}
    for Alex contacts that failed with no_inbox error."""
    rows = list(csv.DictReader(open(LOG_FILE)))
    contacts = []
    seen = set()
    for r in rows:
        if r["cs_owner"] != "Alex":
            continue
        if "no connected" not in r.get("error", ""):
            continue
        cid = r["contact_id"]
        if cid in seen:
            continue
        seen.add(cid)
        contacts.append({
            "company":        r["company"],
            "role":           r["role"],
            "contact_id":     cid,
            "email":          r["email"],
            "billing_status": r["billing_status"],
            "mrr":            r["mrr"],
        })
    return contacts


# ── Batch-read phone numbers from HubSpot ────────────────────────────────────

def fetch_phones(contact_ids):
    """Returns {contact_id: phone_string} for contacts that have a phone."""
    result = {}
    ids = list(contact_ids)
    for i in range(0, len(ids), 100):
        chunk = ids[i:i+100]
        body = {
            "inputs": [{"id": cid} for cid in chunk],
            "properties": ["phone", "mobilephone", "firstname", "lastname"],
        }
        r = requests.post(
            "https://api.hubapi.com/crm/v3/objects/contacts/batch/read",
            headers=HS_HDR, json=body,
        )
        if r.ok:
            for c in r.json().get("results", []):
                cid   = c["id"]
                props = c.get("properties", {})
                phone = (props.get("phone") or props.get("mobilephone") or "").strip()
                fname = (props.get("firstname") or "").strip()
                lname = (props.get("lastname") or "").strip()
                result[cid] = {
                    "phone": phone,
                    "name":  f"{fname} {lname}".strip() or "",
                }
        time.sleep(0.2)
    return result


# ── Create one HubSpot task ───────────────────────────────────────────────────

def create_task(subject, body_text, due_ts_ms, contact_id):
    """Create a CALL task in HubSpot assigned to Alex, associated to contact."""
    payload = {
        "properties": {
            "hs_task_subject":   subject,
            "hs_task_body":      body_text,
            "hs_task_type":      "CALL",
            "hs_timestamp":      str(due_ts_ms),
            "hubspot_owner_id":  ALEX_OWNER_ID,
            "hs_task_priority":  "HIGH",
        },
        "associations": [
            {
                "to":    {"id": contact_id},
                "types": [{"associationCategory": "HUBSPOT_DEFINED",
                           "associationTypeId": 204}],
            }
        ],
    }
    r = requests.post(
        "https://api.hubapi.com/crm/v3/objects/tasks",
        headers=HS_HDR, json=payload,
    )
    return r.status_code, r.json().get("id", "") if r.ok else r.text[:200]


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    dry_run = "--create" not in sys.argv

    print("=== Alex call tasks — 5 days next week ===\n")
    if dry_run:
        print("  DRY-RUN — pass --create to actually create tasks\n")

    # 1. Load contacts
    contacts = load_alex_contacts()
    print(f"  Alex contacts to call: {len(contacts)}")

    # De-duplicate by contact_id (Live Out Solutions appears twice in MCT)
    seen = {}
    for c in contacts:
        key = c["contact_id"]
        if key not in seen:
            seen[key] = c
    contacts = list(seen.values())
    print(f"  After dedup:           {len(contacts)}\n")

    # Sort: Churning first, then MRR desc
    contacts.sort(key=lambda x: (
        {"Churning": 0, "Active": 1}.get(x["billing_status"], 2),
        -float(x["mrr"] or 0),
    ))

    # 2. Fetch phone numbers
    print("  Fetching phone numbers from HubSpot...")
    hs_info = fetch_phones([c["contact_id"] for c in contacts])
    phones_found = sum(1 for v in hs_info.values() if v["phone"])
    print(f"  Phones found: {phones_found}/{len(contacts)}\n")

    # 3. Preview
    total_tasks = len(contacts) * 5
    print(f"  Tasks to create: {total_tasks}  ({len(contacts)} contacts × 5 days)\n")
    print(f"  Schedule:")
    for label, day in zip(DAY_LABELS, CALL_DAYS):
        print(f"    {label}  →  {day.strftime('%a %b %d, 9:00 CET')}")

    print(f"\n  {'#':<4} {'Status':<10} {'MRR':>6}  {'Role':<5} Company  /  Email  /  Phone")
    print(f"  {'-'*4} {'-'*10} {'-'*6}  {'-'*5} {'-'*65}")
    for i, c in enumerate(contacts, 1):
        info  = hs_info.get(c["contact_id"], {})
        phone = info.get("phone") or "—"
        print(f"  {i:<4} {c['billing_status']:<10} {float(c['mrr'] or 0):>6.0f}  "
              f"{c['role']:<5} {c['company'][:35]}  {c['email'][:35]}  {phone}")

    if dry_run:
        print(f"\n  [DRY RUN] Pass --create to create {total_tasks} tasks.")
        return

    # 4. Create tasks
    print(f"\n{'='*80}")
    print(f"  Creating {total_tasks} call tasks...")
    print(f"{'='*80}")

    created = 0
    errors  = 0

    for c in contacts:
        info  = hs_info.get(c["contact_id"], {})
        phone = info.get("phone") or "—"
        name  = info.get("name") or c["email"]

        for day_idx, (label, due_dt) in enumerate(zip(DAY_LABELS, CALL_DAYS)):
            due_ms  = int(due_dt.timestamp() * 1000)
            subject = f"Call {c['company']} [{c['role']}] — {label}"
            body    = (
                f"Company: {c['company']}\n"
                f"Contact: {name} ({c['role']})\n"
                f"Email: {c['email']}\n"
                f"Phone: {phone}\n"
                f"Status: {c['billing_status']} | MRR: €{c['mrr']}\n"
                f"Note: Skip if already booked a meeting."
            )

            status_code, result = create_task(subject, body, due_ms, c["contact_id"])

            if isinstance(result, str) or status_code not in (200, 201):
                errors += 1
                print(f"  ERROR [{c['company']} {label}]: {status_code} {result[:80]}")
            else:
                created += 1

            time.sleep(0.15)

        print(f"  ✓ {c['company']} [{c['role']}]  {phone}  — 5 tasks created")

    print(f"\n  ── Summary ────────────────────────")
    print(f"  Tasks created: {created}")
    print(f"  Errors:        {errors}")
    print(f"\n  View in HubSpot → Sales → Tasks (filter: owner = Alex, type = Call)")


if __name__ == "__main__":
    main()
