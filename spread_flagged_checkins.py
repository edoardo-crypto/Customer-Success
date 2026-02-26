#!/usr/bin/env python3
"""
spread_flagged_checkins.py
--------------------------
Takes all active customers currently flagged with Next Scheduled Check-in
= 2026-02-23 (next Monday) and spreads them across three days by importance:

  - Monday   2026-02-23  → top    1/3  (most important by tier)
  - Wednesday 2026-02-25  → middle 1/3
  - Friday   2026-02-27  → bottom 1/3

Tier priority: Tier 1 > Tier 2 > Tier 3 > no tier set.
Within the same tier, customers are sorted alphabetically.

Usage:
  python3 spread_flagged_checkins.py
"""

import json
import math
import urllib.request
from datetime import date

# ── Config ────────────────────────────────────────────────────────────────────

NOTION_TOKEN          = "***REMOVED***"
NOTION_DATA_SOURCE_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

FLAG_DATE   = date(2026, 2, 23)   # currently assigned to all overdue/no-contact
MONDAY      = date(2026, 2, 23)
WEDNESDAY   = date(2026, 2, 25)
FRIDAY      = date(2026, 2, 27)

TIER_PRIORITY = {"Tier 1": 1, "Tier 2": 2, "Tier 3": 3}   # lower = more important


# ── Notion helpers ────────────────────────────────────────────────────────────

def notion_request(method, path, body=None, version="2022-06-28"):
    url  = f"https://api.notion.com/v1/{path}"
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization",  f"Bearer {NOTION_TOKEN}")
    req.add_header("Notion-Version", version)
    req.add_header("Content-Type",   "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        raise Exception(f"HTTP {e.code} {e.reason} — {body_text}") from None


# ── Fetch flagged customers ───────────────────────────────────────────────────

def fetch_flagged_customers():
    """
    Returns all active customers whose Next Scheduled Check-in == FLAG_DATE.
    """
    flagged = []
    cursor  = None

    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        resp = notion_request(
            "POST",
            f"data_sources/{NOTION_DATA_SOURCE_ID}/query",
            body,
            version="2025-09-03",
        )

        for page in resp.get("results", []):
            props = page.get("properties", {})

            # Active only
            billing_val = (
                ((props.get("💰 Billing Status") or {}).get("select") or {})
                .get("name", "")
            )
            if billing_val != "Active":
                continue

            # Check Next Scheduled Check-in == FLAG_DATE
            checkin_raw = (
                ((props.get("📞 Next Scheduled Check-in") or {}).get("date") or {})
                .get("start", None)
            )
            if not checkin_raw or checkin_raw[:10] != FLAG_DATE.isoformat():
                continue

            # Company Name
            name_parts = (props.get("🏢 Company Name") or {}).get("title", [])
            name = "".join(t.get("plain_text", "") for t in name_parts).strip()
            if not name:
                continue

            # Tier
            tier_val = (
                ((props.get("🏅 Tier") or {}).get("select") or {})
                .get("name", None)
            )

            flagged.append({
                "page_id": page["id"],
                "name":    name,
                "tier":    tier_val,
            })

        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break

    return flagged


# ── Split into thirds ─────────────────────────────────────────────────────────

def assign_days(customers):
    """
    Sort by tier priority then name, split into thirds:
      first  third → Monday
      second third → Wednesday
      last   third → Friday
    """
    sorted_customers = sorted(
        customers,
        key=lambda c: (TIER_PRIORITY.get(c["tier"], 4), c["name"].lower())
    )

    n       = len(sorted_customers)
    # Ceiling division so Monday gets any remainder (most important gets priority)
    size_mon = math.ceil(n / 3)
    size_wed = math.ceil((n - size_mon) / 2)
    size_fri = n - size_mon - size_wed

    assignments = []
    for i, c in enumerate(sorted_customers):
        if i < size_mon:
            day = MONDAY
        elif i < size_mon + size_wed:
            day = WEDNESDAY
        else:
            day = FRIDAY
        assignments.append({**c, "assigned_day": day})

    return assignments


# ── PATCH Notion ──────────────────────────────────────────────────────────────

def update_next_checkin(page_id, next_date):
    body = {"properties": {"📞 Next Scheduled Check-in": {"date": {"start": next_date.isoformat()}}}}
    notion_request("PATCH", f"pages/{page_id}", body)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 72)
    print("  Spread flagged check-ins across Mon / Wed / Fri")
    print("=" * 72)

    print(f"\n[1/3] Fetching customers flagged on {FLAG_DATE}...")
    flagged = fetch_flagged_customers()
    print(f"  Found {len(flagged)} flagged customers")

    print("\n[2/3] Assigning days by tier priority...")
    rows = assign_days(flagged)
    mon_count = sum(1 for r in rows if r["assigned_day"] == MONDAY)
    wed_count = sum(1 for r in rows if r["assigned_day"] == WEDNESDAY)
    fri_count = sum(1 for r in rows if r["assigned_day"] == FRIDAY)
    print(f"  Monday {MONDAY}: {mon_count}  |  Wednesday {WEDNESDAY}: {wed_count}  |  Friday {FRIDAY}: {fri_count}")

    print("\n[3/3] Updating Notion...")
    print()

    hdr = f"  {'Customer':<35} {'Tier':<8} {'Assigned Day'}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    updated = errors = 0
    for r in rows:
        tier_str = r["tier"] or "(none)"
        day_label = {MONDAY: "Mon 23-Feb", WEDNESDAY: "Wed 25-Feb", FRIDAY: "Fri 27-Feb"}[r["assigned_day"]]
        print(f"  {r['name']:<35} {tier_str:<8} {day_label}")
        try:
            update_next_checkin(r["page_id"], r["assigned_day"])
            updated += 1
        except Exception as e:
            print(f"    ✗ Error updating {r['name']}: {e}")
            errors += 1

    print()
    print("=" * 72)
    print(f"  Done.  Updated: {updated}  |  Errors: {errors}")
    print(f"  Mon {MONDAY}: {mon_count}  |  Wed {WEDNESDAY}: {wed_count}  |  Fri {FRIDAY}: {fri_count}")
    print("=" * 72)
    print()


if __name__ == "__main__":
    main()
