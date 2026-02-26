#!/usr/bin/env python3
"""
fetch_intercom_reply_time.py — explore Intercom reply time data for scorecard KPI 3

Uses the Intercom Search API (POST /conversations/search) to filter conversations
by created_at date range, then computes the MEDIAN time from assignment to first
admin response per teammate — matching Intercom's "Teammate performance" report.

Attribution: `last_closed_by_id` (the admin who closed/resolved the conversation).
Metric fields:
  statistics.last_assignment_at              — unix ts when admin was last assigned
  statistics.last_assignment_admin_reply_at  — unix ts when that admin first replied
                                               after being assigned
Delta: last_assignment_admin_reply_at - last_assignment_at  (seconds)
Only included when both are non-null and delta > 0.

Also reports: conversation count per admin (# closed by that admin this week).

Admin IDs (konvoai workspace, discovered Feb 24 2026):
  Alex de Godoy : 7484673
  Aya Guerimej  : 8411967

Run:
    python3 fetch_intercom_reply_time.py
    python3 fetch_intercom_reply_time.py --prev   # check previous week
"""
import sys
import requests
import statistics
from datetime import datetime, timezone, date

INTERCOM_TOKEN = "***REMOVED***"

# Admin IDs (discovered Feb 24 2026 via GET /admins)
ALEX_ID = "7484673"
AYA_ID  = "8411967"

# Toggle between current and previous week
if "--prev" in sys.argv:
    WEEK_START = date(2026, 2, 17)
    WEEK_END   = date(2026, 2, 24)
    LABEL = "W08 (Feb 17 - Feb 23)"
else:
    WEEK_START = date(2026, 2, 24)
    WEEK_END   = date(2026, 3, 2)
    LABEL = "W09 (Feb 24 - Mar 2)"

headers = {
    "Authorization":    f"Bearer {INTERCOM_TOKEN}",
    "Intercom-Version": "2.11",
    "Accept":           "application/json",
    "Content-Type":     "application/json",
}


def to_unix(d):
    """Convert a date to UTC unix timestamp (start of that day)."""
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


# ── Step 1: list all admins ────────────────────────────────────────────────────

def get_admins():
    r = requests.get("https://api.intercom.io/admins", headers=headers)
    r.raise_for_status()
    admins = r.json().get("admins", [])
    print("Admins in workspace:")
    for a in admins:
        print(f"  id={a['id']:>12}  name={a['name']:<30}  email={a.get('email', '')}")
    return {str(a["id"]): a["name"] for a in admins}


# ── Step 2: search conversations with date filter ─────────────────────────────

def fetch_conversations_search(week_start, week_end):
    """
    Uses POST /conversations/search which actually respects date filters.
    GET /conversations ignores created_after/created_before in practice.
    """
    url = "https://api.intercom.io/conversations/search"
    query = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "open",                     "operator": "=",  "value": False},
                {"field": "statistics.last_close_at", "operator": ">",  "value": to_unix(week_start)},
                {"field": "statistics.last_close_at", "operator": "<=", "value": to_unix(week_end)},
            ],
        },
        "pagination": {"per_page": 150},
    }

    all_convs = []
    page = 1
    cursor = None

    while True:
        if cursor:
            query["pagination"]["starting_after"] = cursor
        elif "starting_after" in query["pagination"]:
            del query["pagination"]["starting_after"]

        r = requests.post(url, headers=headers, json=query)
        r.raise_for_status()
        data = r.json()

        batch = data.get("conversations", [])
        all_convs.extend(batch)
        print(f"  Page {page}: {len(batch)} conversations (running total: {len(all_convs)})")

        pages = data.get("pages", {})
        next_page = pages.get("next", {})
        cursor = next_page.get("starting_after") if isinstance(next_page, dict) else None

        if not cursor or len(batch) == 0:
            break
        page += 1

    return all_convs


# ── Step 3: compute median per admin ──────────────────────────────────────────

def compute_median_reply(conversations, admin_map):
    """
    Attribution: last_closed_by_id (null for open/snoozed conversations).
    Metric: last_assignment_admin_reply_at - last_assignment_at
            (seconds from last assignment to first admin reply post-assignment).
    Also tracks conversation count per admin.
    """
    by_admin = {}   # admin_id -> list of delta seconds (reply times)
    conv_count = {} # admin_id -> int (# conversations closed by this admin)
    no_closer = 0
    no_stat   = 0

    for c in conversations:
        stats    = c.get("statistics") or {}
        closer   = stats.get("last_closed_by_id")

        if closer is None:
            no_closer += 1
            continue

        closer = str(closer)
        conv_count[closer] = conv_count.get(closer, 0) + 1

        reply_at      = stats.get("last_assignment_admin_reply_at")
        assignment_at = stats.get("last_assignment_at")
        delta = (reply_at - assignment_at) if (reply_at and assignment_at and reply_at > assignment_at) else None

        if delta is None:
            no_stat += 1
            continue

        by_admin.setdefault(closer, []).append(delta)

    print(f"\n  Conversations not yet closed (no closer):              {no_closer}")
    print(f"  Conversations closed but no valid assignment→reply:    {no_stat}")
    print("\nResults — median assignment→reply time + conversation count per admin:")
    print(f"  {'Admin':<30} {'closed':>7}  {'n (reply)':>10}  {'Median (min)':>13}  {'Min (min)':>10}  {'Max (min)':>10}")
    print("  " + "-" * 85)

    result = {}
    for admin_id in sorted(conv_count.keys(), key=lambda x: admin_map.get(x, x)):
        name   = admin_map.get(admin_id, f"[id={admin_id}]")
        closed = conv_count[admin_id]
        times  = by_admin.get(admin_id, [])
        if times:
            med_min = statistics.median(times) / 60
            min_min = min(times) / 60
            max_min = max(times) / 60
            print(f"  {name:<30} {closed:>7}  {len(times):>10}  {med_min:>12.1f}m  {min_min:>9.1f}m  {max_min:>9.1f}m")
            result[name] = {"median_min": round(med_min, 1), "n": len(times),
                            "convs": closed, "admin_id": admin_id}
        else:
            print(f"  {name:<30} {closed:>7}  {'–':>10}  {'–':>13}  {'–':>10}  {'–':>10}")
            result[name] = {"median_min": None, "n": 0, "convs": closed, "admin_id": admin_id}

    return result


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"Period: {LABEL}  ({WEEK_START} → {WEEK_END})")
    print(f"Unix range: {to_unix(WEEK_START)} → {to_unix(WEEK_END)}\n")

    print("[1] Fetching admins …")
    admin_map = get_admins()

    print(f"\n[2] Searching conversations created between {WEEK_START} and {WEEK_END} …")
    convs = fetch_conversations_search(WEEK_START, WEEK_END)
    print(f"    Total conversations fetched: {len(convs)}")

    # Sanity check: date range of fetched conversations
    dates = [c.get("created_at") for c in convs if c.get("created_at")]
    if dates:
        ts_min = datetime.fromtimestamp(min(dates), tz=timezone.utc).date()
        ts_max = datetime.fromtimestamp(max(dates), tz=timezone.utc).date()
        print(f"    Date range in results: {ts_min} → {ts_max}")

    print("\n[3] Computing median reply times …")
    results = compute_median_reply(convs, admin_map)

    print("\n[4] Summary (for scorecard integration):")
    for name, data in results.items():
        print(f"  {name}: median={data['median_min']} min  (n_reply={data['n']}, "
              f"convs_closed={data['convs']}, admin_id={data['admin_id']})")

    print("\n[5] Alex & Aya specifically:")
    for name, aid in [("Alex de Godoy", ALEX_ID), ("Aya Guerimej", AYA_ID)]:
        if name in results:
            d = results[name]
            print(f"  {name}: reply_time={d['median_min']} min (n={d['n']}), "
                  f"convs_closed={d['convs']}")
        else:
            print(f"  {name}: no data (no closed conversations)")
