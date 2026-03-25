#!/usr/bin/env python3
"""
fetch_cs_monitor_data.py — Fetches data for CS Team Monitor dashboard.

Queries Notion (MCT + Issues Table) and Intercom to produce cs_monitor_data.json.
Manages point-in-time sentiment snapshots in cs_monitor_snapshots.json.

Run: python3 cs_monitor/fetch_cs_monitor_data.py
"""
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

import requests

# Add parent dir for creds.py
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import creds

# ── Credentials ─────────────────────────────────────────────────────────────

NOTION_TOKEN = creds.get("NOTION_TOKEN")
INTERCOM_TOKEN = creds.get("INTERCOM_TOKEN")

# ── IDs ─────────────────────────────────────────────────────────────────────

MCT_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"
ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"

NOTION_API = "https://api.notion.com/v1"
INTERCOM_API = "https://api.intercom.io"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "cs_monitor_data.json")
SNAPSHOTS_FILE = os.path.join(SCRIPT_DIR, "cs_monitor_snapshots.json")

# ── Headers ─────────────────────────────────────────────────────────────────

mct_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2025-09-03",
}
std_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}
intercom_headers = {
    "Authorization": f"Bearer {INTERCOM_TOKEN}",
    "Intercom-Version": "2.11",
    "Accept": "application/json",
    "Content-Type": "application/json",
}


# ═══════════════════════════════════════════════════════════════════════════
# Week computation
# ═══════════════════════════════════════════════════════════════════════════

def compute_weeks():
    """Compute rolling 5-week window (Mon–Sun) from today's date."""
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    weeks = []
    for i in range(4, -1, -1):
        w_start = monday - timedelta(weeks=i)
        w_end = w_start + timedelta(days=6)
        weeks.append({"start": w_start, "end": w_end})
    return weeks


def format_week_label(w_start, w_end):
    """Format week label like 'Feb 16–22' or 'Feb 23–Mar 1'."""
    if w_start.month == w_end.month:
        return f"{w_start.strftime('%b')} {w_start.day}\u2013{w_end.day}"
    else:
        return f"{w_start.strftime('%b')} {w_start.day}\u2013{w_end.strftime('%b')} {w_end.day}"


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _str(prop, *keys):
    """Safely traverse nested dict keys; return str or None."""
    obj = prop
    for k in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(k)
    return obj if isinstance(obj, str) else None


def _parse_date(s):
    """Parse ISO date string to date object."""
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def to_unix(d):
    """Convert date to UTC unix timestamp (start of day)."""
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


# ═══════════════════════════════════════════════════════════════════════════
# Step 1 — Fetch all MCT rows (shared across slides)
# ═══════════════════════════════════════════════════════════════════════════

def fetch_all_mct_rows():
    """Paginated query of MCT via data_sources API."""
    print("\n[1] Fetching MCT rows …")
    url = f"{NOTION_API}/data_sources/{MCT_DS_ID}/query"
    all_results, cursor, page_num = [], None, 0

    while True:
        page_num += 1
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        r = requests.post(url, headers=mct_headers, json=body)
        r.raise_for_status()
        data = r.json()
        batch = data.get("results", [])
        all_results.extend(batch)
        print(f"   Page {page_num}: {len(batch)} rows (total: {len(all_results)})")
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    print(f"   \u2713 {len(all_results)} total rows")
    return all_results


def count_active_customers(mct_rows):
    """Count customers with Billing Status = Active or Churning."""
    count = 0
    for row in mct_rows:
        props = row.get("properties", {})
        billing = _str(props.get("\U0001f4b0 Billing Status", {}), "select", "name") or ""
        if billing in ("Active", "Churning"):
            count += 1
    return count


# ═══════════════════════════════════════════════════════════════════════════
# Step 2 — Slide 1: Config Issues per Customer
# ═══════════════════════════════════════════════════════════════════════════

def fetch_config_issues(weeks):
    """Fetch config issues from Issues Table, bucket by week."""
    print("\n[2] Fetching config issues …")
    url = f"{NOTION_API}/databases/{ISSUES_DB}/query"

    earliest = weeks[0]["start"]
    latest = weeks[-1]["end"]

    all_issues = []
    cursor = None
    while True:
        body = {
            "page_size": 100,
            "filter": {
                "and": [
                    {"property": "Issue Type", "select": {"equals": "Config Issue"}},
                    {"property": "Created At", "date": {"on_or_after": str(earliest)}},
                    {"property": "Created At", "date": {"on_or_before": str(latest)}},
                ]
            },
        }
        if cursor:
            body["start_cursor"] = cursor
        r = requests.post(url, headers=std_headers, json=body)
        r.raise_for_status()
        data = r.json()
        batch = data.get("results", [])
        all_issues.extend(batch)
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    print(f"   \u2713 {len(all_issues)} config issues fetched")

    # Bucket by week
    counts = [0] * len(weeks)
    for issue in all_issues:
        props = issue.get("properties", {})
        created = _parse_date(_str(props.get("Created At", {}), "date", "start"))
        if not created:
            continue
        for i, w in enumerate(weeks):
            if w["start"] <= created <= w["end"]:
                counts[i] += 1
                break

    print(f"   Bucketed: {counts}")
    return counts


# ═══════════════════════════════════════════════════════════════════════════
# Step 3 — Slide 2: Sentiment (point-in-time snapshot)
# ═══════════════════════════════════════════════════════════════════════════

def compute_sentiment(mct_rows):
    """Count Great / At Risk by CS Owner from current MCT (Active + Churning only)."""
    print("\n[3] Computing sentiment counts …")
    counts = {"great_alex": 0, "great_aya": 0, "risk_alex": 0, "risk_aya": 0}

    for row in mct_rows:
        props = row.get("properties", {})
        billing = _str(props.get("\U0001f4b0 Billing Status", {}), "select", "name") or ""
        if billing not in ("Active", "Churning"):
            continue
        owner = _str(props.get("\u2b50 CS Owner", {}), "select", "name")
        sentiment = _str(props.get("\U0001f9e0 CS Sentiment", {}), "select", "name") or ""

        if owner == "Alex":
            if sentiment == "Great":
                counts["great_alex"] += 1
            elif sentiment == "At Risk":
                counts["risk_alex"] += 1
        elif owner == "Aya":
            if sentiment == "Great":
                counts["great_aya"] += 1
            elif sentiment == "At Risk":
                counts["risk_aya"] += 1

    print(f"   Great: Alex={counts['great_alex']}, Aya={counts['great_aya']}")
    print(f"   Risk:  Alex={counts['risk_alex']}, Aya={counts['risk_aya']}")
    return counts


def manage_snapshots(weeks, current_sentiment):
    """Load/save sentiment snapshots. Returns arrays for each series."""
    print("\n[4] Managing sentiment snapshots …")

    # Load existing snapshots
    snapshots = {}
    if os.path.exists(SNAPSHOTS_FILE):
        with open(SNAPSHOTS_FILE) as f:
            snapshots = json.load(f)

    # Store current week's snapshot (keyed by Monday ISO date)
    current_monday = str(weeks[-1]["start"])
    snapshots[current_monday] = current_sentiment

    with open(SNAPSHOTS_FILE, "w") as f:
        json.dump(snapshots, f, indent=2)
    print(f"   \u2713 Saved snapshot for {current_monday}")

    # Build arrays from snapshots for each week
    great_alex, great_aya, risk_alex, risk_aya = [], [], [], []
    for w in weeks:
        key = str(w["start"])
        snap = snapshots.get(key)
        if snap:
            great_alex.append(snap.get("great_alex"))
            great_aya.append(snap.get("great_aya"))
            risk_alex.append(snap.get("risk_alex"))
            risk_aya.append(snap.get("risk_aya"))
        else:
            great_alex.append(None)
            great_aya.append(None)
            risk_alex.append(None)
            risk_aya.append(None)

    print(f"   Great Alex: {great_alex}")
    print(f"   Great Aya:  {great_aya}")
    print(f"   Risk Alex:  {risk_alex}")
    print(f"   Risk Aya:   {risk_aya}")
    return great_alex, great_aya, risk_alex, risk_aya


# ═══════════════════════════════════════════════════════════════════════════
# Step 4 — Slide 2: Churn by owner (historically fetchable)
# ═══════════════════════════════════════════════════════════════════════════

def compute_churn_by_owner(mct_rows, weeks):
    """Bucket churns by week and CS Owner using Churning Since date."""
    print("\n[5] Computing churn by owner …")
    churn_alex = [0] * len(weeks)
    churn_aya = [0] * len(weeks)

    for row in mct_rows:
        props = row.get("properties", {})
        churning_since = _parse_date(
            _str(props.get("\U0001f4c5 Churning Since", {}), "date", "start")
        )
        if not churning_since:
            continue
        owner = _str(props.get("\u2b50 CS Owner", {}), "select", "name")

        for i, w in enumerate(weeks):
            if w["start"] <= churning_since <= w["end"]:
                if owner == "Alex":
                    churn_alex[i] += 1
                elif owner == "Aya":
                    churn_aya[i] += 1
                break

    print(f"   Churn Alex: {churn_alex}")
    print(f"   Churn Aya:  {churn_aya}")
    return churn_alex, churn_aya


# ═══════════════════════════════════════════════════════════════════════════
# Step 5 — Slide 3: Intercom Average Reply Time
# ═══════════════════════════════════════════════════════════════════════════

def fetch_intercom_reply_time(weeks):
    """Fetch average reply time per week from Intercom."""
    print("\n[6] Fetching Intercom reply times …")
    avg_reply_min = []

    for w in weeks:
        start_unix = to_unix(w["start"])
        end_unix = to_unix(w["end"] + timedelta(days=1))  # end of Sunday

        query = {
            "query": {
                "operator": "AND",
                "value": [
                    {"field": "created_at", "operator": ">", "value": start_unix},
                    {"field": "created_at", "operator": "<=", "value": end_unix},
                    {"field": "statistics.time_to_admin_reply", "operator": ">", "value": 0},
                ],
            },
            "pagination": {"per_page": 150},
        }

        all_convs = []
        cursor = None
        while True:
            if cursor:
                query["pagination"]["starting_after"] = cursor
            elif "starting_after" in query.get("pagination", {}):
                del query["pagination"]["starting_after"]

            r = requests.post(
                f"{INTERCOM_API}/conversations/search",
                headers=intercom_headers,
                json=query,
            )
            r.raise_for_status()
            data = r.json()
            batch = data.get("conversations", [])
            all_convs.extend(batch)

            next_p = data.get("pages", {}).get("next", {})
            cursor = next_p.get("starting_after") if isinstance(next_p, dict) else None
            if not cursor or not batch:
                break

        # Average time_to_admin_reply (seconds → minutes)
        reply_times = []
        for c in all_convs:
            stats = c.get("statistics") or {}
            t = stats.get("time_to_admin_reply")
            if t and t > 0:
                reply_times.append(t)

        label = format_week_label(w["start"], w["end"])
        if reply_times:
            avg_min = round(sum(reply_times) / len(reply_times) / 60)
            avg_reply_min.append(avg_min)
            print(f"   {label}: {avg_min} min (n={len(reply_times)})")
        else:
            avg_reply_min.append(None)
            print(f"   {label}: no data")

    return avg_reply_min


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  CS Team Monitor \u2014 Data Fetch")
    print("=" * 60)

    weeks = compute_weeks()
    week_labels = [format_week_label(w["start"], w["end"]) for w in weeks]
    print(f"\n  Weeks: {week_labels}")

    # Shared MCT query (used by slides 1, 2)
    mct_rows = fetch_all_mct_rows()
    active_count = count_active_customers(mct_rows)
    print(f"\n   Active/Churning customers: {active_count}")

    # Slide 1: Config Issues per Customer
    config_counts = fetch_config_issues(weeks)
    config_ratios = [
        round(c / active_count, 2) if active_count > 0 else 0.0
        for c in config_counts
    ]
    print(f"   Config issues per customer: {config_ratios}")

    # Slide 2: Sentiment + Churn
    current_sentiment = compute_sentiment(mct_rows)
    great_alex, great_aya, risk_alex, risk_aya = manage_snapshots(weeks, current_sentiment)
    churn_alex, churn_aya = compute_churn_by_owner(mct_rows, weeks)

    # Slide 3: Intercom Reply Time
    avg_reply_min = fetch_intercom_reply_time(weeks)

    # Build output JSON
    output = {
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "week_labels": week_labels,
        "weeks": [{"start": str(w["start"]), "end": str(w["end"])} for w in weeks],
        "active_customer_count": active_count,
        "onboarding": {
            "config_issues_per_customer": config_ratios,
        },
        "checkins": {
            "great_alex": great_alex,
            "great_aya": great_aya,
            "risk_alex": risk_alex,
            "risk_aya": risk_aya,
            "churn_alex": churn_alex,
            "churn_aya": churn_aya,
        },
        "intercom": {
            "avg_reply_min": avg_reply_min,
        },
    }

    with open(DATA_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n\u2705 Data written to {DATA_FILE}")


if __name__ == "__main__":
    main()
