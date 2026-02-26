#!/usr/bin/env python3
"""
build_weekly_scorecard.py

Creates the "Weekly CS Scorecards" Notion database under the CS Ops Hub page
and populates the W09 (Feb 24 – Mar 2) row with auto-computed KPIs.

KPIs auto-computed (from Master Customer Table):
  1. Red Health          – active customers with Health Status == "Red"
  2. No Contact >21d     – active customers with Days Since Last Contact > 21
  4. Churned this week   – customers with Churn Date in [Feb 24, Mar 2]
  5. Graduated this week – customers with Graduation Date in [Feb 24, Mar 2]

KPIs auto-computed (from Intercom):
  3. Avg Reply Time      – median time_to_admin_reply per admin (minutes)

KPIs left blank (manual entry):
  6. Customers Contacted – from calendar / Intercom

Run: python3 build_weekly_scorecard.py
"""

import statistics
import requests
from datetime import date, datetime, timezone

# ── Constants ─────────────────────────────────────────────────────────────────
NOTION_TOKEN   = "***REMOVED***"
MCT_DS_ID      = "3ceb1ad0-91f1-40db-945a-c51c58035898"
CS_OPS_HUB     = "302e418fd8c4818e9235ff950f55a31b"   # parent page for new DB

INTERCOM_TOKEN = "***REMOVED***"
ALEX_ADMIN_ID  = "7484673"   # Alex de Godoy  — discovered Feb 24 2026
AYA_ADMIN_ID   = "8411967"   # Aya Guerimej   — discovered Feb 24 2026

WEEK_LABEL     = "W09 (Feb 24 - Mar 2)"
WEEK_START     = date(2026, 2, 24)
WEEK_END       = date(2026, 3, 2)

NOTION_API     = "https://api.notion.com/v1"
INTERCOM_API   = "https://api.intercom.io"

# ── HTTP headers ──────────────────────────────────────────────────────────────
# Standard Notion API (database creation, page creation)
std_headers = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}

# Multi-source MCT query (requires newer API version)
mct_headers = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2025-09-03",
}

intercom_headers = {
    "Authorization":    f"Bearer {INTERCOM_TOKEN}",
    "Intercom-Version": "2.11",
    "Accept":           "application/json",
    "Content-Type":     "application/json",
}


# ══════════════════════════════════════════════════════════════════════════════
# Step 0 — Fetch Intercom reply times (KPI 3)
# ══════════════════════════════════════════════════════════════════════════════

def _to_unix(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def fetch_reply_times(week_start, week_end):
    """
    Returns {"Alex": {"reply_min": float|None, "convs": int}, "Aya": {...}}.

    Strategy:
      - POST /conversations/search with created_at date range
      - Attribution: statistics.last_closed_by_id (who resolved the conversation)
      - Metric: last_assignment_admin_reply_at - last_assignment_at
                (seconds from last assignment to first reply post-assignment —
                matches Intercom's "Teammate performance" report)
      - Aggregation: median per admin, converted to minutes
      - Also counts conversations closed per admin (for Customers Contacted KPI)
    """
    print("\n[0] Fetching Intercom reply times …")

    url = f"{INTERCOM_API}/conversations/search"
    query = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "open",                     "operator": "=",  "value": False},
                {"field": "statistics.last_close_at", "operator": ">",  "value": _to_unix(week_start)},
                {"field": "statistics.last_close_at", "operator": "<=", "value": _to_unix(week_end)},
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

        r = requests.post(url, headers=intercom_headers, json=query)
        r.raise_for_status()
        data = r.json()
        batch = data.get("conversations", [])
        all_convs.extend(batch)
        print(f"   Page {page}: {len(batch)} conversations (total: {len(all_convs)})")

        pages = data.get("pages", {})
        next_page = pages.get("next", {})
        cursor = next_page.get("starting_after") if isinstance(next_page, dict) else None
        if not cursor or len(batch) == 0:
            break
        page += 1

    print(f"   ✓ {len(all_convs)} conversations fetched")

    # Group reply times by admin
    by_admin = {}   # admin_id -> list of delta seconds
    for c in all_convs:
        stats  = c.get("statistics") or {}
        closer = stats.get("last_closed_by_id")
        if closer is None:
            continue
        closer = str(closer)

        reply_at      = stats.get("last_assignment_admin_reply_at")
        assignment_at = stats.get("last_assignment_at")
        delta = (reply_at - assignment_at) if (reply_at and assignment_at and reply_at > assignment_at) else None
        if delta is not None:
            by_admin.setdefault(closer, []).append(delta)

    result = {}
    for admin_id, label in [(ALEX_ADMIN_ID, "Alex"), (AYA_ADMIN_ID, "Aya")]:
        times = by_admin.get(admin_id, [])
        if times:
            med_min = round(statistics.median(times) / 60, 1)
            print(f"   {label}: median={med_min} min (n={len(times)})")
            result[label] = {"reply_min": med_min}
        else:
            print(f"   {label}: no reply data")
            result[label] = {"reply_min": None}

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Step 1 — Create the scorecard database
# ══════════════════════════════════════════════════════════════════════════════

def create_database():
    print("\n[1] Creating 'Weekly CS Scorecards' database …")

    body = {
        "parent": {"type": "page_id", "page_id": CS_OPS_HUB},
        "title": [{"type": "text", "text": {"content": "📊 Weekly CS Scorecards"}}],
        "properties": {
            "Week":                       {"title": {}},
            "Week Start":                 {"date": {}},
            "Alex: Red Health":           {"number": {}},
            "Aya: Red Health":            {"number": {}},
            "Alex: No Contact >21d":      {"number": {}},
            "Aya: No Contact >21d":       {"number": {}},
            "Alex: Avg Reply Time":       {"number": {}},
            "Aya: Avg Reply Time":        {"number": {}},
            "Alex: Churned":              {"number": {}},
            "Aya: Churned":               {"number": {}},
            "Alex: Graduated":            {"number": {}},
            "Aya: Graduated":             {"number": {}},
            "Alex: Customers Contacted":  {"number": {}},
            "Aya: Customers Contacted":   {"number": {}},
            "Notes":                      {"rich_text": {}},
        },
    }

    r = requests.post(f"{NOTION_API}/databases", headers=std_headers, json=body)
    r.raise_for_status()
    db = r.json()
    db_id = db["id"]
    print(f"   ✓ Database created: {db_id}")
    return db_id


# ══════════════════════════════════════════════════════════════════════════════
# Step 2 — Query all MCT rows (paginated)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_all_mct_rows():
    print("\n[2] Fetching all MCT rows via data_sources API …")

    url = f"{NOTION_API}/data_sources/{MCT_DS_ID}/query"
    all_results = []
    cursor = None
    page_num = 0

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
        print(f"   Page {page_num}: {len(batch)} rows (total so far: {len(all_results)})")

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    print(f"   ✓ Total rows fetched: {len(all_results)}")
    return all_results


# ══════════════════════════════════════════════════════════════════════════════
# Step 3 — Compute KPIs
# ══════════════════════════════════════════════════════════════════════════════

def parse_date(date_str):
    """Return a date object from a YYYY-MM-DD string, or None."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def in_week(d):
    """Return True if the date falls within the scorecard week."""
    return d is not None and WEEK_START <= d <= WEEK_END


def safe_str(prop, path):
    """Safely navigate a nested dict path (list of keys), return str or None."""
    obj = prop
    for key in path:
        if obj is None or not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj if isinstance(obj, str) else None


def safe_num(prop, path):
    """Safely navigate a nested dict path, return number or None."""
    obj = prop
    for key in path:
        if obj is None or not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj if isinstance(obj, (int, float)) else None


def compute_kpis(rows, reply_times=None):
    print("\n[3] Computing KPIs …")

    reply_times = reply_times or {}
    kpis = {
        "Alex": {"red_health": 0, "no_contact": 0, "churned": 0, "graduated": 0,
                 "avg_reply_time": reply_times.get("Alex", {}).get("reply_min")},
        "Aya":  {"red_health": 0, "no_contact": 0, "churned": 0, "graduated": 0,
                 "avg_reply_time": reply_times.get("Aya", {}).get("reply_min")},
    }
    skipped = 0
    owner_counts = {}

    for row in rows:
        props = row.get("properties", {})

        # CS Owner
        owner_prop = props.get("⭐ CS Owner", {})
        owner = safe_str(owner_prop, ["select", "name"])
        owner_counts[owner] = owner_counts.get(owner, 0) + 1

        if owner not in ("Alex", "Aya"):
            skipped += 1
            continue

        # Health Status (formula → string, may include emoji prefix e.g. "🔴 Red")
        health_status = safe_str(props.get("🚦 Health Status", {}), ["formula", "string"]) or ""

        # Billing Status (select field, no emoji, use to detect churned/inactive)
        billing_status = safe_str(props.get("💰 Billing Status", {}), ["select", "name"]) or ""

        # Days Since Last Contact (formula → number)
        days_no_contact = safe_num(props.get("📞 Days Since Last Contact", {}), ["formula", "number"])

        # Churn Date
        churn_date_str = safe_str(props.get("😢 Churn Date", {}), ["date", "start"])

        # Graduation Date
        grad_date_str = safe_str(props.get("🚀 Graduation Date", {}), ["date", "start"])

        is_churned_stage = billing_status != "Active"

        # KPI 1: Red Health (active customers only)
        if health_status and "Red" in health_status and not is_churned_stage:
            kpis[owner]["red_health"] += 1

        # KPI 2: No Contact >21d (active customers only)
        if days_no_contact is not None and days_no_contact > 21 and not is_churned_stage:
            kpis[owner]["no_contact"] += 1

        # KPI 4: Churned this week
        churn_date = parse_date(churn_date_str)
        if in_week(churn_date):
            kpis[owner]["churned"] += 1

        # KPI 5: Graduated this week
        grad_date = parse_date(grad_date_str)
        if in_week(grad_date):
            kpis[owner]["graduated"] += 1

    print(f"   Owner breakdown (all rows): {owner_counts}")
    print(f"   Skipped (no Alex/Aya owner): {skipped}")
    print(f"\n   KPIs:")
    for mgr, vals in kpis.items():
        print(f"   {mgr}: red_health={vals['red_health']}, no_contact={vals['no_contact']}, "
              f"churned={vals['churned']}, graduated={vals['graduated']}")

    return kpis


# ══════════════════════════════════════════════════════════════════════════════
# Step 4 — Create scorecard row
# ══════════════════════════════════════════════════════════════════════════════

def create_scorecard_row(db_id, kpis):
    print(f"\n[4] Creating scorecard row '{WEEK_LABEL}' …")

    body = {
        "parent": {"database_id": db_id},
        "properties": {
            "Week": {
                "title": [{"text": {"content": WEEK_LABEL}}]
            },
            "Week Start": {
                "date": {"start": str(WEEK_START)}
            },
            "Alex: Red Health":       {"number": kpis["Alex"]["red_health"]},
            "Aya: Red Health":        {"number": kpis["Aya"]["red_health"]},
            "Alex: No Contact >21d":  {"number": kpis["Alex"]["no_contact"]},
            "Aya: No Contact >21d":   {"number": kpis["Aya"]["no_contact"]},
            "Alex: Churned":          {"number": kpis["Alex"]["churned"]},
            "Aya: Churned":           {"number": kpis["Aya"]["churned"]},
            "Alex: Graduated":        {"number": kpis["Alex"]["graduated"]},
            "Aya: Graduated":         {"number": kpis["Aya"]["graduated"]},
            "Notes": {
                "rich_text": [{"text": {"content": "First scorecard."}}]
            },
            **({
                "Alex: Median Reply Time": {"number": kpis["Alex"]["avg_reply_time"]},
            } if kpis["Alex"]["avg_reply_time"] is not None else {}),
            **({
                "Aya: Median Reply Time": {"number": kpis["Aya"]["avg_reply_time"]},
            } if kpis["Aya"]["avg_reply_time"] is not None else {}),
        },
    }

    r = requests.post(f"{NOTION_API}/pages", headers=std_headers, json=body)
    r.raise_for_status()
    page = r.json()
    page_id = page["id"]
    print(f"   ✓ Scorecard row created: {page_id}")
    return page_id


# ══════════════════════════════════════════════════════════════════════════════
# Step 5 — Print results
# ══════════════════════════════════════════════════════════════════════════════

def update_existing_row(page_id, kpis):
    print(f"\n[4b] Patching existing scorecard row {page_id} …")
    props = {
        "Alex: Red Health":      {"number": kpis["Alex"]["red_health"]},
        "Aya: Red Health":       {"number": kpis["Aya"]["red_health"]},
        "Alex: No Contact >21d": {"number": kpis["Alex"]["no_contact"]},
        "Aya: No Contact >21d":  {"number": kpis["Aya"]["no_contact"]},
        "Alex: Churned":         {"number": kpis["Alex"]["churned"]},
        "Aya: Churned":          {"number": kpis["Aya"]["churned"]},
        "Alex: Graduated":       {"number": kpis["Alex"]["graduated"]},
        "Aya: Graduated":        {"number": kpis["Aya"]["graduated"]},
    }
    if kpis["Alex"]["avg_reply_time"] is not None:
        props["Alex: Median Reply Time"] = {"number": kpis["Alex"]["avg_reply_time"]}
    if kpis["Aya"]["avg_reply_time"] is not None:
        props["Aya: Median Reply Time"] = {"number": kpis["Aya"]["avg_reply_time"]}

    r = requests.patch(f"{NOTION_API}/pages/{page_id}", headers=std_headers, json={"properties": props})
    r.raise_for_status()
    print(f"   ✓ Row updated")


def print_results(db_id, kpis, page_id):
    page_id_clean = page_id.replace("-", "")

    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)
    if db_id:
        db_id_clean = db_id.replace("-", "")
        print(f"\nDatabase URL:\n  https://www.notion.so/konvoai/{db_id_clean}")
    print(f"\nScorecard row URL:\n  https://www.notion.so/{page_id_clean}")

    print(f"\n{'KPI':<30} {'Alex':>6} {'Aya':>6}")
    print("-" * 44)
    alex_rt = kpis['Alex']['avg_reply_time']
    aya_rt  = kpis['Aya']['avg_reply_time']
    alex_rt_str = f"{alex_rt}m" if alex_rt is not None else "–"
    aya_rt_str  = f"{aya_rt}m"  if aya_rt  is not None else "–"

    print(f"{'Red Health':<30} {kpis['Alex']['red_health']:>6} {kpis['Aya']['red_health']:>6}")
    print(f"{'No Contact >21d':<30} {kpis['Alex']['no_contact']:>6} {kpis['Aya']['no_contact']:>6}")
    print(f"{'Median Reply Time (Intercom)':<30} {alex_rt_str:>6} {aya_rt_str:>6}")
    print(f"{'Churned this week':<30} {kpis['Alex']['churned']:>6} {kpis['Aya']['churned']:>6}")
    print(f"{'Graduated this week':<30} {kpis['Alex']['graduated']:>6} {kpis['Aya']['graduated']:>6}")
    print(f"{'Customers Contacted':<30} {'(n8n)':>6} {'(n8n)':>6}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

EXISTING_PAGE_ID = "311e418f-d8c4-81b1-8552-d12c067c1089"

import sys
if __name__ == "__main__":
    if "--fix" in sys.argv:
        reply_times = fetch_reply_times(WEEK_START, WEEK_END)
        rows        = fetch_all_mct_rows()
        kpis        = compute_kpis(rows, reply_times)
        update_existing_row(EXISTING_PAGE_ID, kpis)
        print_results(None, kpis, EXISTING_PAGE_ID)
    else:
        reply_times = fetch_reply_times(WEEK_START, WEEK_END)
        db_id       = create_database()
        rows        = fetch_all_mct_rows()
        kpis        = compute_kpis(rows, reply_times)
        page_id     = create_scorecard_row(db_id, kpis)
        print_results(db_id, kpis, page_id)
