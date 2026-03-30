#!/usr/bin/env python3
"""
fetch_ceo_dashboard_data.py -- Fetches data for CEO CS Metrics Dashboard.

Queries Notion MCT + Stripe to produce ceo_dashboard_data.json.
Churn data comes from Stripe (source of truth), not MCT.
Manages point-in-time snapshots in ceo_dashboard_snapshots.json.

Run: python3 ceo_dashboard/fetch_ceo_dashboard_data.py
"""
import csv
import io
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import creds

# ── Credentials ────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
STRIPE_KEY = creds.get("STRIPE_KEY")

# ── IDs ────────────────────────────────────────────────────────────────────
MCT_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"
NOTION_API = "https://api.notion.com/v1"
STRIPE_API = "https://api.stripe.com/v1"

SHEET_ID = "1C9Y5e6Rz9L24EtczXGMvapkL1ENu23SD"
SHEET_GID_STRIPE = "1127966823"   # "Client List - Stripe" tab
SHEET_GID_AI     = "279628241"    # "Client List - AI Sessions" tab

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "ceo_dashboard_data.json")
SNAPSHOTS_FILE = os.path.join(SCRIPT_DIR, "ceo_dashboard_snapshots.json")

mct_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2025-09-03",
}


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _str(prop, *keys):
    obj = prop
    for k in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(k)
    return obj if isinstance(obj, str) else None


def _num(prop, *keys):
    obj = prop
    for k in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(k)
    return obj if isinstance(obj, (int, float)) else None


def _parse_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def compute_weeks(n=8):
    """Compute rolling n-week window (Mon-Sun) from today."""
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    weeks = []
    for i in range(n - 1, -1, -1):
        w_start = monday - timedelta(weeks=i)
        w_end = w_start + timedelta(days=6)
        weeks.append({"start": w_start, "end": w_end})
    return weeks


def format_week_label(w_start, w_end):
    if w_start.month == w_end.month:
        return f"{w_start.strftime('%b')} {w_start.day}\u2013{w_end.day}"
    else:
        return f"{w_start.strftime('%b')} {w_start.day}\u2013{w_end.strftime('%b')} {w_end.day}"


# ═══════════════════════════════════════════════════════════════════════════
# Google Sheet MRR — source of truth for all MRR values
# ═══════════════════════════════════════════════════════════════════════════

def _parse_euro(val):
    """Parse a value like '1,234' or '€1,234.56' to float. Returns 0 for empty/invalid."""
    if not val or val.strip() == "":
        return 0.0
    val = val.strip().replace("€", "").replace(",", "").replace('"', '')
    try:
        return float(val)
    except ValueError:
        return 0.0


def _fetch_sheet_tab(gid):
    """Fetch a Google Sheet tab as CSV and return list of rows."""
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    reader = csv.reader(io.StringIO(r.text))
    return list(reader)


def _parse_tab_mrr(rows, header_marker):
    """Parse a tab and return {stripe_customer_id: mrr} for the latest month.

    header_marker: text in col A of the header row (e.g. 'MRR by Client' or 'AI Sessions by Client')
    """
    # Find the header row
    header_idx = None
    for i, row in enumerate(rows):
        if row and header_marker in (row[0] or ""):
            header_idx = i
            break
    if header_idx is None:
        print(f"   WARNING: Could not find header row with '{header_marker}'")
        return {}, None

    headers = rows[header_idx]

    # Find the FIRST group of month columns (there may be multiple groups separated by blanks).
    # Use the latest month within the first group.
    month_pattern = re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}$")
    first_month_col = None
    latest_col = None
    latest_label = None
    for col_idx, h in enumerate(headers):
        h = h.strip()
        if month_pattern.match(h):
            if first_month_col is None:
                first_month_col = col_idx
            latest_col = col_idx
            latest_label = h
        elif first_month_col is not None:
            # Hit a non-month column after months started → end of first group
            break

    if latest_col is None:
        print(f"   WARNING: No month columns found in '{header_marker}' tab")
        return {}, None

    print(f"   Latest month column: {latest_label} (col {latest_col})")

    # Build per-customer dict
    mrr_dict = {}
    for row in rows[header_idx + 1:]:
        if not row or not row[0] or not row[0].startswith("cus_"):
            continue
        cust_id = row[0].strip()
        val = row[latest_col] if latest_col < len(row) else ""
        mrr = _parse_euro(val)
        if mrr > 0:
            mrr_dict[cust_id] = mrr

    return mrr_dict, latest_label


def fetch_sheet_mrr():
    """Fetch MRR from Google Sheet (Stripe base + AI Sessions).

    Returns: total_mrr, per_customer_dict {stripe_id: total_mrr}
    """
    print("\n[Sheet] Fetching MRR from Google Sheet \u2026")

    # Stripe base subscription MRR
    print("   Fetching Stripe base MRR tab \u2026")
    stripe_rows = _fetch_sheet_tab(SHEET_GID_STRIPE)
    stripe_mrr, stripe_month = _parse_tab_mrr(stripe_rows, "MRR by Client")
    print(f"   \u2713 {len(stripe_mrr)} customers with Stripe MRR, total \u20ac{sum(stripe_mrr.values()):,.0f}")

    # AI Sessions usage revenue
    print("   Fetching AI Sessions tab \u2026")
    ai_rows = _fetch_sheet_tab(SHEET_GID_AI)
    ai_mrr, ai_month = _parse_tab_mrr(ai_rows, "AI Sessions by Client")
    print(f"   \u2713 {len(ai_mrr)} customers with AI Sessions, total \u20ac{sum(ai_mrr.values()):,.0f}")

    # Merge: per-customer total = stripe + AI
    all_ids = set(stripe_mrr.keys()) | set(ai_mrr.keys())
    merged = {}
    for cid in all_ids:
        merged[cid] = stripe_mrr.get(cid, 0) + ai_mrr.get(cid, 0)

    total_mrr = round(sum(merged.values()), 2)
    print(f"   \u2713 Combined MRR: \u20ac{total_mrr:,.0f} ({len(merged)} customers)")
    print(f"   Months: Stripe={stripe_month}, AI={ai_month}")

    return total_mrr, merged


# ═══════════════════════════════════════════════════════════════════════════
# Step 1 -- Fetch all MCT rows
# ═══════════════════════════════════════════════════════════════════════════

def fetch_all_mct_rows():
    print("\n[1] Fetching MCT rows \u2026")
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


# ═══════════════════════════════════════════════════════════════════════════
# Step 2 -- Customer Overview (point-in-time)
# ═══════════════════════════════════════════════════════════════════════════

def compute_overview(mct_rows, sheet_total_mrr):
    print("\n[2] Computing customer overview \u2026")
    active_count = 0

    for row in mct_rows:
        props = row.get("properties", {})
        billing = _str(props.get("\U0001f4b0 Billing Status", {}), "select", "name") or ""
        if billing in ("Active", "Churning", "Past Due"):
            active_count += 1

    print(f"   Active customers: {active_count} (from MCT)")
    print(f"   Total MRR: \u20ac{sheet_total_mrr:,.0f} (from Google Sheet)")
    return {"active_count": active_count, "total_mrr": sheet_total_mrr}


# ═══════════════════════════════════════════════════════════════════════════
# Step 3 -- Churn Trend from Stripe (source of truth)
# ═══════════════════════════════════════════════════════════════════════════

def _ts_to_date(ts):
    """Convert Unix timestamp to date object."""
    if not ts:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).date()


def fetch_stripe_churns(weeks, mct_customer_ids, mct_mrr_dict):
    """Fetch churn data from Stripe. MRR from MCT (preserves historical values).

    Returns: churn_count[], churn_mrr[], current_week_reasons{}, churn_detail[]
    """
    print("\n[3] Fetching churn data from Stripe \u2026")

    earliest = weeks[0]["start"]
    earliest_ts = int(datetime(earliest.year, earliest.month, earliest.day,
                               tzinfo=timezone.utc).timestamp())

    # Fetch canceled subscriptions (ended)
    print("   Fetching canceled subscriptions \u2026")
    canceled_subs = []
    params = {"status": "canceled", "limit": 100,
              "expand[]": "data.customer"}
    while True:
        r = requests.get(f"{STRIPE_API}/subscriptions", auth=(STRIPE_KEY, ""),
                         params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        batch = data.get("data", [])
        canceled_subs.extend(batch)
        if not data.get("has_more"):
            break
        params["starting_after"] = batch[-1]["id"]
    print(f"   \u2713 {len(canceled_subs)} canceled subscriptions")

    # Fetch active subscriptions that are churning (cancel_at_period_end=true)
    print("   Fetching churning (active + cancel_at_period_end) \u2026")
    active_subs = []
    params = {"status": "active", "limit": 100,
              "expand[]": "data.customer"}
    while True:
        r = requests.get(f"{STRIPE_API}/subscriptions", auth=(STRIPE_KEY, ""),
                         params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        batch = data.get("data", [])
        active_subs.extend(batch)
        if not data.get("has_more"):
            break
        params["starting_after"] = batch[-1]["id"]
    churning_subs = [s for s in active_subs if s.get("cancel_at_period_end")]
    print(f"   \u2713 {len(churning_subs)} churning subscriptions (of {len(active_subs)} active)")

    # Deduplicate: one entry per Stripe Customer ID
    # Most recent canceled_at wins; churning overrides canceled (matches meeting report)
    by_customer = {}

    def _cid(sub):
        c = sub.get("customer") or {}
        return c if isinstance(c, str) else c.get("id", "")

    for sub in canceled_subs:
        cid = _cid(sub)
        if not cid:
            continue
        churn_date = _ts_to_date(sub.get("canceled_at"))
        if not churn_date:
            continue
        existing = by_customer.get(cid)
        if not existing or churn_date > existing[1]:
            by_customer[cid] = (sub, churn_date, "canceled")

    for sub in churning_subs:
        cid = _cid(sub)
        if not cid:
            continue
        churn_date = _ts_to_date(sub.get("canceled_at"))
        if not churn_date:
            continue
        existing = by_customer.get(cid)
        # Churning overrides canceled (re-subscribed), or most recent wins among churning
        if not existing or existing[2] == "canceled" or churn_date > existing[1]:
            by_customer[cid] = (sub, churn_date, "churning")

    churn_count = [0] * len(weeks)
    churn_mrr = [0.0] * len(weeks)
    current_week_reasons = {}
    churn_detail = []

    for cust_id, (sub, churn_date, _type) in by_customer.items():
        # Only include if within our 8-week window
        if churn_date < earliest or churn_date > weeks[-1]["end"]:
            continue

        customer = sub.get("customer") or {}
        if isinstance(customer, str):
            cust_name = cust_id
            cust_email = ""
        else:
            cust_name = customer.get("name") or customer.get("email") or cust_id
            cust_email = customer.get("email") or ""

        # Only count customers tracked in MCT (skip test/duplicate accounts)
        if cust_id not in mct_customer_ids:
            continue

        # MRR from MCT (preserves historical values for churned customers)
        mrr = mct_mrr_dict.get(cust_id, 0)

        for i, w in enumerate(weeks):
            if w["start"] <= churn_date <= w["end"]:
                churn_count[i] += 1
                churn_mrr[i] += mrr

                detail = {
                    "company": cust_name,
                    "stripe_id": cust_id,
                    "email": cust_email,
                    "mrr": mrr,
                    "churn_date": str(churn_date),
                    "status": sub.get("status"),
                }

                # For current week, track reasons (from MCT if available)
                if i == len(weeks) - 1:
                    churn_detail.append(detail)

                break

    churn_mrr = [round(m, 2) for m in churn_mrr]

    print(f"   Churn counts: {churn_count}")
    print(f"   Churn MRR: {churn_mrr}")
    print(f"   Current week detail: {len(churn_detail)} churns")

    return churn_count, churn_mrr, current_week_reasons, churn_detail


def build_mct_stripe_ids(mct_rows):
    """Build set of Stripe Customer IDs from MCT for cross-reference."""
    ids = set()
    for row in mct_rows:
        props = row.get("properties", {})
        rt = props.get("\U0001f517 Stripe Customer ID", {}).get("rich_text", [])
        if rt:
            sid = rt[0].get("plain_text", "").strip()
            if sid:
                ids.add(sid)
    return ids


def build_mct_mrr_dict(mct_rows):
    """Build {stripe_customer_id: mrr} from MCT. Preserves historical MRR for churned customers."""
    mrr_dict = {}
    for row in mct_rows:
        props = row.get("properties", {})
        rt = props.get("\U0001f517 Stripe Customer ID", {}).get("rich_text", [])
        if not rt:
            continue
        sid = rt[0].get("plain_text", "").strip()
        if not sid:
            continue
        mrr = _num(props.get("\U0001f4b0 MRR", {}), "number") or 0
        if mrr > 0:
            mrr_dict[sid] = mrr
    return mrr_dict


# ═══════════════════════════════════════════════════════════════════════════
# Step 4 -- Sentiment Health (point-in-time)
# ═══════════════════════════════════════════════════════════════════════════

def compute_sentiment(mct_rows):
    print("\n[4] Computing sentiment \u2026")
    great = 0
    alright = 0
    at_risk = 0

    for row in mct_rows:
        props = row.get("properties", {})
        billing = _str(props.get("\U0001f4b0 Billing Status", {}), "select", "name") or ""
        if billing not in ("Active", "Churning", "Past Due"):
            continue
        sentiment = _str(props.get("\U0001f9e0 CS Sentiment", {}), "select", "name") or ""
        if sentiment == "Great":
            great += 1
        elif sentiment == "Alright":
            alright += 1
        elif sentiment == "At Risk":
            at_risk += 1

    print(f"   Great: {great}, Alright: {alright}, At Risk: {at_risk}")
    return {"great": great, "alright": alright, "at_risk": at_risk}


# ═══════════════════════════════════════════════════════════════════════════
# Step 5 -- Manage snapshots
# ═══════════════════════════════════════════════════════════════════════════

def manage_snapshots(weeks, overview, sentiment):
    print("\n[5] Managing snapshots \u2026")

    snapshots = {}
    if os.path.exists(SNAPSHOTS_FILE):
        with open(SNAPSHOTS_FILE) as f:
            snapshots = json.load(f)

    current_monday = str(weeks[-1]["start"])
    snapshots[current_monday] = {
        "active_count": overview["active_count"],
        "total_mrr": overview["total_mrr"],
        "great": sentiment["great"],
        "alright": sentiment["alright"],
        "at_risk": sentiment["at_risk"],
    }

    with open(SNAPSHOTS_FILE, "w") as f:
        json.dump(snapshots, f, indent=2)
    print(f"   \u2713 Saved snapshot for {current_monday}")

    # Build arrays from snapshots
    active_count_arr = []
    total_mrr_arr = []
    great_arr = []
    alright_arr = []
    at_risk_arr = []

    for w in weeks:
        key = str(w["start"])
        snap = snapshots.get(key)
        if snap:
            active_count_arr.append(snap.get("active_count"))
            total_mrr_arr.append(snap.get("total_mrr"))
            great_arr.append(snap.get("great"))
            alright_arr.append(snap.get("alright"))
            at_risk_arr.append(snap.get("at_risk"))
        else:
            active_count_arr.append(None)
            total_mrr_arr.append(None)
            great_arr.append(None)
            alright_arr.append(None)
            at_risk_arr.append(None)

    print(f"   Active: {active_count_arr}")
    print(f"   MRR: {total_mrr_arr}")
    print(f"   Great: {great_arr}")
    print(f"   Alright: {alright_arr}")
    print(f"   At Risk: {at_risk_arr}")
    return active_count_arr, total_mrr_arr, great_arr, alright_arr, at_risk_arr


# ═══════════════════════════════════════════════════════════════════════════
# Step 6 -- CS Activity (meetings + issues from meetings)
# ═══════════════════════════════════════════════════════════════════════════

ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"
GCAL_SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]
CS_TEAM_EMAILS = ["alex@konvoai.com", "aya@konvoai.com"]
PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def _get_gcal_creds():
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
    except ImportError:
        return None, "google libs not installed"

    token_file = os.path.join(PROJECT_DIR, "token.json")
    client_secrets = os.path.join(PROJECT_DIR, "client_secrets.json")

    if not os.path.exists(client_secrets):
        return None, "client_secrets.json not found"

    creds = (
        Credentials.from_authorized_user_file(token_file, GCAL_SCOPES)
        if os.path.exists(token_file) else None
    )
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_file, "w") as f:
                f.write(creds.to_json())
        else:
            return None, "token.json expired and cannot refresh"

    return creds, None


def fetch_gcal_meetings(weeks):
    """Fetch customer meetings from GCal for the 8-week window, per owner."""
    print("\n[6] Fetching GCal customer meetings \u2026")

    gcreds, err = _get_gcal_creds()
    if not gcreds:
        print(f"   \u26a0\ufe0f  GCal skipped: {err}")
        return []

    from googleapiclient.discovery import build
    service = build("calendar", "v3", credentials=gcreds)

    time_min = datetime(weeks[0]["start"].year, weeks[0]["start"].month, weeks[0]["start"].day,
                        tzinfo=timezone.utc).isoformat()
    time_max = datetime(weeks[-1]["end"].year, weeks[-1]["end"].month, weeks[-1]["end"].day,
                        23, 59, 59, tzinfo=timezone.utc).isoformat()

    # Find calendars for CS team
    cal_list = service.calendarList().list().execute().get("items", [])
    owner_cals = {}
    for cal in cal_list:
        cal_email = (cal.get("id") or "").lower()
        for team_email in CS_TEAM_EMAILS:
            if cal_email == team_email:
                owner_name = team_email.split("@")[0]
                owner_cals[cal["id"]] = owner_name

    print(f"   Calendars: {list(owner_cals.values())}")

    meetings = []
    for cal_id, owner in owner_cals.items():
        page_token = None
        while True:
            resp = service.events().list(
                calendarId=cal_id, timeMin=time_min, timeMax=time_max,
                singleEvents=True, maxResults=250, pageToken=page_token,
            ).execute()
            for ev in resp.get("items", []):
                start = ev.get("start", {})
                dt_str = start.get("dateTime", start.get("date", ""))[:10]
                if not dt_str:
                    continue
                attendees = ev.get("attendees", [])
                has_external = any(
                    not (a.get("email", "").endswith("@konvoai.com") or
                         a.get("email", "").endswith("@resource.calendar.google.com"))
                    for a in attendees if a.get("email")
                )
                if has_external and attendees:
                    meetings.append((dt_str, owner))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    print(f"   \u2713 {len(meetings)} customer meetings")
    return meetings


def fetch_meeting_issues(weeks):
    """Fetch issues with Source='Meeting' from Notion Issues Table."""
    print("\n[7] Fetching meeting-sourced issues \u2026")

    issues_headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    url = f"{NOTION_API}/databases/{ISSUES_DB_ID}/query"
    body = {"filter": {"property": "Source", "select": {"equals": "Meeting"}}}
    pages = []
    cursor = None
    while True:
        if cursor:
            body["start_cursor"] = cursor
        r = requests.post(url, headers=issues_headers, json=body, timeout=30)
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        cursor = data.get("next_cursor")
        if not cursor:
            break

    earliest = str(weeks[0]["start"])
    latest = str(weeks[-1]["end"])

    issues = []
    for page in pages:
        props = page.get("properties", {})
        # Created At date
        ca = props.get("Created At", {}).get("date", {})
        created = ca.get("start", "")[:10] if ca else ""
        if not created or created < earliest or created > latest:
            continue
        # Owner from Assigned To rollup
        assigned = props.get("Assigned To", {})
        rollup_arr = assigned.get("rollup", {}).get("array", [])
        owner = ""
        for item in rollup_arr:
            sel = item.get("select", {})
            if sel and sel.get("name"):
                owner = sel["name"].lower()
                break
        if owner in ("alex", "aya"):
            issues.append({"date": created, "owner": owner})

    print(f"   \u2713 {len(issues)} meeting-sourced issues in window")
    return issues


def bucket_by_owner_week(items, weeks, is_meetings=False):
    """Bucket items by owner per week. Returns {alex: [...], aya: [...], total: [...]}."""
    result = {"alex": [0] * len(weeks), "aya": [0] * len(weeks), "total": [0] * len(weeks)}
    for item in items:
        if is_meetings:
            d_str, owner = item
        else:
            d_str, owner = item["date"], item["owner"]
        for i, w in enumerate(weeks):
            if str(w["start"]) <= d_str <= str(w["end"]):
                if owner in result:
                    result[owner][i] += 1
                result["total"][i] += 1
                break
    return result


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  CEO Dashboard \u2014 Data Fetch")
    print("=" * 60)

    weeks = compute_weeks(8)
    week_labels = [format_week_label(w["start"], w["end"]) for w in weeks]
    print(f"\n  Weeks ({len(weeks)}): {week_labels}")

    # Google Sheet = source of truth for MRR
    sheet_total_mrr, sheet_mrr_dict = fetch_sheet_mrr()

    mct_rows = fetch_all_mct_rows()

    overview = compute_overview(mct_rows, sheet_total_mrr)
    mct_stripe_ids = build_mct_stripe_ids(mct_rows)
    mct_mrr_dict = build_mct_mrr_dict(mct_rows)
    print(f"\n   MCT has {len(mct_stripe_ids)} Stripe Customer IDs, {len(mct_mrr_dict)} with MRR")
    churn_count, churn_mrr, current_week_reasons, churn_detail = fetch_stripe_churns(weeks, mct_stripe_ids, mct_mrr_dict)
    sentiment = compute_sentiment(mct_rows)

    active_arr, mrr_arr, great_arr, alright_arr, risk_arr = manage_snapshots(weeks, overview, sentiment)

    # CS Activity
    gcal_meetings = fetch_gcal_meetings(weeks)
    meeting_issues = fetch_meeting_issues(weeks)
    meetings_by_week = bucket_by_owner_week(gcal_meetings, weeks, is_meetings=True)
    issues_by_week = bucket_by_owner_week(meeting_issues, weeks)
    print(f"\n   Meetings per week: {meetings_by_week['total']}")
    print(f"   Issues per week: {issues_by_week['total']}")

    output = {
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "week_labels": week_labels,
        "weeks": [{"start": str(w["start"]), "end": str(w["end"])} for w in weeks],
        "overview": {
            "active_count": active_arr,
            "total_mrr": mrr_arr,
            "current_active": overview["active_count"],
            "current_mrr": overview["total_mrr"],
        },
        "churn": {
            "churn_count": churn_count,
            "churn_mrr": churn_mrr,
            "current_week_reasons": current_week_reasons,
            "current_week_detail": churn_detail,
        },
        "sentiment": {
            "great": great_arr,
            "alright": alright_arr,
            "at_risk": risk_arr,
            "current_great": sentiment["great"],
            "current_alright": sentiment["alright"],
            "current_at_risk": sentiment["at_risk"],
        },
        "meetings": meetings_by_week,
        "meeting_issues": issues_by_week,
    }

    with open(DATA_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n\u2705 Data written to {DATA_FILE}")


if __name__ == "__main__":
    main()
