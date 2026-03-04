#!/usr/bin/env python3
"""
weekly_snapshot.py — Friday afternoon CS snapshot

WHEN TO RUN: Every Friday at 15:00 CET (automatic via cron), or manually any time.

Time window: previous Friday 15:00 CET → this Friday 15:00 CET.

What it does:
  1. Computes all KPIs for the Fri→Fri window
  2. Writes numbers to the Notion Scorecard DB (finds or creates weekly row)
  3. Regenerates cs_dashboard.html
  4. Saves dated HTML + PDF copy to CS_weekly/

Run:     python3 weekly_snapshot.py
Dry run: python3 weekly_snapshot.py --dry-run

Cron (adjust hour if Mac timezone differs from CET):
  0 15 * * 5 cd /Users/edoardopelli/projects/Customer\ Success && /usr/bin/python3 weekly_snapshot.py >> CS_weekly/snapshot.log 2>&1
"""

import json
import os
import re
import shutil
import statistics
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

# ── Credentials ────────────────────────────────────────────────────────────────

load_dotenv()

NOTION_TOKEN       = os.environ.get("NOTION_TOKEN",   "***REMOVED***")
INTERCOM_TOKEN     = "***REMOVED***"
SLACK_BOT_TOKEN    = os.environ.get("SLACK_BOT_TOKEN", "***REMOVED***")
GUILLEM_DM_CHANNEL = "U05T6VDTTFC"                          # Guillem Oliva's Slack user ID (bot DMs him directly)

MCT_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"
SCORECARD_DB = "311e418f-d8c4-810e-8b11-cdc50357e709"

ALEX_ADMIN_ID = "7484673"   # Alex de Godoy  — Intercom admin ID
AYA_ADMIN_ID  = "8411967"   # Aya Guerimej   — Intercom admin ID

NOTION_API   = "https://api.notion.com/v1"
INTERCOM_API = "https://api.intercom.io"

CET = ZoneInfo("Europe/Amsterdam")

std_headers = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}
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

SCRIPT_DIR = Path(__file__).parent
CS_WEEKLY  = SCRIPT_DIR / "CS_weekly"
CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

DRY_RUN = "--dry-run" in sys.argv

CS_TEAM_EMAILS = ["alex@konvoai.com", "aya@konvoai.com"]
GCAL_SCOPES    = ["https://www.googleapis.com/auth/calendar.readonly"]


# ══════════════════════════════════════════════════════════════════════════════
# Step 0 — Compute time window
# ══════════════════════════════════════════════════════════════════════════════

def compute_window():
    """
    Returns a dict with:
      last_fri        — CET datetime, previous Friday 15:00
      this_fri        — CET datetime, this Friday 15:00
      week_label      — e.g. "W09 (Feb 24 - Mar 2)"
      week_start_date — Monday of this ISO week (used to find/create scorecard row)
      win_start_unix  — unix ts of last_fri (for Intercom queries)
      win_end_unix    — unix ts of this_fri
      win_start_date  — last_fri as date
      win_end_date    — this_fri as date
    """
    now = datetime.now(CET)
    # weekday(): Mon=0 … Fri=4 → days since last Friday
    days_since_fri = (now.weekday() - 4) % 7
    this_fri = now - timedelta(days=days_since_fri)
    this_fri_15 = this_fri.replace(hour=15, minute=0, second=0, microsecond=0)
    last_fri_15 = this_fri_15 - timedelta(days=7)

    iso_week = this_fri_15.isocalendar()[1]
    week_start_date = (this_fri_15 - timedelta(days=this_fri_15.weekday())).date()
    week_end_date   = week_start_date + timedelta(days=6)
    week_label = (
        f"W{iso_week:02d} ({week_start_date.strftime('%b %d')} - {week_end_date.strftime('%b %d')})"
    )

    return {
        "last_fri":        last_fri_15,
        "this_fri":        this_fri_15,
        "week_label":      week_label,
        "week_start_date": week_start_date,
        "win_start_unix":  int(last_fri_15.astimezone(timezone.utc).timestamp()),
        "win_end_unix":    int(this_fri_15.astimezone(timezone.utc).timestamp()),
        "win_start_date":  last_fri_15.date(),
        "win_end_date":    this_fri_15.date(),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Step 1 — Fetch all MCT rows
# ══════════════════════════════════════════════════════════════════════════════

def fetch_all_mct_rows():
    """Paginated query of Master Customer Table via data_sources API."""
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
        data  = r.json()
        batch = data.get("results", [])
        all_results.extend(batch)
        print(f"   Page {page_num}: {len(batch)} rows (total: {len(all_results)})")
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    print(f"   ✓ {len(all_results)} total rows")
    return all_results


# ══════════════════════════════════════════════════════════════════════════════
# Step 2 — Compute MCT KPIs
# ══════════════════════════════════════════════════════════════════════════════

def _parse_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _str(prop, *keys):
    """Safely traverse nested dict keys; return str or None."""
    obj = prop
    for k in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(k)
    return obj if isinstance(obj, str) else None


def _num(prop, *keys):
    """Safely traverse nested dict keys; return number or None."""
    obj = prop
    for k in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(k)
    return obj if isinstance(obj, (int, float)) else None


def compute_kpis(mct_rows, window):
    """
    Scan MCT rows and return:
      kpis               — {"Alex": {red_health, no_contact, churned, graduated}, "Aya": {...}}
      customers_for_gcal — list of {page_id, name, domain, owner, billing}
                           (all customers, used for GCal event matching)
    """
    print("\n[2] Computing MCT KPIs …")

    win_start = window["win_start_date"]
    win_end   = window["win_end_date"]

    kpis = {
        "Alex": {"red_health": 0, "no_contact": 0, "churned": 0, "graduated": 0},
        "Aya":  {"red_health": 0, "no_contact": 0, "churned": 0, "graduated": 0},
    }
    customers_for_gcal = []
    churning_pipeline_mrr = 0   # total MRR at risk across all churning customers

    for row in mct_rows:
        props = row.get("properties", {})

        owner          = _str(props.get("⭐ CS Owner", {}),      "select", "name")
        billing_status = _str(props.get("💰 Billing Status", {}), "select", "name") or ""

        # Build customer record for GCal matching (all rows, regardless of owner)
        name_parts   = props.get("🏢 Company Name", {}).get("title", [])
        name         = "".join(t.get("plain_text", "") for t in name_parts).strip()
        domain_parts = props.get("🏢 Domain", {}).get("rich_text", [])
        domain       = "".join(t.get("plain_text", "") for t in domain_parts).strip().lower()
        domain = re.sub(r"^https?://", "", domain)
        domain = re.sub(r"^www\.", "", domain)
        domain = domain.rstrip("/")

        if name:
            customers_for_gcal.append({
                "page_id": row["id"],
                "name":    name,
                "domain":  domain,
                "owner":   owner,
                "billing": billing_status,
            })

        # Accumulate churning pipeline MRR (regardless of CS owner)
        if billing_status == "Churning":
            churning_pipeline_mrr += _num(props.get("💰 MRR", {}), "number") or 0

        if owner not in ("Alex", "Aya"):
            continue

        health_status    = _str(props.get("🚦 Health Status", {}),             "formula", "string") or ""
        days_no_contact  = _num(props.get("📞 Days Since Last Contact", {}),    "formula", "number")
        # Canceled = gone forever; Active + Churning = still a customer we track
        is_churned_stage = billing_status not in ("Active", "Churning")

        # KPI 1: Red Health (active + churning customers)
        if "Red" in health_status and not is_churned_stage:
            kpis[owner]["red_health"] += 1

        # KPI 2: No Contact >21d (active + churning customers)
        if days_no_contact is not None and days_no_contact > 21 and not is_churned_stage:
            kpis[owner]["no_contact"] += 1

        # KPI 4: Churned in window — use 📅 Churning Since (decision date, not billing end)
        churning_since = _parse_date(_str(props.get("📅 Churning Since", {}), "date", "start"))
        if churning_since and win_start <= churning_since <= win_end:
            kpis[owner]["churned"] += 1

        # KPI 5: Graduated in window
        grad_date = _parse_date(_str(props.get("🚀 Graduation Date", {}), "date", "start"))
        if grad_date and win_start <= grad_date <= win_end:
            kpis[owner]["graduated"] += 1

    for mgr in ("Alex", "Aya"):
        v = kpis[mgr]
        print(f"   {mgr}: red={v['red_health']}, nocontact={v['no_contact']}, "
              f"churned={v['churned']}, graduated={v['graduated']}")
    print(f"   Churning pipeline MRR: €{churning_pipeline_mrr:,.0f}/mo")

    return kpis, customers_for_gcal, churning_pipeline_mrr


# ══════════════════════════════════════════════════════════════════════════════
# Step 3 — Fetch Intercom data (reply times + unique companies contacted)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_intercom_data(window, customers_for_gcal):
    """
    Fetches all conversations closed in the Fri→Fri window.
    Returns:
      reply_times         — {"Alex": {"reply_min": float|None}, "Aya": {...}}
      intercom_contacted  — {"Alex": set, "Aya": set}  (MCT page_ids per closer, via email-domain match)
    """
    print("\n[3] Fetching Intercom conversations …")

    url = f"{INTERCOM_API}/conversations/search"
    query = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "open",                     "operator": "=",  "value": False},
                {"field": "statistics.last_close_at", "operator": ">",  "value": window["win_start_unix"]},
                {"field": "statistics.last_close_at", "operator": "<=", "value": window["win_end_unix"]},
            ],
        },
        "pagination": {"per_page": 150},
    }

    all_convs, page, cursor = [], 1, None
    while True:
        if cursor:
            query["pagination"]["starting_after"] = cursor
        elif "starting_after" in query["pagination"]:
            del query["pagination"]["starting_after"]

        r = requests.post(url, headers=intercom_headers, json=query)
        r.raise_for_status()
        data  = r.json()
        batch = data.get("conversations", [])
        all_convs.extend(batch)
        print(f"   Page {page}: {len(batch)} convs (total: {len(all_convs)})")

        next_p = data.get("pages", {}).get("next", {})
        cursor = next_p.get("starting_after") if isinstance(next_p, dict) else None
        if not cursor or not batch:
            break
        page += 1

    print(f"   ✓ {len(all_convs)} conversations fetched")

    domain_to_page_id  = {c["domain"]: c["page_id"] for c in customers_for_gcal if c["domain"]}
    reply_by_admin     = {}   # admin_id → [delta_seconds]
    page_ids_by_admin  = {}   # admin_id → set of MCT page_ids

    for c in all_convs:
        stats  = c.get("statistics") or {}
        closer = stats.get("last_closed_by_id")
        if not closer:
            continue
        closer = str(closer)

        # Reply time: time from last assignment to admin's first reply post-assignment
        reply_at      = stats.get("last_assignment_admin_reply_at")
        assignment_at = stats.get("last_assignment_at")
        if reply_at and assignment_at and reply_at > assignment_at:
            reply_by_admin.setdefault(closer, []).append(reply_at - assignment_at)

        # Company via email domain → MCT page_id
        author_email = ((c.get("source") or {}).get("author") or {}).get("email") or ""
        if "@" in author_email:
            domain = author_email.split("@")[-1].lower()
            pid = domain_to_page_id.get(domain)
            if pid:
                page_ids_by_admin.setdefault(closer, set()).add(pid)

    reply_times        = {}
    intercom_contacted = {}

    for admin_id, label in [(ALEX_ADMIN_ID, "Alex"), (AYA_ADMIN_ID, "Aya")]:
        times = reply_by_admin.get(admin_id, [])
        if times:
            med_min = round(statistics.median(times) / 60, 1)
            reply_times[label] = {"reply_min": med_min}
            print(f"   {label}: median reply = {med_min} min (n={len(times)})")
        else:
            reply_times[label] = {"reply_min": None}
            print(f"   {label}: no reply data")

        page_ids = page_ids_by_admin.get(admin_id, set())
        intercom_contacted[label] = page_ids
        print(f"   {label}: {len(page_ids)} unique customers via Intercom")

    return reply_times, intercom_contacted


# ══════════════════════════════════════════════════════════════════════════════
# Step 4 — Fetch GCal customers contacted
# ══════════════════════════════════════════════════════════════════════════════

def _get_gcal_creds():
    """Return (credentials, error_string). error_string is None on success."""
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        return None, "google libs not installed (pip3 install google-api-python-client google-auth-oauthlib)"

    client_secrets = SCRIPT_DIR / "client_secrets.json"
    token_file     = SCRIPT_DIR / "token.json"

    if not client_secrets.exists():
        return None, "client_secrets.json not found"

    creds = (
        Credentials.from_authorized_user_file(str(token_file), GCAL_SCOPES)
        if token_file.exists() else None
    )
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow  = InstalledAppFlow.from_client_secrets_file(str(client_secrets), GCAL_SCOPES)
            creds = flow.run_local_server(port=0)
        token_file.write_text(creds.to_json())

    return creds, None


def fetch_gcal_contacted(window, customers_for_gcal):
    """
    Fetches Alex's and Aya's GCal events in the Fri→Fri window.
    Matches events to MCT customers via domain or company name.
    Returns {"Alex": set, "Aya": set} — MCT page_id sets per owner.
    """
    print("\n[4] Fetching GCal customer contacts …")

    creds, err = _get_gcal_creds()
    if err:
        print(f"   ⚠ GCal skipped: {err}")
        return {"Alex": set(), "Aya": set()}

    try:
        from googleapiclient.discovery import build
    except ImportError:
        print("   ⚠ GCal skipped: google libs not installed")
        return {"Alex": set(), "Aya": set()}

    service = build("calendar", "v3", credentials=creds)

    # Discover Alex + Aya calendars
    cal_map, pt = {}, None
    while True:
        resp = service.calendarList().list(pageToken=pt).execute()
        for cal in resp.get("items", []):
            cal_id = cal.get("id", "").lower()
            for email in CS_TEAM_EMAILS:
                name = email.split("@")[0]   # "alex" or "aya"
                if email.lower() in cal_id and name not in cal_map:
                    cal_map[name] = cal["id"]
                    print(f"   ✓ {email} → {cal['id']}")
        pt = resp.get("nextPageToken")
        if not pt:
            break

    if not cal_map:
        print("   ⚠ No CS team calendars found — skipping GCal")
        return {"Alex": set(), "Aya": set()}

    # Fetch events in Fri→Fri window
    time_min = window["last_fri"].isoformat()
    time_max = window["this_fri"].isoformat()
    all_events = []

    for owner_name, cal_id in cal_map.items():
        pt, count = None, 0
        while True:
            resp = service.events().list(
                calendarId=cal_id,
                timeMin=time_min, timeMax=time_max,
                singleEvents=True, orderBy="startTime",
                pageToken=pt, maxResults=2500,
            ).execute()

            for item in resp.get("items", []):
                start    = item.get("start", {})
                raw_date = start.get("date") or start.get("dateTime", "")[:10]
                if not raw_date:
                    continue
                try:
                    event_date = date.fromisoformat(raw_date)
                except ValueError:
                    continue

                attendees = [a.get("email", "").lower() for a in item.get("attendees", [])]

                # Only count events with at least one external (non-KonvoAI) attendee
                has_external = any(
                    not e.endswith("@konvoai.com")
                    and not e.endswith("@resource.calendar.google.com")
                    for e in attendees
                )
                if not has_external:
                    continue

                all_events.append({
                    "date":      event_date,
                    "summary":   item.get("summary", ""),
                    "attendees": attendees,
                    "owner":     owner_name,  # "alex" or "aya"
                })
                count += 1

            pt = resp.get("nextPageToken")
            if not pt:
                break

        print(f"   {owner_name}: {count} events with external attendees in window")

    # Match events to MCT customers
    gcal_page_ids = {"alex": set(), "aya": set()}   # page_ids per owner

    for customer in customers_for_gcal:
        name_lower = customer["name"].lower()
        domain     = customer["domain"]

        for event in all_events:
            matched = False

            # Strategy 1: attendee email domain matches customer domain
            if domain:
                matched = any(e.endswith(f"@{domain}") for e in event["attendees"])

            # Strategy 2: company name in event title (guard against short names)
            if not matched and len(name_lower) > 3:
                matched = name_lower in event["summary"].lower()

            if matched:
                gcal_page_ids[event["owner"]].add(customer["page_id"])

    result = {
        "Alex": gcal_page_ids.get("alex", set()),
        "Aya":  gcal_page_ids.get("aya", set()),
    }
    print(f"   Alex: {len(result['Alex'])} unique customers via GCal")
    print(f"   Aya:  {len(result['Aya'])} unique customers via GCal")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Step 5 — Combine contacts
# ══════════════════════════════════════════════════════════════════════════════

def combine_contacts(intercom_contacted, gcal_contacted):
    """
    Union Intercom + GCal MCT page_id sets per owner for true deduplication.
    Both inputs are {"Alex": set, "Aya": set} of MCT page_ids.
    """
    print("\n[5] Combining customer contacts …")
    result = {}
    for label in ("Alex", "Aya"):
        ic_set = intercom_contacted.get(label, set())
        gc_set = gcal_contacted.get(label, set())
        union  = ic_set | gc_set
        result[label] = len(union)
        print(f"   {label}: {len(ic_set)} Intercom + {len(gc_set)} GCal → {result[label]} unique (union)")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Step 6 — Find or create Notion scorecard row
# ══════════════════════════════════════════════════════════════════════════════

def _build_scorecard_props(kpis, reply_times, contacts, week_label, week_start_date):
    """Assemble the Notion properties dict for a scorecard row."""
    props = {
        "Week":                      {"title": [{"text": {"content": week_label}}]},
        "Week Start":                {"date": {"start": str(week_start_date)}},
        "Alex: Red Health":          {"number": kpis["Alex"]["red_health"]},
        "Aya: Red Health":           {"number": kpis["Aya"]["red_health"]},
        "Alex: No Contact >21d":     {"number": kpis["Alex"]["no_contact"]},
        "Aya: No Contact >21d":      {"number": kpis["Aya"]["no_contact"]},
        "Alex: Churned":             {"number": kpis["Alex"]["churned"]},
        "Aya: Churned":              {"number": kpis["Aya"]["churned"]},
        "Alex: Graduated":           {"number": kpis["Alex"]["graduated"]},
        "Aya: Graduated":            {"number": kpis["Aya"]["graduated"]},
        "Alex: Customers Contacted": {"number": contacts["Alex"]},
        "Aya: Customers Contacted":  {"number": contacts["Aya"]},
    }
    if reply_times.get("Alex", {}).get("reply_min") is not None:
        props["Alex: Median Reply Time"] = {"number": reply_times["Alex"]["reply_min"]}
    if reply_times.get("Aya", {}).get("reply_min") is not None:
        props["Aya: Median Reply Time"] = {"number": reply_times["Aya"]["reply_min"]}
    return props


def find_or_create_scorecard_row(kpis, reply_times, contacts, window):
    """
    Looks up the scorecard row for this ISO week (by Week Start = Monday).
    If found: PATCH with new KPIs.
    If not found: POST new row.
    """
    week_label      = window["week_label"]
    week_start_date = window["week_start_date"]

    print(f"\n[6] Updating scorecard row for {week_label} …")

    r = requests.post(
        f"{NOTION_API}/databases/{SCORECARD_DB}/query",
        headers=std_headers,
        json={
            "filter": {"property": "Week Start", "date": {"equals": str(week_start_date)}},
            "page_size": 10,
        },
    )
    r.raise_for_status()
    results = r.json().get("results", [])

    all_props = _build_scorecard_props(kpis, reply_times, contacts, week_label, week_start_date)

    if DRY_RUN:
        action = "patch" if results else "create"
        print(f"   [dry-run] Would {action} row:")
        for k, v in all_props.items():
            print(f"     {k}: {v}")
        return None

    if results:
        page_id   = results[0]["id"]
        kpi_props = {k: v for k, v in all_props.items() if k not in ("Week", "Week Start")}
        r = requests.patch(
            f"{NOTION_API}/pages/{page_id}",
            headers=std_headers,
            json={"properties": kpi_props},
        )
        r.raise_for_status()
        print(f"   ✓ Existing row updated: {page_id}")
    else:
        r = requests.post(
            f"{NOTION_API}/pages",
            headers=std_headers,
            json={"parent": {"database_id": SCORECARD_DB}, "properties": all_props},
        )
        r.raise_for_status()
        page_id = r.json()["id"]
        print(f"   ✓ New row created: {page_id}")

    return page_id


# ══════════════════════════════════════════════════════════════════════════════
# Step 7 — Regenerate cs_dashboard.html
# ══════════════════════════════════════════════════════════════════════════════

def regenerate_dashboard():
    print("\n[7] Regenerating cs_dashboard.html …")
    if DRY_RUN:
        print("   [dry-run] Would run: python3 cs_dashboard.py")
        return

    result = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / "cs_dashboard.py")],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        print("   ✓ Dashboard regenerated")
    else:
        print(f"   ✗ Dashboard script failed:\n{result.stderr}")


# ══════════════════════════════════════════════════════════════════════════════
# Step 8 — Save HTML + PDF to CS_weekly/
# ══════════════════════════════════════════════════════════════════════════════

def save_weekly_files(window):
    iso_week = window["this_fri"].isocalendar()[1]
    tag      = f"w{iso_week:02d}"

    CS_WEEKLY.mkdir(exist_ok=True)

    src_html = SCRIPT_DIR / "cs_dashboard.html"
    dst_html = CS_WEEKLY / f"CS_weekly_{tag}.html"
    dst_pdf  = CS_WEEKLY / f"CS_weekly_{tag}.pdf"

    print(f"\n[8] Saving weekly files (tag={tag}) …")

    if DRY_RUN:
        print(f"   [dry-run] Would copy → {dst_html}")
        print(f"   [dry-run] Would convert → {dst_pdf}")
        return

    if not src_html.exists():
        print(f"   ✗ cs_dashboard.html not found — skipping")
        return

    shutil.copy2(src_html, dst_html)
    print(f"   ✓ HTML saved: {dst_html}")

    # PDF via Chrome headless
    chrome = Path(CHROME_PATH)
    if not chrome.exists():
        print(f"   ⚠ Chrome not found at {CHROME_PATH} — PDF skipped")
        return

    subprocess.run([
        str(chrome),
        "--headless",
        "--disable-gpu",
        "--no-sandbox",
        f"--print-to-pdf={dst_pdf}",
        "--no-pdf-header-footer",
        f"file://{dst_html.resolve()}",
    ], capture_output=True, text=True, timeout=60)

    if dst_pdf.exists():
        print(f"   ✓ PDF saved: {dst_pdf}")
    else:
        print(f"   ✗ PDF not created — try opening {dst_html} in Chrome manually")


# ══════════════════════════════════════════════════════════════════════════════
# Step 9 — Slack DM to Guillem
# ══════════════════════════════════════════════════════════════════════════════

def post_guillem_dm(kpis, reply_times, contacts, window, churning_mrr):
    """
    Send a structured weekly KPI summary to Guillem via Slack DM.
    Requires SLACK_BOT_TOKEN env var (xoxb-...).
    """
    print("\n[9] Sending weekly summary DM to Guillem …")

    if not SLACK_BOT_TOKEN:
        print("   ⚠ SLACK_BOT_TOKEN not set — DM skipped.")
        print("     Set it in Credentials.md / GitHub Secret 'SLACK_BOT_TOKEN'.")
        return

    a_rt = reply_times["Alex"]["reply_min"]
    y_rt = reply_times["Aya"]["reply_min"]

    def fmt_rt(v):
        return f"{v} min" if v is not None else "—"

    text = (
        f"📊 *Weekly CS Snapshot — {window['week_label']}*\n"
        f"\n"
        f"*Alex de Godoy*\n"
        f"  📞 Contacted: {contacts['Alex']}  |  😢 Churned: {kpis['Alex']['churned']}"
        f"  |  🎉 Graduated: {kpis['Alex']['graduated']}\n"
        f"  🔴 Red Health: {kpis['Alex']['red_health']}  |  ⏰ No Contact >21d: {kpis['Alex']['no_contact']}"
        f"  |  ⚡ Median Reply: {fmt_rt(a_rt)}\n"
        f"\n"
        f"*Aya Guerimej*\n"
        f"  📞 Contacted: {contacts['Aya']}  |  😢 Churned: {kpis['Aya']['churned']}"
        f"  |  🎉 Graduated: {kpis['Aya']['graduated']}\n"
        f"  🔴 Red Health: {kpis['Aya']['red_health']}  |  ⏰ No Contact >21d: {kpis['Aya']['no_contact']}"
        f"  |  ⚡ Median Reply: {fmt_rt(y_rt)}\n"
        f"\n"
        f"*Churning Pipeline*: €{churning_mrr:,.0f}/mo MRR at risk\n"
    )

    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        },
        json={"channel": GUILLEM_DM_CHANNEL, "text": text},
    )
    resp = r.json()
    if resp.get("ok"):
        print("   ✓ DM sent to Guillem")
    else:
        print(f"   ✗ Slack error: {resp.get('error', r.text)}")

    if DRY_RUN:
        print("   [dry-run] DM content:")
        print(text)


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    started_at = datetime.now(CET)
    print("=" * 60)
    print(f"  Weekly Friday CS Snapshot  [{started_at.strftime('%a %d %b %Y %H:%M %Z')}]")
    print("=" * 60)
    if DRY_RUN:
        print("  [DRY RUN — no Notion writes, no files saved]\n")

    window = compute_window()
    print(
        f"\n  Window : {window['last_fri'].strftime('%a %d %b %Y %H:%M %Z')}"
        f" → {window['this_fri'].strftime('%a %d %b %Y %H:%M %Z')}"
    )
    print(f"  Label  : {window['week_label']}")
    print(f"  Week # : W{window['this_fri'].isocalendar()[1]:02d} (scorecard key: {window['week_start_date']})")

    mct_rows                        = fetch_all_mct_rows()
    kpis, customers_gcal, churn_mrr = compute_kpis(mct_rows, window)
    reply_times, ic_cont            = fetch_intercom_data(window, customers_gcal)
    gcal_cont                       = fetch_gcal_contacted(window, customers_gcal)
    contacts                        = combine_contacts(ic_cont, gcal_cont)

    find_or_create_scorecard_row(kpis, reply_times, contacts, window)
    regenerate_dashboard()
    save_weekly_files(window)
    post_guillem_dm(kpis, reply_times, contacts, window, churn_mrr)

    # ── Summary table ──────────────────────────────────────────────────────────
    a_rt = reply_times["Alex"]["reply_min"]
    y_rt = reply_times["Aya"]["reply_min"]

    print("\n" + "=" * 60)
    print("  DONE")
    print("=" * 60)
    print(f"\n  {'KPI':<35} {'Alex':>6} {'Aya':>6}")
    print("  " + "-" * 50)
    print(f"  {'Red Health':<35} {kpis['Alex']['red_health']:>6} {kpis['Aya']['red_health']:>6}")
    print(f"  {'No Contact >21d':<35} {kpis['Alex']['no_contact']:>6} {kpis['Aya']['no_contact']:>6}")
    print(f"  {'Median Reply Time (min)':<35} {str(a_rt) if a_rt is not None else '–':>6} {str(y_rt) if y_rt is not None else '–':>6}")
    print(f"  {'Churned this week':<35} {kpis['Alex']['churned']:>6} {kpis['Aya']['churned']:>6}")
    print(f"  {'Graduated this week':<35} {kpis['Alex']['graduated']:>6} {kpis['Aya']['graduated']:>6}")
    print(f"  {'Customers Contacted':<35} {contacts['Alex']:>6} {contacts['Aya']:>6}")
    print()


if __name__ == "__main__":
    main()
