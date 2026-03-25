#!/usr/bin/env python3
"""
sync_meeting_scheduled.py
--------------------------
Scans Alex's and Aya's Google Calendars for events in the next 90 days,
matches them to customers in the Notion Master Customer Table, and sets
the "Meeting Scheduled" select property to "Yes" on any match.

Rules:
  - If "Meeting Scheduled" is already "Yes" → skip (leave untouched)
  - If a future event matches a customer → set to "Yes"
  - If no match → leave as-is (do NOT set to "No")
  - Covers ALL customers regardless of Billing Status

Usage:
  python3 sync_meeting_scheduled.py            # apply writes
  python3 sync_meeting_scheduled.py --dry-run  # preview only

Requirements:
  pip3 install google-api-python-client google-auth-oauthlib
"""

import json
import os
import sys
import re
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
import creds

# ── Google Auth ───────────────────────────────────────────────────────────────
try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
except ImportError:
    print("ERROR: Missing dependencies. Run:")
    print("  pip3 install google-api-python-client google-auth-oauthlib")
    sys.exit(1)

import urllib.request

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent

SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]
CLIENT_SECRETS_FILE = SCRIPT_DIR / "client_secrets.json"
TOKEN_FILE = SCRIPT_DIR / "token.json"

NOTION_TOKEN = creds.get("NOTION_TOKEN")
NOTION_DATA_SOURCE_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

CS_TEAM_EMAILS = ["alex@konvoai.com", "aya@konvoai.com"]
LOOKAHEAD_DAYS = 90

# Customers to never auto-set (domain false positives, manually verified)
SKIP_CUSTOMERS = {"ZZEN Labs", "Tienda Bass"}


# ── Google Auth ───────────────────────────────────────────────────────────────

def get_google_credentials():
    if not CLIENT_SECRETS_FILE.exists():
        print()
        print("ERROR: client_secrets.json not found.")
        print(f"  Save it as: {CLIENT_SECRETS_FILE}")
        sys.exit(1)

    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing Google token...")
            creds.refresh(Request())
        else:
            print("Opening browser for Google Calendar authorization...")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CLIENT_SECRETS_FILE), SCOPES
            )
            creds = flow.run_local_server(port=0)
        TOKEN_FILE.write_text(creds.to_json())
        print(f"Token saved to {TOKEN_FILE}")

    return creds


# ── Calendar: Find CS calendars ───────────────────────────────────────────────

def find_cs_calendars(service):
    found = {}
    page_token = None
    while True:
        resp = service.calendarList().list(pageToken=page_token).execute()
        for cal in resp.get("items", []):
            cal_id = cal.get("id", "").lower()
            cal_summary = cal.get("summary", "").lower()
            for email in CS_TEAM_EMAILS:
                name = email.split("@")[0]
                if email.lower() in cal_id or email.lower() in cal_summary:
                    found[name] = cal["id"]
                    print(f"  ✓ Found calendar for {email}: {cal['id']} ({cal.get('summary', '')})")
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    for email in CS_TEAM_EMAILS:
        name = email.split("@")[0]
        if name not in found:
            print(f"  ⚠ Calendar NOT found for {email} — skipping")

    return found


# ── Calendar: Fetch future events ─────────────────────────────────────────────

def fetch_future_events(service, calendar_id, owner_name):
    now = datetime.now(timezone.utc)
    time_min = now.isoformat()
    time_max = (now + timedelta(days=LOOKAHEAD_DAYS)).isoformat()

    events = []
    page_token = None
    while True:
        resp = service.events().list(
            calendarId=calendar_id,
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            orderBy="startTime",
            pageToken=page_token,
            maxResults=2500,
        ).execute()

        for item in resp.get("items", []):
            start = item.get("start", {})
            raw_date = start.get("date") or start.get("dateTime", "")[:10]
            if not raw_date:
                continue
            try:
                event_date = date.fromisoformat(raw_date)
            except ValueError:
                continue

            attendee_emails = [
                a.get("email", "").lower()
                for a in item.get("attendees", [])
            ]

            events.append({
                "date": event_date,
                "summary": item.get("summary", ""),
                "description": item.get("description", "") or "",
                "attendees": attendee_emails,
                "owner": owner_name,
            })

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    print(f"  Fetched {len(events)} future events from {owner_name}'s calendar")
    return events


# ── Notion ────────────────────────────────────────────────────────────────────

def notion_request(method, path, body=None, version="2022-06-28"):
    url = f"https://api.notion.com/v1/{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {NOTION_TOKEN}")
    req.add_header("Notion-Version", version)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        raise Exception(f"HTTP {e.code} {e.reason} — {body_text}") from None


def fetch_all_customers():
    """
    Query the MCT for ALL customers (no billing status filter).
    Returns list of { page_id, name, domain, meeting_scheduled }
    """
    customers = []
    cursor = None

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

            # Company Name
            name_prop = props.get("🏢 Company Name", props.get("Company Name", {}))
            title_parts = name_prop.get("title", [])
            name = "".join(t.get("plain_text", "") for t in title_parts).strip()
            if not name:
                continue

            # Domain
            domain_prop = props.get("🏢 Domain", props.get("Domain", {}))
            domain_parts = domain_prop.get("rich_text", [])
            domain = "".join(t.get("plain_text", "") for t in domain_parts).strip().lower()
            domain = re.sub(r"^https?://", "", domain)
            domain = re.sub(r"^www\.", "", domain)
            domain = domain.rstrip("/")

            # Meeting Scheduled (select)
            ms_prop = props.get("Meeting Scheduled", {})
            ms_val = (ms_prop.get("select") or {}).get("name", "")

            customers.append({
                "page_id": page["id"],
                "name": name,
                "domain": domain,
                "meeting_scheduled": ms_val,
            })

        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break

    print(f"  Found {len(customers)} customers in Notion")
    return customers


# ── Matching ──────────────────────────────────────────────────────────────────

def match_future_events_to_customers(future_events, customers):
    """
    For each customer, find the earliest upcoming matching event.
    Strategies: domain > name-in-title > name-in-desc.
    Returns { page_id: { date, name, matched_via, owner } }
    """
    results = {}

    for customer in customers:
        name = customer["name"]
        domain = customer["domain"]
        name_lower = name.lower()

        best_date = None
        best_via = None
        best_owner = None
        best_title = None

        for event in future_events:
            matched_via = None

            if domain:
                for email in event["attendees"]:
                    if email.endswith(f"@{domain}"):
                        matched_via = "domain"
                        break

            if not matched_via and name_lower in event["summary"].lower():
                matched_via = "name-in-title"

            if not matched_via and name_lower in event["description"].lower():
                matched_via = "name-in-desc"

            if matched_via:
                if best_date is None or event["date"] < best_date:
                    best_date = event["date"]
                    best_via = matched_via
                    best_owner = event["owner"]
                    best_title = event["summary"]

        if best_date:
            results[customer["page_id"]] = {
                "date": best_date,
                "name": name,
                "matched_via": best_via,
                "owner": best_owner,
                "event_title": best_title,
            }

    return results


# ── Notion: Write Meeting Scheduled ──────────────────────────────────────────

def set_meeting_scheduled(page_id):
    body = {
        "properties": {
            "Meeting Scheduled": {
                "select": {"name": "Yes"}
            }
        }
    }
    notion_request("PATCH", f"pages/{page_id}", body, version="2025-09-03")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv

    print()
    print("=" * 60)
    print("  GCal → Notion: Meeting Scheduled Sync")
    if dry_run:
        print("  DRY RUN — no Notion writes")
    print("=" * 60)

    # Step 1: Google auth
    print("\n[1/5] Authenticating with Google Calendar...")
    creds = get_google_credentials()
    service = build("calendar", "v3", credentials=creds)
    print("  Google auth OK")

    # Step 2: Find CS calendars
    print("\n[2/5] Locating CS team calendars...")
    cal_map = find_cs_calendars(service)
    if not cal_map:
        print("  ERROR: No CS team calendars found. Nothing to do.")
        sys.exit(1)

    # Step 3: Fetch future events
    print(f"\n[3/5] Fetching future events (next {LOOKAHEAD_DAYS} days)...")
    all_future_events = []
    for owner_name, cal_id in cal_map.items():
        events = fetch_future_events(service, cal_id, owner_name)
        all_future_events.extend(events)
    print(f"  Total future events collected: {len(all_future_events)}")

    # Step 4: Fetch all customers
    print("\n[4/5] Fetching all customers from Notion...")
    customers = fetch_all_customers()

    already_yes = [c for c in customers if c["meeting_scheduled"] == "Yes"]
    already_no  = [c for c in customers if c["meeting_scheduled"] == "No"]
    to_check    = [c for c in customers if c["meeting_scheduled"] not in ("Yes", "No") and c["name"] not in SKIP_CUSTOMERS]
    print(f"  Already 'Yes': {len(already_yes)} | Already 'No': {len(already_no)} | To check: {len(to_check)}")

    # Step 5: Match and update
    print("\n[5/5] Matching events to customers...")
    matches = match_future_events_to_customers(all_future_events, to_check)

    print()
    print(f"  {'Customer':<32} {'Status':<12} {'Next Meeting':<14} {'Via':<18} {'Calendar':<10} Event Title")
    print(f"  {'-'*32} {'-'*12} {'-'*14} {'-'*18} {'-'*10} {'-'*40}")

    updated = 0
    skipped_already = 0
    no_match = 0

    # Report already-Yes and already-No rows (both skipped)
    for c in sorted(already_yes, key=lambda x: x["name"]):
        print(f"  {c['name']:<32} {'already Yes':<12} {'—':<14} {'—':<18} —")
        skipped_already += 1
    for c in sorted(already_no, key=lambda x: x["name"]):
        print(f"  {c['name']:<32} {'already No':<12} {'—':<14} {'—':<18} —")
        skipped_already += 1

    # Process customers to check
    for c in sorted(to_check, key=lambda x: x["name"]):
        pid = c["page_id"]
        if pid in matches:
            m = matches[pid]
            status = "[DRY RUN]" if dry_run else "→ set Yes"
            print(f"  {c['name']:<32} {status:<12} {m['date'].isoformat():<14} {m['matched_via']:<18} {m['owner']:<10} {m['event_title']}")
            if not dry_run:
                try:
                    set_meeting_scheduled(pid)
                    updated += 1
                except Exception as e:
                    print(f"    ✗ Failed for {c['name']}: {e}")
            else:
                updated += 1
        else:
            print(f"  {c['name']:<32} {'no match':<12} {'—':<14} {'—':<18} —")
            no_match += 1

    print()
    print("=" * 60)
    print(f"  Already 'Yes'/'No' (skipped): {skipped_already}")
    print(f"  Set to 'Yes':             {updated}")
    print(f"  No upcoming meeting found: {no_match}")
    if dry_run:
        print("  (dry run — no changes written)")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
