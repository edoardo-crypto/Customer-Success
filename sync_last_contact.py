#!/usr/bin/env python3
"""
sync_last_contact.py
--------------------
WHEN TO RUN: Manually when needed, or via the n8n "Daily Last Contact Date
Sync" workflow (runs every day at 23:30 CET). Safe to re-run at any time.

Scans Alex's and Aya's Google Calendars (shared with Edoardo),
matches events to active customers in Notion, and writes the most
recent contact date back to the "📞 Last Contact Date 🔒" property.

Usage:
  python3 sync_last_contact.py

First run opens a browser for Google OAuth consent and saves token.json.
Subsequent runs reuse the saved token (no browser needed).

Requirements:
  pip3 install google-api-python-client google-auth-oauthlib

Setup:
  1. Go to https://console.cloud.google.com/apis/credentials
  2. Download the OAuth 2.0 Client ID JSON for "n8n"
  3. Save it as client_secrets.json next to this script
"""

import json
import os
import sys
import re
from datetime import datetime, timedelta, date, timezone
from pathlib import Path

# ── Google Auth ──────────────────────────────────────────────────────────────
try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
except ImportError:
    print("ERROR: Missing dependencies. Run:")
    print("  pip3 install google-api-python-client google-auth-oauthlib")
    sys.exit(1)

# ── HTTP (for Notion) ─────────────────────────────────────────────────────────
import urllib.request

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent

# Google OAuth
SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]
CLIENT_SECRETS_FILE = SCRIPT_DIR / "client_secrets.json"
TOKEN_FILE = SCRIPT_DIR / "token.json"

# Credentials.md values
NOTION_TOKEN = "***REMOVED***"
NOTION_DATA_SOURCE_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"
NOTION_DB_ID = "84feda19cfaf4c6e9500bf21d2aaafef"

# CS team calendars to scan
CS_TEAM_EMAILS = ["alex@konvoai.com", "aya@konvoai.com"]

# How far back to look
LOOKBACK_DAYS = 180


# ── Google Auth ───────────────────────────────────────────────────────────────

def get_google_credentials():
    """Authenticate via OAuth2. Opens browser on first run, reuses token.json after."""
    if not CLIENT_SECRETS_FILE.exists():
        print()
        print("ERROR: client_secrets.json not found.")
        print()
        print("To set up Google OAuth:")
        print("  1. Go to https://console.cloud.google.com/apis/credentials")
        print("  2. Find the OAuth 2.0 Client ID for this project")
        print("  3. Click the download icon → Download JSON")
        print(f"  4. Save it as: {CLIENT_SECRETS_FILE}")
        print()
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
        # Save token for future runs
        TOKEN_FILE.write_text(creds.to_json())
        print(f"Token saved to {TOKEN_FILE}")

    return creds


# ── Calendar: Find CS team calendars ─────────────────────────────────────────

def find_cs_calendars(service):
    """
    Walk the list of calendars Edoardo can see and return those
    belonging to Alex and Aya (matching by calendar id/summary).
    """
    found = {}
    page_token = None
    while True:
        resp = service.calendarList().list(pageToken=page_token).execute()
        for cal in resp.get("items", []):
            cal_id = cal.get("id", "").lower()
            cal_summary = cal.get("summary", "").lower()
            for email in CS_TEAM_EMAILS:
                name = email.split("@")[0]  # "alex" or "aya"
                if email.lower() in cal_id or email.lower() in cal_summary:
                    found[name] = cal["id"]
                    print(f"  ✓ Found calendar for {email}: {cal['id']} ({cal.get('summary','')})")
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    for email in CS_TEAM_EMAILS:
        name = email.split("@")[0]
        if name not in found:
            print(f"  ⚠ Calendar NOT found for {email} — skipping (make sure they shared it with Edoardo)")

    return found  # { "alex": "calendar_id", "aya": "calendar_id" }


# ── Calendar: Fetch events ────────────────────────────────────────────────────

def fetch_events(service, calendar_id, owner_name):
    """
    Fetch all events from the past LOOKBACK_DAYS days for a given calendar.
    Returns a list of dicts: { date, summary, description, attendees, owner }
    """
    time_min = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    time_max = datetime.now(timezone.utc).isoformat()

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
            # Parse event date (could be all-day or timed)
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

    print(f"  Fetched {len(events)} events from {owner_name}'s calendar")
    return events


# ── Notion: Fetch active customers ───────────────────────────────────────────

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
        body = e.read().decode()
        raise Exception(f"HTTP {e.code} {e.reason} — {body}") from None


def fetch_active_customers():
    """
    Query the Master Customer Table for all Active customers.
    Returns list of { page_id, name, domain }
    """
    customers = []
    cursor = None

    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        # Must use data_sources API for Master Customer Table (multiple data sources)
        resp = notion_request(
            "POST",
            f"data_sources/{NOTION_DATA_SOURCE_ID}/query",
            body,
            version="2025-09-03",
        )

        for page in resp.get("results", []):
            props = page.get("properties", {})

            # Skip non-Active customers (client-side filter)
            billing = props.get("💰 Billing Status", {})
            billing_val = (billing.get("select") or {}).get("name", "")
            if billing_val != "Active":
                continue

            # Company Name (title property)
            name_prop = props.get("🏢 Company Name", props.get("Company Name", {}))
            title_parts = name_prop.get("title", [])
            name = "".join(t.get("plain_text", "") for t in title_parts).strip()

            # Domain (rich_text property)
            domain_prop = props.get("🏢 Domain", props.get("Domain", {}))
            domain_parts = domain_prop.get("rich_text", [])
            domain = "".join(t.get("plain_text", "") for t in domain_parts).strip().lower()
            # Normalize domain: strip www., strip protocol
            domain = re.sub(r"^https?://", "", domain)
            domain = re.sub(r"^www\.", "", domain)
            domain = domain.rstrip("/")

            if name:
                customers.append({
                    "page_id": page["id"],
                    "name": name,
                    "domain": domain,
                })

        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break

    print(f"  Found {len(customers)} active customers in Notion")
    return customers


# ── Matching ──────────────────────────────────────────────────────────────────

def match_events_to_customers(all_events, customers):
    """
    For each customer, find the most recent event date across all calendars.

    Matching strategies:
    1. Domain: any attendee email ends with @customer_domain
    2. Name in title: customer name (case-insensitive) in event summary
    3. Name in description: customer name in event description

    Returns { page_id: { date, customer_name, matched_via, owner } }
    """
    results = {}

    for customer in customers:
        name = customer["name"]
        domain = customer["domain"]
        name_lower = name.lower()

        best_date = None
        best_via = None
        best_owner = None

        for event in all_events:
            matched_via = None

            # Strategy 1: domain match
            if domain:
                for email in event["attendees"]:
                    if email.endswith(f"@{domain}"):
                        matched_via = "domain"
                        break

            # Strategy 2: name in event title
            if not matched_via and name_lower in event["summary"].lower():
                matched_via = "name-in-title"

            # Strategy 3: name in event description
            if not matched_via and name_lower in event["description"].lower():
                matched_via = "name-in-desc"

            if matched_via:
                if best_date is None or event["date"] > best_date:
                    best_date = event["date"]
                    best_via = matched_via
                    best_owner = event["owner"]

        if best_date:
            results[customer["page_id"]] = {
                "date": best_date,
                "name": name,
                "matched_via": best_via,
                "owner": best_owner,
            }

    return results


# ── Notion: Write last contact date ──────────────────────────────────────────

def update_notion_last_contact(page_id, contact_date):
    """PATCH the '📞 Last Contact Date' property on a Notion page."""
    body = {
        "properties": {
            "📞 Last Contact Date 🔒": {
                "date": {"start": contact_date.isoformat()}
            }
        }
    }
    notion_request("PATCH", f"pages/{page_id}", body, version="2022-06-28")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 60)
    print("  Google Calendar → Notion Last Contact Date Sync")
    print("=" * 60)

    # Step 1: Authenticate with Google
    print("\n[1/5] Authenticating with Google Calendar...")
    creds = get_google_credentials()
    service = build("calendar", "v3", credentials=creds)
    print("  Google auth OK")

    # Step 2: Find Alex's and Aya's calendars
    print("\n[2/5] Locating CS team calendars...")
    cal_map = find_cs_calendars(service)
    if not cal_map:
        print("  ERROR: No CS team calendars found. Nothing to do.")
        sys.exit(1)

    # Step 3: Fetch events from all found calendars
    print(f"\n[3/5] Fetching events (last {LOOKBACK_DAYS} days)...")
    all_events = []
    for owner_name, cal_id in cal_map.items():
        events = fetch_events(service, cal_id, owner_name)
        all_events.extend(events)
    print(f"  Total events collected: {len(all_events)}")

    # Step 4: Fetch active customers from Notion
    print("\n[4/5] Fetching active customers from Notion...")
    customers = fetch_active_customers()

    # Step 5: Match and update
    print("\n[5/5] Matching events to customers and updating Notion...")
    matches = match_events_to_customers(all_events, customers)

    updated = 0
    skipped = 0

    # Print summary header
    print()
    print(f"  {'Customer':<30} {'Last Contact':<14} {'Via':<18} {'Calendar'}")
    print(f"  {'-'*30} {'-'*14} {'-'*18} {'-'*10}")

    for customer in sorted(customers, key=lambda c: c["name"]):
        pid = customer["page_id"]
        if pid in matches:
            m = matches[pid]
            print(f"  {m['name']:<30} {m['date'].isoformat():<14} {m['matched_via']:<18} {m['owner']}")
            try:
                update_notion_last_contact(pid, m["date"])
                updated += 1
            except Exception as e:
                print(f"    ✗ Failed to update {m['name']}: {e}")
        else:
            print(f"  {customer['name']:<30} {'(no match)':<14} {'—':<18} —")
            skipped += 1

    print()
    print("=" * 60)
    print(f"  Done. Updated: {updated} | Skipped (no match): {skipped}")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
