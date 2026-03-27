#!/usr/bin/env python3
"""
check_sla_deadlines.py — Hourly SLA deadline checker

Runs every hour during business hours (Mon-Fri 08-18 CET) via GitHub Actions.
Checks all open Bug issues for approaching or breached SLA deadlines.
Posts warnings (4h before) and breach alerts (2h after) to the #sla-alerts channel.

SLA structure (two stacked SLAs per bug):
  Triage:     Created At + 1 business day → must exit Linear Triage state
  Resolution: Triaged At + severity-dependent business days → must reach Resolved

Notification state machine (only advances forward):
  On Track → Triage Warning → Triage Breach → Resolution Warning → Resolution Breach

Usage:
  python3 check_sla_deadlines.py          # live
  DRY_RUN=true python3 check_sla_deadlines.py  # preview only
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import requests
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
SLACK_BOT_TOKEN = creds.get("SLACK_BOT_TOKEN")

# Channel ID — from env var (GitHub Actions) or Credentials.md
SLA_SLACK_CHANNEL = os.environ.get("SLA_SLACK_CHANNEL", "").strip()
if not SLA_SLACK_CHANNEL:
    try:
        SLA_SLACK_CHANNEL = creds.get("SLA_SLACK_CHANNEL")
    except RuntimeError:
        SLA_SLACK_CHANNEL = ""

NOTION_VERSION = "2022-06-28"
ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"
CET = ZoneInfo("Europe/Amsterdam")

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() not in ("false", "0", "no")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# ── SLA Status state machine — ordered levels ────────────────────────────────
SLA_LEVELS = [
    "On Track",
    "Resolution Warning",
    "Resolution Breach",
]
SLA_LEVEL_INDEX = {name: i for i, name in enumerate(SLA_LEVELS)}


def sla_level_below(current: str, target: str) -> bool:
    """True if current SLA status is strictly below target level."""
    return SLA_LEVEL_INDEX.get(current, 0) < SLA_LEVEL_INDEX.get(target, 0)


def add_business_days(start_dt: datetime, days: int) -> datetime:
    """Add N business days (Mon-Fri) to a datetime, preserving time-of-day."""
    current = start_dt
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


# ── Notion helpers ────────────────────────────────────────────────────────────
def fetch_open_bugs() -> list[dict]:
    """Fetch all Bug issues that are Open or In Progress."""
    url = f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query"
    payload = {
        "filter": {
            "and": [
                {"property": "Issue Type", "select": {"equals": "Bug"}},
                {
                    "or": [
                        {"property": "Status", "select": {"equals": "Open"}},
                        {"property": "Status", "select": {"equals": "In Progress"}},
                    ]
                },
            ]
        },
        "page_size": 100,
    }

    pages = []
    has_more = True
    cursor = None

    while has_more:
        if cursor:
            payload["start_cursor"] = cursor
        resp = requests.post(url, headers=NOTION_HEADERS, json=payload)
        if resp.status_code != 200:
            print(f"[ERROR] Notion query failed {resp.status_code}: {resp.text[:300]}")
            sys.exit(1)
        data = resp.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        cursor = data.get("next_cursor")

    return pages


def extract_bug(page: dict) -> dict:
    """Extract relevant fields from a Notion page."""
    props = page.get("properties", {})

    def get_select(name):
        return ((props.get(name) or {}).get("select") or {}).get("name", "")

    def get_date(name):
        return ((props.get(name) or {}).get("date") or {}).get("start", "")

    def get_title(name):
        title_arr = (props.get(name) or {}).get("title") or []
        return title_arr[0].get("plain_text", "") if title_arr else ""

    def get_url(name):
        return (props.get(name) or {}).get("url") or ""

    def get_rollup_text(name):
        rollup = (props.get(name) or {}).get("rollup") or {}
        arr = rollup.get("array") or []
        parts = []
        for item in arr:
            sel = (item.get("select") or {}).get("name")
            if sel:
                parts.append(sel)
        return ", ".join(parts) if parts else ""

    return {
        "page_id": page["id"],
        "page_url": page.get("url", ""),
        "title": get_title("Issue Title"),
        "status": get_select("Status"),
        "severity": get_select("Severity"),
        "sla_status": get_select("SLA Status") or "On Track",
        "ticket_creation_date": get_date("Ticket creation date"),
        "triaged_at": get_date("Triaged At"),
        "triage_deadline": get_date("SLA Triage Deadline"),
        "resolution_deadline": get_date("SLA Resolution Deadline"),
        "linear_url": get_url("Linear Ticket URL"),
        "assigned_to": get_rollup_text("Assigned To"),
    }


def parse_dt(iso_str: str) -> Optional[datetime]:
    """Parse an ISO datetime or date string into a CET-aware datetime."""
    if not iso_str:
        return None
    try:
        if "T" in iso_str:
            dt = datetime.fromisoformat(iso_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=CET)
            return dt
        else:
            return datetime.strptime(iso_str, "%Y-%m-%d").replace(
                hour=9, minute=0, tzinfo=CET
            )
    except (ValueError, TypeError):
        return None


def patch_sla_status(page_id: str, new_status: str) -> bool:
    """Update the SLA Status field on a Notion page."""
    resp = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": {"SLA Status": {"select": {"name": new_status}}}},
    )
    if resp.status_code == 200:
        return True
    print(f"  [ERROR] PATCH {page_id[:8]}… failed {resp.status_code}: {resp.text[:200]}")
    return False


# ── Slack helper ──────────────────────────────────────────────────────────────
CS_OWNER_SLACK_IDS = {
    "Alex": "U0781C7B3UM",
    "Aya": "U08US7UFH62",
}


def format_deadline(dt: datetime) -> str:
    """Format a deadline datetime for Slack display."""
    return dt.strftime("%b %d, %Y at %H:%M CET")


def send_slack_notification(bug: dict, alert_type: str, deadline_dt: datetime, now: datetime):
    """Post an SLA notification to the #sla-alerts Slack channel."""
    if not SLA_SLACK_CHANNEL:
        print("  [WARN] SLA_SLACK_CHANNEL not set — skipping Slack notification")
        return

    title = bug["title"]
    linear_url = bug["linear_url"]
    assigned = bug["assigned_to"]
    page_url = bug["page_url"]

    # Mention the assigned CS owner
    mention = ""
    for name, slack_id in CS_OWNER_SLACK_IDS.items():
        if name in assigned:
            mention += f" <@{slack_id}>"

    # Build the title link
    title_link = f"<{page_url}|{title}>" if page_url else title
    linear_link = f"  (<{linear_url}|Linear>)" if linear_url else ""

    if "Warning" in alert_type:
        # 4h before deadline
        remaining = deadline_dt - now
        remaining_hrs = max(0, remaining.total_seconds() / 3600)
        emoji = ":warning:"
        sla_phase = "Triage" if "Triage" in alert_type else "Resolution"
        header = f"{emoji}  *SLA Warning — {sla_phase}*"
        time_line = f"*Time remaining:* ~{remaining_hrs:.0f} hours"
        deadline_label = "*Deadline:*"
        color = "#f2c744"  # yellow
    else:
        # 2h after deadline
        overdue = now - deadline_dt
        overdue_hrs = max(0, overdue.total_seconds() / 3600)
        emoji = ":rotating_light:"
        sla_phase = "Triage" if "Triage" in alert_type else "Resolution"
        header = f"{emoji}  *SLA BREACH — {sla_phase}*"
        time_line = f"*Overdue by:* ~{overdue_hrs:.0f} hours"
        deadline_label = "*Deadline was:*"
        color = "#e01e5a"  # red

    severity_line = f"*Severity:* {bug['severity']}" if bug["severity"] else ""

    text_body = "\n".join(filter(None, [
        f"*Bug:* {title_link}{linear_link}",
        f"*Assigned to:*{mention or ' (unassigned)'}",
        severity_line,
        f"{deadline_label} {format_deadline(deadline_dt)}",
        time_line,
    ]))

    payload = {
        "channel": SLA_SLACK_CHANNEL,
        "text": f"{header}\n{title}",  # fallback for notifications
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": header}},
            {"type": "section", "text": {"type": "mrkdwn", "text": text_body}},
        ],
    }

    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json",
        },
        json=payload,
    )

    if resp.status_code == 200 and resp.json().get("ok"):
        print(f"    Slack: {alert_type} sent")
    else:
        print(f"    [WARN] Slack send failed: {resp.text[:200]}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now = datetime.now(tz=CET)
    print(f"{'='*65}")
    print(f"  check_sla_deadlines.py  |  DRY_RUN={DRY_RUN}  |  {now.strftime('%Y-%m-%d %H:%M CET')}")
    print(f"{'='*65}\n")

    if not SLA_SLACK_CHANNEL:
        print("[WARN] SLA_SLACK_CHANNEL not configured — notifications will be skipped.\n")

    # ── 1. Fetch open bugs ────────────────────────────────────────────────────
    print("Fetching open bugs from Issues Table…")
    pages = fetch_open_bugs()
    print(f"  Found {len(pages)} open bug(s).\n")

    if not pages:
        print("No open bugs — nothing to check.")
        return

    # ── 2. Check each bug against SLA deadlines ──────────────────────────────
    warnings_sent = 0
    breaches_sent = 0
    on_track = 0

    print(f"{'Title':<50} {'Severity':<14} {'SLA Status':<20} Action")
    print("-" * 100)

    for page in pages:
        bug = extract_bug(page)
        current_sla = bug["sla_status"]
        title_short = bug["title"][:48]
        actions = []

        # ── Skip bugs without a Linear ticket (no SLA applies yet) ─────────
        if not bug["linear_url"]:
            action_str = "skip (no Linear ticket)"
            on_track += 1
            print(f"  {title_short:<50} {bug['severity']:<14} {bug['sla_status']:<20} {action_str}")
            continue

        # ── Backfill triage deadline if missing (from Linear ticket creation date)
        triage_deadline_dt = parse_dt(bug["triage_deadline"])
        if not triage_deadline_dt and bug["ticket_creation_date"]:
            ticket_dt = parse_dt(bug["ticket_creation_date"])
            if ticket_dt:
                triage_deadline_dt = add_business_days(ticket_dt, 1)
                if not DRY_RUN:
                    requests.patch(
                        f"https://api.notion.com/v1/pages/{bug['page_id']}",
                        headers=NOTION_HEADERS,
                        json={"properties": {
                            "SLA Triage Deadline": {"date": {"start": triage_deadline_dt.isoformat()}}
                        }},
                    )

        # ── Resolution SLA check (only track — no triage notifications) ────
        resolution_deadline_dt = parse_dt(bug["resolution_deadline"])
        triaged = bool(bug["triaged_at"])

        if triaged and resolution_deadline_dt:
            # Check resolution breach (2h after deadline)
            if now >= resolution_deadline_dt + timedelta(hours=2):
                if sla_level_below(current_sla, "Resolution Breach"):
                    actions.append(("Resolution Breach", resolution_deadline_dt))
                    current_sla = "Resolution Breach"

            # Check resolution warning (4h before deadline)
            elif now >= resolution_deadline_dt - timedelta(hours=4):
                if sla_level_below(current_sla, "Resolution Warning"):
                    actions.append(("Resolution Warning", resolution_deadline_dt))
                    current_sla = "Resolution Warning"

        # ── Apply actions ─────────────────────────────────────────────────
        if actions:
            # Use the highest-level action (last in the list since we check breach then warning)
            final_action, final_deadline = actions[-1]
            action_str = final_action

            if DRY_RUN:
                action_str += " (dry run)"
            else:
                # Send Slack notification for each new alert level
                for alert_type, deadline_dt in actions:
                    send_slack_notification(bug, alert_type, deadline_dt, now)

                # Update Notion SLA Status to the highest level reached
                patch_sla_status(bug["page_id"], final_action)

            if "Breach" in final_action:
                breaches_sent += 1
            else:
                warnings_sent += 1
        else:
            action_str = "on track"
            on_track += 1

        print(f"  {title_short:<50} {bug['severity']:<14} {bug['sla_status']:<20} {action_str}")

    print()
    print(f"Summary: {len(pages)} checked / {warnings_sent} warnings / {breaches_sent} breaches / {on_track} on track")

    if DRY_RUN and (warnings_sent + breaches_sent > 0):
        print(f"\nDRY RUN — no Slack messages sent and no Notion updates made.")


if __name__ == "__main__":
    main()
