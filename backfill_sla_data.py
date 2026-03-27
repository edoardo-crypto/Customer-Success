#!/usr/bin/env python3
"""
backfill_sla_data.py — One-time: initialize SLA data for all existing open bugs.

For each open/in-progress bug:
  1. Sets SLA Triage Deadline = Created At + 1 business day
  2. If it has a Linear ticket that's out of Triage → sets Triaged At + SLA Resolution Deadline
  3. Syncs Linear priority → Notion Severity
  4. Sets SLA Status appropriately (breach level for already-overdue bugs to avoid Slack flood)

After running, archive to archive/.

Usage:
  python3 backfill_sla_data.py               # live
  DRY_RUN=true python3 backfill_sla_data.py  # preview only
"""

import os
import re
import sys
import time
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import requests
import creds

NOTION_TOKEN = creds.get("NOTION_TOKEN")
LINEAR_TOKEN = creds.get("LINEAR_TOKEN")

NOTION_VERSION = "2022-06-28"
ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"
LINEAR_GQL = "https://api.linear.app/graphql"
CET = ZoneInfo("Europe/Amsterdam")

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() not in ("false", "0", "no")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}
LINEAR_HEADERS = {
    "Authorization": LINEAR_TOKEN,
    "Content-Type": "application/json",
}

PRIORITY_TO_SEVERITY = {1: "Urgent", 2: "Important", 3: "Not important", 4: "Not important"}
SEVERITY_SLA_DAYS = {"Urgent": 1, "Important": 3, "Not important": 10}

SLA_LEVELS = ["On Track", "Triage Warning", "Triage Breach",
              "Resolution Warning", "Resolution Breach"]
SLA_LEVEL_INDEX = {name: i for i, name in enumerate(SLA_LEVELS)}


def add_business_days(start_dt: datetime, days: int) -> datetime:
    """Add N business days (Mon-Fri) to a datetime, preserving time-of-day."""
    current = start_dt
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def parse_dt(iso_str: str) -> Optional[datetime]:
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


# ── Notion helpers ────────────────────────────────────────────────────────────
def fetch_open_bugs() -> list[dict]:
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


def extract_linear_identifier(url_value: str) -> Optional[str]:
    m = re.search(r"/issue/([A-Z]+-\d+)", url_value)
    return m.group(1) if m else None


def patch_notion_page(page_id: str, props: dict) -> bool:
    resp = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": props},
    )
    if resp.status_code == 200:
        return True
    print(f"  [ERROR] PATCH {page_id[:8]}… failed {resp.status_code}: {resp.text[:200]}")
    return False


# ── Linear helpers ────────────────────────────────────────────────────────────
def fetch_linear_issues(identifiers: list[str]) -> dict[str, dict]:
    """Batch-fetch state + priority for all Linear identifiers."""
    if not identifiers:
        return {}

    by_team: dict[str, list[int]] = {}
    for ident in identifiers:
        m = re.match(r"^([A-Z]+)-(\d+)$", ident)
        if not m:
            continue
        team_key, num = m.group(1), int(m.group(2))
        by_team.setdefault(team_key, []).append(num)

    query = """
    query($teamKey: String!, $numbers: [Float!]!) {
      issues(filter: {
        team: { key: { eq: $teamKey } },
        number: { in: $numbers }
      }) {
        nodes { identifier priority createdAt state { name type } }
      }
    }
    """

    result = {}
    for team_key, numbers in by_team.items():
        resp = requests.post(
            LINEAR_GQL,
            headers=LINEAR_HEADERS,
            json={"query": query, "variables": {"teamKey": team_key, "numbers": numbers}},
        )
        if resp.status_code != 200:
            print(f"[ERROR] Linear API failed: {resp.status_code} {resp.text[:200]}")
            sys.exit(1)
        nodes = resp.json().get("data", {}).get("issues", {}).get("nodes", [])
        for node in nodes:
            ident = node.get("identifier")
            state = node.get("state") or {}
            if ident:
                result[ident] = {
                    "name": state.get("name", ""),
                    "type": state.get("type", ""),
                    "priority": node.get("priority", 0),
                    "createdAt": node.get("createdAt", ""),
                }
    return result


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now = datetime.now(tz=CET)
    print(f"{'='*65}")
    print(f"  backfill_sla_data.py  |  DRY_RUN={DRY_RUN}  |  {now.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*65}\n")

    # ── 1. Fetch open bugs ────────────────────────────────────────────────────
    print("Fetching open bugs…")
    pages = fetch_open_bugs()
    print(f"  Found {len(pages)} open bug(s).\n")

    if not pages:
        print("Nothing to backfill.")
        return

    # ── 2. Extract rows and gather Linear identifiers ─────────────────────────
    rows = []
    identifiers = []

    for page in pages:
        props = page.get("properties", {})

        def get_select(name):
            return ((props.get(name) or {}).get("select") or {}).get("name", "")

        def get_date(name):
            return ((props.get(name) or {}).get("date") or {}).get("start", "")

        def get_url(name):
            return (props.get(name) or {}).get("url") or ""

        def get_title(name):
            arr = (props.get(name) or {}).get("title") or []
            return arr[0].get("plain_text", "") if arr else ""

        linear_url = get_url("Linear Ticket URL")
        identifier = extract_linear_identifier(linear_url) if linear_url else None

        row = {
            "page_id": page["id"],
            "title": get_title("Issue Title"),
            "identifier": identifier,
            "ticket_creation_str": get_date("Ticket creation date"),
            "severity": get_select("Severity"),
            "triaged_at": get_date("Triaged At"),
            "triage_deadline": get_date("SLA Triage Deadline"),
            "resolution_deadline": get_date("SLA Resolution Deadline"),
            "sla_status": get_select("SLA Status"),
        }
        rows.append(row)
        if identifier:
            identifiers.append(identifier)

    # ── 3. Fetch Linear data ──────────────────────────────────────────────────
    print(f"Querying Linear for {len(identifiers)} linked issue(s)…\n")
    linear_data = fetch_linear_issues(identifiers)

    # ── 4. Compute patches ────────────────────────────────────────────────────
    to_patch = []

    for row in rows:
        patch_props = {}
        actions = []
        ident = row["identifier"]

        # ── Linear-dependent fields ───────────────────────────────────────
        linear_info = linear_data.get(ident) if ident else None

        # ── Sync Linear createdAt → Ticket creation date ─────────────────
        if linear_info and linear_info.get("createdAt") and not row["ticket_creation_str"]:
            patch_props["Ticket creation date"] = {"date": {"start": linear_info["createdAt"]}}
            actions.append("ticket_date synced")

        # ── Triage deadline (from Linear ticket creation, not Notion Created At)
        if not row["triage_deadline"]:
            ticket_date_str = (linear_info or {}).get("createdAt") or row["ticket_creation_str"]
            if ticket_date_str:
                ticket_dt = parse_dt(ticket_date_str)
                if ticket_dt:
                    triage_dl = add_business_days(ticket_dt, 1)
                    patch_props["SLA Triage Deadline"] = {"date": {"start": triage_dl.isoformat()}}
                    actions.append("triage_deadline")

        if linear_info:
            # Priority → Severity
            target_sev = PRIORITY_TO_SEVERITY.get(linear_info["priority"])
            if target_sev and target_sev != row["severity"]:
                patch_props["Severity"] = {"select": {"name": target_sev}}
                actions.append(f"severity → {target_sev}")
                effective_severity = target_sev
            else:
                effective_severity = row["severity"]

            # Triage exit
            is_triage = (linear_info["type"] or "").lower() == "triage"
            if not is_triage and not row["triaged_at"]:
                # Use current time as approximation
                patch_props["Triaged At"] = {"date": {"start": now.isoformat()}}
                actions.append("triaged_at")

                # Resolution deadline
                if not row["resolution_deadline"]:
                    sla_days = SEVERITY_SLA_DAYS.get(effective_severity)
                    if sla_days:
                        res_dl = add_business_days(now, sla_days)
                        patch_props["SLA Resolution Deadline"] = {
                            "date": {"start": res_dl.isoformat()}
                        }
                        actions.append(f"res_deadline +{sla_days}bd")

        # ── SLA Status for already-overdue bugs ───────────────────────────
        if not row["sla_status"] or row["sla_status"] == "On Track":
            # Check if triage is already breached
            triage_dl_str = patch_props.get("SLA Triage Deadline", {}).get("date", {}).get("start") or row["triage_deadline"]
            triage_dl_dt = parse_dt(triage_dl_str)
            still_in_triage = not row["triaged_at"] and "Triaged At" not in patch_props

            if still_in_triage and triage_dl_dt and now >= triage_dl_dt + timedelta(hours=2):
                patch_props["SLA Status"] = {"select": {"name": "Triage Breach"}}
                actions.append("sla=Triage Breach")
            elif still_in_triage and triage_dl_dt and now >= triage_dl_dt - timedelta(hours=4):
                patch_props["SLA Status"] = {"select": {"name": "Triage Warning"}}
                actions.append("sla=Triage Warning")
            else:
                # Check resolution SLA
                res_dl_str = patch_props.get("SLA Resolution Deadline", {}).get("date", {}).get("start") or row["resolution_deadline"]
                res_dl_dt = parse_dt(res_dl_str)
                triaged = bool(row["triaged_at"]) or "Triaged At" in patch_props

                if triaged and res_dl_dt and now >= res_dl_dt + timedelta(hours=2):
                    patch_props["SLA Status"] = {"select": {"name": "Resolution Breach"}}
                    actions.append("sla=Resolution Breach")
                elif triaged and res_dl_dt and now >= res_dl_dt - timedelta(hours=4):
                    patch_props["SLA Status"] = {"select": {"name": "Resolution Warning"}}
                    actions.append("sla=Resolution Warning")

        # ── Collect ───────────────────────────────────────────────────────
        if patch_props:
            to_patch.append({
                "page_id": row["page_id"],
                "title": row["title"],
                "identifier": ident,
                "patch_props": patch_props,
                "actions": actions,
            })

    # ── 5. Apply patches ──────────────────────────────────────────────────────
    print(f"{'Title':<50} {'Identifier':<12} Actions")
    print("-" * 90)
    for item in to_patch:
        title_short = item["title"][:48]
        ident = item["identifier"] or "—"
        action_str = "; ".join(item["actions"])
        print(f"  {title_short:<50} {ident:<12} {action_str}")
    print()

    if not to_patch:
        print(f"Summary: {len(rows)} checked / 0 need patching — all set.")
        return

    if DRY_RUN:
        print(f"DRY RUN — {len(to_patch)} page(s) would be patched.")
        return

    print(f"Patching {len(to_patch)} page(s)…\n")
    ok = 0
    for item in to_patch:
        title_short = item["title"][:30]
        print(f"  {title_short}… ", end="", flush=True)
        if patch_notion_page(item["page_id"], item["patch_props"]):
            print("OK")
            ok += 1
        time.sleep(0.3)

    print(f"\nSummary: {len(rows)} checked / {ok} patched / {len(rows) - ok} unchanged")


if __name__ == "__main__":
    main()
