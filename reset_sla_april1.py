#!/usr/bin/env python3
"""
reset_sla_april1.py — One-time: clean slate for SLA tracking from April 1.

Resets all existing open bugs so SLA timers start fresh from April 1, 2026:
  - Bugs in Triage  → triage deadline = April 2 (as if ticket just created)
  - Bugs past Triage → resolution deadline = April 1 + severity-based days

After running, archive to archive/.

Usage:
  DRY_RUN=true python3 reset_sla_april1.py   # preview
  python3 reset_sla_april1.py                 # live
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

SEVERITY_SLA_DAYS = {"Urgent": 1, "Important": 3, "Not important": 10}

# April 1, 2026 at 09:00 CET — the clean-slate start
APRIL_1 = datetime(2026, 4, 1, 9, 0, tzinfo=CET)
APRIL_1_ISO = APRIL_1.isoformat()


def add_business_days(start_dt: datetime, days: int) -> datetime:
    current = start_dt
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


# ── Notion ────────────────────────────────────────────────────────────────────
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
            print(f"[ERROR] {resp.status_code}: {resp.text[:300]}")
            sys.exit(1)
        data = resp.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        cursor = data.get("next_cursor")
    return pages


def patch_page(page_id: str, props: dict) -> bool:
    resp = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": props},
    )
    if resp.status_code == 200:
        return True
    print(f"  [ERROR] PATCH {page_id[:8]}… {resp.status_code}: {resp.text[:200]}")
    return False


# ── Linear ────────────────────────────────────────────────────────────────────
def fetch_linear_states(identifiers: list[str]) -> dict[str, dict]:
    if not identifiers:
        return {}
    by_team: dict[str, list[int]] = {}
    for ident in identifiers:
        m = re.match(r"^([A-Z]+)-(\d+)$", ident)
        if not m:
            continue
        by_team.setdefault(m.group(1), []).append(int(m.group(2)))

    query = """
    query($teamKey: String!, $numbers: [Float!]!) {
      issues(filter: {
        team: { key: { eq: $teamKey } },
        number: { in: $numbers }
      }) {
        nodes { identifier priority state { name type } }
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
            print(f"[ERROR] Linear: {resp.status_code}")
            sys.exit(1)
        for node in resp.json().get("data", {}).get("issues", {}).get("nodes", []):
            ident = node.get("identifier")
            state = node.get("state") or {}
            if ident:
                result[ident] = {
                    "type": state.get("type", ""),
                    "priority": node.get("priority", 0),
                }
    return result


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"{'='*65}")
    print(f"  reset_sla_april1.py  |  DRY_RUN={DRY_RUN}")
    print(f"  Clean slate: {APRIL_1_ISO}")
    print(f"{'='*65}\n")

    pages = fetch_open_bugs()
    print(f"Found {len(pages)} open bugs.\n")

    # Extract identifiers
    rows = []
    idents = []
    for page in pages:
        props = page.get("properties", {})
        url_val = (props.get("Linear Ticket URL") or {}).get("url") or ""
        m = re.search(r"/issue/([A-Z]+-\d+)", url_val)
        ident = m.group(1) if m else None

        severity = ((props.get("Severity") or {}).get("select") or {}).get("name", "")
        title_arr = (props.get("Issue Title") or {}).get("title") or []
        title = title_arr[0].get("plain_text", "") if title_arr else ""

        rows.append({
            "page_id": page["id"],
            "title": title,
            "identifier": ident,
            "severity": severity,
        })
        if ident:
            idents.append(ident)

    linear_data = fetch_linear_states(idents)
    print(f"Fetched {len(linear_data)} Linear states.\n")

    # Build patches
    PRIO_MAP = {1: "Urgent", 2: "Important", 3: "Not important", 4: "Not important"}
    triage_count = 0
    resolution_count = 0
    skipped = 0
    to_patch = []

    triage_deadline = add_business_days(APRIL_1, 1)  # April 2, 09:00 CET

    for row in rows:
        ident = row["identifier"]
        if not ident:
            skipped += 1
            continue

        linear = linear_data.get(ident)
        if not linear:
            skipped += 1
            continue

        is_triage = (linear["type"] or "").lower() == "triage"
        severity = row["severity"]

        # Sync severity from Linear if available
        linear_sev = PRIO_MAP.get(linear["priority"])
        if linear_sev:
            severity = linear_sev

        patch = {
            "SLA Status": {"select": {"name": "On Track"}},
            "Triage SLA Met": {"select": None},       # clear
            "Resolution SLA Met": {"select": None},    # clear
        }

        if linear_sev and linear_sev != row["severity"]:
            patch["Severity"] = {"select": {"name": linear_sev}}

        if is_triage:
            # Bug still in triage — timer starts April 1
            patch["Ticket creation date"] = {"date": {"start": APRIL_1_ISO}}
            patch["SLA Triage Deadline"] = {"date": {"start": triage_deadline.isoformat()}}
            # Clear resolution fields (not triaged yet)
            patch["Triaged At"] = {"date": None}
            patch["SLA Resolution Deadline"] = {"date": None}
            label = "TRIAGE"
            triage_count += 1
        else:
            # Bug past triage — resolution timer starts April 1
            patch["Triaged At"] = {"date": {"start": APRIL_1_ISO}}
            sla_days = SEVERITY_SLA_DAYS.get(severity, 3)
            res_deadline = add_business_days(APRIL_1, sla_days)
            patch["SLA Resolution Deadline"] = {"date": {"start": res_deadline.isoformat()}}
            label = f"RESOLUTION +{sla_days}bd ({severity})"
            resolution_count += 1

        to_patch.append({**row, "patch": patch, "label": label})

    # Print summary
    print(f"{'Title':<50} {'Identifier':<12} Reset")
    print("-" * 85)
    for item in to_patch:
        print(f"  {item['title'][:48]:<50} {item['identifier']:<12} {item['label']}")
    print()
    print(f"Triage resets: {triage_count}")
    print(f"Resolution resets: {resolution_count}")
    print(f"Skipped (no Linear): {skipped}")
    print()

    if DRY_RUN:
        print("DRY RUN — no changes made.")
        return

    print(f"Patching {len(to_patch)} pages…\n")
    ok = 0
    for item in to_patch:
        print(f"  {item['identifier']}… ", end="", flush=True)
        if patch_page(item["page_id"], item["patch"]):
            print("OK")
            ok += 1
        time.sleep(0.3)

    print(f"\nDone: {ok}/{len(to_patch)} patched successfully.")


if __name__ == "__main__":
    main()
