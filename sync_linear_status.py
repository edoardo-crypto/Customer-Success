#!/usr/bin/env python3
"""
sync_linear_status.py — Daily Linear → Notion reconciliation

Runs every morning as a safety-net: fetches every Notion Issues Table row
that has a Linear Ticket URL (across ALL statuses), compares with the live
Linear state, and patches anything that has drifted.

Syncs three dimensions:
  1. Status  — Linear state → Notion Status (In Progress / Resolved / Deprioritized)
  2. Priority — Linear priority → Notion Severity (Urgent / Important / Not important)
  3. Triage  — detects triage exit in Linear, sets Triaged At + computes SLA deadlines

State mapping (identical to webhook Code node logic — name-first):
  "testing"/"review"/"progress" in name → In Progress  (NOT "scoped" — stays Open)
  "done"/"released"/"complete"/"resolved"/"duplicate" in name → Resolved
  "cancel" in name → Deprioritized
  type=started → In Progress  |  type=completed → Resolved  |  type=cancelled → Deprioritized
  anything else (backlog, unstarted) → skip

Priority mapping (Linear → Notion Severity):
  Urgent (1) → Urgent  |  High (2) → Important  |  Medium (3) / Low (4) → Not important

SLA deadlines (business days, Mon–Fri, CET):
  Triage: Created At + 1 business day
  Resolution: Triaged At + severity-dependent (Urgent=1, Important=3, Not important=10)

Usage:
  python3 sync_linear_status.py          # live (default)
  DRY_RUN=true python3 sync_linear_status.py  # dry run (preview only)
"""

import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import requests
import creds

# ── Credentials (env vars preferred; hardcoded as fallback for local runs) ─────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
LINEAR_TOKEN = creds.get("LINEAR_TOKEN")

NOTION_VERSION = "2022-06-28"
ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"
LINEAR_GQL = "https://api.linear.app/graphql"

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() not in ("false", "0", "no")

TODAY = date.today().isoformat()

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}
LINEAR_HEADERS = {
    "Authorization": LINEAR_TOKEN,
    "Content-Type": "application/json",
}

# ── SLA constants ─────────────────────────────────────────────────────────────
CET = ZoneInfo("Europe/Amsterdam")

# Linear priority (int) → Notion Severity (select value)
PRIORITY_TO_SEVERITY = {1: "Urgent", 2: "Important", 3: "Not important", 4: "Not important"}

# Notion Severity → resolution SLA in business days
SEVERITY_SLA_DAYS = {"Urgent": 1, "Important": 3, "Not important": 10}


def add_business_days(start_dt: datetime, days: int) -> datetime:
    """Add N business days (Mon-Fri) to a datetime, preserving time-of-day."""
    current = start_dt
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Mon=0 … Fri=4
            added += 1
    return current


def parse_triaged_at(iso_str: str) -> Optional[datetime]:
    """Parse an ISO datetime/date string into a CET-aware datetime."""
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


# ── State mapping ──────────────────────────────────────────────────────────────
def map_linear_state(state_name: str, state_type: Optional[str]) -> Optional[str]:
    """
    Returns target Notion status string, or None to skip.
    Name-based checks take priority — e.g. "In Testing" (type: completed)
    maps to In Progress, not Resolved.
    """
    name_lower = (state_name or "").lower()
    t = (state_type or "").lower()

    if any(kw in name_lower for kw in ("progress", "review", "testing")):
        return "In Progress"
    if any(kw in name_lower for kw in ("done", "released", "complete", "resolved", "duplicate")):
        return "Resolved"
    if "cancel" in name_lower:
        return "Deprioritized"

    if t == "started":
        return "In Progress"
    if t == "completed":
        return "Resolved"
    if t == "cancelled":
        return "Deprioritized"

    return None  # skip (backlog, unstarted, etc.)


# ── Notion helpers ─────────────────────────────────────────────────────────────
def fetch_issues_with_linear_url() -> list[dict]:
    """
    Queries the Issues Table for all pages where Linear Ticket URL is not empty,
    regardless of current Status. Handles pagination.
    """
    url = f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query"
    payload = {
        "filter": {
            "property": "Linear Ticket URL",
            "url": {"is_not_empty": True},
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
            print(f"[ERROR] Notion query failed {resp.status_code}: {resp.text}")
            sys.exit(1)

        data = resp.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        cursor = data.get("next_cursor")

    return pages


def extract_linear_identifier(url_value: str) -> Optional[str]:
    """
    Parses a Linear issue URL and returns the identifier (e.g. "ENG-124").
    Returns None for project URLs (no /issue/ in path).
    """
    m = re.search(r"/issue/([A-Z]+-\d+)", url_value)
    return m.group(1) if m else None


def patch_notion_page(page_id: str, props: dict) -> bool:
    """PATCHes the Notion page with arbitrary properties. Returns True on success."""
    resp = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": props},
    )
    if resp.status_code == 200:
        return True
    print(f"  [ERROR] PATCH {page_id[:8]}… failed {resp.status_code}: {resp.text[:200]}")
    return False


# ── Linear helper ──────────────────────────────────────────────────────────────
def fetch_linear_states(identifiers: list[str]) -> dict[str, dict]:
    """
    Batch-fetches current state for all identifiers, grouped by team key.
    One GraphQL call per team (e.g. one for ENG-*, one for KON-*).
    Returns { "ENG-124": {"name": "In Testing", "type": "completed"}, ... }
    """
    if not identifiers:
        return {}

    by_team: dict[str, list[int]] = {}
    for ident in identifiers:
        m = re.match(r"^([A-Z]+)-(\d+)$", ident)
        if not m:
            print(f"  [WARN] Cannot parse '{ident}' — skipping")
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
            print(f"[ERROR] Linear API failed for team {team_key}: {resp.status_code} {resp.text[:200]}")
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


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print(f"{'='*65}")
    print(f"  sync_linear_status.py  |  DRY_RUN={DRY_RUN}  |  {TODAY}")
    print(f"{'='*65}\n")

    # ── 1. Fetch all issues with a Linear URL ─────────────────────────────────
    print("Fetching Issues Table rows with a Linear Ticket URL…")
    pages = fetch_issues_with_linear_url()
    print(f"  Found {len(pages)} candidate page(s).\n")

    if not pages:
        print("Nothing to check.")
        return

    # ── 2. Build rows list ────────────────────────────────────────────────────
    rows = []
    identifiers = []

    for page in pages:
        page_id = page["id"]
        props = page.get("properties", {})

        url_prop = props.get("Linear Ticket URL", {})
        linear_url = url_prop.get("url") or ""
        identifier = extract_linear_identifier(linear_url)
        if not identifier:
            continue

        status_prop = (props.get("Status") or {}).get("select") or {}
        current_notion_status = status_prop.get("name", "")

        resolved_at_prop = (props.get("Resolved At") or {}).get("date") or {}
        resolved_at_empty = not resolved_at_prop.get("start")

        severity_prop = (props.get("Severity") or {}).get("select") or {}
        current_severity = severity_prop.get("name", "")

        triaged_at_prop = (props.get("Triaged At") or {}).get("date") or {}
        triaged_at_str = triaged_at_prop.get("start", "")
        triaged_at_empty = not triaged_at_str

        triage_deadline_prop = (props.get("SLA Triage Deadline") or {}).get("date") or {}
        triage_deadline_str = triage_deadline_prop.get("start", "")
        triage_deadline_empty = not triage_deadline_str

        resolution_deadline_prop = (props.get("SLA Resolution Deadline") or {}).get("date") or {}
        resolution_deadline_str = resolution_deadline_prop.get("start", "")
        resolution_deadline_empty = not resolution_deadline_str

        ticket_creation_prop = (props.get("Ticket creation date") or {}).get("date") or {}
        ticket_creation_str = ticket_creation_prop.get("start", "")

        rows.append({
            "page_id": page_id,
            "identifier": identifier,
            "current_notion_status": current_notion_status,
            "resolved_at_empty": resolved_at_empty,
            "current_severity": current_severity,
            "triaged_at_str": triaged_at_str,
            "triaged_at_empty": triaged_at_empty,
            "triage_deadline_str": triage_deadline_str,
            "triage_deadline_empty": triage_deadline_empty,
            "resolution_deadline_str": resolution_deadline_str,
            "resolution_deadline_empty": resolution_deadline_empty,
            "ticket_creation_str": ticket_creation_str,
        })
        identifiers.append(identifier)

    if not identifiers:
        print("No valid issue identifiers found.")
        return

    # ── 3. Batch-fetch Linear states ──────────────────────────────────────────
    print(f"Querying Linear for {len(identifiers)} issue(s)…\n")
    linear_states = fetch_linear_states(identifiers)

    # ── 4. Compare and build patch list ───────────────────────────────────────
    print(f"{'Identifier':<14} {'Notion':<12} {'Linear State':<22} {'Type':<14} Action")
    print("-" * 82)

    to_patch = []
    skipped = 0

    now_iso = datetime.now(tz=CET).isoformat()

    for row in rows:
        ident = row["identifier"]
        current_status = row["current_notion_status"]

        linear_info = linear_states.get(ident)
        if not linear_info:
            print(f"{ident:<14} {current_status:<12} {'(not found in Linear)':<22} {'—':<14} skip")
            skipped += 1
            continue

        linear_name = linear_info["name"]
        linear_type = linear_info["type"]
        linear_priority = linear_info["priority"]
        linear_created_at = linear_info["createdAt"]
        target_status = map_linear_state(linear_name, linear_type)

        actions = []
        patch_props = {}

        # ── Sync Linear createdAt → Notion Ticket creation date ───────────
        if linear_created_at and not row["ticket_creation_str"]:
            patch_props["Ticket creation date"] = {"date": {"start": linear_created_at}}
            actions.append("ticket_date synced")

        # ── Status sync (existing logic) ──────────────────────────────────
        if target_status is not None and target_status != current_status:
            patch_props["Status"] = {"select": {"name": target_status}}
            if target_status == "Resolved" and row["resolved_at_empty"]:
                patch_props["Resolved At"] = {"date": {"start": TODAY}}
                # Resolution SLA Met? Compare resolve moment vs resolution deadline
                if row["resolution_deadline_str"]:
                    res_dl_dt = parse_triaged_at(row["resolution_deadline_str"])
                    if res_dl_dt:
                        now_dt_check = datetime.now(tz=CET)
                        met = "Yes" if now_dt_check <= res_dl_dt else "No"
                        patch_props["Resolution SLA Met"] = {"select": {"name": met}}
                        actions.append(f"res_sla={met}")
            actions.append(f"status → {target_status}")

        # ── Priority → Severity sync ──────────────────────────────────────
        target_severity = PRIORITY_TO_SEVERITY.get(linear_priority)
        if target_severity and target_severity != row["current_severity"]:
            patch_props["Severity"] = {"select": {"name": target_severity}}
            actions.append(f"severity → {target_severity}")

        # ── Triage exit detection ─────────────────────────────────────────
        # If Linear state is no longer "triage" and Notion has no Triaged At,
        # mark triage as complete and compute resolution deadline.
        is_still_triage = (linear_type or "").lower() == "triage"
        if not is_still_triage and row["triaged_at_empty"]:
            patch_props["Triaged At"] = {"date": {"start": now_iso}}
            actions.append("triaged_at = now")

            # Reset SLA Status — triage phase is complete, start fresh for resolution
            patch_props["SLA Status"] = {"select": {"name": "On Track"}}
            actions.append("sla_status → On Track")

            # Triage SLA Met? Compare now (triage moment) vs triage deadline
            if not row["triage_deadline_empty"]:
                triage_dl_dt = parse_triaged_at(row.get("triage_deadline_str", ""))
                now_dt_check = datetime.now(tz=CET)
                if triage_dl_dt:
                    met = "Yes" if now_dt_check <= triage_dl_dt else "No"
                    patch_props["Triage SLA Met"] = {"select": {"name": met}}
                    actions.append(f"triage_sla={met}")

            # Compute resolution deadline from severity
            sev = target_severity or row["current_severity"]
            sla_days = SEVERITY_SLA_DAYS.get(sev)
            if sla_days and row["resolution_deadline_empty"]:
                now_dt = datetime.now(tz=CET)
                deadline = add_business_days(now_dt, sla_days)
                patch_props["SLA Resolution Deadline"] = {
                    "date": {"start": deadline.isoformat()}
                }
                actions.append(f"res_deadline = +{sla_days}bd")

        # ── Triage deadline (from Linear ticket creation, not Notion Created At)
        if row["triage_deadline_empty"]:
            # Use Linear createdAt as the SLA start (when eng received the ticket)
            ticket_date_str = linear_created_at or row["ticket_creation_str"]
            if ticket_date_str:
                try:
                    ticket_dt = parse_triaged_at(ticket_date_str)  # reuse ISO parser
                    if ticket_dt:
                        triage_deadline = add_business_days(ticket_dt, 1)
                        patch_props["SLA Triage Deadline"] = {
                            "date": {"start": triage_deadline.isoformat()}
                        }
                        actions.append("triage_deadline set")
                except (ValueError, TypeError):
                    pass

        # ── Resolution deadline recalc on severity change ─────────────────
        # Fires when: severity changed AND bug is already triaged (but triage
        # exit block above didn't fire, i.e. triaged_at was already set).
        if (target_severity and target_severity != row["current_severity"]
                and not row["triaged_at_empty"]):
            sla_days = SEVERITY_SLA_DAYS.get(target_severity)
            if sla_days:
                # Use actual Triaged At as the base, not now
                triaged_dt = parse_triaged_at(row["triaged_at_str"])
                if triaged_dt:
                    deadline = add_business_days(triaged_dt, sla_days)
                    patch_props["SLA Resolution Deadline"] = {
                        "date": {"start": deadline.isoformat()}
                    }
                    actions.append(f"res_deadline recalc → triaged_at+{sla_days}bd")

        # ── Log ───────────────────────────────────────────────────────────
        if patch_props:
            to_patch.append({**row, "patch_props": patch_props, "actions": actions})
            action_str = "; ".join(actions)
        elif target_status is None:
            action_str = "skip (no mapping)"
            skipped += 1
        else:
            action_str = "skip (in sync)"
            skipped += 1

        print(f"{ident:<14} {current_status:<12} {linear_name:<22} {linear_type:<14} {action_str}")

    print()

    # ── 5. Patch ──────────────────────────────────────────────────────────────
    if not to_patch:
        print(f"Summary: {len(rows)} checked / 0 patched / {skipped} skipped — all in sync.")
        return

    if DRY_RUN:
        print(f"DRY RUN — {len(to_patch)} page(s) would be patched. Set DRY_RUN=false to apply.")
        print(f"Summary: {len(rows)} checked / {len(to_patch)} would patch / {skipped} skipped")
        return

    print(f"Patching {len(to_patch)} page(s)…\n")
    ok = 0
    for item in to_patch:
        ident = item["identifier"]
        page_id = item["page_id"]
        action_str = "; ".join(item["actions"])
        print(f"  {ident} ({page_id[:8]}…) {action_str}… ", end="", flush=True)
        if patch_notion_page(page_id, item["patch_props"]):
            print("OK")
            ok += 1
        time.sleep(0.3)

    print(f"\nSummary: {len(rows)} checked / {ok} patched / {skipped} skipped")


if __name__ == "__main__":
    main()
