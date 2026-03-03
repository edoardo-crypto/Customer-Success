#!/usr/bin/env python3
"""
sync_linear_status.py — Daily Linear → Notion status reconciliation

Runs every morning as a safety-net: fetches every Notion Issues Table row
that has a Linear Ticket URL (across ALL statuses), compares with the live
Linear state, and patches anything that has drifted.

The n8n webhook (xdVkUh6YCtcuW8QM) remains the real-time primary; this is
the fallback that catches missed events, network blips, or regressions.

State mapping (identical to webhook Code node logic — name-first):
  "testing"/"review"/"progress" in name → In Progress
  "done"/"released"/"complete"/"resolved" in name → Resolved
  type=started → In Progress  |  type=completed → Resolved
  anything else (backlog, unstarted, canceled) → skip

Usage:
  python3 sync_linear_status.py          # live (default)
  DRY_RUN=true python3 sync_linear_status.py  # dry run (preview only)
"""

import os
import re
import sys
import time
from datetime import date
from typing import Optional

import requests

# ── Credentials (env vars preferred; hardcoded as fallback for local runs) ─────
NOTION_TOKEN = os.environ.get(
    "NOTION_TOKEN",
    "***REMOVED***",
)
LINEAR_TOKEN = os.environ.get(
    "LINEAR_TOKEN",
    "***REMOVED***",
)

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
    if any(kw in name_lower for kw in ("done", "released", "complete", "resolved")):
        return "Resolved"

    if t == "started":
        return "In Progress"
    if t == "completed":
        return "Resolved"

    return None  # skip (backlog, unstarted, canceled, etc.)


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


def patch_notion_page(page_id: str, notion_status: str, resolved_at_empty: bool) -> bool:
    """
    PATCHes the Notion page Status.
    Sets Resolved At = today only when patching to Resolved AND field is currently empty.
    Returns True on success.
    """
    props: dict = {
        "Status": {"select": {"name": notion_status}},
    }
    if notion_status == "Resolved" and resolved_at_empty:
        props["Resolved At"] = {"date": {"start": TODAY}}

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
        nodes { identifier state { name type } }
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
            # Project URL or malformed — skip silently
            continue

        status_prop = (props.get("Status") or {}).get("select") or {}
        current_notion_status = status_prop.get("name", "")

        resolved_at_prop = (props.get("Resolved At") or {}).get("date") or {}
        resolved_at_empty = not resolved_at_prop.get("start")

        rows.append({
            "page_id": page_id,
            "identifier": identifier,
            "current_notion_status": current_notion_status,
            "resolved_at_empty": resolved_at_empty,
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
        target_status = map_linear_state(linear_name, linear_type)

        if target_status is None:
            action = "skip (no mapping)"
            skipped += 1
        elif target_status == current_status:
            action = f"skip (already {target_status})"
            skipped += 1
        else:
            action = f"→ {target_status}"
            to_patch.append({**row, "target_status": target_status})

        print(f"{ident:<14} {current_status:<12} {linear_name:<22} {linear_type:<14} {action}")

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
        target = item["target_status"]
        resolved_at_empty = item["resolved_at_empty"]
        print(f"  {ident} ({page_id[:8]}…) → {target}… ", end="", flush=True)
        if patch_notion_page(page_id, target, resolved_at_empty):
            print("OK")
            ok += 1
        time.sleep(0.3)

    print(f"\nSummary: {len(rows)} checked / {ok} patched / {skipped} skipped")


if __name__ == "__main__":
    main()
