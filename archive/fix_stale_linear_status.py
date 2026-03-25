#!/usr/bin/env python3
"""
fix_stale_linear_status.py

Phase A (DRY_RUN=True): Discovers Notion Issues rows that are "Open" but have a
Linear Ticket URL, fetches their current Linear state, and prints a table showing
what would be patched.

Phase B (DRY_RUN=False): Actually PATCHes the Notion pages with the correct status.

Usage:
  python3 fix_stale_linear_status.py           # dry run (default)
  DRY_RUN=false python3 fix_stale_linear_status.py  # live patch
"""

import os
import re
import sys
import json
import time
from datetime import date
from typing import Optional

import requests
import creds

# ── Config ────────────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
NOTION_VERSION = "2022-06-28"
ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"

LINEAR_TOKEN = creds.get("LINEAR_TOKEN")
LINEAR_GQL = "https://api.linear.app/graphql"

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() not in ("false", "0", "no")

TODAY = date.today().isoformat()  # e.g. "2026-02-24"

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}
LINEAR_HEADERS = {
    "Authorization": LINEAR_TOKEN,
    "Content-Type": "application/json",
}


# ── State mapping (mirrors workflow Code node logic) ──────────────────────────
def map_linear_state(state_name: str, state_type: Optional[str]) -> Optional[str]:
    """
    Returns the target Notion status string, or None if no update is needed.
    Name-based checks take priority over type — e.g. "In Testing" (type: completed)
    must map to In Progress, not Resolved.
    """
    name_lower = state_name.lower() if state_name else ""
    t = (state_type or "").lower()

    # Name-based check takes priority over type
    if any(kw in name_lower for kw in ("progress", "review", "testing")):
        return "In Progress"
    if any(kw in name_lower for kw in ("done", "released", "complete", "resolved")):
        return "Resolved"

    # Type-based fallback
    if t == "started":
        return "In Progress"
    if t == "completed":
        return "Resolved"

    return None  # skip


# ── Notion helpers ─────────────────────────────────────────────────────────────
def fetch_open_issues_with_linear_url() -> list[dict]:
    """
    Queries the Issues Table for all pages where Status = Open AND
    Linear Ticket URL is not empty. Handles pagination.
    """
    url = f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query"
    payload = {
        "filter": {
            "and": [
                {
                    "property": "Status",
                    "select": {"equals": "Open"},
                },
                {
                    "property": "Linear Ticket URL",
                    "url": {"is_not_empty": True},
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
            print(f"[ERROR] Notion query failed {resp.status_code}: {resp.text}")
            sys.exit(1)

        data = resp.json()
        pages.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        cursor = data.get("next_cursor")

    return pages


def extract_linear_identifier(url_value: str) -> Optional[str]:
    """
    Parses a Linear issue URL like
    https://linear.app/konvoai/issue/ENG-124/some-title
    and returns the identifier (e.g. "ENG-124").
    """
    m = re.search(r"/issue/([A-Z]+-\d+)", url_value)
    return m.group(1) if m else None


def patch_notion_page(page_id: str, notion_status: str) -> bool:
    """
    PATCHes the Notion page Status (and optionally Resolved At).
    Returns True on success.
    """
    props: dict = {
        "Status": {"select": {"name": notion_status}},
    }
    if notion_status == "Resolved":
        props["Resolved At"] = {"date": {"start": TODAY}}

    resp = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": props},
    )
    if resp.status_code == 200:
        return True
    print(f"  [ERROR] PATCH page {page_id} failed {resp.status_code}: {resp.text[:200]}")
    return False


# ── Linear helper ──────────────────────────────────────────────────────────────
def fetch_linear_states(identifiers: list[str]) -> dict[str, dict]:
    """
    Fetches current state for a list of Linear identifiers (e.g. ["ENG-124", "KON-42"]).
    IssueFilter does not expose an `identifier` field, so we group by team key and
    query each team with a number `in` filter (one API call per team).
    Returns a dict: { "ENG-124": {"name": "In Testing", "type": "completed"}, ... }
    """
    if not identifiers:
        return {}

    # Group identifiers by team key: {"ENG": [124, 586], "KON": [42]}
    by_team: dict[str, list[int]] = {}
    num_map: dict[str, str] = {}  # "ENG-124" -> canonical identifier
    for ident in identifiers:
        m = re.match(r"^([A-Z]+)-(\d+)$", ident)
        if not m:
            print(f"  [WARN] Cannot parse team/number from '{ident}' — skipping")
            continue
        team_key, num = m.group(1), int(m.group(2))
        by_team.setdefault(team_key, []).append(num)
        num_map[f"{team_key}-{num}"] = ident

    result = {}
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
    print(f"  fix_stale_linear_status.py  |  DRY_RUN={DRY_RUN}")
    print(f"{'='*65}\n")

    # ── Phase A: Discovery ────────────────────────────────────────────────────
    print("Phase A: Fetching Open issues with Linear URL from Notion…")
    pages = fetch_open_issues_with_linear_url()
    print(f"  Found {len(pages)} candidate page(s).\n")

    if not pages:
        print("Nothing to do.")
        return

    # Build identifier → page_id map
    rows = []  # list of dicts
    identifiers = []

    for page in pages:
        page_id = page["id"]
        props = page.get("properties", {})

        # Extract Linear Ticket URL
        url_prop = props.get("Linear Ticket URL", {})
        linear_url = url_prop.get("url") or ""
        identifier = extract_linear_identifier(linear_url)
        if not identifier:
            print(f"  [WARN] Could not parse identifier from URL '{linear_url}' — skipping page {page_id}")
            continue

        # Current Notion status (should be "Open" by filter, but let's record it)
        status_prop = props.get("Status", {}).get("select") or {}
        current_notion_status = status_prop.get("name", "")

        rows.append({
            "page_id": page_id,
            "identifier": identifier,
            "current_notion_status": current_notion_status,
        })
        identifiers.append(identifier)

    if not identifiers:
        print("No valid identifiers found.")
        return

    # ── Query Linear for all identifiers at once ──────────────────────────────
    print(f"Querying Linear for {len(identifiers)} issue(s)…\n")
    linear_states = fetch_linear_states(identifiers)

    # ── Build results table ───────────────────────────────────────────────────
    print(f"{'Identifier':<14} {'Notion':<10} {'Linear State':<22} {'Linear Type':<14} Action")
    print("-" * 80)

    to_patch = []

    for row in rows:
        ident = row["identifier"]
        current_status = row["current_notion_status"]

        linear_info = linear_states.get(ident)
        if not linear_info:
            print(f"{ident:<14} {current_status:<10} {'(not found in Linear)':<22} {'—':<14} skip (not found)")
            continue

        linear_name = linear_info["name"]
        linear_type = linear_info["type"]
        target_status = map_linear_state(linear_name, linear_type)

        if target_status is None:
            action = "skip (no change needed)"
        elif target_status == current_status:
            action = f"skip (already {target_status})"
        else:
            action = f"→ patch {target_status}"
            to_patch.append({**row, "target_status": target_status})

        print(f"{ident:<14} {current_status:<10} {linear_name:<22} {linear_type:<14} {action}")

    print()

    # ── Phase B: Patch ────────────────────────────────────────────────────────
    if not to_patch:
        print("No pages need patching.")
        return

    if DRY_RUN:
        print(f"DRY RUN — {len(to_patch)} page(s) would be patched. Set DRY_RUN=false to apply.")
        return

    print(f"Phase B: Patching {len(to_patch)} Notion page(s)…\n")
    ok = 0
    for item in to_patch:
        ident = item["identifier"]
        page_id = item["page_id"]
        target = item["target_status"]
        print(f"  Patching {ident} ({page_id[:8]}…) → {target}… ", end="", flush=True)
        success = patch_notion_page(page_id, target)
        if success:
            print("OK")
            ok += 1
        time.sleep(0.3)  # be polite to Notion rate limits

    print(f"\nDone: {ok}/{len(to_patch)} pages updated successfully.")


if __name__ == "__main__":
    main()
