#!/usr/bin/env python3
"""
fix_cs_table_structure.py — Idempotent fix: ensure every non-Canceled customer page
has the correct CS section structure (emoji headings + correct columns).

Detection per page (one GET):
  • First heading_2 starts with "🚧"  → already correct → SKIP
  • First heading_2 exists but no emoji → old structure → delete all + append new
  • No blocks at all                   → empty page     → append new only

Safe to kill and rerun — already-done pages are detected and skipped.

Also adds "Reason" rich_text property to CS Success Criteria DB (idempotent PATCH).

Usage:  python3 fix_cs_table_structure.py
Archive to archive/ after run.
"""

import os
import time

import requests
import creds

NOTION_TOKEN = creds.get("NOTION_TOKEN")
MCT_DS_ID   = "3ceb1ad0-91f1-40db-945a-c51c58035898"
DB_CRITERIA = "b938e808-b10b-4bbc-9dde-ec7a1c74b1ff"

NOTION_API = "https://api.notion.com/v1"

HEADERS_MCT = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2025-09-03",
}
HEADERS_BLOCKS = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}


# ── Block builders ──────────────────────────────────────────────────────────────

def _cell(text=""):
    if text:
        return [{"type": "text", "text": {"content": text}}]
    return []


def _table_block(headers):
    n = len(headers)
    header_row = {"object": "block", "type": "table_row",
                  "table_row": {"cells": [_cell(h) for h in headers]}}
    empty_row  = {"object": "block", "type": "table_row",
                  "table_row": {"cells": [_cell() for _ in headers]}}
    return {
        "object": "block", "type": "table",
        "table": {
            "table_width": n,
            "has_column_header": True,
            "has_row_header": False,
            "children": [header_row, empty_row],
        },
    }


def _heading_2(text):
    return {
        "object": "block", "type": "heading_2",
        "heading_2": {"rich_text": [{"type": "text", "text": {"content": text}}]},
    }


_DIVIDER   = {"object": "block", "type": "divider", "divider": {}}
_PARAGRAPH = {"object": "block", "type": "paragraph", "paragraph": {"rich_text": []}}


def new_section_blocks():
    return [
        _heading_2("🚧 Blockers & Next Actions"),
        _table_block(["Blocker", "Next Action", "Status"]),
        _DIVIDER,
        _heading_2("🎯 Success Criteria"),
        _table_block(["Criterion", "Confirmed (y/n)", "Reason"]),
        _DIVIDER,
        _heading_2("📝 Notes"),
        _PARAGRAPH,
    ]


# ── Notion helpers ──────────────────────────────────────────────────────────────

def fetch_active_pages():
    pages, cursor = [], None
    while True:
        body = {
            "page_size": 100,
            "filter": {"property": "💰 Billing Status",
                       "select": {"does_not_equal": "Canceled"}},
        }
        if cursor:
            body["start_cursor"] = cursor
        resp = requests.post(f"{NOTION_API}/data_sources/{MCT_DS_ID}/query",
                             headers=HEADERS_MCT, json=body)
        resp.raise_for_status()
        data = resp.json()
        for page in data.get("results", []):
            pid = page["id"]
            title_parts = page["properties"].get("🏢 Company Name", {}).get("title", [])
            name = "".join(t.get("plain_text", "") for t in title_parts).strip() or f"(unnamed {pid[:8]})"
            pages.append({"page_id": pid, "company_name": name})
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


def get_top_blocks(page_id):
    """Return list of top-level block dicts (id + type + text snippet)."""
    blocks, cursor = [], None
    while True:
        params = {"page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        resp = requests.get(f"{NOTION_API}/blocks/{page_id}/children",
                            headers=HEADERS_BLOCKS, params=params)
        resp.raise_for_status()
        data = resp.json()
        blocks.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return blocks


def detect_page_state(blocks):
    """
    Returns:
      "correct" — first heading_2 starts with "🚧"
      "old"     — has blocks but first heading_2 lacks emoji (or first block isn't heading_2)
      "empty"   — no blocks at all
    """
    if not blocks:
        return "empty"
    for b in blocks:
        if b.get("type") == "heading_2":
            rich = b.get("heading_2", {}).get("rich_text", [])
            text = "".join(t.get("plain_text", "") for t in rich)
            if text.startswith("🚧"):
                return "correct"
            return "old"
    # Has blocks but no heading_2 found at all → treat as old
    return "old"


def delete_block(block_id):
    requests.delete(f"{NOTION_API}/blocks/{block_id}", headers=HEADERS_BLOCKS).raise_for_status()


def append_blocks(page_id, blocks):
    resp = requests.patch(f"{NOTION_API}/blocks/{page_id}/children",
                          headers=HEADERS_BLOCKS, json={"children": blocks})
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")


def add_reason_property():
    """Idempotent: add 'Reason' rich_text to CS Success Criteria DB."""
    resp = requests.patch(
        f"{NOTION_API}/databases/{DB_CRITERIA}",
        headers=HEADERS_BLOCKS,
        json={"properties": {"Reason": {"rich_text": {}}}},
    )
    if resp.ok:
        print("  [DB] 'Reason' property ensured on CS Success Criteria DB")
    else:
        print(f"  [DB WARN] {resp.status_code}: {resp.text[:200]}")


# ── Main ────────────────────────────────────────────────────────────────────────

def main():
    print("Step 0 — Ensuring 'Reason' property on Success Criteria DB…")
    add_reason_property()
    print()

    print("Fetching non-Canceled customer pages…")
    pages = fetch_active_pages()
    total = len(pages)
    print(f"  → {total} pages\n")

    ok = skip = fail = 0

    for i, p in enumerate(pages, 1):
        pid  = p["page_id"]
        name = p["company_name"]

        try:
            blocks = get_top_blocks(pid)
            time.sleep(0.1)
        except Exception as e:
            print(f"  [FAIL get] {name}: {e}  ({i}/{total})")
            fail += 1
            continue

        state = detect_page_state(blocks)

        if state == "correct":
            print(f"  [SKIP] {name}  ({i}/{total})")
            skip += 1
            continue

        # Delete existing blocks if any
        if state == "old":
            try:
                for b in blocks:
                    delete_block(b["id"])
                    time.sleep(0.1)
            except Exception as e:
                print(f"  [FAIL delete] {name}: {e}  ({i}/{total})")
                fail += 1
                continue

        # Append new structure
        try:
            append_blocks(pid, new_section_blocks())
            print(f"  [OK] {name}  ({i}/{total})")
            ok += 1
        except Exception as e:
            print(f"  [FAIL append] {name}: {e}  ({i}/{total})")
            fail += 1

        time.sleep(0.2)

    print(f"\n{'='*60}")
    print(f"Done — {ok} updated, {skip} skipped (already correct), {fail} failed")


if __name__ == "__main__":
    main()
