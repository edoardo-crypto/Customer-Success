#!/usr/bin/env python3
"""
setup_customer_tables.py — ONE-TIME setup of inline CS tables on MCT customer pages.

Adds 3 sections to every non-Canceled customer page:

  ## Blockers & Next Actions   [table: Blocker | Next Action | Status]
  ## Success Criteria          [table: Criterion | Confirmed]
  ## To Dos                    [table: Task | Done | Notes]

Each section: heading_2 → table (header row + 1 empty data row) → divider.

Usage:  python3 setup_customer_tables.py
Archive to archive/ after run.
"""

import os
import time

import requests
import creds

# ── Credentials ────────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
MCT_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"

NOTION_API = "https://api.notion.com/v1"

# MCT queries require 2025-09-03; block append uses 2022-06-28
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


# ── Helper: empty table cell ────────────────────────────────────────────────────
def _cell(text=""):
    """One table cell with optional text."""
    if text:
        return [{"type": "text", "text": {"content": text}}]
    return []


def make_table_blocks(headers, n_empty_rows=1):
    """
    Build a Notion table block (with children rows) ready for the children-append API.

    headers  — list of column header strings
    Returns a single table block dict with `children` = [header_row, …empty_rows].
    """
    n_cols = len(headers)

    header_row = {
        "object": "block",
        "type": "table_row",
        "table_row": {
            "cells": [_cell(h) for h in headers]
        },
    }
    empty_row = {
        "object": "block",
        "type": "table_row",
        "table_row": {
            "cells": [_cell() for _ in headers]
        },
    }

    return {
        "object": "block",
        "type": "table",
        "table": {
            "table_width": n_cols,
            "has_column_header": True,
            "has_row_header": False,
            "children": [header_row] + [empty_row] * n_empty_rows,
        },
    }


def heading_2(text):
    return {
        "object": "block",
        "type": "heading_2",
        "heading_2": {
            "rich_text": [{"type": "text", "text": {"content": text}}]
        },
    }


DIVIDER = {"object": "block", "type": "divider", "divider": {}}


PARAGRAPH = {"object": "block", "type": "paragraph", "paragraph": {"rich_text": []}}


def section_blocks():
    """The full list of blocks to append to each customer page."""
    return [
        heading_2("🚧 Blockers & Next Actions"),
        make_table_blocks(["Blocker", "Next Action", "Status"]),
        DIVIDER,
        heading_2("🎯 Success Criteria"),
        make_table_blocks(["Criterion", "Confirmed (y/n)", "Reason"]),
        DIVIDER,
        heading_2("📝 Notes"),
        PARAGRAPH,
    ]


# ── MCT helpers ─────────────────────────────────────────────────────────────────
def fetch_active_pages():
    """Return list of {page_id, company_name, billing_status} for non-Canceled customers."""
    pages = []
    cursor = None

    while True:
        body = {
            "page_size": 100,
            "filter": {
                "property": "💰 Billing Status",
                "select": {"does_not_equal": "Canceled"},
            },
        }
        if cursor:
            body["start_cursor"] = cursor

        resp = requests.post(
            f"{NOTION_API}/data_sources/{MCT_DS_ID}/query",
            headers=HEADERS_MCT,
            json=body,
        )
        resp.raise_for_status()
        data = resp.json()

        for page in data.get("results", []):
            page_id = page["id"]
            title_parts = page["properties"].get("🏢 Company Name", {}).get("title", [])
            company_name = "".join(t.get("plain_text", "") for t in title_parts).strip()
            if not company_name:
                company_name = f"(unnamed {page_id[:8]})"

            billing_status = (
                (page["properties"].get("💰 Billing Status", {}).get("select") or {})
                .get("name", "Unknown")
            )
            pages.append({
                "page_id":        page_id,
                "company_name":   company_name,
                "billing_status": billing_status,
            })

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return pages


def get_block_children(page_id):
    """Return all top-level block IDs on a page."""
    block_ids = []
    cursor = None
    while True:
        params = {"page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        resp = requests.get(
            f"{NOTION_API}/blocks/{page_id}/children",
            headers=HEADERS_BLOCKS,
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()
        for block in data.get("results", []):
            block_ids.append(block["id"])
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return block_ids


def append_tables(page_id):
    """Append all 3 table sections to the given page."""
    resp = requests.patch(
        f"{NOTION_API}/blocks/{page_id}/children",
        headers=HEADERS_BLOCKS,
        json={"children": section_blocks()},
    )
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("Fetching non-Canceled customer pages from MCT…")
    pages = fetch_active_pages()
    print(f"  → {len(pages)} pages found\n")

    ok = skip = fail = 0

    for p in pages:
        pid    = p["page_id"]
        name   = p["company_name"]
        status = p["billing_status"]

        existing = get_block_children(pid)
        if existing:
            print(f"  [SKIP]  [{status}] {name} — already has {len(existing)} block(s)")
            skip += 1
            time.sleep(0.1)
            continue

        try:
            append_tables(pid)
            print(f"  [OK]    [{status}] {name}")
            ok += 1
        except Exception as e:
            print(f"  [FAIL]  [{status}] {name} — {e}")
            fail += 1

        time.sleep(0.2)

    print(f"\n{'='*60}")
    print(f"Done — {ok} setup, {skip} skipped (already had blocks), {fail} failed")


if __name__ == "__main__":
    main()
