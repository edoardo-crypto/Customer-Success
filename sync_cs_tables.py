#!/usr/bin/env python3
"""
sync_cs_tables.py — Daily sync of CS inline tables → shared analytics DBs.

WHEN TO RUN: Every day at 09:00 CET (automatic via GitHub Actions cron).
             Can also be run manually at any time.

What it does:
  Phase A — New-customer auto-setup:
    Checks every non-Canceled MCT page.  If it has 0 blocks, adds the 3 inline
    sections (same as setup_customer_tables.py did for existing customers).

  Phase B — Sync inline tables → shared analytics DBs:
    For each customer page:
      1. Reads the 2 inline tables (Blockers, Criteria) — Notes is a text box, not synced
      2. Deletes all existing rows in the 2 shared DBs for this customer
      3. Re-inserts fresh rows from the current table content
      (Only customers with at least one non-empty row are synced.)

Shared analytics DBs:
  CS Blockers & Next Actions:  06fe1bb4-cfc2-4bdf-967c-cd633c8aa8c4
  CS Success Criteria:          b938e808-b10b-4bbc-9dde-ec7a1c74b1ff

Run:  python3 sync_cs_tables.py
"""

import os
import time

import requests
import creds

# ── Credentials ────────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")

NOTION_API = "https://api.notion.com/v1"
MCT_DS_ID  = "3ceb1ad0-91f1-40db-945a-c51c58035898"

# Shared analytics DB IDs
DB_BLOCKERS = "06fe1bb4-cfc2-4bdf-967c-cd633c8aa8c4"
DB_CRITERIA = "b938e808-b10b-4bbc-9dde-ec7a1c74b1ff"

# Section heading text (lowercased) → which DB to sync into
# Notes section is a plain text box — no shared DB, not synced
SECTION_TO_DB = {
    "🚧 blockers & next actions": DB_BLOCKERS,
    "🎯 success criteria":        DB_CRITERIA,
}

# ── Headers ─────────────────────────────────────────────────────────────────────
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


# ══════════════════════════════════════════════════════════════════════════════
# Inline table builder (shared with Phase A auto-setup)
# ══════════════════════════════════════════════════════════════════════════════

def _cell(text=""):
    if text:
        return [{"type": "text", "text": {"content": text}}]
    return []


def _make_table_block(headers):
    n_cols = len(headers)
    header_row = {
        "object": "block", "type": "table_row",
        "table_row": {"cells": [_cell(h) for h in headers]},
    }
    empty_row = {
        "object": "block", "type": "table_row",
        "table_row": {"cells": [_cell() for _ in headers]},
    }
    return {
        "object": "block", "type": "table",
        "table": {
            "table_width": n_cols,
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


_DIVIDER = {"object": "block", "type": "divider", "divider": {}}


_PARAGRAPH = {"object": "block", "type": "paragraph", "paragraph": {"rich_text": []}}


def _section_blocks():
    return [
        _heading_2("🚧 Blockers & Next Actions"),
        _make_table_block(["Blocker", "Next Action", "Status"]),
        _DIVIDER,
        _heading_2("🎯 Success Criteria"),
        _make_table_block(["Criterion", "Confirmed (y/n)", "Reason"]),
        _DIVIDER,
        _heading_2("📝 Notes"),
        _PARAGRAPH,
    ]


def add_tables(page_id):
    """Append the 3 table sections to an empty customer page."""
    resp = requests.patch(
        f"{NOTION_API}/blocks/{page_id}/children",
        headers=HEADERS_BLOCKS,
        json={"children": _section_blocks()},
    )
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")


# ══════════════════════════════════════════════════════════════════════════════
# MCT helpers
# ══════════════════════════════════════════════════════════════════════════════

def fetch_active_pages():
    """Return [{page_id, company_name, billing_status}] for non-Canceled customers."""
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
            headers=HEADERS_MCT, json=body,
        )
        resp.raise_for_status()
        data = resp.json()
        for page in data.get("results", []):
            pid = page["id"]
            title_parts = page["properties"].get("🏢 Company Name", {}).get("title", [])
            name = "".join(t.get("plain_text", "") for t in title_parts).strip() or f"(unnamed {pid[:8]})"
            status = (
                (page["properties"].get("💰 Billing Status", {}).get("select") or {})
                .get("name", "Unknown")
            )
            pages.append({"page_id": pid, "company_name": name, "billing_status": status})
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


def get_block_children(block_id):
    """Return all child blocks (as full dicts) for a block/page ID."""
    blocks = []
    cursor = None
    while True:
        params = {"page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        for attempt in range(3):
            resp = requests.get(
                f"{NOTION_API}/blocks/{block_id}/children",
                headers=HEADERS_BLOCKS, params=params,
            )
            if resp.status_code < 500:
                break
            time.sleep(2 ** attempt)
        resp.raise_for_status()
        data = resp.json()
        blocks.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# Table reader — extracts {section_key: [row_dicts]} from a customer page
# ══════════════════════════════════════════════════════════════════════════════

def _cell_text(cell_list):
    """Extract plain text from a Notion table cell (list of rich_text objects)."""
    return "".join(rt.get("plain_text", "") for rt in cell_list).strip()


def read_page_tables(page_id):
    """
    Walk a customer page's top-level blocks, find heading_2 → table sequences,
    and return a dict keyed by normalised section name:

        {
          "blockers & next actions": [{"Blocker": "...", "Next Action": "...", "Status": "..."}, …],
          "success criteria":        [{"Criterion": "...", "Confirmed": "..."}, …],
          "to dos":                  [{"Task": "...", "Done": "...", "Notes": "..."}, …],
        }

    Rows where all cells are empty are dropped.
    Row 0 (header) is used to build the column-name mapping and is not included in output.
    """
    top_blocks = get_block_children(page_id)
    sections = {}
    current_section = None

    for block in top_blocks:
        btype = block.get("type")

        if btype == "heading_2":
            rich_text = block.get("heading_2", {}).get("rich_text", [])
            heading_text = "".join(rt.get("plain_text", "") for rt in rich_text).strip().lower()
            if heading_text in SECTION_TO_DB:
                current_section = heading_text
            else:
                current_section = None  # unknown heading, stop tracking

        elif btype == "table" and current_section:
            table_id = block["id"]
            rows = get_block_children(table_id)
            time.sleep(0.15)

            if not rows:
                sections[current_section] = []
                continue

            # Row 0 is the header
            header_cells = rows[0].get("table_row", {}).get("cells", [])
            col_names = [_cell_text(c) for c in header_cells]

            data_rows = []
            for row in rows[1:]:  # skip header
                cells = row.get("table_row", {}).get("cells", [])
                values = [_cell_text(c) for c in cells]
                # pad/trim to match header width
                while len(values) < len(col_names):
                    values.append("")
                row_dict = dict(zip(col_names, values[:len(col_names)]))
                if any(v for v in row_dict.values()):  # skip fully-empty rows
                    data_rows.append(row_dict)

            sections[current_section] = data_rows
            current_section = None  # reset; each section has at most one table

        elif btype == "divider":
            pass  # dividers don't reset state

    return sections


# ══════════════════════════════════════════════════════════════════════════════
# Shared DB helpers — delete existing rows, insert new ones
# ══════════════════════════════════════════════════════════════════════════════

def query_db_for_customer(db_id, page_id):
    """Return list of entry IDs in the shared DB that belong to this customer page."""
    entry_ids = []
    cursor = None
    while True:
        body = {
            "page_size": 100,
            "filter": {
                "property": "Customer",
                "relation": {"contains": page_id},
            },
        }
        if cursor:
            body["start_cursor"] = cursor
        resp = requests.post(
            f"{NOTION_API}/databases/{db_id}/query",
            headers=HEADERS_BLOCKS,  # shared DBs use standard 2022-06-28
            json=body,
        )
        resp.raise_for_status()
        data = resp.json()
        entry_ids.extend(e["id"] for e in data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return entry_ids


def delete_entry(entry_id):
    resp = requests.delete(
        f"{NOTION_API}/blocks/{entry_id}",
        headers=HEADERS_BLOCKS,
    )
    resp.raise_for_status()


def _rich_text(value):
    return [{"type": "text", "text": {"content": value}}]


_VALID_BLOCKER_STATUSES = {"Blocked", "In Progress", "Resolved"}


def build_properties(section_key, row_dict, page_id):
    """
    Convert a row dict from an inline table into the Notion properties payload
    for a new entry in the corresponding shared DB.
    """
    customer_rel = {"relation": [{"id": page_id}]}

    if section_key == "🚧 blockers & next actions":
        status_raw = row_dict.get("Status", "").strip()
        # Case-insensitive match to known values; default to "Blocked"
        status_val = next(
            (v for v in _VALID_BLOCKER_STATUSES if v.lower() == status_raw.lower()),
            "Blocked",
        )
        return {
            "Blocker":       {"title": _rich_text(row_dict.get("Blocker", "") or "")},
            "Next Action":   {"rich_text": _rich_text(row_dict.get("Next Action", "") or "")},
            "Status":        {"select": {"name": status_val}},
            "Customer":      customer_rel,
        }

    elif section_key == "🎯 success criteria":
        confirmed_raw = row_dict.get("Confirmed (y/n)", "").strip()
        return {
            "Criterion":              {"title": _rich_text(row_dict.get("Criterion", "") or "")},
            "Confirmed at graduation": {"checkbox": bool(confirmed_raw)},
            "Reason":                 {"rich_text": _rich_text(row_dict.get("Reason", "") or "")},
            "Customer":               customer_rel,
        }

    raise ValueError(f"Unknown section: {section_key!r}")


def insert_entry(db_id, properties):
    resp = requests.post(
        f"{NOTION_API}/pages",
        headers=HEADERS_BLOCKS,
        json={
            "parent": {"database_id": db_id},
            "properties": properties,
        },
    )
    if not resp.ok:
        raise RuntimeError(f"Insert failed: HTTP {resp.status_code}: {resp.text[:300]}")


# ══════════════════════════════════════════════════════════════════════════════
# Phase A — auto-setup new pages
# ══════════════════════════════════════════════════════════════════════════════

def phase_a_auto_setup(pages):
    new_setups = 0
    for p in pages:
        pid  = p["page_id"]
        name = p["company_name"]
        try:
            blocks = get_block_children(pid)
        except Exception as e:
            print(f"  [AUTO-SETUP SKIP] {name}: could not read blocks — {e}")
            time.sleep(0.5)
            continue
        if not blocks:
            try:
                add_tables(pid)
                print(f"  [AUTO-SETUP] {name}")
                new_setups += 1
            except Exception as e:
                print(f"  [AUTO-SETUP FAIL] {name}: {e}")
        time.sleep(0.15)
    print(f"Phase A — {new_setups} new page(s) set up\n")


# ══════════════════════════════════════════════════════════════════════════════
# Phase B — sync tables → shared DBs
# ══════════════════════════════════════════════════════════════════════════════

def phase_b_sync(pages):
    synced = errors = 0

    for p in pages:
        pid  = p["page_id"]
        name = p["company_name"]

        # Read all 3 inline tables from the customer page
        try:
            sections = read_page_tables(pid)
        except Exception as e:
            print(f"  [READ ERR] {name}: {e}")
            errors += 1
            time.sleep(0.5)
            continue

        # Check if there's anything to sync
        has_data = any(rows for rows in sections.values())
        if not has_data:
            time.sleep(0.2)
            continue

        page_synced = False

        for section_key, rows in sections.items():
            db_id = SECTION_TO_DB[section_key]

            # 1. Delete existing entries for this customer
            try:
                existing_ids = query_db_for_customer(db_id, pid)
                for eid in existing_ids:
                    delete_entry(eid)
                    time.sleep(0.1)
            except Exception as e:
                print(f"  [DELETE ERR] {name} / {section_key}: {e}")
                errors += 1
                continue

            # 2. Insert new rows
            inserted = 0
            for row_dict in rows:
                try:
                    props = build_properties(section_key, row_dict, pid)
                    insert_entry(db_id, props)
                    inserted += 1
                    time.sleep(0.15)
                except Exception as e:
                    print(f"  [INSERT ERR] {name} / {section_key}: {e}")
                    errors += 1

            if inserted:
                page_synced = True

        if page_synced:
            total_rows = sum(len(r) for r in sections.values())
            print(f"  [SYNCED] {name} — {total_rows} row(s) across 3 sections")
            synced += 1

        time.sleep(0.5)

    print(f"\nPhase B — {synced} customer(s) synced, {errors} error(s)")


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("=== CS Tables Daily Sync ===\n")

    print("Fetching non-Canceled MCT pages…")
    pages = fetch_active_pages()
    print(f"  → {len(pages)} pages\n")

    print("── Phase A: Auto-setup new pages ──────────────────────────")
    phase_a_auto_setup(pages)

    print("── Phase B: Sync inline tables → shared DBs ───────────────")
    phase_b_sync(pages)

    print("\nDone.")


if __name__ == "__main__":
    main()
