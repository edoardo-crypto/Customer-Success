#!/usr/bin/env python3
"""
sync_mrr_from_sheet.py — Update MCT MRR from Google Sheet (source of truth).

MRR = Stripe base subscription + AI Sessions usage (from the Google Sheet).
Uses the latest available month column in each tab.

Replaces sync_mrr_from_stripe.py (which computed MRR from Stripe API — unreliable
for tiered/metered pricing).

Usage:
    python3 sync_mrr_from_sheet.py              # live
    DRY_RUN=true python3 sync_mrr_from_sheet.py  # preview only
"""

import csv
import io
import os
import re
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import creds

# ── Credentials ──────────────────────────────────────────────────────────────

NOTION_TOKEN = creds.get("NOTION_TOKEN")

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() not in ("false", "0", "no")

# ── Constants ────────────────────────────────────────────────────────────────

MCT_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

SHEET_ID       = "1C9Y5e6Rz9L24EtczXGMvapkL1ENu23SD"
SHEET_GID_STRIPE = "1127966823"   # "Client List - Stripe"
SHEET_GID_AI     = "279628241"    # "Client List - AI Sessions"

NOTION_HDR = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}


# ── Google Sheet helpers ─────────────────────────────────────────────────────


def _parse_euro(val):
    if not val or val.strip() == "":
        return 0.0
    val = val.strip().replace("\u20ac", "").replace(",", "").replace('"', '')
    try:
        return float(val)
    except ValueError:
        return 0.0


def _fetch_sheet_tab(gid):
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return list(csv.reader(io.StringIO(r.text)))


def _parse_tab_mrr(rows, header_marker):
    """Parse a tab → {stripe_customer_id: mrr} for the latest month in the first group."""
    header_idx = None
    for i, row in enumerate(rows):
        if row and header_marker in (row[0] or ""):
            header_idx = i
            break
    if header_idx is None:
        return {}, None

    headers = rows[header_idx]
    month_re = re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}$")

    first_month_col = None
    latest_col = None
    latest_label = None
    for col_idx, h in enumerate(headers):
        h = h.strip()
        if month_re.match(h):
            if first_month_col is None:
                first_month_col = col_idx
            latest_col = col_idx
            latest_label = h
        elif first_month_col is not None:
            break

    if latest_col is None:
        return {}, None

    mrr_dict = {}
    for row in rows[header_idx + 1:]:
        if not row or not row[0] or not row[0].startswith("cus_"):
            continue
        cust_id = row[0].strip()
        val = row[latest_col] if latest_col < len(row) else ""
        mrr = _parse_euro(val)
        if mrr > 0:
            mrr_dict[cust_id] = mrr

    return mrr_dict, latest_label


def fetch_sheet_mrr():
    """Fetch combined MRR from both tabs. Returns {stripe_id: total_mrr}."""
    print("Fetching Stripe base MRR from Google Sheet\u2026")
    stripe_rows = _fetch_sheet_tab(SHEET_GID_STRIPE)
    stripe_mrr, stripe_month = _parse_tab_mrr(stripe_rows, "MRR by Client")
    print(f"  {len(stripe_mrr)} customers, \u20ac{sum(stripe_mrr.values()):,.0f} (month: {stripe_month})")

    print("Fetching AI Sessions from Google Sheet\u2026")
    ai_rows = _fetch_sheet_tab(SHEET_GID_AI)
    ai_mrr, ai_month = _parse_tab_mrr(ai_rows, "AI Sessions by Client")
    print(f"  {len(ai_mrr)} customers, \u20ac{sum(ai_mrr.values()):,.0f} (month: {ai_month})")

    all_ids = set(stripe_mrr.keys()) | set(ai_mrr.keys())
    merged = {}
    for cid in all_ids:
        merged[cid] = round(stripe_mrr.get(cid, 0) + ai_mrr.get(cid, 0), 2)

    print(f"  Combined: {len(merged)} customers, \u20ac{sum(merged.values()):,.0f}\n")
    return merged


# ── Notion helpers ───────────────────────────────────────────────────────────


def get_title(page, prop):
    p = page.get("properties", {}).get(prop)
    return "".join(t.get("plain_text", "") for t in (p or {}).get("title", []))


def get_rich_text(page, prop):
    p = page.get("properties", {}).get(prop)
    return "".join(t.get("plain_text", "") for t in (p or {}).get("rich_text", []))


def get_number(page, prop):
    p = page.get("properties", {}).get(prop)
    return p.get("number") if p else None


def fetch_all_mct():
    pages = []
    body = {"page_size": 100}
    url = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"
    while True:
        r = requests.post(url, headers=NOTION_HDR, json=body, timeout=30)
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        body["start_cursor"] = data["next_cursor"]
    return pages


def patch_mct_page(page_id, properties):
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HDR,
        json={"properties": properties},
        timeout=20,
    )
    if r.ok:
        return True
    print(f"  [ERROR] PATCH {page_id[:8]}\u2026 failed {r.status_code}: {r.text[:300]}")
    return False


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    import datetime
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    print(f"{'='*65}")
    print(f"  sync_mrr_from_sheet.py  |  {mode}  |  {datetime.date.today()}")
    print(f"{'='*65}\n")

    # ── 1. Fetch MRR from Google Sheet ───────────────────────────────────────
    sheet_mrr = fetch_sheet_mrr()

    # ── 2. Fetch MCT ─────────────────────────────────────────────────────────
    print("Fetching MCT rows\u2026")
    pages = fetch_all_mct()
    print(f"  {len(pages)} rows\n")

    # ── 3. Compare and patch ─────────────────────────────────────────────────
    print(f"Reconciling MRR ({mode})\u2026")
    patched = 0
    skipped_no_stripe = 0
    skipped_no_data = 0
    unchanged = 0
    errors = 0

    for page in pages:
        page_id = page["id"]
        company = get_title(page, "\U0001f3e2 Company Name").strip() or "(no name)"
        stripe_id = get_rich_text(page, "\U0001f517 Stripe Customer ID").strip()

        if not stripe_id or not stripe_id.startswith("cus_"):
            skipped_no_stripe += 1
            continue

        current_mrr = get_number(page, "\U0001f4b0 MRR") or 0
        new_mrr = sheet_mrr.get(stripe_id)

        if new_mrr is None:
            skipped_no_data += 1
            continue

        if abs(new_mrr - current_mrr) <= 1:
            unchanged += 1
            continue

        diff = new_mrr - current_mrr
        sign = "+" if diff > 0 else ""
        print(f"  {company}: \u20ac{current_mrr:,.2f} \u2192 \u20ac{new_mrr:,.2f} ({sign}{diff:,.2f})")

        if DRY_RUN:
            patched += 1
            continue

        if patch_mct_page(page_id, {"\U0001f4b0 MRR": {"number": new_mrr}}):
            patched += 1
        else:
            errors += 1
        time.sleep(0.3)

    # ── 4. Summary ───────────────────────────────────────────────────────────
    verb = "would patch" if DRY_RUN else "patched"
    total_sheet = sum(sheet_mrr.values())
    print(f"\n{'='*65}")
    print(f"  Google Sheet MRR total: \u20ac{total_sheet:,.2f}")
    print(f"  {patched} {verb} | {unchanged} unchanged | {skipped_no_data} not in sheet | {skipped_no_stripe} no Stripe ID | {errors} errors")
    print(f"{'='*65}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
