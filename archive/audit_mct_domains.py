#!/usr/bin/env python3
"""
audit_mct_domains.py
---------------------
READ-ONLY audit of every MCT row's 🏢 Domain field.

Classifies each row as:
  CLEAR    — has a real business domain, no action needed
  GENERIC  — has a generic mail provider (gmail.com, hotmail.com, etc.)
  EMPTY    — no domain at all

For GENERIC and EMPTY rows, runs a 4-tier enrichment chain:
  1. Stripe      → HIGH confidence
  2. Intercom    → HIGH confidence
  3. HubSpot     → MEDIUM confidence
  4. DuckDuckGo  → MEDIUM confidence

Outputs:
  - Console report (classification + enrichment results)
  - mct_domain_audit.json  (full row list for fix_mct_domains.py)

Usage:
  python3 audit_mct_domains.py
"""

import json
import time
import requests
from typing import Optional

# ── Import enrichment helpers from existing script ───────────────────────────
# fix_generic_domains.py is in the same directory; its main() is guarded by
# if __name__ == "__main__" so importing it is safe and side-effect-free.
from fix_generic_domains import (
import creds
    bare_domain,
    is_generic,
    lookup_intercom,
    lookup_hubspot,
    lookup_ddg,
    patch_notion_domain,  # imported for re-export (used by fix_mct_domains.py)
    GENERIC_DOMAINS,
)

# ── Constants ─────────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
STRIPE_KEY = creds.get("STRIPE_KEY")
NOTION_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"
OUTPUT_FILE  = "mct_domain_audit.json"

notion_headers = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type":   "application/json",
}
stripe_headers = {
    "Authorization": f"Bearer {STRIPE_KEY}",
}


# ── Step 1: Fetch all MCT rows ────────────────────────────────────────────────

def fetch_all_mct_rows():
    """
    Paginate through all MCT pages via data_sources/query (required for
    multi-source databases — the standard databases/{id}/query endpoint fails).
    """
    pages        = []
    has_more     = True
    start_cursor = None
    page_num     = 0

    while has_more:
        page_num += 1
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor

        r = requests.post(
            f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
            headers=notion_headers,
            json=body,
        )
        r.raise_for_status()
        data = r.json()

        batch        = data.get("results", [])
        has_more     = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

        pages.extend(batch)
        print(f"  Page {page_num}: {len(batch)} rows  (total so far: {len(pages)})")
        time.sleep(0.3)

    return pages


# ── Step 2: Extract fields from a page object ─────────────────────────────────

def extract_row_fields(page_obj) -> dict:
    """
    Pull company_name, current_domain, stripe_id from a Notion page dict.

    Company name: scan all properties for type == "title" (key name can vary
    per database configuration, so we scan dynamically).
    """
    props = page_obj.get("properties", {})

    # Company name — find the title-type property dynamically
    company_name = ""
    for prop in props.values():
        if prop.get("type") == "title":
            title_items  = prop.get("title", [])
            company_name = "".join(t.get("plain_text", "") for t in title_items).strip()
            break

    # Current domain
    domain_texts   = props.get("🏢 Domain", {}).get("rich_text", [])
    current_domain = "".join(t.get("plain_text", "") for t in domain_texts).lower().strip()

    # Stripe Customer ID
    stripe_texts = props.get("🔗 Stripe Customer ID", {}).get("rich_text", [])
    stripe_id    = "".join(t.get("plain_text", "") for t in stripe_texts).strip()

    return {
        "page_id":         page_obj["id"],
        "company_name":    company_name,
        "current_domain":  current_domain,
        "stripe_id":       stripe_id,
        "status":          "",
        "proposed_domain": "",
        "source":          "",
        "confidence":      "",
    }


# ── Step 3: Classify ──────────────────────────────────────────────────────────

def classify_row(row) -> str:
    """Return 'CLEAR', 'GENERIC', or 'EMPTY'."""
    d = row["current_domain"]
    if not d:
        return "EMPTY"
    if is_generic(d):   # checks against GENERIC_DOMAINS set
        return "GENERIC"
    return "CLEAR"


# ── Step 4: Stripe lookup ─────────────────────────────────────────────────────

def get_domain_from_stripe(stripe_id: str) -> Optional[str]:
    """
    Call Stripe to get the customer email, extract the domain, validate it.
    Returns a bare business domain string, or None if:
      - no stripe_id provided
      - API returns non-200
      - email is missing or has no @ sign
      - extracted domain is generic (gmail.com etc.)
    """
    if not stripe_id:
        return None
    try:
        r = requests.get(
            f"https://api.stripe.com/v1/customers/{stripe_id}",
            headers=stripe_headers,
            timeout=10,
        )
        if r.status_code != 200:
            return None
        email = r.json().get("email") or ""
        if "@" not in email:
            return None
        domain = bare_domain(email.split("@")[1].lower())
        if not domain or is_generic(domain):
            return None
        return domain
    except Exception as e:
        print(f"    [Stripe error] {stripe_id}: {e}")
        return None


# ── Step 5: Enrich (4-tier priority chain) ────────────────────────────────────

def enrich_row(row) -> None:
    """
    Try each enrichment tier in priority order; stop at first success.
    Updates row dict in-place: proposed_domain, source, confidence.
    """
    name      = row["company_name"]
    stripe_id = row["stripe_id"]

    # Tier 1 — Stripe (HIGH confidence)
    domain = get_domain_from_stripe(stripe_id)
    time.sleep(0.3)
    if domain and not is_generic(domain):
        row["proposed_domain"] = domain
        row["source"]          = "Stripe"
        row["confidence"]      = "HIGH"
        return

    # Tier 2 — Intercom Companies API (HIGH confidence)
    domain = lookup_intercom(name)
    time.sleep(0.3)
    if domain and not is_generic(domain):
        row["proposed_domain"] = domain
        row["source"]          = "Intercom"
        row["confidence"]      = "HIGH"
        return

    # Tier 3 — HubSpot CRM (MEDIUM confidence)
    domain = lookup_hubspot(name)
    time.sleep(0.3)
    if domain and not is_generic(domain):
        row["proposed_domain"] = domain
        row["source"]          = "HubSpot"
        row["confidence"]      = "MEDIUM"
        return

    # Tier 4 — DuckDuckGo Instant Answer (MEDIUM confidence)
    domain = lookup_ddg(name)
    time.sleep(0.5)
    if domain and not is_generic(domain):
        row["proposed_domain"] = domain
        row["source"]          = "DuckDuckGo"
        row["confidence"]      = "MEDIUM"
        return

    # All tiers exhausted
    row["proposed_domain"] = ""
    row["source"]          = "UNRESOLVED"
    row["confidence"]      = "UNRESOLVED"


# ── Step 6: Print report ──────────────────────────────────────────────────────

def print_report(rows) -> None:
    total   = len(rows)
    clear   = [r for r in rows if r["status"] == "CLEAR"]
    generic = [r for r in rows if r["status"] == "GENERIC"]
    empty   = [r for r in rows if r["status"] == "EMPTY"]

    needs_fix  = generic + empty
    high_conf  = [r for r in needs_fix if r["confidence"] == "HIGH"]
    med_conf   = [r for r in needs_fix if r["confidence"] == "MEDIUM"]
    unresolved = [r for r in needs_fix if r["confidence"] == "UNRESOLVED"]

    print("\n")
    print("=" * 80)
    print("  MCT DOMAIN AUDIT REPORT")
    print("=" * 80)
    print(f"  Total MCT rows   : {total}")
    print(f"  CLEAR (no action): {len(clear)}")
    print(f"  GENERIC          : {len(generic)}")
    print(f"  EMPTY            : {len(empty)}")
    print(f"  ── After enrichment ──────────────────────────────────────────────")
    print(f"  HIGH confidence  : {len(high_conf)}   (auto-apply in fix script)")
    print(f"  MEDIUM confidence: {len(med_conf)}   (manual confirmation required)")
    print(f"  UNRESOLVED       : {len(unresolved)}   (manual lookup needed)")
    print()

    if not needs_fix:
        print("  All domains look good — nothing to fix!")
        print("=" * 80)
        return

    # Column widths
    W = [34, 17, 22, 22, 11, 10]

    def header_line():
        return (
            f"  {'Company':<{W[0]}} {'Stripe ID':<{W[1]}} "
            f"{'Current Domain':<{W[2]}} {'Proposed Domain':<{W[3]}} "
            f"{'Source':<{W[4]}} Conf"
        )

    def row_line(r):
        name     = r["company_name"][:W[0] - 1]
        sid      = (r["stripe_id"] or "(none)")[:W[1] - 1]
        cur      = (r["current_domain"] or "(empty)")[:W[2] - 1]
        proposed = (r["proposed_domain"] or "(none)")[:W[3] - 1]
        src      = r["source"][:W[4] - 1]
        conf     = r["confidence"]
        tag      = "  *** UNRESOLVED ***" if conf == "UNRESOLVED" else ""
        return (
            f"  {name:<{W[0]}} {sid:<{W[1]}} "
            f"{cur:<{W[2]}} {proposed:<{W[3]}} "
            f"{src:<{W[4]}} {conf}{tag}"
        )

    # Sort within each status group: HIGH first, MEDIUM second, UNRESOLVED last
    def sort_key(r):
        c = r["confidence"]
        return (c == "UNRESOLVED", c == "MEDIUM")

    for label, group in (("GENERIC", generic), ("EMPTY", empty)):
        if not group:
            continue
        print(f"  [{label} rows]")
        print(header_line())
        print("  " + "─" * (sum(W) + 15))
        for r in sorted(group, key=sort_key):
            print(row_line(r))
        print()

    print("=" * 80)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 80)
    print("  audit_mct_domains.py — READ-ONLY domain audit")
    print("=" * 80 + "\n")

    # Step 1: Fetch all rows
    print("Step 1: Fetching all MCT rows ...")
    pages = fetch_all_mct_rows()
    print(f"  Total rows fetched: {len(pages)}\n")

    # Step 2+3: Extract fields and classify
    print("Step 2: Extracting fields and classifying ...")
    rows = []
    for page_obj in pages:
        row = extract_row_fields(page_obj)
        row["status"] = classify_row(row)
        rows.append(row)

    clear_count = sum(1 for r in rows if r["status"] == "CLEAR")
    needs_fix   = [r for r in rows if r["status"] in ("GENERIC", "EMPTY")]
    print(f"  CLEAR: {clear_count}  |  Need enrichment: {len(needs_fix)}\n")

    # Step 4+5: Enrich GENERIC + EMPTY rows
    if needs_fix:
        print(f"Step 3: Enriching {len(needs_fix)} rows via 4-tier chain ...")
        print("  (Stripe → Intercom → HubSpot → DuckDuckGo)\n")
        for i, row in enumerate(needs_fix, 1):
            label = row["company_name"] or row["page_id"][:8]
            print(f"  [{i:>3}/{len(needs_fix)}] {label}  ({row['status']})", end=" ... ", flush=True)
            enrich_row(row)
            result = row["proposed_domain"] or "none"
            print(f"{row['source']} → {result}")
        print()
    else:
        print("Step 3: No rows need enrichment.\n")

    # Step 6: Print report
    print_report(rows)

    # Step 7: Save JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

    print(f"\n  Saved {OUTPUT_FILE}  ({len(rows)} rows)\n")
    print("  Next steps:")
    print("    1. Review mct_domain_audit.json (especially MEDIUM + UNRESOLVED rows)")
    print("    2. python3 fix_mct_domains.py --dry-run   # preview")
    print("    3. python3 fix_mct_domains.py             # apply with prompts\n")


if __name__ == "__main__":
    main()
