#!/usr/bin/env python3
"""
diagnose_p1_customer_linking.py

Read-only diagnostic: how many P1 issues (Feb 16–Mar 1 2026) are linked to a
customer in the MCT, which ones are missing, and whether any linked IDs point
to rows that no longer exist in the MCT.

No Notion writes.
"""

import time
from collections import defaultdict
import requests

# ── Constants ─────────────────────────────────────────────────────────────────

NOTION_TOKEN     = "***REMOVED***"
NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"
NOTION_DS_ID     = "3ceb1ad0-91f1-40db-945a-c51c58035898"  # MCT data source

DATE_START       = "2026-02-16"
DATE_END         = "2026-03-01"

# Tunable — will auto-detect if the property name differs
# Set P1_VALUE = "" to skip severity filtering and analyse ALL issues in range
P1_PROPERTY      = "Severity"
P1_VALUE         = ""   # "" = no filter (all 54 issues); "Urgent" = highest severity only

# ── Headers ───────────────────────────────────────────────────────────────────

HEADERS_ISSUES = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type":   "application/json",
}

HEADERS_MCT = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type":   "application/json",
}


# ── Property helpers ──────────────────────────────────────────────────────────

def get_title(prop):
    texts = prop.get("title", [])
    return "".join(t.get("plain_text", "") for t in texts).strip()


def get_rich_text(prop):
    texts = prop.get("rich_text", [])
    return "".join(t.get("plain_text", "") for t in texts).strip()


def get_date(prop):
    d = prop.get("date") or {}
    return d.get("start", "")


def get_select(prop):
    s = prop.get("select") or {}
    return s.get("name", "")


def get_status(prop):
    s = prop.get("status") or {}
    return s.get("name", "")


def get_relation(prop):
    """Return list of related page IDs."""
    return [r["id"] for r in prop.get("relation", [])]


# ── Step 1: Fetch all issues in date range ────────────────────────────────────

def fetch_issues_in_range():
    """Fetch all issues from Issues DB created in DATE_START..DATE_END."""
    print(f"Step 1: Fetching issues created {DATE_START} → {DATE_END} ...")
    issues   = []
    has_more = True
    cursor   = None
    page_num = 0

    while has_more:
        page_num += 1
        body = {
            "page_size": 100,
            "filter": {
                "and": [
                    {
                        "property": "Created At",
                        "date": {"on_or_after": DATE_START},
                    },
                    {
                        "property": "Created At",
                        "date": {"on_or_before": DATE_END},
                    },
                ]
            },
            "sorts": [{"property": "Created At", "direction": "ascending"}],
        }
        if cursor:
            body["start_cursor"] = cursor

        r = requests.post(
            f"https://api.notion.com/v1/databases/{NOTION_ISSUES_DB}/query",
            headers=HEADERS_ISSUES,
            json=body,
        )
        if r.status_code != 200:
            print(f"  ERROR {r.status_code}: {r.text[:400]}")
            raise RuntimeError("Notion Issues query failed")

        data     = r.json()
        batch    = data.get("results", [])
        has_more = data.get("has_more", False)
        cursor   = data.get("next_cursor")

        issues.extend(batch)
        print(f"  Page {page_num}: {len(batch)} results  (running total: {len(issues)})")
        time.sleep(0.3)

    print(f"  → {len(issues)} issues fetched total\n")
    return issues


# ── Step 2: Parse issues and auto-detect P1 property ──────────────────────────

def parse_issues(raw_pages):
    """
    Parse each raw Notion page into a dict of extracted fields.
    Also auto-detects which property to use as the priority/severity filter.
    Returns (parsed_list, detected_priority_prop_name).
    """
    parsed = []
    detected_prop = None

    # Property names that might carry priority/severity
    candidate_props = [
        "Priority", "Severity", "cs_severity", "Issue Priority",
        "priority", "severity",
    ]

    for page in raw_pages:
        props      = page.get("properties", {})
        page_id    = page["id"]

        # ── Detect priority property on first page ─────────────────────────
        if detected_prop is None:
            for cand in candidate_props:
                if cand in props:
                    detected_prop = cand
                    break
            # Fallback: any select/status property with "priority"/"severity" in name
            if detected_prop is None:
                for name, val in props.items():
                    if val.get("type") in ("select", "status") and any(
                        kw in name.lower() for kw in ("priority", "severity")
                    ):
                        detected_prop = name
                        break

        # ── Dates ─────────────────────────────────────────────────────────
        created_at_prop = get_date(props.get("Created At", {}))
        # Fallback to page-level created_time if property is empty
        if not created_at_prop:
            created_at_prop = page.get("created_time", "")[:10]  # ISO date only

        # ── Core fields ───────────────────────────────────────────────────
        title_prop = props.get("Issue Title") or props.get("Name") or {}
        title = get_title(title_prop) if title_prop.get("type") == "title" else ""
        # also scan for any title-type property
        if not title:
            for pval in props.values():
                if pval.get("type") == "title":
                    title = get_title(pval)
                    break

        issue_type   = get_select(props.get("Issue Type", {}))
        source       = get_select(props.get("Source", {}))

        # Status might be type "status" or "select"
        status_prop  = props.get("Status", {})
        status = get_status(status_prop) if status_prop.get("type") == "status" else get_select(status_prop)

        customer_ids = get_relation(props.get("Customer", {}))

        # Priority value (whichever property was detected)
        priority_val = ""
        if detected_prop and detected_prop in props:
            ptype = props[detected_prop].get("type", "")
            if ptype == "select":
                priority_val = get_select(props[detected_prop])
            elif ptype == "status":
                priority_val = get_status(props[detected_prop])
            elif ptype == "rich_text":
                priority_val = get_rich_text(props[detected_prop])

        parsed.append({
            "page_id":      page_id,
            "created_at":   created_at_prop,
            "title":        title,
            "issue_type":   issue_type,
            "source":       source,
            "status":       status,
            "customer_ids": customer_ids,
            "priority_val": priority_val,
        })

    return parsed, detected_prop


# ── Step 3 & 4: Apply P1 filter, classify, print unlinked table ───────────────

def apply_p1_filter(parsed, detected_prop):
    """
    Filter to P1 issues.  If P1_VALUE is "" all issues pass through.
    If nothing matches P1_VALUE, print diagnostic and return None (caller exits).
    """
    prop_name = detected_prop or P1_PROPERTY

    if not P1_VALUE:
        print(f"Step 2: P1_VALUE is empty — analysing ALL {len(parsed)} issues in range\n")
        # Show what severity values exist for reference
        all_vals = sorted({p["priority_val"] for p in parsed if p["priority_val"]})
        print(f"  Severity values present: {all_vals}\n")
        return parsed

    print(f"Step 2: Applying filter — property '{prop_name}' == '{P1_VALUE}'")

    p1_issues = [p for p in parsed if p["priority_val"] == P1_VALUE]

    if not p1_issues:
        print(f"\n  WARNING: 0 issues matched '{P1_VALUE}' on property '{prop_name}'")
        all_vals = sorted({p["priority_val"] for p in parsed if p["priority_val"]})
        print(f"  Available values: {all_vals}")
        print("  Adjust P1_PROPERTY / P1_VALUE at the top of the script and re-run.")
        return None

    print(f"  → Found {len(p1_issues)} matching issues\n")
    return p1_issues


# ── Step 5: Build MCT lookup {page_id → company_name} ────────────────────────

def build_mct_id_lookup():
    """
    Load all MCT pages and return {mct_page_id: company_name}.
    Uses data_sources API with Notion-Version 2025-09-03 (required for MCT).
    """
    print("Step 5: Building MCT page-id → company-name lookup ...")
    lookup   = {}
    cursor   = None
    page_num = 0

    while True:
        page_num += 1
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        r = requests.post(
            f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
            headers=HEADERS_MCT,
            json=body,
        )
        if r.status_code != 200:
            print(f"  ERROR {r.status_code}: {r.text[:400]}")
            raise RuntimeError("MCT data_sources query failed")

        data     = r.json()
        results  = data.get("results", [])
        has_more = data.get("has_more", False)
        cursor   = data.get("next_cursor")

        for page_obj in results:
            pid   = page_obj["id"]
            props = page_obj.get("properties", {})

            # Find the title-type property for company name
            company = ""
            for pval in props.values():
                if pval.get("type") == "title":
                    items   = pval.get("title", [])
                    company = "".join(t.get("plain_text", "") for t in items).strip()
                    break

            lookup[pid] = company

        print(f"  Page {page_num}: {len(results)} rows  (running total: {len(lookup)})")
        time.sleep(0.3)

        if not has_more:
            break

    print(f"  → {len(lookup)} MCT rows loaded\n")
    return lookup


# ── Step 6: Validate linked issues ───────────────────────────────────────────

def validate_linked(issues, mct_lookup):
    """
    For each linked issue, check if all customer_ids exist in mct_lookup.
    Returns (valid_issues, broken_issues).
    """
    valid   = []
    broken  = []

    for issue in issues:
        if not issue["customer_ids"]:
            continue  # unlinked, handled separately
        missing = [cid for cid in issue["customer_ids"] if cid not in mct_lookup]
        if missing:
            broken.append({**issue, "missing_ids": missing})
        else:
            valid.append(issue)

    return valid, broken


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    sep  = "═" * 62
    sep2 = "─" * 62

    # ── Step 1: Fetch ──────────────────────────────────────────────────────
    raw_pages = fetch_issues_in_range()

    # ── Step 2: Parse + detect P1 property ────────────────────────────────
    parsed, detected_prop = parse_issues(raw_pages)
    total_in_range = len(parsed)

    # ── Step 2 (cont.): Apply filter ───────────────────────────────────────
    p1_issues = apply_p1_filter(parsed, detected_prop)
    if p1_issues is None:
        return  # diagnostic already printed

    # ── Step 3: Classify ───────────────────────────────────────────────────
    linked   = [i for i in p1_issues if i["customer_ids"]]
    unlinked = [i for i in p1_issues if not i["customer_ids"]]

    print(f"Step 3: Classification — {len(linked)} linked, {len(unlinked)} unlinked\n")

    # ── Step 4: Print unlinked detail table ────────────────────────────────
    if unlinked:
        print("Step 4: Unlinked issue detail")
        print(sep2)
        col_d = 12   # created at (date only)
        col_t = 50   # title
        col_i = 20   # issue type
        col_s = 18   # source
        col_x = 18   # status
        print(
            f"  {'Created At':<{col_d}}  "
            f"{'Title':<{col_t}}  "
            f"{'Issue Type':<{col_i}}  "
            f"{'Source':<{col_s}}  "
            f"{'Status':<{col_x}}"
        )
        print(f"  {'-'*col_d}  {'-'*col_t}  {'-'*col_i}  {'-'*col_s}  {'-'*col_x}")
        for issue in sorted(unlinked, key=lambda x: x["created_at"]):
            d = (issue["created_at"] or "")[:10]
            t = (issue["title"]      or "—")[:col_t]
            i = (issue["issue_type"] or "—")[:col_i]
            s = (issue["source"]     or "—")[:col_s]
            x = (issue["status"]     or "—")[:col_x]
            print(f"  {d:<{col_d}}  {t:<{col_t}}  {i:<{col_i}}  {s:<{col_s}}  {x:<{col_x}}")
        print()

    # ── Step 5: MCT lookup ─────────────────────────────────────────────────
    mct_lookup = build_mct_id_lookup()

    # ── Step 6: Validate linked ────────────────────────────────────────────
    valid_linked, broken_linked = validate_linked(p1_issues, mct_lookup)

    print("Step 6: Validating linked issues against MCT ...")
    if broken_linked:
        print(f"  WARNING: {len(broken_linked)} issue(s) have broken MCT links:\n")
        for issue in broken_linked:
            print(f"  • {issue['title'][:60]}")
            print(f"    Created: {issue['created_at'][:10]}")
            for mid in issue["missing_ids"]:
                print(f"    Missing MCT page: {mid}")
        print()
    else:
        print(f"  All {len(linked)} linked issues point to valid MCT rows. ✓\n")

    # ── Step 7: Final summary ──────────────────────────────────────────────
    # Breakdowns for unlinked
    type_counts   = defaultdict(int)
    source_counts = defaultdict(int)
    for issue in unlinked:
        type_counts[issue["issue_type"]   or "(blank)"] += 1
        source_counts[issue["source"]     or "(blank)"] += 1

    filter_label = f"Severity={P1_VALUE}" if P1_VALUE else "all severities"
    print(f"\n{sep}")
    print(f"  Issues Diagnostic ({filter_label}) — {DATE_START} to {DATE_END}")
    print(sep)
    print(f"  Total issues in date range:   {total_in_range}")
    print(f"  Issues analysed:              {len(p1_issues)}")
    print(f"    ✓ Linked to customer:       {len(linked)}")
    print(f"      - Valid links:            {len(valid_linked)}")
    print(f"      - Broken links:           {len(broken_linked)}")
    print(f"    ✗ No customer linked:       {len(unlinked)}")

    if unlinked:
        print()
        print("  Unlinked breakdown by Issue Type:")
        for k, v in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"    {k:<30}: {v}")

        print()
        print("  Unlinked breakdown by Source:")
        for k, v in sorted(source_counts.items(), key=lambda x: -x[1]):
            print(f"    {k:<30}: {v}")

    print(sep)
    print()


if __name__ == "__main__":
    main()
