#!/usr/bin/env python3
"""
audit_w09_scorecard.py

Read-only verification script for the W09 scorecard row in Notion.
Re-computes every KPI from raw source data and prints a side-by-side
comparison so you can spot any discrepancies without touching anything.

Run: python3 audit_w09_scorecard.py
"""

import statistics
import time
import requests
from datetime import date, datetime, timezone

# ── Constants ─────────────────────────────────────────────────────────────────
NOTION_TOKEN     = "***REMOVED***"
MCT_DS_ID        = "3ceb1ad0-91f1-40db-945a-c51c58035898"
SCORECARD_PAGE   = "311e418f-d8c4-81b1-8552-d12c067c1089"   # W09 page

INTERCOM_TOKEN   = "***REMOVED***"
ALEX_ADMIN_ID    = "7484673"   # Alex de Godoy
AYA_ADMIN_ID     = "8411967"   # Aya Guerimej

WEEK_LABEL       = "W09 (Feb 24 - Mar 2)"
WEEK_START       = date(2026, 2, 24)
WEEK_END         = date(2026, 3, 2)

NOTION_API       = "https://api.notion.com/v1"
INTERCOM_API     = "https://api.intercom.io"

# Values that were written into Notion when the scorecard was built
# (these are the ground truth we're checking against)
STORED = {
    "Alex": {
        "red_health":          52,
        "no_contact":          44,
        "churned":              0,
        "graduated":            0,
        "customers_contacted": 10,
        "avg_reply_time":     0.8,
    },
    "Aya": {
        "red_health":          12,
        "no_contact":          34,
        "churned":              0,
        "graduated":            0,
        "customers_contacted": 10,
        "avg_reply_time":      37,
    },
}

# ── HTTP headers ──────────────────────────────────────────────────────────────
std_headers = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}

mct_headers = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2025-09-03",
}

intercom_headers = {
    "Authorization":    f"Bearer {INTERCOM_TOKEN}",
    "Intercom-Version": "2.11",
    "Accept":           "application/json",
    "Content-Type":     "application/json",
}


# ── Section 1: Helpers (verbatim from build_weekly_scorecard.py) ──────────────

def _to_unix(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def parse_date(date_str):
    """Return a date object from a YYYY-MM-DD string, or None."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def in_week(d):
    """Return True if the date falls within the scorecard week."""
    return d is not None and WEEK_START <= d <= WEEK_END


def safe_str(prop, path):
    """Safely navigate a nested dict path (list of keys), return str or None."""
    obj = prop
    for key in path:
        if obj is None or not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj if isinstance(obj, str) else None


def safe_num(prop, path):
    """Safely navigate a nested dict path, return number or None."""
    obj = prop
    for key in path:
        if obj is None or not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj if isinstance(obj, (int, float)) else None


# ── Section 2: Fetch live W09 scorecard from Notion ──────────────────────────

def fetch_stored_scorecard():
    """GET the live W09 Notion page and return a dict of its numeric properties."""
    print(f"\n[0] Fetching live W09 scorecard from Notion (page {SCORECARD_PAGE}) …")
    r = requests.get(f"{NOTION_API}/pages/{SCORECARD_PAGE}", headers=std_headers)
    r.raise_for_status()
    page = r.json()
    props = page.get("properties", {})

    def num(key):
        p = props.get(key, {})
        return p.get("number")  # None if blank

    live = {
        "Alex": {
            "red_health":          num("Alex: Red Health"),
            "no_contact":          num("Alex: No Contact >21d"),
            "churned":             num("Alex: Churned"),
            "graduated":           num("Alex: Graduated"),
            "customers_contacted": num("Alex: Customers Contacted"),
            "avg_reply_time":      num("Alex: Avg Reply Time"),
        },
        "Aya": {
            "red_health":          num("Aya: Red Health"),
            "no_contact":          num("Aya: No Contact >21d"),
            "churned":             num("Aya: Churned"),
            "graduated":           num("Aya: Graduated"),
            "customers_contacted": num("Aya: Customers Contacted"),
            "avg_reply_time":      num("Aya: Avg Reply Time"),
        },
    }

    print(f"   Alex (live): {live['Alex']}")
    print(f"   Aya  (live): {live['Aya']}")
    return live


# ── Section 3: Fetch all MCT rows ─────────────────────────────────────────────

def fetch_all_mct_rows():
    """Paginated POST /data_sources/{id}/query — identical to build script."""
    print("\n[1] Fetching all MCT rows via data_sources API …")

    url = f"{NOTION_API}/data_sources/{MCT_DS_ID}/query"
    all_results = []
    cursor = None
    page_num = 0

    while True:
        page_num += 1
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        r = requests.post(url, headers=mct_headers, json=body)
        r.raise_for_status()
        data = r.json()

        batch = data.get("results", [])
        all_results.extend(batch)
        print(f"   Page {page_num}: {len(batch)} rows (total: {len(all_results)})")
        time.sleep(0.3)

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    print(f"   Total rows fetched: {len(all_results)}")
    return all_results


# ── Section 4: Compute MCT KPIs with full row capture ─────────────────────────

def compute_mct_kpis(rows):
    """
    Identical KPI logic as build_weekly_scorecard.py compute_kpis(),
    plus full row lists for spot-checking.
    """
    print("\n[2] Computing MCT KPIs …")

    counts = {
        "Alex": {"red_health": 0, "no_contact": 0, "churned": 0, "graduated": 0},
        "Aya":  {"red_health": 0, "no_contact": 0, "churned": 0, "graduated": 0},
    }

    # Per-KPI detail lists: (company_name, page_id, extra_info, billing_status)
    detail = {
        "Alex": {"red_health": [], "no_contact": [], "churned": [], "graduated": []},
        "Aya":  {"red_health": [], "no_contact": [], "churned": [], "graduated": []},
    }

    owner_counts = {}       # owner_name -> total rows
    billing_by_owner = {}   # owner_name -> {billing_status -> count}
    skipped = 0

    for row in rows:
        props   = row.get("properties", {})
        page_id = row.get("id", "")

        # Company name (title property)
        title_items = props.get("🏢 Company Name", {}).get("title", [])
        company = title_items[0].get("plain_text", "(no name)") if title_items else "(no name)"

        # CS Owner
        owner = safe_str(props.get("⭐ CS Owner", {}), ["select", "name"])
        owner_counts[owner] = owner_counts.get(owner, 0) + 1

        # Billing status
        billing_status = safe_str(props.get("💰 Billing Status", {}), ["select", "name"]) or ""

        # Track billing breakdown per owner
        billing_by_owner.setdefault(owner, {})
        billing_by_owner[owner][billing_status] = billing_by_owner[owner].get(billing_status, 0) + 1

        if owner not in ("Alex", "Aya"):
            skipped += 1
            continue

        is_active = billing_status == "Active"

        # Health Status (formula → string)
        health_status = safe_str(props.get("🚦 Health Status", {}), ["formula", "string"]) or ""

        # Days Since Last Contact (formula → number)
        days_no_contact = safe_num(props.get("📞 Days Since Last Contact", {}), ["formula", "number"])

        # Churn Date
        churn_date_str = safe_str(props.get("😢 Churn Date", {}), ["date", "start"])

        # Graduation Date
        grad_date_str = safe_str(props.get("🚀 Graduation Date", {}), ["date", "start"])

        is_churned_stage = not is_active

        # KPI 1: Red Health (active only)
        if health_status and "Red" in health_status and not is_churned_stage:
            counts[owner]["red_health"] += 1
            detail[owner]["red_health"].append((company, page_id, health_status, billing_status))

        # KPI 2: No Contact >21d (active only)
        if days_no_contact is not None and days_no_contact > 21 and not is_churned_stage:
            counts[owner]["no_contact"] += 1
            detail[owner]["no_contact"].append((company, page_id, days_no_contact, billing_status))

        # KPI 4: Churned this week
        churn_date = parse_date(churn_date_str)
        if in_week(churn_date):
            counts[owner]["churned"] += 1
            detail[owner]["churned"].append((company, page_id, str(churn_date), billing_status))

        # KPI 5: Graduated this week
        grad_date = parse_date(grad_date_str)
        if in_week(grad_date):
            counts[owner]["graduated"] += 1
            detail[owner]["graduated"].append((company, page_id, str(grad_date), billing_status))

    print(f"   Owner breakdown (all rows): {owner_counts}")
    print(f"   Skipped (no Alex/Aya owner): {skipped}")
    print(f"   Alex: red={counts['Alex']['red_health']}, no_contact={counts['Alex']['no_contact']}, "
          f"churned={counts['Alex']['churned']}, grad={counts['Alex']['graduated']}")
    print(f"   Aya:  red={counts['Aya']['red_health']}, no_contact={counts['Aya']['no_contact']}, "
          f"churned={counts['Aya']['churned']}, grad={counts['Aya']['graduated']}")

    return counts, detail, owner_counts, billing_by_owner


# ── Section 5: Fetch Intercom conversations ───────────────────────────────────

def fetch_intercom_conversations():
    """
    Paginated POST /conversations/search — identical to build_weekly_scorecard.py.
    Returns full raw conversation list.
    """
    print("\n[3] Fetching Intercom conversations …")

    url = f"{INTERCOM_API}/conversations/search"
    query = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "created_at", "operator": ">",  "value": _to_unix(WEEK_START)},
                {"field": "created_at", "operator": "<=", "value": _to_unix(WEEK_END)},
            ],
        },
        "pagination": {"per_page": 150},
    }

    all_convs = []
    page = 1
    cursor = None

    while True:
        if cursor:
            query["pagination"]["starting_after"] = cursor
        elif "starting_after" in query["pagination"]:
            del query["pagination"]["starting_after"]

        r = requests.post(url, headers=intercom_headers, json=query)
        r.raise_for_status()
        data = r.json()
        batch = data.get("conversations", [])
        all_convs.extend(batch)
        print(f"   Page {page}: {len(batch)} conversations (total: {len(all_convs)})")

        pages = data.get("pages", {})
        next_page = pages.get("next", {})
        cursor = next_page.get("starting_after") if isinstance(next_page, dict) else None
        if not cursor or len(batch) == 0:
            break
        page += 1

    print(f"   Total fetched: {len(all_convs)}")
    return all_convs


# ── Section 6: Compute Intercom KPIs ──────────────────────────────────────────

def _contact_name(conv):
    """Extract first contact's display name from a conversation object."""
    try:
        contacts = conv.get("contacts", {}).get("contacts", [])
        if contacts:
            name = contacts[0].get("name") or ""
            if name.strip():
                return name.strip()
            cid = contacts[0].get("id", "")
            return f"(id={cid})"
    except Exception:
        pass
    return "(unknown)"


def _format_ts(ts):
    if not ts:
        return "?"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%b %-d %H:%M")


def compute_intercom_kpis(conversations):
    """
    Same attribution and metric logic as build_weekly_scorecard.py fetch_reply_times().
    Also captures per-conversation details for spot-checking.
    """
    print("\n[4] Computing Intercom KPIs …")

    by_admin   = {}    # admin_id -> list of delta seconds
    conv_count = {}    # admin_id -> int
    conv_detail = {}   # admin_id -> list of (conv_id, contact_name, created_str, delta_min|None)
    other_admins = {}  # admin_id -> count (not Alex or Aya)
    no_closer = 0

    for c in conversations:
        stats  = c.get("statistics") or {}
        closer = stats.get("last_closed_by_id")
        if closer is None:
            no_closer += 1
            continue
        closer = str(closer)
        conv_count[closer] = conv_count.get(closer, 0) + 1

        reply_at      = stats.get("last_assignment_admin_reply_at")
        assignment_at = stats.get("last_assignment_at")
        delta = (reply_at - assignment_at) if (reply_at and assignment_at and reply_at > assignment_at) else None
        if delta is not None:
            by_admin.setdefault(closer, []).append(delta)

        # Detail record
        conv_id     = c.get("id", "")
        contact     = _contact_name(c)
        created_str = _format_ts(c.get("created_at"))
        delta_min   = round(delta / 60, 1) if delta is not None else None
        conv_detail.setdefault(closer, []).append((conv_id, contact, created_str, delta_min))

        if closer not in (ALEX_ADMIN_ID, AYA_ADMIN_ID):
            other_admins[closer] = other_admins.get(closer, 0) + 1

    # Build result dict
    result = {}
    for admin_id, label in [(ALEX_ADMIN_ID, "Alex"), (AYA_ADMIN_ID, "Aya")]:
        times = by_admin.get(admin_id, [])
        convs = conv_count.get(admin_id, 0)
        if times:
            med_min = round(statistics.median(times) / 60, 1)
        else:
            med_min = None
        result[label] = {
            "reply_min":   med_min,
            "convs":       convs,
            "n_reply":     len(times),
            "times":       times,
            "detail":      conv_detail.get(admin_id, []),
        }
        print(f"   {label}: median={med_min} min (n_reply={len(times)}), convs_closed={convs}")

    print(f"   No closer (open/snoozed): {no_closer}")
    print(f"   Other admins: {other_admins}")

    return result, other_admins, no_closer


# ── Section 7: Print report ────────────────────────────────────────────────────

SEP  = "=" * 80
SEP2 = "-" * 80


def _flag(stored_val, computed_val, tolerance=0):
    """Return OK or DIFF."""
    if stored_val is None and computed_val is None:
        return "OK"
    if stored_val is None or computed_val is None:
        return "DIFF"
    if abs(stored_val - computed_val) <= tolerance:
        return "OK"
    return "DIFF"


def print_report(live_stored, computed_mct, computed_intercom, owner_counts,
                 billing_by_owner, detail, other_admins, no_closer):
    counts_mct, _, _, _ = computed_mct   # unpack the tuple returned by compute_mct_kpis
    intercom_result, _, _ = computed_intercom

    diffs = []

    # ── Block 1: KPI comparison table ──────────────────────────────────────────
    print(f"\n{SEP}")
    print("  KPI COMPARISON  —  W09 (Feb 24 – Mar 2 2026)")
    print(SEP)

    col_kpi  = 30
    col_who  = 6
    col_val  = 10
    col_flg  = 6

    header = (f"  {'KPI':<{col_kpi}}"
              f"  {'Who':>{col_who}}"
              f"  {'Stored':>{col_val}}"
              f"  {'Computed':>{col_val}}"
              f"  {'Flag':>{col_flg}}")
    print(header)
    print(f"  {SEP2}")

    def row(kpi_label, owner, stored_val, computed_val, tolerance=0):
        flag = _flag(stored_val, computed_val, tolerance)
        sv   = str(stored_val)   if stored_val   is not None else "–"
        cv   = str(computed_val) if computed_val is not None else "–"
        line = (f"  {kpi_label:<{col_kpi}}"
                f"  {owner:>{col_who}}"
                f"  {sv:>{col_val}}"
                f"  {cv:>{col_val}}"
                f"  {flag:>{col_flg}}")
        print(line)
        if flag == "DIFF":
            diffs.append(f"{kpi_label} / {owner}: stored={sv}, computed={cv}")

    for owner in ("Alex", "Aya"):
        row("Red Health",          owner,
            STORED[owner]["red_health"],
            counts_mct[owner]["red_health"])
        row("No Contact >21d",     owner,
            STORED[owner]["no_contact"],
            counts_mct[owner]["no_contact"])
        row("Churned this week",   owner,
            STORED[owner]["churned"],
            counts_mct[owner]["churned"])
        row("Graduated this week", owner,
            STORED[owner]["graduated"],
            counts_mct[owner]["graduated"])
        row("Avg Reply Time (min)", owner,
            STORED[owner]["avg_reply_time"],
            intercom_result[owner]["reply_min"],
            tolerance=0.1)
        row("Customers Contacted", owner,
            STORED[owner]["customers_contacted"],
            intercom_result[owner]["convs"])

    print()

    # ── Block 1b: Live Notion values vs stored hardcoded ───────────────────────
    print(f"\n{SEP2}")
    print("  LIVE NOTION VALUES  (what's actually in the Notion row right now)")
    print(SEP2)
    print(f"  {'KPI':<{col_kpi}}  {'Who':>{col_who}}  {'Hardcoded':>{col_val}}  {'Notion live':>{col_val}}  {'Flag':>{col_flg}}")
    print(f"  {SEP2}")

    kpi_map = [
        ("Red Health",           "red_health"),
        ("No Contact >21d",      "no_contact"),
        ("Churned this week",    "churned"),
        ("Graduated this week",  "graduated"),
        ("Avg Reply Time (min)", "avg_reply_time"),
        ("Customers Contacted",  "customers_contacted"),
    ]
    tol_map = {"avg_reply_time": 0.1}

    for kpi_label, key in kpi_map:
        for owner in ("Alex", "Aya"):
            hc  = STORED[owner][key]
            lv  = live_stored[owner][key]
            flg = _flag(hc, lv, tolerance=tol_map.get(key, 0))
            hc_s = str(hc) if hc is not None else "–"
            lv_s = str(lv) if lv is not None else "–"
            print(f"  {kpi_label:<{col_kpi}}  {owner:>{col_who}}  {hc_s:>{col_val}}  {lv_s:>{col_val}}  {flg:>{col_flg}}")

    print()

    # ── Block 2: Owner distribution ────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  OWNER DISTRIBUTION  (all MCT rows)")
    print(SEP2)
    for owner, total in sorted(owner_counts.items(), key=lambda x: -x[1]):
        owner_label = owner if owner else "(no owner)"
        billing_str = ", ".join(
            f"{k}:{v}" for k, v in
            sorted(billing_by_owner.get(owner, {}).items(), key=lambda x: -x[1])
        )
        print(f"  {owner_label:<20}  {total:>4} rows    {billing_str}")

    print()

    # ── Block 3: Red Health samples ────────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  RED HEALTH SAMPLES  (active customers, health contains 'Red')")
    print(SEP2)
    for owner in ("Alex", "Aya"):
        rows_list = detail[owner]["red_health"]
        print(f"\n  {owner}: {len(rows_list)} customers")
        for company, page_id, health, billing in rows_list[:20]:
            pid_clean = page_id.replace("-", "")
            print(f"    {company:<40}  {health:<20}  {billing:<12}  notion.so/{pid_clean}")
        if len(rows_list) > 20:
            print(f"    … and {len(rows_list) - 20} more")

    print()

    # ── Block 4: No Contact >21d samples ──────────────────────────────────────
    print(f"\n{SEP2}")
    print("  NO CONTACT >21d SAMPLES  (active customers)")
    print(SEP2)
    for owner in ("Alex", "Aya"):
        rows_list = sorted(detail[owner]["no_contact"], key=lambda x: -x[2])
        print(f"\n  {owner}: {len(rows_list)} customers")
        for company, page_id, days, billing in rows_list[:20]:
            pid_clean = page_id.replace("-", "")
            print(f"    {company:<40}  {int(days):>4}d  {billing:<12}  notion.so/{pid_clean}")
        if len(rows_list) > 20:
            print(f"    … and {len(rows_list) - 20} more")

    print()

    # ── Block 5: Churned / Graduated ──────────────────────────────────────────
    print(f"\n{SEP2}")
    print("  CHURNED THIS WEEK")
    print(SEP2)
    total_churned = 0
    for owner in ("Alex", "Aya"):
        for company, page_id, churn_date, billing in detail[owner]["churned"]:
            pid_clean = page_id.replace("-", "")
            print(f"  [{owner}]  {company:<40}  churn={churn_date}  {billing}  notion.so/{pid_clean}")
            total_churned += 1
    if total_churned == 0:
        print("  (none)")

    print(f"\n{SEP2}")
    print("  GRADUATED THIS WEEK")
    print(SEP2)
    total_grad = 0
    for owner in ("Alex", "Aya"):
        for company, page_id, grad_date, billing in detail[owner]["graduated"]:
            pid_clean = page_id.replace("-", "")
            print(f"  [{owner}]  {company:<40}  grad={grad_date}  {billing}  notion.so/{pid_clean}")
            total_grad += 1
    if total_grad == 0:
        print("  (none)")

    print()

    # ── Block 6: Intercom conversation detail ──────────────────────────────────
    print(f"\n{SEP2}")
    print("  INTERCOM CONVERSATION DETAIL  (per admin)")
    print(SEP2)

    for owner, label_id in [("Alex", ALEX_ADMIN_ID), ("Aya", AYA_ADMIN_ID)]:
        data        = intercom_result[owner]
        convs_list  = data["detail"]
        times_sec   = data["times"]
        n_reply     = data["n_reply"]
        no_reply    = data["convs"] - n_reply

        print(f"\n  {owner}: {data['convs']} conversations closed,  "
              f"{n_reply} with reply data,  {no_reply} no-reply,  "
              f"median={data['reply_min']} min")

        if times_sec:
            min_min = round(min(times_sec) / 60, 1)
            max_min = round(max(times_sec) / 60, 1)
            print(f"    min={min_min} min, max={max_min} min")

        print(f"\n    {'#':>3}  {'Contact':<28}  {'Created':<17}  {'Reply (min)':>12}  Conv ID")
        print(f"    {'─'*3}  {'─'*28}  {'─'*17}  {'─'*12}  {'─'*20}")
        for i, (conv_id, contact, created, delta_min) in enumerate(convs_list, 1):
            dstr = f"{delta_min}m" if delta_min is not None else "–"
            print(f"    {i:>3}  {contact:<28}  {created:<17}  {dstr:>12}  {conv_id}")

    if other_admins:
        print(f"\n  Other admins (convs closed): {other_admins}")
        print(f"  Open/snoozed (no closer): {no_closer}")

    print()

    # ── Block 7: Discrepancy summary ──────────────────────────────────────────
    print(f"\n{SEP}")
    if diffs:
        print(f"  DISCREPANCIES FOUND: {len(diffs)}")
        print(SEP)
        for d in diffs:
            print(f"  DIFF  {d}")
        print(f"\n  OVERALL: FAIL  ({len(diffs)} KPI(s) mismatch)")
    else:
        print(f"  ALL KPIs MATCH  —  OVERALL: PASS")
    print(SEP)
    print()


# ── Section 8: Main ───────────────────────────────────────────────────────────

def main():
    print(SEP)
    print(f"  W09 Scorecard Audit  —  {WEEK_LABEL}")
    print(f"  Verification run: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(SEP)

    # Step 0: live Notion scorecard
    live_stored = fetch_stored_scorecard()

    # Step 1-2: MCT rows + KPI computation
    rows = fetch_all_mct_rows()
    counts_mct, detail, owner_counts, billing_by_owner = compute_mct_kpis(rows)

    # Step 3-4: Intercom conversations + KPI computation
    conversations = fetch_intercom_conversations()
    intercom_result, other_admins, no_closer = compute_intercom_kpis(conversations)

    # Package results as tuples for print_report
    computed_mct        = (counts_mct, detail, owner_counts, billing_by_owner)
    computed_intercom   = (intercom_result, other_admins, no_closer)

    print_report(
        live_stored,
        computed_mct,
        computed_intercom,
        owner_counts,
        billing_by_owner,
        detail,
        other_admins,
        no_closer,
    )


if __name__ == "__main__":
    main()
