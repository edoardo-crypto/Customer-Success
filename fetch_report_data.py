"""
fetch_report_data.py

WHEN TO RUN: Before every biweekly DAGs & Churn meeting (every 2 weeks).
Run again on Sunday night / Monday morning for the most up-to-date data.

Fetches live data from Notion Issues Table, MCT, and Stripe.
Writes report_data.json, which generate_meeting_report.py reads.
Also auto-generates Key Takeaways (Slide 2) and freezes resolution snapshots.

    python3 fetch_report_data.py
    python3 generate_meeting_report.py   # then open meeting_report.html
"""

import json
import os
import sys
import time
import requests
from datetime import datetime, date, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────

NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
STRIPE_KEY        = os.environ["STRIPE_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"
MCT_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"

OUTPUT_FILE      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report_data.json")
SNAPSHOTS_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resolution_snapshots.json")

NOTION_HDR_V1 = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type":  "application/json",
}
NOTION_HDR_V2 = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type":  "application/json",
}

# ── PERIOD DEFINITIONS ────────────────────────────────────────────────────────

PERIOD_RANGES = [
    {"label": "P1", "display": "P1 (Feb 16 – Mar 1)",  "start": "2026-02-16", "end": "2026-03-01"},
    {"label": "P2", "display": "P2 (Mar 2 – Mar 15)",  "start": "2026-03-02", "end": "2026-03-15"},
    {"label": "P3", "display": "P3 (Mar 16 – Mar 29)", "start": "2026-03-16", "end": "2026-03-29"},
]
CURRENT_PERIOD = "P1"   # P1 is the only period with data; update each cycle
TODAY = date.today()

# ── ISSUE CLASSIFICATION ──────────────────────────────────────────────────────

# CS owners whose issues are in scope
CS_OWNERS_IN_SCOPE = {"Alex", "Aya"}

# Bug type (for the bug/feature split in slide 6)
BUG_TYPES_SET     = {"Bug"}
# Feature requests — shown separately in slide 6 chart
FEATURE_TYPES_SET = {"New feature request", "Feature improvement", "Feature Improvement Request"}
# Explicitly excluded from all counts
EXCLUDE_TYPES_SET = {"No Issue", "Config Issue"}

# Notion Category → display name for slide 2 (maps to one of 6 chart buckets)
# Items with no/unknown category → "Uncategorized"
CATEGORY_DISPLAY = {
    "Feature request":   "Feature request",
    "New feature":       "Feature request",   # legacy value — map to new name
    "AI Behavior":       "AI Behavior",
    "Integration":       "Integration",
    "Platform & UI":     "Platform & UI",
    "Billing & Account": "Billing & Account",
    # Everything else collapses into "Uncategorized"
}
# Exactly 5 named Notion category buckets — issues with no/unknown category are excluded from the chart
CATEGORY_BUCKETS = ["Feature request", "AI Behavior", "Integration", "Platform & UI", "Billing & Account"]

VALID_CATEGORIES    = ["Feature request", "AI Behavior", "Integration", "Platform & UI", "Billing & Account"]
CLASSIFY_BATCH_SIZE = 20


# ── NOTION HELPERS ────────────────────────────────────────────────────────────

def notion_query_all(url, headers, body):
    """Paginate through all results from a Notion query endpoint."""
    pages = []
    body  = {**body, "page_size": 100}
    while True:
        r = requests.post(url, headers=headers, json=body, timeout=30)
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        body["start_cursor"] = data["next_cursor"]
    return pages


def get_date(page, prop_name):
    prop = page.get("properties", {}).get(prop_name)
    if not prop or prop.get("type") != "date":
        return None
    d = prop.get("date")
    return d["start"][:10] if d and d.get("start") else None


def get_select(page, prop_name):
    prop = page.get("properties", {}).get(prop_name)
    if not prop:
        return None
    sel = prop.get("select")
    return sel.get("name") if sel else None


def get_checkbox(page, prop_name):
    prop = page.get("properties", {}).get(prop_name)
    return bool(prop.get("checkbox", False)) if prop else False


def get_title(page, prop_name):
    prop = page.get("properties", {}).get(prop_name)
    if not prop:
        return ""
    return "".join(t.get("plain_text", "") for t in prop.get("title", []))


def get_rich_text(page, prop_name):
    prop = page.get("properties", {}).get(prop_name)
    if not prop:
        return ""
    return "".join(t.get("plain_text", "") for t in prop.get("rich_text", []))


def get_relation_ids(page, prop_name):
    prop = page.get("properties", {}).get(prop_name)
    if not prop:
        return []
    return [r["id"] for r in prop.get("relation", [])]


def get_number(page, prop_name):
    prop = page.get("properties", {}).get(prop_name)
    return prop.get("number") if prop else None


def get_rollup_select_names(page, prop_name):
    """Return list of names from a rollup whose array contains select items."""
    prop = page.get("properties", {}).get(prop_name)
    if not prop or prop.get("type") != "rollup":
        return []
    arr = prop.get("rollup", {}).get("array", [])
    return [
        item["select"]["name"]
        for item in arr
        if item.get("type") == "select" and item.get("select")
    ]


def date_in_period(d_str, period):
    return bool(d_str) and period["start"] <= d_str <= period["end"]


def period_for_date(d_str):
    for p in PERIOD_RANGES:
        if date_in_period(d_str, p):
            return p["label"]
    return None


# ── FETCH RAW DATA ────────────────────────────────────────────────────────────

def fetch_all_issues():
    print("📋 Fetching Issues Table…")
    url   = f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query"
    pages = notion_query_all(url, NOTION_HDR_V1, {})
    print(f"   → {len(pages)} issue pages")
    return pages


def fetch_all_mct():
    print("🏢 Fetching Master Customer Table…")
    url   = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"
    pages = notion_query_all(url, NOTION_HDR_V2, {})
    print(f"   → {len(pages)} customer pages")
    return pages


# ── AUTO-CLASSIFICATION ───────────────────────────────────────────────────────

CLASSIFY_PROMPT = """\
Classify each customer-support issue into exactly one category.

VALID CATEGORIES (use these exact strings):
- Feature request
- AI Behavior
- Integration
- Platform & UI
- Billing & Account

ISSUES:
{issues_json}

Return ONLY a JSON object mapping each "id" to its "category".
Example: {{"abc": "AI Behavior", "def": "Integration"}}
Use the exact strings above. No other text."""


def _call_claude_classify(batch):
    """Call Claude Haiku to classify a batch of issues. Returns {id: category} dict."""
    try:
        issues_json = json.dumps([{"id": item["id"], "text": item["text"]} for item in batch], ensure_ascii=False)
        prompt = CLASSIFY_PROMPT.format(issues_json=issues_json)
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 1024,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        r.raise_for_status()
        content_blocks = r.json().get("content", [])
        # Keep last text block (per MEMORY.md pattern — first may be preamble)
        last_text = ""
        for block in content_blocks:
            if block.get("type") == "text":
                last_text = block.get("text", "")
        # Strip markdown code fences if Claude wrapped the JSON
        stripped = last_text.strip()
        if stripped.startswith("```"):
            lines = stripped.splitlines()
            stripped = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        result = json.loads(stripped)
        # Validate each returned category
        validated = {}
        for pid, cat in result.items():
            if cat in VALID_CATEGORIES:
                validated[pid] = cat
            else:
                print(f"   ⚠️  Unknown category '{cat}' for {pid} — skipping")
        return validated
    except Exception as e:
        print(f"   ⚠️  Claude classify failed: {e}")
        return {}


def _patch_notion_category(page_id, category):
    """Write the Category select back to an Issues Table page. Returns True on success."""
    try:
        r = requests.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=NOTION_HDR_V1,
            json={"properties": {"Category": {"select": {"name": category}}}},
            timeout=20,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"   ⚠️  Notion PATCH failed for {page_id}: {e}")
        return False


def classify_missing_categories(raw_issues):
    """
    For any issue page without a Category, call Claude to classify it and
    write the result back to Notion. Returns {page_id: category} for all
    successfully classified issues so the caller can apply them in-memory.
    """
    unclassified = []
    for page in raw_issues:
        existing = get_select(page, "Category")
        if existing:
            continue  # already classified — skip
        title   = get_title(page, "Issue Title")
        summary = get_rich_text(page, "Summary")
        if not title and not summary:
            continue  # nothing to classify
        text = (title + " | " + summary)[:500]
        unclassified.append({"id": page["id"], "text": text})

    if not unclassified:
        print("   No unclassified issues — skipping Claude classification.")
        return {}

    print(f"   {len(unclassified)} issues need classification")

    all_results = {}
    patched = 0
    failed  = 0

    for i in range(0, len(unclassified), CLASSIFY_BATCH_SIZE):
        batch = unclassified[i: i + CLASSIFY_BATCH_SIZE]
        print(f"   Classifying batch {i // CLASSIFY_BATCH_SIZE + 1} ({len(batch)} issues)…")
        batch_results = _call_claude_classify(batch)
        for pid, cat in batch_results.items():
            ok = _patch_notion_category(pid, cat)
            if ok:
                all_results[pid] = cat
                patched += 1
            else:
                failed += 1
            time.sleep(0.25)

    print(f"   Classification done: {patched} patched, {failed} failed")
    return all_results


# ── PARSE ─────────────────────────────────────────────────────────────────────

def parse_issue(page):
    # Prefer the 'Created At' date property; fall back to page creation timestamp
    created_at = get_date(page, "Created At")
    if not created_at:
        created_at = page.get("created_time", "")[:10]

    issue_type = get_select(page, "Issue Type") or ""
    # "Assigned To" is a rollup of ⭐ CS Owner from the linked customer row
    cs_owner_names = get_rollup_select_names(page, "Assigned To")
    cs_owner       = cs_owner_names[0] if cs_owner_names else ""
    is_excluded = issue_type in EXCLUDE_TYPES_SET or not issue_type
    return {
        "id":           page["id"],
        "created_at":   created_at,
        "resolved_at":  get_date(page, "Resolved At"),
        "issue_type":   issue_type,
        "category":     get_select(page, "Category") or "",
        "status":       get_select(page, "Status") or "",
        "source":       get_select(page, "Source") or "",
        "informed":     get_checkbox(page, "✅ Customer Informed?"),
        "customer_ids": get_relation_ids(page, "Customer"),
        "cs_owner":     cs_owner,
        "title":        get_title(page, "Issue Title"),
        "is_bug":       issue_type in BUG_TYPES_SET,
        "is_feature":   issue_type in FEATURE_TYPES_SET,
        "is_excluded":  is_excluded,
        # in_scope: everything except excluded types, AND must have a CS owner assigned
        "is_in_scope":  not is_excluded and cs_owner in CS_OWNERS_IN_SCOPE,
        "period":       period_for_date(created_at),
    }


def build_customer_lookup(mct_pages):
    lookup = {}
    for page in mct_pages:
        lookup[page["id"]] = {
            "name":           get_title(page, "🏢 Company Name"),
            "mrr":            get_number(page, "💰 MRR"),
            "billing_status": get_select(page, "💰 Billing Status"),
            "churn_date":     get_date(page, "😢 Churn Date"),
            "churn_reason":   get_select(page, "🔁 Churn Reason"),
        }
    return lookup


# ── SLIDE 1: BUG VOLUME ───────────────────────────────────────────────────────

def build_slide1(issues_by_period):
    bug_volume          = []
    bug_source_intercom = []
    bug_source_meetings = []

    for p in PERIOD_RANGES:
        bugs = [i for i in issues_by_period[p["label"]] if i["is_in_scope"]]
        bug_volume.append(len(bugs))
        bug_source_intercom.append(sum(1 for i in bugs if i["source"] == "Intercom"))
        bug_source_meetings.append(sum(1 for i in bugs if i["source"] == "Meeting"))

    p1 = bug_volume[0] or 1
    target_line = [p1, round(p1 * 0.85, 1), round(p1 * 0.85 ** 2, 1)]

    return {
        "bug_volume":          bug_volume,
        "target_line":         target_line,
        "bug_source_intercom": bug_source_intercom,
        "bug_source_meetings": bug_source_meetings,
    }


# ── SLIDE 2: BUG CATEGORIZATION ───────────────────────────────────────────────

# Keyword theme clusters for the Key Takeaways card on Slide 2.
# Each entry: (list-of-keywords-in-title-lowercase, display-label)
# Keywords are matched against the issue title (lowercased). First match wins.
_THEMES_BY_CATEGORY = {
    "Platform & UI": [
        (["flow", "flows"],                                    "Flows stopping mid-execution"),
        (["broadcast", "campaign send"],                       "Broadcasts not sending"),
        (["inbox", "loading", "slow", "performance"],          "Inbox & performance issues"),
        (["handover", "transfer", "access", "notification"],   "Handover & access failures"),
    ],
    "AI Behavior": [
        (["discount", "wrong product", "wrong variation",
          "outdated", "incorrect", "didn't ident"],            "AI using wrong or outdated information"),
        (["confirm", "didn't execute", "failed to update"],    "AI confirming actions not completed"),
        (["duplicat", "after transfer", "timing"],             "AI message timing & duplication"),
        (["react", "reopen", "conversation state"],            "Incorrect conversation state changes"),
    ],
    "Integration": [
        (["outlook", "microsoft", "email integr"],             "Email/Outlook connection issues"),
        (["livechat", "live chat"],                            "Livechat channel issues"),
        (["instagram", "social", "whatsapp", "telegram"],      "Social channel issues"),
        (["campaign", "flow fail"],                            "Campaign/flow execution issues"),
    ],
}
_CAT_COLORS = {
    "Feature request":   "#4F8EF7",
    "AI Behavior":       "#F87171",
    "Integration":       "#A78BFA",
    "Platform & UI":     "#34D399",
    "Billing & Account": "#FBBF24",
}


def _cluster_themes(issues, category):
    """Bucket a list of issues into named themes via keyword matching on title."""
    theme_rules = _THEMES_BY_CATEGORY.get(category, [])
    buckets = defaultdict(int)
    for issue in issues:
        title_lc = issue.get("title", "").lower()
        matched = False
        for keywords, label in theme_rules:
            if any(kw in title_lc for kw in keywords):
                buckets[label] += 1
                matched = True
                break
        if not matched:
            buckets["_other"] += 1

    # Build ordered list: defined themes first (only those with ≥1), then "Other"
    result = []
    for _, label in theme_rules:
        if buckets.get(label, 0) > 0:
            result.append(f"{label} ({buckets[label]})")
    other_n = buckets.get("_other", 0)
    if other_n > 0:
        result.append(f"Other ({other_n})")
    return result


def build_slide2_takeaways(issues_by_period):
    """Return key_takeaways_s2: a list of category blocks for the current period."""
    cur_issues = [
        i for i in issues_by_period.get(CURRENT_PERIOD, [])
        if i["is_in_scope"]
    ]
    by_cat = defaultdict(list)
    for i in cur_issues:
        bucket = CATEGORY_DISPLAY.get(i["category"], "Uncategorized")
        by_cat[bucket].append(i)

    blocks = []
    # Sort by count descending so the biggest category appears first
    cats_with_issues = sorted(
        [(cat, by_cat[cat]) for cat in CATEGORY_BUCKETS if by_cat.get(cat)],
        key=lambda x: -len(x[1]),
    )
    for cat, issues in cats_with_issues:
        # Only include categories that have defined theme rules
        if cat not in _THEMES_BY_CATEGORY:
            continue
        blocks.append({
            "category": cat,
            "count":    len(issues),
            "color":    _CAT_COLORS.get(cat, "#94A3B8"),
            "themes":   _cluster_themes(issues, cat),
        })
    return blocks

def build_slide2(issues_by_period):
    bug_types = []

    for p in PERIOD_RANGES:
        bugs   = [i for i in issues_by_period[p["label"]] if i["is_in_scope"]]
        counts = defaultdict(int)
        for bug in bugs:
            bucket = CATEGORY_DISPLAY.get(bug["category"], "Uncategorized")
            counts[bucket] += 1
        bug_types.append([counts.get(b, 0) for b in CATEGORY_BUCKETS])

    return {
        "bug_types":      bug_types,
        "bug_type_names": CATEGORY_BUCKETS,
    }


# ── SNAPSHOT HELPERS ──────────────────────────────────────────────────────────

def load_snapshots() -> dict:
    if os.path.exists(SNAPSHOTS_FILE):
        with open(SNAPSHOTS_FILE) as f:
            return json.load(f)
    # Initialise empty structure for all periods
    return {p["label"]: {"7d": None, "14d": None, "28d": None} for p in PERIOD_RANGES}


def save_snapshots(snapshots: dict):
    with open(SNAPSHOTS_FILE, "w") as f:
        json.dump(snapshots, f, indent=2)


# ── SLIDE 3: RESOLUTION STATUS ────────────────────────────────────────────────

def build_slide3(issues_by_period, snapshots):
    """
    snapshots is mutated in-place when a new snapshot window becomes due.
    Caller must call save_snapshots() after this returns.
    """
    resolution_by_period = []
    resolution_rates     = []

    for p in PERIOD_RANGES:
        issues  = [i for i in issues_by_period[p["label"]] if i["is_in_scope"]]
        total_n = len(issues)

        open_n        = sum(1 for i in issues if i["status"] == "Open")
        in_progress_n = sum(1 for i in issues if i["status"] == "In Progress")
        resolved_n    = sum(1 for i in issues if i["status"] == "Resolved")

        resolution_by_period.append({
            "Open":        open_n,
            "In Progress": in_progress_n,
            "Resolved":    resolved_n,
        })

        # Resolution rate snapshots: frozen once measurement window closes
        rates = [None, None, None, None]   # 7d, 14d, 28d, current

        if total_n > 0:
            period_start = date.fromisoformat(p["start"])
            period_end   = date.fromisoformat(p["end"])
            label        = p["label"]
            snap       = snapshots.setdefault(label, {"7d": None, "14d": None, "28d": None})

            resolved_with_dates = [
                i for i in issues
                if i["resolved_at"] and i["created_at"]
            ]

            def pct_within(days):
                cnt = sum(
                    1 for i in resolved_with_dates
                    if (date.fromisoformat(i["resolved_at"]) -
                        date.fromisoformat(i["created_at"])).days <= days
                )
                return round(cnt / total_n * 100)

            # For each timeframe: use frozen snapshot if available; compute+freeze if due
            for key, days, idx in [("7d", 7, 0), ("14d", 14, 1), ("28d", 28, 2)]:
                trigger = period_start + timedelta(days=days)
                if snap[key] is not None:
                    rates[idx] = snap[key]          # already frozen — use it
                elif TODAY >= trigger:
                    value      = pct_within(days)
                    snap[key]  = value              # freeze now
                    rates[idx] = value
                # else: window not yet closed → remains None (renders as —)

            # "Current" = live resolved % — always recomputed, never frozen
            rates[3] = round(resolved_n / total_n * 100)

        resolution_rates.append(rates)

    return {
        "resolution_by_period": resolution_by_period,
        "resolution_rates":     resolution_rates,
    }


# ── SLIDE 4: COMMUNICATION LOOP ───────────────────────────────────────────────

def build_slide4(issues_by_period, all_issues, customer_lookup):
    comm_rate_trend = []

    for p in PERIOD_RANGES:
        resolved       = [i for i in issues_by_period[p["label"]]
                          if i["status"] == "Resolved" and i["is_in_scope"]]
        resolved_count = len(resolved)
        informed_count = sum(1 for i in resolved if i["informed"])
        rate = round(informed_count / resolved_count * 100) if resolved_count > 0 else 0
        comm_rate_trend.append({
            "period":   p["label"],
            "resolved": resolved_count,
            "informed": informed_count,
            "rate":     rate,
        })

    # Flagged customers: those with open issues that need attention
    open_issues     = [i for i in all_issues
                       if i["status"] == "Open" and i["is_in_scope"]]
    customer_open   = defaultdict(list)
    for issue in open_issues:
        for cid in issue["customer_ids"]:
            customer_open[cid].append(issue)

    flagged = []
    for cid, c_issues in customer_open.items():
        open_count   = len(c_issues)
        dates        = [i["created_at"] for i in c_issues if i["created_at"]]
        oldest_date  = min(dates) if dates else None
        days_waiting = (TODAY - date.fromisoformat(oldest_date)).days if oldest_date else 0

        if open_count > 3 or days_waiting > 10:
            cname = customer_lookup.get(cid, {}).get("name") or "Unknown"
            if open_count > 3 and days_waiting > 10:
                flag = "Both"
            elif days_waiting > 10:
                flag = "Wait time"
            else:
                flag = "Open issues"
            flagged.append({
                "Customer":     cname,
                "Days waiting": days_waiting,
                "Open issues":  open_count,
                "Flag":         flag,
            })

    flagged.sort(key=lambda x: (-x["Days waiting"], -x["Open issues"]))

    return {
        "comm_rate_trend":   comm_rate_trend,
        "flagged_customers": flagged,
    }


# ── SLIDE 5: CHURNS ───────────────────────────────────────────────────────────

def build_slide5(mct_pages):
    """Pull churn data from MCT with strict period filtering.

    - canceled_per_period: accounts whose subscription actually ended in each period
      (Billing Status = Canceled, Churn Date falls in that period's window)
    - churning_pipeline: all currently-churning customers sorted by cancel date
      (Billing Status = Churning, any cancel date — forward-looking pipeline)
    - canceled_this_period: convenience slice of canceled_per_period for CURRENT_PERIOD
    """
    # Build canceled_per_period — strict Churn Date filter, no clipping
    canceled_per_period = []
    for p in PERIOD_RANGES:
        customers = []
        for page in mct_pages:
            if get_select(page, "💰 Billing Status") != "Canceled":
                continue
            churn_date = get_date(page, "😢 Churn Date")
            if not date_in_period(churn_date, p):
                continue
            name   = get_title(page, "🏢 Company Name") or "Unknown"
            mrr    = get_number(page, "💰 MRR") or 0
            reason = get_select(page, "🔁 Churn Reason") or "Unknown"
            customers.append({"name": name, "mrr_raw": mrr, "reason": reason})
        customers.sort(key=lambda x: -x["mrr_raw"])
        canceled_per_period.append({
            "period":    p["label"],
            "label":     p["display"],
            "end":       p["end"],
            "count":     len(customers),
            "mrr":       sum(c["mrr_raw"] for c in customers),
            "customers": customers,
        })

    # Build churning_pipeline — all Churning customers, sorted by cancel date
    churning_pipeline = []
    for page in mct_pages:
        if get_select(page, "💰 Billing Status") != "Churning":
            continue
        name        = get_title(page, "🏢 Company Name") or "Unknown"
        mrr         = get_number(page, "💰 MRR") or 0
        cancel_date    = get_date(page, "📅 Cancel Date") or ""
        reason         = get_select(page, "🔁 Churn Reason") or "Unknown"
        churning_since = get_date(page, "📅 Churning Since") or ""
        churning_pipeline.append({
            "name":           name,
            "mrr_raw":        mrr,
            "cancel_date":    cancel_date,
            "churning_since": churning_since,
            "reason":         reason,
        })
    churning_pipeline.sort(key=lambda x: x["cancel_date"])

    cur = next((p for p in canceled_per_period if p["period"] == CURRENT_PERIOD), None)
    canceled_this_period = cur["customers"] if cur else []

    return {
        "canceled_per_period":  canceled_per_period,
        "churning_pipeline":    churning_pipeline,
        "canceled_this_period": canceled_this_period,
    }


# ── SLIDE 6: TOP CUSTOMERS ────────────────────────────────────────────────────

def build_slide6(issues_by_period, customer_lookup):
    """Top 10 customers by total issue volume in the current period."""
    issues = issues_by_period[CURRENT_PERIOD]

    customer_bugs     = defaultdict(int)
    customer_features = defaultdict(int)
    customer_names    = {}

    total_in_scope = sum(1 for i in issues if i["is_in_scope"])

    for issue in issues:
        if not issue["is_in_scope"]:
            continue
        for cid in issue["customer_ids"]:
            if cid not in customer_names:
                customer_names[cid] = customer_lookup.get(cid, {}).get("name") or "Unknown"
            if issue["is_bug"]:
                customer_bugs[cid] += 1
            elif issue["is_feature"]:
                customer_features[cid] += 1

    all_cids = set(customer_bugs) | set(customer_features)
    rows = [
        {
            "customer": customer_names.get(cid, "Unknown"),
            "issues":   customer_bugs.get(cid, 0) + customer_features.get(cid, 0),
            "bugs":     customer_bugs.get(cid, 0),
            "features": customer_features.get(cid, 0),
        }
        for cid in all_cids
    ]
    rows.sort(key=lambda x: -x["issues"])

    customers_count = len(set(
        cid for i in issues if i["is_in_scope"] for cid in i["customer_ids"]
    ))

    return {
        "top_customers_by_issues": rows[:10],
        "total_in_scope_issues":   total_in_scope,
        "customers_count":         customers_count,
    }


# ── MAIN ──────────────────────────────────────────────────────────────────────

def build_report_data(no_classify=False):
    raw_issues = fetch_all_issues()
    mct_pages  = fetch_all_mct()

    if not no_classify:
        print("🤖 Auto-classifying unclassified issues…")
        new_cats = classify_missing_categories(raw_issues)
        # Apply in-memory so this run sees the results immediately
        if new_cats:
            idx = {p["id"]: p for p in raw_issues}
            for pid, cat in new_cats.items():
                if pid in idx:
                    idx[pid].setdefault("properties", {})["Category"] = {
                        "type": "select", "select": {"name": cat}
                    }
    else:
        print("⏩ Skipping classification (--no-classify)")

    print("🔧 Parsing issues…")
    all_issues = [parse_issue(p) for p in raw_issues]

    issues_by_period = defaultdict(list)
    for issue in all_issues:
        if issue["period"]:
            issues_by_period[issue["period"]].append(issue)

    for p in PERIOD_RANGES:
        n = len(issues_by_period[p["label"]])
        print(f"   {p['label']}: {n} issues in range")

    customer_lookup = build_customer_lookup(mct_pages)

    snapshots = load_snapshots()

    print("🔨 Building slides…")
    s1 = build_slide1(issues_by_period)
    s2 = build_slide2(issues_by_period)
    s3 = build_slide3(issues_by_period, snapshots)   # may mutate snapshots
    save_snapshots(snapshots)                         # persist any newly-computed snapshots
    s4 = build_slide4(issues_by_period, all_issues, customer_lookup)
    s5 = build_slide5(mct_pages)
    s6 = build_slide6(issues_by_period, customer_lookup)
    takeaways = build_slide2_takeaways(issues_by_period)

    return {
        "fetched_at":       datetime.utcnow().isoformat() + "Z",
        "periods":          [p["display"] for p in PERIOD_RANGES],
        "current_period":   CURRENT_PERIOD,
        **s1, **s2, **s3, **s4, **s5, **s6,
        "key_takeaways_s2": takeaways,
    }


if __name__ == "__main__":
    no_classify = "--no-classify" in sys.argv
    data = build_report_data(no_classify=no_classify)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Written → {OUTPUT_FILE}")
    print("   Run `python3 generate_meeting_report.py` to render the HTML.")
