"""
fetch_report_data.py

WHEN TO RUN: Before every weekly Bug & Churn meeting.
Run again on Sunday night / Monday morning for the most up-to-date data.

Fetches live data from Notion Issues Table, MCT, and Stripe.
Writes report_data.json, which generate_meeting_report.py reads.
Also auto-generates Key Takeaways (Slide 2) and freezes resolution snapshots.

    python3 fetch_report_data.py
    python3 generate_meeting_report.py   # then open meeting_report.html
"""

import json
import os
import re
import sys
import time
import requests
from datetime import datetime, date, timedelta
from collections import defaultdict
from pathlib import Path
import creds


# ── CONFIG ────────────────────────────────────────────────────────────────────

NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
STRIPE_KEY        = os.environ.get("STRIPE_KEY", "")
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
LINEAR_TOKEN      = creds.get("LINEAR_TOKEN")

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

EPOCH_START = date(2026, 2, 16)  # First period's Monday — never changes


PERIOD_RANGES = [
    {"label": "W1", "display": "W1 (Feb 16–22)",
     "start": "2026-02-16", "end": "2026-02-22"},
    {"label": "W2", "display": "W2 (Feb 23–Mar 1)",
     "start": "2026-02-23", "end": "2026-03-01"},
    {"label": "W3", "display": "W3 (Mar 2–8)",
     "start": "2026-03-02", "end": "2026-03-08"},
    {"label": "W4", "display": "W4 (Mar 9–15)",
     "start": "2026-03-09", "end": "2026-03-15"},
    {"label": "W5", "display": "W5 (Mar 16–22)",
     "start": "2026-03-16", "end": "2026-03-22"},
]
CURRENT_PERIOD = "W5"
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

# Notion Category → display name for slide 2 (maps to one of 4 chart buckets)
# Items with no/unknown category → excluded from chart
CATEGORY_DISPLAY = {
    "AI Agent":           "AI Agent",
    "Inbox":              "Inbox",
    "WhatsApp Marketing": "WhatsApp Marketing",
    "Integration":        "Integration",
    "Platform & UI":      "Platform & UI",
    # Legacy values — map to current categories
    "AI Behavior":        "AI Agent",
    "Feature request":    "WhatsApp Marketing",
    "New feature":        "WhatsApp Marketing",
    "Billing & Account":  "Integration",
}
CATEGORY_BUCKETS = ["AI Agent", "Inbox", "WhatsApp Marketing", "Integration", "Platform & UI"]

VALID_CATEGORIES = ["AI Agent", "Inbox", "WhatsApp Marketing", "Integration", "Platform & UI"]
# Old categories being retired — issues with these will be reclassified by backfill
_RETIRED_CATEGORIES = {"Feature request", "New feature", "Billing & Account"}
CLASSIFY_BATCH_SIZE = 20

# ── GCAL CONFIG ──────────────────────────────────────────────────────────────

CS_TEAM_EMAILS = ["alex@konvoai.com", "aya@konvoai.com"]
GCAL_SCOPES    = ["https://www.googleapis.com/auth/calendar.readonly"]
PROJECT_DIR    = Path(__file__).resolve().parent


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


def get_formula_number(page, prop_name):
    prop = page.get("properties", {}).get(prop_name)
    if not prop or prop.get("type") != "formula":
        return None
    return prop.get("formula", {}).get("number")


def get_rollup_number(page, prop_name):
    """Return the numeric value of a rollup with type=number (e.g. count/sum)."""
    prop = page.get("properties", {}).get(prop_name)
    if not prop or prop.get("type") != "rollup":
        return None
    return prop.get("rollup", {}).get("number")


def get_url(page, prop_name):
    prop = page.get("properties", {}).get(prop_name)
    return prop.get("url") or "" if prop else ""


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


# ── GCAL HELPERS ─────────────────────────────────────────────────────────────

def _get_gcal_creds():
    """OAuth2 credentials for Google Calendar (reuses token.json)."""
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        return None, "google libs not installed"

    client_secrets = PROJECT_DIR / "client_secrets.json"
    token_file     = PROJECT_DIR / "token.json"

    if not client_secrets.exists():
        return None, "client_secrets.json not found"

    creds = (
        Credentials.from_authorized_user_file(str(token_file), GCAL_SCOPES)
        if token_file.exists() else None
    )
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow  = InstalledAppFlow.from_client_secrets_file(str(client_secrets), GCAL_SCOPES)
            creds = flow.run_local_server(port=0)
        token_file.write_text(creds.to_json())

    return creds, None


def fetch_gcal_meetings(time_min_iso, time_max_iso):
    """Fetch customer meetings for Alex & Aya. Returns [(date_str, owner), ...]."""
    print("📅 Fetching GCal customer meetings…")

    creds, err = _get_gcal_creds()
    if err:
        print(f"   GCal skipped: {err}")
        return []

    try:
        from googleapiclient.discovery import build
    except ImportError:
        print("   GCal skipped: google libs not installed")
        return []

    service = build("calendar", "v3", credentials=creds)

    cal_map, pt = {}, None
    while True:
        resp = service.calendarList().list(pageToken=pt).execute()
        for cal in resp.get("items", []):
            cal_id = cal.get("id", "").lower()
            for email in CS_TEAM_EMAILS:
                name = email.split("@")[0]
                if email.lower() in cal_id and name not in cal_map:
                    cal_map[name] = cal["id"]
                    print(f"   Calendar: {email} -> {cal['id']}")
        pt = resp.get("nextPageToken")
        if not pt:
            break

    if not cal_map:
        print("   No CS team calendars found")
        return []

    meetings = []
    for owner_name, cal_id in cal_map.items():
        pt, count = None, 0
        while True:
            resp = service.events().list(
                calendarId=cal_id,
                timeMin=time_min_iso, timeMax=time_max_iso,
                singleEvents=True, orderBy="startTime",
                pageToken=pt, maxResults=2500,
            ).execute()

            for item in resp.get("items", []):
                start    = item.get("start", {})
                raw_date = start.get("date") or start.get("dateTime", "")[:10]
                if not raw_date:
                    continue

                attendees = [a.get("email", "").lower() for a in item.get("attendees", [])]
                has_external = any(
                    not e.endswith("@konvoai.com")
                    and not e.endswith("@resource.calendar.google.com")
                    for e in attendees
                )
                if not has_external:
                    continue

                meetings.append((raw_date, owner_name))
                count += 1

            pt = resp.get("nextPageToken")
            if not pt:
                break

        print(f"   {owner_name}: {count} customer meetings")

    return meetings


def count_meetings_per_period(meetings):
    """Total customer meetings (Alex + Aya) per period."""
    return [
        sum(1 for d, _ in meetings if p["start"] <= d <= p["end"])
        for p in PERIOD_RANGES
    ]


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

DECISION FLOWCHART — follow this order:
1. Is it about a FLOW or BROADCAST? → WhatsApp Marketing
2. Is it about an EXTERNAL TOOL not working with Konvo (Shopify, Gorgias, Zendesk, \
Klaviyo, Outlook, WhatsApp/Instagram channel connection, files from external channel \
not appearing, customer data from external tools not showing)? → Integration
3. Is it about the AI's behavior, responses, or an AI-specific feature (product recs, \
order lookup, handover, OTP handled by AI, AI not responding, AI response quality, \
playground, personas)? → AI Agent
4. Everything else about Konvo's own platform UI/inbox/messaging → Inbox

VALID CATEGORIES (use these exact strings):
- AI Agent          → AI recommending wrong products, AI can't find orders, \
handover/transfer failures, AI wrong language, OTP not identified by AI, \
AI turned off/not responding, AI giving wrong info, AI-specific features (playground, personas)
- Inbox             → inbox slow/not loading, messages missing/duplicated/expired, \
notifications not updating, search bar issues, UI glitches, snooze bugs, conversation display
- WhatsApp Marketing → ALL broadcast issues (not sending, errors, variables, media), \
ALL flow issues (stopping, misfiring, wrong triggers, sent to wrong person, opt-out flows)
- Integration       → Gorgias/Zendesk issues, Shopify/WooCommerce sync, Klaviyo data sync, \
email/Outlook/Instagram/WhatsApp channel connection, files from external channels not appearing, \
customer data from external tools not showing, OTP/SMS delivery from provider

DISAMBIGUATION:
- ALL flow and broadcast bugs → WhatsApp Marketing (no exceptions, even AI opt-out flows)
- Files/attachments from external channels not appearing in Konvo → Integration (NOT Inbox)
- Customer data from Shopify/Klaviyo not showing in Konvo → Integration (NOT Inbox)
- Gorgias/Zendesk limited functionality → Integration (NOT Inbox)
- "AI" in title but platform feature broken (not AI-specific) → Inbox
- OTP from SMS provider failing → Integration; OTP not identified by AI → AI Agent

ISSUES:
{issues_json}

Return ONLY a JSON object mapping each "id" to its "category".
Example: {{"abc": "AI Agent", "def": "Integration"}}
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
        if existing and existing not in _RETIRED_CATEGORIES:
            continue  # already classified with a valid category — skip
        # Fall through: either no category or a retired one → needs reclassification
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
        "linear_url":   get_url(page, "Linear Ticket URL"),
        "severity":     get_select(page, "Severity") or "",
        "ticket_creation_date": get_date(page, "Ticket creation date"),
        "triaged_at":   get_date(page, "Triaged At"),
        "sla_triage_deadline":    get_date(page, "SLA Triage Deadline"),
        "sla_resolution_deadline": get_date(page, "SLA Resolution Deadline"),
        "triage_sla_met":     get_select(page, "Triage SLA Met") or "",
        "resolution_sla_met": get_select(page, "Resolution SLA Met") or "",
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
    bug_only_count      = []
    feature_count       = []

    for p in PERIOD_RANGES:
        bugs = [i for i in issues_by_period[p["label"]] if i["is_in_scope"]]
        bug_volume.append(len(bugs))
        bug_source_intercom.append(sum(1 for i in bugs if i["source"] == "Intercom"))
        bug_source_meetings.append(sum(1 for i in bugs if i["source"] == "Meeting"))
        bug_only_count.append(sum(1 for i in bugs if i["is_bug"]))
        feature_count.append(sum(1 for i in bugs if i["is_feature"]))

    p1 = bug_volume[0] or 1
    target_line = [round(p1 * 0.85 ** i, 1) for i in range(len(PERIOD_RANGES))]

    return {
        "bug_volume":          bug_volume,
        "target_line":         target_line,
        "bug_source_intercom": bug_source_intercom,
        "bug_source_meetings": bug_source_meetings,
        "bug_only_count":      bug_only_count,
        "feature_count":       feature_count,
    }


# ── SLIDE 2: BUG CATEGORIZATION ───────────────────────────────────────────────

# Keyword theme clusters for the Key Takeaways card on Slide 2.
# Each entry: (list-of-keywords-in-title-lowercase, display-label)
# Keywords are matched against the issue title (lowercased). First match wins.
_THEMES_BY_CATEGORY = {
    "AI Behavior": [
        (["wrong product", "wrong variation", "recommend",
          "product rec", "incorrect product"],                 "AI product recommendations wrong"),
        (["discount", "outdated", "incorrect info",
          "didn't ident", "wrong info", "wrong answer"],       "AI using wrong or outdated information"),
        (["confirm", "didn't execute", "failed to update",
          "didn't complete"],                                  "AI confirming actions not completed"),
        (["duplicat", "after transfer", "timing"],             "AI message timing & duplication"),
        (["react", "reopen", "conversation state"],            "Incorrect conversation state changes"),
    ],
    "Platform & UI": [
        (["flow", "flows"],                                    "Flows stopping mid-execution"),
        (["inbox", "loading", "slow", "performance"],          "Inbox & performance issues"),
        (["handover", "transfer", "access", "notification"],   "Handover & access failures"),
        (["onboarding", "stuck", "setup"],                     "Onboarding & setup issues"),
    ],
    "WhatsApp Marketing": [
        (["broadcast", "didn't send", "not send",
          "failed to send", "shows finished"],                 "Broadcast send failures"),
        (["segment", "contact list", "audience"],              "Segment & audience issues"),
        (["utm", "tracking", "link"],                          "UTM & link tracking issues"),
        (["schedule", "scheduled", "campaign send"],           "Campaign scheduling issues"),
    ],
    "Integration": [
        (["outlook", "microsoft", "email integr"],             "Email/Outlook connection issues"),
        (["gorgias", "livechat", "live chat"],                 "Helpdesk channel issues"),
        (["shopify", "sync", "product sync"],                  "Shopify/product sync issues"),
        (["instagram", "social", "telegram"],                  "Social channel issues"),
    ],
}
_CAT_COLORS = {
    "AI Behavior":        "#F87171",   # red
    "Platform & UI":      "#34D399",   # green
    "WhatsApp Marketing": "#25D366",   # WhatsApp green
    "Integration":        "#A78BFA",   # purple
}


def _cluster_themes(issues, category):
    """Bucket a list of issues into named themes via keyword matching on title.

    Returns list of dicts: {label, total, resolved, open}
    """
    theme_rules = _THEMES_BY_CATEGORY.get(category, [])
    buckets          = defaultdict(int)
    buckets_resolved = defaultdict(int)
    for issue in issues:
        title_lc = issue.get("title", "").lower()
        matched = False
        for keywords, label in theme_rules:
            if any(kw in title_lc for kw in keywords):
                buckets[label] += 1
                if issue.get("status") == "Resolved":
                    buckets_resolved[label] += 1
                matched = True
                break
        if not matched:
            buckets["_other"] += 1
            if issue.get("status") == "Resolved":
                buckets_resolved["_other"] += 1

    # Build ordered list: defined themes first (only those with ≥1), then "Other"
    result = []
    for _, label in theme_rules:
        total = buckets.get(label, 0)
        if total > 0:
            resolved = buckets_resolved.get(label, 0)
            result.append({"label": label, "total": total, "resolved": resolved, "open": total - resolved})
    other_n = buckets.get("_other", 0)
    if other_n > 0:
        other_r = buckets_resolved.get("_other", 0)
        result.append({"label": "Other", "total": other_n, "resolved": other_r, "open": other_n - other_r})
    return result


def _review_period_label(issues_by_period):
    """Most recent period with ≥5 in-scope issues (the period being reviewed in the meeting).

    On day 1 of a new period the new period has 0 issues, so this walks back to the
    just-completed period — which is exactly what the weekly meeting is reviewing.
    """
    labels  = [p["label"] for p in PERIOD_RANGES]
    cur_idx = labels.index(CURRENT_PERIOD) if CURRENT_PERIOD in labels else 0
    for i in range(cur_idx, -1, -1):
        if len([x for x in issues_by_period.get(labels[i], []) if x["is_in_scope"]]) >= 5:
            return labels[i]
    return labels[0]


def build_slide2_takeaways(issues_by_period):
    """Return key_takeaways_s2: a list of category blocks for the review period."""
    review_period = _review_period_label(issues_by_period)
    cur_issues = [i for i in issues_by_period.get(review_period, []) if i["is_in_scope"]]
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

def build_slide2_commentary(issues_by_period):
    """Use Claude to generate a short commentary on the review period's issues."""
    review_period = _review_period_label(issues_by_period)
    cur_issues = [i for i in issues_by_period.get(review_period, []) if i["is_in_scope"]]
    by_cat = defaultdict(list)
    for i in cur_issues:
        bucket = CATEGORY_DISPLAY.get(i["category"], "Uncategorized")
        by_cat[bucket].append(i)

    # Build a summary of titles per category for Claude
    cat_summaries = []
    for cat in CATEGORY_BUCKETS:
        issues = by_cat.get(cat, [])
        if not issues:
            continue
        bugs = [i for i in issues if i["is_bug"]]
        features = [i for i in issues if i["is_feature"]]
        titles = [i["title"] for i in issues]
        cat_summaries.append(f"{cat} ({len(issues)} issues, {len(bugs)} bugs, {len(features)} feature requests):\n" +
                            "\n".join(f"  - {t}" for t in titles))

    prompt = f"""\
You are writing a short commentary for a weekly CS meeting slide about issues reported in {review_period}.

Here are the issues grouped by category:

{chr(10).join(cat_summaries)}

Write a JSON object with one key per category (use the exact category names above).
Each value should be an array of 2-3 short bullet strings (one line each, no bullet character).

Format each bullet as: "**Topic** — sub-issue 1 (N reports), sub-issue 2 (N reports)"
Start each bullet with a bolded topic wrapped in double asterisks (**like this**).
Group related failures under one topic. Include report counts where possible.

Example: ["**Flows** — stop mid-execution (5 reports), don't trigger on keywords (3 reports)", "**Order data** — AI can't find orders (4 reports), sends wrong prices (2 reports)"]

Focus on what's broken. Plain language, no jargon. Be specific with counts.

Return ONLY the JSON object, no other text."""

    try:
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
            timeout=30,
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
        return json.loads(text)
    except Exception as e:
        print(f"   ⚠️ Commentary generation failed: {e}")
        return {}


def build_slide2(issues_by_period):
    bug_types = []

    for p in PERIOD_RANGES:
        bugs   = [i for i in issues_by_period[p["label"]] if i["is_in_scope"]]
        counts = defaultdict(int)
        for bug in bugs:
            bucket = CATEGORY_DISPLAY.get(bug["category"], "Uncategorized")
            counts[bucket] += 1
        bug_types.append([counts.get(b, 0) for b in CATEGORY_BUCKETS])

    # Per-category bug/feature split for the current (review) period
    review_period = _review_period_label(issues_by_period)
    cur_issues = [i for i in issues_by_period.get(review_period, []) if i["is_in_scope"]]
    cat_split = []
    for cat in CATEGORY_BUCKETS:
        cat_issues = [i for i in cur_issues
                      if CATEGORY_DISPLAY.get(i["category"], "Uncategorized") == cat]
        cat_split.append({
            "category": cat,
            "total":    len(cat_issues),
            "bugs":     sum(1 for i in cat_issues if i["is_bug"]),
            "features": sum(1 for i in cat_issues if i["is_feature"]),
        })

    return {
        "bug_types":      bug_types,
        "bug_type_names": CATEGORY_BUCKETS,
        "category_split": cat_split,
    }


# ── SNAPSHOT HELPERS ──────────────────────────────────────────────────────────

def load_snapshots() -> dict:
    if os.path.exists(SNAPSHOTS_FILE):
        with open(SNAPSHOTS_FILE) as f:
            raw = json.load(f)
        # Auto-migrate old label keys (P1/P2/P3) to date-based keys ("2026-02-16")
        label_to_start = {p["label"]: p["start"] for p in PERIOD_RANGES}
        migrated = {}
        for k, v in raw.items():
            new_key = label_to_start.get(k, k)   # remap P1→"2026-02-16", else keep
            migrated[new_key] = v
        return migrated
    return {}


def save_snapshots(snapshots: dict):
    with open(SNAPSHOTS_FILE, "w") as f:
        json.dump(snapshots, f, indent=2)


# ── Linear priority helper ────────────────────────────────────────────────────

LINEAR_GQL = "https://api.linear.app/graphql"
PRIORITY_NAMES = {1: "Urgent", 2: "High", 3: "Medium", 4: "Low", 0: "None"}


def _extract_linear_identifier(url: str) -> str:
    """Extract e.g. 'ENG-124' from a Linear issue URL."""
    m = re.search(r"/issue/([A-Z]+-\d+)", url)
    return m.group(1) if m else ""


def fetch_linear_priorities(urls: list[str]) -> dict[str, str]:
    """
    Given a list of Linear URLs, batch-query for priority.
    Returns { url: "Urgent" | "High" | "Medium" | "Low" | "None" }
    """
    if not LINEAR_TOKEN or not urls:
        return {}

    headers = {"Authorization": LINEAR_TOKEN, "Content-Type": "application/json"}
    url_to_ident = {}
    by_team: dict[str, list[int]] = {}
    for u in urls:
        ident = _extract_linear_identifier(u)
        if not ident:
            continue
        url_to_ident[u] = ident
        m = re.match(r"^([A-Z]+)-(\d+)$", ident)
        if m:
            by_team.setdefault(m.group(1), []).append(int(m.group(2)))

    query = """
    query($teamKey: String!, $numbers: [Float!]!) {
      issues(filter: {
        team: { key: { eq: $teamKey } },
        number: { in: $numbers }
      }) {
        nodes { identifier priority }
      }
    }
    """

    ident_to_priority = {}
    for team_key, numbers in by_team.items():
        resp = requests.post(LINEAR_GQL, headers=headers,
                             json={"query": query, "variables": {"teamKey": team_key, "numbers": numbers}})
        if resp.status_code != 200:
            print(f"  [WARN] Linear priority query failed for {team_key}: {resp.status_code}")
            continue
        for node in resp.json().get("data", {}).get("issues", {}).get("nodes", []):
            ident_to_priority[node["identifier"]] = PRIORITY_NAMES.get(node.get("priority", 0), "None")

    return {u: ident_to_priority.get(ident, "None") for u, ident in url_to_ident.items()}


def fetch_linear_issue_details(urls: list[str]) -> dict[str, dict]:
    """
    Given Linear URLs, batch-query for priority + state type.
    Returns { url: {"priority": "Urgent", "state_type": "started"} }
    """
    if not LINEAR_TOKEN or not urls:
        return {}

    headers = {"Authorization": LINEAR_TOKEN, "Content-Type": "application/json"}
    url_to_ident = {}
    by_team: dict[str, list[int]] = {}
    for u in urls:
        ident = _extract_linear_identifier(u)
        if not ident:
            continue
        url_to_ident[u] = ident
        m = re.match(r"^([A-Z]+)-(\d+)$", ident)
        if m:
            by_team.setdefault(m.group(1), []).append(int(m.group(2)))

    query = """
    query($teamKey: String!, $numbers: [Float!]!) {
      issues(filter: {
        team: { key: { eq: $teamKey } },
        number: { in: $numbers }
      }) {
        nodes { identifier priority state { name type } }
      }
    }
    """

    ident_to_detail = {}
    for team_key, numbers in by_team.items():
        resp = requests.post(LINEAR_GQL, headers=headers,
                             json={"query": query, "variables": {"teamKey": team_key, "numbers": numbers}})
        if resp.status_code != 200:
            print(f"  [WARN] Linear detail query failed for {team_key}: {resp.status_code}")
            continue
        for node in resp.json().get("data", {}).get("issues", {}).get("nodes", []):
            state = node.get("state") or {}
            ident_to_detail[node["identifier"]] = {
                "priority": PRIORITY_NAMES.get(node.get("priority", 0), "None"),
                "state_type": state.get("type", ""),
                "state_name": state.get("name", ""),
            }

    return {u: ident_to_detail.get(ident, {"priority": "None", "state_type": "", "state_name": ""})
            for u, ident in url_to_ident.items()}


# ── SLIDE 3: RESOLUTION STATUS ────────────────────────────────────────────────

def build_slide3(issues_by_period, snapshots, all_issues_flat=None):
    """
    snapshots is mutated in-place when a new snapshot window becomes due.
    Caller must call save_snapshots() after this returns.
    all_issues_flat: ALL parsed issues (not period-filtered), used for SLA metrics.
    """
    resolution_by_period = []
    resolution_rates     = []
    open_bugs_by_period  = []   # collect open bugs per period for priority lookup

    for p in PERIOD_RANGES:
        issues  = [i for i in issues_by_period[p["label"]] if i["is_in_scope"] and i["is_bug"]]
        total_n = len(issues)

        open_n        = sum(1 for i in issues if i["status"] == "Open")
        in_progress_n = sum(1 for i in issues if i["status"] == "In Progress")
        resolved_n    = sum(1 for i in issues if i["status"] == "Resolved")
        deprio_n      = sum(1 for i in issues if i["status"] == "Deprioritized")

        open_bugs_by_period.append([i for i in issues if i["status"] == "Open"])

        resolution_by_period.append({
            "Open":           open_n,
            "In Progress":    in_progress_n,
            "Resolved":       resolved_n,
            "Deprioritized":  deprio_n,
        })

        # Resolution rate snapshots: frozen once measurement window closes
        rates = [None, None, None, None]   # 7d, 14d, 28d, current

        if total_n > 0:
            period_start = date.fromisoformat(p["start"])
            period_end   = date.fromisoformat(p["end"])
            snap       = snapshots.setdefault(p["start"], {"7d": None, "14d": None, "28d": None})

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

    # Fetch Linear priorities for all open bugs across all periods
    all_open_urls = []
    for bugs in open_bugs_by_period:
        all_open_urls.extend(i["linear_url"] for i in bugs if i.get("linear_url"))
    print(f"   Fetching Linear priorities for {len(all_open_urls)} open bugs…")
    url_to_priority = fetch_linear_priorities(list(set(all_open_urls)))

    open_by_priority = []
    for bugs in open_bugs_by_period:
        counts = {"Urgent": 0, "High": 0, "Medium": 0, "Low": 0}
        for b in bugs:
            prio = url_to_priority.get(b.get("linear_url", ""), "None")
            if prio in counts:
                counts[prio] += 1
        open_by_priority.append(counts)

    # ── SLA metrics — query Linear BUG Backlog Push project directly ─────
    cur_period = PERIOD_RANGES[-1]
    cur_start = cur_period["start"]
    cur_end   = cur_period["end"]
    BUG_PROJECT_ID = "7aa31126-9b3e-4e32-bc09-8e13e2e49721"
    PAST_TRIAGE_TYPES = {"backlog", "unstarted", "started"}
    PRIO_DISPLAY = {"Urgent": "Urgent", "High": "High", "Medium": "Medium", "Low": "Medium", "None": "Medium"}

    print(f"   Fetching BUG Backlog Push project issues from Linear (SLA)…")
    sla_query = """
    query($projectId: String!) {
      project(id: $projectId) {
        issues(first: 250, filter: { state: { type: { nin: ["completed", "cancelled"] } } }) {
          nodes { identifier priority createdAt state { name type } }
        }
      }
    }
    """
    sla_headers = {"Authorization": LINEAR_TOKEN, "Content-Type": "application/json"}
    sla_resp = requests.post(LINEAR_GQL, headers=sla_headers,
                             json={"query": sla_query, "variables": {"projectId": BUG_PROJECT_ID}})
    project_issues = []
    if sla_resp.status_code == 200:
        project_issues = (sla_resp.json().get("data") or {}).get("project", {}).get("issues", {}).get("nodes", [])
    print(f"   → {len(project_issues)} active issues in project")

    # TRIAGE: tickets created this week, split by priority
    triage_created_this_week = [
        n for n in project_issues
        if n.get("createdAt", "")[:10] >= cur_start
        and n.get("createdAt", "")[:10] <= cur_end
    ]

    sla_triage = {d: {"total": 0, "breached": 0} for d in ["Urgent", "High", "Medium"]}
    for n in triage_created_this_week:
        prio_name = PRIORITY_NAMES.get(n.get("priority", 0), "None")
        display = PRIO_DISPLAY.get(prio_name, "Medium")
        sla_triage[display]["total"] += 1
        # Breached = still in triage past 1 business day from creation
        if (n["state"]["type"] == "triage"
                and n.get("createdAt", "")[:10] < str(TODAY - timedelta(days=1))):
            sla_triage[display]["breached"] += 1

    triage_total = sum(v["total"] for v in sla_triage.values())
    triage_breached = sum(v["breached"] for v in sla_triage.values())

    # RESOLUTION: all tickets past triage, grouped by priority
    sla_resolution = {d: {"total": 0, "breached": 0} for d in ["Urgent", "High", "Medium"]}

    past_triage_issues = [n for n in project_issues if n["state"]["type"] in PAST_TRIAGE_TYPES]
    for n in past_triage_issues:
        prio_name = PRIORITY_NAMES.get(n.get("priority", 0), "None")
        display = PRIO_DISPLAY.get(prio_name, "Medium")
        sla_resolution[display]["total"] += 1
        # SLA breach check: cross-reference with Notion if possible (via identifier)
        # For now, deadlines are set from April 1 — no breaches yet

    print(f"   → Triage: {triage_total} this week ({triage_breached} breached)")
    print(f"   → Resolution: {len(past_triage_issues)} past triage")

    sla_data = {
        "triage": sla_triage,
        "triage_total": triage_total,
        "triage_breached": triage_breached,
        "resolution": sla_resolution,
    }

    return {
        "resolution_by_period": resolution_by_period,
        "resolution_rates":     resolution_rates,
        "open_by_priority":     open_by_priority,
        "sla_data":             sla_data,
    }


# ── SLIDE 5: CHURNS (Stripe-sourced weekly trend) ────────────────────────────

def _fetch_stripe_canceled_at(stripe_customer_id):
    """Fetch the canceled_at timestamp from Stripe for a given customer.

    Returns the most recent canceled_at as an ISO date string, or None.
    """
    if not STRIPE_KEY or not stripe_customer_id:
        return None
    try:
        r = requests.get(
            "https://api.stripe.com/v1/subscriptions",
            params={"customer": stripe_customer_id, "status": "all", "limit": 10},
            auth=(STRIPE_KEY, ""),
            timeout=15,
        )
        r.raise_for_status()
        subs = r.json().get("data", [])
        best = None
        for sub in subs:
            cat = sub.get("canceled_at")
            if cat:
                d = datetime.utcfromtimestamp(cat).strftime("%Y-%m-%d")
                if best is None or d > best:
                    best = d
        return best
    except Exception as e:
        print(f"   ⚠️  Stripe lookup failed for {stripe_customer_id}: {e}")
        return None


def build_slide5(mct_pages):
    """Pull churn data from MCT + Stripe with weekly trend bucketing.

    For each Churning/Canceled customer, fetches the exact cancel-click date
    from Stripe (canceled_at on the subscription), then buckets into W1/W2/W3.
    """
    def _extract_customer_detail(page):
        return {
            "name":               get_title(page, "🏢 Company Name") or "Unknown",
            "mrr_raw":            get_number(page, "💰 MRR") or 0,
            "cancel_date":        get_date(page, "📅 Cancel Date") or "",
            "churning_since":     get_date(page, "📅 Churning Since") or "",
            "churn_date":         get_date(page, "😢 Churn Date") or "",
            "reason":             get_select(page, "🔁 Churn Reason") or "Unknown",
            "days_since_contact": get_formula_number(page, "📞 Days Since Last Contact"),
            "cs_sentiment":       get_select(page, "🧠 CS Sentiment") or "",
            "ai_resolution_rate": get_number(page, "🤖 AI Resolution Rate"),
            "open_issues":        int(v) if (v := get_rollup_number(page, "⚠️ # of Open Issues")) is not None else None,
            "cs_owner":           get_select(page, "⭐ CS Owner") or "",
        }

    # ── Collect all Churning + Canceled customers from MCT ──
    churn_pages = [p for p in mct_pages
                   if get_select(p, "💰 Billing Status") in ("Churning", "Canceled")]
    print(f"   {len(churn_pages)} churning/canceled customers found in MCT")

    # ── Fetch exact cancel-click date from Stripe for each ──
    churn_entries = []
    if STRIPE_KEY:
        print("   Fetching cancel dates from Stripe…")
        for page in churn_pages:
            stripe_id = get_rich_text(page, "🔗 Stripe Customer ID").strip()
            stripe_canceled_at = _fetch_stripe_canceled_at(stripe_id)

            row = _extract_customer_detail(page)
            row["billing_status"] = get_select(page, "💰 Billing Status")
            row["type"] = "canceled" if row["billing_status"] == "Canceled" else "churning"
            row["stripe_canceled_at"] = stripe_canceled_at or ""
            # Stripe canceled_at is the canonical cancel-click date;
            # fall back to MCT "Churning Since" if Stripe unavailable
            row["cancel_click_date"] = stripe_canceled_at or row["churning_since"]
            churn_entries.append(row)
            time.sleep(0.1)  # gentle rate limit
        stripe_hits = sum(1 for e in churn_entries if e["stripe_canceled_at"])
        print(f"   → {stripe_hits} had Stripe canceled_at dates")
    else:
        print("   ⚠️  STRIPE_KEY not set — using MCT 'Churning Since' as fallback")
        for page in churn_pages:
            row = _extract_customer_detail(page)
            row["billing_status"] = get_select(page, "💰 Billing Status")
            row["type"] = "canceled" if row["billing_status"] == "Canceled" else "churning"
            row["stripe_canceled_at"] = ""
            row["cancel_click_date"] = row["churning_since"]
            churn_entries.append(row)

    # ── Bucket into periods by cancel-click date ──
    churn_volume = []
    churn_canceled_count = []
    churn_churning_count = []
    churn_mrr = []
    churn_details = []

    for p in PERIOD_RANGES:
        period_entries = [e for e in churn_entries
                          if date_in_period(e["cancel_click_date"], p)]
        period_entries.sort(key=lambda x: -x["mrr_raw"])

        churn_volume.append(len(period_entries))
        churn_canceled_count.append(sum(1 for e in period_entries if e["type"] == "canceled"))
        churn_churning_count.append(sum(1 for e in period_entries if e["type"] == "churning"))
        churn_mrr.append(sum(e["mrr_raw"] for e in period_entries))
        churn_details.append(period_entries)

    total_in_range = sum(churn_volume)
    buckets_str = " / ".join(
        f"{p['label']}={n}" for p, n in zip(PERIOD_RANGES, churn_volume)
    )
    print(f"   Churn bucketing: {buckets_str} (total {total_in_range})")

    # ── churning_pipeline + churn_combined (for detail table) ──
    churning_pipeline = sorted(
        [e for e in churn_entries if e["type"] == "churning"],
        key=lambda x: x["cancel_date"],
    )

    # Only include current period's churns in the detail table
    current_period_entries = churn_details[-1] if churn_details else []
    churn_combined = sorted(
        current_period_entries,
        key=lambda x: (-1 if x["type"] == "canceled" else 0, -x["mrr_raw"]),
    )

    return {
        "churn_volume":          churn_volume,
        "churn_canceled_count":  churn_canceled_count,
        "churn_churning_count":  churn_churning_count,
        "churn_mrr":             churn_mrr,
        "churn_details":         churn_details,
        "churning_pipeline":     churning_pipeline,
        "churn_combined":        churn_combined,
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

    review_period = _review_period_label(issues_by_period)
    print(f"   Review period: {review_period} (CURRENT_PERIOD={CURRENT_PERIOD})")

    # Fetch GCal customer meetings for meeting-count bubbles on Slide 1
    gcal_start = PERIOD_RANGES[0]["start"] + "T00:00:00Z"
    gcal_end   = (date.fromisoformat(PERIOD_RANGES[-1]["end"]) + timedelta(days=1)).isoformat() + "T00:00:00Z"
    gcal_meetings = fetch_gcal_meetings(gcal_start, gcal_end)
    meetings_per_period = count_meetings_per_period(gcal_meetings)

    print("🔨 Building slides…")
    s1 = build_slide1(issues_by_period)
    s2 = build_slide2(issues_by_period)
    s3 = build_slide3(issues_by_period, snapshots, all_issues)  # may mutate snapshots
    save_snapshots(snapshots)                         # persist any newly-computed snapshots
    s5 = build_slide5(mct_pages)
    takeaways = build_slide2_takeaways(issues_by_period)
    commentary = build_slide2_commentary(issues_by_period)

    return {
        "fetched_at":       datetime.utcnow().isoformat() + "Z",
        "periods":          [p["display"] for p in PERIOD_RANGES],
        "period_ranges":    PERIOD_RANGES,
        "current_period":   CURRENT_PERIOD,
        "review_period":    review_period,
        **s1, **s2, **s3, **s5,
        "meetings_per_period": meetings_per_period,
        "key_takeaways_s2": takeaways,
        "slide2_commentary": commentary,
    }


if __name__ == "__main__":
    no_classify = "--no-classify" in sys.argv
    data = build_report_data(no_classify=no_classify)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Written → {OUTPUT_FILE}")
    print("   Run `python3 generate_meeting_report.py` to render the HTML.")
