"""
fetch_checkin_data.py

Fetches customer-specific issues from Notion for a CS check-in meeting,
plus product metrics from ClickHouse, response time from Intercom,
and channel connection status from the MCT.

    python3 meetings/checkin/fetch_checkin_data.py "Company Name"

Writes checkin_data.json in this directory, which generate_checkin.py reads.

Also importable: generate_all_checkins.py uses fetch_all_mct(), fetch_all_issues(),
and parse_customer_issues() directly.
"""

import json
import os
import re
import sys
import time
import statistics as stats_mod
import requests
from datetime import datetime, timezone, timedelta

# Add parent dir so we can import creds.py
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
import creds

MCT_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"
ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "checkin_data.json")

# Hours-saved estimation: avg minutes saved per AI-resolved session
MINUTES_PER_AI_SESSION = 3

# Free email providers to exclude from domain matching
GENERIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "aol.com", "protonmail.com", "live.com", "msn.com", "me.com",
    "googlemail.com", "ymail.com", "mail.com",
}

# Status sort priority: Open first, then In Progress, Resolved, Deprioritized
STATUS_ORDER = {"Open": 0, "In Progress": 1, "Resolved": 2, "Deprioritized": 3}


# ── TOKEN (lazy — safe to import without NOTION_TOKEN set) ────────────────────

def _get_token():
    token = os.environ.get("NOTION_TOKEN", "")
    if token:
        return token
    creds_path = os.path.join(SCRIPT_DIR, "..", "..", "Credentials.md")
    if os.path.exists(creds_path):
        with open(creds_path) as f:
            for line in f:
                if "ntn_" in line:
                    for word in line.split():
                        if word.startswith("ntn_"):
                            return word.strip()
    raise RuntimeError("NOTION_TOKEN not set and not found in Credentials.md")


def _headers_v1():
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def _headers_v2():
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Notion-Version": "2025-09-03",
        "Content-Type": "application/json",
    }


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


def get_url(page, prop_name):
    prop = page.get("properties", {}).get(prop_name)
    return prop.get("url") or "" if prop else ""


def get_relation_ids(page, prop_name):
    prop = page.get("properties", {}).get(prop_name)
    if not prop:
        return []
    return [r["id"] for r in prop.get("relation", [])]


# ── FETCH ────────────────────────────────────────────────────────────────────

def fetch_all_mct():
    print("🏢 Fetching Master Customer Table…")
    url   = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"
    pages = notion_query_all(url, _headers_v2(), {})
    print(f"   → {len(pages)} customer pages")
    return pages


def fetch_all_issues():
    print("📋 Fetching Issues Table…")
    url   = f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query"
    pages = notion_query_all(url, _headers_v1(), {})
    print(f"   → {len(pages)} issue pages")
    return pages


# ── MATCH CUSTOMER ───────────────────────────────────────────────────────────

def match_customer(mct_pages, query):
    """Case-insensitive substring match. Fail with suggestions if ambiguous."""
    query_lower = query.lower().strip()
    matches = []
    for page in mct_pages:
        name = get_title(page, "🏢 Company Name")
        if not name:
            continue
        if query_lower == name.lower():
            # Exact match — return immediately
            return page, name
        if query_lower in name.lower():
            matches.append((page, name))

    if len(matches) == 1:
        return matches[0]
    elif len(matches) == 0:
        print(f"❌ No customer found matching '{query}'")
        # Show some suggestions
        all_names = sorted(
            [get_title(p, "🏢 Company Name") for p in mct_pages
             if get_title(p, "🏢 Company Name")],
        )
        # Show names containing any word from the query
        words = query_lower.split()
        suggestions = [n for n in all_names if any(w in n.lower() for w in words)][:10]
        if suggestions:
            print("   Did you mean one of these?")
            for s in suggestions:
                print(f"   • {s}")
        sys.exit(1)
    else:
        print(f"❌ Ambiguous match for '{query}' — {len(matches)} customers found:")
        for _, name in matches:
            print(f"   • {name}")
        print("   Please use a more specific name.")
        sys.exit(1)


# ── PARSE ISSUES ─────────────────────────────────────────────────────────────

def parse_customer_issues(issue_pages, customer_page_id):
    """Filter and parse issues linked to this customer."""
    issues = []
    for page in issue_pages:
        customer_ids = get_relation_ids(page, "Customer")
        if customer_page_id not in customer_ids:
            continue

        status = get_select(page, "Status") or ""

        created_at = get_date(page, "Created At")
        if not created_at:
            created_at = page.get("created_time", "")[:10]

        issues.append({
            "title":      get_title(page, "Issue Title"),
            "category":   get_select(page, "Category") or "",
            "status":     status,
            "issue_type": get_select(page, "Issue Type") or "",
            "source":     get_select(page, "Source") or "",
            "created_at": created_at,
            "resolved_at": get_date(page, "Resolved At"),
            "informed":   get_checkbox(page, "✅ Customer Informed?"),
            "linear_url": get_url(page, "Linear Ticket URL"),
            "summary":    get_rich_text(page, "Summary"),
        })

    # Sort: Open → In Progress → Resolved → Deprioritized, then by Created At desc
    issues.sort(key=lambda i: (
        STATUS_ORDER.get(i["status"], 99),
        -(datetime.strptime(i["created_at"], "%Y-%m-%d").timestamp() if i["created_at"] else 0),
    ))

    return issues


# ── CLICKHOUSE METRICS ───────────────────────────────────────────────────────

def _ch_creds():
    """Get ClickHouse host / user / password from env vars (CI) or Credentials.md (local)."""
    host = os.environ.get("CLICKHOUSE_HOST", "").strip()
    user = os.environ.get("CLICKHOUSE_USER", "").strip()
    password = os.environ.get("CLICKHOUSE_PASSWORD", "").strip()

    if host and user and password:
        if not host.endswith(":8443"):
            host += ":8443"
        return host, user, password

    # Fallback: parse from Credentials.md
    creds_path = os.path.join(SCRIPT_DIR, "..", "..", "Credentials.md")
    if not os.path.exists(creds_path):
        return "", "", ""
    raw = open(creds_path).read()

    # Host
    m = re.search(r"\*\*Host:\*\*\s*`([^`]+)`", raw)
    if m:
        host = m.group(1).rstrip("/")
        if not host.endswith(":8443"):
            host += ":8443"
    # Key ID (username)
    m = re.search(r"Key ID \(username\).*?```\s*(\S+)\s*```", raw, re.DOTALL)
    if m:
        user = m.group(1)
    # Key Secret (password)
    m = re.search(r"Key Secret \(password\).*?```\s*(.+?)\s*```", raw, re.DOTALL)
    if m:
        password = m.group(1)

    return host, user, password


def fetch_clickhouse_metrics(stripe_customer_id):
    """Fetch 8 weeks of product metrics from ClickHouse for one customer."""
    if not stripe_customer_id:
        print("   ⚠️  No Stripe Customer ID — skipping ClickHouse metrics")
        return None

    host, user, password = _ch_creds()
    sql = f"""
        SELECT
            toMonday(toDate(created_at))                   AS week_start,
            argMax(ai_sessions_count, created_at)          AS ai_sessions_count,
            argMax(ai_sessions_resolved, created_at)       AS ai_sessions_resolved
        FROM operator.public_workspace_report_snapshot
        WHERE stripe_customer_id = '{stripe_customer_id}'
          AND toDate(created_at) >= toMonday(today()) - 189
        GROUP BY week_start
        ORDER BY week_start
        FORMAT JSON
    """

    print(f"📊 Fetching ClickHouse metrics for {stripe_customer_id}…")
    try:
        r = requests.get(
            host, params={"query": sql},
            auth=(user, password), timeout=30, verify=True,
        )
        r.raise_for_status()
        rows = r.json().get("data", [])
    except Exception as e:
        print(f"   ⚠️  ClickHouse query failed: {e}")
        return None

    if not rows:
        print("   ⚠️  No ClickHouse data found for this customer")
        return None

    result = _rows_to_metrics(rows)
    if result is None:
        print("   ⚠️  Not enough ClickHouse data to compute bi-weekly deltas")
        return None

    print(f"   → {len(result['labels'])} bi-weekly periods")
    return result


def _rows_to_metrics(rows):
    """Convert cumulative ClickHouse weekly snapshots into bi-weekly delta metrics.

    The snapshot table stores running totals grouped by week. We select every-other
    week as anchor points (aligned to the latest week) and compute deltas between
    them, producing one bar per 2-week period.
    """
    if len(rows) < 3:
        return None

    # Parse cumulative values from all weekly rows
    cum_count, cum_resolved, week_starts = [], [], []
    for row in rows:
        cum_count.append(float(row.get("ai_sessions_count") or 0))
        cum_resolved.append(float(row.get("ai_sessions_resolved") or 0))
        week_starts.append(row.get("week_start", ""))

    # Build bi-weekly anchor points, aligned from the end so the latest
    # period always covers a full 2-week span
    anchors = []
    i = len(rows) - 1
    while i >= 0:
        anchors.append(i)
        i -= 2
    anchors.reverse()
    # Always include the earliest data point so we don't waste a period
    if anchors[0] != 0:
        anchors.insert(0, 0)

    if len(anchors) < 2:
        return None

    # Compute bi-weekly deltas between consecutive anchor points
    labels, ai_resolution, sessions_total = [], [], []
    sessions_ai, sessions_human, hours_saved = [], [], []

    for j in range(1, len(anchors)):
        prev_idx = anchors[j - 1]
        curr_idx = anchors[j]

        # Label: date range of this bi-weekly period (e.g. "Feb 10–23")
        try:
            start = datetime.strptime(week_starts[prev_idx], "%Y-%m-%d") + timedelta(days=7)
            end = datetime.strptime(week_starts[curr_idx], "%Y-%m-%d") + timedelta(days=6)
            s_str = start.strftime("%b %d").replace(" 0", " ")
            e_str = start.strftime("%b") == end.strftime("%b") \
                and str(end.day) \
                or end.strftime("%b %d").replace(" 0", " ")
            label = f"{s_str}–{e_str}"
        except ValueError:
            label = week_starts[curr_idx]
        labels.append(label)

        d_count = max(0, int(cum_count[curr_idx] - cum_count[prev_idx]))
        d_resolved = max(0, int(cum_resolved[curr_idx] - cum_resolved[prev_idx]))
        d_human = max(0, d_count - d_resolved)

        sessions_total.append(d_count)
        sessions_ai.append(d_resolved)
        sessions_human.append(d_human)
        ai_resolution.append(round(d_resolved / d_count, 4) if d_count > 0 else 0.0)
        hours_saved.append(round(d_resolved * MINUTES_PER_AI_SESSION / 60, 1))

    return {
        "labels": labels,
        "ai_resolution_rate": ai_resolution,
        "sessions_total": sessions_total,
        "sessions_ai": sessions_ai,
        "sessions_human": sessions_human,
        "hours_saved": hours_saved,
    }


def fetch_all_clickhouse_metrics():
    """Fetch 8 weeks of metrics for ALL customers in one bulk query.
    Returns {stripe_customer_id: metrics_dict}."""
    host, user, password = _ch_creds()
    if not host or not user:
        print("   ⚠️  ClickHouse credentials not available — skipping metrics")
        return {}

    sql = """
        SELECT
            stripe_customer_id,
            toMonday(toDate(created_at))                   AS week_start,
            argMax(ai_sessions_count, created_at)          AS ai_sessions_count,
            argMax(ai_sessions_resolved, created_at)       AS ai_sessions_resolved
        FROM operator.public_workspace_report_snapshot
        WHERE toDate(created_at) >= toMonday(today()) - 189
          AND stripe_customer_id != ''
        GROUP BY stripe_customer_id, week_start
        ORDER BY stripe_customer_id, week_start
        FORMAT JSON
    """

    print("📊 Fetching ClickHouse metrics for all customers (bulk)…")
    try:
        r = requests.get(
            host, params={"query": sql},
            auth=(user, password), timeout=60, verify=True,
        )
        r.raise_for_status()
        rows = r.json().get("data", [])
    except Exception as e:
        print(f"   ⚠️  ClickHouse bulk query failed: {e}")
        return {}

    # Group rows by stripe_customer_id
    by_customer = {}
    for row in rows:
        sid = row.get("stripe_customer_id", "")
        if sid:
            by_customer.setdefault(sid, []).append(row)

    # Convert each customer's rows to a metrics dict
    result = {}
    for sid, cust_rows in by_customer.items():
        m = _rows_to_metrics(cust_rows)
        if m is not None:
            result[sid] = m

    print(f"   → {len(rows)} rows for {len(result)} customers")
    return result


# ── INTERCOM RESPONSE TIME ──────────────────────────────────────────────────

def _intercom_headers():
    return {
        "Authorization":    f"Bearer {creds.get('INTERCOM_TOKEN')}",
        "Intercom-Version": "2.11",
        "Accept":           "application/json",
        "Content-Type":     "application/json",
    }


def fetch_intercom_response_time(customer_domain):
    """Compute median first-reply time for conversations matching a customer domain."""
    if not customer_domain:
        print("   ⚠️  No domain — skipping Intercom response time")
        return None

    # Clean domain
    domain = re.sub(r"^https?://", "", customer_domain).strip().lower()
    domain = re.sub(r"^www\.", "", domain).rstrip("/")

    if not domain or domain in GENERIC_DOMAINS:
        print(f"   ⚠️  Domain '{domain}' is generic/empty — skipping")
        return None

    hdrs = _intercom_headers()
    since = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())

    print(f"⏱️  Fetching Intercom conversations for domain '{domain}' (last 30d)…")

    # Search closed conversations from last 30 days
    query = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "open",                     "operator": "=",  "value": False},
                {"field": "statistics.last_close_at", "operator": ">",  "value": since},
            ],
        },
        "pagination": {"per_page": 150},
    }

    all_convs = []
    cursor = None
    page = 1
    while True:
        if cursor:
            query["pagination"]["starting_after"] = cursor
        elif "starting_after" in query["pagination"]:
            del query["pagination"]["starting_after"]

        try:
            r = requests.post("https://api.intercom.io/conversations/search",
                              headers=hdrs, json=query, timeout=30)
            r.raise_for_status()
        except Exception as e:
            print(f"   ⚠️  Intercom search failed: {e}")
            return None

        data = r.json()
        batch = data.get("conversations", [])
        all_convs.extend(batch)

        pages_info = data.get("pages", {})
        next_info = pages_info.get("next", {})
        cursor = next_info.get("starting_after") if isinstance(next_info, dict) else None

        if not cursor or not batch:
            break
        page += 1

    print(f"   → {len(all_convs)} closed conversations fetched")

    # Match conversations to domain by extracting emails from contacts/source
    reply_times = []
    for conv in all_convs:
        emails = set()
        # source author email
        author_email = conv.get("source", {}).get("author", {}).get("email", "")
        if author_email:
            emails.add(author_email.lower())
        # contacts
        for cref in conv.get("contacts", {}).get("contacts", []):
            email = cref.get("email", "")
            if email:
                emails.add(email.lower())

        # Check if any email matches the customer domain
        matched = any(e.split("@")[-1] == domain for e in emails if "@" in e)
        if not matched:
            continue

        # Extract reply time
        s = conv.get("statistics") or {}
        reply_at = s.get("last_assignment_admin_reply_at")
        assign_at = s.get("last_assignment_at")
        if reply_at and assign_at and reply_at > assign_at:
            reply_times.append(reply_at - assign_at)

    if not reply_times:
        print(f"   ⚠️  No matched conversations with reply data for {domain}")
        return None

    median_sec = stats_mod.median(reply_times)
    print(f"   → {len(reply_times)} conversations matched, median reply: {median_sec:.0f}s")
    return round(median_sec)


# ── CHANNEL STATUS FROM MCT ─────────────────────────────────────────────────

CHANNEL_PROPS = {
    "whatsapp":  "💬 Whatsapp ",
    "livechat":  "🗨️ Live chat",
    "email":     "✉️ Email ",
    "instagram": "📸 Instagram",
}


def extract_channels(mct_page):
    """Read channel multi_select properties from an MCT page. Returns dict of bools."""
    props = mct_page.get("properties", {})
    channels = {}
    for key, prop_name in CHANNEL_PROPS.items():
        prop = props.get(prop_name, {})
        options = prop.get("multi_select", [])
        connected = any("Connected" in opt.get("name", "") and "Not" not in opt.get("name", "")
                        for opt in options)
        channels[key] = connected
    return channels


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 fetch_checkin_data.py \"Company Name\"")
        sys.exit(1)

    query = sys.argv[1]
    mct_pages = fetch_all_mct()
    customer_page, customer_name = match_customer(mct_pages, query)
    customer_id = customer_page["id"]
    print(f"✅ Matched: {customer_name} ({customer_id})")

    # Issues
    issue_pages = fetch_all_issues()
    issues = parse_customer_issues(issue_pages, customer_id)
    print(f"   → {len(issues)} issues for {customer_name}")

    # ClickHouse product metrics
    stripe_id = get_rich_text(customer_page, "🔗 Stripe Customer ID").strip()
    metrics = fetch_clickhouse_metrics(stripe_id)

    # Intercom response time
    domain = get_rich_text(customer_page, "🏢 Domain").strip()
    avg_response_time = fetch_intercom_response_time(domain)

    # Channel status
    channels = extract_channels(customer_page)
    print(f"📡 Channels: {channels}")

    data = {
        "customer_name":          customer_name,
        "customer_id":            customer_id,
        "fetched_at":             datetime.now(timezone.utc).isoformat(),
        "issues":                 issues,
        "metrics":                metrics,
        "avg_response_time_seconds": avg_response_time,
        "channels":               channels,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Written → {OUTPUT_FILE}")
    print("   Run `python3 meetings/checkin/generate_checkin.py` to render the HTML.")


if __name__ == "__main__":
    main()
