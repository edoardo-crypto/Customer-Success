"""
fetch_checkin_data.py

Fetches customer-specific issues from Notion for a CS check-in meeting.

    python3 meetings/checkin/fetch_checkin_data.py "Company Name"

Writes checkin_data.json in this directory, which generate_checkin.py reads.

Also importable: generate_all_checkins.py uses fetch_all_mct(), fetch_all_issues(),
and parse_customer_issues() directly.
"""

import json
import os
import sys
import requests
from datetime import datetime

MCT_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"
ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "checkin_data.json")

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

    issue_pages = fetch_all_issues()
    issues = parse_customer_issues(issue_pages, customer_id)
    print(f"   → {len(issues)} issues for {customer_name}")

    data = {
        "customer_name": customer_name,
        "customer_id":   customer_id,
        "fetched_at":    datetime.utcnow().isoformat() + "Z",
        "issues":        issues,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Written → {OUTPUT_FILE}")
    print("   Run `python3 meetings/checkin/generate_checkin.py` to render the HTML.")


if __name__ == "__main__":
    main()
