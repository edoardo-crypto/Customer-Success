#!/usr/bin/env python3
"""
audit_pipeline.py — Read-only audit of the Intercom → Notion Issues pipeline.

Checks conversations closed on 2026-02-19 or 2026-02-20:
  - Every conversation that SHOULD be logged → verify it's in Notion
  - Every conversation that SHOULD be excluded → verify it's NOT in Notion

Makes NO writes to Intercom or Notion.
"""

import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ── Credentials ───────────────────────────────────────────────────────────────
INTERCOM_TOKEN = "***REMOVED***"
NOTION_TOKEN   = "***REMOVED***"
ISSUES_DB_ID   = "bd1ed48de20e426f8bebeb8e700d19d8"

# Start of 2026-02-19 UTC as Unix timestamp
AUDIT_START_TS  = int(datetime(2026, 2, 19, 0, 0, 0, tzinfo=timezone.utc).timestamp())
AUDIT_START_ISO = "2026-02-19T00:00:00.000Z"

notion_headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type":  "application/json",
    "Notion-Version": "2022-06-28",
}
intercom_headers = {
    "Authorization":    f"Bearer {INTERCOM_TOKEN}",
    "Accept":           "application/json",
    "Intercom-Version": "2.10",
    "Content-Type":     "application/json",
}


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def http_post(url, headers, data):
    body = json.dumps(data).encode()
    req  = urllib.request.Request(url, data=body, method="POST", headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def http_get(url, headers):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


# ── Notion helpers ─────────────────────────────────────────────────────────────

def get_prop(page, name, kind):
    prop = page.get("properties", {}).get(name, {})
    if kind == "rich_text":
        rich = prop.get("rich_text", [])
        return rich[0]["plain_text"] if rich else ""
    if kind == "select":
        sel = prop.get("select")
        return sel["name"] if sel else ""
    if kind == "title":
        rich = prop.get("title", [])
        return rich[0]["plain_text"] if rich else ""
    return ""


def query_notion_all(filter_body):
    """Paginate through a Notion DB query and return all pages."""
    pages   = []
    cursor  = None
    page_no = 0
    while True:
        page_no += 1
        payload = {"filter": filter_body, "page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        data = http_post(
            f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query",
            notion_headers,
            payload,
        )
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


# ═════════════════════════════════════════════════════════════════════════════
# STEP 1 — Get Notion side
# ═════════════════════════════════════════════════════════════════════════════

def fetch_notion_rows():
    print("\n[Step 1] Querying Notion Issues table…")

    # Pass A — rows created in the audit window
    pass_a = query_notion_all({
        "timestamp": "created_time",
        "created_time": {"on_or_after": AUDIT_START_ISO},
    })
    print(f"  Pass A (created_time ≥ 2026-02-19): {len(pass_a)} rows")

    # Pass B — rows last edited in the audit window (catches re-closed conversations)
    pass_b = query_notion_all({
        "timestamp": "last_edited_time",
        "last_edited_time": {"on_or_after": AUDIT_START_ISO},
    })
    print(f"  Pass B (last_edited_time ≥ 2026-02-19): {len(pass_b)} rows")

    # Merge by page ID
    merged = {p["id"]: p for p in pass_a}
    for p in pass_b:
        merged[p["id"]] = p
    all_pages = list(merged.values())
    print(f"  After dedup: {len(all_pages)} unique rows")

    # Extract Source ID (Intercom conv ID) and metadata
    notion_rows = {}  # source_id → {page_id, issue_type, severity, source}
    for page in all_pages:
        source_id  = get_prop(page, "Source ID", "rich_text").strip()
        issue_type = get_prop(page, "Issue Type", "select")
        severity   = get_prop(page, "Severity",   "select")
        source     = get_prop(page, "Source",     "select")
        if source_id:
            notion_rows[source_id] = {
                "page_id":    page["id"],
                "issue_type": issue_type,
                "severity":   severity,
                "source":     source,
            }

    print(f"  Rows with a Source ID (Intercom): {len(notion_rows)}")
    return notion_rows


# ═════════════════════════════════════════════════════════════════════════════
# STEP 2 — Get Intercom side
# ═════════════════════════════════════════════════════════════════════════════

def extract_custom_attrs(conv):
    """
    Extract the 3 filter fields from a conversation's custom_attributes.
    The Intercom direct GET API returns Title Case keys; the webhook payload
    uses snake_case machine names.  We check both so the audit works with
    either source.
    """
    ca = conv.get("custom_attributes", {}) or {}
    issue_type        = (ca.get("issue_type")        or ca.get("Issue Type")        or "").strip()
    cs_severity       = (ca.get("cs_severity")       or ca.get("Severity")          or "").strip()
    issue_description = (ca.get("issue_description") or ca.get("Issue Description") or "").strip()
    return issue_type, cs_severity, issue_description


def apply_filter(conv):
    """Return (should_log, failed_reasons) for a conversation."""
    issue_type, cs_severity, issue_description = extract_custom_attrs(conv)

    failed = []
    if issue_type == "Not an Issue":
        failed.append('issue_type = "Not an Issue"')
    if issue_type == "":
        failed.append("issue_type is empty")
    if cs_severity == "":
        failed.append("cs_severity is empty")
    if cs_severity == "Not important":
        failed.append('cs_severity = "Not important"')
    if issue_description == "":
        failed.append("issue_description is empty")

    return len(failed) == 0, failed


def fetch_intercom_conversations():
    """
    Step 2a — Search for closed conversations updated since audit start.
    The search endpoint returns abbreviated objects without custom_attributes,
    so we collect IDs here and then fetch each one individually in step 2b.
    """
    print("\n[Step 2a] Searching Intercom for closed conversations (IDs only)…")

    url = "https://api.intercom.io/conversations/search"
    payload = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "state",      "operator": "=", "value": "closed"},
                {"field": "updated_at", "operator": ">", "value": AUDIT_START_TS},
            ],
        },
        "pagination": {"per_page": 150},
    }

    conv_ids    = []
    page_no     = 0
    next_cursor = None

    while True:
        page_no += 1
        if next_cursor:
            payload["pagination"]["starting_after"] = next_cursor

        try:
            data = http_post(url, intercom_headers, payload)
        except urllib.error.HTTPError as e:
            status = e.code
            body   = e.read().decode(errors="replace")
            if status == 401:
                print(f"\n  ⚠ Intercom 401 Unauthorized — token may have expired.")
                print(f"    Response: {body[:300]}")
                return None  # Signal caller to run fallback
            raise

        convs = data.get("conversations", [])
        conv_ids.extend(str(c["id"]) for c in convs)

        pages_info  = data.get("pages", {}) or {}
        next_info   = pages_info.get("next") or {}
        next_cursor = next_info.get("starting_after")
        total_pages = pages_info.get("total_pages", "?")
        total_count = data.get("total_count", "?")

        print(f"  Page {page_no}/{total_pages}: got {len(convs)} IDs (total so far: {len(conv_ids)}/{total_count})")

        if not next_cursor or len(convs) == 0:
            break

    print(f"  Total conversation IDs: {len(conv_ids)}")

    # Step 2b — Fetch each conversation individually to get custom_attributes
    print(f"\n[Step 2b] Fetching full conversation objects (with custom_attributes)…")
    conversations = []
    for i, conv_id in enumerate(conv_ids, 1):
        try:
            conv = http_get(
                f"https://api.intercom.io/conversations/{conv_id}",
                {k: v for k, v in intercom_headers.items() if k != "Content-Type"},
            )
            conversations.append(conv)
        except urllib.error.HTTPError as e:
            print(f"  WARNING: Could not fetch conv {conv_id}: HTTP {e.code}")
        if i % 20 == 0:
            print(f"  Fetched {i}/{len(conv_ids)}…")
            time.sleep(0.5)  # gentle rate limiting

    print(f"  Done. {len(conversations)} conversations fetched.")
    return conversations


# ═════════════════════════════════════════════════════════════════════════════
# STEP 3 — Cross-reference and report
# ═════════════════════════════════════════════════════════════════════════════

def cross_reference(conversations, notion_rows):
    print("\n[Step 3] Cross-referencing…")

    should_be_in    = {}  # conv_id → conv
    should_not_be   = {}  # conv_id → (conv, failed_reasons)

    for conv in conversations:
        conv_id = str(conv.get("id", ""))
        ok, reasons = apply_filter(conv)
        if ok:
            should_be_in[conv_id] = conv
        else:
            should_not_be[conv_id] = (conv, reasons)

    notion_ids = set(notion_rows.keys())

    correct_inclusions = []
    missing            = []   # should be in Notion, isn't
    spurious           = []   # shouldn't be in Notion, is
    correct_exclusions = []

    for conv_id, conv in should_be_in.items():
        if conv_id in notion_ids:
            correct_inclusions.append((conv_id, conv))
        else:
            missing.append((conv_id, conv))

    for conv_id, (conv, reasons) in should_not_be.items():
        if conv_id in notion_ids:
            spurious.append((conv_id, conv, reasons))
        else:
            correct_exclusions.append(conv_id)

    # ── Print summary ─────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"=== INTERCOM → NOTION AUDIT (2026-02-19 to 2026-02-20) ===")
    print(f"{'='*60}")
    print(f"\nIntercom: {len(conversations)} closed conversations found")
    print(f"Notion:   {len(notion_ids)} Intercom-sourced rows in audit window\n")
    print(f"✓ CORRECT INCLUSIONS:                        {len(correct_inclusions)}")
    print(f"✓ CORRECT EXCLUSIONS (count only):           {len(correct_exclusions)}")
    print(f"⚠ MISSING  (should be in Notion but isn't): {len(missing)}")
    print(f"❌ SPURIOUS (in Notion but shouldn't be):    {len(spurious)}")

    # ── Missing details ───────────────────────────────────────────────────────
    if missing:
        print(f"\n--- ⚠ MISSING ROWS ({len(missing)}) ---")
        for i, (conv_id, conv) in enumerate(missing, 1):
            issue_type, cs_severity, issue_description = extract_custom_attrs(conv)
            title = _conv_title(conv)
            print(f"\n[{i}] Conv {conv_id}  \"{title}\"")
            print(f"    https://app.intercom.com/a/apps/konvoai/conversations/{conv_id}")
            print(f"    issue_type:        {issue_type!r}")
            print(f"    cs_severity:       {cs_severity!r}")
            print(f"    issue_description: {issue_description[:80]!r}")
            print(f"    updated_at: {_fmt_ts(conv.get('updated_at'))}")
            print(f"    Notion: NOT FOUND")

    # ── Spurious details ──────────────────────────────────────────────────────
    if spurious:
        print(f"\n--- ❌ SPURIOUS ROWS ({len(spurious)}) ---")
        for i, (conv_id, conv, reasons) in enumerate(spurious, 1):
            issue_type, cs_severity, issue_description = extract_custom_attrs(conv)
            title = _conv_title(conv)
            nrow  = notion_rows.get(conv_id, {})
            print(f"\n[{i}] Conv {conv_id}  \"{title}\"")
            print(f"    https://app.intercom.com/a/apps/konvoai/conversations/{conv_id}")
            print(f"    issue_type:        {issue_type!r}")
            print(f"    cs_severity:       {cs_severity!r}")
            print(f"    issue_description: {issue_description[:80]!r}")
            print(f"    Reason(s) blocked: {' + '.join(reasons)}")
            print(f"    Notion page: {nrow.get('page_id', '?')}  (Issue Type: {nrow.get('issue_type') or 'blank'})")

    # ── All-clear messages ────────────────────────────────────────────────────
    if not missing and not spurious:
        print("\n✅ Pipeline is clean — no missing or spurious rows found.")
    elif not missing:
        print("\n✅ No missing rows — every qualifying conversation is in Notion.")
    elif not spurious:
        print("\n✅ No spurious rows — no excluded conversations leaked into Notion.")

    print(f"\n{'='*60}\n")


# ═════════════════════════════════════════════════════════════════════════════
# STEP 3b — Notion-only fallback
# ═════════════════════════════════════════════════════════════════════════════

def notion_only_report(notion_rows):
    print(f"\n{'='*60}")
    print("=== NOTION-ONLY AUDIT (Intercom API unavailable) ===")
    print(f"{'='*60}")
    print(f"\nNotion rows in audit window: {len(notion_rows)}")
    print("NOTE: Cannot verify false negatives without Intercom access.\n")

    blank_type = [(sid, row) for sid, row in notion_rows.items() if not row["issue_type"]]
    valid_type  = [(sid, row) for sid, row in notion_rows.items() if row["issue_type"]]

    print(f"Rows with a valid Issue Type:  {len(valid_type)}  ← likely correct")
    print(f"Rows with blank Issue Type:    {len(blank_type)}  ← possibly bad (filter bypass)")

    if blank_type:
        print("\n--- Rows with blank Issue Type ---")
        for i, (sid, row) in enumerate(blank_type, 1):
            print(f"[{i}] Source ID: {sid}")
            print(f"     Notion page: {row['page_id']}")
            print(f"     Severity: {row['severity'] or 'blank'}  |  Source: {row['source'] or 'blank'}")

    print(f"\n{'='*60}\n")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _conv_title(conv):
    """Best-effort conversation title."""
    return (
        conv.get("source", {}) or {}
    ).get("subject", "") or conv.get("title", "") or "(no title)"


def _fmt_ts(ts):
    if not ts:
        return "unknown"
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts)


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    notion_rows   = fetch_notion_rows()
    conversations = fetch_intercom_conversations()

    if conversations is None:
        # Intercom auth failed — run Notion-only report
        notion_only_report(notion_rows)
    else:
        cross_reference(conversations, notion_rows)


if __name__ == "__main__":
    main()
