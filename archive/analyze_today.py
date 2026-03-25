#!/usr/bin/env python3
"""
analyze_today.py — Full table of all closed Intercom conversations today (2026-02-20)
vs what's logged in Notion Issues.  Read-only.
"""

import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
import creds

INTERCOM_TOKEN = creds.get("INTERCOM_TOKEN")
NOTION_TOKEN   = creds.get("NOTION_TOKEN")
ISSUES_DB_ID   = "bd1ed48de20e426f8bebeb8e700d19d8"

TODAY_START_TS  = int(datetime(2026, 2, 20, 0, 0, 0, tzinfo=timezone.utc).timestamp())
TODAY_START_ISO = "2026-02-20T00:00:00.000Z"

notion_headers = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}
intercom_headers = {
    "Authorization":    f"Bearer {INTERCOM_TOKEN}",
    "Accept":           "application/json",
    "Intercom-Version": "2.10",
    "Content-Type":     "application/json",
}


def http_post(url, headers, data):
    body = json.dumps(data).encode()
    req  = urllib.request.Request(url, data=body, method="POST", headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def http_get(url, headers):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def extract_ca(conv):
    ca = conv.get("custom_attributes", {}) or {}
    it  = (ca.get("issue_type")        or ca.get("Issue Type")        or "").strip()
    sev = (ca.get("cs_severity")       or ca.get("Severity")          or "").strip()
    desc= (ca.get("issue_description") or ca.get("Issue Description") or "").strip()
    return it, sev, desc


def should_log(it, sev, desc):
    if it  == "Not an Issue": return False, 'issue_type="Not an Issue"'
    if it  == "":             return False, "issue_type empty"
    if sev == "":             return False, "cs_severity empty"
    if sev == "Not important":return False, 'cs_severity="Not important"'
    if desc == "":            return False, "issue_description empty"
    return True, ""


# ── 1. Notion: all rows created OR edited today ────────────────────────────────
print("Fetching Notion rows created or edited today…")
notion_rows = {}  # source_id → {page_id, issue_type}

for ts_key in ("created_time", "last_edited_time"):
    cursor = None
    while True:
        payload = {
            "filter": {
                "timestamp": ts_key,
                ts_key: {"on_or_after": TODAY_START_ISO},
            },
            "page_size": 100,
        }
        if cursor:
            payload["start_cursor"] = cursor
        data = http_post(
            f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}/query",
            notion_headers, payload,
        )
        for page in data.get("results", []):
            rt = page["properties"].get("Source ID", {}).get("rich_text", [])
            sid = rt[0]["plain_text"].strip() if rt else ""
            sel = page["properties"].get("Issue Type", {}).get("select")
            it  = sel["name"] if sel else ""
            if sid:
                notion_rows[sid] = {"page_id": page["id"], "issue_type": it}
        if not data.get("has_more"):
            break
        cursor = data["next_cursor"]

print(f"  → {len(notion_rows)} Notion rows with Intercom Source IDs\n")


# ── 2. Intercom: IDs of closed conversations updated today ─────────────────────
print("Searching Intercom for closed conversations updated today…")
payload = {
    "query": {
        "operator": "AND",
        "value": [
            {"field": "state",      "operator": "=", "value": "closed"},
            {"field": "updated_at", "operator": ">", "value": TODAY_START_TS},
        ],
    },
    "pagination": {"per_page": 150},
}
conv_ids = []
next_cursor = None
while True:
    if next_cursor:
        payload["pagination"]["starting_after"] = next_cursor
    data = http_post("https://api.intercom.io/conversations/search", intercom_headers, payload)
    convs = data.get("conversations", [])
    conv_ids.extend(str(c["id"]) for c in convs)
    pages_info  = data.get("pages", {}) or {}
    next_cursor = (pages_info.get("next") or {}).get("starting_after")
    total = data.get("total_count", "?")
    if not next_cursor or not convs:
        break
print(f"  → {len(conv_ids)} conversation IDs found (total reported by Intercom: {total})\n")


# ── 3. Fetch each full conversation ────────────────────────────────────────────
print(f"Fetching full conversation objects (for custom_attributes)…")
get_headers = {k: v for k, v in intercom_headers.items() if k != "Content-Type"}
conversations = []
for i, cid in enumerate(conv_ids, 1):
    try:
        conv = http_get(f"https://api.intercom.io/conversations/{cid}", get_headers)
        conversations.append(conv)
    except urllib.error.HTTPError as e:
        print(f"  WARNING: conv {cid} → HTTP {e.code}")
    if i % 20 == 0:
        print(f"  {i}/{len(conv_ids)}…")
        time.sleep(0.3)
print(f"  → {len(conversations)} fetched\n")


# ── 4. Build table ─────────────────────────────────────────────────────────────
rows = []
for conv in conversations:
    cid  = str(conv.get("id", ""))
    it, sev, desc = extract_ca(conv)
    ok, reason = should_log(it, sev, desc)
    in_notion  = cid in notion_rows
    nrow       = notion_rows.get(cid, {})

    # closed_at timestamp
    closed_at = _fmt = ""
    stats = conv.get("statistics") or {}
    closed_ts = stats.get("last_closed_at") or conv.get("updated_at")
    if closed_ts:
        try:
            _fmt = datetime.fromtimestamp(int(closed_ts), tz=timezone.utc).strftime("%H:%M")
        except:
            _fmt = str(closed_ts)

    title = ((conv.get("source") or {}).get("subject") or
             conv.get("title") or "(no title)")
    # strip HTML tags crudely
    import re
    title = re.sub(r"<[^>]+>", "", title).strip()
    title = title[:55] + "…" if len(title) > 55 else title

    rows.append({
        "id":        cid,
        "time":      _fmt,
        "title":     title,
        "it":        it[:25] if it else "(empty)",
        "sev":       sev[:15] if sev else "(empty)",
        "should":    ok,
        "in_notion": in_notion,
        "n_type":    nrow.get("issue_type", ""),
        "reason":    reason,
    })

# Sort by time
rows.sort(key=lambda r: r["time"])

# ── 5. Print table ─────────────────────────────────────────────────────────────
logged_count   = sum(1 for r in rows if r["in_notion"])
missing_count  = sum(1 for r in rows if r["should"] and not r["in_notion"])
spurious_count = sum(1 for r in rows if not r["should"] and r["in_notion"])
correct_excl   = sum(1 for r in rows if not r["should"] and not r["in_notion"])
correct_incl   = sum(1 for r in rows if r["should"] and r["in_notion"])

print(f"\n{'='*120}")
print(f"  TODAY'S CLOSED INTERCOM CONVERSATIONS — 2026-02-20")
print(f"{'='*120}")
print(f"  Total closed today: {len(rows)}  |  Logged in Notion: {logged_count}  |  "
      f"Correct inclusions: {correct_incl}  |  Correct exclusions: {correct_excl}  |  "
      f"Missing: {missing_count}  |  Spurious: {spurious_count}")
print(f"{'='*120}")

col_id    = 19
col_time  = 6
col_title = 56
col_it    = 26
col_sev   = 16
col_stat  = 8
col_ntype = 26

header = (
    f"{'Conv ID':<{col_id}} {'Time':<{col_time}} {'Title':<{col_title}} "
    f"{'Issue Type':<{col_it}} {'Severity':<{col_sev}} "
    f"{'Status':<{col_stat}} {'Notion Issue Type':<{col_ntype}} Notes"
)
print(header)
print("-" * 140)

for r in rows:
    if r["should"] and r["in_notion"]:
        status = "✓ OK"
        notes  = ""
    elif r["should"] and not r["in_notion"]:
        status = "⚠ MISS"
        notes  = "should be logged but isn't"
    elif not r["should"] and r["in_notion"]:
        status = "❌ SPUR"
        notes  = f"blocked by: {r['reason']}"
    else:
        status = "– excl"
        notes  = r["reason"]

    n_type = r["n_type"] if r["in_notion"] else "—"

    line = (
        f"{r['id']:<{col_id}} {r['time']:<{col_time}} {r['title']:<{col_title}} "
        f"{r['it']:<{col_it}} {r['sev']:<{col_sev}} "
        f"{status:<{col_stat}} {n_type:<{col_ntype}} {notes}"
    )
    print(line)

print(f"\n  Legend:  ✓ OK = correctly logged  |  – excl = correctly excluded  |  ⚠ MISS = missing from Notion  |  ❌ SPUR = in Notion but shouldn't be")
print(f"{'='*120}\n")
