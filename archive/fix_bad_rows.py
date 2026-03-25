#!/usr/bin/env python3
"""
fix_bad_rows.py — Archive 10 bad Notion Issues rows + classify 4 feature-request rows.

Step 1: Archive 10 rows with no real conversation data or not genuine issues:
  - 2 rows (Feb 12-13): gratitude message + unable-to-classify
  - 4 rows (Group B, Feb 17 batch): blank issue_type, no data
  - 4 rows (Group D, Feb 19 batch): blank issue_type, no data (caused by broken filter)

Step 2: Set Issue Type = "Feature Improvement Request" on 4 real feature-request rows
  that slipped through with blank Issue Type but have valid summaries.
"""

import requests
import time
import creds

NOTION_TOKEN = creds.get("NOTION_TOKEN")
NOTION_VERSION = "2022-06-28"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# ── Step 1: Archive these 10 rows ─────────────────────────────────────────────

ROWS_TO_ARCHIVE = [
    # Feb-12 — gratitude message, not an issue
    {"page_id": "305e418f-d8c4-81c6-8701-e95b7612f2e0", "conv_id": "215473070878563",
     "reason": "Gratitude message, not an issue"},
    # Feb-13 — unable to classify, no data
    {"page_id": "306e418f-d8c4-8197-8f8f-dc9e90fc4104", "conv_id": "215473087992773",
     "reason": "Unable to classify, no data"},
    # Group B — Feb 17 batch (4 rows, 17:30–17:43, broken filter)
    {"page_id": "30ae418f-d8c4-81f4-b96b-f0975e6f1d76", "conv_id": "215473136302710",
     "reason": "Group B batch, no conversation data"},
    {"page_id": "30ae418f-d8c4-81bf-9128-eb14ddc7c524", "conv_id": "215473134561136",
     "reason": "Group B batch, no conversation data"},
    {"page_id": "30ae418f-d8c4-81e1-b403-d6ec8b8103bf", "conv_id": "215473137172245",
     "reason": "Group B batch, no conversation data"},
    {"page_id": "30ae418f-d8c4-81aa-81e0-e9bc044ca16d", "conv_id": "215473139250442",
     "reason": "Group B batch, no conversation data"},
    # Group D — Feb 19 batch (4 rows, 16:48–17:00, caused by fix_aten_filter.py)
    {"page_id": "30ce418f-d8c4-8162-b6b7-c546d9ff82f4", "conv_id": "215473167202929",
     "reason": "Group D batch, no conversation data"},
    {"page_id": "30ce418f-d8c4-8108-b6d6-f377152fd7b8", "conv_id": "215473162286731",
     "reason": "Group D batch, no conversation data"},
    {"page_id": "30ce418f-d8c4-812f-9bec-e745e56a7c05", "conv_id": "215473134189887",
     "reason": "Group D batch, no conversation data"},
    {"page_id": "30ce418f-d8c4-8154-bce6-e23cefbe8422", "conv_id": "215473163269728",
     "reason": "Group D batch, no conversation data"},
]

# ── Step 2: Set Issue Type on these 4 rows ────────────────────────────────────

ROWS_TO_CLASSIFY = [
    # Feb-16 feature requests (had blank Issue Type but real summaries)
    {"page_id": "309e418f-d8c4-8107-82d8-f1d79d5fa82f", "conv_id": "215473096052773",
     "title": "Response time per agent + search bar"},
    {"page_id": "309e418f-d8c4-811a-b4f4-ff8cbea5e8cf", "conv_id": "215473123906953",
     "title": "Livechat widget analytics"},
    # Feb-17 feature requests (had blank Issue Type but real summaries)
    {"page_id": "30ae418f-d8c4-819f-8ec3-f123eaf97783", "conv_id": "215473132765568",
     "title": "Opt-out metrics per campaign"},
    {"page_id": "30ae418f-d8c4-814b-92e7-ebef4629861b", "conv_id": "215473132262308",
     "title": "Manual contact unification"},
]


def archive_page(page_id: str) -> dict:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    return requests.patch(url, headers=HEADERS, json={"archived": True})


def set_issue_type(page_id: str, issue_type: str) -> dict:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            "Issue Type": {
                "select": {"name": issue_type}
            }
        }
    }
    return requests.patch(url, headers=HEADERS, json=payload)


def main():
    print("=" * 60)
    print("STEP 1 — Archiving 10 bad rows")
    print("=" * 60)

    archive_ok = 0
    archive_fail = 0
    for row in ROWS_TO_ARCHIVE:
        resp = archive_page(row["page_id"])
        if resp.status_code == 200:
            print(f"  ✓ Archived  conv={row['conv_id']}  ({row['reason']})")
            archive_ok += 1
        else:
            print(f"  ✗ FAILED    conv={row['conv_id']}  status={resp.status_code}  body={resp.text[:200]}")
            archive_fail += 1
        time.sleep(0.3)  # respect Notion rate limits

    print()
    print(f"  Archive result: {archive_ok} succeeded, {archive_fail} failed")

    print()
    print("=" * 60)
    print("STEP 2 — Classifying 4 feature-request rows")
    print("=" * 60)

    classify_ok = 0
    classify_fail = 0
    for row in ROWS_TO_CLASSIFY:
        resp = set_issue_type(row["page_id"], "Feature Improvement Request")
        if resp.status_code == 200:
            print(f"  ✓ Classified  conv={row['conv_id']}  → Feature Improvement Request  ({row['title']})")
            classify_ok += 1
        else:
            print(f"  ✗ FAILED      conv={row['conv_id']}  status={resp.status_code}  body={resp.text[:200]}")
            classify_fail += 1
        time.sleep(0.3)

    print()
    print(f"  Classify result: {classify_ok} succeeded, {classify_fail} failed")

    print()
    print("=" * 60)
    print("DONE")
    print(f"  Archived : {archive_ok}/{len(ROWS_TO_ARCHIVE)}")
    print(f"  Classified: {classify_ok}/{len(ROWS_TO_CLASSIFY)}")
    if archive_fail or classify_fail:
        print("  ⚠ Some operations failed — check output above")
    else:
        print("  All operations succeeded ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
