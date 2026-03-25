#!/usr/bin/env python3
"""
deploy_checkin_report_field.py
-------------------------------
ONE-TIME SETUP SCRIPT — run once, then archive.

Adds "📊 Check-in Report" (url type) to the Master Customer Table
via PATCH /data_sources/{MCT_DS_ID} with Notion-Version 2025-09-03.

Safe to re-run: adding a property that already exists is a no-op on Notion.

NOTE: Never set any other property to null in the data_sources PATCH — it can
silently corrupt other properties on multi-source MCT databases.
"""

import json
import os
import urllib.request
import urllib.error

# ── Config ────────────────────────────────────────────────────────────────────
MCT_DS_ID  = "3ceb1ad0-91f1-40db-945a-c51c58035898"
FIELD_NAME = "📊 Check-in Report"


def _get_token():
    token = os.environ.get("NOTION_TOKEN", "")
    if token:
        return token
    creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Credentials.md")
    if os.path.exists(creds_path):
        with open(creds_path) as f:
            for line in f:
                if "NOTION_TOKEN" in line or "ntn_" in line:
                    for word in line.split():
                        if word.startswith("ntn_"):
                            return word.strip()
    raise RuntimeError("NOTION_TOKEN not set and not found in Credentials.md")


def notion_request(method, path, body=None, version="2022-06-28"):
    url = f"https://api.notion.com/v1/{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {_get_token()}")
    req.add_header("Notion-Version", version)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        raise Exception(f"HTTP {e.code} {e.reason} — {body_text}") from None


def main():
    print()
    print("=" * 60)
    print("  deploy_checkin_report_field.py")
    print(f"  Adds '{FIELD_NAME}' (url) to MCT schema")
    print("=" * 60)

    body = {
        "properties": {
            FIELD_NAME: {"url": {}}
        }
    }
    print(f"\n  PATCHing data_sources/{MCT_DS_ID} to add '{FIELD_NAME}'...")
    notion_request("PATCH", f"data_sources/{MCT_DS_ID}", body, version="2025-09-03")
    print("  ✓ PATCH complete")

    print()
    print("Next steps:")
    print("  1. Verify 📊 Check-in Report column appears in Notion MCT")
    print("  2. Archive this script to archive/")
    print()


if __name__ == "__main__":
    main()
