#!/usr/bin/env python3
"""
update_mct_report_urls.py

Reads site/manifest.json and writes the GitHub Pages URL into each customer's
"📊 Check-in Report" field in the MCT.

    python3 meetings/checkin/update_mct_report_urls.py

Run after generate_all_checkins.py and the gh-pages deploy step.
"""

import json
import os
import sys
import time
import requests

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
MANIFEST_PATH = os.path.join(SCRIPT_DIR, "site", "manifest.json")

BASE_URL   = "https://edoardo-crypto.github.io/Customer-Success"
FIELD_NAME = "📊 Check-in Report"
MCT_DS_ID  = "3ceb1ad0-91f1-40db-945a-c51c58035898"


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


def _headers():
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Notion-Version": "2025-09-03",
        "Content-Type": "application/json",
    }


def patch_page_url(page_id, url):
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=_headers(),
        json={"properties": {FIELD_NAME: {"url": url}}},
        timeout=20,
    )
    if r.ok:
        return True
    print(f"  [ERROR] PATCH {page_id[:8]}… → {r.status_code}: {r.text[:200]}")
    return False


def main():
    if not os.path.exists(MANIFEST_PATH):
        print(f"❌ {MANIFEST_PATH} not found — run generate_all_checkins.py first")
        sys.exit(1)

    with open(MANIFEST_PATH, encoding="utf-8") as f:
        manifest = json.load(f)

    print("=" * 60)
    print("  update_mct_report_urls.py")
    print(f"  Writing {FIELD_NAME} to {len(manifest)} MCT pages")
    print("=" * 60)

    updated = errors = 0

    for entry in sorted(manifest, key=lambda e: e["name"].lower()):
        url = f"{BASE_URL}/{entry['slug']}.html"
        print(f"  {entry['name']:<40} → {url}")

        if patch_page_url(entry["page_id"], url):
            updated += 1
        else:
            errors += 1

        time.sleep(0.3)

    print(f"\n✅ Done — updated: {updated}, errors: {errors}")


if __name__ == "__main__":
    main()
