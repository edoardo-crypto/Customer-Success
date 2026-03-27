#!/usr/bin/env python3
"""
deploy_sla_fields.py — One-time: add SLA tracking fields to the Issues Table.

Adds four new properties:
  - Triaged At        (date)   — when issue left Linear Triage state
  - SLA Triage Deadline   (date)   — Created At + 1 business day
  - SLA Resolution Deadline (date) — Triaged At + severity-based business days
  - SLA Status        (select) — notification state machine

After running, archive this script to archive/.

Usage:
  python3 deploy_sla_fields.py
"""

import requests
import creds

NOTION_TOKEN = creds.get("NOTION_TOKEN")
ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"
NOTION_VERSION = "2022-06-28"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

NEW_PROPERTIES = {
    "Triaged At": {"date": {}},
    "SLA Triage Deadline": {"date": {}},
    "SLA Resolution Deadline": {"date": {}},
    "SLA Status": {
        "select": {
            "options": [
                {"name": "On Track", "color": "green"},
                {"name": "Triage Warning", "color": "yellow"},
                {"name": "Triage Breach", "color": "orange"},
                {"name": "Resolution Warning", "color": "yellow"},
                {"name": "Resolution Breach", "color": "red"},
            ]
        }
    },
}


def main():
    print("Adding SLA fields to Issues Table…")
    print(f"  DB: {ISSUES_DB_ID}")
    print(f"  Fields: {', '.join(NEW_PROPERTIES.keys())}\n")

    resp = requests.patch(
        f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}",
        headers=HEADERS,
        json={"properties": NEW_PROPERTIES},
    )

    if resp.status_code == 200:
        print("OK — all 4 properties added successfully.")
        print("\nNext steps:")
        print("  1. Check the Issues Table in Notion — new columns should appear")
        print("  2. Create #sla-alerts Slack channel and note its ID")
        print("  3. Add SLA_SLACK_CHANNEL to GitHub secrets")
        print("  4. Archive this script: mv deploy_sla_fields.py archive/")
    else:
        print(f"FAILED — {resp.status_code}: {resp.text[:500]}")


if __name__ == "__main__":
    main()
