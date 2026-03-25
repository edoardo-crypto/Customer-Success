"""
add_meeting_fields.py

Patches Notion to add two missing fields needed for the biweekly DAGs & Churn meeting:
  1. "🔁 Churn Reason" (select) → Master Customer Table (multi-source, uses data_sources API)
  2. "✅ Customer Informed?" (checkbox) → Issues Table (standard, uses databases API)
"""

import requests
import creds

NOTION_TOKEN = creds.get("NOTION_TOKEN")
HEADERS_MCT   = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2025-09-03"}
HEADERS_ISSUES = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

MCT_DATA_SOURCE_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"
ISSUES_DB_ID       = "bd1ed48de20e426f8bebeb8e700d19d8"


def add_churn_reason():
    """Add '🔁 Churn Reason' select property to MCT via the data_sources endpoint."""
    print("Adding '🔁 Churn Reason' to Master Customer Table...")

    url = f"https://api.notion.com/v1/data_sources/{MCT_DATA_SOURCE_ID}"
    body = {
        "properties": {
            "🔁 Churn Reason": {
                "select": {
                    "options": [
                        {"name": "Bugs",              "color": "red"},
                        {"name": "Performance",        "color": "orange"},
                        {"name": "AI behavior",        "color": "purple"},
                        {"name": "Configuration",      "color": "blue"},
                        {"name": "Sales misfit",       "color": "yellow"},
                        {"name": "Other",              "color": "gray"},
                    ]
                }
            }
        }
    }

    resp = requests.patch(url, headers=HEADERS_MCT, json=body)
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        print("  ✅ Churn Reason field added successfully.")
    else:
        print(f"  ❌ Error: {resp.text[:400]}")
    return resp.status_code == 200


def add_customer_informed():
    """Add '✅ Customer Informed?' checkbox property to Issues Table via standard databases API."""
    print("Adding '✅ Customer Informed?' to Issues Table...")

    url = f"https://api.notion.com/v1/databases/{ISSUES_DB_ID}"
    body = {
        "properties": {
            "✅ Customer Informed?": {
                "checkbox": {}
            }
        }
    }

    resp = requests.patch(url, headers=HEADERS_ISSUES, json=body)
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        print("  ✅ Customer Informed? field added successfully.")
    else:
        print(f"  ❌ Error: {resp.text[:400]}")
    return resp.status_code == 200


if __name__ == "__main__":
    ok1 = add_churn_reason()
    ok2 = add_customer_informed()
    print()
    if ok1 and ok2:
        print("✅ Both fields added. Ready for Phase 2 (HTML report).")
    else:
        print("⚠️  One or more fields failed — check errors above.")
