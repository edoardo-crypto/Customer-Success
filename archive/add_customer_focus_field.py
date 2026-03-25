#!/usr/bin/env python3
"""
add_customer_focus_field.py — One-time script: adds 🎯 Customer Focus select to MCT

Options: AI for CS | WhatsApp Marketing | Both

Run once, then archive to archive/.
"""
import requests
import creds

NOTION_TOKEN = creds.get("NOTION_TOKEN")
DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}

payload = {
    "properties": {
        "🎯 Customer Focus": {
            "select": {
                "options": [
                    {"name": "AI for CS",           "color": "blue"},
                    {"name": "WhatsApp Marketing",   "color": "green"},
                    {"name": "Both",                 "color": "purple"},
                ]
            }
        }
    }
}

print("Adding 🎯 Customer Focus select property to MCT...")
r = requests.patch(
    f"https://api.notion.com/v1/data_sources/{DS_ID}",
    headers=headers,
    json=payload,
)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    print("✅ Property added. Alex and Aya can now fill in Customer Focus in the MCT.")
    print("   Values: AI for CS | WhatsApp Marketing | Both")
else:
    print(f"❌ Error: {r.text}")
