#!/usr/bin/env python3
"""
rename_last_contact_column.py

Renames '📞 Last Contact Date' → '📞 Last Contact Date 🔒' atomically:
  1a. Notion MCT property (via data_sources API)
  1b. n8n workflow veEIgePuCQ0z9jYr  (Daily Last Contact Date Sync)
  1c. n8n "Log Customer Contact" workflow (found by name)
"""

import json
import requests
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
N8N_API_KEY  = creds.get("N8N_API_KEY")
N8N_BASE    = "https://konvoai.app.n8n.cloud"
NOTION_BASE = "https://api.notion.com/v1"

DATA_SOURCE_ID      = "3ceb1ad0-91f1-40db-945a-c51c58035898"
DAILY_SYNC_WF       = "veEIgePuCQ0z9jYr"
LOG_CONTACT_WF_NAME = "Log Customer Contact"

# Plain-emoji form (used in Python PATCH bodies and Notion API calls)
OLD_NAME = "📞 Last Contact Date"
NEW_NAME = "📞 Last Contact Date 🔒"

# JS unicode-escaped form (used inside Code-node JS strings)
# 📞 = \uD83D\uDCDE   🔒 = \uD83D\uDD12
OLD_JS = r"\uD83D\uDCDE Last Contact Date"
NEW_JS = r"\uD83D\uDCDE Last Contact Date \uD83D\uDD12"


# ── Notion ────────────────────────────────────────────────────────────────────

def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2025-09-03",
        "Content-Type": "application/json",
    }


def rename_notion_property():
    print("Step 1a: Renaming Notion MCT property...")
    url  = f"{NOTION_BASE}/data_sources/{DATA_SOURCE_ID}"
    body = {"properties": {OLD_NAME: {"name": NEW_NAME}}}
    r = requests.patch(url, headers=notion_headers(), json=body)
    if r.status_code == 200:
        print(f"  ✅ Notion property renamed → '{NEW_NAME}'")
    elif r.status_code == 400 and "not a valid property schema" in r.text:
        print(f"  ✅ Notion property already renamed (old name no longer exists — idempotent)")
    else:
        print(f"  ❌ Notion PATCH failed: {r.status_code}  {r.text[:400]}")
        raise SystemExit(1)


# ── n8n ───────────────────────────────────────────────────────────────────────

def n8n_headers():
    return {
        "X-N8N-API-KEY": N8N_API_KEY,
        "Content-Type": "application/json",
    }


def get_workflow(wf_id):
    r = requests.get(f"{N8N_BASE}/api/v1/workflows/{wf_id}", headers=n8n_headers())
    r.raise_for_status()
    return r.json()


def find_workflow_id_by_name(name):
    r = requests.get(
        f"{N8N_BASE}/api/v1/workflows",
        headers=n8n_headers(),
        params={"name": name},
    )
    r.raise_for_status()
    for wf in r.json().get("data", []):
        if wf.get("name") == name:
            return wf["id"]
    return None


def put_workflow(wf_id, wf):
    payload = {
        "name":        wf["name"],
        "nodes":       wf["nodes"],
        "connections": wf["connections"],
        "settings":    wf.get("settings", {}),
    }
    r = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{wf_id}",
        headers=n8n_headers(),
        json=payload,
    )
    if r.status_code == 200:
        return True
    print(f"  ❌ PUT failed: {r.status_code}  {r.text[:400]}")
    return False


def replace_in_code_nodes(wf, old, new, label=""):
    """Replace old → new in every Code node's jsCode.
    Returns (changed, already_ok) counts."""
    changed = already_ok = 0
    for node in wf.get("nodes", []):
        if node.get("type") == "n8n-nodes-base.code":
            code = node.get("parameters", {}).get("jsCode", "")
            if new in code:
                print(f"    ✓ Code node '{node.get('name', '?')}' already up-to-date {label}")
                already_ok += 1
            elif old in code:
                node["parameters"]["jsCode"] = code.replace(old, new)
                changed += 1
                print(f"    ✓ Updated Code node '{node.get('name', '?')}' {label}")
    return changed, already_ok


def update_n8n_workflow(wf_id, description):
    print(f"\nStep: Updating n8n workflow {wf_id} ({description})...")
    wf = get_workflow(wf_id)

    # Try both forms: plain emoji and JS unicode-escaped
    c1, ok1 = replace_in_code_nodes(wf, OLD_NAME, NEW_NAME, "(emoji form)")
    c2, ok2 = replace_in_code_nodes(wf, OLD_JS,   NEW_JS,   "(JS unicode form)")
    changed = c1 + c2
    already = ok1 + ok2

    if changed == 0 and already > 0:
        print(f"  ✅ Workflow {wf_id} already up-to-date — no changes needed")
        return
    if changed == 0 and already == 0:
        print("  ⚠️  No occurrences of old or new name found — check node manually")
        return

    if put_workflow(wf_id, wf):
        print(f"  ✅ Workflow {wf_id} updated ({changed} Code node(s) changed)")
    else:
        raise SystemExit(1)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # 1a. Rename Notion property
    rename_notion_property()

    # 1b. Daily Last Contact Date Sync
    update_n8n_workflow(DAILY_SYNC_WF, "Daily Last Contact Date Sync")

    # 1c. Log Customer Contact
    print(f"\nStep 1c: Finding '{LOG_CONTACT_WF_NAME}' workflow by name...")
    log_wf_id = find_workflow_id_by_name(LOG_CONTACT_WF_NAME)
    if not log_wf_id:
        print(f"  ⚠️  Workflow '{LOG_CONTACT_WF_NAME}' not found in n8n — not deployed yet.")
        print(f"     deploy_log_contact.py source has already been updated; skip API step.")
    else:
        print(f"  Found workflow ID: {log_wf_id}")
        update_n8n_workflow(log_wf_id, LOG_CONTACT_WF_NAME)

    print("\n" + "="*60)
    print("✅ All steps completed successfully!")
    print("="*60)
    print("\nNext steps:")
    print("  1. Open Notion MCT → confirm column is '📞 Last Contact Date 🔒'")
    print("  2. Trigger workflow veEIgePuCQ0z9jYr in n8n UI → verify HTTP 200 PATCHes")
    print("  3. Open Log Customer Contact in n8n UI → check Code node JS")
