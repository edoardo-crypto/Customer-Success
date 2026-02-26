#!/usr/bin/env python3
"""
fix_stripe_sync_paireditem.py

Two-part fix:
1. Patch the Stripe→Notion Sync workflow (Ai9Y3FWjqMtEhr57) so new customers
   are never silently dropped:
   - Enable includeInputFields:true on 'Notion Query by Stripe ID' so customer
     fields (stripe_customer_id, name, domain, mrr, etc.) are merged directly
     into the response item — no pairedItem index lookup needed.
   - Simplify 'Prepare New Customer Payload' to read those merged fields
     straight from $input.all(), eliminating the pairedItem null-crash.

2. Backfill the missing Baymo row in Notion MCT.
"""

import json
import sys
import requests

# ── Config ─────────────────────────────────────────────────────────────────────
N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = (
    "***REMOVED_JWT***."
    "eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJpc3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleHAiOjE3NzMyNzAwMDB9."
    "X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)
WORKFLOW_ID = "Ai9Y3FWjqMtEhr57"
BACKUP_PATH = "/tmp/workflow_backup_stripe_paireditem_fix.json"

NOTION_TOKEN   = "***REMOVED***"
NOTION_DS_ID   = "3ceb1ad0-91f1-40db-945a-c51c58035898"
NOTION_VERSION = "2025-09-03"

n8n_headers = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}
notion_headers = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": NOTION_VERSION,
}

sep = "=" * 64


def step(label):
    print(f"\n{sep}\n  {label}\n{sep}")


def check(r, label):
    if r.status_code in (200, 201):
        print(f"  \u2713 {label} \u2192 {r.status_code}")
    else:
        print(f"  \u2717 {label} \u2192 {r.status_code}")
        print(f"    {r.text[:800]}")
        sys.exit(1)


# ── Step 1: Load workflow ──────────────────────────────────────────────────────
step("1 · Load workflow from n8n")

r = requests.get(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=n8n_headers)
check(r, "GET workflow")
workflow = r.json()

print(f"  Name   : {workflow['name']}")
print(f"  Nodes  : {len(workflow['nodes'])}")
print(f"  Active : {workflow.get('active', False)}")

with open(BACKUP_PATH, "w") as f:
    json.dump(workflow, f, indent=2)
print(f"  Backup : {BACKUP_PATH}")

print("\n  All nodes:")
for n in workflow["nodes"]:
    print(f"    - {n.get('name', '?')!r}  [{n.get('type', '?')}]")


# ── Step 2: Patch 'Notion Query by Stripe ID' ─────────────────────────────────
step("2 · Patch 'Notion Query by Stripe ID' — enable includeInputFields")

query_node = next(
    (n for n in workflow["nodes"] if "Notion Query" in n.get("name", "")),
    None,
)
if not query_node:
    print("  \u2717 Could not find 'Notion Query by Stripe ID' node.")
    print("  Check the node list above and adjust the search string.")
    sys.exit(1)

print(f"  Found node: {query_node['name']!r}")
params = query_node.setdefault("parameters", {})
options = params.setdefault("options", {})

if options.get("includeInputFields") is True:
    print("  includeInputFields already True — no change needed for this node.")
else:
    options["includeInputFields"] = True
    print("  Set options.includeInputFields = true")

print(f"  options now: {json.dumps(options)}")


# ── Step 3: Patch 'Prepare New Customer Payload' ──────────────────────────────
step("3 · Patch 'Prepare New Customer Payload' — remove pairedItem dependency")

prepare_node = next(
    (n for n in workflow["nodes"] if "Prepare New Customer Payload" in n.get("name", "")),
    None,
)
if not prepare_node:
    print("  \u2717 Could not find 'Prepare New Customer Payload' node.")
    print("  Check the node list above and adjust the search string.")
    sys.exit(1)

print(f"  Found node: {prepare_node['name']!r}")
print(f"  Current jsCode (first 300 chars):")
current_code = prepare_node.get("parameters", {}).get("jsCode", "")
print(f"    {current_code[:300]}")

NEW_JS_CODE = """\
// Customer data is now available directly on each item
// (includeInputFields=true on Notion Query merges input fields into the response)
return $input.all()
    .filter(item => item.json.stripe_customer_id)   // safety: skip any item without ID
    .map(item => ({ json: item.json }));
"""

prepare_node.setdefault("parameters", {})["jsCode"] = NEW_JS_CODE
print(f"\n  Replaced jsCode with simplified version (no pairedItem lookups)")


# ── Step 4: Deactivate → PUT → Activate ───────────────────────────────────────
step("4 · Deactivate workflow")
r = requests.post(
    f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate",
    headers=n8n_headers,
)
check(r, "Deactivate")

step("5 · PUT updated workflow")
put_payload = {
    "name":        workflow["name"],
    "nodes":       workflow["nodes"],
    "connections": workflow["connections"],
    "settings":    workflow.get("settings", {}),
}
r = requests.put(
    f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}",
    headers=n8n_headers,
    json=put_payload,
)
check(r, "PUT workflow")

step("6 · Reactivate workflow")
r = requests.post(
    f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate",
    headers=n8n_headers,
)
check(r, "Activate")


# ── Step 7: Backfill Baymo row in Notion MCT ──────────────────────────────────
step("7 · Create Baymo row in Notion MCT")

baymo_payload = {
    "parent": {"data_source_id": NOTION_DS_ID},
    "properties": {
        "\U0001f3e2 Company Name":       {"title": [{"text": {"content": "BAYMO THE LABEL"}}]},
        "\U0001f517 Stripe Customer ID": {"rich_text": [{"text": {"content": "cus_U1y5I3iTb5CMRk"}}]},
        "\U0001f3e2 Domain":             {"rich_text": [{"text": {"content": "baymo.com"}}]},
        "\U0001f4b0 Plan Tier":          {"select": {"name": "Scale"}},
        "\U0001f4b0 MRR":                {"number": 249},
        "\U0001f4b0 Billing Status":     {"select": {"name": "Active"}},
        "\U0001f4cb Contract Start":     {"date": {"start": "2026-02-23"}},
        "\U0001f4cb Renewal Date":       {"date": {"start": "2026-03-23"}},
        "\U0001f680 Kickoff Date":       {"date": {"start": "2026-02-24"}},
        "\u2b50 CS Owner":               {"select": {"name": "Aya"}},
    },
}

r = requests.post(
    "https://api.notion.com/v1/pages",
    headers=notion_headers,
    json=baymo_payload,
)
check(r, "Create Baymo page in Notion MCT")

page = r.json()
print(f"  Page ID  : {page.get('id')}")
print(f"  Page URL : {page.get('url')}")


# ── Done ───────────────────────────────────────────────────────────────────────
print(f"\n{sep}")
print("  ALL DONE")
print(sep)
print()
print("  What was changed:")
print("  1. 'Notion Query by Stripe ID' — includeInputFields=true")
print("     (customer data flows directly into each item, no pairedItem needed)")
print("  2. 'Prepare New Customer Payload' — simplified to $input.all() map")
print("     (new customers will no longer be silently dropped)")
print("  3. Baymo row created in Notion MCT")
print()
print("  Next steps:")
print("  - Open Notion MCT and verify BAYMO THE LABEL appears with correct fields")
print("  - In n8n UI, manually trigger 'Stripe Sync' to confirm new logic works")
print(f"  - Backup saved to: {BACKUP_PATH}")
print()
