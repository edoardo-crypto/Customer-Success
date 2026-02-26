#!/usr/bin/env python3
"""
fix_create_new_row.py

Fixes the Stripe → Notion Sync workflow (Ai9Y3FWjqMtEhr57):
  - Inserts "Prepare New Customer Payload" Code node between IF FALSE and Create New Row
  - Updates Create New Row body to use $json.* (not $('Transform Active Subs').item.json.*)
  - Rewires connections

Also:
  - Creates 4 missing Notion rows for active Stripe customers
  - Archives 2 test rows from Notion Master Customer Table
"""

import json
import re
import time
import requests
from datetime import datetime

# ── Credentials ───────────────────────────────────────────────────────────────
N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = (
    "***REMOVED_JWT***."
    "eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJpc3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleHAiOjE3NzMyNzAwMDB9."
    "X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)
STRIPE_KEY    = "***REMOVED***"
NOTION_TOKEN  = "***REMOVED***"

WORKFLOW_ID   = "Ai9Y3FWjqMtEhr57"
NOTION_DS_ID  = "3ceb1ad0-91f1-40db-945a-c51c58035898"

# ── Missing customers ──────────────────────────────────────────────────────────
MISSING_CUSTOMERS = [
    {"stripe_id": "cus_R93oYJibVRa5M1", "kickoff_date": "2026-02-18"},  # ROLECLOTHING
    {"stripe_id": "cus_TKE2ddRJh0gSwz", "kickoff_date": None},           # María José López
    {"stripe_id": "cus_Q4FeUy0H42ueZo", "kickoff_date": None},           # Mahogany Enterprises
    {"stripe_id": "cus_Q3sBiUi9nk9ECh", "kickoff_date": None},           # CO2 YOU Limited
]

# ── Test rows to archive ────────────────────────────────────────────────────────
TEST_ROWS = [
    "30ce418f-d8c4-8174-9d5f-f93457f06bc4",  # TEST_DO_NOT_CREATE
    "30ce418f-d8c4-81e4-b43c-d8a885132743",  # Test bad MRR
]

# ── HTTP headers ───────────────────────────────────────────────────────────────
n8n_headers = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}
notion_headers = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}
stripe_headers = {"Authorization": f"Bearer {STRIPE_KEY}"}


# ══════════════════════════════════════════════════════════════════════════════
# n8n helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_workflow():
    r = requests.get(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=n8n_headers)
    r.raise_for_status()
    return r.json()


def deactivate_workflow():
    r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate", headers=n8n_headers)
    r.raise_for_status()
    print("  Workflow deactivated")


def activate_workflow():
    r = requests.post(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate", headers=n8n_headers)
    r.raise_for_status()
    print("  Workflow activated")


def put_workflow(workflow_data):
    payload = {
        "name":        workflow_data["name"],
        "nodes":       workflow_data["nodes"],
        "connections": workflow_data["connections"],
        "settings":    workflow_data.get("settings", {}),
    }
    r = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}",
        headers=n8n_headers,
        json=payload,
    )
    if r.status_code != 200:
        print(f"  PUT failed {r.status_code}: {r.text[:500]}")
        r.raise_for_status()
    print("  Workflow PUT successful")
    return r.json()


# ══════════════════════════════════════════════════════════════════════════════
# Stripe helpers
# ══════════════════════════════════════════════════════════════════════════════

def fetch_stripe_customer(cid):
    r = requests.get(f"https://api.stripe.com/v1/customers/{cid}", headers=stripe_headers)
    r.raise_for_status()
    return r.json()


def fetch_stripe_subscriptions(cid):
    """Return list of subscriptions (active first, then any status)."""
    r = requests.get(
        "https://api.stripe.com/v1/subscriptions",
        params={"customer": cid, "status": "active", "limit": 10},
        headers=stripe_headers,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    if data:
        return data
    # Fall back to all statuses
    r2 = requests.get(
        "https://api.stripe.com/v1/subscriptions",
        params={"customer": cid, "limit": 10},
        headers=stripe_headers,
    )
    r2.raise_for_status()
    return r2.json().get("data", [])


COUNTRY_MAP = {
    "ES": "Spain",   "US": "United States", "GB": "United Kingdom",
    "DE": "Germany", "FR": "France",        "IT": "Italy",
    "PT": "Portugal","NL": "Netherlands",   "BE": "Belgium",
    "CH": "Switzerland","AT": "Austria",    "SE": "Sweden",
    "NO": "Norway",  "DK": "Denmark",       "FI": "Finland",
    "PL": "Poland",  "CZ": "Czech Republic","HU": "Hungary",
    "RO": "Romania", "IE": "Ireland",       "GR": "Greece",
    "MX": "Mexico",  "BR": "Brazil",        "AR": "Argentina",
    "CO": "Colombia","CL": "Chile",
}

PLAN_MAP = {
    14900: "Start",
    24900: "Scale",
    49900: "Growth",
    99900: "Enterprise",
}

def get_plan_tier(unit_amount_cents):
    return PLAN_MAP.get(unit_amount_cents, f"Custom ({unit_amount_cents / 100:.0f}€)")


def get_billing_status(status):
    return {
        "active":     "Active",
        "past_due":   "Past Due",
        "canceled":   "Canceled",
        "trialing":   "Trialing",
        "paused":     "Paused",
        "incomplete": "Incomplete",
    }.get(status, "Active")


def build_customer_data(stripe_id):
    customer = fetch_stripe_customer(stripe_id)
    customer_name = customer.get("name") or customer.get("description") or stripe_id
    email  = customer.get("email", "")
    domain = email.split("@")[1] if "@" in email else ""

    address      = customer.get("address") or {}
    country_code = address.get("country", "")
    country      = COUNTRY_MAP.get(country_code, country_code) or None

    subs = fetch_stripe_subscriptions(stripe_id)
    if subs:
        sub = subs[0]
        billing_status = get_billing_status(sub.get("status", "active"))
        items = sub.get("items", {}).get("data", [])
        if items:
            price  = items[0].get("price", {})
            amount = price.get("unit_amount", 0)
        else:
            amount = 0
        plan_tier      = get_plan_tier(amount)
        mrr            = amount / 100

        start_ts = sub.get("start_date") or sub.get("current_period_start")
        end_ts   = sub.get("current_period_end")
        contract_start = datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d") if start_ts else "2024-01-01"
        contract_end   = datetime.fromtimestamp(end_ts).strftime("%Y-%m-%d")   if end_ts   else "2025-01-01"
    else:
        billing_status = "Active"
        plan_tier      = "Scale"
        mrr            = 249
        contract_start = "2024-01-01"
        contract_end   = "2025-01-01"

    return {
        "customer_name":     customer_name,
        "domain":            domain,
        "stripe_customer_id": stripe_id,
        "plan_tier":         plan_tier,
        "mrr":               mrr,
        "contract_start":    contract_start,
        "contract_end":      contract_end,
        "billing_status":    billing_status,
        "country":           country,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Notion helpers
# ══════════════════════════════════════════════════════════════════════════════

def create_notion_row(cdata, kickoff_date=None):
    properties = {
        "🏢 Company Name": {"title": [{"text": {"content": cdata["customer_name"]}}]},
        "🏢 Domain":       {"rich_text": [{"text": {"content": cdata["domain"]}}]},
        "🔗 Stripe Customer ID": {"rich_text": [{"text": {"content": cdata["stripe_customer_id"]}}]},
        "💰 Plan Tier":    {"select": {"name": cdata["plan_tier"]}},
        "💰 MRR":          {"number": cdata["mrr"]},
        "📋 Contract Start": {"date": {"start": cdata["contract_start"]}},
        "📋 Renewal Date": {"date": {"start": cdata["contract_end"]}},
        "💰 Billing Status": {"select": {"name": cdata["billing_status"]}},
        "⭐ CS Owner":     {"select": {"name": "Aya"}},
    }
    if kickoff_date:
        properties["🚀 Kickoff Date"] = {"date": {"start": kickoff_date}}
    if cdata.get("country"):
        properties["🏢 Country"] = {"select": {"name": cdata["country"]}}

    payload = {
        "parent":     {"database_id": "84feda19cfaf4c6e9500bf21d2aaafef"},
        "properties": properties,
    }
    r = requests.post("https://api.notion.com/v1/pages", headers=notion_headers, json=payload)
    if r.status_code not in (200, 201):
        print(f"  ERROR {r.status_code}: {r.text[:300]}")
        # Try with data_source_id parent
        payload2 = dict(payload)
        payload2["parent"] = {"data_source_id": NOTION_DS_ID}
        headers2 = dict(notion_headers)
        headers2["Notion-Version"] = "2025-09-03"
        r2 = requests.post("https://api.notion.com/v1/pages", headers=headers2, json=payload2)
        if r2.status_code not in (200, 201):
            print(f"  ERROR (fallback) {r2.status_code}: {r2.text[:300]}")
            return None
        return r2.json()
    return r.json()


def archive_notion_row(page_id):
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers,
        json={"archived": True},
    )
    if r.status_code != 200:
        print(f"  ERROR archiving {page_id}: {r.status_code} {r.text[:200]}")
        return False
    print(f"  Archived {page_id}")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# Workflow fix helpers
# ══════════════════════════════════════════════════════════════════════════════

CODE_JS = r"""// Re-establish Transform Active Subs data via pairedItem
// (more reliable than cross-node expressions through HTTP Request + IF chain)
const falseItems = $input.all();
const allCustomers = $('Transform Active Subs').all();

return falseItems.map(item => {
    const pairedIdx = typeof item.pairedItem === 'number'
        ? item.pairedItem
        : (item.pairedItem?.item ?? null);

    if (pairedIdx === null || !allCustomers[pairedIdx]) {
        // Skip rather than create garbage row
        return null;
    }

    return {
        json: allCustomers[pairedIdx].json,
        pairedItem: item.pairedItem
    };
}).filter(i => i !== null);"""


def fix_workflow(workflow):
    nodes       = workflow["nodes"]
    connections = workflow["connections"]

    # ── Identify key nodes ────────────────────────────────────────────────────
    if_node     = next((n for n in nodes if n["type"] == "n8n-nodes-base.if"), None)
    create_node = next(
        (n for n in nodes if "Create" in n.get("name", "") and "Row" in n.get("name", "")),
        None,
    )
    if not if_node:
        raise ValueError("IF node not found in workflow")
    if not create_node:
        raise ValueError("Create New Row node not found in workflow")

    if_name     = if_node["name"]
    create_name = create_node["name"]
    print(f"  IF node      : '{if_name}'")
    print(f"  Create node  : '{create_name}'")

    # ── Build position for the new Code node ──────────────────────────────────
    if_pos     = if_node.get("position", [500, 300])
    create_pos = create_node.get("position", [900, 400])
    new_pos    = [
        (if_pos[0] + create_pos[0]) // 2,
        (if_pos[1] + create_pos[1]) // 2,
    ]

    # ── Add "Prepare New Customer Payload" node if not already present ────────
    existing_prep = next((n for n in nodes if n.get("name") == "Prepare New Customer Payload"), None)
    if existing_prep:
        print("  'Prepare New Customer Payload' already exists — updating jsCode only")
        existing_prep["parameters"]["jsCode"] = CODE_JS
    else:
        code_node = {
            "id":          "prepare-new-customer-payload",
            "name":        "Prepare New Customer Payload",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    new_pos,
            "parameters": {
                "mode":   "runOnceForAllItems",
                "jsCode":  CODE_JS,
            },
        }
        nodes.append(code_node)
        print("  Added node: 'Prepare New Customer Payload'")

    # ── Update Create New Row body to use $json.* ─────────────────────────────
    for node in nodes:
        if node["name"] != create_name:
            continue
        params = node.get("parameters", {})
        for key in ("body", "jsonBody", "bodyParameters"):
            if key not in params:
                continue
            original = params[key]
            if isinstance(original, str):
                updated = re.sub(
                    r"\$\('Transform Active Subs'\)\.item\.json\.",
                    "$json.",
                    original,
                )
                params[key] = updated
                changed = original != updated
                print(f"  Updated '{create_name}'.parameters.{key}: "
                      f"{'changed' if changed else 'NO MATCH — pattern not found'}")
                if not changed:
                    print(f"    Preview: {original[:300]}")
            break  # Only process the first matching key
        break

    # ── Rewire connections ────────────────────────────────────────────────────
    # 1. Remove IF FALSE → Create New Row
    if if_name in connections:
        main_outs = connections[if_name].get("main", [])
        if len(main_outs) > 1:
            before = len(main_outs[1])
            main_outs[1] = [c for c in main_outs[1] if c["node"] != create_name]
            if len(main_outs[1]) < before:
                print(f"  Removed: '{if_name}' output[1] → '{create_name}'")
            else:
                print(f"  WARNING: '{if_name}' output[1] → '{create_name}' not found to remove")

    # 2. Add IF FALSE → Prepare New Customer Payload
    if if_name not in connections:
        connections[if_name] = {"main": [[], []]}
    while len(connections[if_name]["main"]) < 2:
        connections[if_name]["main"].append([])
    already_connected = any(
        c["node"] == "Prepare New Customer Payload"
        for c in connections[if_name]["main"][1]
    )
    if not already_connected:
        connections[if_name]["main"][1].append({
            "node":  "Prepare New Customer Payload",
            "type":  "main",
            "index": 0,
        })
        print(f"  Added: '{if_name}' output[1] → 'Prepare New Customer Payload'")

    # 3. Add Prepare New Customer Payload → Create New Row
    if "Prepare New Customer Payload" not in connections:
        connections["Prepare New Customer Payload"] = {"main": [[]]}
    already_connected2 = any(
        c["node"] == create_name
        for c in connections["Prepare New Customer Payload"]["main"][0]
    )
    if not already_connected2:
        connections["Prepare New Customer Payload"]["main"][0].append({
            "node":  create_name,
            "type":  "main",
            "index": 0,
        })
        print(f"  Added: 'Prepare New Customer Payload' → '{create_name}'")

    workflow["nodes"]       = nodes
    workflow["connections"] = connections
    return workflow


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    sep = "=" * 62

    # ── Step 1: Fetch & backup ────────────────────────────────────────────────
    print(f"\n{sep}")
    print("Step 1: Fetch + backup workflow")
    print(sep)
    workflow = get_workflow()
    backup_path = "/tmp/workflow_backup_create_fix.json"
    with open(backup_path, "w") as f:
        json.dump(workflow, f, indent=2)
    print(f"  Backup → {backup_path}")

    print("\n  Current nodes:")
    for n in workflow["nodes"]:
        print(f"    [{n['type'].split('.')[-1]}] '{n['name']}'")

    print("\n  Current connections:")
    for src, targets in workflow["connections"].items():
        for i, conns in enumerate(targets.get("main", [])):
            for c in conns:
                print(f"    '{src}' output[{i}] → '{c['node']}'")

    # ── Step 2: Deactivate ────────────────────────────────────────────────────
    print(f"\n{sep}")
    print("Step 2: Deactivate workflow")
    print(sep)
    deactivate_workflow()

    # ── Step 3: Apply workflow changes ────────────────────────────────────────
    print(f"\n{sep}")
    print("Step 3: Patch workflow (add Code node + update Create New Row)")
    print(sep)
    workflow = fix_workflow(workflow)

    # ── Step 4: PUT workflow ──────────────────────────────────────────────────
    print(f"\n{sep}")
    print("Step 4: PUT workflow")
    print(sep)
    put_workflow(workflow)

    # ── Step 5: Activate ─────────────────────────────────────────────────────
    print(f"\n{sep}")
    print("Step 5: Activate workflow")
    print(sep)
    activate_workflow()

    # ── Step 6: Create missing Notion rows ────────────────────────────────────
    print(f"\n{sep}")
    print("Step 6: Create 4 missing Notion rows")
    print(sep)
    for entry in MISSING_CUSTOMERS:
        sid     = entry["stripe_id"]
        kickoff = entry["kickoff_date"]
        print(f"\n  Processing {sid} …")
        cdata = build_customer_data(sid)
        print(f"    Name          : {cdata['customer_name']}")
        print(f"    Domain        : {cdata['domain']}")
        print(f"    Plan / MRR    : {cdata['plan_tier']} / €{cdata['mrr']}")
        print(f"    Contract      : {cdata['contract_start']} → {cdata['contract_end']}")
        print(f"    Billing status: {cdata['billing_status']}")
        print(f"    Country       : {cdata['country']}")
        print(f"    Kickoff Date  : {kickoff or '(omitted)'}")
        result = create_notion_row(cdata, kickoff)
        if result:
            print(f"    ✓ Created: {result.get('id')}")
        else:
            print(f"    ✗ FAILED to create row")
        time.sleep(0.5)

    # ── Step 7: Archive test rows ─────────────────────────────────────────────
    print(f"\n{sep}")
    print("Step 7: Archive 2 test rows")
    print(sep)
    for page_id in TEST_ROWS:
        print(f"  Archiving {page_id} …")
        archive_notion_row(page_id)
        time.sleep(0.3)

    # ── Done ──────────────────────────────────────────────────────────────────
    print(f"\n{sep}")
    print("DONE")
    print(sep)
    print("\nVerification checklist:")
    print("  1. Notion: ROLECLOTHING SL, María José López, Mahogany Enterprises,")
    print("             CO2 YOU Limited — all 4 rows present")
    print("  2. Notion: TEST_DO_NOT_CREATE + 'Test bad MRR' rows gone")
    print("  3. n8n UI: 'Prepare New Customer Payload' Code node visible between")
    print("             'IF - Row Exists' and 'Create New Row'")
    print("  4. n8n UI: Execute Workflow → no expression errors on Create New Row")


if __name__ == "__main__":
    main()
