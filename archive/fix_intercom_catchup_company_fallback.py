#!/usr/bin/env python3
"""
fix_intercom_catchup_company_fallback.py — Add company-name fallback to Intercom catchup

Modifies workflow J1l8oI22H26f9iM5 (Intercom Catch-up Polling) to search MCT by
company name when the email-domain lookup returns zero results.  This helps when
customers write from personal email addresses (gmail, etc.) — Intercom still knows
the company they belong to even without a work email.

Changes (11 → 13 nodes):
  - Rename "Notion: Find Customer"      → "Notion: Find Customer by Domain"
  - Insert "Notion: Find Customer by Company"  (HTTP Request, MCT title search)
  - Insert "Merge: Customer Result"             (Code node — domain wins > company)
  - Shift "Build Notion Payload" and "Notion: Create Issue" positions right by 500 px

The "Build Notion Payload" node is UNCHANGED — its $input.all() now comes from
"Merge: Customer Result" which outputs the same { results: [...] } format.
"""

import json
import uuid
import time
import requests
import sys
import creds

# ── Config ───────────────────────────────────────────────────────────────────
N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
NOTION_TOKEN = creds.get("NOTION_TOKEN")
NOTION_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"   # MCT data source

WORKFLOW_ID = "J1l8oI22H26f9iM5"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

# ── Node name constants ───────────────────────────────────────────────────────
OLD_CUSTOMER_NODE  = "Notion: Find Customer"
NEW_DOMAIN_NODE    = "Notion: Find Customer by Domain"
NEW_COMPANY_NODE   = "Notion: Find Customer by Company"
MERGE_NODE         = "Merge: Customer Result"
BUILD_PAYLOAD_NODE = "Build Notion Payload"
CREATE_ISSUE_NODE  = "Notion: Create Issue"
EXTRACT_TEXT_NODE  = "Extract Conv Text"

# ── Merge Code node JS ────────────────────────────────────────────────────────
# This node sits between the two customer-lookup HTTP nodes and "Build Notion Payload".
# It outputs { results: [...] } — same shape as a raw Notion query — so that
# "Build Notion Payload" (which does customerJson.results) requires no changes.
MERGE_CUSTOMER_JS = """\
// Merge domain-based and company-name-based MCT lookup results.
// Domain match takes priority; company name is the fallback.
// Outputs { results: [...] } — same shape as a Notion query response —
// so "Build Notion Payload" works without any modification.

const domainItems  = $('Notion: Find Customer by Domain').all();
const companyItems = $input.all();   // from: Notion: Find Customer by Company
const convItems    = $('Extract Conv Text').all();

const results = [];

for (let i = 0; i < convItems.length; i++) {
    const domainJson  = (domainItems[i]  && domainItems[i].json)  || {};
    const companyJson = (companyItems[i] && companyItems[i].json) || {};

    const domainResults  = domainJson.results  || [];
    const companyResults = companyJson.results || [];

    // Domain match wins; fall back to company name match
    const customerResults = domainResults.length > 0 ? domainResults : companyResults;

    const convData = (convItems[i] && convItems[i].json) || {};
    const convId   = convData.conversation_id || '(unknown)';

    if (domainResults.length > 0) {
        console.log(`[catchup] ${convId} — customer matched via domain`);
    } else if (companyResults.length > 0) {
        console.log(`[catchup] ${convId} — customer matched via company name`);
    } else {
        console.log(`[catchup] ${convId} — no customer match`);
    }

    results.push({ json: { results: customerResults } });
}

return results;
"""


def uid():
    return str(uuid.uuid4())


# ── n8n API helpers ──────────────────────────────────────────────────────────

def get_workflow():
    r = requests.get(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}",
        headers=N8N_HEADERS,
    )
    r.raise_for_status()
    return r.json()


def put_workflow(wf):
    """PUT updated workflow — only name/nodes/connections/settings are accepted."""
    payload = {
        "name":        wf["name"],
        "nodes":       wf["nodes"],
        "connections": wf["connections"],
        "settings":    wf.get("settings", {}),
    }
    r = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}",
        headers=N8N_HEADERS,
        json=payload,
    )
    if r.status_code not in (200, 201):
        print(f"  PUT failed: {r.status_code}")
        print(f"  Response: {r.text[:600]}")
        r.raise_for_status()
    return r.json()


def deactivate():
    r = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate",
        headers=N8N_HEADERS,
    )
    if r.status_code in (200, 204):
        print("  Deactivated")
    else:
        print(f"  Deactivate returned {r.status_code} (may already be inactive)")


def activate():
    r = requests.post(
        f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate",
        headers=N8N_HEADERS,
    )
    if r.status_code in (200, 204):
        print("  Activated")
    else:
        print(f"  Activate returned {r.status_code}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("fix_intercom_catchup_company_fallback.py")
    print(f"Workflow: {WORKFLOW_ID}")
    print("=" * 65)

    # 1. Fetch current workflow
    print("\n[1/5] Fetching current workflow...")
    wf = get_workflow()
    print(f"  Name:   {wf['name']}")
    print(f"  Nodes:  {len(wf['nodes'])}")
    print(f"  Active: {wf.get('active')}")

    nodes       = wf["nodes"]
    connections = wf["connections"]
    node_by_name = {n["name"]: n for n in nodes}

    # 2. Safety checks
    print("\n[2/5] Validating node structure...")

    for required in [OLD_CUSTOMER_NODE, BUILD_PAYLOAD_NODE, CREATE_ISSUE_NODE, EXTRACT_TEXT_NODE]:
        if required not in node_by_name:
            print(f"  ERROR: Expected node '{required}' not found.")
            print(f"  Present nodes: {list(node_by_name.keys())}")
            sys.exit(1)
        print(f"  ✓ {required!r}")

    # Guard: don't double-patch
    if NEW_DOMAIN_NODE in node_by_name:
        print(f"\n  '{NEW_DOMAIN_NODE}' already exists — looks like this was already applied.")
        print("  Aborting to avoid a double-patch.")
        sys.exit(0)

    if NEW_COMPANY_NODE in node_by_name or MERGE_NODE in node_by_name:
        print(f"\n  New fallback nodes already exist — looks like this was already applied.")
        print("  Aborting.")
        sys.exit(0)

    # 3. Deactivate
    print("\n[3/5] Deactivating workflow...")
    deactivate()
    time.sleep(1)

    # 4. Build the modified node list
    print("\n[4/5] Building modified node list...")

    # Rename existing domain-lookup node
    domain_node = node_by_name[OLD_CUSTOMER_NODE]
    domain_node["name"] = NEW_DOMAIN_NODE

    # Read positions to place new nodes after the domain node
    cx, cy = domain_node["position"]

    # Shift "Build Notion Payload" and "Notion: Create Issue" right by +500 px
    # so the two new nodes fit between them visually.
    build_node  = node_by_name[BUILD_PAYLOAD_NODE]
    create_node = node_by_name[CREATE_ISSUE_NODE]
    build_node["position"]  = [build_node["position"][0]  + 500, build_node["position"][1]]
    create_node["position"] = [create_node["position"][0] + 500, create_node["position"][1]]

    # New node: "Notion: Find Customer by Company"
    # Searches MCT by title (customer name) — fires for every item but is
    # a no-op when company_name is empty/short (placeholder ensures no match).
    company_lookup_body = (
        '={{ JSON.stringify({'
        '"filter": {'
        '"property": "title",'
        '"title": {"contains": ($json.company_name && $json.company_name.length >= 4) '
        '? $json.company_name : "SKIP_NO_MATCH__zz99"'
        '}'
        '}'
        '}) }}'
    )
    company_node = {
        "id":          uid(),
        "name":        NEW_COMPANY_NODE,
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [cx + 250, cy],
        "parameters": {
            "method": "POST",
            "url":    f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
            "sendHeaders": True,
            "headerParameters": {
                "parameters": [
                    {"name": "Authorization",   "value": f"Bearer {NOTION_TOKEN}"},
                    {"name": "Notion-Version",  "value": "2025-09-03"},
                ]
            },
            "sendBody":    True,
            "specifyBody": "json",
            "jsonBody":    company_lookup_body,
            "options":     {"continueOnFail": True},
        },
    }

    # New node: "Merge: Customer Result"
    merge_node = {
        "id":          uid(),
        "name":        MERGE_NODE,
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [cx + 500, cy],
        "parameters": {
            "mode":   "runOnceForAllItems",
            "jsCode": MERGE_CUSTOMER_JS,
        },
    }

    nodes.append(company_node)
    nodes.append(merge_node)

    # 5. Rebuild connections
    print("\n[5a/5] Rebuilding connections...")

    new_connections = {}

    for src_name, conn_data in connections.items():
        # Rename old domain-node key
        new_src = NEW_DOMAIN_NODE if src_name == OLD_CUSTOMER_NODE else src_name

        # Rename old domain-node in destination edges
        new_main = []
        for branch in conn_data.get("main", []):
            new_branch = []
            for edge in branch:
                if edge.get("node") == OLD_CUSTOMER_NODE:
                    edge = {**edge, "node": NEW_DOMAIN_NODE}
                new_branch.append(edge)
            new_main.append(new_branch)

        new_connections[new_src] = {"main": new_main}

    # Insert the new chain:
    #   [Domain] → [Company] → [Merge] → [Build Payload]
    # (this overwrites whatever was set for NEW_DOMAIN_NODE by the loop above)
    new_connections[NEW_DOMAIN_NODE] = {
        "main": [[{"node": NEW_COMPANY_NODE, "type": "main", "index": 0}]]
    }
    new_connections[NEW_COMPANY_NODE] = {
        "main": [[{"node": MERGE_NODE, "type": "main", "index": 0}]]
    }
    new_connections[MERGE_NODE] = {
        "main": [[{"node": BUILD_PAYLOAD_NODE, "type": "main", "index": 0}]]
    }

    wf["nodes"]       = nodes
    wf["connections"] = new_connections

    # Print summary
    print(f"\nFinal node list ({len(nodes)} nodes):")
    for i, n in enumerate(nodes, 1):
        marker = " ← NEW" if n["name"] in (NEW_COMPANY_NODE, MERGE_NODE) else \
                 " ← RENAMED" if n["name"] == NEW_DOMAIN_NODE else ""
        print(f"  {i:2}. {n['name']}{marker}")

    # 6. PUT updated workflow
    print("\n[5b/5] Pushing to n8n...")
    result = put_workflow(wf)
    got_nodes = len(result.get("nodes", []))
    print(f"  PUT OK — {got_nodes} nodes confirmed")

    # Re-activate
    print("\nReactivating workflow...")
    time.sleep(1)
    activate()

    print()
    print("=" * 65)
    print("Done! Company-name fallback added to Intercom catchup workflow.")
    print("=" * 65)
    print()
    print("What changed:")
    print("  1. 'Notion: Find Customer' renamed to 'Notion: Find Customer by Domain'")
    print("  2. New: 'Notion: Find Customer by Company' — searches MCT by title")
    print("     when company_name is >= 4 chars; fires for every item but is a")
    print("     no-op for empty/short names (uses a placeholder that won't match)")
    print("  3. New: 'Merge: Customer Result' — domain match wins; if no domain")
    print("     match, passes company-name match through to Build Notion Payload")
    print("  4. 'Build Notion Payload' is UNCHANGED (still reads $input.all().results)")
    print()
    print("IMPORTANT: Toggle INACTIVE → ACTIVE in the n8n UI to re-register")
    print("the schedule trigger after any PUT.")
    print(f"  → {N8N_BASE}/workflow/{WORKFLOW_ID}")


if __name__ == "__main__":
    main()
