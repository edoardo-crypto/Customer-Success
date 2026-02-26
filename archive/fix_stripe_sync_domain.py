#!/usr/bin/env python3
"""
fix_stripe_sync_domain.py

Adds 🏢 Domain to the 'Update Existing Row' HTTP Request node in the
Stripe → Notion Sync workflow (Ai9Y3FWjqMtEhr57), so future Stripe Sync
runs automatically keep Domain populated.

This is a focused alternative to setting RUN_N8N_UPDATE = True in
restore_domain.py — it only applies the n8n workflow patch, nothing else.

How it works
------------
1. GET the workflow and print all node names + the current Update body
2. Verify Domain is not already present
3. Insert the Domain field into the body JSON string
4. Deactivate → PUT → Activate

The domain expression reads from the 'Transform Active Subs' Code node,
which already extracts `domain` from the Stripe customer email.
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
BACKUP_PATH = "/tmp/workflow_backup_stripe_domain_fix.json"

n8n_headers = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

# n8n expression that reads the domain extracted by 'Transform Active Subs'
DOMAIN_EXPRESSION = "{{ $('Transform Active Subs').item.json.domain }}"

# The JSON snippet to inject into the body — note: leading comma to append a field
DOMAIN_SNIPPET = (
    ',"\U0001f3e2 Domain":{"rich_text":[{"text":{"content":"'
    + DOMAIN_EXPRESSION
    + '"}}]}'
)

sep = "=" * 64


def step(label):
    print(f"\n{sep}\n  {label}\n{sep}")


def check(r, label):
    if r.status_code in (200, 201):
        print(f"  \u2713 {label} \u2192 {r.status_code}")
    else:
        print(f"  \u2717 {label} \u2192 {r.status_code}")
        print(f"    {r.text[:600]}")
        sys.exit(1)


# ── Step 1: Load workflow ──────────────────────────────────────────────────────
step("1 · Load workflow from n8n")

r = requests.get(f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}", headers=n8n_headers)
check(r, "GET workflow")
workflow = r.json()

print(f"  Name   : {workflow['name']}")
print(f"  Nodes  : {len(workflow['nodes'])}")
print(f"  Active : {workflow.get('active', False)}")

# Save backup before any changes
with open(BACKUP_PATH, "w") as f:
    json.dump(workflow, f, indent=2)
print(f"  Backup : {BACKUP_PATH}")

print("\n  All nodes in this workflow:")
for node in workflow["nodes"]:
    print(f"    - {node.get('name', '?')!r}  [{node.get('type', '?')}]")


# ── Step 2: Find Update Existing Row node ─────────────────────────────────────
step("2 · Locate 'Update Existing Row' node")

update_node = next(
    (n for n in workflow["nodes"]
     if "Update" in n.get("name", "") and "Row" in n.get("name", "")),
    None,
)

if not update_node:
    print("  \u2717 Could not find a node containing 'Update' + 'Row' in its name.")
    print("  Check the node list above and update the search string in this script.")
    sys.exit(1)

print(f"  Found : {update_node['name']!r}")
params = update_node.get("parameters", {})
print(f"  Param keys: {list(params.keys())}")


# ── Step 3: Inspect and patch body ────────────────────────────────────────────
step("3 · Inspect body and inject 🏢 Domain")

body_key = next(
    (k for k in ("jsonBody", "body", "bodyParameters") if k in params),
    None,
)

if not body_key:
    print(f"  \u2717 No recognized body key found.")
    print(f"    Parameters: {json.dumps(params, indent=4, ensure_ascii=False)[:600]}")
    sys.exit(1)

body_str = params[body_key]
print(f"  Body key : {body_key!r}")
print(f"  Length   : {len(body_str)} chars")
print(f"\n  --- Body preview (first 400 chars) ---")
print(f"  {body_str[:400]}")
print(f"  --- End preview ---")

# Guard: already patched
if "\U0001f3e2 Domain" in body_str:
    print("\n  \U0001f3e2 Domain already present in the Update Existing Row body.")
    print("  Nothing to do. Exiting without changes.")
    sys.exit(0)

# The body should be a JSON object that ends with "}}" at the outermost level:
# {"properties": {"field1": {...}, ..., "fieldN": {...}}}
#                                                        ^^
# We insert the new domain field just before the closing "}}" so it becomes
# a sibling of the existing properties fields.
#
# Use rstrip() to ignore any trailing whitespace, then check the last 2 chars.
body_stripped = body_str.rstrip()
tail = body_stripped[-2:]

if tail != "}}":
    # Maybe there are extra closing braces — find the right insertion point
    # by looking for the last occurrence of "}" that closes the properties object.
    # Fallback: just show what we found and abort safely.
    print(f"\n  Warning: body does not end with '}}}}' — got {tail!r}")
    print(f"  Last 80 chars: {repr(body_stripped[-80:])}")
    print()
    print("  Cannot safely insert Domain. Options:")
    print("  1. Inspect the backup at", BACKUP_PATH)
    print("  2. Manually edit the Update Existing Row node body in n8n UI")
    print(f'  3. Add this snippet before the final closing braces:')
    print(f"     {DOMAIN_SNIPPET}")
    sys.exit(1)

# Safe: insert Domain before the final "}}" that closes {"properties": {... <here> }}
insert_pos   = len(body_stripped) - 2
updated_body = body_stripped[:insert_pos] + DOMAIN_SNIPPET + body_stripped[insert_pos:]
params[body_key] = updated_body

print(f"\n  Inserted Domain snippet at position {insert_pos}")
print(f"  Snippet: {DOMAIN_SNIPPET}")
print(f"\n  New body tail (last 120 chars):")
print(f"  ...{updated_body[-120:]}")


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


# ── Done ───────────────────────────────────────────────────────────────────────
print(f"\n{sep}")
print("  DONE — \U0001f3e2 Domain field added to Update Existing Row")
print(sep)
print()
print("  Verification steps:")
print("  1. Go to n8n UI → Stripe Sync workflow (Ai9Y3FWjqMtEhr57)")
print("     Click 'Execute Workflow' to trigger a sync run")
print("  2. Open a customer row in Notion MCT — 🏢 Domain should still be")
print("     populated (not overwritten with blank)")
print("  3. If something looks wrong, restore from backup:")
print(f"     python3 -c \"")
print(f"       import json, requests")
print(f"       wf = json.load(open('{BACKUP_PATH}'))")
print(f"       # PUT wf back via n8n API")
print(f"     \"")
print()
