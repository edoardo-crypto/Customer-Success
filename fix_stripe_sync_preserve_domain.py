#!/usr/bin/env python3
"""
fix_stripe_sync_preserve_domain.py

Patches the Stripe Sync workflow (Ai9Y3FWjqMtEhr57) so that manually-set
business domains are NOT overwritten on each Monday sync run.

Root cause:
  The 'Update Existing Row' node writes:
    "🏢 Domain": {{ $('Transform Active Subs').item.json.domain }}
  This always uses the Stripe email domain (gmail.com for 7 customers)
  → any manual domain fix gets silently reverted on the next Monday run.

Fix:
  Replace the bare Stripe domain expression with a guard IIFE that:
  - Reads the CURRENT domain from the Notion query result ($json.results[0])
  - If it's already a real business domain (not a generic provider) → keep it
  - If it's gmail.com / empty / other generic → fall back to Stripe email domain

This way:
  - New customers onboarding still work (no existing domain → Stripe domain set)
  - Manually corrected domains (cuchy.es, dukefotografia.com, etc.) are preserved
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
BACKUP_PATH = "/tmp/workflow_backup_stripe_preserve_domain.json"

n8n_headers = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

# ── Old expression (what we're replacing) ─────────────────────────────────────
OLD_EXPR = "{{ $('Transform Active Subs').item.json.domain }}"

# ── New expression: guard IIFE that preserves non-generic domains ──────────────
# Single line to embed cleanly in the JSON body string.
# NO }} inside the JS body — })() ends with ) not } — safe for n8n template parser.
NEW_EXPR = (
    "={{ (() => {"
    " const cur = ($json.results || [])[0]"
    "?.properties?.['🏢 Domain']"
    "?.rich_text?.[0]?.plain_text || '';"
    " const GENERIC = ['gmail.com','hotmail.com','yahoo.com','outlook.com',"
    "'icloud.com','protonmail.com','live.com','me.com','msn.com','aol.com'];"
    " return (cur && !GENERIC.includes(cur))"
    " ? cur"
    " : $('Transform Active Subs').item.json.domain;"
    " })() }}"
)

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
for node in workflow["nodes"]:
    print(f"    - {node.get('name', '?')!r}  [{node.get('type', '?')}]")


# ── Step 2: Find 'Update Existing Row' node ────────────────────────────────────
step("2 · Locate 'Update Existing Row' node")

update_node = next(
    (n for n in workflow["nodes"]
     if "Update" in n.get("name", "") and "Row" in n.get("name", "")),
    None,
)

if not update_node:
    print("  \u2717 Could not find node containing 'Update' + 'Row'.")
    print("  Check node list above and update search string in this script.")
    sys.exit(1)

print(f"  Found : {update_node['name']!r}")
params = update_node.get("parameters", {})
print(f"  Param keys: {list(params.keys())}")


# ── Step 3: Locate body key ────────────────────────────────────────────────────
step("3 · Locate body key in node parameters")

body_key = next(
    (k for k in ("jsonBody", "body", "bodyParameters") if k in params),
    None,
)

if not body_key:
    print("  \u2717 No recognized body key (jsonBody / body / bodyParameters).")
    print(f"    Parameters: {json.dumps(params, indent=4, ensure_ascii=False)[:800]}")
    sys.exit(1)

body_str = params[body_key]
print(f"  Body key : {body_key!r}")
print(f"  Length   : {len(body_str)} chars")
print(f"\n  --- Body preview (first 500 chars) ---")
print(f"  {body_str[:500]}")
print(f"  --- End preview ---")


# ── Step 4: Assert OLD expression is present ──────────────────────────────────
step("4 · Verify OLD domain expression is present")

if OLD_EXPR not in body_str:
    # Maybe already patched?
    if "GENERIC" in body_str and "rich_text" in body_str:
        print("  Body appears to already contain the guard expression.")
        print("  Nothing to do. Exiting without changes.")
        sys.exit(0)
    print(f"  \u2717 OLD expression not found in body:")
    print(f"    Expected: {OLD_EXPR!r}")
    print(f"\n  Tip: print the full body from the backup at {BACKUP_PATH}")
    sys.exit(1)

print(f"  \u2713 OLD expression found: {OLD_EXPR!r}")
print(f"\n  NEW expression (guard IIFE):")
print(f"  {NEW_EXPR}")


# ── Step 5: Apply replacement ──────────────────────────────────────────────────
step("5 · Replace expression in body")

updated_body = body_str.replace(OLD_EXPR, NEW_EXPR, 1)

# Confirm replacement happened
if NEW_EXPR not in updated_body:
    print("  \u2717 Replacement failed — NEW_EXPR not found in updated body.")
    sys.exit(1)

# Confirm OLD is gone
if OLD_EXPR in updated_body:
    print("  \u2717 OLD expression still present after replacement — unexpected.")
    sys.exit(1)

params[body_key] = updated_body
print(f"  \u2713 Replacement successful")
print(f"\n  Body tail after fix (last 200 chars):")
print(f"  ...{updated_body[-200:]}")


# ── Step 6: Deactivate → PUT → Activate ───────────────────────────────────────
step("6 · Deactivate workflow")

r = requests.post(
    f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/deactivate",
    headers=n8n_headers,
)
check(r, "Deactivate")

step("7 · PUT updated workflow")

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

step("8 · Reactivate workflow")

r = requests.post(
    f"{N8N_BASE}/api/v1/workflows/{WORKFLOW_ID}/activate",
    headers=n8n_headers,
)
check(r, "Activate")


# ── Done ───────────────────────────────────────────────────────────────────────
print(f"\n{sep}")
print("  DONE — 🏢 Domain guard expression installed in Update Existing Row")
print(sep)
print()
print("  Behaviour going forward:")
print("    • Customer already has real domain (e.g. cuchy.es)  → preserved")
print("    • Customer has gmail.com / empty                     → Stripe email domain used")
print("    • New onboarding (no existing row yet)               → Stripe email domain set")
print()
print("  Verification:")
print("    1. Open n8n UI → Stripe Sync (Ai9Y3FWjqMtEhr57)")
print("    2. Click 'Update Existing Row' node → inspect 🏢 Domain expression")
print("       Should show the guard IIFE, not the bare Stripe domain expression")
print(f"  Backup: {BACKUP_PATH}")
print()
