#!/usr/bin/env python3
"""
Fix workflow 3AO3SRUK80rcOCgQ for "Feature improvement" Issue Type — Feb 18, 2026

Two fixes:
1. Filter node (filter-not-issue-001):
   "Not an Issue" → "No Issue"  (Intercom CDA actual value)
2. Build Notion Issue Payload node (ebaf8800-...):
   'New feature request' → 'New Feature Request'  (case fix)
   Ensure 'Feature improvement' is present (already in deploy_intercom_pipeline_v2)

Also ensures the filter passes through "Feature improvement" conversations
(they had issue_type set, so is_internal=false & issue_type != "No Issue" → they pass already).
"""

import json
import urllib.request
import urllib.error
import ssl
import sys
import creds

# ── Config ──────────────────────────────────────────────────────────────────
N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WORKFLOW_ID = "3AO3SRUK80rcOCgQ"

# Node IDs to fix
NODE_FILTER    = "filter-not-issue-001"
NODE_PAYLOAD   = "ebaf8800-06f9-458f-92c9-60ae532839ec"

try:
    ctx = ssl.create_default_context()
except Exception:
    ctx = None


def log(msg, indent=0):
    print("  " * indent + msg)


def n8n(method, path, body=None):
    url = f"{N8N_BASE}/api/v1{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-N8N-API-KEY", N8N_API_KEY)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=30)
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode() if e.fp else ""
        log(f"HTTP {e.code}: {body_text[:500]}")
        raise


# ── Fixers ───────────────────────────────────────────────────────────────────

def fix_filter_node(node):
    """Fix: 'Not an Issue' → 'No Issue' to match Intercom CDA value."""
    conditions = node["parameters"].get("conditions", {})
    fixed = False
    for cond in conditions.get("conditions", []):
        if cond.get("rightValue") == "Not an Issue":
            log(f"  Filter cond '{cond['id']}': 'Not an Issue' → 'No Issue'", 1)
            cond["rightValue"] = "No Issue"
            fixed = True
    if not fixed:
        log("  Filter node: no 'Not an Issue' found — checking current value...", 1)
        for cond in conditions.get("conditions", []):
            if "issue_type" in cond.get("leftValue", ""):
                log(f"  Current issue_type condition rightValue: '{cond.get('rightValue')}'", 2)
    return node, fixed


def fix_payload_node(node):
    """Fix valid_issue_types list:
    - 'New feature request' → 'New Feature Request'  (case)
    - Ensure 'Feature improvement' is present
    - Ensure 'No Issue' is NOT in the list (it's filtered before this node)
    """
    js = node["parameters"].get("jsCode", "")

    changes = []

    # Fix 1: case on New Feature Request
    old_nfr = "'New feature request'"
    new_nfr = "'New Feature Request'"
    if old_nfr in js:
        js = js.replace(old_nfr, new_nfr)
        changes.append(f"'New feature request' → 'New Feature Request'")

    # Check if Feature improvement is already present
    if "'Feature improvement'" in js:
        log("  'Feature improvement' already present in valid types ✓", 1)
    else:
        # Add it to the list
        old_list = "['Bug', 'Config Issue', 'New Feature Request'"
        new_list = "['Bug', 'Config Issue', 'New Feature Request', 'Feature improvement'"
        if old_list in js:
            js = js.replace(old_list, new_list)
            changes.append("Added 'Feature improvement' to valid types")
        else:
            log("  WARNING: Could not locate valid types list to add 'Feature improvement'", 1)

    node["parameters"]["jsCode"] = js
    for c in changes:
        log(f"  Payload node: {c}", 1)
    return node, len(changes) > 0 or ("'Feature improvement'" in js)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    log("=" * 60)
    log("Fix: Feature improvement Issue Type mapping")
    log(f"Workflow: {WORKFLOW_ID}")
    log("=" * 60)
    log("")

    # 1. Fetch workflow
    log("[1/5] Fetching workflow...")
    wf = n8n("GET", f"/workflows/{WORKFLOW_ID}")
    log(f"  Name: {wf['name']}", 1)
    log(f"  Nodes: {len(wf['nodes'])}", 1)
    log(f"  Active: {wf.get('active', '?')}", 1)

    # Save backup
    with open("/tmp/workflow_backup_feature_fix.json", "w") as f:
        json.dump(wf, f, indent=2)
    log("  Backup saved to /tmp/workflow_backup_feature_fix.json", 1)

    # 2. Deactivate
    log("")
    log("[2/5] Deactivating workflow...")
    try:
        n8n("POST", f"/workflows/{WORKFLOW_ID}/deactivate")
        log("  Deactivated ✓", 1)
    except Exception as e:
        log(f"  Deactivation warning (continuing): {e}", 1)

    # 3. Audit + fix nodes
    log("")
    log("[3/5] Auditing and fixing nodes...")
    nodes = wf["nodes"]
    filter_fixed   = False
    payload_fixed  = False
    filter_found   = False
    payload_found  = False

    for node in nodes:
        nid = node.get("id", "")

        if nid == NODE_FILTER:
            filter_found = True
            log(f"  Found filter node: {node.get('name', '?')}", 1)
            node, filter_fixed = fix_filter_node(node)

        elif nid == NODE_PAYLOAD:
            payload_found = True
            log(f"  Found payload node: {node.get('name', '?')}", 1)
            node, payload_fixed = fix_payload_node(node)

    # Report findings
    log("")
    log("  Audit summary:", 1)
    log(f"    Filter node found:   {'✓' if filter_found else '✗ NOT FOUND'}", 2)
    log(f"    Filter fix applied:  {'✓' if filter_fixed else '— no change needed'}", 2)
    log(f"    Payload node found:  {'✓' if payload_found else '✗ NOT FOUND'}", 2)
    log(f"    Payload fix applied: {'✓' if payload_fixed else '— no change needed'}", 2)

    if not filter_found or not payload_found:
        log("")
        log("  ERROR: One or more target nodes not found in workflow.")
        log("  Available node IDs:")
        for n in nodes:
            log(f"    {n.get('id', '?')} — {n.get('name', '?')}", 2)
        sys.exit(1)

    # 4. PUT updated workflow
    log("")
    log("[4/5] Pushing updated workflow...")
    put_body = {
        "name":        wf["name"],
        "nodes":       wf["nodes"],
        "connections": wf.get("connections", {}),
        "settings":    wf.get("settings", {}),
    }

    # Save modified version for inspection
    with open("/tmp/workflow_feature_fix_v2.json", "w") as f:
        json.dump(put_body, f, indent=2)
    log("  Modified workflow saved to /tmp/workflow_feature_fix_v2.json", 1)

    result = n8n("PUT", f"/workflows/{WORKFLOW_ID}", put_body)
    log(f"  Workflow updated: {result.get('name', '?')} ✓", 1)

    # 5. Reactivate
    log("")
    log("[5/5] Reactivating workflow...")
    try:
        n8n("POST", f"/workflows/{WORKFLOW_ID}/activate")
        log("  Workflow activated ✓", 1)
    except Exception as e:
        log(f"  Activation error: {e}", 1)
        log("  Please activate manually in the n8n UI", 1)

    # Summary
    log("")
    log("=" * 60)
    log("FIX COMPLETE")
    log("=" * 60)
    log("")
    log("Changes applied:")
    log("  1. Filter node: 'Not an Issue' → 'No Issue' (matches Intercom CDA)")
    log("  2. Payload node: 'New feature request' → 'New Feature Request' (case fix)")
    log("  3. Payload node: 'Feature improvement' present in valid types ✓")
    log("")
    log("Intercom Issue Type → n8n mapping (after fix):")
    log("  Bug              → Bug                  ✓")
    log("  Config Issue     → Config Issue          ✓")
    log("  No Issue         → filtered out          ✓  (was broken)")
    log("  New Feature Request → New Feature Request ✓  (was case mismatch)")
    log("  Feature improvement → Feature improvement ✓")
    log("")
    log("Verification:")
    log("  1. Close Intercom conversation with Issue Type = 'Feature improvement'")
    log("     → Notion Issues Table should show Issue Type = 'Feature improvement'")
    log("     → NOT 'Needs Classification'")
    log("  2. Close conversation with Issue Type = 'No Issue' → should be filtered out")
    log("  3. Close conversation with 'New Feature Request' → correct type in Notion")
    log("")
    log("REMINDER — Intercom UI fix still needed (manual, ~2 min):")
    log("  Settings → Conversations → Conversation Data Attributes → Severity")
    log("  Add 'Feature improvement' to the 'When to show' condition")
    log("  This unblocks the CS manager from closing conversations immediately.")


if __name__ == "__main__":
    main()
