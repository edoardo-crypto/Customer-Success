#!/usr/bin/env python3
"""
Fix severity rename in workflow 3AO3SRUK80rcOCgQ — Feb 18, 2026

Intercom Severity CDA values changed:
  Critical  →  Urgent
  Medium    →  Important
  Minor     →  Not Urgent

Nodes to update:
  1. Parse AI Summary       (f55665bc) — validSeverities list + 'Medium' fallback
  2. Build Notion Payload   (ebaf8800) — "Medium" default fallback
  3. Gather Context         (765f594f) — classification.severity → mapped_severity, 'Medium' → 'Important'
  4. Build Claude Ticket    (63f28214) — system prompt priority guidance
"""

import json
import urllib.request
import urllib.error
import ssl
import sys

# ── Config ───────────────────────────────────────────────────────────────────
N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = (
    "***REMOVED_JWT***."
    "eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJpc3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleHAiOjE3NzMyNzAwMDB9."
    "X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)
WORKFLOW_ID = "3AO3SRUK80rcOCgQ"

NODE_PARSE_SUMMARY    = "f55665bc-4fbb-4419-af85-eb1c4191c0b8"
NODE_BUILD_PAYLOAD    = "ebaf8800-06f9-458f-92c9-60ae532839ec"
NODE_GATHER_CONTEXT   = "765f594f-7ffd-4ce4-8470-266d35b4f5dc"
NODE_CLAUDE_TICKET    = "63f28214-4d22-4c14-8281-2d16df1d92d6"

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


# ── Per-node fixers ───────────────────────────────────────────────────────────

def fix_parse_summary(node):
    """
    validSeverities: ['Critical', 'Medium', 'Minor'] → ['Urgent', 'Important', 'Not Urgent']
    fallback: 'Medium' → 'Important'
    """
    js = node["parameters"]["jsCode"]
    changes = []

    old = "const validSeverities = ['Critical', 'Medium', 'Minor'];"
    new = "const validSeverities = ['Urgent', 'Important', 'Not Urgent'];"
    if old in js:
        js = js.replace(old, new)
        changes.append("validSeverities list updated")
    else:
        log("  WARN: validSeverities not found with expected exact string", 1)

    old2 = "const mappedSeverity = validSeverities.includes(sev) ? sev : 'Medium';"
    new2 = "const mappedSeverity = validSeverities.includes(sev) ? sev : 'Important';"
    if old2 in js:
        js = js.replace(old2, new2)
        changes.append("fallback 'Medium' → 'Important'")
    else:
        log("  WARN: mappedSeverity fallback not found with expected exact string", 1)

    node["parameters"]["jsCode"] = js
    return node, changes


def fix_build_payload(node):
    """
    "Severity": { "select": { "name": d.mapped_severity || "Medium" } }
    → "Medium" fallback → "Important"
    """
    js = node["parameters"]["jsCode"]
    changes = []

    old = 'd.mapped_severity || "Medium"'
    new = 'd.mapped_severity || "Important"'
    if old in js:
        js = js.replace(old, new)
        changes.append('Notion Severity fallback "Medium" → "Important"')
    else:
        log("  WARN: mapped_severity fallback 'Medium' not found", 1)

    node["parameters"]["jsCode"] = js
    return node, changes


def fix_gather_context(node):
    """
    classification.severity || 'Medium'  →  classification.mapped_severity || 'Important'
    """
    js = node["parameters"]["jsCode"]
    changes = []

    old = "severity: classification.severity || 'Medium',"
    new = "severity: classification.mapped_severity || 'Important',"
    if old in js:
        js = js.replace(old, new)
        changes.append("severity: classification.severity → classification.mapped_severity, fallback 'Medium' → 'Important'")
    else:
        log("  WARN: severity line not found with expected exact string in Gather Context", 1)
        # Try to find any 'Medium' in context
        if "'Medium'" in js:
            log("  Found 'Medium' but in different context — manual review needed", 1)

    node["parameters"]["jsCode"] = js
    return node, changes


def fix_claude_ticket(node):
    """
    System prompt priority guidance:
    'Critical -> priority 1 or 2' → 'Urgent -> priority 1 or 2'
    'Medium -> priority 3'         → 'Important -> priority 3'
    'Minor -> priority 4'          → 'Not Urgent -> priority 4'
    """
    js = node["parameters"]["jsCode"]
    changes = []

    replacements = [
        ("Critical -> priority 1 or 2", "Urgent -> priority 1 or 2"),
        ("Medium -> priority 3",         "Important -> priority 3"),
        ("Minor -> priority 4",          "Not Urgent -> priority 4"),
    ]

    for old, new in replacements:
        if old in js:
            js = js.replace(old, new)
            changes.append(f"'{old}' → '{new}'")
        else:
            log(f"  WARN: '{old}' not found in Build Claude Ticket", 1)

    node["parameters"]["jsCode"] = js
    return node, changes


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log("=" * 60)
    log("Fix: Severity rename Critical/Medium/Minor → Urgent/Important/Not Urgent")
    log(f"Workflow: {WORKFLOW_ID}")
    log("=" * 60)
    log("")

    # 1. Fetch
    log("[1/5] Fetching workflow...")
    wf = n8n("GET", f"/workflows/{WORKFLOW_ID}")
    log(f"  Name: {wf['name']}", 1)
    log(f"  Nodes: {len(wf['nodes'])}", 1)

    with open("/tmp/workflow_backup_severity.json", "w") as f:
        json.dump(wf, f, indent=2)
    log("  Backup: /tmp/workflow_backup_severity.json", 1)

    # 2. Deactivate
    log("")
    log("[2/5] Deactivating workflow...")
    try:
        n8n("POST", f"/workflows/{WORKFLOW_ID}/deactivate")
        log("  Deactivated ✓", 1)
    except Exception as e:
        log(f"  Warning (continuing): {e}", 1)

    # 3. Fix nodes
    log("")
    log("[3/5] Patching nodes...")

    fixers = {
        NODE_PARSE_SUMMARY:  ("Parse AI Summary",           fix_parse_summary),
        NODE_BUILD_PAYLOAD:  ("Build Notion Issue Payload",  fix_build_payload),
        NODE_GATHER_CONTEXT: ("Gather Context",              fix_gather_context),
        NODE_CLAUDE_TICKET:  ("Build Claude Ticket Payload", fix_claude_ticket),
    }

    found = set()
    all_changes = []

    for node in wf["nodes"]:
        nid = node.get("id", "")
        if nid in fixers:
            name, fixer = fixers[nid]
            found.add(nid)
            log(f"  → {name}", 1)
            node, changes = fixer(node)
            if changes:
                for c in changes:
                    log(f"    ✓ {c}", 2)
                all_changes.extend(changes)
            else:
                log("    — no changes needed", 2)

    missing = set(fixers.keys()) - found
    if missing:
        log("")
        log("  ERROR: nodes not found:")
        for nid in missing:
            name, _ = fixers[nid]
            log(f"    {nid} — {name}")
        sys.exit(1)

    log(f"  Total changes applied: {len(all_changes)}", 1)

    # 4. PUT
    log("")
    log("[4/5] Pushing updated workflow...")
    put_body = {
        "name":        wf["name"],
        "nodes":       wf["nodes"],
        "connections": wf.get("connections", {}),
        "settings":    wf.get("settings", {}),
    }
    with open("/tmp/workflow_severity_fixed.json", "w") as f:
        json.dump(put_body, f, indent=2)
    log("  Saved to /tmp/workflow_severity_fixed.json", 1)

    result = n8n("PUT", f"/workflows/{WORKFLOW_ID}", put_body)
    log(f"  Updated: {result.get('name', '?')} ✓", 1)

    # 5. Reactivate
    log("")
    log("[5/5] Reactivating workflow...")
    try:
        n8n("POST", f"/workflows/{WORKFLOW_ID}/activate")
        log("  Workflow activated ✓", 1)
    except Exception as e:
        log(f"  Activation error: {e}", 1)
        log("  Please activate manually in n8n UI", 1)

    # Summary
    log("")
    log("=" * 60)
    log("FIX COMPLETE")
    log("=" * 60)
    log("")
    log("Severity mapping (after fix):")
    log("  Intercom 'Urgent'     → Notion 'Urgent'     (was Critical)")
    log("  Intercom 'Important'  → Notion 'Important'  (was Medium)")
    log("  Intercom 'Not Urgent' → Notion 'Not Urgent' (was Minor)")
    log("  Unrecognized/missing  → Notion 'Important'  (fallback, was 'Medium')")
    log("")
    log("Nodes patched:")
    log("  1. Parse AI Summary        — validSeverities + fallback")
    log("  2. Build Notion Payload    — Notion severity fallback")
    log("  3. Gather Context          — reads mapped_severity, fallback 'Important'")
    log("  4. Build Claude Ticket     — system prompt priority guidance")
    log("")
    log("Verification:")
    log("  1. Close Intercom conversation → set Severity = 'Urgent'")
    log("     → Notion Issues Table should show Severity = 'Urgent'")
    log("  2. Close with Severity = 'Important' → Notion shows 'Important'")
    log("  3. Close with Severity = 'Not Urgent' → Notion shows 'Not Urgent'")


if __name__ == "__main__":
    main()
