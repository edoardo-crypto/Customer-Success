#!/usr/bin/env python3
"""
Fix workflow 3AO3SRUK80rcOCgQ — Feb 19, 2026

Changes:
1. Filter node (filter-not-issue-001):
   - Add cs_severity notEmpty condition
   - Add issue_description notEmpty condition
   (Total: 5 AND conditions — all must pass to log to Notion)
2. Parse AI Summary (f55665bc): 'important' → 'Important' (capital I)
3. Build Notion Payload (ebaf8800): severity fallback "important" → "Important"
4. Add Slack alert HTTP Request node on filter false branch
   - continueOnFail: true (Slack failure CANNOT break the main pipeline)
   - Positioned on a separate branch: main pipeline is unaffected
"""

import json
import uuid
import urllib.request
import urllib.error
import ssl
import sys
import creds

# ── Config ────────────────────────────────────────────────────────────────────
N8N_BASE     = "https://konvoai.app.n8n.cloud"
N8N_API_KEY  = creds.get("N8N_API_KEY")
WORKFLOW_ID   = "3AO3SRUK80rcOCgQ"
NODE_FILTER   = "filter-not-issue-001"
NODE_PARSE    = "f55665bc-4fbb-4419-af85-eb1c4191c0b8"
NODE_PAYLOAD  = "ebaf8800-06f9-458f-92c9-60ae532839ec"
SLACK_CRED_ID = "IMuEGtYutmUKwCqY"
SLACK_CHANNEL = "C0AC2BTCJVA"  # private channel ID

try:
    ctx = ssl.create_default_context()
except Exception:
    ctx = None


def log(msg, indent=0):
    print("  " * indent + msg)


def n8n(method, path, body=None):
    url = f"{N8N_BASE}/api/v1{path}"
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-N8N-API-KEY", N8N_API_KEY)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    try:
        resp = urllib.request.urlopen(req, context=ctx, timeout=30)
        return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8") if e.fp else ""
        log(f"HTTP {e.code}: {body_text[:500]}")
        raise


# ── Fixers ────────────────────────────────────────────────────────────────────

def fix_filter_node(node):
    """Add cs_severity and issue_description notEmpty conditions."""
    conditions = node["parameters"]["conditions"]
    existing = conditions.get("conditions", [])
    existing_lefts = [c.get("leftValue", "") for c in existing]

    log(f"  Current conditions ({len(existing)}):", 1)
    for c in existing:
        op = c.get("operator", {}).get("operation", "?")
        rv = c.get("rightValue", "")
        log(f"    {c.get('leftValue', '')} [{op}] {rv!r}", 2)

    changes = []

    if not any("cs_severity" in lv for lv in existing_lefts):
        existing.append({
            "id": f"check-severity-{uuid.uuid4().hex[:8]}",
            "leftValue": "={{ $json.cs_severity }}",
            "rightValue": "",
            "operator": {"type": "string", "operation": "notEmpty"}
        })
        changes.append("+ cs_severity notEmpty")
    else:
        log("    cs_severity condition already exists — skipped", 2)

    if not any("issue_description" in lv for lv in existing_lefts):
        existing.append({
            "id": f"check-description-{uuid.uuid4().hex[:8]}",
            "leftValue": "={{ $json.issue_description }}",
            "rightValue": "",
            "operator": {"type": "string", "operation": "notEmpty"}
        })
        changes.append("+ issue_description notEmpty")
    else:
        log("    issue_description condition already exists — skipped", 2)

    conditions["conditions"] = existing
    node["parameters"]["conditions"] = conditions
    return node, changes


def fix_parse_summary(node):
    """Fix 'important' → 'Important' (capital I) in validSeverities and fallback."""
    js = node["parameters"]["jsCode"]
    changes = []

    for old, new in [
        ("'important'",  "'Important'"),
        ('"important"',  '"Important"'),
    ]:
        count = js.count(old)
        if count:
            js = js.replace(old, new)
            changes.append(f"{old} → {new}  ({count}x replaced)")

    node["parameters"]["jsCode"] = js
    return node, changes


def fix_payload_node(node):
    """Fix severity fallback 'important' → 'Important'."""
    js = node["parameters"]["jsCode"]
    changes = []

    for old, new in [
        ('d.mapped_severity || "important"', 'd.mapped_severity || "Important"'),
        ("d.mapped_severity || 'important'", "d.mapped_severity || 'Important'"),
    ]:
        if old in js:
            js = js.replace(old, new)
            changes.append(f"fallback {old!r} → {new!r}")

    node["parameters"]["jsCode"] = js
    return node, changes


def make_slack_node(x, y):
    """
    Build a Slack alert HTTP Request node.
    continueOnFail: true — if Slack API call fails for any reason,
    the failure is swallowed and does NOT affect the main pipeline.
    """
    # The main pipeline runs on filter output[0] (true branch).
    # This node runs on filter output[1] (false branch).
    # These are fully independent execution branches in n8n.
    body_expr = (
        "={{ JSON.stringify({ channel: '" + SLACK_CHANNEL + "', "
        "text: ':warning: *Intercom conversation blocked from Notion*\\n'"
        " + '*Conv ID:* '    + ($json.conversation_id || 'unknown') + '\\n'"
        " + '*Issue Type:* ' + ($json.issue_type        || '_(empty)_') + '\\n'"
        " + '*Severity:* '   + ($json.cs_severity       || '_(empty)_') + '\\n'"
        " + '*Desc:* '       + ($json.issue_description ? 'filled' : '_(empty)_')"
        " }) }}"
    )
    return {
        "id": "node-slack-blocked-alert",
        "name": "Slack: Blocked Conversation Alert",
        "type": "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position": [x, y],
        "continueOnFail": True,
        "credentials": {
            "httpHeaderAuth": {
                "id": SLACK_CRED_ID,
                "name": "Slack Bot for Alerts"
            }
        },
        "parameters": {
            "method": "POST",
            "url": "https://slack.com/api/chat.postMessage",
            "authentication": "genericCredentialType",
            "genericAuthType": "httpHeaderAuth",
            "sendBody": True,
            "specifyBody": "json",
            "jsonBody": body_expr,
            "options": {"timeout": 10000}
        }
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log("=" * 60)
    log("Fix: Field validation + Severity case + Slack alert")
    log(f"Workflow: {WORKFLOW_ID}")
    log("=" * 60)
    log("")

    # 1. Fetch workflow
    log("[1/6] Fetching workflow...")
    wf = n8n("GET", f"/workflows/{WORKFLOW_ID}")
    log(f"  Name: {wf['name']}", 1)
    log(f"  Nodes: {len(wf['nodes'])}", 1)
    log(f"  Active: {wf.get('active')}", 1)

    with open("/tmp/workflow_backup_field_validation.json", "w", encoding="utf-8") as f:
        json.dump(wf, f, indent=2, ensure_ascii=False)
    log("  Backup: /tmp/workflow_backup_field_validation.json", 1)

    # 2. Deactivate
    log("")
    log("[2/6] Deactivating workflow...")
    try:
        n8n("POST", f"/workflows/{WORKFLOW_ID}/deactivate")
        log("  Deactivated ✓", 1)
    except Exception as e:
        log(f"  Warning (continuing): {e}", 1)

    # 3. Patch nodes
    log("")
    log("[3/6] Patching code nodes...")

    found = {NODE_FILTER: False, NODE_PARSE: False, NODE_PAYLOAD: False}
    filter_node_ref = None

    for node in wf["nodes"]:
        nid = node.get("id", "")

        if nid == NODE_FILTER:
            found[NODE_FILTER] = True
            filter_node_ref = node
            log(f"  → Filter node: '{node['name']}'", 1)
            node, changes = fix_filter_node(node)
            for c in changes:
                log(f"    ✓ {c}", 2)
            if not changes:
                log("    — no new conditions needed", 2)

        elif nid == NODE_PARSE:
            found[NODE_PARSE] = True
            log(f"  → Parse AI Summary: '{node['name']}'", 1)
            node, changes = fix_parse_summary(node)
            for c in changes:
                log(f"    ✓ {c}", 2)
            if not changes:
                log("    — no changes needed (already correct)", 2)

        elif nid == NODE_PAYLOAD:
            found[NODE_PAYLOAD] = True
            log(f"  → Build Notion Payload: '{node['name']}'", 1)
            node, changes = fix_payload_node(node)
            for c in changes:
                log(f"    ✓ {c}", 2)
            if not changes:
                log("    — no changes needed (already correct)", 2)

    missing = [nid for nid, ok in found.items() if not ok]
    if missing:
        log(f"\n  ERROR: nodes not found: {missing}")
        log("  Available node IDs:")
        for n in wf["nodes"]:
            log(f"    {n.get('id','?')} — {n.get('name','?')}", 2)
        sys.exit(1)

    if filter_node_ref is None:
        log("  ERROR: filter node reference lost — exiting")
        sys.exit(1)

    # 4. Add Slack node
    log("")
    log("[4/6] Adding Slack alert node...")

    fx, fy = filter_node_ref.get("position", [800, 300])
    slack_node = make_slack_node(fx + 260, fy + 220)

    # Remove if already exists (idempotent)
    wf["nodes"] = [n for n in wf["nodes"] if n.get("id") != slack_node["id"]]
    wf["nodes"].append(slack_node)

    log(f"  Node: '{slack_node['name']}'", 1)
    log(f"  Position: {slack_node['position']}", 1)
    log(f"  continueOnFail: True  ← Slack failure will NOT break the workflow", 1)
    log(f"  Channel: #{SLACK_CHANNEL}", 1)

    # 5. Wire filter false branch → Slack node
    log("")
    log("[5/6] Wiring filter false output [1] → Slack node...")

    filter_name = filter_node_ref["name"]
    connections = wf.get("connections", {})

    if filter_name not in connections:
        connections[filter_name] = {"main": [[], []]}

    fc = connections[filter_name].get("main", [])
    while len(fc) < 2:
        fc.append([])

    # Log what was on false branch before
    prev_false = fc[1]
    if prev_false:
        log(f"  Previous false-branch connections: {prev_false}", 1)
    else:
        log("  Previous false-branch: empty (nothing was connected)", 1)

    slack_conn = {"node": slack_node["name"], "type": "main", "index": 0}
    already = any(c.get("node") == slack_node["name"] for c in fc[1])
    if not already:
        fc[1] = [slack_conn]
        log(f"  Connected: '{filter_name}' [false] → '{slack_node['name']}' ✓", 1)
    else:
        log(f"  Already connected — skipped", 1)

    connections[filter_name]["main"] = fc
    wf["connections"] = connections

    # 6. PUT + activate
    log("")
    log("[6/6] Pushing updated workflow and activating...")

    put_body = {
        "name":        wf["name"],
        "nodes":       wf["nodes"],
        "connections": wf.get("connections", {}),
        "settings":    wf.get("settings", {}),
    }
    with open("/tmp/workflow_field_validation_fixed.json", "w", encoding="utf-8") as f:
        json.dump(put_body, f, indent=2, ensure_ascii=False)
    log("  Saved: /tmp/workflow_field_validation_fixed.json", 1)

    result = n8n("PUT", f"/workflows/{WORKFLOW_ID}", put_body)
    log(f"  Updated: '{result.get('name', '?')}' ✓", 1)

    try:
        n8n("POST", f"/workflows/{WORKFLOW_ID}/activate")
        log("  Activated ✓", 1)
    except Exception as e:
        log(f"  Activation error: {e}", 1)
        log("  → Please activate manually in n8n UI", 1)

    # ── Summary ───────────────────────────────────────────────────────────────
    log("")
    log("=" * 60)
    log("FIX COMPLETE")
    log("=" * 60)
    log("")
    log("Changes applied:")
    log("  1. Filter node — now 5 AND conditions:")
    log("       issue_type notEmpty")
    log("       issue_type != 'No Issue'")
    log("       is_internal != true")
    log("     + cs_severity notEmpty        ← NEW")
    log("     + issue_description notEmpty  ← NEW")
    log("  2. Parse AI Summary: 'important' → 'Important' (capital I)")
    log("  3. Build Notion Payload: severity fallback 'important' → 'Important'")
    log("  4. Slack alert node on filter false branch (continueOnFail: true)")
    log("")
    log("Logging matrix (going forward):")
    log("  Issue Type ✓ + Severity ✓ + Description ✓  →  logged to Notion")
    log("  Any field empty / 'No Issue' / is_internal  →  BLOCKED + Slack alert")
    log("  Existing Notion entries with blank fields   →  unchanged (left as-is)")
    log("")
    log("Slack isolation guarantee:")
    log("  Filter output[0] (true)  → main pipeline  (independent branch)")
    log("  Filter output[1] (false) → Slack alert     (separate branch)")
    log("  continueOnFail: true     → Slack API error is swallowed, workflow OK")
    log("")
    log(f"  IMPORTANT: verify the Slack channel '#{SLACK_CHANNEL}' is correct.")
    log("  If not, change SLACK_CHANNEL in this script and re-run.")
    log("")
    log("Verification:")
    log("  1. Close Intercom convo: all 3 fields filled  → appears in Notion ✓")
    log("  2. Close Intercom convo: Severity empty       → NOT in Notion, Slack alert")
    log("  3. Close Intercom convo: Description empty    → NOT in Notion, Slack alert")
    log("  4. Close Intercom convo: Issue Type = No Issue → filtered out, Slack alert")


if __name__ == "__main__":
    main()
