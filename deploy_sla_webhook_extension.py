#!/usr/bin/env python3
"""
deploy_sla_webhook_extension.py — Extend the Linear → Notion webhook for SLA.

Fetches the existing workflow (xdVkUh6YCtcuW8QM), replaces the Code node JS
to also handle:
  1. Priority changes → Notion Severity sync
  2. Triage exit detection → Triaged At + SLA Resolution Deadline

After running, archive to archive/.

Architecture (6 nodes):
  Linear Webhook → Parse Event → Search Notion → Build SLA Patch → Has Changes? → Update Page

Usage:
  python3 deploy_sla_webhook_extension.py              # deploy
  DRY_RUN=true python3 deploy_sla_webhook_extension.py # preview JS only
"""

import json
import os
import ssl
import urllib.request
import urllib.error
import creds

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
NOTION_TOKEN = creds.get("NOTION_TOKEN")
NOTION_ISSUES_DB = "bd1ed48de20e426f8bebeb8e700d19d8"
WORKFLOW_ID = "xdVkUh6YCtcuW8QM"

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() not in ("false", "0", "no")

ctx = ssl.create_default_context()


def n8n(method, path, body=None):
    url = f"{N8N_BASE}/api/v1{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"X-N8N-API-KEY": N8N_API_KEY, "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, context=ctx) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        print(f"  HTTP {e.code}: {body_text[:500]}")
        raise


# ---------------------------------------------------------------------------
# Code node 1 — Parse Event
# Accepts ALL issue updates (state + priority changes, not just state).
# Outputs: identifier, stateChanged, newStateType, newStateName,
#          priorityChanged, newPriority, searchBody
# ---------------------------------------------------------------------------
PARSE_EVENT_CODE = r"""
const items = $input.all();
const body = items[0].json;

const { action, type, data, updatedFrom } = body;

// Only handle issue updates
if (action !== 'update' || type !== 'Issue') return [];
if (!data || !data.identifier) return [];
if (!updatedFrom) return [];

const stateChanged = !!updatedFrom.stateId;
const priorityChanged = updatedFrom.priority !== undefined;

// Must have at least one relevant change
if (!stateChanged && !priorityChanged) return [];

const identifier = data.identifier;
const newStateName = data.state?.name || '';
const newStateType = data.state?.type || '';
const newPriority = data.priority || 0;
const linearCreatedAt = data.createdAt || '';

const searchBody = JSON.stringify({
  filter: { property: 'Linear Ticket URL', url: { contains: identifier } },
  page_size: 1,
});

return [{ json: {
  identifier,
  stateChanged,
  newStateType,
  newStateName,
  priorityChanged,
  newPriority,
  linearCreatedAt,
  searchBody,
} }];
""".strip()


# ---------------------------------------------------------------------------
# Code node 2 — Build SLA Patch
# Reads the Notion search results + event data, applies guards, builds patch.
# ---------------------------------------------------------------------------
BUILD_PATCH_CODE = r"""
const items = $input.all();
const searchResults = items[0].json.results || [];
if (searchResults.length === 0) return [];

const page = searchResults[0];
const pageId = page.id;
const props = page.properties || {};

// Retrieve parsed event data from the previous Code node
const event = $('Parse Event').first().json;

// ── Read current Notion values ──────────────────────────────────────────
const currentStatus = props.Status?.select?.name || '';
const currentSeverity = props.Severity?.select?.name || '';
const triagedAt = props['Triaged At']?.date?.start || '';
const resolvedAt = props['Resolved At']?.date?.start || '';
const resDeadline = props['SLA Resolution Deadline']?.date?.start || '';
const ticketCreationDate = props['Ticket creation date']?.date?.start || '';
const triageDeadline = props['SLA Triage Deadline']?.date?.start || '';

// ── Priority → Severity mapping ─────────────────────────────────────────
const PRIO_MAP = { 1: 'Urgent', 2: 'Important', 3: 'Not important', 4: 'Not important' };
const SEVERITY_SLA_DAYS = { 'Urgent': 1, 'Important': 3, 'Not important': 10 };

// ── Business day calculator (Mon-Fri) ───────────────────────────────────
function addBusinessDays(startDate, days) {
  let current = new Date(startDate.getTime());
  let added = 0;
  while (added < days) {
    current.setDate(current.getDate() + 1);
    const dow = current.getDay(); // 0=Sun, 6=Sat
    if (dow !== 0 && dow !== 6) added++;
  }
  return current;
}

// ── State mapping (identical to sync_linear_status.py) ──────────────────
function mapLinearState(name, type) {
  const n = (name || '').toLowerCase();
  const t = (type || '').toLowerCase();
  if (['progress', 'review', 'testing'].some(kw => n.includes(kw))) return 'In Progress';
  if (['done', 'released', 'complete', 'resolved', 'duplicate'].some(kw => n.includes(kw))) return 'Resolved';
  if (n.includes('cancel')) return 'Deprioritized';
  if (t === 'started') return 'In Progress';
  if (t === 'completed') return 'Resolved';
  if (t === 'cancelled') return 'Deprioritized';
  return null;
}

// ── Build patch ─────────────────────────────────────────────────────────
const patchProps = {};
let hasChanges = false;

// 0. Sync Linear createdAt → Ticket creation date + compute triage deadline
if (event.linearCreatedAt && !ticketCreationDate) {
  patchProps['Ticket creation date'] = { date: { start: event.linearCreatedAt } };
  hasChanges = true;
}
if (!triageDeadline && event.linearCreatedAt) {
  const ticketDt = new Date(event.linearCreatedAt);
  const triageDl = addBusinessDays(ticketDt, 1);
  patchProps['SLA Triage Deadline'] = { date: { start: triageDl.toISOString() } };
  hasChanges = true;
}

// 1. Status sync
if (event.stateChanged) {
  const targetStatus = mapLinearState(event.newStateName, event.newStateType);
  if (targetStatus && targetStatus !== currentStatus) {
    patchProps.Status = { select: { name: targetStatus } };
    if (targetStatus === 'Resolved' && !resolvedAt) {
      patchProps['Resolved At'] = { date: { start: new Date().toISOString() } };
    }
    hasChanges = true;
  }
}

// 2. Priority → Severity sync
if (event.priorityChanged) {
  const targetSeverity = PRIO_MAP[event.newPriority];
  if (targetSeverity && targetSeverity !== currentSeverity) {
    patchProps.Severity = { select: { name: targetSeverity } };
    hasChanges = true;
  }
}

// 3. Triage exit detection
if (event.stateChanged && event.newStateType !== 'triage' && !triagedAt) {
  const now = new Date();
  patchProps['Triaged At'] = { date: { start: now.toISOString() } };
  hasChanges = true;

  // Reset SLA Status — triage phase is complete, start fresh for resolution
  patchProps['SLA Status'] = { select: { name: 'On Track' } };

  // Compute resolution deadline
  const effectiveSeverity = (patchProps.Severity?.select?.name) || currentSeverity;
  const slaDays = SEVERITY_SLA_DAYS[effectiveSeverity];
  if (slaDays && !resDeadline) {
    const deadline = addBusinessDays(now, slaDays);
    patchProps['SLA Resolution Deadline'] = { date: { start: deadline.toISOString() } };
  }
}

// 4. Recalc resolution deadline on severity change (if already triaged)
if (event.priorityChanged && triagedAt && resDeadline) {
  const newSeverity = PRIO_MAP[event.newPriority];
  const slaDays = SEVERITY_SLA_DAYS[newSeverity];
  if (slaDays && newSeverity !== currentSeverity) {
    const triagedDate = new Date(triagedAt);
    const deadline = addBusinessDays(triagedDate, slaDays);
    patchProps['SLA Resolution Deadline'] = { date: { start: deadline.toISOString() } };
    hasChanges = true;
  }
}

if (!hasChanges) return [];

const patchBodyStr = JSON.stringify({ properties: patchProps });
const patchUrl = 'https://api.notion.com/v1/pages/' + pageId;

return [{ json: { pageId, patchUrl, patchBodyStr, hasChanges: true } }];
""".strip()


# ---------------------------------------------------------------------------
# Workflow definition
# ---------------------------------------------------------------------------
NOTION_AUTH_HEADERS = {
    "parameters": [
        {"name": "Authorization", "value": f"Bearer {NOTION_TOKEN}"},
        {"name": "Notion-Version", "value": "2022-06-28"},
        {"name": "Content-Type",   "value": "application/json"},
    ]
}


def build_workflow_body(existing_wf):
    """Build the PUT body using existing webhook settings + new nodes."""

    # Preserve the existing webhook node (keeps webhookId, path, etc.)
    existing_nodes = {n["name"]: n for n in existing_wf.get("nodes", [])}
    webhook_node = existing_nodes.get("Linear Webhook")

    if not webhook_node:
        # Fallback — find by type
        for n in existing_wf.get("nodes", []):
            if "webhook" in n.get("type", "").lower():
                webhook_node = n
                break

    if not webhook_node:
        raise RuntimeError("Could not find webhook node in existing workflow")

    # Keep webhook node as-is but ensure position
    webhook_node["position"] = webhook_node.get("position", [250, 300])

    nodes = [
        webhook_node,
        # ── Node 2: Parse Event ───────────────────────────────────────────
        {
            "id": "n2",
            "name": "Parse Event",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [500, 300],
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": PARSE_EVENT_CODE,
            },
        },
        # ── Node 3: Search Notion Issue ───────────────────────────────────
        {
            "id": "n3",
            "name": "Search Notion Issue",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position": [750, 300],
            "parameters": {
                "method": "POST",
                "url": f"https://api.notion.com/v1/databases/{NOTION_ISSUES_DB}/query",
                "sendHeaders": True,
                "headerParameters": NOTION_AUTH_HEADERS,
                "sendBody": True,
                "contentType": "raw",
                "rawContentType": "application/json",
                "body": "={{ $json.searchBody }}",
            },
        },
        # ── Node 4: Build SLA Patch ───────────────────────────────────────
        {
            "id": "n4",
            "name": "Build SLA Patch",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1000, 300],
            "parameters": {
                "mode": "runOnceForAllItems",
                "jsCode": BUILD_PATCH_CODE,
            },
        },
        # ── Node 5: Has Changes? ─────────────────────────────────────────
        {
            "id": "n5",
            "name": "Has Changes?",
            "type": "n8n-nodes-base.if",
            "typeVersion": 1,
            "position": [1250, 300],
            "parameters": {
                "conditions": {
                    "boolean": [
                        {
                            "value1": "={{ $json.hasChanges }}",
                            "operation": "equal",
                            "value2": True,
                        }
                    ]
                }
            },
        },
        # ── Node 6: Update Notion Page ───────────────────────────────────
        {
            "id": "n6",
            "name": "Update Notion Page",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position": [1500, 300],
            "parameters": {
                "method": "PATCH",
                "url": "={{ $json.patchUrl }}",
                "sendHeaders": True,
                "headerParameters": NOTION_AUTH_HEADERS,
                "sendBody": True,
                "contentType": "raw",
                "rawContentType": "application/json",
                "body": "={{ $json.patchBodyStr }}",
            },
        },
    ]

    webhook_name = webhook_node["name"]

    connections = {
        webhook_name: {
            "main": [[{"node": "Parse Event", "type": "main", "index": 0}]]
        },
        "Parse Event": {
            "main": [[{"node": "Search Notion Issue", "type": "main", "index": 0}]]
        },
        "Search Notion Issue": {
            "main": [[{"node": "Build SLA Patch", "type": "main", "index": 0}]]
        },
        "Build SLA Patch": {
            "main": [[{"node": "Has Changes?", "type": "main", "index": 0}]]
        },
        "Has Changes?": {
            "main": [
                [{"node": "Update Notion Page", "type": "main", "index": 0}],
                [],  # false branch — silent stop
            ]
        },
    }

    return {
        "name": existing_wf.get("name", "Linear Issue State → Notion Status Sync"),
        "nodes": nodes,
        "connections": connections,
        "settings": existing_wf.get("settings", {"executionOrder": "v1"}),
    }


def main():
    print(f"{'='*65}")
    print(f"  deploy_sla_webhook_extension.py  |  DRY_RUN={DRY_RUN}")
    print(f"{'='*65}\n")

    # ── 1. Fetch existing workflow ────────────────────────────────────────────
    print(f"Step 1/3 — Fetching workflow {WORKFLOW_ID}…")
    existing_wf = n8n("GET", f"/workflows/{WORKFLOW_ID}")
    print(f"  Found: {existing_wf.get('name', '?')}")
    print(f"  Nodes: {len(existing_wf.get('nodes', []))}")
    print(f"  Active: {existing_wf.get('active', False)}\n")

    # ── 2. Build updated workflow ─────────────────────────────────────────────
    print("Step 2/3 — Building updated workflow…")
    updated_body = build_workflow_body(existing_wf)
    print(f"  New nodes: {len(updated_body['nodes'])}")

    if DRY_RUN:
        print("\nDRY RUN — printing workflow JSON:\n")
        print(json.dumps(updated_body, indent=2)[:3000])
        print("\n… (truncated)")
        print("\nParse Event JS:")
        print(PARSE_EVENT_CODE[:500])
        print("\nBuild SLA Patch JS:")
        print(BUILD_PATCH_CODE[:500])
        return

    # ── 3. PUT updated workflow + activate ────────────────────────────────────
    print("\nStep 3/3 — Deploying…")
    n8n("PUT", f"/workflows/{WORKFLOW_ID}", updated_body)
    print("  PUT OK")

    n8n("POST", f"/workflows/{WORKFLOW_ID}/activate")
    print("  Activated OK")

    print(f"\n{'='*65}")
    print(f"  Workflow updated: {N8N_BASE}/workflow/{WORKFLOW_ID}")
    print(f"{'='*65}")
    print()
    print("New capabilities:")
    print("  - Priority changes → Notion Severity sync")
    print("  - Triage exit → sets Triaged At + SLA Resolution Deadline")
    print("  - Status sync (existing, preserved)")
    print()
    print("IMPORTANT: Open the workflow in n8n UI and verify the webhook")
    print("toggle is still active. Flip it off/on if events stop flowing.")


if __name__ == "__main__":
    main()
