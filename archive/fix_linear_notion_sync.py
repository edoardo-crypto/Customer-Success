#!/usr/bin/env python3
"""Fix Linear Issue State → Notion Status Sync workflow (ce8BpceG04fjgOCz).

Fixes applied:
  Bug 1 (CRITICAL): Notion URL search prefix collision — append "/" to identifier
                    so "KON-4/" never matches "KON-42/".
  Bug 2 (MINOR):    "testing" state name mapped to Resolved → moved to In Progress.
"""

import json
import ssl
import urllib.request
import urllib.error

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = (
    "***REMOVED_JWT***"
    ".eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJpc3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleHAiOjE3NzMyNzAwMDB9"
    ".X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)
WORKFLOW_ID = "ce8BpceG04fjgOCz"

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
        print(f"  HTTP {e.code}: {body_text}")
        raise


# ---------------------------------------------------------------------------
# Corrected Code node — Parse & Map State
# Fix 1: identifier + '/' prevents prefix collision in Notion URL filter
# Fix 2: 'testing' moved from Resolved branch → In Progress branch
# ---------------------------------------------------------------------------
CORRECTED_PARSE_CODE = r"""
const items = $input.all();
const body = items[0].json;

const { action, type, data, updatedFrom } = body;

// Guard: only issue state-change updates
if (action !== 'update' || type !== 'Issue') return [];
if (!data || !data.identifier) return [];

const stateChanged = updatedFrom && (updatedFrom.stateId || updatedFrom.state?.id);
if (!stateChanged) return [];

const stateType = data.state?.type;
const stateName = (data.state?.name || '').toLowerCase();
let notionStatus = null;

if (stateType === 'started') {
  notionStatus = 'In Progress';
} else if (stateType === 'completed') {
  notionStatus = 'Resolved';
} else if (!stateType) {
  // Fallback: name-based matching for teams with non-standard state types
  if (stateName.includes('progress') || stateName.includes('review') || stateName.includes('testing')) {
    notionStatus = 'In Progress';
  } else if (
    stateName.includes('done') || stateName.includes('released') ||
    stateName.includes('complete') || stateName.includes('resolved')
  ) {
    notionStatus = 'Resolved';
  }
}
if (!notionStatus) return [];

const identifier = data.identifier; // e.g. "KON-42"

// FIX 1: append "/" so "KON-4/" doesn't collide with "KON-42/"
// Linear URLs always have format: .../issue/{identifier}/{slug}
const searchBody = JSON.stringify({
  filter: { property: 'Linear Ticket URL', url: { contains: identifier + '/' } },
  page_size: 1,
});

const props = { Status: { select: { name: notionStatus } } };
if (notionStatus === 'Resolved') {
  props['Resolved At'] = { date: { start: new Date().toISOString() } };
}
const patchBodyStr = JSON.stringify({ properties: props });

return [{ json: { identifier, notionStatus, searchBody, patchBodyStr } }];
""".strip()


def main():
    # Step 1: GET current workflow
    print(f"Step 1/3 — Fetching workflow {WORKFLOW_ID}...")
    wf = n8n("GET", f"/workflows/{WORKFLOW_ID}")
    print(f"  + Fetched  name='{wf['name']}'  nodes={len(wf['nodes'])}")

    # Step 2: Find node n2 (Parse & Map State) and replace its jsCode
    print("Step 2/3 — Patching 'Parse & Map State' node (n2)...")
    nodes = wf["nodes"]
    patched = False
    for node in nodes:
        if node.get("id") == "n2" or node.get("name") == "Parse & Map State":
            old_code = node["parameters"].get("jsCode", "")
            node["parameters"]["jsCode"] = CORRECTED_PARSE_CODE
            patched = True
            old_has_slash = "identifier + '/'" in old_code
            old_testing_resolved = ("'testing'" in old_code and
                                    "notionStatus = 'Resolved'" in old_code.split("'testing'")[1][:200])
            print(f"  Node found: id={node.get('id')!r}  name={node.get('name')!r}")
            print(f"  Fix 1 (append '/'): {'already applied' if old_has_slash else 'APPLIED'}")
            print(f"  Fix 2 (testing->InProgress): {'already applied' if not old_testing_resolved else 'APPLIED'}")
            break

    if not patched:
        print("  ERROR: Could not find node 'Parse & Map State' (id=n2)")
        raise SystemExit(1)

    # Step 3: PUT updated workflow — only allowed fields
    print("Step 3/3 — Uploading corrected workflow...")
    put_body = {
        "name": wf["name"],
        "nodes": nodes,
        "connections": wf["connections"],
        "settings": wf.get("settings", {}),
    }
    result = n8n("PUT", f"/workflows/{WORKFLOW_ID}", put_body)
    print(f"  + Updated  id={result['id']}  updatedAt={result.get('updatedAt', 'n/a')}")

    # Verification: re-fetch and confirm
    print()
    print("Verification — re-fetching to confirm jsCode...")
    verify = n8n("GET", f"/workflows/{WORKFLOW_ID}")
    for node in verify["nodes"]:
        if node.get("id") == "n2" or node.get("name") == "Parse & Map State":
            deployed_code = node["parameters"].get("jsCode", "")
            ok1 = "identifier + '/'" in deployed_code
            idx_testing = deployed_code.find("'testing'")
            idx_resolved_branch = deployed_code.find("stateName.includes('done')")
            ok2 = idx_testing != -1 and idx_resolved_branch != -1 and idx_testing < idx_resolved_branch
            print(f"  Fix 1 deployed (contains '/'): {'OK' if ok1 else 'FAILED'}")
            print(f"  Fix 2 deployed (testing in InProgress branch): {'OK' if ok2 else 'FAILED'}")
            break

    print()
    print("=" * 60)
    print(f"  Workflow : {N8N_BASE}/workflow/{WORKFLOW_ID}")
    print(f"  Webhook  : {N8N_BASE}/webhook/linear-issue-sync")
    print("=" * 60)
    print()
    print("Manual verification steps:")
    print("  1. Send a test webhook for a real issue (e.g. KON-4) via curl")
    print("  2. Confirm only the KON-4 Notion row is updated (not KON-42 etc.)")
    print("  3. Confirm cancelled-type payload returns [] and touches nothing")


if __name__ == "__main__":
    main()
