#!/usr/bin/env python3
"""
Fix Duplicate Detection workflow — Feb 17, 2026

Root cause: Prepare Claude Prompt and Parse Claude Response use
runOnceForEachItem mode with $('NodeName').item paired-item references.
This breaks when items pass through HTTP Request nodes that don't preserve
pairedItem metadata, causing "Can't use .first() here" or
"json property isn't an object" errors.

Fix: Switch both Code nodes to runOnceForAllItems mode with index-based
pairing via .all(). Also avoid optional chaining (?.) for sandbox safety.
"""

import json
import urllib.request
import ssl
import sys
import creds

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")
WF_ID = "G4bxsv1nrzON6XXd"

try:
    ctx = ssl.create_default_context()
except Exception:
    ctx = None


def log(msg, indent=0):
    print(f"{'  ' * indent}{msg}")


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


# New code for Prepare Claude Prompt — runOnceForAllItems with index pairing
PREPARE_CLAUDE_CODE = """// Pair Notion issues with Linear tickets (runOnceForAllItems mode)
const issues = $('Split Issues').all();
const linearResponses = $input.all();
const results = [];

for (let i = 0; i < linearResponses.length; i++) {
  const issue = issues[i].json;
  const linearData = linearResponses[i].json;
  const issueNodes = (linearData.data && linearData.data.issues && linearData.data.issues.nodes) || [];

  if (issueNodes.length === 0) {
    results.push({ json: {
      page_id: issue.page_id,
      skip: true,
      duplicate_status: 'New',
      potential_duplicate_url: null
    }});
    continue;
  }

  const ticketsList = issueNodes.map((t, j) => {
    const desc = (t.description || '').substring(0, 200).replace(/\\n/g, ' ');
    const teamName = (t.team && t.team.name) || 'Unknown';
    const stateName = (t.state && t.state.name) || 'Unknown';
    return `${j+1}. [${t.identifier}] ${t.title}\\n   Team: ${teamName} | Status: ${stateName}\\n   URL: ${t.url}${desc ? '\\n   Desc: ' + desc : ''}`;
  }).join('\\n\\n');

  results.push({ json: {
    page_id: issue.page_id,
    issue_title: issue.issue_title,
    summary: issue.summary,
    category: issue.category,
    issue_type: issue.issue_type,
    linear_tickets_formatted: ticketsList,
    ticket_count: issueNodes.length,
    skip: false
  }});
}

return results;"""


# New code for Parse Claude Response — runOnceForAllItems with index pairing
PARSE_CLAUDE_CODE = """// Parse Claude duplicate detection responses (runOnceForAllItems mode)
const prompts = $('Prepare Claude Prompt').all();
const responses = $input.all();
const results = [];

for (let i = 0; i < responses.length; i++) {
  const resp = responses[i].json;
  const pageId = (prompts[i] && prompts[i].json && prompts[i].json.page_id) || '';
  let result;
  try {
    const text = resp.content[0].text.trim().replace(/^```json\\n?/, '').replace(/\\n?```$/, '').trim();
    result = JSON.parse(text);
  } catch (e) {
    result = {
      is_duplicate: false,
      confidence: 0,
      matched_ticket_url: null,
      matched_ticket_id: null,
      reasoning: 'Parse error: ' + e.message
    };
  }
  results.push({ json: {
    page_id: pageId,
    duplicate_status: result.is_duplicate ? 'Likely Duplicate' : 'New',
    potential_duplicate_url: result.matched_ticket_url || null,
    confidence: result.confidence || 0,
    reasoning: result.reasoning || ''
  }});
}

return results;"""


def main():
    log("=" * 60)
    log("Fix Duplicate Detection: Issues → Linear")
    log("=" * 60)

    # 1. Fetch workflow
    log("\n[1/4] Fetching workflow...")
    wf = n8n("GET", f"/workflows/{WF_ID}")
    log(f"Got: {wf['name']} ({len(wf['nodes'])} nodes)", 1)

    # 2. Deactivate
    log("[2/4] Deactivating...")
    try:
        n8n("POST", f"/workflows/{WF_ID}/deactivate")
        log("Deactivated", 1)
    except Exception as e:
        log(f"Deactivation warning: {e}", 1)

    # 3. Modify nodes
    log("[3/4] Modifying nodes...")
    modified = 0

    for node in wf["nodes"]:
        if node["name"] == "Prepare Claude Prompt":
            node["parameters"]["jsCode"] = PREPARE_CLAUDE_CODE
            # Switch from runOnceForEachItem to runOnceForAllItems
            if "mode" in node["parameters"]:
                del node["parameters"]["mode"]  # default is runOnceForAllItems
            log("✓ Prepare Claude Prompt: switched to runOnceForAllItems + index pairing", 1)
            modified += 1

        elif node["name"] == "Parse Claude Response":
            node["parameters"]["jsCode"] = PARSE_CLAUDE_CODE
            # Switch from runOnceForEachItem to runOnceForAllItems
            if "mode" in node["parameters"]:
                del node["parameters"]["mode"]  # default is runOnceForAllItems
            log("✓ Parse Claude Response: switched to runOnceForAllItems + index pairing", 1)
            modified += 1

    if modified != 2:
        log(f"ERROR: Expected 2 modifications, got {modified}")
        sys.exit(1)

    # PUT updated workflow
    put_body = {
        "name": wf["name"],
        "nodes": wf["nodes"],
        "connections": wf.get("connections", {}),
        "settings": wf.get("settings", {}),
    }
    result = n8n("PUT", f"/workflows/{WF_ID}", put_body)
    log(f"Workflow updated: {result.get('name', '?')}", 1)

    # 4. Reactivate
    log("[4/4] Reactivating...")
    try:
        n8n("POST", f"/workflows/{WF_ID}/activate")
        log("✓ Activated", 1)
    except Exception as e:
        log(f"Activation error: {e}", 1)
        try:
            n8n("PATCH", f"/workflows/{WF_ID}", {"active": True})
            log("✓ Activated (via PATCH)", 1)
        except Exception as e2:
            log(f"Could not activate: {e2}", 1)

    log("\n" + "=" * 60)
    log("FIX COMPLETE")
    log("=" * 60)
    log("Changes:")
    log("  Prepare Claude Prompt: runOnceForEachItem → runOnceForAllItems")
    log("  Parse Claude Response: runOnceForEachItem → runOnceForAllItems")
    log("  Both: $('Node').item → $('Node').all() with index pairing")
    log("  Both: removed optional chaining (?.) for sandbox safety")
    log("\nThe next execution (within 15 min) should succeed.")


if __name__ == "__main__":
    main()
