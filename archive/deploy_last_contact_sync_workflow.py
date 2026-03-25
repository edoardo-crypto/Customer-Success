#!/usr/bin/env python3
"""
deploy_last_contact_sync_workflow.py

Deploys "📞 Daily Last Contact Date Sync" n8n workflow.

Schedule: Daily 23:30 Europe/Berlin
What it does:
  - Computes today's date and Unix-second bounds
  - Loads all MCT customers into domain + name lookup maps
  - Fetches Alex's and Aya's Google Calendar events for today
  - Fetches Intercom conversations closed today (by Alex or Aya)
  - Matches GCal attendee emails / event titles → MCT customers
  - Matches Intercom contact email domains → MCT customers
  - PATCHes '📞 Last Contact Date = today' for every unique matched customer

After deploy: toggle the workflow ON in the n8n UI to activate the schedule.
"""

import json
import uuid
import requests
import sys
import creds

# ── Constants ──────────────────────────────────────────────────────────────────
N8N_BASE    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")

NOTION_TOKEN   = creds.get("NOTION_TOKEN")
INTERCOM_TOKEN = creds.get("INTERCOM_TOKEN")

MCT_DS_ID        = "3ceb1ad0-91f1-40db-945a-c51c58035898"
NOTION_CRED_ID   = "LH587kxanQCPcd9y"
NOTION_CRED_NAME = "Notion - Enrichment"
GCAL_ALEX_CRED      = "xOx94JAFWoaxC9mF"
GCAL_ALEX_CRED_NAME = "Google Calendar - Alex"
GCAL_AYA_CRED       = "yBKyR84fOJ6Uf3tQ"
GCAL_AYA_CRED_NAME  = "Google Calendar - Aya"

WORKFLOW_NAME = "\U0001F4DE Daily Last Contact Date Sync"  # 📞

MCT_URL = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}


def uid():
    return str(uuid.uuid4())


def notion_cred():
    return {"httpHeaderAuth": {"id": NOTION_CRED_ID, "name": NOTION_CRED_NAME}}


def notion_auth():
    return {"authentication": "genericCredentialType", "genericAuthType": "httpHeaderAuth"}


def notion_header_v3():
    return {
        "sendHeaders": True,
        "headerParameters": {
            "parameters": [{"name": "Notion-Version", "value": "2025-09-03"}]
        },
    }


# ── JS code blocks ─────────────────────────────────────────────────────────────
# All written as plain triple-quoted strings (not f-strings).
# Unicode escapes \\uXXXX become \uXXXX in the string value → JS interprets as emoji.
# No f-string interpolation to keep brace characters unambiguous.

# Node 2: Compute Today Bounds
# At 23:30 CET (UTC+1 winter / UTC+2 summer), UTC time is 22:30 or 21:30 —
# the same calendar day as Berlin. Safe to use UTC date directly.
COMPUTE_TODAY_BOUNDS_JS = """\
const now    = new Date();
const pad    = n => String(n).padStart(2, '0');
const todayStr = now.getUTCFullYear() + '-' + pad(now.getUTCMonth() + 1) + '-' + pad(now.getUTCDate());

// Full day in UTC seconds
const todayStartTs = Math.floor(new Date(todayStr + 'T00:00:00Z').getTime() / 1000);
const todayEndTs   = todayStartTs + 86399;  // 23:59:59

// Pre-build Intercom body to avoid }} in downstream templates.
// Filter: closed today (statistics.last_close_at) — open:false is implicit for closed convs.
const intercomBody = JSON.stringify({
    query: {
        operator: "AND",
        value: [
            { field: "open",                     operator: "=",  value: false        },
            { field: "statistics.last_close_at", operator: ">",  value: todayStartTs },
            { field: "statistics.last_close_at", operator: "<=", value: todayEndTs   },
        ],
    },
    pagination: { per_page: 150 },
});

console.log('[today-bounds] todayStr=' + todayStr + ' ts=' + todayStartTs + '..' + todayEndTs);
return [{ json: { todayStr, todayStartTs, todayEndTs, intercomBody } }];
"""

# Node 4: Prep MCT Page 2 Body — verbatim from deploy_customers_contacted_workflow.py
PREP_MCT_PAGE2_JS = """\
// Read has_more + next_cursor from Fetch MCT Page 1, build body for page 2 request.
const r = $('Fetch MCT Page 1').first().json;
const body = { page_size: 100 };
if (r.has_more && r.next_cursor) {
    body.start_cursor = r.next_cursor;
}
return [{ json: { body: JSON.stringify(body), skip: !r.has_more } }];
"""

# Node 6: Build MCT Lookup — verbatim from deploy_customers_contacted_workflow.py
BUILD_MCT_LOOKUP_JS = """\
// Merge page 1 + page 2 results, build domain_map and name_map for customer matching.
const page1         = $('Fetch MCT Page 1').first().json.results || [];
const skipPage2     = !!$('Prep MCT Page 2 Body').first().json.skip;
const page2Response = skipPage2 ? { results: [], has_more: false } : $input.first().json;
const page2         = page2Response.results || [];

// Deduplicate across pages by Notion page ID
const seen = new Set();
const all  = [...page1, ...page2].filter(r => {
    if (seen.has(r.id)) return false;
    seen.add(r.id);
    return true;
});

const GENERIC_DOMAINS = new Set([
    'gmail.com', 'hotmail.com', 'outlook.com', 'yahoo.com',
    'icloud.com', 'protonmail.com', 'live.com', 'me.com',
]);

const domain_map = {};  // domain (lowercase) -> { page_id, name }
const name_map   = {};  // company name (lowercase) -> { page_id, name }

for (const row of all) {
    const props = row.properties || {};

    // Company Name (title property)
    const name = (
        props['\\uD83C\\uDFE2 Company Name']?.title?.[0]?.plain_text || ''
    ).trim();

    // Domain (rich_text property)
    const rawDomain = (
        props['\\uD83C\\uDFE2 Domain']?.rich_text?.[0]?.plain_text || ''
    ).toLowerCase().trim();

    if (rawDomain && !GENERIC_DOMAINS.has(rawDomain)) {
        domain_map[rawDomain] = { page_id: row.id, name };
    }
    if (name) {
        name_map[name.toLowerCase()] = { page_id: row.id, name };
    }
}

// Warn loudly if the 200-row cap was hit — customers beyond row 200 are silently excluded.
if (page2Response.has_more) {
    console.warn('[mct-lookup] WARNING: MCT has >200 rows — some customers are missing from the lookup!');
}

console.log('[mct-lookup] Rows=' + all.length
    + ' domains=' + Object.keys(domain_map).length
    + ' names=' + Object.keys(name_map).length);

return [{ json: { domain_map, name_map } }];
"""

# Node 10: Build Patch List
# Reads GCal + Intercom data, returns ONE item per unique matched customer.
# Returns [] (empty) if nobody was contacted today → node 11 simply doesn't run.
BUILD_PATCH_LIST_JS = """\
const mctData    = $('Build MCT Lookup').first().json;
const domain_map = mctData.domain_map || {};
const name_map   = mctData.name_map   || {};
const todayStr   = $('Compute Today Bounds').first().json.todayStr;

const GENERIC_DOMAINS = new Set([
    'gmail.com', 'hotmail.com', 'outlook.com', 'yahoo.com',
    'icloud.com', 'protonmail.com', 'live.com', 'me.com', 'konvoai.com',
]);
const ALEX_ADMIN_ID = '7484673';
const AYA_ADMIN_ID  = '8411967';

function matchDomain(email) {
    if (!email || !email.includes('@')) return null;
    const domain = email.split('@')[1].toLowerCase();
    if (GENERIC_DOMAINS.has(domain)) return null;
    return domain_map[domain] ? domain_map[domain].page_id : null;
}

// Name-match fallback — threshold > 6 avoids short names (e.g. "Base", "Meta")
// causing false-positive matches on generic event titles.
function matchName(summary) {
    if (!summary) return null;
    const sumLc = summary.toLowerCase();
    for (const [nameLc, entry] of Object.entries(name_map)) {
        if (nameLc.length > 6 && sumLc.includes(nameLc)) {
            console.log('[last-contact] name-match: "' + nameLc + '" in "' + summary + '"');
            return entry.page_id;
        }
    }
    return null;
}

const contactedIds = new Set();

// ── GCal: Alex ────────────────────────────────────────────────────────────────
for (const event of ($('Fetch Alex GCal').first().json.items || [])) {
    if (event.status === 'cancelled') continue;
    const attendees = (event.attendees || []).filter(a =>
        a.email && !a.email.includes('konvoai.com') && !a.self
    );
    let pid = null;
    for (const att of attendees) { pid = matchDomain(att.email); if (pid) break; }
    if (!pid && attendees.length > 0) pid = matchName(event.summary);
    if (pid) contactedIds.add(pid);
}

// ── GCal: Aya ─────────────────────────────────────────────────────────────────
for (const event of ($('Fetch Aya GCal').first().json.items || [])) {
    if (event.status === 'cancelled') continue;
    const attendees = (event.attendees || []).filter(a =>
        a.email && !a.email.includes('konvoai.com') && !a.self
    );
    let pid = null;
    for (const att of attendees) { pid = matchDomain(att.email); if (pid) break; }
    if (!pid && attendees.length > 0) pid = matchName(event.summary);
    if (pid) contactedIds.add(pid);
}

// ── Intercom: closed conversations by Alex or Aya ─────────────────────────────
for (const conv of ($('Fetch Intercom Convs').first().json.conversations || [])) {
    const stats  = conv.statistics || {};
    const closer = String(stats.last_closed_by_id || '');
    if (closer !== ALEX_ADMIN_ID && closer !== AYA_ADMIN_ID) continue;
    const contacts = (conv.contacts && conv.contacts.contacts) || [];
    const email    = ((contacts[0] || {}).email || '').toLowerCase();
    if (!email || !email.includes('@')) continue;
    const domain = email.split('@')[1];
    if (GENERIC_DOMAINS.has(domain)) continue;
    const match = domain_map[domain];
    if (match) contactedIds.add(match.page_id);
}

console.log('[last-contact] Unique customers contacted today: ' + contactedIds.size);

if (contactedIds.size === 0) {
    console.log('[last-contact] Nothing to update — returning empty.');
    return [];   // n8n stops cleanly; Update Last Contact Date does not execute
}

// Pre-build PATCH body: same date + same property for all customers.
// \\uD83D\\uDCDE is the JS surrogate pair for U+1F4DE (📞 telephone receiver).
// JSON.stringify outputs the actual emoji character, which Notion accepts.
const patchBody = JSON.stringify({
    properties: {
        '\\uD83D\\uDCDE Last Contact Date \\uD83D\\uDD12': { date: { start: todayStr } }
    }
});

return [...contactedIds].map(page_id => ({ json: { page_id, patchBody } }));
"""


# ── Build workflow ─────────────────────────────────────────────────────────────

def build_workflow():
    xs = [i * 280 for i in range(11)]
    y  = 300

    nodes = [
        # ── 1. Schedule: Daily 23:30 ──────────────────────────────────────────
        {
            "id":          uid(),
            "name":        "Schedule: Daily 23:30",
            "type":        "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.2,
            "position":    [xs[0], y],
            "parameters": {
                "rule": {
                    "interval": [
                        {"field": "cronExpression", "expression": "30 23 * * *"}
                    ]
                },
                "timezone": "Europe/Berlin",
            },
        },

        # ── 2. Compute Today Bounds ───────────────────────────────────────────
        {
            "id":          uid(),
            "name":        "Compute Today Bounds",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    [xs[1], y],
            "parameters": {
                "mode":   "runOnceForAllItems",
                "jsCode": COMPUTE_TODAY_BOUNDS_JS,
            },
        },

        # ── 3. Fetch MCT Page 1 ───────────────────────────────────────────────
        {
            "id":          uid(),
            "name":        "Fetch MCT Page 1",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [xs[2], y],
            "credentials": notion_cred(),
            "parameters": {
                **notion_auth(),
                "method":         "POST",
                "url":            MCT_URL,
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                "body":           '{"page_size": 100}',
                **notion_header_v3(),
                "options": {},
            },
        },

        # ── 4. Prep MCT Page 2 Body ───────────────────────────────────────────
        {
            "id":          uid(),
            "name":        "Prep MCT Page 2 Body",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    [xs[3], y],
            "parameters": {
                "mode":   "runOnceForAllItems",
                "jsCode": PREP_MCT_PAGE2_JS,
            },
        },

        # ── 5. Fetch MCT Page 2 ───────────────────────────────────────────────
        # continueOnFail guards the case where MCT fits in one page
        {
            "id":          uid(),
            "name":        "Fetch MCT Page 2",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [xs[4], y],
            "credentials": notion_cred(),
            "parameters": {
                **notion_auth(),
                "method":         "POST",
                "url":            MCT_URL,
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                # Body was pre-built in Prep MCT Page 2 Body — plain field reference, no }} risk
                "body":           "={{ $json.body }}",
                **notion_header_v3(),
                "options": {"continueOnFail": True},
            },
        },

        # ── 6. Build MCT Lookup ───────────────────────────────────────────────
        {
            "id":          uid(),
            "name":        "Build MCT Lookup",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    [xs[5], y],
            "parameters": {
                "mode":   "runOnceForAllItems",
                "jsCode": BUILD_MCT_LOOKUP_JS,
            },
        },

        # ── 7. Fetch Alex GCal ────────────────────────────────────────────────
        # predefinedCredentialType → Google Calendar OAuth2 (Alex)
        {
            "id":          uid(),
            "name":        "Fetch Alex GCal",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [xs[6], y],
            "credentials": {
                "googleCalendarOAuth2Api": {
                    "id":   GCAL_ALEX_CRED,
                    "name": GCAL_ALEX_CRED_NAME,
                }
            },
            "parameters": {
                "authentication":     "predefinedCredentialType",
                "nodeCredentialType": "googleCalendarOAuth2Api",
                "method": "GET",
                "url":    "https://www.googleapis.com/calendar/v3/calendars/alex%40konvoai.com/events",
                "sendQuery": True,
                "queryParameters": {
                    "parameters": [
                        # todayStr + 'T00:00:00Z' — no }} inside the expression
                        {"name": "timeMin",      "value": "={{ $('Compute Today Bounds').first().json.todayStr + 'T00:00:00Z' }}"},
                        {"name": "timeMax",      "value": "={{ $('Compute Today Bounds').first().json.todayStr + 'T23:59:59Z' }}"},
                        {"name": "singleEvents", "value": "true"},
                        {"name": "maxResults",   "value": "2500"},
                        {"name": "orderBy",      "value": "startTime"},
                    ]
                },
                "options": {},
            },
        },

        # ── 8. Fetch Aya GCal ─────────────────────────────────────────────────
        {
            "id":          uid(),
            "name":        "Fetch Aya GCal",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [xs[7], y],
            "credentials": {
                "googleCalendarOAuth2Api": {
                    "id":   GCAL_AYA_CRED,
                    "name": GCAL_AYA_CRED_NAME,
                }
            },
            "parameters": {
                "authentication":     "predefinedCredentialType",
                "nodeCredentialType": "googleCalendarOAuth2Api",
                "method": "GET",
                "url":    "https://www.googleapis.com/calendar/v3/calendars/aya%40konvoai.com/events",
                "sendQuery": True,
                "queryParameters": {
                    "parameters": [
                        {"name": "timeMin",      "value": "={{ $('Compute Today Bounds').first().json.todayStr + 'T00:00:00Z' }}"},
                        {"name": "timeMax",      "value": "={{ $('Compute Today Bounds').first().json.todayStr + 'T23:59:59Z' }}"},
                        {"name": "singleEvents", "value": "true"},
                        {"name": "maxResults",   "value": "2500"},
                        {"name": "orderBy",      "value": "startTime"},
                    ]
                },
                "options": {},
            },
        },

        # ── 9. Fetch Intercom Convs ───────────────────────────────────────────
        # intercomBody was pre-built in node 2 — plain field reference, no }} risk
        {
            "id":          uid(),
            "name":        "Fetch Intercom Convs",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [xs[8], y],
            "parameters": {
                "method": "POST",
                "url":    "https://api.intercom.io/conversations/search",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {"name": "Authorization",    "value": f"Bearer {INTERCOM_TOKEN}"},
                        {"name": "Intercom-Version", "value": "2.11"},
                        {"name": "Accept",           "value": "application/json"},
                        {"name": "Content-Type",     "value": "application/json"},
                    ]
                },
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                # intercomBody was pre-built in node 2 — single field reference
                "body": "={{ $('Compute Today Bounds').first().json.intercomBody }}",
                "options": {},
            },
        },

        # ── 10. Build Patch List ──────────────────────────────────────────────
        # runOnceForAllItems — reads all prior nodes, outputs N items (one per
        # unique matched customer), or [] if nobody was contacted today.
        {
            "id":          uid(),
            "name":        "Build Patch List",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    [xs[9], y],
            "parameters": {
                "mode":   "runOnceForAllItems",
                "jsCode": BUILD_PATCH_LIST_JS,
            },
        },

        # ── 11. Update Last Contact Date ──────────────────────────────────────
        # n8n iterates automatically — runs once per item from node 10.
        # If node 10 returned [] (no contacts today) this node never executes.
        {
            "id":          uid(),
            "name":        "Update Last Contact Date",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [xs[10], y],
            "credentials": notion_cred(),
            "parameters": {
                **notion_auth(),
                "method": "PATCH",
                # Plain string concatenation — no }} risk
                "url":    "={{ 'https://api.notion.com/v1/pages/' + $json.page_id }}",
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                # patchBody was pre-built in node 10 — plain field reference
                "body": "={{ $json.patchBody }}",
                # MCT pages require Notion-Version 2025-09-03
                **notion_header_v3(),
                "options": {},
            },
        },
    ]

    # Linear chain: each node connects to the next; last node has no outgoing connection
    node_names  = [n["name"] for n in nodes]
    connections = {}
    for i in range(len(node_names) - 1):
        connections[node_names[i]] = {
            "main": [[{"node": node_names[i + 1], "type": "main", "index": 0}]]
        }

    return {
        "name":  WORKFLOW_NAME,
        "nodes": nodes,
        "connections": connections,
        "settings": {
            "executionOrder": "v1",
            "saveManualExecutions": True,
            "timezone": "Europe/Berlin",
        },
    }


# ── n8n API helpers ────────────────────────────────────────────────────────────

def list_workflows():
    r = requests.get(f"{N8N_BASE}/api/v1/workflows", headers=N8N_HEADERS)
    r.raise_for_status()
    return r.json().get("data", [])


def create_workflow(wf_body):
    r = requests.post(f"{N8N_BASE}/api/v1/workflows", headers=N8N_HEADERS, json=wf_body)
    if r.status_code not in (200, 201):
        print(f"  CREATE failed: {r.status_code} — {r.text[:600]}")
        r.raise_for_status()
    return r.json()


def delete_workflow(wf_id):
    r = requests.delete(f"{N8N_BASE}/api/v1/workflows/{wf_id}", headers=N8N_HEADERS)
    print(f"  Deleted workflow {wf_id} (HTTP {r.status_code})")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("deploy_last_contact_sync_workflow.py")
    print(f"Deploying: {WORKFLOW_NAME!r}")
    print("=" * 65)

    # Step 1: Check for existing workflow with same name
    print(f"\n[1/3] Checking for existing workflow named {WORKFLOW_NAME!r} …")
    existing   = list_workflows()
    duplicates = [w for w in existing if w.get("name") == WORKFLOW_NAME]
    if duplicates:
        print(f"  Found {len(duplicates)} existing workflow(s):")
        for d in duplicates:
            print(f"    ID={d['id']}  active={d.get('active')}")
        answer = input("  Delete and redeploy? [y/N]: ").strip().lower()
        if answer == "y":
            for d in duplicates:
                delete_workflow(d["id"])
        else:
            print("  Aborting.")
            sys.exit(0)
    else:
        print("  No existing workflow found — creating fresh.")

    # Step 2: Build workflow JSON
    print("\n[2/3] Building workflow JSON …")
    wf_body = build_workflow()
    print(f"  Nodes ({len(wf_body['nodes'])}):")
    for i, node in enumerate(wf_body["nodes"], 1):
        print(f"    {i:2}. {node['name']}")

    save_path = "/tmp/last_contact_sync_workflow.json"
    with open(save_path, "w") as f:
        json.dump(wf_body, f, indent=2)
    print(f"  Saved to {save_path}")

    # Step 3: Create in n8n
    print("\n[3/3] Creating workflow in n8n …")
    result = create_workflow(wf_body)
    wf_id  = result.get("id", "?")
    print(f"  ✓ Created  ID={wf_id}  active={result.get('active')}")

    print()
    print("=" * 65)
    print("IMPORTANT: Manual activation required")
    print("=" * 65)
    print()
    print("Scheduled triggers only start when you toggle the workflow ON")
    print("in the n8n UI (the API cannot register the schedule otherwise).")
    print()
    print(f"  1. Open:  {N8N_BASE}/workflow/{wf_id}")
    print("  2. Click the 'Active' toggle (top-right).")
    print("  3. Confirm it turns green.")
    print()
    print("To test immediately:")
    print("  4. Click 'Execute Workflow' (triangle/play button).")
    print("  5. Node 10 should log '[last-contact] Unique customers contacted today: N'")
    print("  6. Node 11 should show N successful PATCH calls (HTTP 200).")
    print("  7. In Notion MCT, those customers should have '📞 Last Contact Date = today'.")
    print()
    print("What this workflow does each evening at 23:30 CET:")
    print("  • Loads MCT customer list (domain + name lookup maps)")
    print("  • Fetches today's GCal events for Alex and Aya")
    print("  • Fetches Intercom conversations closed today by Alex or Aya")
    print("  • Matches attendee email domains / event titles → MCT page IDs")
    print("  • Deduplicates, then PATCHes '📞 Last Contact Date = today'")
    print("    for every unique matched customer")
    print("  • If nobody was contacted, does nothing (node 11 is skipped cleanly)")
    print()
    print(f"  Workflow JSON: {save_path}")


if __name__ == "__main__":
    main()
