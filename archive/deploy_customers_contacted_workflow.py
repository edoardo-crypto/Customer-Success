#!/usr/bin/env python3
"""
deploy_customers_contacted_workflow.py

Deploys "📞 Weekly Customers Contacted Tracker" n8n workflow.

Schedule: Daily 07:00 Europe/Berlin
What it does:
  - Computes current week bounds (Monday → Sunday of the current ISO week)
  - Loads all MCT customers into domain + name lookup maps
  - Fetches Alex's and Aya's Google Calendar events for the week
  - Fetches Intercom conversations closed this week
  - Matches GCal events → MCT customers via attendee email domain / event title
  - Fetches Intercom contact emails, matches → MCT via email domain
  - Computes unique customer counts (union of GCal + Intercom) per admin
  - Updates "Alex/Aya: Customers Contacted" in the weekly scorecard Notion row

After deploy: toggle the workflow ON in the n8n UI to activate the schedule.
"""

import json
import uuid
import requests
import sys
import creds

# ── Constants ──────────────────────────────────────────────────────────────────
N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = creds.get("N8N_API_KEY")

NOTION_TOKEN   = creds.get("NOTION_TOKEN")
INTERCOM_TOKEN = creds.get("INTERCOM_TOKEN")

# Scorecard DB — confirmed at deploy time by confirm_scorecard_db() below.
# W09 page: 311e418f-d8c4-81b1-8552-d12c067c1089  (PAGE inside the DB)
# Actual DB:  311e418f-d8c4-810e-8b11-cdc50357e709  (confirmed Feb 25 2026)
SCORECARD_DB_ID     = "311e418f-d8c4-810e-8b11-cdc50357e709"
MCT_DS_ID           = "3ceb1ad0-91f1-40db-945a-c51c58035898"
W09_PAGE_ID         = "311e418fd8c481b18552d12c067c1089"  # used to confirm SCORECARD_DB_ID

NOTION_CRED_ID      = "LH587kxanQCPcd9y"
NOTION_CRED_NAME    = "Notion - Enrichment"
GCAL_ALEX_CRED      = "xOx94JAFWoaxC9mF"
GCAL_ALEX_CRED_NAME = "Google Calendar - Alex"
GCAL_AYA_CRED       = "yBKyR84fOJ6Uf3tQ"
GCAL_AYA_CRED_NAME  = "Google Calendar - Aya"

WORKFLOW_NAME = "\U0001F4DE Weekly Customers Contacted Tracker"  # 📞

MCT_URL          = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"
SCORECARD_DB_URL = f"https://api.notion.com/v1/databases/{SCORECARD_DB_ID}/query"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}
NOTION_HEADERS_V2 = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}


def uid():
    return str(uuid.uuid4())


def notion_cred():
    return {"httpHeaderAuth": {"id": NOTION_CRED_ID, "name": NOTION_CRED_NAME}}


def notion_auth():
    return {"authentication": "genericCredentialType", "genericAuthType": "httpHeaderAuth"}


def notion_header_v2():
    return {
        "sendHeaders": True,
        "headerParameters": {
            "parameters": [{"name": "Notion-Version", "value": "2022-06-28"}]
        },
    }


def notion_header_v3():
    return {
        "sendHeaders": True,
        "headerParameters": {
            "parameters": [{"name": "Notion-Version", "value": "2025-09-03"}]
        },
    }


# ── JS code blocks ─────────────────────────────────────────────────────────────
# All written as plain triple-quoted strings (not f-strings).
# Unicode escapes \\uXXXX become \uXXXX in string value → JS interprets as emoji.
# No f-string interpolation to keep brace characters unambiguous.

# Node 2: Compute Week Bounds
COMPUTE_WEEK_BOUNDS_JS = """\
// Week window: current ISO week Monday 00:00 UTC → Sunday 23:59:59 UTC
// Consistent regardless of which day this runs — Mon, mid-week, or Sun.
const now = new Date();
const dayOfWeek = now.getUTCDay();  // 0=Sun, 1=Mon, ..., 6=Sat (UTC)
const daysToMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;

const monday = new Date(now);
monday.setUTCDate(monday.getUTCDate() - daysToMonday);
monday.setUTCHours(0, 0, 0, 0);

const sunday = new Date(monday);
sunday.setUTCDate(monday.getUTCDate() + 6);
sunday.setUTCHours(23, 59, 59, 999);

const pad = n => String(n).padStart(2, '0');
const toDateStr = d => d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate());

const weekStart   = toDateStr(monday);
const weekEnd     = toDateStr(sunday);
const weekStartTs = Math.floor(monday.getTime() / 1000);
const weekEndTs   = Math.floor(sunday.getTime() / 1000);

// Pre-build Intercom search body to avoid }} inside template expressions downstream
const intercomBody = JSON.stringify({
    query: {
        operator: "AND",
        value: [
            { field: "created_at", operator: ">",  value: weekStartTs },
            { field: "created_at", operator: "<=", value: weekEndTs   },
        ],
    },
    pagination: { per_page: 150 },
});

console.log('[week-bounds] weekStart=' + weekStart + ' weekEnd=' + weekEnd
    + ' ts=' + weekStartTs + '..' + weekEndTs);

return [{ json: { weekStart, weekEnd, weekStartTs, weekEndTs, intercomBody } }];
"""

# Node 4: Prep MCT Page 2 Body
PREP_MCT_PAGE2_JS = """\
// Read has_more + next_cursor from Fetch MCT Page 1, build body for page 2 request.
const r = $('Fetch MCT Page 1').first().json;
const body = { page_size: 100 };
if (r.has_more && r.next_cursor) {
    body.start_cursor = r.next_cursor;
}
return [{ json: { body: JSON.stringify(body), skip: !r.has_more } }];
"""

# Node 6: Build MCT Lookup
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

    // Domain (rich_text property, same emoji prefix as Company Name)
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

# Node 10: Match GCal + Extract Contact IDs
MATCH_GCAL_EXTRACT_JS = """\
// 1. Match GCal events to MCT customers via attendee email domain, then event title.
// 2. Match Intercom contacts directly from the search response — email is available at
//    conv.contacts.contacts[0].email without any extra API call.
// Output: single item with four arrays: alex_gcal_ids, aya_gcal_ids,
//         alex_intercom_ids, aya_intercom_ids.

const mctData    = $('Build MCT Lookup').first().json;
const domain_map = mctData.domain_map || {};
const name_map   = mctData.name_map   || {};

const GENERIC_DOMAINS = new Set([
    'gmail.com', 'hotmail.com', 'outlook.com', 'yahoo.com',
    'icloud.com', 'protonmail.com', 'live.com', 'me.com', 'konvoai.com',
]);

function matchDomain(email) {
    if (!email || !email.includes('@')) return null;
    const domain = email.split('@')[1].toLowerCase();
    if (GENERIC_DOMAINS.has(domain)) return null;
    return domain_map[domain] ? domain_map[domain].page_id : null;
}

// Name-match fallback — threshold > 6 avoids short names (e.g. "Base", "Meta")
// causing false-positive matches on internal event titles.
// Only tried when the event has external attendees (none matched by domain).
function matchName(summary) {
    if (!summary) return null;
    const sumLc = summary.toLowerCase();
    for (const [nameLc, entry] of Object.entries(name_map)) {
        if (nameLc.length > 6 && sumLc.includes(nameLc)) {
            console.log('[match-gcal] name-match hit: "' + nameLc + '" in "' + summary + '"');
            return entry.page_id;
        }
    }
    return null;
}

// ── Process Alex GCal events ─────────────────────────────────────────────────
const alexEvents    = $('Fetch Alex GCal').first().json.items || [];
const alex_gcal_ids = [];
const alexSeen      = new Set();

for (const event of alexEvents) {
    if (event.status === 'cancelled') continue;
    const attendees = (event.attendees || []).filter(a =>
        a.email && !a.email.includes('konvoai.com') && !a.self
    );
    let pageId = null;
    for (const att of attendees) {
        pageId = matchDomain(att.email);
        if (pageId) break;
    }
    // Name-match fallback: only when external attendees exist but none matched by domain
    if (!pageId && attendees.length > 0) pageId = matchName(event.summary);
    if (pageId && !alexSeen.has(pageId)) {
        alexSeen.add(pageId);
        alex_gcal_ids.push(pageId);
    }
}

// ── Process Aya GCal events ──────────────────────────────────────────────────
const ayaEvents    = $('Fetch Aya GCal').first().json.items || [];
const aya_gcal_ids = [];
const ayaSeen      = new Set();

for (const event of ayaEvents) {
    if (event.status === 'cancelled') continue;
    const attendees = (event.attendees || []).filter(a =>
        a.email && !a.email.includes('konvoai.com') && !a.self
    );
    let pageId = null;
    for (const att of attendees) {
        pageId = matchDomain(att.email);
        if (pageId) break;
    }
    // Name-match fallback: only when external attendees exist but none matched by domain
    if (!pageId && attendees.length > 0) pageId = matchName(event.summary);
    if (pageId && !ayaSeen.has(pageId)) {
        ayaSeen.add(pageId);
        aya_gcal_ids.push(pageId);
    }
}

// ── Match Intercom contacts (email directly from search response) ─────────────
// conv.contacts.contacts[0].email is populated by the search API — no extra fetch needed.
const ALEX_ADMIN_ID = '7484673';
const AYA_ADMIN_ID  = '8411967';

const convs             = $('Fetch Intercom Convs').first().json.conversations || [];
const alex_intercom_ids = [];
const aya_intercom_ids  = [];
const alexIntercomSeen  = new Set();
const ayaIntercomSeen   = new Set();

for (const conv of convs) {
    const stats  = conv.statistics || {};
    const closer = String(stats.last_closed_by_id || '');
    if (![ALEX_ADMIN_ID, AYA_ADMIN_ID].includes(closer)) continue;

    const contacts     = (conv.contacts && conv.contacts.contacts) || [];
    const firstContact = contacts[0] || {};
    const email        = (firstContact.email || '').toLowerCase();
    if (!email || !email.includes('@')) continue;

    const domain = email.split('@')[1];
    if (GENERIC_DOMAINS.has(domain)) continue;

    const match = domain_map[domain];
    if (!match) continue;

    const page_id = match.page_id;
    if (closer === ALEX_ADMIN_ID && !alexIntercomSeen.has(page_id)) {
        alexIntercomSeen.add(page_id);
        alex_intercom_ids.push(page_id);
    } else if (closer === AYA_ADMIN_ID && !ayaIntercomSeen.has(page_id)) {
        ayaIntercomSeen.add(page_id);
        aya_intercom_ids.push(page_id);
    }
}

console.log('[match-gcal] Alex GCal: '      + alex_gcal_ids.length    + ' customers');
console.log('[match-gcal] Aya GCal: '        + aya_gcal_ids.length     + ' customers');
console.log('[match-intercom] Alex Intercom: ' + alex_intercom_ids.length + ' customers');
console.log('[match-intercom] Aya Intercom: '  + aya_intercom_ids.length  + ' customers');

return [{ json: { alex_gcal_ids, aya_gcal_ids, alex_intercom_ids, aya_intercom_ids } }];
"""

# Node 11: Match Intercom + Union + Count
MATCH_INTERCOM_UNION_JS = """\
// Union of GCal + Intercom customer sets, compute counts, build Notion update bodies.
// Email matching was done inline in node 10 — no extra Intercom API call needed.

const data              = $input.first().json;
const alex_gcal_ids     = data.alex_gcal_ids     || [];
const aya_gcal_ids      = data.aya_gcal_ids      || [];
const alex_intercom_ids = data.alex_intercom_ids || [];
const aya_intercom_ids  = data.aya_intercom_ids  || [];

// Union of GCal + Intercom sets (unique page_ids per admin)
const alexSet   = new Set([...alex_gcal_ids, ...alex_intercom_ids]);
const ayaSet    = new Set([...aya_gcal_ids,  ...aya_intercom_ids]);
const alexCount = alexSet.size;
const ayaCount  = ayaSet.size;

const weekStart = $('Compute Week Bounds').first().json.weekStart;

// Pre-build Notion query body (filter scorecard row by Week Start date).
// Avoids }} patterns in the HTTP Request template expression for Find Scorecard Row.
const scorecardFilterBody = JSON.stringify({
    filter: { property: 'Week Start', date: { equals: weekStart } },
    page_size: 1,
});

// Pre-build PATCH body for Update Scorecard Row.
// Avoids }} patterns in the HTTP Request template expression.
const updateBody = JSON.stringify({
    properties: {
        'Alex: Customers Contacted': { number: alexCount },
        'Aya: Customers Contacted':  { number: ayaCount  },
    },
});

console.log('[union-count] Alex: ' + alexCount
    + ' (gcal=' + alex_gcal_ids.length + ', intercom=' + alex_intercom_ids.length + ')');
console.log('[union-count] Aya: '  + ayaCount
    + ' (gcal=' + aya_gcal_ids.length  + ', intercom=' + aya_intercom_ids.length  + ')');

return [{ json: { alexCount, ayaCount, weekStart, scorecardFilterBody, updateBody } }];
"""


# ── Build workflow ─────────────────────────────────────────────────────────────

def build_workflow(scorecard_db_id=None):
    # x positions for the linear chain (280px spacing, 13 nodes)
    xs = [i * 280 for i in range(13)]
    y = 300

    # Use confirmed DB ID from Notion page lookup; fall back to hardcoded constant.
    _scorecard_db_url = (
        f"https://api.notion.com/v1/databases/{scorecard_db_id}/query"
        if scorecard_db_id else SCORECARD_DB_URL
    )

    nodes = [
        # ── 1. Schedule Trigger — daily 07:00 Europe/Berlin ──────────────────
        {
            "id":          uid(),
            "name":        "Schedule Trigger",
            "type":        "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.2,
            "position":    [xs[0], y],
            "parameters": {
                "rule": {
                    "interval": [
                        {"field": "cronExpression", "expression": "0 7 * * *"}
                    ]
                },
                "timezone": "Europe/Berlin",
            },
        },

        # ── 2. Compute Week Bounds ────────────────────────────────────────────
        {
            "id":          uid(),
            "name":        "Compute Week Bounds",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    [xs[1], y],
            "parameters": {
                "mode":   "runOnceForAllItems",
                "jsCode": COMPUTE_WEEK_BOUNDS_JS,
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
                "authentication":    "predefinedCredentialType",
                "nodeCredentialType": "googleCalendarOAuth2Api",
                "method": "GET",
                "url":    "https://www.googleapis.com/calendar/v3/calendars/alex%40konvoai.com/events",
                "sendQuery": True,
                "queryParameters": {
                    "parameters": [
                        # weekStart + 'T00:00:00Z' — no }} inside the expression
                        {"name": "timeMin",        "value": "={{ $('Compute Week Bounds').first().json.weekStart + 'T00:00:00Z' }}"},
                        {"name": "timeMax",        "value": "={{ $('Compute Week Bounds').first().json.weekEnd   + 'T23:59:59Z' }}"},
                        {"name": "singleEvents",   "value": "true"},
                        {"name": "maxResults",     "value": "2500"},
                        {"name": "orderBy",        "value": "startTime"},
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
                "authentication":    "predefinedCredentialType",
                "nodeCredentialType": "googleCalendarOAuth2Api",
                "method": "GET",
                "url":    "https://www.googleapis.com/calendar/v3/calendars/aya%40konvoai.com/events",
                "sendQuery": True,
                "queryParameters": {
                    "parameters": [
                        {"name": "timeMin",        "value": "={{ $('Compute Week Bounds').first().json.weekStart + 'T00:00:00Z' }}"},
                        {"name": "timeMax",        "value": "={{ $('Compute Week Bounds').first().json.weekEnd   + 'T23:59:59Z' }}"},
                        {"name": "singleEvents",   "value": "true"},
                        {"name": "maxResults",     "value": "2500"},
                        {"name": "orderBy",        "value": "startTime"},
                    ]
                },
                "options": {},
            },
        },

        # ── 9. Fetch Intercom Convs ───────────────────────────────────────────
        # Body was pre-built in Compute Week Bounds — plain field reference, no }} risk
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
                # intercomBody was pre-built in node 2 — plain field reference
                "body": "={{ $('Compute Week Bounds').first().json.intercomBody }}",
                "options": {},
            },
        },

        # ── 10. Match GCal + Extract Contact IDs ─────────────────────────────
        {
            "id":          uid(),
            "name":        "Match GCal + Extract Contact IDs",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    [xs[9], y],
            "parameters": {
                "mode":   "runOnceForAllItems",
                "jsCode": MATCH_GCAL_EXTRACT_JS,
            },
        },

        # ── 11. Match Intercom + Union + Count ───────────────────────────────
        # Reads alex/aya_gcal_ids + alex/aya_intercom_ids from node 10 output.
        {
            "id":          uid(),
            "name":        "Match Intercom + Union + Count",
            "type":        "n8n-nodes-base.code",
            "typeVersion": 2,
            "position":    [xs[10], y],
            "parameters": {
                "mode":   "runOnceForAllItems",
                "jsCode": MATCH_INTERCOM_UNION_JS,
            },
        },

        # ── 12. Find Scorecard Row ────────────────────────────────────────────
        # scorecardFilterBody was pre-built in node 11 — plain field reference
        {
            "id":          uid(),
            "name":        "Find Scorecard Row",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [xs[11], y],
            "credentials": notion_cred(),
            "parameters": {
                **notion_auth(),
                "method":         "POST",
                "url":            _scorecard_db_url,
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                "body": "={{ $json.scorecardFilterBody }}",
                **notion_header_v2(),
                "options": {},
            },
        },

        # ── 13. Update Scorecard Row ──────────────────────────────────────────
        # updateBody was pre-built in node 11.
        # No continueOnFail — if the scorecard row is missing, we want n8n to
        # surface this as a visible failure rather than silently skipping it.
        {
            "id":          uid(),
            "name":        "Update Scorecard Row",
            "type":        "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position":    [xs[12], y],
            "credentials": notion_cred(),
            "parameters": {
                **notion_auth(),
                "method": "PATCH",
                "url": "={{ 'https://api.notion.com/v1/pages/' + $('Find Scorecard Row').first().json.results[0].id }}",
                "sendBody":       True,
                "contentType":    "raw",
                "rawContentType": "application/json",
                # updateBody was pre-built in node 11 — plain field reference
                "body": "={{ $('Match Intercom + Union + Count').first().json.updateBody }}",
                **notion_header_v2(),
                "options": {},
            },
        },
    ]

    # Linear chain: each node connects to the next
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


def confirm_scorecard_db():
    """Query the W09 Notion page to confirm SCORECARD_DB_ID."""
    url = f"https://api.notion.com/v1/pages/{W09_PAGE_ID}"
    r   = requests.get(url, headers=NOTION_HEADERS_V2)
    if r.status_code != 200:
        print(f"  [warn] Could not fetch W09 page: {r.status_code} — using constant SCORECARD_DB_ID")
        return SCORECARD_DB_ID
    page   = r.json()
    db_id  = page.get("parent", {}).get("database_id", "")
    # normalize to no-dash for comparison
    actual = db_id.replace("-", "")
    expect = SCORECARD_DB_ID.replace("-", "")
    if actual == expect:
        print(f"  ✓ SCORECARD_DB_ID confirmed: {db_id}")
    else:
        print(f"  [warn] SCORECARD_DB_ID mismatch!")
        print(f"         constant = {SCORECARD_DB_ID}")
        print(f"         actual   = {db_id}")
        print(f"         Using actual value from Notion page.")
        return db_id
    return SCORECARD_DB_ID


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("deploy_customers_contacted_workflow.py")
    print(f"Deploying: {WORKFLOW_NAME!r}")
    print("=" * 65)

    # Step 0: Confirm SCORECARD_DB_ID
    print("\n[0/4] Confirming Scorecard DB ID via Notion …")
    confirmed_scorecard_db_id = confirm_scorecard_db()

    # Step 1: Check for existing workflow with same name
    print(f"\n[1/4] Checking for existing workflow named {WORKFLOW_NAME!r} …")
    existing    = list_workflows()
    duplicates  = [w for w in existing if w.get("name") == WORKFLOW_NAME]
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

    # Step 2: Build workflow JSON
    print("\n[2/4] Building workflow JSON …")
    wf_body = build_workflow(scorecard_db_id=confirmed_scorecard_db_id)
    print(f"  Nodes ({len(wf_body['nodes'])}):")
    for i, node in enumerate(wf_body["nodes"], 1):
        print(f"    {i:2}. {node['name']}")

    # Save for inspection
    save_path = "/tmp/customers_contacted_workflow.json"
    with open(save_path, "w") as f:
        json.dump(wf_body, f, indent=2)
    print(f"  Saved to {save_path}")

    # Step 3: Create in n8n
    print("\n[3/4] Creating workflow in n8n …")
    result = create_workflow(wf_body)
    wf_id  = result.get("id", "?")
    print(f"  ✓ Created  ID={wf_id}  active={result.get('active')}")

    # Step 4: Instructions
    print("\n[4/4] Deployment complete.")
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
    print("  5. Check the W09 Notion scorecard row — 'Alex/Aya: Customers Contacted'")
    print("     should be updated with the new unique-customer counts.")
    print()
    print("What this workflow does each morning at 07:00 CET:")
    print("  • Loads MCT customer list (domain + name lookup maps)")
    print("  • Fetches Alex's and Aya's GCal events for Mon → yesterday")
    print("  • Fetches Intercom conversations closed this week")
    print("  • Matches events/contacts to MCT customers by email domain")
    print("  • Computes unique customer counts, unions GCal + Intercom sets")
    print("  • Patches the scorecard Notion row with the final counts")
    print()
    print(f"  Workflow JSON: {save_path}")


if __name__ == "__main__":
    main()
