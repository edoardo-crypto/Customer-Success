#!/usr/bin/env python3
"""
deploy_scorecard_builder_workflow.py

Deploys "📊 Weekly Scorecard Builder" n8n workflow.

Schedule: Monday 06:00 CET — runs 1 hour before the KPI 6 workflow (07:00),
so the scorecard row is created/updated before KPI 6 tries to patch it.

What it does:
  - Computes last week's bounds (Mon 00:00 UTC → Sun 23:59 UTC)
  - In parallel: checks if current week's shell row exists; creates it if not
    (so the Customers Contacted Tracker can find it at 07:00 the same morning)
  - Fetches all MCT customers (paginated, 2 pages × 100)
  - Counts red health, no-contact >21d, churned, graduated per owner (Alex / Aya)
  - Fetches Intercom conversations closed last week (statistics.last_close_at filter)
  - Computes median reply time per admin
  - Creates or updates the scorecard Notion row for the week

16 nodes:
  1  Schedule: Monday 06:00         scheduleTrigger
  2  Compute Week Bounds             code
  3  Fetch MCT Page 1                httpRequest   ─┐ main KPI chain
  4  Prep MCT Page 2 Body            code            │
  5  Fetch MCT Page 2                httpRequest     │
  6  Compute MCT KPIs                code            │
  7  Fetch Intercom Convs            httpRequest     │
  8  Compute Reply Time KPI          code            │
  9  Build Scorecard Body            code            │
  10 Find Scorecard Row              httpRequest     │
  11 Row exists?                     if              │
  12a Update Scorecard Row           httpRequest     │ (true branch)
  12b Create Scorecard Row           httpRequest    ─┘ (false branch)
  13 Find Current Week Row           httpRequest   ─┐ shell branch (parallel)
  14 Shell Needed?                   code            │
  15 Create Current Week Shell       httpRequest   ─┘

After deploy: toggle ON in n8n UI to activate the schedule.
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

# Confirmed Feb 25 2026 via confirm_scorecard_db()
SCORECARD_DB_ID = "311e418f-d8c4-810e-8b11-cdc50357e709"
W09_PAGE_ID     = "311e418fd8c481b18552d12c067c1089"   # used to confirm DB ID
MCT_DS_ID       = "3ceb1ad0-91f1-40db-945a-c51c58035898"

NOTION_CRED_ID   = "LH587kxanQCPcd9y"
NOTION_CRED_NAME = "Notion - Enrichment"

WORKFLOW_NAME = "\U0001F4CA Weekly Scorecard Builder"   # 📊

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

NOTION_HEADERS_STD = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}

MCT_URL = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"


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
# Written as plain triple-quoted strings (no f-string) except BUILD_SCORECARD_BODY_JS
# which uses a template placeholder for SCORECARD_DB_ID injection.
# Unicode emoji property keys use \\uXXXX escapes to avoid JSON encoding issues.
# Important: never embed }} inside template expressions — pre-build bodies in code nodes.

# Node 2: Compute Week Bounds
# Runs ON Monday 06:00 CET. Yesterday = Sunday = last day of completed week.
# weekStart = Sunday - 6 = last Monday.  No Monday-clamp needed (unlike KPI 6 workflow).
COMPUTE_WEEK_BOUNDS_JS = """\
// Runs Monday 06:00. Yesterday = Sunday = end of completed week.
const now       = new Date();
const yesterday = new Date(now);
yesterday.setUTCDate(yesterday.getUTCDate() - 1);
yesterday.setUTCHours(23, 59, 59, 999);

const weekStart = new Date(yesterday);
weekStart.setUTCDate(weekStart.getUTCDate() - 6);
weekStart.setUTCHours(0, 0, 0, 0);

const pad       = n => String(n).padStart(2, '0');
const toDateStr = d => d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate());

const weekStartStr = toDateStr(weekStart);
const weekEndStr   = toDateStr(yesterday);
const weekStartTs  = Math.floor(weekStart.getTime()  / 1000);
const weekEndTs    = Math.floor(yesterday.getTime()  / 1000);

// ISO week number (week starts Monday; first week of year contains Thursday)
function isoWeek(d) {
    const thu = new Date(d);
    thu.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7) + 3);
    const jan4 = new Date(Date.UTC(thu.getUTCFullYear(), 0, 4));
    return 1 + Math.round((thu - jan4) / 604800000);
}
const weekNum   = isoWeek(weekStart);
const weekLabel = 'W' + String(weekNum).padStart(2, '0')
    + ' (' + weekStartStr + ' - ' + weekEndStr + ')';

// Pre-build Intercom body (statistics.last_close_at = closed during the week)
// Avoids }} inside template expressions downstream.
const intercomBody = JSON.stringify({
    query: {
        operator: "AND",
        value: [
            { field: "open",                     operator: "=",  value: false      },
            { field: "statistics.last_close_at", operator: ">",  value: weekStartTs },
            { field: "statistics.last_close_at", operator: "<=", value: weekEndTs   },
        ],
    },
    pagination: { per_page: 150 },
});

console.log('[scorecard-bounds] ' + weekLabel
    + ' ts=' + weekStartTs + '..' + weekEndTs);

// Current week (today = Monday = new week's first day)
const currentWeekStart = new Date(now);
currentWeekStart.setUTCHours(0, 0, 0, 0);
const currentWeekStartStr = toDateStr(currentWeekStart);
const currentWeekNum = isoWeek(currentWeekStart);
const currentWeekEndDate = new Date(currentWeekStart);
currentWeekEndDate.setUTCDate(currentWeekStart.getUTCDate() + 6);
const currentWeekLabel = 'W' + String(currentWeekNum).padStart(2, '0')
    + ' (' + currentWeekStartStr + ' - ' + toDateStr(currentWeekEndDate) + ')';
const currentWeekQueryBody = JSON.stringify({
    filter: { property: 'Week Start', date: { equals: currentWeekStartStr } },
    page_size: 1,
});

return [{ json: {
    weekStartStr, weekEndStr, weekStartTs, weekEndTs, weekLabel, intercomBody,
    currentWeekStartStr, currentWeekLabel, currentWeekQueryBody,
} }];
"""

# Node 4: Prep MCT Page 2 Body — identical to customers_contacted workflow
PREP_MCT_PAGE2_JS = """\
const r    = $('Fetch MCT Page 1').first().json;
const body = { page_size: 100 };
if (r.has_more && r.next_cursor) {
    body.start_cursor = r.next_cursor;
}
return [{ json: { body: JSON.stringify(body), skip: !r.has_more } }];
"""

# Node 6: Compute MCT KPIs
# Unicode escapes for Notion property key emojis (UTF-16 surrogate pairs):
#   \u2B50  = ⭐  CS Owner       (BMP, no surrogate)
#   \uD83D\uDCB0 = 💰  Billing Status
#   \uD83D\uDEA6 = 🚦  Health Status
#   \uD83D\uDCDE = 📞  Days Since Last Contact
#   \uD83D\uDE22 = 😢  Churn Date
#   \uD83D\uDE80 = 🚀  Graduation Date
COMPUTE_MCT_KPIS_JS = """\
// Merge page 1 + page 2 results, compute per-owner KPI counts.
const page1         = $('Fetch MCT Page 1').first().json.results || [];
const skipPage2     = !!$('Prep MCT Page 2 Body').first().json.skip;
const page2Response = skipPage2 ? { results: [], has_more: false } : $input.first().json;
const page2         = page2Response.results || [];

if (page2Response.has_more) {
    console.warn('[mct-kpis] WARNING: MCT has >200 rows — KPI counts may be incomplete!');
}

const seen = new Set();
const all  = [...page1, ...page2].filter(r => {
    if (seen.has(r.id)) return false;
    seen.add(r.id); return true;
});

const bounds       = $('Compute Week Bounds').first().json;
const weekStartStr = bounds.weekStartStr;
const weekEndStr   = bounds.weekEndStr;

function inWeek(dateStr) {
    if (!dateStr) return false;
    const s = dateStr.slice(0, 10);
    return s >= weekStartStr && s <= weekEndStr;
}

const kpis = {
    Alex: { red_health: 0, no_contact_21d: 0, churned: 0, graduated: 0 },
    Aya:  { red_health: 0, no_contact_21d: 0, churned: 0, graduated: 0 },
};

for (const row of all) {
    const props = row.properties || {};

    const owner = props['\\u2B50 CS Owner']?.select?.name;
    if (owner !== 'Alex' && owner !== 'Aya') continue;

    const billing   = props['\\uD83D\\uDCB0 Billing Status']?.select?.name || '';
    const is_active = billing === 'Active';

    // KPI 1: Red Health (active customers only)
    if (is_active) {
        const health = props['\\uD83D\\uDEA6 Health Status']?.formula?.string || '';
        if (health.includes('Red')) kpis[owner].red_health++;
    }

    // KPI 2: No Contact >21d (active customers only)
    if (is_active) {
        const days = props['\\uD83D\\uDCDE Days Since Last Contact']?.formula?.number;
        if (days != null && days > 21) kpis[owner].no_contact_21d++;
    }

    // KPI 4: Churned this week (all customers, date-based)
    const churnDateStr = props['\\uD83D\\uDE22 Churn Date']?.date?.start;
    if (inWeek(churnDateStr)) kpis[owner].churned++;

    // KPI 5: Graduated this week (all customers, date-based)
    const gradDateStr = props['\\uD83D\\uDE80 Graduation Date']?.date?.start;
    if (inWeek(gradDateStr)) kpis[owner].graduated++;
}

console.log('[mct-kpis] rows=' + all.length
    + ' Alex=' + JSON.stringify(kpis.Alex)
    + ' Aya='  + JSON.stringify(kpis.Aya));
return [{ json: { kpis } }];
"""

# Node 8: Compute Reply Time KPI
COMPUTE_REPLY_TIME_JS = """\
// Compute median assignment-to-reply time per admin from Intercom convs.
// Input: output of Fetch Intercom Convs (conversations array).
const convs   = $input.first().json.conversations || [];
const ALEX_ID = '7484673';
const AYA_ID  = '8411967';

const alexTimes = [];
const ayaTimes  = [];

for (const c of convs) {
    const stats  = c.statistics || {};
    const closer = String(stats.last_closed_by_id || '');
    if (closer !== ALEX_ID && closer !== AYA_ID) continue;

    const reply_at      = stats.last_assignment_admin_reply_at;
    const assignment_at = stats.last_assignment_at;
    if (!reply_at || !assignment_at || reply_at <= assignment_at) continue;

    const delta = reply_at - assignment_at;
    if (closer === ALEX_ID) alexTimes.push(delta);
    else                    ayaTimes.push(delta);
}

function median(arr) {
    if (arr.length === 0) return null;
    const sorted = [...arr].sort((a, b) => a - b);
    const mid    = Math.floor(sorted.length / 2);
    return sorted.length % 2 === 0
        ? (sorted[mid - 1] + sorted[mid]) / 2
        : sorted[mid];
}

const alexMedian = median(alexTimes);
const ayaMedian  = median(ayaTimes);
const alexReply  = alexMedian !== null ? Math.round(alexMedian / 60 * 10) / 10 : null;
const ayaReply   = ayaMedian  !== null ? Math.round(ayaMedian  / 60 * 10) / 10 : null;

console.log('[reply-time] convs=' + convs.length
    + ' alex n=' + alexTimes.length + ' median=' + alexReply + 'min'
    + ' aya n='  + ayaTimes.length  + ' median=' + ayaReply  + 'min');
return [{ json: { alexReply, ayaReply } }];
"""

# Node 9: Build Scorecard Body
# Uses placeholder SCORECARD_DB_ID_PLACEHOLDER — replaced at deploy time.
# Builds updateBody, createBody, queryBody to avoid }} in downstream templates.
BUILD_SCORECARD_BODY_JS_TEMPLATE = """\
// Pre-build all Notion API payloads. Avoids }} patterns in HTTP Request templates.
const kpis       = $('Compute MCT KPIs').first().json.kpis;
const reply      = $('Compute Reply Time KPI').first().json;
const bounds     = $('Compute Week Bounds').first().json;
const SCORECARD_DB_ID = 'SCORECARD_DB_ID_PLACEHOLDER';

const alexReply = reply.alexReply;
const ayaReply  = reply.ayaReply;

// Shared KPI properties (used in both update and create payloads)
const kpiProps = {
    'Alex: Red Health':      { number: kpis.Alex.red_health     },
    'Aya: Red Health':       { number: kpis.Aya.red_health      },
    'Alex: No Contact >21d': { number: kpis.Alex.no_contact_21d },
    'Aya: No Contact >21d':  { number: kpis.Aya.no_contact_21d  },
    'Alex: Churned':         { number: kpis.Alex.churned        },
    'Aya: Churned':          { number: kpis.Aya.churned         },
    'Alex: Graduated':       { number: kpis.Alex.graduated      },
    'Aya: Graduated':        { number: kpis.Aya.graduated       },
};
if (alexReply !== null && alexReply !== undefined) {
    kpiProps['Alex: Median Reply Time'] = { number: alexReply };
}
if (ayaReply !== null && ayaReply !== undefined) {
    kpiProps['Aya: Median Reply Time'] = { number: ayaReply };
}

// PATCH body (for update path — existing row)
const updateBody = JSON.stringify({ properties: kpiProps });

// POST body (for create path — new row, includes Week title + Week Start date)
const createProps = Object.assign({}, kpiProps, {
    'Week':       { title: [{ text: { content: bounds.weekLabel } }] },
    'Week Start': { date:  { start: bounds.weekStartStr             } },
});
const createBody = JSON.stringify({
    parent:     { database_id: SCORECARD_DB_ID },
    properties: createProps,
});

// Query body (find existing row by Week Start date)
const queryBody = JSON.stringify({
    filter:    { property: 'Week Start', date: { equals: bounds.weekStartStr } },
    page_size: 1,
});

console.log('[scorecard-body] week=' + bounds.weekLabel
    + ' alexReply=' + alexReply + ' ayaReply=' + ayaReply);
return [{ json: { updateBody, createBody, queryBody } }];
"""


# Node 14: Shell Needed?
# Uses SCORECARD_DB_ID_PLACEHOLDER — replaced at build time with confirmed DB ID.
# Plain triple-quoted string (no f-string) so JS braces are literal.
SHELL_NEEDED_JS_TEMPLATE = """\
const SCORECARD_DB_ID = 'SCORECARD_DB_ID_PLACEHOLDER';
const results = $input.first().json.results || [];
if (results.length > 0) {
    console.log('[shell-check] current week row already exists, skip');
    return [];   // stops execution cleanly — no create needed
}
const bounds = $('Compute Week Bounds').first().json;
const createBody = JSON.stringify({
    parent: { database_id: SCORECARD_DB_ID },
    properties: {
        'Week':       { title: [{ text: { content: bounds.currentWeekLabel } }] },
        'Week Start': { date:  { start: bounds.currentWeekStartStr } },
    },
});
console.log('[shell-check] creating shell row for ' + bounds.currentWeekLabel);
return [{ json: { createBody } }];
"""


# ── Build workflow ─────────────────────────────────────────────────────────────

def build_workflow(scorecard_db_id):
    """Build the 16-node scorecard builder workflow JSON."""

    # Inject confirmed DB ID into the Build Scorecard Body code
    build_body_js = BUILD_SCORECARD_BODY_JS_TEMPLATE.replace(
        "SCORECARD_DB_ID_PLACEHOLDER", scorecard_db_id
    )

    # Inject confirmed DB ID into the Shell Needed? code
    shell_needed_js = SHELL_NEEDED_JS_TEMPLATE.replace(
        "SCORECARD_DB_ID_PLACEHOLDER", scorecard_db_id
    )

    scorecard_db_query_url = (
        f"https://api.notion.com/v1/databases/{scorecard_db_id}/query"
    )

    y_main = 300   # y for linear chain
    xs     = [i * 280 for i in range(12)]

    # ── 1. Schedule Trigger ───────────────────────────────────────────────────
    node_schedule = {
        "id":          uid(),
        "name":        "Schedule: Monday 06:00",
        "type":        "n8n-nodes-base.scheduleTrigger",
        "typeVersion": 1.2,
        "position":    [xs[0], y_main],
        "parameters": {
            "rule": {
                "interval": [
                    {"field": "cronExpression", "expression": "0 6 * * 1"}
                ]
            },
            "timezone": "Europe/Berlin",
        },
    }

    # ── 2. Compute Week Bounds ────────────────────────────────────────────────
    node_bounds = {
        "id":          uid(),
        "name":        "Compute Week Bounds",
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [xs[1], y_main],
        "parameters":  {"mode": "runOnceForAllItems", "jsCode": COMPUTE_WEEK_BOUNDS_JS},
    }

    # ── 3. Fetch MCT Page 1 ───────────────────────────────────────────────────
    node_mct1 = {
        "id":          uid(),
        "name":        "Fetch MCT Page 1",
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [xs[2], y_main],
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
    }

    # ── 4. Prep MCT Page 2 Body ───────────────────────────────────────────────
    node_prep2 = {
        "id":          uid(),
        "name":        "Prep MCT Page 2 Body",
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [xs[3], y_main],
        "parameters":  {"mode": "runOnceForAllItems", "jsCode": PREP_MCT_PAGE2_JS},
    }

    # ── 5. Fetch MCT Page 2 ───────────────────────────────────────────────────
    node_mct2 = {
        "id":          uid(),
        "name":        "Fetch MCT Page 2",
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [xs[4], y_main],
        "credentials": notion_cred(),
        "parameters": {
            **notion_auth(),
            "method":         "POST",
            "url":            MCT_URL,
            "sendBody":       True,
            "contentType":    "raw",
            "rawContentType": "application/json",
            "body":           "={{ $json.body }}",
            **notion_header_v3(),
            "options": {"continueOnFail": True},
        },
    }

    # ── 6. Compute MCT KPIs ───────────────────────────────────────────────────
    node_mct_kpis = {
        "id":          uid(),
        "name":        "Compute MCT KPIs",
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [xs[5], y_main],
        "parameters":  {"mode": "runOnceForAllItems", "jsCode": COMPUTE_MCT_KPIS_JS},
    }

    # ── 7. Fetch Intercom Convs ───────────────────────────────────────────────
    # intercomBody pre-built in node 2 — no }} risk
    node_intercom = {
        "id":          uid(),
        "name":        "Fetch Intercom Convs",
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [xs[6], y_main],
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
            "body": "={{ $('Compute Week Bounds').first().json.intercomBody }}",
            "options": {},
        },
    }

    # ── 8. Compute Reply Time KPI ─────────────────────────────────────────────
    node_reply = {
        "id":          uid(),
        "name":        "Compute Reply Time KPI",
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [xs[7], y_main],
        "parameters":  {"mode": "runOnceForAllItems", "jsCode": COMPUTE_REPLY_TIME_JS},
    }

    # ── 9. Build Scorecard Body ───────────────────────────────────────────────
    node_build_body = {
        "id":          uid(),
        "name":        "Build Scorecard Body",
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [xs[8], y_main],
        "parameters":  {"mode": "runOnceForAllItems", "jsCode": build_body_js},
    }

    # ── 10. Find Scorecard Row ────────────────────────────────────────────────
    # queryBody pre-built in node 9 — no }} risk
    node_find = {
        "id":          uid(),
        "name":        "Find Scorecard Row",
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [xs[9], y_main],
        "credentials": notion_cred(),
        "parameters": {
            **notion_auth(),
            "method":         "POST",
            "url":            scorecard_db_query_url,
            "sendBody":       True,
            "contentType":    "raw",
            "rawContentType": "application/json",
            "body": "={{ $json.queryBody }}",
            **notion_header_v2(),
            "options": {},
        },
    }

    # ── 11. Row exists? (IF) ──────────────────────────────────────────────────
    node_if = {
        "id":          uid(),
        "name":        "Row exists?",
        "type":        "n8n-nodes-base.if",
        "typeVersion": 1,
        "position":    [xs[10], y_main],
        "parameters": {
            "conditions": {
                "number": [
                    {
                        "value1":    "={{ $('Find Scorecard Row').first().json.results.length }}",
                        "operation": "larger",
                        "value2":    0,
                    }
                ]
            }
        },
    }

    # ── 12a. Update Scorecard Row (true branch) ───────────────────────────────
    # updateBody pre-built in node 9 — no }} risk
    node_update = {
        "id":          uid(),
        "name":        "Update Scorecard Row",
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [xs[11], y_main - 120],
        "credentials": notion_cred(),
        "parameters": {
            **notion_auth(),
            "method": "PATCH",
            "url":    "={{ 'https://api.notion.com/v1/pages/' + $('Find Scorecard Row').first().json.results[0].id }}",
            "sendBody":       True,
            "contentType":    "raw",
            "rawContentType": "application/json",
            "body": "={{ $('Build Scorecard Body').first().json.updateBody }}",
            **notion_header_v2(),
            "options": {},
        },
    }

    # ── 12b. Create Scorecard Row (false branch) ──────────────────────────────
    # createBody pre-built in node 9 — no }} risk
    node_create = {
        "id":          uid(),
        "name":        "Create Scorecard Row",
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [xs[11], y_main + 120],
        "credentials": notion_cred(),
        "parameters": {
            **notion_auth(),
            "method":         "POST",
            "url":            "https://api.notion.com/v1/pages",
            "sendBody":       True,
            "contentType":    "raw",
            "rawContentType": "application/json",
            "body": "={{ $('Build Scorecard Body').first().json.createBody }}",
            **notion_header_v2(),
            "options": {},
        },
    }

    # ── 13. Find Current Week Row (shell branch — parallel to main chain) ────
    node_find_current = {
        "id":          uid(),
        "name":        "Find Current Week Row",
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [280, 500],
        "credentials": notion_cred(),
        "parameters": {
            **notion_auth(),
            "method":         "POST",
            "url":            scorecard_db_query_url,
            "sendBody":       True,
            "contentType":    "raw",
            "rawContentType": "application/json",
            "body": "={{ $('Compute Week Bounds').first().json.currentWeekQueryBody }}",
            **notion_header_v2(),
            "options": {"continueOnFail": True},
        },
    }

    # ── 14. Shell Needed? ─────────────────────────────────────────────────────
    node_shell_check = {
        "id":          uid(),
        "name":        "Shell Needed?",
        "type":        "n8n-nodes-base.code",
        "typeVersion": 2,
        "position":    [560, 500],
        "parameters":  {"mode": "runOnceForAllItems", "jsCode": shell_needed_js},
    }

    # ── 15. Create Current Week Shell ─────────────────────────────────────────
    node_create_shell = {
        "id":          uid(),
        "name":        "Create Current Week Shell",
        "type":        "n8n-nodes-base.httpRequest",
        "typeVersion": 4.2,
        "position":    [840, 500],
        "credentials": notion_cred(),
        "parameters": {
            **notion_auth(),
            "method":         "POST",
            "url":            "https://api.notion.com/v1/pages",
            "sendBody":       True,
            "contentType":    "raw",
            "rawContentType": "application/json",
            "body":           "={{ $json.createBody }}",
            **notion_header_v2(),
            "options": {"continueOnFail": True},
        },
    }

    # ── Connections ───────────────────────────────────────────────────────────
    # Main KPI chain: 1→2→3→4→5→6→7→8→9→10→11 (IF)
    # IF true (output 0)  → 12a Update
    # IF false (output 1) → 12b Create
    # Shell branch (parallel from node 2): 2→13→14→15
    linear = [
        node_schedule, node_bounds, node_mct1, node_prep2, node_mct2,
        node_mct_kpis, node_intercom, node_reply, node_build_body,
        node_find, node_if,
    ]
    connections = {}
    for i in range(len(linear) - 1):
        connections[linear[i]["name"]] = {
            "main": [[{"node": linear[i + 1]["name"], "type": "main", "index": 0}]]
        }

    # Override: Compute Week Bounds fans out to BOTH chains in parallel
    connections["Compute Week Bounds"] = {
        "main": [[
            {"node": "Fetch MCT Page 1",      "type": "main", "index": 0},
            {"node": "Find Current Week Row",  "type": "main", "index": 0},
        ]]
    }

    # IF branches (overwrites the last entry added above for node_if → nothing)
    connections["Row exists?"] = {
        "main": [
            [{"node": "Update Scorecard Row", "type": "main", "index": 0}],   # true
            [{"node": "Create Scorecard Row", "type": "main", "index": 0}],   # false
        ]
    }

    # Shell branch linear chain
    connections["Find Current Week Row"] = {
        "main": [[{"node": "Shell Needed?",             "type": "main", "index": 0}]]
    }
    connections["Shell Needed?"] = {
        "main": [[{"node": "Create Current Week Shell", "type": "main", "index": 0}]]
    }

    nodes = linear + [node_update, node_create, node_find_current, node_shell_check, node_create_shell]

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
    """Fetch W09 page → return confirmed parent database_id."""
    url = f"https://api.notion.com/v1/pages/{W09_PAGE_ID}"
    r   = requests.get(url, headers=NOTION_HEADERS_STD)
    if r.status_code != 200:
        print(f"  [warn] Could not fetch W09 page: HTTP {r.status_code} — using constant")
        return SCORECARD_DB_ID
    db_id  = r.json().get("parent", {}).get("database_id", "")
    actual = db_id.replace("-", "")
    known  = SCORECARD_DB_ID.replace("-", "")
    if actual == known:
        print(f"  ✓ SCORECARD_DB_ID confirmed: {db_id}")
    else:
        print(f"  [warn] Mismatch! constant={SCORECARD_DB_ID}, actual={db_id}")
        print(f"         Using actual value from Notion.")
    return db_id if db_id else SCORECARD_DB_ID


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("deploy_scorecard_builder_workflow.py")
    print(f"Deploying: {WORKFLOW_NAME!r}")
    print("=" * 65)

    # Step 0: Confirm SCORECARD_DB_ID
    print("\n[0/4] Confirming Scorecard DB ID via Notion …")
    confirmed_db_id = confirm_scorecard_db()

    # Step 1: Check for existing workflow
    print(f"\n[1/4] Checking for existing workflow named {WORKFLOW_NAME!r} …")
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
    print("\n[2/4] Building workflow JSON …")
    wf_body = build_workflow(scorecard_db_id=confirmed_db_id)
    print(f"  Nodes ({len(wf_body['nodes'])}):")
    for i, node in enumerate(wf_body["nodes"], 1):
        print(f"    {i:2}. {node['name']}")

    save_path = "/tmp/scorecard_builder_workflow.json"
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
    print("Schedule: Monday 06:00 CET (1h before KPI 6 workflow at 07:00).")
    print()
    print("What this workflow does each Monday morning:")
    print("  • Computes last week's Mon–Sun bounds")
    print("  • Fetches all MCT customers (2 pages × 100 rows)")
    print("  • Counts red health, no-contact >21d, churned, graduated per owner")
    print("  • Fetches Intercom conversations closed last week")
    print("  • Computes median reply time per admin (Alex / Aya)")
    print("  • Creates the week's scorecard row (or updates if it already exists)")
    print()
    print(f"  Workflow JSON: {save_path}")
    print(f"  Using SCORECARD_DB_ID: {confirmed_db_id}")


if __name__ == "__main__":
    main()
