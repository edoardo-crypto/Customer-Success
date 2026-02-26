#!/usr/bin/env python3
"""
fix_customers_contacted_week_bounds.py

Patches the live "📞 Weekly Customers Contacted Tracker" workflow
(ID: iDA5BBJxsp0cmv2M) to use a fixed Mon→Sun ISO-week window instead
of Mon→yesterday.

Problem fixed:
  - Old: weekEnd = yesterday 23:59 UTC
         → On Sundays, yesterday=Sat, so Sunday contacts are permanently lost.
         → On Mondays, weekEnd < weekStart → Monday clamp fires, returning 0.
  - New: weekEnd = sunday of the current ISO week (= monday + 6 days, 23:59 UTC)
         → Window is always Mon–Sun, consistent every day of the week.
         → Monday clamp block is removed (sunday >= monday always).
"""

import json
import requests
import sys

# ── Constants ──────────────────────────────────────────────────────────────────
N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = (
    "***REMOVED_JWT***"
    ".eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJpc3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleHAiOjE3NzMyNzAwMDB9"
    ".X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)

WORKFLOW_ID   = "iDA5BBJxsp0cmv2M"
TARGET_NODE   = "Compute Week Bounds"
IDEMPOTENCY_MARKER = "const sunday = new Date(monday);"

N8N_HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}

# ── New JS for Compute Week Bounds ─────────────────────────────────────────────
NEW_JS = """\
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


def get_workflow(wf_id):
    r = requests.get(f"{N8N_BASE}/api/v1/workflows/{wf_id}", headers=N8N_HEADERS)
    r.raise_for_status()
    return r.json()


def put_workflow(wf_id, wf):
    payload = {
        "name":        wf["name"],
        "nodes":       wf["nodes"],
        "connections": wf["connections"],
        "settings":    wf.get("settings", {}),
    }
    r = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{wf_id}",
        headers=N8N_HEADERS,
        json=payload,
    )
    if r.status_code not in (200, 201):
        print(f"  PUT failed: {r.status_code} — {r.text[:600]}")
        r.raise_for_status()
    return r.json()


def main():
    print("=" * 65)
    print("fix_customers_contacted_week_bounds.py")
    print(f"Target workflow: {WORKFLOW_ID}")
    print("=" * 65)

    # Step 1: GET workflow
    print(f"\n[1/4] Fetching workflow {WORKFLOW_ID} …")
    wf = get_workflow(WORKFLOW_ID)
    print(f"  ✓ Got '{wf['name']}' ({len(wf['nodes'])} nodes, active={wf.get('active')})")

    # Step 2: Find target node
    print(f"\n[2/4] Locating node '{TARGET_NODE}' …")
    target_idx = None
    for i, node in enumerate(wf["nodes"]):
        if node["name"] == TARGET_NODE:
            target_idx = i
            break

    if target_idx is None:
        print(f"  ERROR: Node '{TARGET_NODE}' not found in workflow.")
        print("  Available nodes:")
        for n in wf["nodes"]:
            print(f"    • {n['name']}")
        sys.exit(1)

    current_js = wf["nodes"][target_idx].get("parameters", {}).get("jsCode", "")
    print(f"  ✓ Found at index {target_idx}")

    # Step 3: Idempotency check
    print(f"\n[3/4] Checking idempotency …")
    if IDEMPOTENCY_MARKER in current_js:
        print(f"  ✓ Already patched (marker found: {IDEMPOTENCY_MARKER!r})")
        print("  Nothing to do — exiting.")
        sys.exit(0)

    print(f"  Old JS starts with: {current_js[:80].strip()!r}…")
    print(f"  Will replace with Sunday-based window JS.")

    # Step 4: Patch and PUT
    print(f"\n[4/4] Patching node and pushing workflow …")
    wf["nodes"][target_idx]["parameters"]["jsCode"] = NEW_JS

    result = put_workflow(WORKFLOW_ID, wf)
    print(f"  ✓ PUT successful  ID={result.get('id')}  active={result.get('active')}")

    print("\n" + "=" * 65)
    print("Patch applied successfully.")
    print("=" * 65)
    print()
    print("What changed:")
    print("  • weekEnd = Sunday of the current ISO week (monday + 6 days)")
    print("  • weekEndTs derived from sunday (not yesterday)")
    print("  • Monday clamp block removed — window is always Mon–Sun")
    print()
    print("Verification:")
    print(f"  1. Open n8n UI → workflow {WORKFLOW_ID}")
    print("  2. Click 'Execute Workflow'")
    print("  3. In 'Compute Week Bounds' output, confirm:")
    print("     • weekStart = 2026-02-23  (Mon of W09)")
    print("     • weekEnd   = 2026-03-01  (Sun of W09)")
    print("     • No 'Monday clamp applied' log line")
    print("  4. Check W09 Notion scorecard row — KPI 6 updated with non-zero count")


if __name__ == "__main__":
    main()
