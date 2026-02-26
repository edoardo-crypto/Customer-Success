#!/usr/bin/env python3
"""
verify_reclose_webhook.py
Checks n8n executions for workflow 3AO3SRUK80rcOCgQ to find conversation IDs
that appear more than once — confirming that reopen → re-close fires a new webhook.
"""

import json
import time
import requests
from collections import defaultdict
from datetime import datetime

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = "***REMOVED***"
WORKFLOW_ID = "3AO3SRUK80rcOCgQ"

HEADERS = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type": "application/json",
}


def get_executions(limit=50):
    """Fetch the last N executions for the workflow."""
    url = f"{N8N_BASE}/api/v1/executions"
    params = {"workflowId": WORKFLOW_ID, "limit": limit}
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


def get_execution_detail(exec_id):
    """Fetch full execution data including node outputs."""
    url = f"{N8N_BASE}/api/v1/executions/{exec_id}"
    params = {"includeData": "true"}
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def extract_field(node_data, *keys):
    """Safely navigate nested dicts/lists."""
    val = node_data
    for k in keys:
        if val is None:
            return None
        if isinstance(val, list):
            val = val[0] if val else None
        if isinstance(val, dict):
            val = val.get(k)
        else:
            return None
    if isinstance(val, list):
        val = val[0] if val else None
    return val


def parse_execution(detail):
    """
    Extract conversation_id, filter_passed, issue_type, cs_severity,
    issue_description from a full execution record.
    """
    result = {
        "exec_id": detail.get("id"),
        "status": detail.get("status"),
        "finished_at": detail.get("stoppedAt") or detail.get("startedAt"),
        "conversation_id": None,
        "filter_passed": None,
        "issue_type": None,
        "cs_severity": None,
        "issue_description": None,
    }

    run_data = detail.get("data", {})
    result_data = run_data.get("resultData", {})
    run_data_nodes = result_data.get("runData", {})

    # ── 1. Try to get conversation_id from the Intercom webhook trigger node ──
    # Common node names for the trigger
    trigger_node_names = [
        "Webhook",
        "webhook",
        "Intercom Webhook",
        "intercom-webhook",
    ]
    for node_name in trigger_node_names:
        node_runs = run_data_nodes.get(node_name, [])
        if node_runs:
            try:
                body = node_runs[0]["data"]["main"][0][0]["json"]["body"]
                conv_id = (
                    body.get("data", {}).get("item", {}).get("id")
                    or body.get("id")
                )
                if conv_id:
                    result["conversation_id"] = str(conv_id)
                    break
            except (KeyError, IndexError, TypeError):
                pass

    # ── 2. Try "Parse AI Summary" node for issue fields ──
    parse_node_names = [
        "Parse AI Summary",
        "Parse AI Output",
        "Extract Fields",
        "Code",
    ]
    for node_name in parse_node_names:
        node_runs = run_data_nodes.get(node_name, [])
        if node_runs:
            try:
                item_json = node_runs[0]["data"]["main"][0][0]["json"]
                for field in ["issue_type", "cs_severity", "issue_description"]:
                    if item_json.get(field):
                        result[field] = item_json[field]
                # Also check conversation_id here if not found yet
                if not result["conversation_id"]:
                    conv_id = item_json.get("conversation_id")
                    if conv_id:
                        result["conversation_id"] = str(conv_id)
            except (KeyError, IndexError, TypeError):
                pass

    # ── 3. Determine filter branch from "Filter: Not an Issue" node ──
    filter_node_names = [
        "Filter: Not an Issue",
        "Filter",
        "IF",
        "If",
    ]
    for node_name in filter_node_names:
        node_runs = run_data_nodes.get(node_name, [])
        if node_runs:
            try:
                main_outputs = node_runs[0]["data"]["main"]
                # output[0] = TRUE branch, output[1] = FALSE branch
                true_branch = main_outputs[0] if len(main_outputs) > 0 else []
                false_branch = main_outputs[1] if len(main_outputs) > 1 else []
                if true_branch and len(true_branch) > 0:
                    result["filter_passed"] = True
                elif false_branch and len(false_branch) > 0:
                    result["filter_passed"] = False
                else:
                    result["filter_passed"] = None
            except (KeyError, IndexError, TypeError):
                pass
            break

    # ── 4. Fallback: scan all nodes for conversation_id ──
    if not result["conversation_id"]:
        for node_name, node_runs in run_data_nodes.items():
            if not node_runs:
                continue
            try:
                item_json = node_runs[0]["data"]["main"][0][0]["json"]
                # Check body.data.item.id (Intercom webhook format)
                conv_id = (
                    item_json.get("body", {}).get("data", {}).get("item", {}).get("id")
                    or item_json.get("conversation_id")
                    or item_json.get("id")
                )
                if conv_id and str(conv_id).isdigit() and len(str(conv_id)) > 10:
                    result["conversation_id"] = str(conv_id)
                    break
            except (KeyError, IndexError, TypeError, AttributeError):
                pass

    # ── 5. Fallback: scan all nodes for issue fields ──
    if not any([result["issue_type"], result["cs_severity"]]):
        for node_name, node_runs in run_data_nodes.items():
            if not node_runs:
                continue
            try:
                item_json = node_runs[0]["data"]["main"][0][0]["json"]
                for field in ["issue_type", "cs_severity", "issue_description"]:
                    if not result[field] and item_json.get(field):
                        result[field] = item_json[field]
            except (KeyError, IndexError, TypeError):
                pass

    return result


def main():
    print(f"Fetching last 50 executions for workflow {WORKFLOW_ID}...")
    executions = get_executions(limit=50)
    print(f"Found {len(executions)} executions\n")

    records = []
    for i, ex in enumerate(executions):
        exec_id = ex.get("id")
        print(f"  [{i+1}/{len(executions)}] Fetching detail for execution {exec_id}...", end=" ")
        detail = get_execution_detail(exec_id)
        if detail is None:
            print("NOT FOUND — skipping")
            continue
        rec = parse_execution(detail)
        records.append(rec)
        print(f"conv_id={rec['conversation_id']} filter={rec['filter_passed']} status={rec['status']}")
        time.sleep(0.2)  # gentle rate limiting

    # ── Group by conversation_id ──
    by_conv = defaultdict(list)
    for rec in records:
        key = rec["conversation_id"] or f"UNKNOWN_{rec['exec_id']}"
        by_conv[key].append(rec)

    # Sort each group by finished_at
    for key in by_conv:
        by_conv[key].sort(key=lambda r: r["finished_at"] or "")

    # ── Report ──
    print("\n" + "=" * 70)
    print("REPORT: Conversation IDs in last 50 executions")
    print("=" * 70)

    duplicates = {k: v for k, v in by_conv.items() if len(v) > 1}
    singles_dropped = {k: v for k, v in by_conv.items() if len(v) == 1 and v[0]["filter_passed"] is False}
    singles_passed = {k: v for k, v in by_conv.items() if len(v) == 1 and v[0]["filter_passed"] is True}
    singles_unknown = {k: v for k, v in by_conv.items() if len(v) == 1 and v[0]["filter_passed"] is None}

    print(f"\nTotal unique conversations: {len(by_conv)}")
    print(f"  Duplicates (2+ executions): {len(duplicates)}")
    print(f"  Single - filter PASSED (logged): {len(singles_passed)}")
    print(f"  Single - filter DROPPED: {len(singles_dropped)}")
    print(f"  Single - filter UNKNOWN: {len(singles_unknown)}")

    # ── Duplicate analysis ──
    if duplicates:
        print("\n" + "-" * 70)
        print("DUPLICATE CONVERSATIONS (potential reopen → re-close confirmations)")
        print("-" * 70)
        for conv_id, recs in duplicates.items():
            print(f"\n  Conv ID: {conv_id}")
            for j, r in enumerate(recs):
                ts = r["finished_at"] or "unknown"
                passed = r["filter_passed"]
                branch = "✅ LOGGED" if passed is True else ("❌ DROPPED" if passed is False else "? UNKNOWN")
                print(f"    [{j+1}] {ts}  filter={branch}  issue_type={r['issue_type']}  severity={r['cs_severity']}")
                if r["issue_description"]:
                    desc = str(r["issue_description"])[:80]
                    print(f"         description: {desc}{'...' if len(str(r['issue_description'])) > 80 else ''}")

            # Assess pattern
            filters = [r["filter_passed"] for r in recs]
            if filters[0] is False and filters[-1] is True:
                print(f"    => ✅ CONFIRMED: First dropped, then logged (reopen → re-close works!)")
            elif all(f is True for f in filters):
                print(f"    => ⚠️  DUPLICATE LOGGING: All passed filter — possible double-fire")
            elif all(f is False for f in filters):
                print(f"    => ❌ STILL INCOMPLETE: Dropped multiple times, never logged")
            else:
                print(f"    => ❓ MIXED PATTERN: {filters}")
    else:
        print("\n  No duplicate conversation IDs found in last 50 executions.")
        print("  (Either no conversations were re-closed yet, or the window is too small)")

    # ── Dropped-only list ──
    if singles_dropped:
        print("\n" + "-" * 70)
        print("SINGLE-PASS DROPPED (incomplete, never re-triggered in last 50 execs)")
        print("-" * 70)
        for conv_id, recs in list(singles_dropped.items())[:20]:
            r = recs[0]
            ts = r["finished_at"] or "unknown"
            print(f"  Conv ID: {conv_id}  ts={ts}  issue_type={r['issue_type']}  severity={r['cs_severity']}")

    print("\n" + "=" * 70)
    print("Done.")


if __name__ == "__main__":
    main()
