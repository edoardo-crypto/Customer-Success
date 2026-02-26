#!/usr/bin/env python3
"""
Fix Notion API calls for Customer Enrichment and Nightly Issue Score — Feb 18, 2026

Root cause: The Master Customer Table (84feda19...) now has multiple data sources,
making `databases/{id}/query` with Notion-Version 2022-06-28 fail with:
"Databases with multiple data sources are not supported in this API version."

Fix: Switch Master Customer Table queries to use `data_sources/{ds_id}/query`
with Notion-Version 2025-09-03. Leave Issues table and PATCH calls unchanged.
"""

import json
import urllib.request
import ssl
import sys

N8N_BASE = "https://konvoai.app.n8n.cloud"
N8N_API_KEY = "***REMOVED***"

MASTER_DB_ID = "84feda19cfaf4c6e9500bf21d2aaafef"
DATA_SOURCE_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"
OLD_URL = f"https://api.notion.com/v1/databases/{MASTER_DB_ID}/query"
NEW_URL = f"https://api.notion.com/v1/data_sources/{DATA_SOURCE_ID}/query"

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


def fix_workflow(wf_id, label):
    log(f"\n{'=' * 60}")
    log(f"Fixing: {label} ({wf_id})")
    log(f"{'=' * 60}")

    wf = n8n("GET", f"/workflows/{wf_id}")
    log(f"Got: {wf['name']} ({len(wf['nodes'])} nodes)", 1)

    # Deactivate
    try:
        n8n("POST", f"/workflows/{wf_id}/deactivate")
    except:
        pass

    modified = 0
    for node in wf["nodes"]:
        if node["type"].endswith(".httpRequest"):
            params = node["parameters"]
            url = params.get("url", "")

            # Only fix nodes that query the Master Customer Table
            if MASTER_DB_ID in url and "/databases/" in url:
                old_url = params["url"]
                params["url"] = NEW_URL
                log(f"Fixed URL: {node['name']}", 1)
                log(f"  {old_url[:70]} -> {NEW_URL[:70]}", 2)

                # Update Notion-Version header
                headers = params.get("headerParameters", {}).get("parameters", [])
                for h in headers:
                    if h.get("name") == "Notion-Version":
                        h["value"] = "2025-09-03"
                        log(f"  Notion-Version: 2022-06-28 -> 2025-09-03", 2)

                modified += 1

    if modified == 0:
        log("No nodes needed fixing", 1)
        # Reactivate
        try:
            n8n("POST", f"/workflows/{wf_id}/activate")
        except:
            pass
        return True

    log(f"Modified {modified} node(s)", 1)

    # PUT and reactivate
    put_body = {
        "name": wf["name"],
        "nodes": wf["nodes"],
        "connections": wf.get("connections", {}),
        "settings": wf.get("settings", {}),
    }
    n8n("PUT", f"/workflows/{wf_id}", put_body)
    log("Workflow updated", 1)

    try:
        n8n("POST", f"/workflows/{wf_id}/activate")
        log("Activated", 1)
    except Exception as e:
        log(f"Activation error: {e}", 1)

    return True


def main():
    log("Fix Notion API: databases/query -> data_sources/query")
    log(f"Master Customer Table: {MASTER_DB_ID}")
    log(f"Data Source: {DATA_SOURCE_ID}")

    fix_workflow("1FG950L1j8rkG4SJ", "Customer Enrichment (Daily)")
    fix_workflow("6xIuCyBje6QnynUh", "Nightly Issue Score Computation")

    log(f"\n{'=' * 60}")
    log("DONE")
    log(f"{'=' * 60}")
    log("Both workflows will run on their next schedule:")
    log("  Customer Enrichment: 08:00 CET tomorrow")
    log("  Nightly Issue Score: 06:00 Berlin tomorrow")


if __name__ == "__main__":
    main()
