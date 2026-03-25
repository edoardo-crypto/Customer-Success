#!/usr/bin/env python3
"""
patch_n8n_duplicate_resolved.py

Patches the 'Parse & Map State' Code node in the Linear→Notion sync workflow
(xdVkUh6YCtcuW8QM) so that Linear issues with state name "Duplicate" map to
'Resolved' in Notion instead of falling through to the 'cancelled' type check
(which would incorrectly map them to 'Deprioritized').

Adds `|| stateName.includes('duplicate')` to the existing Resolved name-check
condition in the JS if/else chain.

After running, archive this script to archive/.
"""

import requests
import creds

N8N_BASE = "https://konvoai.app.n8n.cloud"
API_KEY  = creds.get("N8N_API_KEY")
WF_ID    = "xdVkUh6YCtcuW8QM"

HEADERS = {
    "X-N8N-API-KEY": API_KEY,
    "Content-Type": "application/json",
}


def main():
    # 1. Fetch workflow
    resp = requests.get(f"{N8N_BASE}/api/v1/workflows/{WF_ID}", headers=HEADERS)
    resp.raise_for_status()
    wf = resp.json()
    print(f"Fetched workflow: {wf.get('name')} (active={wf.get('active')})")

    # 2. Find Parse & Map State node
    node = None
    for n in wf.get("nodes", []):
        if n.get("name") == "Parse & Map State":
            node = n
            break

    if not node:
        raise ValueError("Node 'Parse & Map State' not found in workflow.")

    code = node["parameters"].get("jsCode", "")

    # Check if already patched
    if "duplicate" in code.lower() and "Resolved" in code:
        # Verify it's specifically the duplicate→Resolved mapping
        idx_dup = code.lower().find("duplicate")
        idx_res = code.find("Resolved", idx_dup)
        if idx_res != -1 and idx_res - idx_dup < 100:
            print("Already patched — 'duplicate' → 'Resolved' mapping found. Nothing to do.")
            return

    # 3. Patch: add stateName.includes('duplicate') to the Resolved name-check.
    #    The current Resolved name-check looks like:
    #      stateName.includes('done') || stateName.includes('released') ||
    #      stateName.includes('complete') || stateName.includes('resolved')
    #    We append: || stateName.includes('duplicate')

    # Find the resolved name-check line — look for the last keyword before the Resolved assignment
    old_fragment = "stateName.includes('resolved')"
    if old_fragment not in code:
        print("--- Current jsCode (lines with state logic) ---")
        for i, line in enumerate(code.splitlines()):
            if any(kw in line.lower() for kw in ('statetype', 'statename', 'notionstatus', 'resolved', 'duplicate')):
                print(f"  {i:3}: {line}")
        print("---")
        raise ValueError("Could not find 'stateName.includes('resolved')' — manual inspection required.")

    # We need to add the duplicate check on the same condition line.
    # The pattern is: ...stateName.includes('resolved')) {
    # We replace with: ...stateName.includes('resolved') || stateName.includes('duplicate')) {
    old_condition_end = "stateName.includes('resolved')"
    new_condition_end = "stateName.includes('resolved') ||\n    stateName.includes('duplicate')"

    # Only replace the first occurrence (in the name-check block, before the Resolved assignment)
    new_code = code.replace(old_condition_end, new_condition_end, 1)

    if new_code == code:
        raise ValueError("Replacement had no effect — check the code manually.")

    node["parameters"]["jsCode"] = new_code
    print("Patched 'Parse & Map State' node — added duplicate → Resolved mapping.")

    # 4. PUT updated workflow
    put_body = {
        "name":        wf["name"],
        "nodes":       wf["nodes"],
        "connections": wf["connections"],
        "settings":    wf.get("settings", {}),
    }

    put_resp = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{WF_ID}",
        headers=HEADERS,
        json=put_body,
    )
    if put_resp.status_code != 200:
        print(f"PUT failed {put_resp.status_code}: {put_resp.text[:500]}")
        put_resp.raise_for_status()

    print(f"\nWorkflow updated (HTTP {put_resp.status_code}).")
    print("Duplicate → Resolved mapping is now live in the webhook workflow.")


if __name__ == "__main__":
    main()
