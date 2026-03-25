#!/usr/bin/env python3
"""
patch_n8n_canceled_mapping.py

Patches the 'Parse & Map State' Code node in the Linear→Notion sync workflow
(xdVkUh6YCtcuW8QM) to map canceled Linear states to 'Deprioritized' in Notion.

Adds two new branches to the existing if/else chain:
  - stateName.includes('cancel') → Deprioritized
  - stateType === 'cancelled'    → Deprioritized

After running, archive this script to archive/.
"""

import re
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
    if "Deprioritized" in code:
        print("Already patched — 'Deprioritized' mapping found in node. Nothing to do.")
        return

    # 3. Patch: insert canceled branches before the final closing brace of the if/else chain.
    #    The current chain ends with:
    #      } else if (stateType === 'completed') {
    #        notionStatus = 'Resolved';
    #      }
    #    We add two new branches after that last 'completed' block.

    # Match the "stateType === 'completed'" block and its closing brace
    pattern = re.compile(
        r"(\} else if \(stateType === 'completed'\) \{\s*\n\s*notionStatus = 'Resolved';\s*\n\s*\})"
    )
    m = pattern.search(code)
    if not m:
        print("--- Current jsCode (lines with state logic) ---")
        for i, line in enumerate(code.splitlines()):
            if any(kw in line for kw in ('stateType', 'stateName', 'notionStatus', 'cancel')):
                print(f"  {i:3}: {line}")
        print("---")
        raise ValueError("Could not find 'completed' block to insert after — manual inspection required.")

    # Build the new canceled branches (matching the indentation style)
    canceled_block = (
        " else if (stateName.includes('cancel')) {\n"
        "  notionStatus = 'Deprioritized';\n"
        "} else if (stateType === 'cancelled') {\n"
        "  notionStatus = 'Deprioritized';\n"
        "}"
    )

    # Replace the closing "}" of the completed block with the new branches
    old_match = m.group(1)
    # Strip the final "}" and append the new branches
    new_match = old_match.rstrip().rstrip("}").rstrip() + "\n}" + canceled_block

    node["parameters"]["jsCode"] = code.replace(old_match, new_match, 1)
    print("Patched 'Parse & Map State' node — added canceled → Deprioritized mapping.")

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

    print(f"\n✅ Workflow updated (HTTP {put_resp.status_code}).")
    print("Canceled → Deprioritized mapping is now live in the webhook workflow.")


if __name__ == "__main__":
    main()
