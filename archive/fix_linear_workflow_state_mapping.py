#!/usr/bin/env python3
"""
fix_linear_workflow_state_mapping.py

Patches the 'Parse & Map State' Code node in the Linear→Notion sync workflow
(xdVkUh6YCtcuW8QM) to use name-first state mapping so that "In Testing"
(type=completed, name includes 'testing') is correctly mapped to 'In Progress'
instead of 'Resolved'.
"""

import json
import requests
import creds

N8N_BASE = "https://konvoai.app.n8n.cloud"
API_KEY  = creds.get("N8N_API_KEY")
WF_ID    = "xdVkUh6YCtcuW8QM"

HEADERS = {
    "X-N8N-API-KEY": API_KEY,
    "Content-Type": "application/json",
}

# --- The current broken block (partial fix + leftover duplicate + stray closing brace) ---
OLD_BLOCK = """if (stateName.includes('progress') || stateName.includes('review') || stateName.includes('testing')) {
      notionStatus = 'In Progress';
    } else if (stateName.includes('done') || stateName.includes('released') ||
               stateName.includes('complete') || stateName.includes('resolved')) {
      notionStatus = 'Resolved';
    } else if (stateType === 'started') {
      notionStatus = 'In Progress';
    } else if (stateType === 'completed') {
      notionStatus = 'Resolved';
    } else if (
    stateName.includes('done') || stateName.includes('released') ||
    stateName.includes('complete') || stateName.includes('resolved')
  ) {
    notionStatus = 'Resolved';
  }
}"""

# --- The clean fixed block (name-first, no duplicates, no stray brace) ---
NEW_BLOCK = """if (stateName.includes('progress') || stateName.includes('review') || stateName.includes('testing')) {
  notionStatus = 'In Progress';
} else if (stateName.includes('done') || stateName.includes('released') ||
           stateName.includes('complete') || stateName.includes('resolved')) {
  notionStatus = 'Resolved';
} else if (stateType === 'started') {
  notionStatus = 'In Progress';
} else if (stateType === 'completed') {
  notionStatus = 'Resolved';
}"""


def main():
    # 1. Fetch workflow
    resp = requests.get(f"{N8N_BASE}/api/v1/workflows/{WF_ID}", headers=HEADERS)
    resp.raise_for_status()
    wf = resp.json()
    print(f"Fetched workflow: {wf.get('name')} (active={wf.get('active')})")

    # 2. Find Parse & Map State node and patch jsCode
    patched = False
    for node in wf.get("nodes", []):
        if node.get("name") == "Parse & Map State":
            code = node["parameters"].get("jsCode", "")
            if OLD_BLOCK not in code:
                print("\n⚠️  Old block not found verbatim in node jsCode.")
                print("--- Current jsCode snippet around 'stateType' ---")
                # Print the relevant section for debugging
                for i, line in enumerate(code.splitlines()):
                    if 'stateType' in line or 'stateName' in line or 'notionStatus' in line:
                        print(f"  {i:3}: {line}")
                print("---")
                print("Attempting flexible patch…")
                patched = flexible_patch(node)
                if not patched:
                    raise ValueError("Could not patch node — manual inspection required.")
            else:
                node["parameters"]["jsCode"] = code.replace(OLD_BLOCK, NEW_BLOCK, 1)
                patched = True
                print("Patched 'Parse & Map State' node (exact match).")
            break

    if not patched:
        raise ValueError("Node 'Parse & Map State' not found in workflow.")

    # 3. Build PUT body — only allowed keys
    put_body = {
        "name":        wf["name"],
        "nodes":       wf["nodes"],
        "connections": wf["connections"],
        "settings":    wf.get("settings", {}),
    }

    # 4. PUT updated workflow
    put_resp = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{WF_ID}",
        headers=HEADERS,
        json=put_body,
    )
    if put_resp.status_code != 200:
        print(f"PUT failed {put_resp.status_code}: {put_resp.text[:500]}")
        put_resp.raise_for_status()

    print(f"\n✅ Workflow updated successfully (HTTP {put_resp.status_code}).")
    print("Name-first state mapping is now live. No UI toggle needed.")


def flexible_patch(node):
    """
    Fallback: replace the whole if-else chain using a broad regex that
    matches from the first stateName/stateType check to the final closing brace.
    """
    import re
    code = node["parameters"].get("jsCode", "")

    # Match the whole conditional block starting with the name-first check
    # and ending with the stray closing brace (the partially-fixed state)
    pattern = re.compile(
        r"if \(stateName\.includes\('progress'\).*?"   # opening if
        r"notionStatus = 'Resolved';\s*\}\s*\}",       # last closing brace (stray one)
        re.DOTALL,
    )
    m = pattern.search(code)
    if not m:
        # Also try the very original broken form (type-first)
        pattern2 = re.compile(
            r"if \(stateType === 'started'\).*?else if \(!stateType\) \{.*?\}",
            re.DOTALL,
        )
        m = pattern2.search(code)
        if not m:
            return False

    node["parameters"]["jsCode"] = code[:m.start()] + NEW_BLOCK + code[m.end():]
    print("Patched 'Parse & Map State' node (regex fallback).")
    return True


if __name__ == "__main__":
    main()
