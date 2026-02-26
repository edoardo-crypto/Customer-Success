#!/usr/bin/env python3
"""
restore_domain.py

Fixes the 🏢 Domain column in the Notion Master Customer Table (MCT),
which was wiped when the property type was inadvertently changed from
'rich_text' to 'date'.

Steps
-----
1. Patch the database schema: restore 🏢 Domain type to rich_text
   (via PATCH /databases/{id} with Notion-Version 2025-09-03)
2. Query all MCT pages (paginated, via data_sources query)
3. For each page: read 🔗 Stripe Customer ID → call Stripe → extract
   domain from email → PATCH the Notion page with the domain value
4. (Optional) Update n8n workflow Ai9Y3FWjqMtEhr57 so that future
   Stripe Sync runs also write Domain — set RUN_N8N_UPDATE = True below.
"""

import json
import time
import requests

# ── Credentials ───────────────────────────────────────────────────────────────
STRIPE_KEY    = "***REMOVED***"
NOTION_TOKEN  = "***REMOVED***"
N8N_BASE      = "https://konvoai.app.n8n.cloud"
N8N_API_KEY   = (
    "***REMOVED_JWT***."
    "eyJzdWIiOiI0ODJlMzA2MS04MjAwLTQ2ZTgtODBiZS1iZjJhYjE0Mzg0MTUiLCJpc3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzcwNzIzNjIxLCJleHAiOjE3NzMyNzAwMDB9."
    "X4wZVbatYXVttzSEZIXQd-Ot--VbQupJsoNoOmZc8o0"
)

# ── Constants ─────────────────────────────────────────────────────────────────
NOTION_DB_ID    = "84feda19cfaf4c6e9500bf21d2aaafef"
NOTION_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"
STRIPE_SYNC_WF  = "Ai9Y3FWjqMtEhr57"

# Set to True to also update the n8n Stripe Sync workflow (Step 4)
RUN_N8N_UPDATE = False

# ── HTTP headers ───────────────────────────────────────────────────────────────
notion_v2022 = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2022-06-28",
}
notion_v2025 = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Content-Type":   "application/json",
    "Notion-Version": "2025-09-03",
}
stripe_headers = {"Authorization": f"Bearer {STRIPE_KEY}"}
n8n_headers    = {
    "X-N8N-API-KEY": N8N_API_KEY,
    "Content-Type":  "application/json",
}


# ══════════════════════════════════════════════════════════════════════════════
# Step 1 — Restore schema
# ══════════════════════════════════════════════════════════════════════════════

def fix_domain_schema():
    """Patch 🏢 Domain type back to rich_text on the MCT data source.

    Note: PATCH /databases/{id} with 2025-09-03 returns 200 but does NOT
    actually change property types on multi-source databases — it silently
    does nothing.  PATCH /data_sources/{ds_id} is the correct endpoint.
    """
    r = requests.patch(
        f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}",
        headers=notion_v2025,
        json={"properties": {"🏢 Domain": {"rich_text": {}}}},
    )
    if r.status_code == 200:
        print("  ✓ Schema restored via data_sources (200)")
    else:
        print(f"  ✗ Failed: {r.status_code} {r.text[:400]}")
        raise RuntimeError("Schema fix failed — aborting")


# ══════════════════════════════════════════════════════════════════════════════
# Step 2 — Query all MCT pages
# ══════════════════════════════════════════════════════════════════════════════

def query_all_pages():
    """Return all non-archived pages from MCT via data_sources/query (paginated)."""
    pages        = []
    has_more     = True
    start_cursor = None
    page_num     = 0

    while has_more:
        page_num += 1
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor

        r = requests.post(
            f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
            headers=notion_v2025,
            json=body,
        )
        r.raise_for_status()
        data = r.json()

        batch        = data.get("results", [])
        has_more     = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

        pages.extend(batch)
        print(f"  Page {page_num}: {len(batch)} rows (total: {len(pages)})")
        time.sleep(0.3)

    return pages


# ══════════════════════════════════════════════════════════════════════════════
# Step 3 — Stripe lookup + Notion PATCH
# ══════════════════════════════════════════════════════════════════════════════

def get_domain_from_stripe(stripe_id):
    """Return the domain extracted from the Stripe customer email, or None."""
    r = requests.get(
        f"https://api.stripe.com/v1/customers/{stripe_id}",
        headers=stripe_headers,
    )
    if r.status_code != 200:
        return None
    email = r.json().get("email", "")
    return email.split("@")[1].lower() if "@" in email else None


def patch_notion_domain(page_id, domain, verbose=False):
    """Write the domain value to 🏢 Domain on a Notion page. Returns True on success."""
    payload = {
        "properties": {
            "🏢 Domain": {
                "rich_text": [{"text": {"content": domain}}]
            }
        }
    }
    # 2025-09-03 is required for pages in multi-source databases; try 2022 as fallback
    for headers in (notion_v2025, notion_v2022):
        r = requests.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=headers,
            json=payload,
        )
        if r.status_code == 200:
            return True
        if verbose:
            print(f"    [{headers['Notion-Version']}] {r.status_code}: {r.text[:200]}")
    return False


def repopulate_domains(pages):
    success = skip = fail = 0

    for page in pages:
        page_id = page["id"]
        props   = page.get("properties", {})

        # Company name (for log readability)
        name_prop = props.get("🏢 Company Name", {})
        titles    = name_prop.get("title", [])
        name      = titles[0]["plain_text"] if titles else page_id[:8]

        # Stripe Customer ID
        stripe_prop  = props.get("🔗 Stripe Customer ID", {})
        stripe_texts = stripe_prop.get("rich_text", [])
        stripe_id    = stripe_texts[0]["plain_text"].strip() if stripe_texts else ""

        if not stripe_id:
            print(f"  SKIP  {name[:45]:<45}  (no Stripe ID)")
            skip += 1
            continue

        domain = get_domain_from_stripe(stripe_id)
        if not domain:
            print(f"  SKIP  {name[:45]:<45}  [{stripe_id}] (no email in Stripe)")
            skip += 1
            time.sleep(0.2)
            continue

        ok = patch_notion_domain(page_id, domain, verbose=(success + skip + fail == 0))
        if ok:
            print(f"  OK    {name[:45]:<45}  → {domain}")
            success += 1
        else:
            print(f"  FAIL  {name[:45]:<45}  → {domain}")
            fail += 1

        time.sleep(0.35)  # Stay well within Notion rate limits (3 req/s)

    return success, skip, fail


# ══════════════════════════════════════════════════════════════════════════════
# Step 4 (Optional) — Update n8n Stripe Sync to also write Domain
# ══════════════════════════════════════════════════════════════════════════════

def update_stripe_sync_workflow():
    """
    Adds 🏢 Domain to the 'Update Existing Row' HTTP Request node body in the
    Stripe → Notion Sync workflow so future sync runs keep Domain up to date.
    """
    import re

    print("  Fetching workflow …")
    r = requests.get(f"{N8N_BASE}/api/v1/workflows/{STRIPE_SYNC_WF}", headers=n8n_headers)
    r.raise_for_status()
    workflow = r.json()

    # Find 'Update Existing Row' node
    update_node = next(
        (n for n in workflow["nodes"] if "Update" in n.get("name", "") and "Row" in n.get("name", "")),
        None,
    )
    if not update_node:
        print("  ✗ 'Update Existing Row' node not found — skipping n8n update")
        return

    print(f"  Found node: '{update_node['name']}'")
    params = update_node.get("parameters", {})

    # Locate the body field (could be 'body', 'jsonBody', or 'bodyParameters')
    body_key = next((k for k in ("body", "jsonBody", "bodyParameters") if k in params), None)
    if not body_key:
        print(f"  ✗ No recognized body field in node params — skipping")
        return

    body_str = params[body_key]
    domain_snippet = r',"🏢 Domain":{"rich_text":[{"text":{"content":"{{ $(\'Transform Active Subs\').item.json.domain }}"}}]}'

    if "🏢 Domain" in body_str:
        print("  Domain already present in Update Existing Row — no change needed")
        return

    # Insert before the closing } of the JSON body
    # Find the last } and insert before it
    idx = body_str.rfind("}")
    if idx == -1:
        print("  ✗ Could not find closing brace in body — skipping")
        return

    updated_body = body_str[:idx] + domain_snippet + body_str[idx:]
    params[body_key] = updated_body
    print("  Domain field added to Update Existing Row body")

    # Deactivate → PUT → Activate
    requests.post(f"{N8N_BASE}/api/v1/workflows/{STRIPE_SYNC_WF}/deactivate", headers=n8n_headers)
    print("  Workflow deactivated")

    put_payload = {
        "name":        workflow["name"],
        "nodes":       workflow["nodes"],
        "connections": workflow["connections"],
        "settings":    workflow.get("settings", {}),
    }
    r2 = requests.put(
        f"{N8N_BASE}/api/v1/workflows/{STRIPE_SYNC_WF}",
        headers=n8n_headers,
        json=put_payload,
    )
    if r2.status_code != 200:
        print(f"  ✗ PUT failed {r2.status_code}: {r2.text[:300]}")
    else:
        print("  ✓ Workflow updated")

    requests.post(f"{N8N_BASE}/api/v1/workflows/{STRIPE_SYNC_WF}/activate", headers=n8n_headers)
    print("  Workflow re-activated")


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    sep = "=" * 62

    # ── Step 1: Fix schema ───────────────────────────────────────────────────
    print(f"\n{sep}")
    print("STEP 1 — Restore 🏢 Domain property type to rich_text")
    print(sep)
    fix_domain_schema()

    # Give Notion a moment to propagate the schema change before writing values
    print("  Waiting 3s for schema propagation …")
    time.sleep(3)

    # ── Step 2: Fetch all pages ──────────────────────────────────────────────
    print(f"\n{sep}")
    print("STEP 2 — Fetch all MCT pages")
    print(sep)
    pages = query_all_pages()
    print(f"  Total pages retrieved: {len(pages)}")

    # ── Step 3: Repopulate domains ───────────────────────────────────────────
    print(f"\n{sep}")
    print("STEP 3 — Repopulate Domain from Stripe emails")
    print(sep)
    success, skip, fail = repopulate_domains(pages)

    # ── Step 4 (Optional): Update n8n Stripe Sync ────────────────────────────
    if RUN_N8N_UPDATE:
        print(f"\n{sep}")
        print("STEP 4 — Update n8n Stripe Sync workflow (add Domain to Update Existing Row)")
        print(sep)
        update_stripe_sync_workflow()

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{sep}")
    print("DONE")
    print(sep)
    print(f"  Updated : {success}")
    print(f"  Skipped : {skip}  (no Stripe ID or no email found)")
    print(f"  Failed  : {fail}")
    print()
    print("Verification checklist:")
    print("  1. Open Notion MCT — 🏢 Domain column should show email domains")
    print("  2. Spot-check 3-5 rows: domain should match customer email @-suffix")
    print("  3. Rows with no Stripe ID will remain blank (expected)")
    if not RUN_N8N_UPDATE:
        print()
        print("  Optional Step 4 (n8n Stripe Sync update) was NOT run.")
        print("  To enable: set RUN_N8N_UPDATE = True at the top of this script.")


if __name__ == "__main__":
    main()
