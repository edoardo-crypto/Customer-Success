"""
cleanup_customer_page_tables.py
Phase 1 of CS Onboarding Linked Views setup.

1. Queries all non-Canceled MCT customer pages via data_sources API
2. Deletes any existing blocks (inline tables, headings, dividers) from each page
3. Prints page IDs + names + Notion URLs for Phase 2 browser automation

Usage:  python3 cleanup_customer_page_tables.py
Archive to archive/ after run.
"""

import requests
import time
import creds

# ── Credentials ────────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
MCT_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"

HEADERS_DS     = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}
HEADERS_BLOCKS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# ── Step 1: Collect all non-Canceled customer pages ────────────────────────────
def fetch_active_pages():
    """Return list of {page_id, company_name} for Active + Churning customers."""
    pages = []
    cursor = None

    while True:
        body = {
            "page_size": 100,
            "filter": {
                "property": "💰 Billing Status",
                "select": {
                    "does_not_equal": "Canceled"
                }
            }
        }
        if cursor:
            body["start_cursor"] = cursor

        resp = requests.post(
            f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query",
            headers=HEADERS_DS,
            json=body,
        )
        resp.raise_for_status()
        data = resp.json()

        for page in data.get("results", []):
            page_id = page["id"]
            # Extract company name from title property
            title_prop = page["properties"].get("🏢 Company Name", {})
            title_parts = title_prop.get("title", [])
            company_name = "".join(t.get("plain_text", "") for t in title_parts).strip()
            if not company_name:
                company_name = f"(unnamed {page_id[:8]})"

            billing_status = (
                page["properties"]
                .get("💰 Billing Status", {})
                .get("select", {}) or {}
            ).get("name", "Unknown")

            pages.append({
                "page_id": page_id,
                "company_name": company_name,
                "billing_status": billing_status,
            })

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return pages


# ── Step 2 & 3: Get children + delete each block ───────────────────────────────
def get_block_children(page_id):
    """Return list of top-level block IDs for a page."""
    block_ids = []
    cursor = None

    while True:
        params = {"page_size": 100}
        if cursor:
            params["start_cursor"] = cursor

        resp = requests.get(
            f"https://api.notion.com/v1/blocks/{page_id}/children",
            headers=HEADERS_BLOCKS,
            params=params,
        )
        resp.raise_for_status()
        data = resp.json()

        for block in data.get("results", []):
            block_ids.append((block["id"], block.get("type", "unknown")))

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return block_ids


def delete_block(block_id):
    """Delete a single block."""
    resp = requests.delete(
        f"https://api.notion.com/v1/blocks/{block_id}",
        headers=HEADERS_BLOCKS,
    )
    resp.raise_for_status()


def page_url(page_id):
    return f"https://www.notion.so/{page_id.replace('-', '')}"


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("Fetching active/churning customer pages from MCT…")
    pages = fetch_active_pages()
    print(f"  → Found {len(pages)} non-Canceled customers\n")

    cleaned = []

    for p in pages:
        pid  = p["page_id"]
        name = p["company_name"]
        status = p["billing_status"]

        print(f"[{status}] {name}")
        blocks = get_block_children(pid)

        if not blocks:
            print(f"  (no blocks — skipping)")
        else:
            print(f"  Deleting {len(blocks)} blocks…")
            for bid, btype in blocks:
                print(f"    delete {btype} {bid}")
                delete_block(bid)
                time.sleep(0.15)   # stay under Notion rate limit

        cleaned.append(p)
        time.sleep(0.1)

    print("\n" + "=" * 60)
    print("PHASE 2 — Customer page URLs")
    print("=" * 60)
    for p in cleaned:
        url = page_url(p["page_id"])
        print(f"{p['company_name']}\t{url}")

    print(f"\nTotal: {len(cleaned)} pages ready for Phase 2")


if __name__ == "__main__":
    main()
