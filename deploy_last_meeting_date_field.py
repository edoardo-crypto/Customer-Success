#!/usr/bin/env python3
"""
deploy_last_meeting_date_field.py
----------------------------------
ONE-TIME SETUP SCRIPT — run once, then archive.

Step 1: Adds "📅 Last Meeting Date 🔒" (date type) to the Master Customer Table
        via PATCH /data_sources/{MCT_DS_ID} with Notion-Version 2025-09-03.

Step 2: Backfills it by copying the current "📞 Last Contact Date 🔒" value
        into the new field for every MCT page.

Safe to re-run: adding a property that already exists is a no-op on Notion.

NOTE: Never set any other property to null in the data_sources PATCH — it can
silently corrupt other properties on multi-source MCT databases.
"""

import json
import urllib.request
import urllib.error
import creds

# ── Config ────────────────────────────────────────────────────────────────────
NOTION_TOKEN      = creds.get("NOTION_TOKEN")
MCT_DS_ID         = "3ceb1ad0-91f1-40db-945a-c51c58035898"
MCT_DB_ID         = "84feda19cfaf4c6e9500bf21d2aaafef"

SRC_FIELD  = "📞 Last Contact Date 🔒"   # copy source
DEST_FIELD = "📅 Last Meeting Date 🔒"   # new field to create + fill


# ── Notion helper ─────────────────────────────────────────────────────────────

def notion_request(method, path, body=None, version="2022-06-28"):
    url  = f"https://api.notion.com/v1/{path}"
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization",  f"Bearer {NOTION_TOKEN}")
    req.add_header("Notion-Version", version)
    req.add_header("Content-Type",   "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        raise Exception(f"HTTP {e.code} {e.reason} — {body_text}") from None


# ── Step 1: Add the new property to MCT schema ────────────────────────────────

def add_last_meeting_date_property():
    """
    PATCH the MCT data source schema to add 📅 Last Meeting Date 🔒 (date type).
    Only adds the new property — never sets others to null.
    """
    body = {
        "properties": {
            DEST_FIELD: {"date": {}}
        }
    }
    print(f"  PATCHing data_sources/{MCT_DS_ID} to add '{DEST_FIELD}'...")
    resp = notion_request(
        "PATCH",
        f"data_sources/{MCT_DS_ID}",
        body,
        version="2025-09-03",
    )
    # Notion-Version 2025-09-03 doesn't return properties in the response body —
    # trust the 200 status.
    print(f"  ✓ PATCH complete (status: 200 assumed if no exception)")
    return resp


# ── Step 2: Fetch all MCT pages ───────────────────────────────────────────────

def fetch_all_mct_pages():
    """
    Fetch every page from MCT regardless of billing status (we want to backfill all).
    Returns list of { page_id, name, src_date } where src_date may be None.
    """
    pages  = []
    cursor = None

    while True:
        req_body = {"page_size": 100}
        if cursor:
            req_body["start_cursor"] = cursor

        resp = notion_request(
            "POST",
            f"data_sources/{MCT_DS_ID}/query",
            req_body,
            version="2025-09-03",
        )

        for page in resp.get("results", []):
            props = page.get("properties", {})

            name_parts = (props.get("🏢 Company Name") or {}).get("title", [])
            name = "".join(t.get("plain_text", "") for t in name_parts).strip()

            src_date_raw = (
                ((props.get(SRC_FIELD) or {}).get("date") or {})
                .get("start", None)
            )
            src_date = src_date_raw[:10] if src_date_raw else None

            pages.append({
                "page_id":  page["id"],
                "name":     name or "(unnamed)",
                "src_date": src_date,
            })

        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break

    return pages


# ── Step 3: Backfill each page ────────────────────────────────────────────────

def backfill_page(page_id, date_str):
    """Write date_str into DEST_FIELD on one MCT page."""
    body = {
        "properties": {
            DEST_FIELD: {"date": {"start": date_str}}
        }
    }
    notion_request("PATCH", f"pages/{page_id}", body, version="2025-09-03")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 65)
    print("  deploy_last_meeting_date_field.py")
    print(f"  Adds '{DEST_FIELD}' to MCT and backfills from '{SRC_FIELD}'")
    print("=" * 65)

    # Step 1: Schema change
    print("\n[1/3] Adding property to MCT schema...")
    add_last_meeting_date_property()

    # Step 2: Fetch all pages
    print("\n[2/3] Fetching all MCT pages...")
    pages = fetch_all_mct_pages()
    with_date    = [p for p in pages if p["src_date"]]
    without_date = [p for p in pages if not p["src_date"]]
    print(f"  Total pages: {len(pages)}")
    print(f"  With '{SRC_FIELD}': {len(with_date)}")
    print(f"  Without (will skip):   {len(without_date)}")

    # Step 3: Backfill
    print(f"\n[3/3] Backfilling '{DEST_FIELD}'...")
    print()
    print(f"  {'Customer':<35} {'Date'}")
    print(f"  {'-'*35} {'-'*12}")

    updated = errors = skipped = 0

    for p in sorted(pages, key=lambda x: x["name"].lower()):
        if not p["src_date"]:
            print(f"  {p['name']:<35} (no source date — skip)")
            skipped += 1
            continue

        print(f"  {p['name']:<35} {p['src_date']}")
        try:
            backfill_page(p["page_id"], p["src_date"])
            updated += 1
        except Exception as e:
            print(f"    ✗ Error: {e}")
            errors += 1

    print()
    print("=" * 65)
    print(f"  Done.  Backfilled: {updated}  |  Skipped (no date): {skipped}  |  Errors: {errors}")
    print("=" * 65)
    print()
    print("Next steps:")
    print("  1. Verify 📅 Last Meeting Date 🔒 appears in Notion MCT")
    print("  2. Run sync_last_contact.py once to confirm it writes to the new field")
    print("  3. Run fix_last_contact_sync_field.py to update the n8n workflow")
    print()


if __name__ == "__main__":
    main()
