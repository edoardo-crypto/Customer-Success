"""
Migrate 15 Notion integration sub-articles to a new Intercom Help Center collection.

Usage:
    python3 migrate_integrations.py

Reuses helper functions from migrate.py.
Creates a new collection "5: Integrations" and posts all 15 articles as drafts.
"""

import sys
import time
import os

# Add project directory to path so we can import from migrate.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from migrate import (
    read_credentials,
    fetch_article_html,
    get_intercom_admin_id,
    create_intercom_collection,
    create_intercom_article,
    api_request,
    intercom_headers,
    INTERCOM_API_BASE,
)

# ─── Integration pages to migrate ─────────────────────────────────────────────
# 4 empty stubs (Amphora, Byrd, Recurly, MintSoft) are intentionally excluded.

INTEGRATION_PAGES = [
    ("WhatsApp",                "30de418fd8c48078ab06da17b9ec6ee8"),
    ("Gmail",                   "30de418fd8c4800f90d6d1807cfa049d"),
    ("Meta / Instagram",        "30de418fd8c48014931ae681871ef14d"),
    ("Live Chat",               "310e418fd8c48074bb07e021d33c32f2"),
    ("Generic Email",           "310e418fd8c48052948dcc02d8bfce38"),
    ("Side Conversation Email", "310e418fd8c48016a956f64a8e4308cf"),
    ("Shopify",                 "30de418fd8c4800a993bcb8c5d28ffe4"),
    ("WooCommerce",             "30de418fd8c480d6b1eef58a9c5b9958"),
    ("Klaviyo",                 "30de418fd8c480758320dc47474bfabb"),
    ("Appstle",                 "310e418fd8c480769ff4c91c0495d01c"),
    ("Loop",                    "310e418fd8c480579aa1e143ed251694"),
    ("Recharge",                "310e418fd8c480a19c06c2b4ba233720"),
    ("SendCloud",               "310e418fd8c4801ca13df4be8d14b81d"),
    ("Gorgias",                 "310e418fd8c480028cd2f628b8c0f168"),
    ("Zendesk",                 "310e418fd8c480a1a827ed810a65143a"),
]

COLLECTION_NAME = "5: Integrations"


def get_existing_collections(intercom_token):
    """Fetch all existing Help Center collections to avoid duplicates."""
    data = api_request(
        "GET",
        f"{INTERCOM_API_BASE}/help_center/collections",
        intercom_headers(intercom_token),
    )
    return data.get("data", [])


def main():
    print("=" * 60)
    print("Konvoai Integrations Migration")
    print("=" * 60)

    # Load credentials
    creds = read_credentials()
    notion_token = creds["notion"]
    intercom_token = creds["intercom"]
    print("✓ Credentials loaded")

    # Get admin ID for article authorship
    print("\nFetching Intercom admin ID...")
    admin_id = get_intercom_admin_id(intercom_token)
    print(f"✓ Admin ID: {admin_id}")

    # Check if collection already exists (idempotency)
    print(f"\nChecking for existing '{COLLECTION_NAME}' collection...")
    existing = get_existing_collections(intercom_token)
    collection_id = None
    for col in existing:
        if col.get("name") == COLLECTION_NAME:
            collection_id = col["id"]
            print(f"  Collection already exists (ID: {collection_id}) — reusing it")
            break

    if collection_id is None:
        print(f"  Creating new collection: '{COLLECTION_NAME}'...")
        collection_id = create_intercom_collection(COLLECTION_NAME, intercom_token)
        print(f"  ✓ Collection created (ID: {collection_id})")
        time.sleep(0.5)

    # Migrate each article
    print(f"\nMigrating {len(INTEGRATION_PAGES)} articles...")
    print("-" * 60)

    results = []
    for i, (title, page_id) in enumerate(INTEGRATION_PAGES, start=1):
        print(f"\n[{i}/{len(INTEGRATION_PAGES)}] {title}")
        print(f"    Notion page: {page_id}")

        try:
            # Fetch content from Notion and convert to HTML
            print("    Fetching from Notion...")
            body_html = fetch_article_html(page_id, notion_token)
            char_count = len(body_html)
            print(f"    ✓ Got {char_count} chars of HTML")
            time.sleep(0.4)  # Notion rate limit courtesy

            if char_count < 50:
                print(f"    ⚠ Content looks very short ({char_count} chars) — posting anyway")

            # Create article in Intercom
            print("    Creating article in Intercom (draft)...")
            article_id = create_intercom_article(
                title, body_html, collection_id, admin_id, intercom_token
            )
            time.sleep(0.5)

            print(f"    ✓ Created: '{title}' (Intercom ID: {article_id})")
            results.append({
                "title": title,
                "page_id": page_id,
                "article_id": article_id,
                "status": "OK",
            })

        except Exception as e:
            print(f"    ✗ FAILED: {e}")
            results.append({
                "title": title,
                "page_id": page_id,
                "article_id": None,
                "status": f"FAILED: {e}",
            })

    # Summary
    print("\n\n" + "=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)
    ok_count = sum(1 for r in results if r["status"] == "OK")
    fail_count = len(results) - ok_count

    print(f"\nCollection: '{COLLECTION_NAME}' (ID: {collection_id})")
    print(f"Articles created: {ok_count}/{len(results)}")
    if fail_count:
        print(f"Failures: {fail_count}")

    print("\n{:<30} {:<15} {}".format("Title", "Intercom ID", "Status"))
    print("-" * 65)
    for r in results:
        title_short = r["title"][:29]
        art_id = str(r["article_id"]) if r["article_id"] else "—"
        print(f"{title_short:<30} {art_id:<15} {r['status']}")

    # Gap analysis update
    print("\n" + "=" * 60)
    print("UPDATED GAP ANALYSIS")
    print("=" * 60)
    print(f"\n  Articles in Intercom before migration: 27")
    print(f"  Articles migrated now:                 {ok_count}")
    print(f"  Articles in Intercom after migration:  {27 + ok_count}")
    print()
    print("  Remaining gaps (still to write from scratch):")
    remaining_gaps = [
        ("HIGH",   "Message Composer",              "File limits, audio, internal notes, template access"),
        ("HIGH",   "Quick Replies",                  'The "/" shortcut — create, use, delete'),
        ("HIGH",   "Snooze Conversations",           "Snooze, resurface, workflow use cases"),
        ("HIGH",   "Open a New Conversation",        "Outbound conversation to new or existing contact"),
        ("MEDIUM", "Invite Your Team & First Setup", "Invite users, roles, notifications, orientation"),
        ("MEDIUM", "Enable Notifications",           "Browser push + mobile push setup"),
        ("MEDIUM", "Troubleshooting & FAQ",          "Common errors, file limits, support contact"),
        ("LOW",    "GDPR & Opt-In Compliance",       "Data handling, opt-in, consent"),
    ]
    print(f"  {'#':<4} {'Priority':<8} {'Title':<35} Notes")
    print("  " + "-" * 80)
    for idx, (priority, title, notes) in enumerate(remaining_gaps, start=1):
        print(f"  {idx:<4} {priority:<8} {title:<35} {notes}")

    print(f"\n  Total articles needed (new): {len(remaining_gaps)}")
    print(f"  Final target article count:  {27 + ok_count + len(remaining_gaps)}")
    print()

    if fail_count:
        sys.exit(1)


if __name__ == "__main__":
    main()
