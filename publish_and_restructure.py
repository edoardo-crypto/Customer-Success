#!/usr/bin/env python3
"""
Restructure Intercom Help Center:
  Step 1 — Rename + move 15 integration articles from Section 5 → Section 4
  Step 2 — Delete the now-empty Section 5 collection
  Step 3 — Publish all draft articles across sections 1–4
  Step 4 — Print summary and verify Section 4 count
"""

import os
import re
import time
import requests

# ─── Constants ────────────────────────────────────────────────────────────────

INTERCOM_API_BASE = "https://api.intercom.io"
INTERCOM_VERSION  = "2.11"
CREDENTIALS_FILE  = "credentials.md"

SECTION_4_ID = 18648386   # Settings — destination for integration articles
SECTION_5_ID = 18649943   # Integrations standalone collection — to be deleted

# All section IDs to publish drafts from (sections 1–4)
ALL_SECTION_IDS = [18648330, 18648340, 18648371, 18648386]

# 15 integration articles: (article_id, new_title)
INTEGRATION_ARTICLES = [
    (13834957, "4.4.1: WhatsApp"),
    (13834959, "4.4.2: Gmail"),
    (13834960, "4.4.3: Meta / Instagram"),
    (13834961, "4.4.4: Live Chat"),
    (13834962, "4.4.5: Generic Email"),
    (13834964, "4.4.6: Side Conversation Email"),
    (13834966, "4.4.7: Shopify"),
    (13834967, "4.4.8: WooCommerce"),
    (13834968, "4.4.9: Klaviyo"),
    (13834969, "4.4.10: Appstle"),
    (13834988, "4.4.11: Loop"),
    (13834970, "4.4.12: Recharge"),
    (13834971, "4.4.13: SendCloud"),
    (13834972, "4.4.14: Gorgias"),
    (13834973, "4.4.15: Zendesk"),
]


# ─── Credentials ──────────────────────────────────────────────────────────────

def read_credentials():
    creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), CREDENTIALS_FILE)
    with open(creds_path, "r") as f:
        content = f.read()
    notion   = re.search(r"## Notion\n```\n(.+?)\n```",   content, re.DOTALL).group(1).strip()
    intercom = re.search(r"## Intercom\n```\n(.+?)\n```", content, re.DOTALL).group(1).strip()
    return {"notion": notion, "intercom": intercom}


# ─── HTTP helper ──────────────────────────────────────────────────────────────

def api_request(method, url, headers, json_body=None, retries=3, allow_404=False):
    for attempt in range(retries + 1):
        try:
            resp = requests.request(method, url, headers=headers, json=json_body, timeout=30)
        except requests.RequestException as e:
            if attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Network error on {method} {url}: {e}")

        if resp.status_code == 429:
            wait = 2 ** attempt
            print(f"    [rate limit] sleeping {wait}s …")
            time.sleep(wait)
            continue

        if resp.status_code == 404 and allow_404:
            return None   # caller handles gracefully

        if resp.status_code >= 500:
            if attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Server error {resp.status_code} on {method} {url}: {resp.text[:200]}")

        if resp.status_code == 204:
            return {}     # DELETE with no body — success

        if not resp.ok:
            raise RuntimeError(f"HTTP {resp.status_code} on {method} {url}: {resp.text[:300]}")

        return resp.json()

    raise RuntimeError(f"Failed after {retries} retries: {method} {url}")


def intercom_headers(token):
    return {
        "Authorization":    f"Bearer {token}",
        "Intercom-Version": INTERCOM_VERSION,
        "Content-Type":     "application/json",
    }


# ─── List articles in a collection ───────────────────────────────────────────

def list_articles_in_collection(collection_id, token):
    """
    Paginate through GET /articles and return those whose parent_id matches
    the given collection.  Intercom's default page size is 15; we use 50.
    """
    headers  = intercom_headers(token)
    articles = []
    page     = 1
    while True:
        url  = f"{INTERCOM_API_BASE}/articles?page={page}&per_page=50"
        data = api_request("GET", url, headers)
        for item in data.get("data", []):
            if (item.get("parent_id")   == collection_id and
                    item.get("parent_type") == "collection"):
                articles.append(item)
        pages = data.get("pages", {})
        if page >= pages.get("total_pages", 1):
            break
        page += 1
    return articles


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    creds   = read_credentials()
    token   = creds["intercom"]
    headers = intercom_headers(token)

    moved     = []   # records for summary
    published = []
    failed    = []

    # ── Step 1: Rename + move integration articles ────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 1  Rename + move 15 integration articles → Section 4")
    print("=" * 60)

    for article_id, new_title in INTEGRATION_ARTICLES:
        url     = f"{INTERCOM_API_BASE}/articles/{article_id}"
        payload = {
            "title":       new_title,
            "parent_id":   SECTION_4_ID,
            "parent_type": "collection",
        }
        try:
            api_request("PUT", url, headers, json_body=payload)
            print(f"  ✓  {article_id}  →  {new_title}")
            moved.append({"id": article_id, "title": new_title, "result": "OK"})
        except Exception as e:
            print(f"  ✗  FAILED {article_id} ({new_title}): {e}")
            failed.append({"id": article_id, "title": new_title, "result": f"MOVE FAILED: {e}"})
        time.sleep(0.4)

    # ── Step 2: Delete Section 5 ──────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 2  Delete Section 5 (standalone Integrations collection)")
    print("=" * 60)

    del_url = f"{INTERCOM_API_BASE}/help_center/collections/{SECTION_5_ID}"
    try:
        result = api_request("DELETE", del_url, headers, allow_404=True)
        if result is None:
            print(f"  → 404: collection {SECTION_5_ID} already gone or not found — skipping")
        else:
            print(f"  ✓  Deleted collection {SECTION_5_ID}")
    except Exception as e:
        print(f"  ✗  DELETE failed (non-critical, continuing): {e}")

    # ── Step 3: Publish all draft articles in sections 1–4 ───────────────────
    print("\n" + "=" * 60)
    print("STEP 3  Publish all draft articles (sections 1–4)")
    print("=" * 60)

    for section_id in ALL_SECTION_IDS:
        print(f"\n  Section {section_id}:")
        try:
            articles = list_articles_in_collection(section_id, token)
        except Exception as e:
            print(f"    ✗  Could not list articles: {e}")
            continue

        drafts = [a for a in articles if a.get("state") == "draft"]
        print(f"    {len(articles)} articles total, {len(drafts)} draft(s)")

        for article in drafts:
            art_id    = article["id"]
            art_title = article.get("title", "(no title)")
            url       = f"{INTERCOM_API_BASE}/articles/{art_id}"
            try:
                api_request("PUT", url, headers, json_body={"state": "published"})
                print(f"    ✓  Published: {art_title} ({art_id})")
                published.append({"id": art_id, "title": art_title,
                                  "section": section_id, "result": "OK"})
            except Exception as e:
                print(f"    ✗  FAILED: {art_title} ({art_id}): {e}")
                failed.append({"id": art_id, "title": art_title,
                               "section": section_id, "result": f"PUBLISH FAILED: {e}"})
            time.sleep(0.4)

    # ── Step 4: Summary ───────────────────────────────────────────────────────
    print("\n\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Moved:     {len(moved)}/15 integration articles into Section 4")
    print(f"  Published: {len(published)} articles")
    print(f"  Failed:    {len(failed)}")

    if failed:
        print("\n  Failed items:")
        for item in failed:
            print(f"    [{item['id']}] {item['title']}  →  {item['result']}")

    # ── Step 5: Quick API verification ────────────────────────────────────────
    print("\n" + "=" * 60)
    print("VERIFICATION  Section 4 article count")
    print("=" * 60)
    try:
        s4_articles = list_articles_in_collection(SECTION_4_ID, token)
        pub_count   = sum(1 for a in s4_articles if a.get("state") == "published")
        draft_count = sum(1 for a in s4_articles if a.get("state") == "draft")
        print(f"  Section 4 total : {len(s4_articles)} articles  (expected 26)")
        print(f"  Published : {pub_count}   Draft : {draft_count}")
        print()
        for a in sorted(s4_articles, key=lambda x: x.get("title", "")):
            print(f"    [{a['id']}]  {a.get('state','?'):9s}  {a.get('title','')}")
    except Exception as e:
        print(f"  ✗  Verification failed: {e}")

    # Check Section 5 gone
    print("\n" + "=" * 60)
    print("VERIFICATION  Section 5 should be gone")
    print("=" * 60)
    check_url = f"{INTERCOM_API_BASE}/help_center/collections/{SECTION_5_ID}"
    result = api_request("GET", check_url, headers, allow_404=True)
    if result is None:
        print(f"  ✓  404 confirmed — collection {SECTION_5_ID} no longer exists")
    else:
        print(f"  ✗  Collection still exists: {result}")


if __name__ == "__main__":
    main()
