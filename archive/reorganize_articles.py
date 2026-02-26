"""
Reorganize existing Intercom articles into the new section structure.

Steps:
1. Move 4 articles into Section 3: Inbox (18648371)
2. Move 1 article into Section 1: Konvo AI Overview (18648330)
3. Refresh the body of "Open a New Conversation" (10301960) — make it channel-agnostic
4. Print summary
"""

import os
import re
import time
import requests

# ─── Reuse helpers from migrate.py ────────────────────────────────────────────

INTERCOM_API_BASE = "https://api.intercom.io"
INTERCOM_VERSION  = "2.11"
CREDENTIALS_FILE  = "credentials.md"

def read_credentials():
    creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), CREDENTIALS_FILE)
    with open(creds_path, 'r') as f:
        content = f.read()
    intercom = re.search(r'## Intercom\n```\n(.+?)\n```', content, re.DOTALL).group(1).strip()
    return {'intercom': intercom}


def api_request(method, url, headers, json_body=None, retries=3):
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
            print(f"    [rate limit] sleeping {wait}s...")
            time.sleep(wait)
            continue

        if resp.status_code >= 500:
            if attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Server error {resp.status_code}: {resp.text[:200]}")

        if not resp.ok:
            raise RuntimeError(f"HTTP {resp.status_code} on {method} {url}: {resp.text[:300]}")

        return resp.json()

    raise RuntimeError(f"Failed after {retries} retries: {method} {url}")


def intercom_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Intercom-Version": INTERCOM_VERSION,
        "Content-Type": "application/json",
    }


# ─── Article moves ────────────────────────────────────────────────────────────

SECTION_3_INBOX    = 18648371   # 3: Inbox
SECTION_1_OVERVIEW = 18648330   # 1: Konvo AI Overview

# Articles to move into Section 3: Inbox
MOVE_TO_SECTION_3 = [
    {"id": 9474341,  "new_title": "3.4: Message Composer"},
    {"id": 9538772,  "new_title": "3.5: Quick Replies"},
    {"id": 11537314, "new_title": "3.6: Snooze Conversations"},
    {"id": 10301960, "new_title": "3.7: Open a New Conversation"},
]

# Article to move into Section 1: Overview
MOVE_TO_SECTION_1 = [
    {"id": 9685054, "new_title": "1.4: Enable Notifications"},
]

# Article whose body needs refreshing (channel-agnostic rewrite)
REFRESH_ARTICLE_ID = 10301960


# ─── Body refresh ─────────────────────────────────────────────────────────────

REFRESHED_BODY = """
<h2>Open a New Conversation</h2>

<p>
  You can start a new outbound conversation with any existing contact — whether they've
  reached out before or you want to reach out proactively.
</p>

<h2>How to open a new conversation</h2>

<ol>
  <li>Go to the <b>Inbox</b> in the left navigation bar.</li>
  <li>Click the <b>pencil / compose icon</b> (usually in the top-right of the conversation list).</li>
  <li>In the <b>To</b> field, search for the contact by name, email address, or phone number. Select them from the dropdown.</li>
  <li>If the contact doesn't exist yet, you can create a new contact by typing their details.</li>
  <li>Choose the <b>channel</b> you want to use — for example WhatsApp, email, or live chat — from the channel selector.</li>
  <li>Type your message in the composer and press <b>Send</b>.</li>
</ol>

<h2>Tips</h2>

<ul>
  <li>The available channels depend on which integrations your workspace has enabled (WhatsApp, Gmail, live chat widget, etc.).</li>
  <li>For WhatsApp conversations, the contact must have previously opted in or you must use an approved message template to initiate the conversation.</li>
  <li>For email, you can compose a free-form message at any time.</li>
  <li>The conversation will appear in your Inbox and in the contact's conversation history.</li>
</ul>

<h2>Can't find a channel?</h2>

<p>
  If you don't see the channel you want, check that the relevant integration is connected in
  <b>Settings → Integrations</b>. If it's connected but still not appearing, contact Konvo AI support.
</p>
""".strip()


# ─── Main ─────────────────────────────────────────────────────────────────────

def move_article(article_id, new_title, parent_id, token):
    """Update an article's title and parent collection."""
    url = f"{INTERCOM_API_BASE}/articles/{article_id}"
    data = api_request(
        "PUT", url, intercom_headers(token),
        json_body={
            "title": new_title,
            "parent_id": parent_id,
            "parent_type": "collection",
        }
    )
    return data


def refresh_article_body(article_id, body_html, token):
    """Update only the body of an article (title/parent untouched in this call)."""
    url = f"{INTERCOM_API_BASE}/articles/{article_id}"
    data = api_request(
        "PUT", url, intercom_headers(token),
        json_body={"body": body_html}
    )
    return data


def main():
    creds = read_credentials()
    token = creds['intercom']
    results = []

    # ── Step 1: Refresh body of "Open a New Conversation" first ──────────────
    # (We do this before the move so the final PUT in the move also carries the
    #  fresh content — actually we'll do them as separate calls for clarity.)
    print("Step 1: Refreshing body of 'Open a New Conversation' (10301960)...")
    try:
        refresh_article_body(REFRESH_ARTICLE_ID, REFRESHED_BODY, token)
        print("  ✓ Body refreshed")
        results.append({"action": "refresh body", "id": REFRESH_ARTICLE_ID,
                        "title": "Open a New Conversation", "status": "OK"})
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        results.append({"action": "refresh body", "id": REFRESH_ARTICLE_ID,
                        "title": "Open a New Conversation", "status": f"FAILED: {e}"})
    time.sleep(0.5)

    # ── Step 2: Move articles into Section 3: Inbox ───────────────────────────
    print(f"\nStep 2: Moving {len(MOVE_TO_SECTION_3)} articles → Section 3: Inbox ({SECTION_3_INBOX})...")
    for article in MOVE_TO_SECTION_3:
        print(f"  Moving {article['id']} → '{article['new_title']}'...")
        try:
            move_article(article['id'], article['new_title'], SECTION_3_INBOX, token)
            print(f"  ✓ Done")
            results.append({"action": "move → Section 3", "id": article['id'],
                            "title": article['new_title'], "status": "OK"})
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            results.append({"action": "move → Section 3", "id": article['id'],
                            "title": article['new_title'], "status": f"FAILED: {e}"})
        time.sleep(0.5)

    # ── Step 3: Move notifications article into Section 1: Overview ───────────
    print(f"\nStep 3: Moving {len(MOVE_TO_SECTION_1)} article → Section 1: Overview ({SECTION_1_OVERVIEW})...")
    for article in MOVE_TO_SECTION_1:
        print(f"  Moving {article['id']} → '{article['new_title']}'...")
        try:
            move_article(article['id'], article['new_title'], SECTION_1_OVERVIEW, token)
            print(f"  ✓ Done")
            results.append({"action": "move → Section 1", "id": article['id'],
                            "title": article['new_title'], "status": "OK"})
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            results.append({"action": "move → Section 1", "id": article['id'],
                            "title": article['new_title'], "status": f"FAILED: {e}"})
        time.sleep(0.5)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n\n=== REORGANIZATION SUMMARY ===")
    print(f"{'Action':<22} {'Article ID':<12} {'Title':<42} {'Status'}")
    print("─" * 100)
    for r in results:
        print(f"{r['action']:<22} {str(r['id']):<12} {r['title']:<42} {r['status']}")

    ok  = sum(1 for r in results if r['status'] == 'OK')
    fail = len(results) - ok
    print(f"\n{ok}/{len(results)} operations succeeded" + (f", {fail} FAILED" if fail else "."))

    print("\nExpected final state:")
    print("  Section 1 (Konvo AI Overview): 1.1 Welcome, 1.2 Use Cases, 1.3 Quick Start, 1.4 Enable Notifications")
    print("  Section 3 (Inbox):             3.0 Overview, 3.1 Feed, 3.2 Agents Config, 3.3 Shared Views,")
    print("                                  3.4 Message Composer, 3.5 Quick Replies, 3.6 Snooze, 3.7 Open Conversation")


if __name__ == '__main__':
    main()
