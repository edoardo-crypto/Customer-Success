#!/usr/bin/env python3
"""
diagnose_alex_convs.py

Lists every conversation attributed to Alex de Godoy in W09 (Feb 24 – Mar 2),
with enough detail to verify whether Alex actually handled each one or whether
Aya closed them on his behalf.

Key signal:
  - "Alex replied?" = YES  → last_assignment_admin_reply_at is non-null
                             (Alex was assigned AND sent at least one reply)
  - "Alex replied?" = NO   → null
                             (Alex is credited for closing but never replied —
                              Aya likely did the actual work)

Run: python3 diagnose_alex_convs.py
"""

import requests
from datetime import date, datetime, timezone
import creds

# ── Constants ─────────────────────────────────────────────────────────────────
INTERCOM_TOKEN = creds.get("INTERCOM_TOKEN")
ALEX_ADMIN_ID  = "7484673"   # Alex de Godoy
INTERCOM_APP_ID = "o0lp6qsb"

WEEK_LABEL = "W09 (Feb 24 – Mar 2)"
WEEK_START = date(2026, 2, 24)
WEEK_END   = date(2026, 3, 2)

INTERCOM_API = "https://api.intercom.io"

headers = {
    "Authorization":    f"Bearer {INTERCOM_TOKEN}",
    "Intercom-Version": "2.11",
    "Accept":           "application/json",
    "Content-Type":     "application/json",
}


def _to_unix(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def fetch_all_conversations() -> list:
    """Paginate through all W09 conversations."""
    url = f"{INTERCOM_API}/conversations/search"
    query = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "created_at", "operator": ">",  "value": _to_unix(WEEK_START)},
                {"field": "created_at", "operator": "<=", "value": _to_unix(WEEK_END)},
            ],
        },
        "pagination": {"per_page": 150},
    }

    all_convs = []
    page = 1
    cursor = None

    while True:
        if cursor:
            query["pagination"]["starting_after"] = cursor
        elif "starting_after" in query["pagination"]:
            del query["pagination"]["starting_after"]

        r = requests.post(url, headers=headers, json=query)
        r.raise_for_status()
        data = r.json()
        batch = data.get("conversations", [])
        all_convs.extend(batch)
        print(f"  Page {page}: {len(batch)} convs (total: {len(all_convs)})")

        pages      = data.get("pages", {})
        next_page  = pages.get("next", {})
        cursor     = next_page.get("starting_after") if isinstance(next_page, dict) else None
        if not cursor or len(batch) == 0:
            break
        page += 1

    return all_convs


def format_ts(ts) -> str:
    """Convert unix timestamp to short human-readable date like 'Feb 24'."""
    if not ts:
        return "?"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%b %-d")


def contact_name(conv) -> str:
    """Extract the first contact's display name from the conversation object."""
    try:
        contacts = conv.get("contacts", {}).get("contacts", [])
        if contacts:
            name = contacts[0].get("name") or ""
            if name.strip():
                return name.strip()
            cid = contacts[0].get("id", "")
            return f"(id={cid})"
    except Exception:
        pass
    return "(unknown)"


def main():
    print(f"Fetching Intercom conversations — {WEEK_LABEL} …\n")
    all_convs = fetch_all_conversations()
    print(f"\nTotal fetched: {len(all_convs)}")

    # Filter to conversations closed by Alex
    alex_convs = []
    for c in all_convs:
        stats  = c.get("statistics") or {}
        closer = str(stats.get("last_closed_by_id") or "")
        if closer == ALEX_ADMIN_ID:
            alex_convs.append(c)

    if not alex_convs:
        print("\nNo conversations attributed to Alex in this period.")
        return

    # Build rows
    rows = []
    for c in alex_convs:
        stats        = c.get("statistics") or {}
        conv_id      = c.get("id", "")
        created_ts   = c.get("created_at")
        reply_at     = stats.get("last_assignment_admin_reply_at")
        replied      = "YES" if reply_at else "NO"
        name         = contact_name(c)
        created_str  = format_ts(created_ts)
        link         = f"https://app.intercom.com/a/inbox/{INTERCOM_APP_ID}/inbox/conversation/{conv_id}"
        rows.append((name, created_str, replied, link))

    # Print table
    print(f"\nConversations closed by Alex de Godoy — {WEEK_LABEL}")
    print("─" * 90)
    print(f"{'#':>3}  {'Contact':<28} {'Created':>8}  {'Alex replied?':>14}  Link")
    print("─" * 90)
    for i, (name, created, replied, link) in enumerate(rows, 1):
        print(f"{i:>3}  {name:<28} {created:>8}  {replied:>14}  {link}")

    # Summary
    total      = len(rows)
    replied_n  = sum(1 for _, _, r, _ in rows if r == "YES")
    no_reply_n = total - replied_n

    print("─" * 90)
    print(f"\nSummary: {total} closed by Alex  |  {replied_n} replied  |  {no_reply_n} just closed (no reply from Alex)")
    print()
    if no_reply_n > 0:
        print(f"  ⚠  {no_reply_n} conversation(s) where Alex is credited but never replied — ")
        print(f"     Aya likely handled these. Share the list above with Aya to confirm.")
    else:
        print("  ✓  Alex replied in every conversation attributed to him.")


if __name__ == "__main__":
    main()
