#!/usr/bin/env python3
"""
sync_engaging_cs.py — Fill / fix the "Engaging with CS" select field in the MCT.

Rules (from dry-run analysis):
  - 0 convs                          → "No"
  - >=6 convs                        → "Yes"
  - 1-5 convs + label "Engaging"    → "Yes"
  - 1-5 convs + label "Not Engaging"→ "No"
  - No data / domain mismatch        → skip (None)

Actions per MCT row:
  - current empty AND computed not None        → UPDATE to computed value
  - current "No"  AND computed "Yes"
    AND company in NO_TO_YES_LIST              → UPDATE to "Yes"
  - current "Yes" AND computed "No"            → SKIP (trust MCT)
  - computed None                              → SKIP (report)
  - current already equals computed            → no-op

Run:  python3 sync_engaging_cs.py
"""

import csv
import os
import time

import requests
import creds

# ── Credentials ────────────────────────────────────────────────────────────────
NOTION_TOKEN = os.environ.get(
    "NOTION_TOKEN", creds.get("NOTION_TOKEN")
)
NOTION_API = "https://api.notion.com/v1"
MCT_DS_ID  = "3ceb1ad0-91f1-40db-945a-c51c58035898"

HEADERS_NEW = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2025-09-03",
}

# ── Decision constants ─────────────────────────────────────────────────────────
# Companies where MCT says "No" but computed says "Yes" → trust computed, update
NO_TO_YES_LIST = {
    "aprilplants",
    "complements azulito barcelona slu",
    "femmeup",
    "old school spain",
    "pilar martín",
    "pilar martin",
}

# Companies to skip entirely and report
SKIP_REPORT_LIST = {
    "b-ethic sl",
    "funda hogar",
    "health nutrition lab",
    "henchman",
    "international cosmetic science s.l.",
    "legend lifestyle s.l.",
    "love digital factory s.l.",
    "otso sport",
    "platanomelón",
    "platanomelon",
    "pott candles",
    "weritwerit sl",
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# ── Step 1: Build computed engagement map (domain → "Yes" | "No" | None) ──────
def build_computed_map():
    """Returns dict: domain (lowercase) → "Yes" | "No" | None"""
    # Load main engagement file (all customers)
    eng = {}  # domain → total_conversations
    with open(os.path.join(SCRIPT_DIR, "intercom_engagement.csv"), newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            domain = row["domain"].strip().lower()
            try:
                convs = int(row["conversations_since_dec_2025"])
            except (ValueError, KeyError):
                convs = 0
            eng[domain] = convs

    # Load low-engagement deep analysis (1-5 conv customers)
    low = {}  # domain → engagement_label
    with open(os.path.join(SCRIPT_DIR, "intercom_low_engagement_analysis.csv"), newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            domain = row["domain"].strip().lower()
            label = row.get("engagement_label", "").strip()
            low[domain] = label

    computed = {}
    all_domains = set(eng.keys()) | set(low.keys())

    for domain in all_domains:
        convs = eng.get(domain, 0)
        label = low.get(domain, "")

        if convs == 0:
            computed[domain] = "No"
        elif convs >= 6:
            computed[domain] = "Yes"
        elif 1 <= convs <= 5:
            if label == "Engaging":
                computed[domain] = "Yes"
            elif label in ("Not Engaging", "No Data"):
                computed[domain] = "No" if label == "Not Engaging" else None
            else:
                computed[domain] = None  # unknown label
        else:
            computed[domain] = None

    return computed


# ── Step 2: Fetch all MCT rows ─────────────────────────────────────────────────
def fetch_mct_rows():
    rows = []
    cursor = None
    page_num = 0
    while True:
        page_num += 1
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        resp = requests.post(
            f"{NOTION_API}/data_sources/{MCT_DS_ID}/query",
            headers=HEADERS_NEW,
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        for page in data.get("results", []):
            props = page.get("properties", {})

            # Company name (title)
            title_items = props.get("🏢 Company Name", {}).get("title", [])
            company = "".join(t.get("plain_text", "") for t in title_items).strip()

            # Domain
            domain_items = props.get("🏢 Domain", {}).get("rich_text", [])
            domain = "".join(d.get("plain_text", "") for d in domain_items).strip().lower()

            # Current "Engaging with CS" value
            engage_prop = props.get("Engaging with CS", {})
            select_obj = engage_prop.get("select")
            current = select_obj.get("name") if select_obj else None  # "Yes", "No", or None

            rows.append({
                "page_id": page["id"],
                "company": company,
                "domain": domain,
                "current": current,
            })

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return rows


# ── Step 3+4: Decide and execute PATCHes ──────────────────────────────────────
def patch_engaging(page_id, value, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.patch(
                f"{NOTION_API}/pages/{page_id}",
                headers=HEADERS_NEW,
                json={"properties": {"Engaging with CS": {"select": {"name": value}}}},
                timeout=30,
            )
            resp.raise_for_status()
            return
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # 1s, 2s backoff
            else:
                raise


def main():
    print("Building computed engagement map...")
    computed_map = build_computed_map()
    print(f"  {len(computed_map)} domains in computed map")

    print("Fetching MCT rows...")
    rows = fetch_mct_rows()
    print(f"  {len(rows)} MCT rows fetched")

    updated = 0
    already_correct = 0
    skipped_trust_mct = []
    skipped_no_data = []
    errors = []

    for row in rows:
        company_lc = row["company"].lower()
        domain = row["domain"]
        current = row["current"]
        page_id = row["page_id"]

        # Check if in skip/report list
        if company_lc in SKIP_REPORT_LIST:
            skipped_no_data.append(row["company"])
            continue

        # Look up computed value by domain
        computed = computed_map.get(domain)

        # If no domain match, try to see if domain is empty → skip
        if computed is None:
            if domain:
                skipped_no_data.append(row["company"])
            # no domain → silently skip (can't determine)
            continue

        # Decide action
        if current is None:
            # Empty cell → fill with computed
            action = computed
        elif current == "No" and computed == "Yes":
            # Mismatch: only update if company is in approved list
            if company_lc in NO_TO_YES_LIST:
                action = "Yes"
            else:
                # Not in approved list but computed says Yes — unexpected, skip safely
                action = None
        elif current == "Yes" and computed == "No":
            # Trust MCT
            skipped_trust_mct.append(row["company"])
            continue
        elif current == computed:
            already_correct += 1
            continue
        else:
            action = computed  # e.g. current="No" computed="No" already handled above

        if action is None:
            continue

        # Execute PATCH
        try:
            patch_engaging(page_id, action)
            print(f"  [UPDATE] {row['company']} ({domain}): {current!r} → {action!r}")
            updated += 1
            time.sleep(0.15)
        except requests.HTTPError as e:
            print(f"  [ERROR] {row['company']}: {e}")
            errors.append(row["company"])

    # ── Step 5: Summary ─────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print(f"SUMMARY")
    print(f"  Updated:           {updated} rows")
    print(f"  Already correct:   {already_correct} rows")
    print(f"  Skipped (trust MCT, Yes→No): {len(skipped_trust_mct)}")
    for c in skipped_trust_mct:
        print(f"    - {c}")
    print(f"  Could not fill (No Data / domain mismatch): {len(skipped_no_data)}")
    for c in sorted(set(skipped_no_data)):
        print(f"    - {c}")
    if errors:
        print(f"  Errors: {len(errors)}")
        for c in errors:
            print(f"    - {c}")
    print("=" * 60)


if __name__ == "__main__":
    main()
