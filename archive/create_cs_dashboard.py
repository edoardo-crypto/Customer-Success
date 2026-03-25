#!/usr/bin/env python3
"""
create_cs_dashboard.py

Creates a "📊 CS Dashboard" Notion page under "CS Operations Hub".
Builds the full 5-section skeleton via the Notion API, then prints
the step-by-step UI guide for adding linked views and charts.

Usage: python3 create_cs_dashboard.py
"""

import requests
import sys
import creds

# ── Config ─────────────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
NOTION_VERSION = "2022-06-28"
BASE_URL = "https://api.notion.com/v1"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

DASHBOARD_TITLE = "📊 CS Dashboard"
PARENT_TITLE = "CS Operations Hub"

SCORECARD_DB_ID = "311e418f-d8c4-810e-8b11-cdc50357e709"
MCT_DB_ID = "84feda19cfaf4c6e9500bf21d2aaafef"
ISSUES_DB_ID = "bd1ed48de20e426f8bebeb8e700d19d8"


# ── API helpers ────────────────────────────────────────────────────────────────

def search_pages(query):
    r = requests.post(f"{BASE_URL}/search", headers=HEADERS, json={
        "query": query,
        "filter": {"value": "page", "property": "object"},
    })
    r.raise_for_status()
    return r.json().get("results", [])


def get_children(block_id):
    r = requests.get(f"{BASE_URL}/blocks/{block_id}/children", headers=HEADERS)
    r.raise_for_status()
    return r.json().get("results", [])


def create_page(parent_id, title):
    r = requests.post(f"{BASE_URL}/pages", headers=HEADERS, json={
        "parent": {"page_id": parent_id},
        "properties": {
            "title": {"title": [{"type": "text", "text": {"content": title}}]}
        },
    })
    r.raise_for_status()
    return r.json()


def append_blocks(block_id, children):
    r = requests.patch(f"{BASE_URL}/blocks/{block_id}/children", headers=HEADERS,
                       json={"children": children})
    if not r.ok:
        print(f"  ERROR {r.status_code}: {r.text[:500]}", file=sys.stderr)
    r.raise_for_status()
    return r.json()


# ── Block builders ─────────────────────────────────────────────────────────────

def h2(t):
    return {"type": "heading_2", "heading_2": {
        "rich_text": [{"type": "text", "text": {"content": t}}]
    }}

def para(t, bold=False):
    ann = {"bold": True} if bold else {}
    return {"type": "paragraph", "paragraph": {
        "rich_text": [{"type": "text", "text": {"content": t}, "annotations": ann}]
    }}

def divider():
    return {"type": "divider", "divider": {}}

def callout(text, emoji="💡", color="gray_background"):
    return {"type": "callout", "callout": {
        "rich_text": [{"type": "text", "text": {"content": text}}],
        "icon": {"type": "emoji", "emoji": emoji},
        "color": color,
    }}

def toggle(title_text, children=None):
    return {"type": "toggle", "toggle": {
        "rich_text": [{"type": "text", "text": {"content": title_text}}],
        "children": children or [],
    }}

def columns(left_blocks, right_blocks):
    """2-column layout using the Notion column_list block type."""
    return {
        "type": "column_list",
        "column_list": {
            "children": [
                {"type": "column", "column": {"children": left_blocks}},
                {"type": "column", "column": {"children": right_blocks}},
            ]
        },
    }


# ── Section builders ───────────────────────────────────────────────────────────

def build_section_1():
    """This Week — 2-column callout banners for Alex and Aya."""
    left = [
        callout("Alex de Godoy", emoji="👤", color="blue_background"),
        para("📞 Contacted → see Scorecard DB"),
        para("🔴 Red Health → see Scorecard DB"),
        para("⏰ No Contact >21d → see Scorecard DB"),
        para("😢 Churned → see Scorecard DB"),
    ]
    right = [
        callout("Aya Guerimej", emoji="👤", color="purple_background"),
        para("📞 Contacted → see Scorecard DB"),
        para("🔴 Red Health → see Scorecard DB"),
        para("⏰ No Contact >21d → see Scorecard DB"),
        para("😢 Churned → see Scorecard DB"),
    ]
    return [
        h2("📅 This Week"),
        para("Live KPI snapshot for the current week. Numbers come from the Scorecard DB."),
        columns(left, right),
        divider(),
    ]


def build_section_2():
    """Weekly Trends — labelled callout placeholders (one per chart)."""
    chart_configs = [
        ("📊 Customers Contacted — Alex",
         "Bar chart · Source: Scorecard DB\nX-axis: Week Start (date)\nY-axis: Alex: Customers Contacted"),
        ("📊 Customers Contacted — Aya",
         "Bar chart · Source: Scorecard DB\nX-axis: Week Start (date)\nY-axis: Aya: Customers Contacted"),
        ("🔴 Red Health (Alex + Aya)",
         "Bar chart (side-by-side) · Source: Scorecard DB\nX-axis: Week Start\nY-axis: Alex: Red Health  +  Aya: Red Health"),
        ("⏰ No Contact >21d (Alex + Aya)",
         "Bar chart (side-by-side) · Source: Scorecard DB\nX-axis: Week Start\nY-axis: Alex: No Contact >21d  +  Aya: No Contact >21d"),
        ("📞 Median Reply Time (mins)",
         "Line chart · Source: Scorecard DB\nX-axis: Week Start\nY-axis: Alex: Median Reply Time  +  Aya: Median Reply Time"),
        ("😢 Churned (Alex + Aya)",
         "Bar chart (side-by-side) · Source: Scorecard DB\nX-axis: Week Start\nY-axis: Alex: Churned  +  Aya: Churned"),
    ]
    blocks = [
        h2("📈 Weekly Trends"),
        para("Trend charts over time. Add a Notion chart view of the Scorecard DB for each slot below, then delete the placeholder callout."),
    ]
    for title, config in chart_configs:
        blocks.append(callout(f"{title}\n→ ADD CHART HERE\n{config}", emoji="📉", color="yellow_background"))
    blocks.append(divider())
    return blocks


def build_section_3():
    """Portfolio Health — 4 toggle panels with MCT filter specs."""
    panels = [
        (
            "🔴 Red Health",
            "Linked view of Master Customer Table\n"
            "Filter: Health Status  contains  Red\n"
            "Columns to show: Company, CS Owner, Last Contact Date, Days Since Last Contact, MRR, Reason for contact\n"
            "Sort: Days Since Last Contact ↓ (descending)",
            "🔴", "red_background",
        ),
        (
            "⏰ No Contact >21 days",
            "Linked view of Master Customer Table\n"
            "Filter: 📞 Days Since Last Contact  >  21\n"
            "   AND  Billing Status  is  Active\n"
            "Columns to show: Company, CS Owner, Last Contact Date, Days Since Last Contact, Sentiment\n"
            "Sort: Days Since Last Contact ↓ (descending)",
            "⏰", "orange_background",
        ),
        (
            "⚠️ At Risk / Past Due",
            "Linked view of Master Customer Table\n"
            "Filter (OR): CS Sentiment  is  At Risk\n"
            "         OR  Billing Status  is  Past Due\n"
            "Columns to show: Company, CS Owner, Billing Status, MRR, Plan Tier, Renewal Date\n"
            "Sort: MRR ↓ (descending)",
            "⚠️", "yellow_background",
        ),
        (
            "🔄 Renewals < 30 days",
            "Linked view of Master Customer Table\n"
            "Filter: Days to Renewal  <  30\n"
            "   AND  Billing Status  is  Active\n"
            "Columns to show: Company, Renewal Date, Plan Tier, MRR, CS Owner\n"
            "Sort: Days to Renewal ↑ (ascending)",
            "🔄", "green_background",
        ),
    ]
    blocks = [
        h2("🏥 Portfolio Health"),
        para("Filtered views of the Master Customer Table. Click a toggle to expand, then add a linked view inside it."),
    ]
    for title, spec, emoji, color in panels:
        inner = [callout(spec, emoji=emoji, color=color)]
        blocks.append(toggle(title, children=inner))
    blocks.append(divider())
    return blocks


def build_section_4():
    """Open Issues — 2 toggle panels with Issues DB filter specs."""
    panels = [
        (
            "🐛 All Open Issues",
            "Linked view of Issues Table\n"
            "View type: Board\n"
            "Filter: Status  is not  Resolved\n"
            "Group by: Severity  (order: Urgent → Important → Not Urgent)\n"
            "Columns to show: Title, Customer, Category, Created At",
            "🐛", "orange_background",
        ),
        (
            "🔧 Needs Engineering",
            "Linked view of Issues Table\n"
            "View type: Table\n"
            "Filter: Engineering Required  is checked\n"
            "   AND  Status  is not  Resolved\n"
            "Columns to show: Title, Customer, Status, Severity, Linear Ticket URL",
            "🔧", "blue_background",
        ),
    ]
    blocks = [
        h2("🐛 Open Issues"),
        para("Live views of the Issues Table. Click a toggle to expand, then add a linked view inside it."),
    ]
    for title, spec, emoji, color in panels:
        inner = [callout(spec, emoji=emoji, color=color)]
        blocks.append(toggle(title, children=inner))
    blocks.append(divider())
    return blocks


def build_section_5():
    """Portfolio Overview — 3 chart placeholders from the MCT."""
    charts = [
        ("👥 Customers by CS Owner",
         "Donut chart · Source: Master Customer Table\nGroup by: CS Owner\nFilter: Billing Status  is  Active"),
        ("💚 Customers by Health Status",
         "Donut chart · Source: Master Customer Table\nGroup by: Health Status\nFilter: Billing Status  is  Active"),
        ("📦 Customers by Plan Tier",
         "Bar chart · Source: Master Customer Table\nGroup by: Plan Tier\nFilter: Billing Status  is  Active"),
    ]
    blocks = [
        h2("🌐 Portfolio Overview"),
        para("Summary charts showing portfolio composition. Add a Notion chart view of the MCT for each slot, then delete the placeholder callout."),
    ]
    for title, config in charts:
        blocks.append(callout(f"{title}\n→ ADD CHART HERE\n{config}", emoji="📊", color="green_background"))
    blocks.append(divider())
    return blocks


# ── UI Guide ───────────────────────────────────────────────────────────────────

def print_ui_guide(page_url):
    print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║            📋  Notion UI Guide — Finish Your CS Dashboard                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

Dashboard URL: {page_url}

The script created the full page skeleton. Follow the steps below to add
live database panels and charts (~15 min total).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 1 — "📅 This Week"  (no action needed)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The two callout banners already exist. You can optionally replace the
"→ see Scorecard DB" text with hyperlinks to specific Scorecard rows.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 2 — "📈 Weekly Trends"  (6 charts to add)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For each yellow callout slot:
  1. Click just ABOVE the callout → type /chart or click + → Chart
  2. Select "Scorecard DB" as the data source
  3. Configure per the callout text (type, X-axis, Y-axis)
  4. For side-by-side charts: after setting the first Y, click "+ Add series"
  5. Delete the yellow callout when done

Charts:
  ① Customers Contacted — Alex
     Bar | X: Week Start | Y: Alex: Customers Contacted

  ② Customers Contacted — Aya
     Bar | X: Week Start | Y: Aya: Customers Contacted

  ③ Red Health (Alex + Aya)
     Bar | X: Week Start | Y: Alex: Red Health → + Add series → Aya: Red Health

  ④ No Contact >21d
     Bar | X: Week Start | Y: Alex: No Contact >21d → + Add series → Aya: No Contact >21d

  ⑤ Median Reply Time
     Line | X: Week Start | Y: Alex: Median Reply Time → + Add series → Aya: Median Reply Time

  ⑥ Churned
     Bar | X: Week Start | Y: Alex: Churned → + Add series → Aya: Churned

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 3 — "🏥 Portfolio Health"  (4 linked MCT views)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For each toggle:
  1. Click the arrow to expand the toggle
  2. Click just ABOVE the callout → type /linked or click + → "Linked view of database"
  3. Select "Master Customer Table" as the source
  4. Apply filters, columns, and sort shown in the callout
  5. Delete the callout when done

Panel configs:
  🔴 Red Health
     Filter: Health Status contains Red
     Sort: Days Since Last Contact ↓
     Show: Company, CS Owner, Last Contact Date, Days Since Last Contact, MRR, Reason for contact

  ⏰ No Contact >21 days
     Filter: Days Since Last Contact > 21  AND  Billing Status is Active
     Sort: Days Since Last Contact ↓
     Show: Company, CS Owner, Last Contact Date, Days Since Last Contact, Sentiment

  ⚠️ At Risk / Past Due
     Filter: CS Sentiment is At Risk  (OR)  Billing Status is Past Due
     Sort: MRR ↓
     Show: Company, CS Owner, Billing Status, MRR, Plan Tier, Renewal Date

  🔄 Renewals < 30 days
     Filter: Days to Renewal < 30  AND  Billing Status is Active
     Sort: Days to Renewal ↑
     Show: Company, Renewal Date, Plan Tier, MRR, CS Owner

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 4 — "🐛 Open Issues"  (2 linked Issues views)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Same process as Section 3, but select "Issues Table" as the source.

  🐛 All Open Issues
     View type: Board
     Filter: Status is not Resolved
     Group by: Severity  (Urgent → Important → Not Urgent)
     Show: Title, Customer, Category, Created At

  🔧 Needs Engineering
     View type: Table
     Filter: Engineering Required is checked  AND  Status is not Resolved
     Show: Title, Customer, Status, Severity, Linear Ticket URL

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SECTION 5 — "🌐 Portfolio Overview"  (3 charts)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Same process as Section 2, but select "Master Customer Table" as source.

  ① Customers by CS Owner
     Donut | Group by: CS Owner | Filter: Billing Status is Active

  ② Customers by Health Status
     Donut | Group by: Health Status | Filter: Billing Status is Active

  ③ Customers by Plan Tier
     Bar | Group by: Plan Tier | Filter: Billing Status is Active

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ All done! The dashboard is fully live once all views + charts are added.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")


# ── Main ───────────────────────────────────────────────────────────────────────

def find_parent_page():
    """Find the CS Operations Hub page via Notion search."""
    print(f"  Searching for '{PARENT_TITLE}'...")
    results = search_pages(PARENT_TITLE)

    for page in results:
        props = page.get("properties", {})
        for _, v in props.items():
            if v.get("type") == "title":
                title_text = "".join(t.get("plain_text", "") for t in v.get("title", []))
                if PARENT_TITLE.lower() in title_text.lower():
                    return page

    # If not found via properties, try child_page title (for subpages)
    for page in results:
        if page.get("type") == "child_page":
            if PARENT_TITLE.lower() in page.get("child_page", {}).get("title", "").lower():
                return page

    return None


def find_existing_dashboard(parent_id):
    """Check if the dashboard page already exists as a child."""
    try:
        children = get_children(parent_id)
    except Exception:
        return None

    for child in children:
        if child.get("type") == "child_page":
            if child.get("child_page", {}).get("title", "") == DASHBOARD_TITLE:
                return child
    return None


def main():
    # 1. Find parent page
    parent = find_parent_page()
    if not parent:
        print(f"✗ Could not find '{PARENT_TITLE}' in Notion.", file=sys.stderr)
        print("  Make sure the Notion integration has access to that page.", file=sys.stderr)
        sys.exit(1)

    parent_id = parent["id"]
    print(f"  ✓ Parent page found: {parent_id}")

    # 2. Idempotency check
    print(f"  Checking if '{DASHBOARD_TITLE}' already exists...")
    existing = find_existing_dashboard(parent_id)
    if existing:
        page_id = existing["id"]
        page_url = f"https://www.notion.so/{page_id.replace('-', '')}"
        print(f"  ⚠️  '{DASHBOARD_TITLE}' already exists: {page_url}")
        print("  Delete the existing page first, then re-run this script to recreate it.")
        print_ui_guide(page_url)
        return

    # 3. Create the dashboard page
    print(f"  Creating '{DASHBOARD_TITLE}'...")
    page = create_page(parent_id, DASHBOARD_TITLE)
    page_id = page["id"]
    page_url = page.get("url", f"https://www.notion.so/{page_id.replace('-', '')}")
    print(f"  ✓ Page created: {page_url}")

    # 4. Build and append all 5 sections
    sections = [
        ("Section 1: This Week",         build_section_1),
        ("Section 2: Weekly Trends",     build_section_2),
        ("Section 3: Portfolio Health",  build_section_3),
        ("Section 4: Open Issues",       build_section_4),
        ("Section 5: Portfolio Overview",build_section_5),
    ]

    for name, builder in sections:
        blocks = builder()
        print(f"  Appending {name} ({len(blocks)} block(s))...")
        # Send up to 20 blocks per request (conservative; API allows 100)
        for i in range(0, len(blocks), 20):
            chunk = blocks[i:i + 20]
            append_blocks(page_id, chunk)
        print(f"  ✓ {name} done")

    print(f"\n✅ Dashboard page created successfully!")
    print(f"   URL: {page_url}")
    print_ui_guide(page_url)


if __name__ == "__main__":
    main()
