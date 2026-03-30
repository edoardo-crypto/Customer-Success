#!/usr/bin/env python3
"""
generate_open_bugs.py — Generate the Open Bugs dashboard HTML from open_bugs_data.json.

Bugs are grouped into subsections within each category quadrant.
Clicking a subsection expands to show individual bugs.
"""

import json
import os
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "open_bugs_data.json")
OUT_DIR = os.path.join(SCRIPT_DIR, "site")
OUT_FILE = os.path.join(OUT_DIR, "index.html")

CATEGORIES = [
    {"key": "AI Agent",           "color": "#F97316", "bg": "#FFF7ED"},
    {"key": "Inbox",              "color": "#3B82F6", "bg": "#EFF6FF"},
    {"key": "WhatsApp Marketing", "color": "#22C55E", "bg": "#F0FDF4"},
    {"key": "Integration",        "color": "#A78BFA", "bg": "#F5F3FF"},
]

# Subsections per category: (label, [keywords to match in title, lowercased])
SUBSECTIONS = {
    "AI Agent": [
        ("Product recommendations", ["wrong product", "wrong variation", "recommend", "product rec",
            "incorrect product", "draft product", "unavailable", "stock", "out-of-stock",
            "metafield", "product group", "product sync"]),
        ("Order management", ["order status", "order management", "find order", "retrieve order",
            "order number"]),
        ("Handover & transfers", ["handover", "handoff", "transfer", "escalat"]),
        ("Language & formatting", ["language", "spanish", "english", "translat", "formatting",
            "/n/n", "switched language"]),
        ("OTP & verification", ["otp", "verification", "verify", "code not"]),
        ("AI not responding", ["ai not working", "ai was turned off", "not working despite",
            "emails stuck", "emails abandoned", "automated queue", "without ai"]),
        ("AI response quality", ["custom answer", "why this reply", "sources not working",
            "identify itself", "misinterpreted", "falsely promises", "incorrect report"]),
    ],
    "Inbox": [
        ("Speed & performance", ["slow", "speed", "performance", "not loading", "loading",
            "not smooth", "not immediate", "without refresh"]),
        ("Messages & conversations", ["message", "conversation", "duplicate", "missing",
            "expired", "expiring", "snooze", "reappear", "open simultaneously", "side conversation",
            "assignment not moving", "sent 2 times", "twice", "contact response", "closed",
            "opening conversation"]),
        ("Notifications", ["notification", "red dot"]),
        ("Other UI glitches", ["search bar", "snooze button", "visual bug", "name of the user",
            "feed is different", "command", "audio transcri"]),
    ],
    "WhatsApp Marketing": [
        ("Broadcasts not sending", ["broadcast", "campaign", "variable", "csv", "pending template",
            "media file", "cost not showing", "programmed"]),
        ("Flows stopping / misfiring", ["flow", "trigger", "infinite loop", "mid-execution",
            "wrong phone", "wrong user", "multiple times despite", "auto-switching",
            "without client confirmation", "closing conversation", "post purchase"]),
    ],
    "Integration": [
        ("Helpdesk", ["gorgias", "zendesk", "helpdesk", "help desk"]),
        ("Shopify / product sync", ["shopify", "product sync", "product image", "woocommerce",
            "order sync", "orders not getting synced"]),
        ("Channels (Email, Instagram, WhatsApp)", ["outlook", "microsoft", "email integr",
            "instagram", "telegram", "whatsapp audio", "whatsapp message",
            "not receiving message", "live chat", "channel"]),
        ("CRM data sync", ["klaviyo", "crm", "sync", "synching", "customer profile",
            "customer order"]),
    ],
}

SEVERITY_DOTS = {
    "Urgent":        "#EF4444",
    "Important":     "#F59E0B",
    "Not important": "#94A3B8",
}

STATUS_PILLS = {
    "Open":        {"bg": "#FEE2E2", "text": "#991B1B"},
    "In Progress": {"bg": "#FEF3C7", "text": "#92400E"},
}


def days_ago(date_str):
    if not date_str:
        return "?"
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - dt
        d = delta.days
        if d == 0:
            return "today"
        if d == 1:
            return "1d ago"
        return f"{d}d ago"
    except Exception:
        return "?"


def escape(s):
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def clean_title(raw_title):
    """Strip customer/contact name prefix from title."""
    for sep in [" - ", " — ", ": "]:
        if sep in raw_title:
            return raw_title.split(sep, 1)[1]
    return raw_title


def classify_subsection(bug, category):
    """Match a bug to a subsection by keyword. Returns subsection label or None."""
    title_lower = bug.get("title", "").lower()
    summary_lower = bug.get("summary", "").lower()
    text = title_lower + " " + summary_lower

    for label, keywords in SUBSECTIONS.get(category, []):
        for kw in keywords:
            if kw in text:
                return label
    return None


def render_bug_row(bug):
    sev = bug.get("severity", "")
    dot_color = SEVERITY_DOTS.get(sev, "#94A3B8")
    status = bug.get("status", "Open")
    pill = STATUS_PILLS.get(status, STATUS_PILLS["Open"])
    customer = escape(bug.get("customer", ""))
    title = escape(clean_title(bug.get("title", "")))
    age = days_ago(bug.get("created_at", ""))
    notion_url = bug.get("notion_url", "#")

    return f"""<a href="{notion_url}" target="_blank" class="bug-row">
            <span class="sev-dot" style="background:{dot_color}" title="{sev}"></span>
            <span class="bug-customer">{customer}</span>
            <span class="bug-title">{title}</span>
            <span class="bug-age">{age}</span>
          </a>"""


def render_quadrant(cat_info, bugs, expand_all=False):
    key = cat_info["key"]
    color = cat_info["color"]
    bg = cat_info["bg"]
    cat_bugs = [b for b in bugs if b.get("category") == key]
    count = len(cat_bugs)
    urgent_count = sum(1 for b in cat_bugs if b.get("severity") == "Urgent")
    open_attr = " open" if expand_all else ""

    # Group into subsections
    grouped = {}
    ungrouped = []
    for b in cat_bugs:
        sub = classify_subsection(b, key)
        if sub:
            grouped.setdefault(sub, []).append(b)
        else:
            ungrouped.append(b)

    # Render subsections in defined order
    sections_html = ""
    for label, _ in SUBSECTIONS.get(key, []):
        sub_bugs = grouped.get(label, [])
        if not sub_bugs:
            continue
        rows = "\n".join(render_bug_row(b) for b in sub_bugs)
        sub_count = len(sub_bugs)
        sections_html += f"""
        <details class="subsection"{open_attr}>
          <summary class="sub-header">
            <span class="sub-name">{label}</span>
            <span class="sub-count" style="color:{color}">{sub_count}</span>
          </summary>
          <div class="sub-bugs">{rows}</div>
        </details>"""

    # Ungrouped bugs
    if ungrouped:
        rows = "\n".join(render_bug_row(b) for b in ungrouped)
        sections_html += f"""
        <details class="subsection"{open_attr}>
          <summary class="sub-header">
            <span class="sub-name">Other</span>
            <span class="sub-count" style="color:{color}">{len(ungrouped)}</span>
          </summary>
          <div class="sub-bugs">{rows}</div>
        </details>"""

    if not sections_html:
        sections_html = '<div class="empty-state">No open bugs</div>'

    return f"""
    <div class="quadrant" style="border-top: 4px solid {color};">
      <div class="quad-header" style="background:{bg};">
        <span class="quad-name" style="color:{color}">{key}</span>
        <span class="quad-badges">
          <span class="quad-count urgent-count" style="background:{color}30;color:#EF4444">{urgent_count}</span>
          <span class="quad-count" style="background:{color}">{count}</span>
        </span>
      </div>
      <div class="quad-body">
        {sections_html}
      </div>
    </div>"""


def build_page(bugs, title, total_badge_color, updated_str, expand_all=False):
    """Build the full HTML page for a set of bugs."""
    total = len(bugs)
    quadrants_html = "\n".join(render_quadrant(c, bugs, expand_all=expand_all) for c in CATEGORIES)

    categorized_keys = {c["key"] for c in CATEGORIES}
    uncategorized = [b for b in bugs if b.get("category") not in categorized_keys]
    uncat_section = ""
    if uncategorized:
        rows = "\n".join(render_bug_row(b) for b in uncategorized)
        uncat_section = f"""
    <div class="uncategorized">
      <div class="quad-header" style="background:#F8FAFC;">
        <span class="quad-name" style="color:#64748B">Uncategorized</span>
        <span class="quad-count" style="background:#64748B">{len(uncategorized)}</span>
      </div>
      <div class="quad-body">
        <details class="subsection" open>
          <summary class="sub-header"><span class="sub-name">Needs classification</span><span class="sub-count" style="color:#64748B">{len(uncategorized)}</span></summary>
          <div class="sub-bugs">{rows}</div>
        </details>
      </div>
    </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="300">
<title>{title} — Konvo AI</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: 'Inter', -apple-system, sans-serif;
    background: #F8FAFC;
    color: #1E293B;
    min-height: 100vh;
  }}

  .header {{
    background: #fff;
    border-bottom: 1px solid #E2E8F0;
    padding: 20px 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    z-index: 10;
  }}
  .header-left {{
    display: flex;
    align-items: center;
    gap: 14px;
  }}
  .header h1 {{
    font-size: 22px;
    font-weight: 700;
    color: #0F172A;
  }}
  .total-badge {{
    background: #EF4444;
    color: #fff;
    font-size: 14px;
    font-weight: 700;
    padding: 3px 10px;
    border-radius: 20px;
  }}
  .header-right {{
    font-size: 13px;
    color: #94A3B8;
  }}

  .legend {{
    display: flex;
    gap: 16px;
    padding: 8px 32px 0;
    font-size: 12px;
    color: #94A3B8;
  }}
  .legend-item {{
    display: flex;
    align-items: center;
    gap: 5px;
  }}
  .legend-dot {{
    width: 8px;
    height: 8px;
    border-radius: 50%;
  }}

  .grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    padding: 16px 32px 24px;
    max-width: 1600px;
    margin: 0 auto;
  }}
  @media (max-width: 900px) {{
    .grid {{ grid-template-columns: 1fr; padding: 16px; }}
  }}

  .quadrant {{
    background: #fff;
    border-radius: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    overflow: hidden;
    display: flex;
    flex-direction: column;
    max-height: calc(50vh - 50px);
  }}

  .quad-header {{
    padding: 14px 18px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid #F1F5F9;
    flex-shrink: 0;
  }}
  .quad-name {{
    font-size: 15px;
    font-weight: 700;
    letter-spacing: 0.02em;
  }}
  .quad-badges {{
    display: flex;
    align-items: center;
    gap: 6px;
  }}
  .quad-count {{
    color: #fff;
    font-size: 13px;
    font-weight: 700;
    padding: 2px 9px;
    border-radius: 12px;
    min-width: 28px;
    text-align: center;
  }}
  .urgent-count {{
    color: #EF4444 !important;
  }}

  .quad-body {{
    overflow-y: auto;
    flex: 1;
  }}

  /* ── Subsections (collapsible) ── */
  .subsection {{
    border-bottom: 1px solid #F1F5F9;
  }}
  .subsection:last-child {{
    border-bottom: none;
  }}
  .sub-header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 18px;
    cursor: pointer;
    user-select: none;
    list-style: none;
    transition: background 0.1s;
  }}
  .sub-header:hover {{
    background: #F8FAFC;
  }}
  .sub-header::-webkit-details-marker {{
    display: none;
  }}
  .sub-name {{
    font-size: 13px;
    font-weight: 600;
    color: #334155;
  }}
  .sub-name::before {{
    content: '\\25B8';
    display: inline-block;
    margin-right: 8px;
    font-size: 11px;
    color: #94A3B8;
    transition: transform 0.15s;
  }}
  details[open] > .sub-header .sub-name::before {{
    transform: rotate(90deg);
  }}
  .sub-count {{
    font-size: 13px;
    font-weight: 700;
  }}

  .sub-bugs {{
    background: #FAFBFC;
    border-top: 1px solid #F1F5F9;
  }}

  /* ── Bug rows ── */
  .bug-row {{
    display: grid;
    grid-template-columns: 10px 1fr 2.5fr auto;
    align-items: center;
    gap: 10px;
    padding: 8px 18px 8px 30px;
    text-decoration: none;
    color: inherit;
    transition: background 0.1s;
  }}
  .bug-row:hover {{
    background: #F1F5F9;
  }}

  .sev-dot {{
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
  }}
  .bug-customer {{
    font-size: 12px;
    font-weight: 600;
    color: #475569;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}
  .bug-title {{
    font-size: 12px;
    color: #64748B;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }}
  .bug-age {{
    font-size: 11px;
    color: #94A3B8;
    white-space: nowrap;
  }}

  .empty-state {{
    padding: 32px;
    text-align: center;
    color: #CBD5E1;
    font-size: 14px;
  }}

  .uncategorized {{
    margin: 0 32px 24px;
    background: #fff;
    border-radius: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    overflow: hidden;
    border-top: 4px solid #94A3B8;
  }}

  .footer {{
    text-align: center;
    padding: 16px;
    font-size: 12px;
    color: #CBD5E1;
  }}
</style>
</head>
<body>

<div class="header">
  <div class="header-left">
    <h1>{title}</h1>
    <span class="total-badge" style="background:{total_badge_color}">{total}</span>
  </div>
  <div class="header-right">Last updated: {updated_str}</div>
</div>

<div class="legend">
  <div class="legend-item"><span class="legend-dot" style="background:#EF4444"></span> Urgent</div>
  <div class="legend-item"><span class="legend-dot" style="background:#F59E0B"></span> Important</div>
  <div class="legend-item"><span class="legend-dot" style="background:#94A3B8"></span> Not important</div>
</div>

<div class="grid">
{quadrants_html}
</div>
{uncat_section}

<div class="footer">Auto-updated every 2h during business hours</div>

</body>
</html>"""

    return html


def generate():
    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)

    all_bugs = data.get("bugs", [])
    generated_at = data.get("generated_at", "")

    try:
        ts = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        updated_str = ts.strftime("%b %d, %Y at %H:%M UTC")
    except Exception:
        updated_str = generated_at

    # 1. All open bugs (collapsed)
    os.makedirs(OUT_DIR, exist_ok=True)
    html_all = build_page(all_bugs, "Open Bugs", "#EF4444", updated_str, expand_all=False)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        f.write(html_all)
    print(f"Generated {OUT_FILE} — {len(all_bugs)} bugs")

    # 2. Urgent only (expanded)
    urgent_bugs = [b for b in all_bugs if b.get("severity") == "Urgent"]
    urgent_dir = os.path.join(OUT_DIR, "urgent")
    os.makedirs(urgent_dir, exist_ok=True)
    html_urgent = build_page(urgent_bugs, "Open Bugs — Urgent", "#DC2626", updated_str, expand_all=True)
    urgent_file = os.path.join(urgent_dir, "index.html")
    with open(urgent_file, "w", encoding="utf-8") as f:
        f.write(html_urgent)
    print(f"Generated {urgent_file} — {len(urgent_bugs)} urgent bugs")


if __name__ == "__main__":
    generate()
