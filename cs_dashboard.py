#!/usr/bin/env python3
"""
cs_dashboard.py — Generates cs_dashboard.html with live KPI data.

Sources:
  - Weekly CS Scorecards DB  → This Week KPIs + trend charts
  - Master Customer Table    → Churning pipeline + Red Health list

Run:  python3 cs_dashboard.py
Then: open cs_dashboard.html   (or reload if already open)
"""

import json
import os
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
SCORECARD_DB = "311e418f-d8c4-810e-8b11-cdc50357e709"
MCT_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"

HDR_V1 = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
HDR_V2 = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}

OUTPUT = Path(__file__).parent / "cs_dashboard.html"


# ── Fetch: Scorecard ───────────────────────────────────────────────────────────

def query_scorecard():
    rows, cursor = [], None
    while True:
        payload = {
            "sorts": [{"property": "Week Start", "direction": "descending"}],
            "page_size": 100,
        }
        if cursor:
            payload["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{SCORECARD_DB}/query",
            headers=HDR_V1, json=payload,
        )
        r.raise_for_status()
        data = r.json()
        rows.extend(data["results"])
        if not data.get("has_more"):
            break
        cursor = data["next_cursor"]
    return rows


def extract_scorecard(page):
    props = page["properties"]

    def num(key):
        return props.get(key, {}).get("number")

    def date_val(key):
        d = props.get(key, {}).get("date")
        return d["start"] if d and d.get("start") else None

    def title_val(key):
        t = props.get(key, {}).get("title", [])
        return t[0]["plain_text"] if t else ""

    return {
        "week":           title_val("Week"),
        "week_start":     date_val("Week Start"),
        "alex_contacted": num("Alex: Customers Contacted"),
        "alex_graduated": num("Alex: Graduated"),
        "alex_red":       num("Alex: Red Health"),
        "alex_nocontact": num("Alex: No Contact >21d"),
        "alex_churned":   num("Alex: Churned"),
        "aya_contacted":  num("Aya: Customers Contacted"),
        "aya_graduated":  num("Aya: Graduated"),
        "aya_red":        num("Aya: Red Health"),
        "aya_nocontact":  num("Aya: No Contact >21d"),
        "aya_churned":    num("Aya: Churned"),
    }


# ── Fetch: MCT ────────────────────────────────────────────────────────────────

def query_mct(filter_payload):
    """Query the Master Customer Table via data_sources endpoint."""
    rows, cursor = [], None
    while True:
        payload = {**filter_payload, "page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query",
            headers=HDR_V2, json=payload,
        )
        r.raise_for_status()
        data = r.json()
        rows.extend(data["results"])
        if not data.get("has_more"):
            break
        cursor = data["next_cursor"]
    return rows


def extract_mct(page):
    props = page["properties"]

    def title_val(key):
        t = props.get(key, {}).get("title", [])
        return "".join(b.get("plain_text", "") for b in t)

    def sel(key):
        s = props.get(key, {}).get("select")
        return s["name"] if s else None

    def rich_text(key):
        rt = props.get(key, {}).get("rich_text", [])
        return "".join(b.get("plain_text", "") for b in rt).strip()

    def num(key):
        return props.get(key, {}).get("number")

    def date_val(key):
        d = props.get(key, {}).get("date")
        return d["start"] if d and d.get("start") else None

    return {
        "name":          title_val("🏢 Company Name"),
        "owner":         sel("⭐ CS Owner"),
        "mrr":           num("💰 MRR"),
        "billing":       sel("💰 Billing Status"),
        "focus":         sel("🎯 Customer Focus"),
        "churn_cat":       sel("🔁 Churn Reason"),         # select: category
        "churn_note":      rich_text("😢 Churn Reason"),  # free text: CS explanation
        "cancel_date":     date_val("📅 Cancel Date"),
        "sentiment":       sel("🧠 CS Sentiment"),
        "at_risk_reason":  rich_text("🔴 At Risk Reason"), # NEW: free text why at risk
    }


def fetch_churning():
    print("  Fetching churning customers...")
    pages = query_mct({
        "filter": {"property": "💰 Billing Status", "select": {"equals": "Churning"}},
        "sorts":  [{"property": "📅 Cancel Date", "direction": "ascending"}],
    })
    return [extract_mct(p) for p in pages]


def fetch_red_health():
    print("  Fetching at-risk customers...")
    pages = query_mct({
        "filter": {"property": "🧠 CS Sentiment", "select": {"equals": "At Risk"}},
        "sorts":  [{"property": "🏢 Company Name", "direction": "ascending"}],
    })
    return [extract_mct(p) for p in pages]


def fetch_arr_by_focus():
    """Fetch all Active + Churning customers, group MRR by 🎯 Customer Focus."""
    print("  Fetching MRR by customer focus...")
    pages = query_mct({
        "filter": {
            "or": [
                {"property": "💰 Billing Status", "select": {"equals": "Active"}},
                {"property": "💰 Billing Status", "select": {"equals": "Churning"}},
            ]
        },
    })
    buckets = {"AI for CS": 0, "WhatsApp Marketing": 0, "Both": 0, "Untagged": 0}
    counts  = {"AI for CS": 0, "WhatsApp Marketing": 0, "Both": 0, "Untagged": 0}
    for page in pages:
        c = extract_mct(page)
        key = c["focus"] if c["focus"] in buckets else "Untagged"
        buckets[key] += c["mrr"] or 0
        counts[key]  += 1
    return [
        {"label": k, "mrr": round(v), "count": counts[k]}
        for k, v in buckets.items()
        if counts[k] > 0
    ]


# ── HTML helpers ───────────────────────────────────────────────────────────────

def val_html(v, style="ok"):
    if v is None:
        return '<span class="val-na">—</span>'
    css = "val-ok" if style == "ok" else "val-warn"
    return f'<span class="{css}">{int(v)}</span>'


def kpi_row(emoji, label, v, style="ok"):
    return f'<li>{emoji} {label} → {val_html(v, style)}</li>'


def series_js(key, rows):
    return json.dumps([r[key] for r in rows])


def fmt_mrr(v):
    if v is None:
        return "—"
    return f"€{int(v):,}"


def fmt_date(d):
    if not d:
        return "—"
    try:
        return datetime.strptime(d, "%Y-%m-%d").strftime("%d %b %Y")
    except Exception:
        return d


def owner_badge(owner):
    css = "badge-alex" if owner == "Alex" else "badge-aya" if owner == "Aya" else "badge-na"
    return f'<span class="badge {css}">{owner or "—"}</span>'


def churn_cat_badge(cat):
    if not cat:
        return '<span class="badge badge-na">—</span>'
    colors = {
        "Missing features": "badge-blue",
        "AI Behavior":      "badge-purple",
        "Platform & UI":    "badge-orange",
        "Integration":      "badge-yellow",
        "Competitor":       "badge-red",
        "Unknown":          "badge-gray",
    }
    css = colors.get(cat, "badge-gray")
    return f'<span class="badge {css}">{cat}</span>'


def sentiment_badge(sentiment):
    if sentiment == "At Risk":
        return '<span class="badge badge-red">⚠️ At Risk</span>'
    elif sentiment == "Alright":
        return '<span class="badge badge-yellow">Alright</span>'
    elif sentiment == "Great":
        return '<span class="badge badge-green">Great</span>'
    elif sentiment:
        return f'<span class="badge badge-gray">{sentiment}</span>'
    return '<span class="val-na">—</span>'


def build_churn_rows(customers):
    if not customers:
        return '<tr><td colspan="6" class="empty-row">No churning customers right now 🎉</td></tr>'
    rows = []
    for c in customers:
        note = c["churn_note"] or '<span class="val-na">not filled</span>'
        rows.append(f"""
    <tr>
      <td class="td-name">{c["name"] or "—"}</td>
      <td>{owner_badge(c["owner"])}</td>
      <td class="td-num">{fmt_mrr(c["mrr"])}</td>
      <td>{sentiment_badge(c["sentiment"])}</td>
      <td>{churn_cat_badge(c["churn_cat"])}</td>
      <td class="td-note">{note}</td>
      <td class="td-date">{fmt_date(c["cancel_date"])}</td>
    </tr>""")
    return "\n".join(rows)


def build_health_rows(customers):
    if not customers:
        return '<tr><td colspan="4" class="empty-row">No at-risk customers right now 🎉</td></tr>'
    rows = []
    for c in customers:
        reason = c["at_risk_reason"] or '<span class="val-na">not filled</span>'
        rows.append(f"""
    <tr>
      <td class="td-name">{c["name"] or "—"}</td>
      <td>{owner_badge(c["owner"])}</td>
      <td class="td-num">{fmt_mrr(c["mrr"])}</td>
      <td class="td-note">{reason}</td>
    </tr>""")
    return "\n".join(rows)


# ── Generate HTML ──────────────────────────────────────────────────────────────

def generate_html(current, history, churning, red_health, arr_by_focus):
    c = current
    week_label  = c["week"] or "Current Week"
    fetched_at  = datetime.now().strftime("%a %d %b %Y, %H:%M")

    trend          = list(reversed(history[:8]))
    week_labels_js = json.dumps([r["week"].split(" ")[0] for r in trend])

    churn_rows  = build_churn_rows(churning)
    health_rows = build_health_rows(red_health)

    # ARR by Customer Focus
    focus_labels_js = json.dumps([r["label"] for r in arr_by_focus])
    focus_mrr_js    = json.dumps([r["mrr"]   for r in arr_by_focus])
    focus_colors    = {
        "AI for CS":          "rgba(59,130,246,0.85)",
        "WhatsApp Marketing": "rgba(34,197,94,0.85)",
        "Both":               "rgba(147,51,234,0.85)",
        "Untagged":           "rgba(156,163,175,0.7)",
    }
    focus_bg_js = json.dumps([
        focus_colors.get(r["label"], "rgba(156,163,175,0.7)") for r in arr_by_focus
    ])
    total_tagged_mrr = sum(r["mrr"] for r in arr_by_focus if r["label"] != "Untagged")
    focus_subtitle = f"Active + churning customers by use case. Total tagged MRR: {fmt_mrr(total_tagged_mrr)}/mo."

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>📊 CS Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #fff; color: #1a1a1a;
    padding: 48px 64px; max-width: 1100px; margin: 0 auto;
  }}

  h1 {{ font-size: 2.4rem; font-weight: 700; margin-bottom: 44px; }}
  h2 {{ font-size: 1.35rem; font-weight: 600; margin-bottom: 6px; }}
  .subtitle {{ color: #666; font-size: 0.9rem; margin-bottom: 28px; }}
  hr {{ border: none; border-top: 1px solid #e5e7eb; margin: 44px 0; }}

  /* ── KPI Cards ── */
  .cards {{ display: grid; grid-template-columns: 1fr 1fr; gap: 48px; margin-bottom: 56px; }}
  .rep-header {{
    border-radius: 12px; padding: 15px 20px;
    font-size: 1.05rem; font-weight: 500;
    margin-bottom: 20px; display: flex; align-items: center; gap: 12px;
  }}
  .rep-header.alex {{ background: #dbeafe; }}
  .rep-header.aya  {{ background: #ede9fe; }}
  .kpi-list {{ list-style: none; display: flex; flex-direction: column; gap: 16px; }}
  .kpi-list li {{ font-size: 1.02rem; }}
  .val-ok   {{ font-weight: 700; color: #166534; background: #dcfce7; padding: 1px 8px; border-radius: 5px; }}
  .val-warn {{ font-weight: 700; color: #991b1b; background: #fee2e2; padding: 1px 8px; border-radius: 5px; }}
  .val-na   {{ color: #999; font-weight: 500; }}

  /* ── Trend Charts ── */
  .charts-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 32px; }}
  .chart-box {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 22px; }}
  .chart-title {{ font-size: 0.95rem; font-weight: 600; margin-bottom: 14px; color: #333; }}

  /* ── Tables ── */
  .section-count {{
    display: inline-block; background: #fee2e2; color: #991b1b;
    font-size: 0.8rem; font-weight: 700; padding: 2px 8px;
    border-radius: 20px; margin-left: 8px; vertical-align: middle;
  }}
  .section-count.green {{ background: #dcfce7; color: #166534; }}
  table {{
    width: 100%; border-collapse: collapse; margin-top: 4px;
    font-size: 0.92rem;
  }}
  th {{
    text-align: left; padding: 10px 14px;
    border-bottom: 2px solid #e5e7eb;
    font-size: 0.8rem; text-transform: uppercase;
    letter-spacing: 0.05em; color: #6b7280; font-weight: 600;
  }}
  td {{ padding: 11px 14px; border-bottom: 1px solid #f3f4f6; vertical-align: top; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #fafafa; }}
  .td-name  {{ font-weight: 600; }}
  .td-num   {{ font-variant-numeric: tabular-nums; white-space: nowrap; }}
  .td-note  {{ color: #555; max-width: 340px; line-height: 1.45; }}
  .td-date  {{ white-space: nowrap; color: #888; font-size: 0.87rem; }}
  .empty-row {{ text-align: center; color: #6b7280; padding: 24px; font-style: italic; }}

  /* ── Badges ── */
  .badge {{
    display: inline-block; padding: 2px 9px; border-radius: 20px;
    font-size: 0.8rem; font-weight: 600; white-space: nowrap;
  }}
  .badge-alex   {{ background: #dbeafe; color: #1e40af; }}
  .badge-aya    {{ background: #ede9fe; color: #6d28d9; }}
  .badge-na     {{ background: #f3f4f6; color: #9ca3af; }}
  .badge-blue   {{ background: #dbeafe; color: #1e40af; }}
  .badge-purple {{ background: #ede9fe; color: #6d28d9; }}
  .badge-orange {{ background: #ffedd5; color: #9a3412; }}
  .badge-yellow {{ background: #fef9c3; color: #854d0e; }}
  .badge-red    {{ background: #fee2e2; color: #991b1b; }}
  .badge-gray   {{ background: #f3f4f6; color: #374151; }}
  .badge-green  {{ background: #dcfce7; color: #166534; }}

  .footer {{ font-size: 0.78rem; color: #aaa; margin-top: 48px; }}
  code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 4px; font-size: 0.85em; }}
</style>
</head>
<body>

<h1>📊 CS Dashboard</h1>

<!-- ── THIS WEEK ── -->
<h2>📅 This Week</h2>
<p class="subtitle">Live KPI snapshot for <strong>{week_label}</strong>. Fetched {fetched_at}.</p>

<div class="cards">
  <div>
    <div class="rep-header alex">👤 Alex de Godoy</div>
    <ul class="kpi-list">
      {kpi_row("📞", "Contacted",           c["alex_contacted"],  "ok")}
      {kpi_row("🎉", "Graduated",           c["alex_graduated"],  "ok")}
      {kpi_row("🔴", "Red Health",          c["alex_red"],        "warn")}
      {kpi_row("⏰", "No Contact &gt;21d",  c["alex_nocontact"],  "warn")}
      {kpi_row("😢", "Churned",             c["alex_churned"],    "warn")}
    </ul>
  </div>
  <div>
    <div class="rep-header aya">👤 Aya Guerimej</div>
    <ul class="kpi-list">
      {kpi_row("📞", "Contacted",           c["aya_contacted"],   "ok")}
      {kpi_row("🎉", "Graduated",           c["aya_graduated"],   "ok")}
      {kpi_row("🔴", "Red Health",          c["aya_red"],         "warn")}
      {kpi_row("⏰", "No Contact &gt;21d",  c["aya_nocontact"],   "warn")}
      {kpi_row("😢", "Churned",             c["aya_churned"],     "warn")}
    </ul>
  </div>
</div>

<hr>

<!-- ── WEEKLY TRENDS ── -->
<h2>📈 Weekly Trends</h2>
<p class="subtitle">Last {len(trend)} weeks — Alex (blue) vs Aya (purple).</p>

<div class="charts-grid">
  <div class="chart-box">
    <div class="chart-title">📞 Customers Contacted</div>
    <canvas id="cContacted" height="170"></canvas>
  </div>
  <div class="chart-box">
    <div class="chart-title">🔴 Red Health</div>
    <canvas id="cRed" height="170"></canvas>
  </div>
  <div class="chart-box">
    <div class="chart-title">⏰ No Contact &gt;21d</div>
    <canvas id="cNoContact" height="170"></canvas>
  </div>
  <div class="chart-box">
    <div class="chart-title">😢 Churned</div>
    <canvas id="cChurned" height="170"></canvas>
  </div>
</div>

<hr>

<!-- ── ARR BY CUSTOMER FOCUS ── -->
<h2>🎯 MRR by Customer Focus</h2>
<p class="subtitle">{focus_subtitle}</p>

<div class="chart-box" style="max-width:560px">
  <canvas id="cFocus" height="140"></canvas>
</div>

<hr>

<!-- ── CHURNING PIPELINE ── -->
<h2>😢 Churning Pipeline <span class="section-count">{len(churning)}</span></h2>
<p class="subtitle">Customers who clicked cancel (Billing Status = Churning). Fill <strong>Churn Reason</strong> and <strong>Churn Notes</strong> in Notion.</p>

<table>
  <thead>
    <tr>
      <th>Customer</th>
      <th>Owner</th>
      <th>MRR</th>
      <th>Was at risk?</th>
      <th>Category</th>
      <th>Why they churned (CS notes)</th>
      <th>Sub ends</th>
    </tr>
  </thead>
  <tbody>
    {churn_rows}
  </tbody>
</table>

<hr>

<!-- ── RED HEALTH ── -->
<h2>🔴 Red Health <span class="section-count">{len(red_health)}</span></h2>
<p class="subtitle">Customers marked <strong>At Risk</strong> (CS Sentiment). Fill <strong>🔴 At Risk Reason</strong> in the MCT to explain the situation.</p>

<table>
  <thead>
    <tr>
      <th>Customer</th>
      <th>Owner</th>
      <th>MRR</th>
      <th>Key Blockers / Why at risk (CS notes)</th>
    </tr>
  </thead>
  <tbody>
    {health_rows}
  </tbody>
</table>

<p class="footer">
  Refresh: <code>python3 cs_dashboard.py</code> then reload this tab.
</p>

<script>
const labels = {week_labels_js};
const BLUE   = "rgba(59,130,246,0.9)";
const PURPLE = "rgba(147,51,234,0.9)";

function mkChart(id, d1, d2) {{
  new Chart(document.getElementById(id), {{
    type: "line",
    data: {{
      labels,
      datasets: [
        {{ label: "Alex", data: d1, borderColor: BLUE,   backgroundColor: "rgba(59,130,246,0.1)",
           fill: true, tension: 0.3, pointRadius: 5, borderWidth: 2 }},
        {{ label: "Aya",  data: d2, borderColor: PURPLE, backgroundColor: "rgba(147,51,234,0.05)",
           fill: true, tension: 0.3, pointRadius: 5, borderWidth: 2 }},
      ]
    }},
    options: {{
      responsive: true,
      plugins: {{ legend: {{ position: "bottom", labels: {{ boxWidth: 12 }} }} }},
      scales: {{ y: {{ beginAtZero: true, ticks: {{ precision: 0 }} }},
                 x: {{ ticks: {{ font: {{ size: 11 }} }} }} }}
    }}
  }});
}}

mkChart("cContacted", {series_js("alex_contacted", trend)}, {series_js("aya_contacted", trend)});
mkChart("cRed",       {series_js("alex_red",       trend)}, {series_js("aya_red",       trend)});
mkChart("cNoContact", {series_js("alex_nocontact", trend)}, {series_js("aya_nocontact", trend)});
mkChart("cChurned",   {series_js("alex_churned",   trend)}, {series_js("aya_churned",   trend)});

// ARR by Customer Focus
new Chart(document.getElementById("cFocus"), {{
  type: "bar",
  data: {{
    labels: {focus_labels_js},
    datasets: [{{
      label: "MRR (€/mo)",
      data: {focus_mrr_js},
      backgroundColor: {focus_bg_js},
      borderRadius: 6,
    }}]
  }},
  options: {{
    indexAxis: "y",
    responsive: true,
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        callbacks: {{
          label: ctx => " €" + ctx.raw.toLocaleString() + "/mo"
        }}
      }}
    }},
    scales: {{
      x: {{ beginAtZero: true, ticks: {{ callback: v => "€" + v.toLocaleString() }} }},
      y: {{ ticks: {{ font: {{ size: 12 }} }} }}
    }}
  }}
}});
</script>
</body>
</html>"""


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("Fetching scorecard data...")
    rows    = query_scorecard()
    history = [extract_scorecard(r) for r in rows]
    current = history[0]
    print(f"  Current week: {current['week']} ({len(rows)} weeks total)")

    churning      = fetch_churning()
    red_health    = fetch_red_health()
    arr_by_focus  = fetch_arr_by_focus()
    print(f"  Churning: {len(churning)} customers")
    print(f"  At risk:  {len(red_health)} customers")
    print(f"  Focus groups: {arr_by_focus}")

    html = generate_html(current, history, churning, red_health, arr_by_focus)
    OUTPUT.write_text(html)
    print(f"  Written → {OUTPUT}")
    print("Done. Open cs_dashboard.html in your browser.")


if __name__ == "__main__":
    main()
