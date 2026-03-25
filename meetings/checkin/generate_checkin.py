"""
generate_checkin.py

Reads checkin_data.json + releases.json and renders checkin.html — a 4-slide
interactive presentation for CS check-in calls. Navigate with arrow keys
or the dot nav on the right.

    python3 meetings/checkin/generate_checkin.py

Also importable: generate_all_checkins.py uses render_checkin_html() directly.
"""

import json
import os
from datetime import datetime

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_FILE     = os.path.join(SCRIPT_DIR, "checkin_data.json")
RELEASES_FILE = os.path.join(SCRIPT_DIR, "releases.json")
OUTPUT_FILE   = os.path.join(SCRIPT_DIR, "checkin.html")


# ── LOAD DATA ────────────────────────────────────────────────────────────────

def _load_json(path):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


# ── RENDER HELPERS ───────────────────────────────────────────────────────────

STATUS_BADGE = {
    "Open":          ("badge-red",    "Open"),
    "In Progress":   ("badge-yellow", "In Progress"),
    "Resolved":      ("badge-green",  "Resolved"),
    "Deprioritized": ("badge-grey",   "Deprioritized"),
}

ISSUE_TYPE_BADGE = {
    "Bug":                         ("badge-itype-bug",     "Bug"),
    "New feature request":         ("badge-itype-feature", "New feature request"),
    "Feature improvement":         ("badge-itype-feature", "Feature improvement"),
    "Feature Improvement Request": ("badge-itype-feature", "Feature improvement"),
}


def _fmt_date(iso_str):
    """'2026-03-10' → 'Mar 10'"""
    if not iso_str:
        return ""
    try:
        d = datetime.strptime(iso_str, "%Y-%m-%d")
        return d.strftime("%b %d").replace(" 0", " ")
    except ValueError:
        return iso_str


def _render_issues_table(issues):
    if not issues:
        return (
            '<tr><td colspan="7" style="text-align:center; color:var(--muted); padding:40px;">'
            'No issues found for this customer</td></tr>'
        )

    rows = []
    for i, issue in enumerate(issues):
        cls, label = STATUS_BADGE.get(issue["status"], ("badge-grey", issue["status"]))

        itype_raw = issue.get("issue_type", "")
        itype_cls, itype_label = ISSUE_TYPE_BADGE.get(itype_raw, ("badge-itype-other", itype_raw))
        itype_html = f'<span class="badge {itype_cls}">{itype_label}</span>' if itype_label else ""

        link_html = ""
        if issue.get("linear_url"):
            link_html = (
                f'<a href="{issue["linear_url"]}" target="_blank" rel="noopener" '
                f'style="color:var(--blue); text-decoration:none; font-size:16px;" '
                f'title="Open in Linear">↗</a>'
            )

        resolved_html = _fmt_date(issue.get("resolved_at")) or '<span style="color:var(--muted);">—</span>'

        summary_preview = (issue.get("summary") or "")[:120]
        if len(issue.get("summary") or "") > 120:
            summary_preview += "…"

        rows.append(
            f'<tr class="issue-row" data-idx="{i}">'
            f'<td class="date-cell">{_fmt_date(issue["created_at"])}</td>'
            f'<td class="date-cell">{resolved_html}</td>'
            f'<td><span class="badge {cls}">{label}</span></td>'
            f'<td class="issue-title-cell">{issue["title"]}</td>'
            f'<td style="text-align:center;">{itype_html}</td>'
            f'<td style="text-align:center;">{link_html}</td>'
            f'</tr>'
            f'<tr class="notes-row" data-idx="{i}">'
            f'<td colspan="7" style="padding:0;">'
            f'<div class="notes-container">'
            f'<div class="notes-summary" style="color:var(--muted); font-size:13px; margin-bottom:4px;">'
            f'{summary_preview}</div>'
            f'<div class="notes-editable" contenteditable="true" '
            f'data-placeholder="Click to add notes…"></div>'
            f'</div>'
            f'</td>'
            f'</tr>'
        )

    return "".join(rows)


def _render_releases_list(releases_data):
    items = releases_data.get("items", [])
    if not items:
        return '<li class="release-item">No releases yet — edit releases.json to add items</li>'

    html_items = []
    for item in items:
        emoji = item.get("emoji", "🚀")
        desc = f' — {item["description"]}' if item.get("description") else ""
        html_items.append(
            f'<li class="release-item">'
            f'<span class="release-emoji">{emoji}</span>'
            f'<strong>{item["title"]}</strong>'
            f'<span class="release-date">{item.get("date", "")}</span>'
            f'{desc}'
            f'</li>'
        )
    return "".join(html_items)


# ── MAIN RENDER FUNCTION (importable) ────────────────────────────────────────

def render_checkin_html(customer_name, issues, releases_data):
    """Render a complete 4-slide check-in HTML for one customer. Returns HTML string."""

    issues_table = _render_issues_table(issues)
    releases_list = _render_releases_list(releases_data)
    issue_count = len(issues)
    releases_updated = releases_data.get("updated", "")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Check-in — {customer_name}</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"/>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  :root {{
    --navy:   #0f172a;
    --card:   #ffffff;
    --blue:   #4F8EF7;
    --red:    #F87171;
    --green:  #34D399;
    --yellow: #FBBF24;
    --text:   #1e293b;
    --muted:  #64748b;
    --border: #e2e8f0;
    --shadow: 0 2px 12px rgba(0,0,0,0.08);
  }}

  html {{ scroll-behavior: smooth; font-family: 'Inter', sans-serif; }}
  body {{ background: #f1f5f9; color: var(--text); overflow-x: hidden; }}

  /* ── NAV ─────────────────────────────────────────────────────────── */
  #dot-nav {{
    position: fixed; top: 50%; right: 18px; transform: translateY(-50%);
    display: flex; flex-direction: column; gap: 10px; z-index: 100;
  }}
  .dot {{
    width: 10px; height: 10px; border-radius: 50%;
    background: rgba(255,255,255,0.35); border: 2px solid rgba(255,255,255,0.6);
    cursor: pointer; transition: background .25s, transform .2s;
  }}
  .dot.active {{ background: var(--blue); border-color: var(--blue); transform: scale(1.3); }}

  /* ── SLIDES ──────────────────────────────────────────────────────── */
  .slide {{
    min-height: 100vh; display: flex; flex-direction: column;
    padding: 0 0 40px 0; scroll-margin-top: 0;
  }}
  .slide-header {{
    background: var(--navy); color: #fff;
    padding: 28px 48px 24px;
    display: flex; align-items: baseline; gap: 16px;
  }}
  .slide-num {{ font-size: 13px; font-weight: 600; letter-spacing: 2px; text-transform: uppercase; opacity: .55; }}
  .slide-title {{ font-size: 28px; font-weight: 700; }}
  .slide-subtitle {{ font-size: 14px; opacity: .6; margin-left: auto; }}

  .slide-body {{
    flex: 1; padding: 32px 48px;
    display: flex; flex-direction: column; gap: 24px;
  }}

  /* ── CARDS ───────────────────────────────────────────────────────── */
  .card {{
    background: var(--card); border-radius: 12px; box-shadow: var(--shadow);
    padding: 24px; display: flex; flex-direction: column; gap: 12px;
  }}
  .card-title {{ font-size: 14px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: .8px; }}

  /* ── TABLES ──────────────────────────────────────────────────────── */
  table {{ width: 100%; border-collapse: collapse; font-size: 15px; }}
  th    {{ text-align: left; font-size: 12px; font-weight: 600; color: var(--muted);
           text-transform: uppercase; letter-spacing: .6px; padding: 9px 12px; border-bottom: 2px solid var(--border); }}
  td    {{ padding: 10px 12px; border-bottom: 1px solid var(--border); vertical-align: top; }}
  tr:last-child td {{ border-bottom: none; }}

  /* ── BADGES ──────────────────────────────────────────────────────── */
  .badge {{ display: inline-block; padding: 3px 11px; border-radius: 20px; font-size: 13px; font-weight: 600; white-space: nowrap; }}
  .badge-red    {{ background: #FEE2E2; color: #B91C1C; }}
  .badge-yellow {{ background: #FEF9C3; color: #713F12; }}
  .badge-green  {{ background: #D1FAE5; color: #065F46; }}
  .badge-grey   {{ background: #F1F5F9; color: #64748B; }}
  .badge-blue   {{ background: #DBEAFE; color: #1D4ED8; }}
  .badge-itype-bug     {{ background: #FEE2E2; color: #B91C1C; }}
  .badge-itype-feature {{ background: #EDE9FE; color: #6D28D9; }}
  .badge-itype-other   {{ background: #F1F5F9; color: #64748B; }}

  /* ── ISSUE TABLE SPECIFICS ──────────────────────────────────────── */
  .issue-row {{ cursor: pointer; transition: background .15s; }}
  .issue-row:hover {{ background: #f8fafc; }}
  .issue-title-cell {{ font-weight: 600; }}
  .date-cell {{ white-space: nowrap; color: var(--muted); font-size: 13px; }}

  .notes-row {{ display: none; }}
  .notes-row.expanded {{ display: table-row; }}
  .notes-container {{
    padding: 12px 16px 16px;
    background: #f8fafc;
    border-left: 3px solid var(--blue);
    margin: 0;
  }}
  .notes-editable {{
    min-height: 36px; padding: 8px 12px;
    background: #fff; border: 1px dashed var(--border); border-radius: 6px;
    font-size: 14px; line-height: 1.6; color: var(--text);
    outline: none; transition: border-color .2s;
  }}
  .notes-editable:focus {{ border-color: var(--blue); border-style: solid; }}
  .notes-editable:empty::before {{
    content: attr(data-placeholder);
    color: var(--muted); font-style: italic;
  }}

  /* ── RELEASES LIST ──────────────────────────────────────────────── */
  .releases-list {{
    list-style: none; padding: 0;
    display: flex; flex-direction: column; gap: 12px;
  }}
  .release-item {{
    padding: 16px 20px;
    background: #f8fafc;
    border-radius: 8px;
    border-left: 3px solid var(--green);
    font-size: 16px;
    line-height: 1.5;
    display: flex;
    align-items: baseline;
    gap: 12px;
  }}
  .release-emoji {{
    font-size: 24px;
    flex-shrink: 0;
  }}
  .release-date {{
    display: inline-block;
    margin-left: 8px;
    font-size: 12px;
    color: var(--muted);
    font-weight: 400;
  }}
  .releases-editable {{
    min-height: 60px; padding: 16px;
    background: #fff; border: 1px dashed var(--border); border-radius: 8px;
    font-size: 15px; line-height: 1.6; color: var(--text);
    outline: none;
  }}
  .releases-editable:focus {{ border-color: var(--blue); border-style: solid; }}

  /* ── NOTES SLIDE ────────────────────────────────────────────────── */
  .notes-section {{
    flex: 1;
  }}
  .notes-section-title {{
    font-size: 18px; font-weight: 700; color: var(--navy);
    margin-bottom: 8px;
  }}
  .notes-area {{
    min-height: 120px; padding: 16px;
    background: #fff; border: 1px dashed var(--border); border-radius: 8px;
    font-size: 15px; line-height: 1.8; color: var(--text);
    outline: none;
  }}
  .notes-area:focus {{ border-color: var(--blue); border-style: solid; }}
  .notes-area:empty::before {{
    content: attr(data-placeholder);
    color: var(--muted); font-style: italic;
  }}

  /* ── METRICS DASHBOARD ──────────────────────────────────────────── */
  .metrics-grid {{
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px;
  }}
  .metric-card {{
    background: var(--card); border-radius: 12px; box-shadow: var(--shadow);
    padding: 24px; text-align: center; position: relative; overflow: hidden;
  }}
  .metric-card .metric-icon {{
    font-size: 28px; margin-bottom: 8px;
  }}
  .metric-card .metric-value {{
    font-size: 42px; font-weight: 800; line-height: 1; margin-bottom: 4px;
  }}
  .metric-card .metric-label {{
    font-size: 13px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: .5px;
  }}
  .metric-card .metric-sub {{
    font-size: 12px; color: var(--muted); margin-top: 6px;
  }}
  .metric-card .metric-bar {{
    position: absolute; bottom: 0; left: 0; right: 0; height: 4px;
    border-radius: 0 0 12px 12px;
  }}
  .charts-row {{
    display: flex; gap: 20px; flex: 1;
  }}
  .charts-row .card {{ flex: 1; }}
  .chart-container {{ position: relative; width: 100%; }}
  .chart-container canvas {{ width: 100% !important; }}

  /* ── CHANNELS CONNECTED ────────────────────────────────────────── */
  .channels-grid {{
    display: flex; gap: 16px;
  }}
  .channel-box {{
    flex: 1; background: var(--card); border-radius: 12px; box-shadow: var(--shadow);
    padding: 18px 16px; display: flex; align-items: center; gap: 14px;
  }}
  .channel-icon {{
    width: 44px; height: 44px; border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 22px; flex-shrink: 0;
  }}
  .channel-icon.whatsapp  {{ background: #dcfce7; }}
  .channel-icon.livechat  {{ background: #dbeafe; }}
  .channel-icon.email     {{ background: #fef3c7; }}
  .channel-icon.instagram {{ background: #fce7f3; }}
  .channel-name {{
    font-size: 15px; font-weight: 600; color: var(--text);
  }}
  .channel-status {{
    margin-left: auto; font-size: 20px; flex-shrink: 0;
  }}

  /* ── NEXT STEPS BULLET LIST ─────────────────────────────────────── */
  .next-steps-list {{
    list-style: disc; padding: 8px 16px 8px 36px;
    font-size: 20px; line-height: 2; color: var(--text);
    outline: none; min-height: 100%;
  }}
  .next-steps-list li {{ margin-bottom: 4px; }}
  .next-steps-list:empty::before {{
    content: attr(data-placeholder);
    color: var(--muted); font-style: italic;
  }}

  /* ── PRINT (landscape, one slide per page) ─────────────────────── */
  @page {{ size: landscape; margin: 0.5cm; }}
  @media print {{
    html, body {{
      -webkit-print-color-adjust: exact !important;
      print-color-adjust: exact !important;
      color-adjust: exact !important;
    }}
    .slide {{
      min-height: auto; height: auto;
      page-break-after: always; page-break-inside: avoid;
      padding-bottom: 0;
    }}
    .slide:last-child {{ page-break-after: auto; }}
    #dot-nav {{ display: none !important; }}
    .notes-row.expanded {{ display: table-row !important; }}
    .notes-editable, .notes-area, .releases-editable {{ border-style: solid !important; }}
    .slide-body {{ padding: 16px 32px; }}
    .metrics-grid {{ gap: 10px; }}
    .charts-row {{ gap: 12px; }}
    canvas {{ max-height: 180px !important; }}
  }}
</style>
</head>
<body>

<!-- ── DOT NAV ─────────────────────────────────────────────────────────── -->
<nav id="dot-nav">
  <div class="dot active" data-target="slide1" title="Success Metrics"></div>
  <div class="dot"        data-target="slide2" title="Past Issues"></div>
  <div class="dot"        data-target="slide3" title="New Releases"></div>
  <div class="dot"        data-target="slide4" title="Next Steps"></div>
</nav>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 1 — Success Metrics
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide1">
  <header class="slide-header">
    <span class="slide-num">01 / 04</span>
    <h1 class="slide-title">{customer_name} — Success Metrics</h1>
    <span class="slide-subtitle">Last 30 days</span>
  </header>

  <div class="slide-body">
    <div class="metrics-grid">
      <div class="metric-card">
        <div class="metric-icon">🤖</div>
        <div class="metric-value" style="color:var(--blue);">78%</div>
        <div class="metric-label">AI Resolution Rate</div>
        <div class="metric-sub">+6% vs prev. month</div>
        <div class="metric-bar" style="background:var(--blue);"></div>
      </div>
      <div class="metric-card">
        <div class="metric-icon">🎫</div>
        <div class="metric-value" style="color:var(--green);">1,240</div>
        <div class="metric-label">Tickets Handled</div>
        <div class="metric-sub">892 by AI · 348 by humans</div>
        <div class="metric-bar" style="background:var(--green);"></div>
      </div>
      <div class="metric-card">
        <div class="metric-icon">⏱️</div>
        <div class="metric-value" style="color:#A78BFA;">62h</div>
        <div class="metric-label">Hours Saved</div>
        <div class="metric-sub">~3 min avg per AI ticket</div>
        <div class="metric-bar" style="background:#A78BFA;"></div>
      </div>
      <div class="metric-card">
        <div class="metric-icon">⚡</div>
        <div class="metric-value" style="color:var(--yellow);">18s</div>
        <div class="metric-label">Avg Response Time</div>
        <div class="metric-sub">AI first reply</div>
        <div class="metric-bar" style="background:var(--yellow);"></div>
      </div>
    </div>

    <div class="charts-row">
      <div class="card">
        <div class="card-title">Ticket volume &amp; AI resolution — last 8 weeks</div>
        <div class="chart-container">
          <canvas id="chart-tickets" height="220"></canvas>
        </div>
      </div>
      <div class="card">
        <div class="card-title">Hours saved per week</div>
        <div class="chart-container">
          <canvas id="chart-hours" height="220"></canvas>
        </div>
      </div>
    </div>

    <div>
      <div class="card-title" style="margin-bottom:12px;">Channels connected</div>
      <div class="channels-grid">
        <div class="channel-box">
          <div class="channel-icon whatsapp">💬</div>
          <div class="channel-name">WhatsApp</div>
          <div class="channel-status">✅</div>
        </div>
        <div class="channel-box">
          <div class="channel-icon livechat">🌐</div>
          <div class="channel-name">Live Chat</div>
          <div class="channel-status">✅</div>
        </div>
        <div class="channel-box">
          <div class="channel-icon email">📧</div>
          <div class="channel-name">Email</div>
          <div class="channel-status">❌</div>
        </div>
        <div class="channel-box">
          <div class="channel-icon instagram">📸</div>
          <div class="channel-name">Instagram</div>
          <div class="channel-status">❌</div>
        </div>
      </div>
    </div>
  </div>
</section>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 2 — Past Issues Status
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide2">
  <header class="slide-header">
    <span class="slide-num">02 / 04</span>
    <h1 class="slide-title">Recent Issues — {customer_name}</h1>
    <span class="slide-subtitle">{issue_count} issues · Click row to expand notes</span>
  </header>

  <div class="slide-body">
    <div class="card" style="flex:1; overflow-y:auto;">
      <table>
        <thead>
          <tr>
            <th style="width:90px;">Created</th>
            <th style="width:90px;">Resolved</th>
            <th style="width:110px;">Status</th>
            <th>Issue Title</th>
            <th style="width:160px; text-align:center;">Issue Type</th>
            <th style="width:50px; text-align:center;">Link</th>
          </tr>
        </thead>
        <tbody>
          {issues_table}
        </tbody>
      </table>
    </div>

    <div class="card">
      <div class="card-title">Notes</div>
      <div class="notes-area" contenteditable="true"
           data-placeholder="Type notes about past issues…"></div>
    </div>
  </div>
</section>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 3 — New Releases & Features
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide3">
  <header class="slide-header">
    <span class="slide-num">03 / 04</span>
    <h1 class="slide-title">New Releases &amp; Features</h1>
    <span class="slide-subtitle">Updated {releases_updated}</span>
  </header>

  <div class="slide-body">
    <div class="card" style="flex:1;">
      <div class="card-title">Recent releases</div>
      <ul class="releases-list">
        {releases_list}
      </ul>
    </div>

    <div class="card">
      <div class="card-title">Additional notes</div>
      <div class="releases-editable" contenteditable="true"
           data-placeholder="Add custom release notes or talking points for this customer…"></div>
    </div>
  </div>
</section>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 4 — Next Steps
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide4">
  <header class="slide-header">
    <span class="slide-num">04 / 04</span>
    <h1 class="slide-title">Next Steps — {customer_name}</h1>
    <span class="slide-subtitle">Live notes during the call</span>
  </header>

  <div class="slide-body" style="gap:20px;">
    <div class="card notes-section" style="flex:1;">
      <ul class="next-steps-list" contenteditable="true" data-placeholder="Type next steps…">
        <li><br></li>
      </ul>
    </div>
  </div>
</section>


<!-- ══════════════════════════════════════════════════════════════════════
     JAVASCRIPT
════════════════════════════════════════════════════════════════════════ -->
<script>
// ── CHARTS (Slide 1 — Success Metrics) ──────────────────────────────
const WEEKS = ['W1','W2','W3','W4','W5','W6','W7','W8'];
const TOTAL_TICKETS = [120, 135, 148, 160, 155, 170, 162, 180];
const AI_RESOLVED   = [ 85,  98, 110, 122, 118, 132, 128, 142];
const HOURS_SAVED   = [5.2, 6.0, 6.8, 7.5, 7.2, 8.1, 7.8, 8.7];

new Chart(document.getElementById('chart-tickets'), {{
  type: 'bar',
  data: {{
    labels: WEEKS,
    datasets: [
      {{
        label: 'AI resolved',
        data: AI_RESOLVED,
        backgroundColor: 'rgba(79,142,247,0.75)',
        borderRadius: 4,
        stack: 'a',
      }},
      {{
        label: 'Human handled',
        data: TOTAL_TICKETS.map((t,i) => t - AI_RESOLVED[i]),
        backgroundColor: 'rgba(203,213,225,0.6)',
        borderRadius: 4,
        stack: 'a',
      }},
    ]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ position: 'top', labels: {{ font: {{ size: 13 }} }} }} }},
    scales: {{
      y: {{ stacked: true, beginAtZero: true, grid: {{ display: false }}, ticks: {{ font: {{ size: 12 }} }} }},
      x: {{ stacked: true, grid: {{ display: false }}, ticks: {{ font: {{ size: 12 }} }} }}
    }}
  }}
}});

new Chart(document.getElementById('chart-hours'), {{
  type: 'line',
  data: {{
    labels: WEEKS,
    datasets: [{{
      label: 'Hours saved',
      data: HOURS_SAVED,
      borderColor: '#A78BFA',
      backgroundColor: 'rgba(167,139,250,0.15)',
      fill: true,
      tension: 0.3,
      pointBackgroundColor: '#A78BFA',
      pointRadius: 5,
      borderWidth: 2.5,
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      y: {{ beginAtZero: true, grid: {{ display: false }}, ticks: {{ font: {{ size: 12 }}, callback: v => v + 'h' }} }},
      x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 12 }} }} }}
    }}
  }}
}});

// ── Issue row expand/collapse ───────────────────────────────────────
document.querySelectorAll('.issue-row').forEach(row => {{
  row.addEventListener('click', e => {{
    // Don't toggle if clicking a link
    if (e.target.closest('a')) return;
    const idx = row.dataset.idx;
    const notesRow = document.querySelector(`.notes-row[data-idx="${{idx}}"]`);
    if (notesRow) {{
      notesRow.classList.toggle('expanded');
    }}
  }});
}});

// ── Stop arrow key navigation when typing in editable areas ─────────
function isEditing(el) {{
  return el && (el.isContentEditable || el.tagName === 'INPUT' || el.tagName === 'TEXTAREA');
}}

// ── DOT NAV + ARROW KEYS ─────────────────────────────────────────────
const slides = ['slide1', 'slide2', 'slide3', 'slide4'];
const dots   = document.querySelectorAll('.dot');

function activateDot(idx) {{
  dots.forEach((d, i) => d.classList.toggle('active', i === idx));
}}

// Intersection observer — update dots as user scrolls
const obs = new IntersectionObserver(entries => {{
  entries.forEach(e => {{
    if (e.isIntersecting) {{
      const idx = slides.indexOf(e.target.id);
      if (idx !== -1) activateDot(idx);
    }}
  }});
}}, {{ threshold: 0.5 }});

slides.forEach(id => {{
  const el = document.getElementById(id);
  if (el) obs.observe(el);
}});

// Dot click
dots.forEach((dot, i) => {{
  dot.addEventListener('click', () => {{
    document.getElementById(slides[i]).scrollIntoView({{ behavior: 'smooth' }});
  }});
}});

// Arrow key navigation (disabled when typing in editable fields)
let currentSlide = 0;
document.addEventListener('keydown', e => {{
  if (isEditing(document.activeElement)) return;
  if (e.key === 'ArrowDown' || e.key === 'ArrowRight') {{
    currentSlide = Math.min(currentSlide + 1, slides.length - 1);
    document.getElementById(slides[currentSlide]).scrollIntoView({{ behavior: 'smooth' }});
  }} else if (e.key === 'ArrowUp' || e.key === 'ArrowLeft') {{
    currentSlide = Math.max(currentSlide - 1, 0);
    document.getElementById(slides[currentSlide]).scrollIntoView({{ behavior: 'smooth' }});
  }}
}});

// Sync arrow-key tracker with scroll observer
const obsSync = new IntersectionObserver(entries => {{
  entries.forEach(e => {{
    if (e.isIntersecting) currentSlide = slides.indexOf(e.target.id);
  }});
}}, {{ threshold: 0.5 }});
slides.forEach(id => {{ const el = document.getElementById(id); if (el) obsSync.observe(el); }});
</script>
</body>
</html>
"""


# ── CLI MODE ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    data = _load_json(DATA_FILE)
    if not data:
        print("❌ checkin_data.json not found — run fetch_checkin_data.py first")
        raise SystemExit(1)

    releases = _load_json(RELEASES_FILE) or {"updated": "", "items": []}

    customer_name = data["customer_name"]
    issues = data["issues"]

    print(f"📊 Rendering check-in for: {customer_name}")
    print(f"   {len(issues)} issues, {len(releases['items'])} releases")

    html = render_checkin_html(customer_name, issues, releases)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅ Written → {OUTPUT_FILE}")
    print("   Open checkin.html in your browser to use during the call.")
