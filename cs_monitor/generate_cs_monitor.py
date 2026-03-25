#!/usr/bin/env python3
"""
generate_cs_monitor.py — Generates CS Team Monitor HTML + landscape PDF.

Reads cs_monitor_data.json, injects data into HTML template, and archives
the HTML to cs_monitor/reports/.

Run: python3 cs_monitor/generate_cs_monitor.py
"""
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATA_FILE = SCRIPT_DIR / "cs_monitor_data.json"
HTML_FILE = SCRIPT_DIR / "cs_team_monitor.html"
REPORTS_DIR = SCRIPT_DIR / "reports"


# ═══════════════════════════════════════════════════════════════════════════
# HTML Template
# ═══════════════════════════════════════════════════════════════════════════

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CS Team Monitor — Weekly Trends</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
<style>
  :root {
    --navy: #0f172a;
    --blue: #4F8EF7;
    --green: #34D399;
    --red: #F87171;
    --yellow: #FBBF24;
    --orange: #FB923C;
    --purple: #A78BFA;
    --text: #1e293b;
    --muted: #64748b;
    --border: #e2e8f0;
    --shadow: 0 2px 12px rgba(0,0,0,0.08);
    /* slide accent colors */
    --onb-accent: #3B82F6;
    --onb-bg: #EFF6FF;
    --chk-accent: #10B981;
    --chk-bg: #ECFDF5;
    --int-accent: #F59E0B;
    --int-bg: #FFFBEB;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; scroll-snap-type: y mandatory; }
  body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; color: var(--text); background: #f8fafc; }

  /* ── Slides ── */
  .slide {
    min-height: 100vh; scroll-snap-align: start;
    display: flex; flex-direction: column;
    padding: 40px 60px 40px;
  }
  .slide-header {
    display: flex; align-items: baseline; gap: 16px;
    margin-bottom: 28px; padding-bottom: 16px;
    border-bottom: 2px solid var(--border);
  }
  .slide-num { font-size: 13px; font-weight: 700; color: var(--muted); letter-spacing: 1px; }
  .slide-title { font-size: 28px; font-weight: 700; }
  .slide-subtitle { font-size: 14px; color: var(--muted); margin-left: auto; }

  /* ── KPI row ── */
  .kpi-row {
    display: flex; gap: 20px; margin-bottom: 28px;
  }
  .kpi-card {
    flex: 1; border-radius: 12px; padding: 20px 24px;
    box-shadow: var(--shadow); text-align: center;
    background: #fff; border-top: 4px solid var(--muted);
  }
  .kpi-card.accent-blue   { border-top-color: var(--onb-accent); background: var(--onb-bg); }
  .kpi-card.accent-green  { border-top-color: var(--chk-accent); background: var(--chk-bg); }
  .kpi-card.accent-yellow { border-top-color: var(--int-accent); background: var(--int-bg); }
  .kpi-card.accent-red    { border-top-color: var(--red); }
  .kpi-label { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.8px; color: var(--muted); margin-bottom: 6px; }
  .kpi-value { font-size: 36px; font-weight: 700; line-height: 1.1; }
  .kpi-sub   { font-size: 12px; color: var(--muted); margin-top: 4px; }
  .kpi-value.blue   { color: var(--onb-accent); }
  .kpi-value.green  { color: var(--chk-accent); }
  .kpi-value.yellow { color: var(--int-accent); }
  .kpi-value.red    { color: var(--red); }

  /* ── Charts ── */
  .chart-row { display: flex; gap: 24px; flex: 1; min-height: 0; }
  .card {
    flex: 1; background: #fff; border-radius: 12px;
    box-shadow: var(--shadow); padding: 20px 24px;
    display: flex; flex-direction: column;
  }
  .card-title {
    font-size: 13px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.8px; color: var(--muted); margin-bottom: 12px;
  }
  .chart-box { flex: 1; position: relative; min-height: 0; }

  /* ── Vertical chart stack (Slide 2) ── */
  .chart-stack { display: flex; flex-direction: column; gap: 16px; flex: 1; min-height: 0; }
  .chart-stack .card-row { flex: 1; min-height: 0; }

  /* ── Dot nav ── */
  .dot-nav {
    position: fixed; right: 24px; top: 50%; transform: translateY(-50%);
    display: flex; flex-direction: column; gap: 10px; z-index: 100;
  }
  .dot {
    width: 11px; height: 11px; border-radius: 50%;
    background: rgba(0,0,0,0.18); cursor: pointer;
    transition: all 0.2s;
  }
  .dot.active { transform: scale(1.4); }
  .dot[data-slide="0"].active { background: var(--onb-accent); }
  .dot[data-slide="1"].active { background: var(--chk-accent); }
  .dot[data-slide="2"].active { background: var(--int-accent); }

  /* ── Trend badge ── */
  .trend { font-size: 13px; font-weight: 600; margin-left: 6px; }
  .trend.up-good { color: var(--chk-accent); }
  .trend.up-bad  { color: var(--red); }
  .trend.down-good { color: var(--chk-accent); }
  .trend.down-bad  { color: var(--red); }
  .trend.flat { color: var(--muted); }


</style>
</head>
<body>

<!-- Dot Navigation -->
<nav class="dot-nav">
  <div class="dot active" data-slide="0" title="Onboarding"></div>
  <div class="dot" data-slide="1" title="Check-ins"></div>
  <div class="dot" data-slide="2" title="Intercom Support"></div>
</nav>

<!-- SLIDE 1 — ONBOARDING -->
<section class="slide" id="slide0">
  <div class="slide-header">
    <span class="slide-num">01 / 03</span>
    <h1 class="slide-title" style="color:var(--onb-accent)">Onboarding</h1>
    <span class="slide-subtitle">Config issues per customer — weekly trend</span>
  </div>

  <div class="chart-row" style="max-height:70vh;">
    <div class="card">
      <div class="card-title">Config Issues per Customer — Weekly</div>
      <div class="chart-box"><canvas id="chart-onb-bar"></canvas></div>
    </div>
  </div>
</section>

<!-- SLIDE 2 — CHECK-INS -->
<section class="slide" id="slide1">
  <div class="slide-header">
    <span class="slide-num">02 / 03</span>
    <h1 class="slide-title" style="color:var(--chk-accent)">Check-ins</h1>
    <span class="slide-subtitle">Customer health & churn — weekly trend</span>
  </div>

  <div class="chart-stack">
    <div class="card card-row" style="background:rgba(16,185,129,0.06);">
      <div class="card-title">Customers "Great"</div>
      <div class="chart-box"><canvas id="chart-chk-great"></canvas></div>
    </div>
    <div class="card card-row" style="background:rgba(248,113,113,0.08);">
      <div class="card-title">Customers "At Risk"</div>
      <div class="chart-box"><canvas id="chart-chk-risk"></canvas></div>
    </div>
    <div class="card card-row">
      <div class="card-title">Customers Churned</div>
      <div class="chart-box"><canvas id="chart-chk-churn"></canvas></div>
    </div>
  </div>
</section>

<!-- SLIDE 3 — INTERCOM SUPPORT -->
<section class="slide" id="slide2">
  <div class="slide-header">
    <span class="slide-num">03 / 03</span>
    <h1 class="slide-title" style="color:var(--int-accent)">Intercom Support</h1>
    <span class="slide-subtitle">Average reply time — weekly trend</span>
  </div>

  <div class="chart-row" style="max-height:70vh;">
    <div class="card">
      <div class="card-title">Average Reply Time (minutes) — Weekly</div>
      <div class="chart-box"><canvas id="chart-int-reply"></canvas></div>
    </div>
  </div>
</section>

<script>
// ═══════════════════════════════════════════════════════════════════════
// DATA — injected by generate_cs_monitor.py
// ═══════════════════════════════════════════════════════════════════════
const WEEKS = __WEEKS_JSON__;

const DATA = __DATA_JSON__;

// ─── Helpers ───
Chart.register(ChartDataLabels);
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.padding = 16;

const last = arr => arr[arr.length - 1];

function trendHTML(curr, prev, lowerIsBetter) {
  if (prev == null) return '';
  const diff = curr - prev;
  if (Math.abs(diff) < 0.01) return '<span class="trend flat">→ flat</span>';
  const arrow = diff > 0 ? '↑' : '↓';
  const good = lowerIsBetter ? diff < 0 : diff > 0;
  const cls = good ? (diff > 0 ? 'up-good' : 'down-good') : (diff > 0 ? 'up-bad' : 'down-bad');
  const pct = prev !== 0 ? Math.abs(diff / prev * 100).toFixed(0) + '%' : '';
  return `<span class="trend ${cls}">${arrow} ${pct}</span>`;
}

const d = DATA;

// ═══════════════════════════════════════════════════════════════════════
// CHARTS
// ═══════════════════════════════════════════════════════════════════════

// ── Slide 1: Onboarding ──
new Chart(document.getElementById('chart-onb-bar'), {
  type: 'bar',
  data: {
    labels: WEEKS,
    datasets: [{
      label: 'Total Config Issues',
      data: d.onboarding.configIssuesPerCustomer,
      backgroundColor: WEEKS.map((_, i) => i === WEEKS.length - 1 ? '#3B82F6' : 'rgba(59,130,246,0.35)'),
      borderRadius: 6,
    }]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    scales: {
      y: { beginAtZero: true, max: 0.2, grid: { color: '#f1f5f9' } },
      x: { grid: { display: false } }
    },
    plugins: {
      legend: { display: false },
      datalabels: { anchor: 'end', align: 'end', color: '#3B82F6', font: { size: 13, weight: 'bold' } }
    }
  }
});

// ── Slide 2: Check-ins — 3 separate charts ──
const groupedOpts = (yMax, stepSize, labelColor) => ({
  responsive: true, maintainAspectRatio: false,
  scales: {
    y: { beginAtZero: true, max: yMax, grid: { color: '#f1f5f9' }, ticks: { stepSize } },
    x: { grid: { display: false } }
  },
  plugins: {
    legend: { display: true },
    datalabels: {
      display: (ctx) => ctx.dataset.data[ctx.dataIndex] != null,
      anchor: 'end', align: 'end', color: labelColor, font: { size: 12, weight: 'bold' }
    }
  }
});

new Chart(document.getElementById('chart-chk-great'), {
  type: 'bar',
  data: {
    labels: WEEKS,
    datasets: [
      { label: 'Alex', data: d.checkins.greatAlex, backgroundColor: 'rgba(59,130,246,0.75)', borderRadius: 6 },
      { label: 'Aya',  data: d.checkins.greatAya,  backgroundColor: 'rgba(167,139,250,0.75)', borderRadius: 6 },
    ]
  },
  options: groupedOpts(100, 20, '#10B981')
});

new Chart(document.getElementById('chart-chk-risk'), {
  type: 'bar',
  data: {
    labels: WEEKS,
    datasets: [
      { label: 'Alex', data: d.checkins.riskAlex, backgroundColor: 'rgba(59,130,246,0.75)', borderRadius: 6 },
      { label: 'Aya',  data: d.checkins.riskAya,  backgroundColor: 'rgba(167,139,250,0.75)', borderRadius: 6 },
    ]
  },
  options: groupedOpts(100, 20, '#F87171')
});

new Chart(document.getElementById('chart-chk-churn'), {
  type: 'bar',
  data: {
    labels: WEEKS,
    datasets: [
      { label: 'Alex', data: d.checkins.churnAlex, backgroundColor: 'rgba(59,130,246,0.75)', borderRadius: 6 },
      { label: 'Aya',  data: d.checkins.churnAya,  backgroundColor: 'rgba(167,139,250,0.75)', borderRadius: 6 },
    ]
  },
  options: groupedOpts(null, 1, '#64748b')
});

// ── Slide 3: Intercom Support ──
new Chart(document.getElementById('chart-int-reply'), {
  type: 'bar',
  data: {
    labels: WEEKS,
    datasets: [{
      label: 'Avg Reply Time (min)',
      data: d.intercom.avgReplyMin,
      backgroundColor: WEEKS.map((_, i) => i === WEEKS.length - 1 ? '#F59E0B' : 'rgba(245,158,11,0.35)'),
      borderRadius: 6,
    }]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    scales: {
      y: { beginAtZero: true, grid: { color: '#f1f5f9' } },
      x: { grid: { display: false } }
    },
    plugins: {
      legend: { display: false },
      datalabels: { anchor: 'end', align: 'end', color: '#F59E0B', font: { size: 13, weight: 'bold' }, formatter: v => v + 'm' }
    }
  }
});

// ═══════════════════════════════════════════════════════════════════════
// NAVIGATION — arrow keys + dot nav
// ═══════════════════════════════════════════════════════════════════════
const slides = ['slide0', 'slide1', 'slide2'];
const dots = document.querySelectorAll('.dot');
let currentSlide = 0;

function activateDot(idx) {
  dots.forEach(d => d.classList.remove('active'));
  dots[idx].classList.add('active');
  currentSlide = idx;
}

const obs = new IntersectionObserver(entries => {
  entries.forEach(e => {
    if (e.isIntersecting) {
      const idx = slides.indexOf(e.target.id);
      if (idx !== -1) activateDot(idx);
    }
  });
}, { threshold: 0.5 });
slides.forEach(id => obs.observe(document.getElementById(id)));

dots.forEach((dot, i) => {
  dot.addEventListener('click', () => {
    document.getElementById(slides[i]).scrollIntoView({ behavior: 'smooth' });
  });
});

document.addEventListener('keydown', e => {
  if (e.key === 'ArrowDown' || e.key === 'ArrowRight') {
    currentSlide = Math.min(currentSlide + 1, slides.length - 1);
    document.getElementById(slides[currentSlide]).scrollIntoView({ behavior: 'smooth' });
  } else if (e.key === 'ArrowUp' || e.key === 'ArrowLeft') {
    currentSlide = Math.max(currentSlide - 1, 0);
    document.getElementById(slides[currentSlide]).scrollIntoView({ behavior: 'smooth' });
  }
});
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════════════
# Generate
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  CS Team Monitor \u2014 Generate HTML")
    print("=" * 60)

    # Load data
    if not DATA_FILE.exists():
        print(f"\u2717 {DATA_FILE} not found \u2014 run fetch_cs_monitor_data.py first")
        sys.exit(1)

    with open(DATA_FILE) as f:
        raw = json.load(f)

    print(f"\n  Data from: {raw['fetched_at']}")
    print(f"  Weeks: {raw['week_labels']}")

    # Map JSON keys to JS camelCase for the DATA object
    data_js = {
        "onboarding": {
            "configIssuesPerCustomer": raw["onboarding"]["config_issues_per_customer"],
        },
        "checkins": {
            "greatAlex": raw["checkins"]["great_alex"],
            "greatAya": raw["checkins"]["great_aya"],
            "riskAlex": raw["checkins"]["risk_alex"],
            "riskAya": raw["checkins"]["risk_aya"],
            "churnAlex": raw["checkins"]["churn_alex"],
            "churnAya": raw["checkins"]["churn_aya"],
        },
        "intercom": {
            "avgReplyMin": raw["intercom"]["avg_reply_min"],
        },
    }

    weeks_json = json.dumps(raw["week_labels"])
    data_json = json.dumps(data_js, indent=2)

    html = HTML_TEMPLATE.replace("__WEEKS_JSON__", weeks_json)
    html = html.replace("__DATA_JSON__", data_json)

    # Write working HTML
    HTML_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HTML_FILE, "w") as f:
        f.write(html)
    print(f"\n  \u2713 HTML written to {HTML_FILE}")

    # Archive HTML to reports/
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    current_week = raw["weeks"][-1]
    start = datetime.strptime(current_week["start"], "%Y-%m-%d")
    end = datetime.strptime(current_week["end"], "%Y-%m-%d")

    if start.month == end.month:
        archive_name = f"CS_monitor_{start.strftime('%b')}{start.day}-{end.day}.html"
    else:
        archive_name = (
            f"CS_monitor_{start.strftime('%b')}{start.day}-"
            f"{end.strftime('%b')}{end.day}.html"
        )

    archive_path = REPORTS_DIR / archive_name
    shutil.copy2(HTML_FILE, archive_path)
    print(f"  \u2713 Archived to {archive_path}")

    print(f"\n\u2705 Done")


if __name__ == "__main__":
    main()
