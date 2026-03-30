#!/usr/bin/env python3
"""
generate_ceo_dashboard.py -- Generates CEO CS Metrics Dashboard HTML.

Reads ceo_dashboard_data.json, injects data into HTML template, outputs HTML + archive.

Run: python3 ceo_dashboard/generate_ceo_dashboard.py
"""
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATA_FILE = SCRIPT_DIR / "ceo_dashboard_data.json"
HTML_FILE = SCRIPT_DIR / "ceo_dashboard.html"
REPORTS_DIR = SCRIPT_DIR / "reports"


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CS Metrics — Executive Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
<style>
  :root {
    --navy: #0f172a;
    --blue: #3B82F6;
    --green: #10B981;
    --red: #EF4444;
    --text: #1e293b;
    --muted: #64748b;
    --border: #e2e8f0;
    --shadow: 0 2px 12px rgba(0,0,0,0.08);
    --s1-accent: #3B82F6; --s1-bg: #EFF6FF;
    --s2-accent: #EF4444; --s2-bg: #FEF2F2;
    --yellow: #F59E0B; --s3-yellow-bg: #FFFBEB;
    --s3-accent: #10B981; --s3-bg: #ECFDF5;
    --s4-accent: #8B5CF6; --s4-bg: #F5F3FF;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html { scroll-behavior: smooth; scroll-snap-type: y mandatory; }
  body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; color: var(--text); background: #f8fafc; }

  .slide {
    min-height: 100vh; scroll-snap-align: start;
    display: flex; flex-direction: column;
    padding: 40px 60px;
  }
  .slide-header {
    display: flex; align-items: baseline; gap: 16px;
    margin-bottom: 28px; padding-bottom: 16px;
    border-bottom: 2px solid var(--border);
  }
  .slide-num { font-size: 13px; font-weight: 700; color: var(--muted); letter-spacing: 1px; }
  .slide-title { font-size: 28px; font-weight: 700; }
  .slide-subtitle { font-size: 14px; color: var(--muted); margin-left: auto; }

  .kpi-row { display: flex; gap: 20px; margin-bottom: 28px; }
  .kpi-card {
    flex: 1; border-radius: 12px; padding: 20px 24px;
    box-shadow: var(--shadow); text-align: center;
    background: #fff; border-top: 4px solid var(--muted);
  }
  .kpi-card.blue  { border-top-color: var(--s1-accent); background: var(--s1-bg); }
  .kpi-card.red   { border-top-color: var(--s2-accent); background: var(--s2-bg); }
  .kpi-card.green { border-top-color: var(--s3-accent); background: var(--s3-bg); }
  .kpi-card.yellow { border-top-color: var(--yellow); background: var(--s3-yellow-bg); }
  .kpi-label { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.8px; color: var(--muted); margin-bottom: 6px; }
  .kpi-value { font-size: 36px; font-weight: 700; line-height: 1.1; }

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

  .dot-nav {
    position: fixed; right: 24px; top: 50%; transform: translateY(-50%);
    display: flex; flex-direction: column; gap: 10px; z-index: 100;
  }
  .dot {
    width: 11px; height: 11px; border-radius: 50%;
    background: rgba(0,0,0,0.18); cursor: pointer; transition: all 0.2s;
  }
  .dot.active { transform: scale(1.4); }
  .dot[data-slide="0"].active { background: var(--s1-accent); }
  .dot[data-slide="1"].active { background: var(--s2-accent); }
  .dot[data-slide="2"].active { background: var(--s3-accent); }
  .dot[data-slide="3"].active { background: var(--s4-accent); }

  .trend { font-size: 13px; font-weight: 600; margin-left: 6px; }
  .trend.good { color: var(--green); }
  .trend.bad  { color: var(--red); }
  .trend.flat { color: var(--muted); }

  .reason-pill {
    display: inline-block; padding: 3px 10px; border-radius: 99px;
    font-size: 13px; font-weight: 600;
    background: var(--s2-bg); color: var(--s2-accent);
    border: 1px solid rgba(239,68,68,0.2);
  }
</style>
</head>
<body>

<nav class="dot-nav">
  <div class="dot active" data-slide="0" title="Customer Overview"></div>
  <div class="dot" data-slide="1" title="Churn Trend"></div>
  <div class="dot" data-slide="2" title="Sentiment Health"></div>
  <div class="dot" data-slide="3" title="CS Activity"></div>
</nav>

<!-- ═══════════ SLIDE 1 — CUSTOMER OVERVIEW ═══════════ -->
<section class="slide" id="slide0">
  <div class="slide-header">
    <span class="slide-num">01 / 04</span>
    <h1 class="slide-title" style="color:var(--s1-accent)">Customer Overview</h1>
    <span class="slide-subtitle">Active customers & MRR — weekly snapshot</span>
  </div>

  <div class="kpi-row">
    <div class="kpi-card blue">
      <div class="kpi-label">Active Customers</div>
      <div class="kpi-value" id="kpi-active" style="color:var(--s1-accent)">—</div>
    </div>
    <div class="kpi-card blue">
      <div class="kpi-label">Total MRR</div>
      <div class="kpi-value" id="kpi-mrr" style="color:var(--s1-accent)">—</div>
    </div>
    <div class="kpi-card blue">
      <div class="kpi-label">Net Change</div>
      <div class="kpi-value" id="kpi-net" style="color:var(--s1-accent);font-size:24px;">—</div>
    </div>
  </div>

  <div class="chart-row">
    <div class="card">
      <div class="card-title">Active Customers — Weekly</div>
      <div class="chart-box"><canvas id="chart-active"></canvas></div>
    </div>
    <div class="card">
      <div class="card-title">Total MRR — Weekly</div>
      <div class="chart-box"><canvas id="chart-mrr"></canvas></div>
    </div>
  </div>
</section>

<!-- ═══════════ SLIDE 2 — CHURN TREND ═══════════ -->
<section class="slide" id="slide1">
  <div class="slide-header">
    <span class="slide-num">02 / 04</span>
    <h1 class="slide-title" style="color:var(--s2-accent)">Churn Trend</h1>
    <span class="slide-subtitle">New churns per week — count & MRR lost</span>
  </div>

  <div class="kpi-row">
    <div class="kpi-card red">
      <div class="kpi-label">Churns This Week</div>
      <div class="kpi-value" id="kpi-churns" style="color:var(--s2-accent)">—</div>
    </div>
    <div class="kpi-card red">
      <div class="kpi-label">MRR Lost</div>
      <div class="kpi-value" id="kpi-churn-mrr" style="color:var(--s2-accent)">—</div>
    </div>
    <div class="kpi-card red">
      <div class="kpi-label">Top Reason</div>
      <div class="kpi-value" id="kpi-reason" style="font-size:18px;">—</div>
    </div>
  </div>

  <div class="chart-row">
    <div class="card">
      <div class="card-title">New Churns — Weekly</div>
      <div class="chart-box"><canvas id="chart-churn-count"></canvas></div>
    </div>
    <div class="card">
      <div class="card-title">MRR Lost — Weekly</div>
      <div class="chart-box"><canvas id="chart-churn-mrr"></canvas></div>
    </div>
  </div>
</section>

<!-- ═══════════ SLIDE 3 — SENTIMENT HEALTH ═══════════ -->
<section class="slide" id="slide2">
  <div class="slide-header">
    <span class="slide-num">03 / 04</span>
    <h1 class="slide-title" style="color:var(--s3-accent)">Sentiment Health</h1>
    <span class="slide-subtitle">Customer sentiment — weekly snapshot</span>
  </div>

  <div class="kpi-row">
    <div class="kpi-card green">
      <div class="kpi-label">Great</div>
      <div class="kpi-value" id="kpi-great" style="color:var(--s3-accent)">—</div>
    </div>
    <div class="kpi-card yellow">
      <div class="kpi-label">Alright</div>
      <div class="kpi-value" id="kpi-alright" style="color:var(--yellow)">—</div>
    </div>
    <div class="kpi-card" style="border-top-color:var(--red);background:var(--s2-bg);">
      <div class="kpi-label">At Risk</div>
      <div class="kpi-value" id="kpi-risk" style="color:var(--red)">—</div>
    </div>
    <div class="kpi-card green">
      <div class="kpi-label">% Great</div>
      <div class="kpi-value" id="kpi-ratio-great" style="color:var(--s3-accent)">—</div>
    </div>
    <div class="kpi-card" style="border-top-color:var(--red);background:var(--s2-bg);">
      <div class="kpi-label">% At Risk</div>
      <div class="kpi-value" id="kpi-ratio-risk" style="color:var(--red)">—</div>
    </div>
  </div>

  <div class="chart-row">
    <div class="card">
      <div class="card-title">Sentiment Breakdown — Weekly Trend</div>
      <div class="chart-box"><canvas id="chart-sentiment"></canvas></div>
    </div>
  </div>
</section>

<!-- ═══════════ SLIDE 4 — CS ACTIVITY ═══════════ -->
<section class="slide" id="slide3">
  <div class="slide-header">
    <span class="slide-num">04 / 04</span>
    <h1 class="slide-title" style="color:var(--s4-accent)">CS Activity</h1>
    <span class="slide-subtitle">Customer meetings & issues reported — weekly</span>
  </div>

  <div class="kpi-row">
    <div class="kpi-card" style="border-top-color:var(--s4-accent);background:var(--s4-bg);">
      <div class="kpi-label">Meetings This Week</div>
      <div class="kpi-value" id="kpi-meetings" style="color:var(--s4-accent)">—</div>
    </div>
    <div class="kpi-card" style="border-top-color:var(--s4-accent);background:var(--s4-bg);">
      <div class="kpi-label">Issues This Week</div>
      <div class="kpi-value" id="kpi-issues" style="color:var(--s4-accent)">—</div>
    </div>
    <div class="kpi-card" style="border-top-color:var(--s4-accent);background:var(--s4-bg);">
      <div class="kpi-label">Issues / Meeting</div>
      <div class="kpi-value" id="kpi-ratio-mtg" style="color:var(--s4-accent);font-size:24px;">—</div>
    </div>
  </div>

  <div class="chart-row">
    <div class="card" style="flex:1;">
      <div class="card-title">Customer Meetings — Weekly <span style="font-weight:400;font-size:11px;margin-left:16px;"><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#7C3AED;vertical-align:middle;margin-right:3px;"></span><span style="color:#7C3AED;">Alex issues</span> <span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#D946EF;vertical-align:middle;margin-left:8px;margin-right:3px;"></span><span style="color:#D946EF;">Aya issues</span></span></div>
      <div class="chart-box"><canvas id="chart-meetings"></canvas></div>
    </div>
  </div>
</section>

<script>
const WEEKS = __WEEKS_JSON__;
const DATA = __DATA_JSON__;

Chart.register(ChartDataLabels);
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.padding = 16;

const d = DATA;
const N = WEEKS.length;

function highlight(color, alpha) {
  return WEEKS.map((_, i) => i === N - 1 ? color : alpha);
}

// ── KPIs: Slide 1 ──
document.getElementById('kpi-active').textContent = d.overview.currentActive.toLocaleString();
document.getElementById('kpi-mrr').textContent = '\u20ac' + d.overview.currentMrr.toLocaleString();

// Net change: compare last two snapshot values
(function() {
  var arr = d.overview.activeCount;
  var prev = null;
  for (var i = arr.length - 2; i >= 0; i--) { if (arr[i] != null) { prev = arr[i]; break; } }
  if (prev != null) {
    var diff = d.overview.currentActive - prev;
    var sign = diff >= 0 ? '+' : '';
    document.getElementById('kpi-net').textContent = sign + diff + ' customers';
  } else {
    document.getElementById('kpi-net').textContent = 'First week';
  }
})();

// ── KPIs: Slide 2 ──
var lastChurn = d.churn.churnCount[N - 1];
document.getElementById('kpi-churns').textContent = lastChurn;
document.getElementById('kpi-churn-mrr').textContent = '\u20ac' + d.churn.churnMrr[N - 1].toLocaleString();

(function() {
  var reasons = d.churn.currentWeekReasons;
  var topReason = null, topCount = 0;
  for (var r in reasons) {
    if (reasons[r] > topCount) { topReason = r; topCount = reasons[r]; }
  }
  if (topReason) {
    document.getElementById('kpi-reason').innerHTML = '<span class="reason-pill">' + topReason + '</span>';
  } else {
    document.getElementById('kpi-reason').innerHTML = '<span style="color:var(--muted);font-size:16px;">None</span>';
  }
})();

// ── KPIs: Slide 3 ──
document.getElementById('kpi-great').textContent = d.sentiment.currentGreat;
document.getElementById('kpi-alright').textContent = d.sentiment.currentAlright;
document.getElementById('kpi-risk').textContent = d.sentiment.currentAtRisk;
var total = d.sentiment.currentGreat + d.sentiment.currentAlright + d.sentiment.currentAtRisk;
var ratioGreat = total > 0 ? Math.round(d.sentiment.currentGreat / total * 100) : 0;
var ratioRisk = total > 0 ? Math.round(d.sentiment.currentAtRisk / total * 100) : 0;
document.getElementById('kpi-ratio-great').textContent = ratioGreat + '%';
document.getElementById('kpi-ratio-risk').textContent = ratioRisk + '%';

// ═══════════════════════════════════════════════════════════════════════
// CHARTS
// ═══════════════════════════════════════════════════════════════════════

// ── Slide 1: Two separate bar charts ──
new Chart(document.getElementById('chart-active'), {
  type: 'bar',
  data: {
    labels: WEEKS,
    datasets: [{
      label: 'Active Customers',
      data: d.overview.activeCount,
      backgroundColor: highlight('#3B82F6', 'rgba(59,130,246,0.35)'),
      borderRadius: 6,
    }]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    scales: {
      y: { beginAtZero: true, grace: '15%', grid: { color: '#f1f5f9' },
           ticks: { precision: 0 } },
      x: { grid: { display: false } }
    },
    plugins: {
      legend: { display: false },
      datalabels: {
        display: function(ctx) { return ctx.dataset.data[ctx.dataIndex] != null; },
        anchor: 'end', align: 'end', color: '#3B82F6', font: { size: 12, weight: 'bold' }
      }
    }
  }
});

new Chart(document.getElementById('chart-mrr'), {
  type: 'bar',
  data: {
    labels: WEEKS,
    datasets: [{
      label: 'MRR',
      data: d.overview.totalMrr,
      backgroundColor: highlight('#3B82F6', 'rgba(59,130,246,0.35)'),
      borderRadius: 6,
    }]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    scales: {
      y: { beginAtZero: true, grace: '15%', grid: { color: '#f1f5f9' },
           ticks: { callback: function(v) { return '\u20ac' + v.toLocaleString(); } } },
      x: { grid: { display: false } }
    },
    plugins: {
      legend: { display: false },
      datalabels: {
        display: function(ctx) { return ctx.dataset.data[ctx.dataIndex] != null; },
        anchor: 'end', align: 'end', color: '#3B82F6', font: { size: 12, weight: 'bold' },
        formatter: function(v) { return '\u20ac' + v.toLocaleString(); }
      }
    }
  }
});

// ── Slide 2: Two bar charts ──
function churnChart(canvasId, data, formatter) {
  new Chart(document.getElementById(canvasId), {
    type: 'bar',
    data: {
      labels: WEEKS,
      datasets: [{
        label: canvasId === 'chart-churn-count' ? 'Churns' : 'MRR Lost',
        data: data,
        backgroundColor: highlight('#EF4444', 'rgba(239,68,68,0.35)'),
        borderRadius: 6,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {
        y: { beginAtZero: true, grace: '15%', grid: { color: '#f1f5f9' } },
        x: { grid: { display: false } }
      },
      plugins: {
        legend: { display: false },
        datalabels: {
          anchor: 'end', align: 'end', color: '#EF4444',
          font: { size: 12, weight: 'bold' },
          formatter: formatter || function(v) { return v; }
        }
      }
    }
  });
}

churnChart('chart-churn-count', d.churn.churnCount);
churnChart('chart-churn-mrr', d.churn.churnMrr, function(v) { return '\u20ac' + v.toLocaleString(); });

// ── Slide 3: Stacked bar ──
new Chart(document.getElementById('chart-sentiment'), {
  type: 'bar',
  data: {
    labels: WEEKS,
    datasets: [
      {
        label: 'Great',
        data: d.sentiment.great,
        backgroundColor: '#34D399',
        borderRadius: 6,
      },
      {
        label: 'Alright',
        data: d.sentiment.alright,
        backgroundColor: '#FBBF24',
        borderRadius: 6,
      },
      {
        label: 'At Risk',
        data: d.sentiment.atRisk,
        backgroundColor: '#F87171',
        borderRadius: 6,
      }
    ]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    scales: {
      x: { stacked: true, grid: { display: false } },
      y: { stacked: true, beginAtZero: true, grid: { color: '#f1f5f9' } }
    },
    plugins: {
      legend: { display: true },
      datalabels: {
        display: function(ctx) { return ctx.dataset.data[ctx.dataIndex] != null && ctx.dataset.data[ctx.dataIndex] > 0; },
        color: '#fff', font: { size: 13, weight: 'bold' }
      }
    }
  }
});

// ── Slide 4: CS Activity ──
var mtgTotal = d.meetings.total[N - 1] || 0;
var issTotal = d.meetingIssues.total[N - 1] || 0;
document.getElementById('kpi-meetings').textContent = mtgTotal;
document.getElementById('kpi-issues').textContent = issTotal;
document.getElementById('kpi-ratio-mtg').textContent = mtgTotal > 0 ? (issTotal / mtgTotal).toFixed(1) : '—';

// Custom plugin: draw issue-count bubbles in a fixed row above bars
var issueBubblePlugin = {
  id: 'issueBubbles',
  afterDatasetsDraw: function(chart) {
    var ctx = chart.ctx;
    var issuesData = [d.meetingIssues.alex, d.meetingIssues.aya];
    var colors = ['#7C3AED', '#D946EF']; // purple/pink — distinct from bar colors
    var chartArea = chart.chartArea;
    var bubbleY = chartArea.top + 14; // fixed row near top of chart
    var r = 13;

    chart.data.datasets.forEach(function(ds, dsIdx) {
      if (dsIdx > 1) return;
      var meta = chart.getDatasetMeta(dsIdx);
      meta.data.forEach(function(bar, i) {
        var issues = issuesData[dsIdx][i] || 0;
        if (issues <= 0) return;
        var x = bar.x;
        ctx.save();
        ctx.beginPath();
        ctx.arc(x, bubbleY, r, 0, Math.PI * 2);
        ctx.fillStyle = colors[dsIdx];
        ctx.fill();
        ctx.strokeStyle = '#fff';
        ctx.lineWidth = 2;
        ctx.stroke();
        ctx.fillStyle = '#fff';
        ctx.font = 'bold 10px Inter, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(issues, x, bubbleY);
        ctx.restore();
      });
    });
  }
};

new Chart(document.getElementById('chart-meetings'), {
  type: 'bar',
  data: {
    labels: WEEKS,
    datasets: [
      { label: 'Alex', data: d.meetings.alex, backgroundColor: '#3B82F6', borderRadius: 6 },
      { label: 'Aya', data: d.meetings.aya, backgroundColor: '#14B8A6', borderRadius: 6 }
    ]
  },
  options: {
    responsive: true, maintainAspectRatio: false,
    scales: {
      y: { beginAtZero: true, grace: '20%', grid: { color: '#f1f5f9' }, ticks: { precision: 0 } },
      x: { grid: { display: false } }
    },
    plugins: {
      legend: { display: true },
      datalabels: {
        display: function(ctx) { return ctx.dataset.data[ctx.dataIndex] > 0; },
        anchor: 'end', align: 'end', font: { size: 11, weight: 'bold' },
        color: function(ctx) { return ctx.datasetIndex === 0 ? '#3B82F6' : '#14B8A6'; }
      }
    }
  },
  plugins: [issueBubblePlugin]
});

// ═══════════════════════════════════════════════════════════════════════
// NAVIGATION
// ═══════════════════════════════════════════════════════════════════════
var slides = ['slide0', 'slide1', 'slide2', 'slide3'];
var dots = document.querySelectorAll('.dot');
var currentSlide = 0;

function activateDot(idx) {
  dots.forEach(function(d) { d.classList.remove('active'); });
  dots[idx].classList.add('active');
  currentSlide = idx;
}

var obs = new IntersectionObserver(function(entries) {
  entries.forEach(function(e) {
    if (e.isIntersecting) {
      var idx = slides.indexOf(e.target.id);
      if (idx !== -1) activateDot(idx);
    }
  });
}, { threshold: 0.5 });
slides.forEach(function(id) { obs.observe(document.getElementById(id)); });

dots.forEach(function(dot, i) {
  dot.addEventListener('click', function() {
    document.getElementById(slides[i]).scrollIntoView({ behavior: 'smooth' });
  });
});

document.addEventListener('keydown', function(e) {
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


def main():
    print("=" * 60)
    print("  CEO Dashboard \u2014 Generate HTML")
    print("=" * 60)

    if not DATA_FILE.exists():
        print(f"\u2717 {DATA_FILE} not found \u2014 run fetch_ceo_dashboard_data.py first")
        sys.exit(1)

    with open(DATA_FILE) as f:
        raw = json.load(f)

    print(f"\n  Data from: {raw['fetched_at']}")
    print(f"  Weeks: {raw['week_labels']}")

    data_js = {
        "overview": {
            "activeCount": raw["overview"]["active_count"],
            "totalMrr": raw["overview"]["total_mrr"],
            "currentActive": raw["overview"]["current_active"],
            "currentMrr": raw["overview"]["current_mrr"],
        },
        "churn": {
            "churnCount": raw["churn"]["churn_count"],
            "churnMrr": raw["churn"]["churn_mrr"],
            "currentWeekReasons": raw["churn"]["current_week_reasons"],
        },
        "sentiment": {
            "great": raw["sentiment"]["great"],
            "alright": raw["sentiment"]["alright"],
            "atRisk": raw["sentiment"]["at_risk"],
            "currentGreat": raw["sentiment"]["current_great"],
            "currentAlright": raw["sentiment"]["current_alright"],
            "currentAtRisk": raw["sentiment"]["current_at_risk"],
        },
        "meetings": raw.get("meetings", {"alex": [], "aya": [], "total": []}),
        "meetingIssues": raw.get("meeting_issues", {"alex": [], "aya": [], "total": []}),
    }

    weeks_json = json.dumps(raw["week_labels"])
    data_json = json.dumps(data_js, indent=2)

    html = HTML_TEMPLATE.replace("__WEEKS_JSON__", weeks_json)
    html = html.replace("__DATA_JSON__", data_json)

    HTML_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HTML_FILE, "w") as f:
        f.write(html)
    print(f"\n  \u2713 HTML written to {HTML_FILE}")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    current_week = raw["weeks"][-1]
    start = datetime.strptime(current_week["start"], "%Y-%m-%d")
    end = datetime.strptime(current_week["end"], "%Y-%m-%d")

    if start.month == end.month:
        archive_name = f"CEO_dashboard_{start.strftime('%b')}{start.day}-{end.day}.html"
    else:
        archive_name = f"CEO_dashboard_{start.strftime('%b')}{start.day}-{end.strftime('%b')}{end.day}.html"

    archive_path = REPORTS_DIR / archive_name
    shutil.copy2(HTML_FILE, archive_path)
    print(f"  \u2713 Archived to {archive_path}")
    print(f"\n\u2705 Done")


if __name__ == "__main__":
    main()
