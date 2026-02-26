"""
generate_meeting_report.py

Generates meeting_report.html — a single-page presentation for the biweekly
DAGs & Churn meeting.

Reads live data from report_data.json (written by fetch_report_data.py).
Falls back to hardcoded mock data if report_data.json is not present.

Navigate with arrow keys or click the dot nav. Best viewed fullscreen (F11).
"""

import json
import os

OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "meeting_report.html")
DATA_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report_data.json")

# ──────────────────────────────────────────────────────────────────────────────
# LOAD LIVE DATA  (falls back to mock if report_data.json is missing)
# ──────────────────────────────────────────────────────────────────────────────

_MOCK_KEY_TAKEAWAYS = [
    {"category": "Platform & UI", "count": 0, "color": "#34D399",
     "themes": ["No data — run fetch_report_data.py"]},
]

def _load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, encoding="utf-8") as f:
            return json.load(f)
    return None

_data = _load_data()

if _data:
    _source = "report_data.json"
    PERIODS               = _data["periods"]
    BUG_VOLUME            = _data["bug_volume"]
    TARGET_LINE           = _data["target_line"]
    BUG_SOURCE_INTERCOM   = _data["bug_source_intercom"]
    BUG_SOURCE_MEETINGS   = _data["bug_source_meetings"]
    BUG_TYPES             = _data["bug_types"]
    BUG_TYPE_NAMES        = _data["bug_type_names"]
    KEY_TAKEAWAYS_S2      = _data.get("key_takeaways_s2") or _MOCK_KEY_TAKEAWAYS
    RESOLUTION_BY_PERIOD  = _data["resolution_by_period"]
    RESOLUTION_RATES      = _data["resolution_rates"]
    COMM_RATE_TREND       = _data["comm_rate_trend"]
    FLAGGED_CUSTOMERS     = _data["flagged_customers"]
    CHURN_MRR_BY_PERIOD   = _data["churn_mrr_by_period"]
    CHURNED               = _data["churned"]
    TOP_CUSTOMERS_BY_ISSUES = _data["top_customers_by_issues"]
    TOTAL_IN_SCOPE_ISSUES = _data.get("total_in_scope_issues", sum(c["issues"] for c in _data["top_customers_by_issues"]))
    CUSTOMERS_COUNT_S6    = _data.get("customers_count", len(set()))
    CURRENT_PERIOD        = _data["current_period"]
else:
    _source = "mock data"
    PERIODS = ["P1 (Feb 2–15)", "P2 (Feb 16–Mar 1)", "P3 (Mar 2–15)"]

    BUG_VOLUME = [28, 24, 21]
    TARGET_LINE = [28, round(28 * 0.85, 1), round(28 * 0.85 ** 2, 1)]

    BUG_SOURCE_INTERCOM = [18, 15, 13]
    BUG_SOURCE_MEETINGS = [10,  9,  8]

    BUG_TYPES = [
        [10, 6, 7, 3, 2],
        [9,  5, 5, 3, 2],
        [9,  5, 3, 2, 2],
    ]
    BUG_TYPE_NAMES = ["Feature request", "AI Behavior", "Integration", "Platform & UI", "Billing & Account"]

    KEY_TAKEAWAYS_S2 = _MOCK_KEY_TAKEAWAYS

    RESOLUTION_BY_PERIOD = [
        {"Open": 10, "In Progress": 8,  "Resolved": 10},
        {"Open":  9, "In Progress": 5,  "Resolved": 10},
        {"Open":  8, "In Progress": 6,  "Resolved": 10},
    ]

    RESOLUTION_RATES = [
        [25,   40,   60,   None],
        [37,   60,   70,   None],
        [50,   80,   None, None],
    ]

    COMM_RATE_TREND = [
        {"period": "P1", "resolved": 20, "informed": 13, "rate": 65},
        {"period": "P2", "resolved": 22, "informed": 15, "rate": 68},
        {"period": "P3", "resolved": 24, "informed": 18, "rate": 75},
    ]

    FLAGGED_CUSTOMERS = [
        {"Customer": "Acme Corp",    "Days waiting": 13, "Open issues": 4, "Flag": "Both"},
        {"Customer": "Globex Inc",   "Days waiting": 11, "Open issues": 2, "Flag": "Wait time"},
        {"Customer": "Initech",      "Days waiting":  6, "Open issues": 5, "Flag": "Open issues"},
        {"Customer": "Pawnee Parks", "Days waiting":  3, "Open issues": 4, "Flag": "Open issues"},
    ]

    CHURN_MRR_BY_PERIOD = {
        "Globex":   [1500,    0,    0],
        "Initech":  [ 900,    0,    0],
        "Brawndo":  [ 600,    0,    0],
        "Vandelay": [   0, 1200,    0],
        "Prestige": [   0,  800,    0],
        "Umbrella": [   0,    0, 1100],
        "Sabre":    [   0,    0,  900],
    }

    CHURNED = [
        {"Customer": "Globex",   "MRR": "$1,500", "Period": "P1", "Reason": "Performance"},
        {"Customer": "Initech",  "MRR": "$900",   "Period": "P1", "Reason": "Platform"},
        {"Customer": "Brawndo",  "MRR": "$600",   "Period": "P1", "Reason": "AI behavior"},
        {"Customer": "Vandelay", "MRR": "$1,200", "Period": "P2", "Reason": "Performance"},
        {"Customer": "Prestige", "MRR": "$800",   "Period": "P2", "Reason": "AI behavior"},
        {"Customer": "Umbrella", "MRR": "$1,100", "Period": "P3", "Reason": "Platform"},
        {"Customer": "Sabre",    "MRR": "$900",   "Period": "P3", "Reason": "Performance"},
    ]

    TOP_CUSTOMERS_BY_ISSUES = [
        {"customer": "Acme Corp",      "issues": 12, "bugs": 8,  "features": 4},
        {"customer": "Pawnee Parks",   "issues":  9, "bugs": 7,  "features": 2},
        {"customer": "Globex Inc",     "issues":  8, "bugs": 5,  "features": 3},
        {"customer": "Initech",        "issues":  7, "bugs": 6,  "features": 1},
        {"customer": "Bluth Company",  "issues":  6, "bugs": 4,  "features": 2},
        {"customer": "Dunder Mifflin", "issues":  5, "bugs": 3,  "features": 2},
        {"customer": "Vandelay Ind.",  "issues":  4, "bugs": 4,  "features": 0},
        {"customer": "Umbrella Co",    "issues":  4, "bugs": 2,  "features": 2},
        {"customer": "Sabre Corp",     "issues":  3, "bugs": 2,  "features": 1},
        {"customer": "Wayne Ent.",     "issues":  2, "bugs": 1,  "features": 1},
    ]

    TOTAL_IN_SCOPE_ISSUES = sum(c["issues"] for c in TOP_CUSTOMERS_BY_ISSUES)
    CUSTOMERS_COUNT_S6    = len(TOP_CUSTOMERS_BY_ISSUES)
    CURRENT_PERIOD = "P3"

print(f"📊 Rendering from: {_source}")

CHURNED_THIS_PERIOD = [c for c in CHURNED if c["Period"] == CURRENT_PERIOD]

# ── Slide 5: current-period-only KPIs ──
CHURNED_COUNT_THIS_PERIOD = len(CHURNED_THIS_PERIOD)
TOTAL_MRR_THIS_PERIOD     = sum(c.get("mrr_raw", 0) for c in CHURNED_THIS_PERIOD)

# Group by churn reason (current period), sorted by MRR desc
_reason_agg = {}
for c in CHURNED_THIS_PERIOD:
    r = c.get("Reason") or "Unknown"
    if r not in _reason_agg:
        _reason_agg[r] = {"reason": r, "mrr": 0, "customers": []}
    _reason_agg[r]["mrr"] += c.get("mrr_raw", 0)
    _reason_agg[r]["customers"].append(c["Customer"])
CHURN_BY_REASON = sorted(_reason_agg.values(), key=lambda x: -x["mrr"])

# Computed values — slide 6
CHURNED_COUNT     = CHURNED_COUNT_THIS_PERIOD
TOTAL_ISSUES_S6   = TOTAL_IN_SCOPE_ISSUES
# Issues that actually appear in the tick chart (have a customer linked)
LINKED_ISSUES_S6  = sum(c["issues"] for c in TOP_CUSTOMERS_BY_ISSUES)
CUSTOMERS_5PLUS   = sum(1 for c in TOP_CUSTOMERS_BY_ISSUES if c["issues"] >= 5)
TOP10_PCT         = (round(sum(c["issues"] for c in TOP_CUSTOMERS_BY_ISSUES[:10])
                            / TOTAL_ISSUES_S6 * 100)
                     if TOTAL_ISSUES_S6 > 0 else 0)

# ── Slide 1 KPI cards (computed from live data) ──
_PERIOD_LABELS = [p.split(" ")[0] for p in PERIODS]   # ["P1", "P2", "P3"]
_CUR_IDX  = _PERIOD_LABELS.index(CURRENT_PERIOD) if CURRENT_PERIOD in _PERIOD_LABELS else len(PERIODS) - 1
_PREV_IDX = max(_CUR_IDX - 1, 0)

# Long-form current period label: "P1 (Feb 16 – Mar 1)" → "Period 1 (Feb 16 – Mar 1)"
_PERIOD_NUM_MAP  = {"P1": "Period 1", "P2": "Period 2", "P3": "Period 3"}
_CUR_PERIOD_LONG = PERIODS[_CUR_IDX].replace(CURRENT_PERIOD, _PERIOD_NUM_MAP.get(CURRENT_PERIOD, CURRENT_PERIOD))

KPI_BUGS_THIS_PERIOD = BUG_VOLUME[_CUR_IDX]
KPI_PERIOD_LABEL     = PERIODS[_CUR_IDX]
KPI_PREV_LABEL       = _PERIOD_LABELS[_PREV_IDX]

_prev_vol = BUG_VOLUME[_PREV_IDX]
if _prev_vol > 0 and _CUR_IDX != _PREV_IDX:
    _pop_raw = (_CUR_IDX != _PREV_IDX and
                round((KPI_BUGS_THIS_PERIOD - _prev_vol) / _prev_vol * 100, 1))
    KPI_POP_CHANGE_STR = (f"+{_pop_raw}%" if _pop_raw > 0
                          else f"{_pop_raw}%") if _pop_raw is not False else "—"
    _pop_color = "kpi-green" if _pop_raw <= 0 else "kpi-red"
else:
    KPI_POP_CHANGE_STR, _pop_color = "—", "kpi-blue"

_p1_vol = BUG_VOLUME[0]
if _p1_vol > 0 and _CUR_IDX > 0:
    _cum_raw = round((KPI_BUGS_THIS_PERIOD - _p1_vol) / _p1_vol * 100, 1)
    KPI_CUM_CHANGE_STR = (f"+{_cum_raw}%" if _cum_raw > 0 else f"{_cum_raw}%")
    _cum_color = "kpi-green" if _cum_raw <= 0 else "kpi-red"
else:
    KPI_CUM_CHANGE_STR, _cum_color = "—", "kpi-blue"

# ── Slide 2 mini-KPI cards (top 3 categories by current-period volume) ──
def _cat_kpi(cat_name, color_var):
    """Return HTML for one slide-2 mini-KPI card showing P1→current trend."""
    if cat_name not in BUG_TYPE_NAMES:
        return ""
    idx   = BUG_TYPE_NAMES.index(cat_name)
    p1_n  = BUG_TYPES[0][idx]
    cur_n = BUG_TYPES[_CUR_IDX][idx]
    if p1_n > 0:
        pct   = round((cur_n - p1_n) / p1_n * 100)
        pct_s = (f"+{pct}%" if pct > 0 else f"{pct}%")
        clr   = "kpi-green" if pct <= 0 else "kpi-red"
        sub   = f"{p1_n} → {cur_n} ({_PERIOD_LABELS[0]} → {CURRENT_PERIOD})"
    else:
        pct_s, clr = f"{cur_n}", "kpi-blue"
        sub = f"{cur_n} issues this period"
    return (
        f'<div class="kpi-card compact">'
        f'<div class="kpi-label" style="display:flex;align-items:center;gap:6px;">'
        f'<span class="legend-dot" style="background:{color_var};"></span>{cat_name}'
        f'</div>'
        f'<div class="kpi-value {clr}" style="font-size:28px;">{pct_s}</div>'
        f'<div class="kpi-sub">{sub}</div>'
        f'</div>'
    )

# Pick the 3 categories with the most bugs in the current period
_sorted_cats = sorted(
    zip(BUG_TYPE_NAMES, [BUG_TYPES[_CUR_IDX][i] for i in range(len(BUG_TYPE_NAMES))]),
    key=lambda x: -x[1]
)
_CSS_VARS = ["var(--c1)", "var(--c2)", "var(--c3)", "var(--c4)", "var(--c5)"]
SLIDE2_MINI_KPIS = "".join(
    _cat_kpi(name, _CSS_VARS[BUG_TYPE_NAMES.index(name)])
    for name, _ in _sorted_cats[:3]
)


# ──────────────────────────────────────────────────────────────────────────────
# RENDER HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _rate_badge(value):
    if value is None:
        return '<span style="color:var(--muted);">—</span>'
    elif value >= 60:
        return f'<span class="badge badge-green">{value}%</span>'
    elif value >= 40:
        return f'<span class="badge badge-yellow">{value}%</span>'
    else:
        return f'<span class="badge badge-red">{value}%</span>'


def render_churned_rows():
    reason_badge = {
        "Performance": "badge-orange",
        "Platform":    "badge-blue",
        "AI behavior": "badge-red",
    }
    rows = []
    for c in CHURNED_THIS_PERIOD:
        bc = reason_badge.get(c["Reason"], "badge-blue")
        rows.append(
            f'<tr><td>{c["Customer"]}</td><td>{c["MRR"]}</td>'
            f'<td><span class="badge {bc}">{c["Reason"]}</span></td></tr>'
        )
    return "\n".join(rows)


def render_resolution_rate_table():
    headers = ["Period", "7d", "14d", "28d", "Current"]
    header_html = "".join(f"<th>{h}</th>" for h in headers)
    rows = []
    for i, rates in enumerate(RESOLUTION_RATES):
        period_label = PERIODS[i].split(" ")[0]
        cells = f"<td><strong>{period_label}</strong></td>"
        for r in rates:
            cells += f"<td style='text-align:center;'>{_rate_badge(r)}</td>"
        rows.append(f"<tr>{cells}</tr>")
    return f"<thead><tr>{header_html}</tr></thead><tbody>{''.join(rows)}</tbody>"


def render_key_takeaways():
    # KEY_TAKEAWAYS_S2 is a list of category blocks:
    # [{"category": str, "count": int, "color": hex, "themes": [str, ...]}, ...]
    blocks = []
    for blk in KEY_TAKEAWAYS_S2:
        cat   = blk.get("category", "")
        count = blk.get("count", 0)
        color = blk.get("color", "#94A3B8")
        themes = blk.get("themes", [])
        theme_items = "".join(f"<li>{t}</li>" for t in themes)
        blocks.append(
            f'<div class="takeaway-block">'
            f'<div class="takeaway-cat-header">'
            f'<span class="takeaway-dot" style="background:{color};"></span>'
            f'<strong>{cat}</strong>'
            f'<span class="takeaway-count">{count} issues</span>'
            f'</div>'
            f'<ul class="takeaway-theme-list">{theme_items}</ul>'
            f'</div>'
        )
    return "".join(blocks)


def render_all_churned_summary():
    reason_badge = {
        "Performance": "badge-orange",
        "Platform":    "badge-blue",
        "AI behavior": "badge-red",
    }
    rows = []
    for c in CHURNED:
        bc = reason_badge.get(c["Reason"], "badge-blue")
        rows.append(
            f'<tr><td>{c["Customer"]}</td>'
            f'<td style="color:var(--muted);">{c["Period"]}</td>'
            f'<td><span class="badge {bc}">{c["Reason"]}</span></td></tr>'
        )
    return "\n".join(rows)


def render_comm_rate_cards():
    cards = []
    for c in COMM_RATE_TREND:
        rate = c["rate"]
        if rate >= 75:
            val_html = f'<div class="kpi-value kpi-green">{rate}%</div>'
        elif rate >= 60:
            val_html = f'<div class="kpi-value" style="color:var(--yellow);">{rate}%</div>'
        else:
            val_html = f'<div class="kpi-value kpi-red">{rate}%</div>'
        cards.append(
            f'<div class="kpi-card compact">'
            f'<div class="kpi-label">{c["period"]}</div>'
            f'{val_html}'
            f'<div class="kpi-sub">{c["informed"]}/{c["resolved"]} informed</div>'
            f'</div>'
        )
    return "\n".join(cards)


CHURN_REASON_ORDER = [
    "Missing features", "AI Behavior", "Platform & UI", "Integration", "Competitor", "Unknown"
]


def render_churn_reason_table():
    reason_lookup = {r["reason"]: r for r in CHURN_BY_REASON}
    rows = []
    for reason in CHURN_REASON_ORDER:
        d = reason_lookup.get(reason)
        if d is None:
            rows.append(
                f'<tr>'
                f'<td>{reason}</td>'
                f'<td style="text-align:center;color:var(--muted);">—</td>'
                f'<td style="text-align:right;color:var(--muted);">—</td>'
                f'</tr>'
            )
        else:
            mrr_fmt = f'${d["mrr"]:,.0f}' if d["mrr"] else '—'
            rows.append(
                f'<tr>'
                f'<td>{reason}</td>'
                f'<td style="text-align:center;">{len(d["customers"])}</td>'
                f'<td style="text-align:right;">{mrr_fmt}</td>'
                f'</tr>'
            )
    # Catch-all: reasons not in the predefined list
    for r in CHURN_BY_REASON:
        if r["reason"] not in CHURN_REASON_ORDER:
            rows.append(
                f'<tr>'
                f'<td>{r["reason"]}</td>'
                f'<td style="text-align:center;">{len(r["customers"])}</td>'
                f'<td style="text-align:right;">${r["mrr"]:,.0f}</td>'
                f'</tr>'
            )
    header = (
        '<thead><tr>'
        '<th>Reason</th>'
        '<th style="text-align:center;"># Customers</th>'
        '<th style="text-align:right;">Total MRR</th>'
        '</tr></thead>'
    )
    return header + '<tbody>' + ''.join(rows) + '</tbody>'


def render_tick_chart():
    rows = []
    for c in TOP_CUSTOMERS_BY_ISSUES:
        bug_ticks  = '<div class="tick tick-bug"  title="Bug"></div>'  * c["bugs"]
        feat_ticks = '<div class="tick tick-feat" title="Feature request"></div>' * c["features"]
        rows.append(
            f'<div class="tick-row">'
            f'<div class="tick-label">{c["customer"]}</div>'
            f'<div class="tick-bar">{bug_ticks}{feat_ticks}</div>'
            f'<div class="tick-total">{c["issues"]}</div>'
            f'</div>'
        )
    return "\n".join(rows)


def render_flagged_customers():
    rows = []
    badge_class = {
        "Both":        "badge-red",
        "Wait time":   "badge-orange",
        "Open issues": "badge-yellow",
    }
    for c in FLAGGED_CUSTOMERS:
        bc = badge_class.get(c["Flag"], "badge-blue")
        rows.append(
            f'<tr><td>{c["Customer"]}</td>'
            f'<td>{c["Days waiting"]}d</td>'
            f'<td>{c["Open issues"]}</td>'
            f'<td><span class="badge {bc}">{c["Flag"]}</span></td></tr>'
        )
    return "\n".join(rows)


# ──────────────────────────────────────────────────────────────────────────────
# HTML TEMPLATE
# ──────────────────────────────────────────────────────────────────────────────

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>DAGs &amp; Churn — Biweekly Meeting</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"/>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
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

    /* bug-type palette */
    --c1: #4F8EF7;
    --c2: #F87171;
    --c3: #A78BFA;
    --c4: #34D399;
    --c5: #FBBF24;
    --c6: #94A3B8;
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

  .row {{ display: flex; gap: 20px; align-items: stretch; }}
  .col {{ flex: 1; }}

  /* ── KPI CARD ────────────────────────────────────────────────────── */
  .kpi-grid {{ display: flex; gap: 16px; }}
  .kpi-card {{
    flex: 1; background: var(--card); border-radius: 12px; box-shadow: var(--shadow);
    padding: 20px 22px;
  }}
  .kpi-label {{ font-size: 13px; color: var(--muted); font-weight: 500; margin-bottom: 8px; }}
  .kpi-value {{ font-size: 36px; font-weight: 800; color: var(--navy); line-height: 1; }}
  .kpi-sub {{ font-size: 13px; color: var(--muted); margin-top: 4px; }}
  .kpi-green {{ color: var(--green) !important; }}
  .kpi-blue  {{ color: var(--blue)  !important; }}
  .kpi-red   {{ color: var(--red)   !important; }}

  /* ── COMPACT KPI (slide 2) ────────────────────────────────────────── */
  .kpi-card.compact .kpi-value {{ font-size: 28px; }}

  /* ── COMM RATE GRID (slide 4) ─────────────────────────────────────── */
  .comm-rate-grid {{ display: flex; gap: 16px; }}

  /* ── TABLES ──────────────────────────────────────────────────────── */
  table {{ width: 100%; border-collapse: collapse; font-size: 15px; }}
  th    {{ text-align: left; font-size: 12px; font-weight: 600; color: var(--muted);
           text-transform: uppercase; letter-spacing: .6px; padding: 9px 12px; border-bottom: 2px solid var(--border); }}
  td    {{ padding: 10px 12px; border-bottom: 1px solid var(--border); }}
  tr:last-child td {{ border-bottom: none; }}
  tr.table-flag {{ background: #FEF2F2; }}
  tr.table-flag td:first-child {{ border-left: 3px solid var(--red); }}

  /* ── BADGES ──────────────────────────────────────────────────────── */
  .badge {{ display: inline-block; padding: 3px 11px; border-radius: 20px; font-size: 13px; font-weight: 600; }}
  .badge-red    {{ background: #FEE2E2; color: #B91C1C; }}
  .badge-orange {{ background: #FEF3C7; color: #92400E; }}
  .badge-yellow {{ background: #FEF9C3; color: #713F12; }}
  .badge-green  {{ background: #D1FAE5; color: #065F46; }}
  .badge-blue   {{ background: #DBEAFE; color: #1D4ED8; }}

  /* ── CHART CONTAINERS ────────────────────────────────────────────── */
  .chart-box {{ position: relative; }}
  .chart-box canvas {{ max-height: 280px; }}

  /* ── KEY TAKEAWAYS (slide 2) ─────────────────────────────────────── */
  .takeaway-block {{
    margin-bottom: 14px;
  }}
  .takeaway-cat-header {{
    display: flex; align-items: center; gap: 8px;
    font-size: 14px; font-weight: 700; color: var(--text);
    margin-bottom: 6px;
  }}
  .takeaway-dot {{
    width: 11px; height: 11px; border-radius: 50%; flex-shrink: 0;
  }}
  .takeaway-count {{
    font-size: 12px; font-weight: 500; color: var(--muted);
    margin-left: auto;
  }}
  .takeaway-theme-list {{
    list-style: none; padding: 0;
    display: flex; flex-direction: column; gap: 5px; margin-left: 19px;
  }}
  .takeaway-theme-list li {{
    padding: 7px 12px; background: #f8fafc; border-radius: 6px;
    border-left: 3px solid var(--border); font-size: 13px; color: var(--text); line-height: 1.5;
  }}

  /* ── SLIDE 5 SUMMARY CARDS ───────────────────────────────────────── */
  .summary-grid {{ display: flex; gap: 16px; }}
  .summary-card {{
    flex: 1; border-radius: 12px; box-shadow: var(--shadow);
    padding: 22px 24px; display: flex; flex-direction: column; gap: 8px;
  }}
  .summary-card.red-card   {{ background: #FEF2F2; border: 1.5px solid #FECACA; }}
  .summary-card.amber-card {{ background: #FFFBEB; border: 1.5px solid #FDE68A; }}
  .summary-number {{ font-size: 52px; font-weight: 800; line-height: 1; }}
  .summary-label  {{ font-size: 15px; font-weight: 600; color: var(--muted); }}

  /* ── LEGEND HELPERS ──────────────────────────────────────────────── */
  .legend-dot  {{ width: 11px; height: 11px; border-radius: 50%; display: inline-block; margin-right: 6px; }}
  .legend-item {{ font-size: 14px; color: var(--muted); display: flex; align-items: center; }}

  /* ── TICK CHART (slide 6) ─────────────────────────────────────────── */
  .tick-chart  {{ display: flex; flex-direction: column; gap: 12px; padding: 4px 0; }}
  .tick-row    {{ display: flex; align-items: center; gap: 14px; }}
  .tick-label  {{ width: 190px; font-size: 14px; font-weight: 500; color: var(--text);
                  text-align: right; flex-shrink: 0;
                  white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
  .tick-bar    {{ display: flex; gap: 4px; flex: 1; align-items: center; flex-wrap: wrap; }}
  .tick        {{ width: 16px; height: 28px; border-radius: 3px; }}
  .tick-bug    {{ background: var(--red); }}
  .tick-feat   {{ background: var(--blue); }}
  .tick-total  {{ font-size: 13px; color: var(--muted); width: 28px; text-align: right; flex-shrink: 0; }}
</style>
</head>
<body>

<!-- ── DOT NAV ─────────────────────────────────────────────────────────── -->
<nav id="dot-nav">
  <div class="dot active" data-target="slide1" title="Bug Volume"></div>
  <div class="dot"        data-target="slide2" title="Categorization"></div>
  <div class="dot"        data-target="slide3" title="Resolution"></div>
  <div class="dot"        data-target="slide4" title="Comms Loop"></div>
  <div class="dot"        data-target="slide5" title="Churns"></div>
  <div class="dot"        data-target="slide6" title="Top Customers"></div>
</nav>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 1 — Bug Volume & Trend
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide1">
  <header class="slide-header">
    <span class="slide-num">01 / 06</span>
    <h1 class="slide-title">Bug Volume &amp; Trend</h1>
    <span class="slide-subtitle">Target: −15% per period</span>
  </header>

  <div class="slide-body">
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Bugs this period</div>
        <div class="kpi-value kpi-blue">{KPI_BUGS_THIS_PERIOD}</div>
        <div class="kpi-sub">{KPI_PERIOD_LABEL}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Period-over-period change</div>
        <div class="kpi-value {_pop_color}">{KPI_POP_CHANGE_STR}</div>
        <div class="kpi-sub">vs {KPI_PREV_LABEL} (target −15%)</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Cumulative change</div>
        <div class="kpi-value {_cum_color}">{KPI_CUM_CHANGE_STR}</div>
        <div class="kpi-sub">P1 → {CURRENT_PERIOD}</div>
      </div>
    </div>

    <div class="card" style="flex:1; margin-top:20px;">
      <div class="card-title">Bugs received per period — Intercom vs Meetings vs −15% target</div>
      <div class="chart-box">
        <canvas id="chart-volume"></canvas>
      </div>
    </div>
  </div>
</section>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 2 — Bug Categorization
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide2">
  <header class="slide-header">
    <span class="slide-num">02 / 06</span>
    <h1 class="slide-title">Bug Categorization</h1>
    <span class="slide-subtitle">Type breakdown over time</span>
  </header>

  <div class="slide-body">
    <div class="row" style="flex:1; gap:20px;">
      <div style="flex:2; display:flex; flex-direction:column; gap:16px;">
        <div class="card" style="flex:1;">
          <div class="card-title">Bug types per period (stacked)</div>
          <div class="chart-box">
            <canvas id="chart-stacked" style="max-height:360px;"></canvas>
          </div>
        </div>
        <div class="kpi-grid">
          {SLIDE2_MINI_KPIS}
        </div>
      </div>
      <div style="flex:1;">
        <div class="card" style="height:100%;">
          <div class="card-title">Key Takeaways &amp; Actions</div>
          {render_key_takeaways()}
        </div>
      </div>
    </div>
  </div>
</section>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 3 — Resolution Status
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide3">
  <header class="slide-header">
    <span class="slide-num">03 / 06</span>
    <h1 class="slide-title">Resolution Status</h1>
    <span class="slide-subtitle">By period — Open / In Progress / Resolved</span>
  </header>

  <div class="slide-body">
    <div class="row" style="flex:1; gap:24px;">
      <div class="card col" style="flex:1.4;">
        <div class="card-title">Issue status by period</div>
        <div class="chart-box">
          <canvas id="chart-resolution-h" style="max-height:220px;"></canvas>
        </div>
      </div>
      <div class="card col" style="flex:1;">
        <div class="card-title">Resolution rate snapshots</div>
        <table>
          {render_resolution_rate_table()}
        </table>
        <div style="margin-top:12px; font-size:12px; color:var(--muted);">
          % of issues resolved within timeframe from creation date
        </div>
      </div>
    </div>
  </div>
</section>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 4 — Customer Communication Loop
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide4">
  <header class="slide-header">
    <span class="slide-num">04 / 06</span>
    <h1 class="slide-title">Customer Communication Loop</h1>
    <span class="slide-subtitle">Are we closing the loop after resolution?</span>
  </header>

  <div class="slide-body">
    <div class="row" style="flex:1; gap:20px;">
      <div class="card col">
        <div class="card-title">Communication rate — per period</div>
        <div style="font-size:13px; color:var(--muted); margin-bottom:4px;">
          % of resolved issues where customer was informed · target: 90%
        </div>
        <div class="chart-box" style="flex:1;">
          <canvas id="chart-comm-rate" style="max-height:320px;"></canvas>
        </div>
      </div>
      <div class="card col">
        <div class="card-title">Customers to flag</div>
        <div style="font-size:12px; color:var(--muted); margin-bottom:4px;">
          Wait &gt; 10d or open issues &gt; 3
        </div>
        <table>
          <thead>
            <tr><th>Customer</th><th>Days waiting</th><th>Open issues</th><th>Flag</th></tr>
          </thead>
          <tbody>
            {render_flagged_customers()}
          </tbody>
        </table>
      </div>
    </div>
  </div>
</section>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 5 — Churns & MRR Impact
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide5">
  <header class="slide-header">
    <span class="slide-num">05 / 06</span>
    <h1 class="slide-title">Churns &amp; MRR Impact</h1>
    <span class="slide-subtitle">{_CUR_PERIOD_LONG}</span>
  </header>

  <div class="slide-body">
    <div class="summary-grid">
      <div class="summary-card red-card">
        <div class="summary-number kpi-red">{CHURNED_COUNT_THIS_PERIOD}</div>
        <div class="summary-label">Customers churned</div>
        <div style="font-size:13px;color:var(--muted);">{_CUR_PERIOD_LONG}</div>
      </div>
      <div class="summary-card amber-card">
        <div class="summary-number" style="color:var(--yellow);">${TOTAL_MRR_THIS_PERIOD:,}</div>
        <div class="summary-label">Total MRR churned</div>
        <div style="font-size:13px;color:var(--muted);">{_CUR_PERIOD_LONG}</div>
      </div>
    </div>

    <div class="row" style="flex:1; gap:20px;">

      <!-- LEFT: per-customer stacked bar, no legend -->
      <div class="card col" style="flex:1.6;">
        <div class="card-title">MRR churned per period — hover for customer details</div>
        <canvas id="chart-churn-trend" style="max-height:300px;"></canvas>
      </div>

      <!-- RIGHT: reason summary table -->
      <div class="card col" style="flex:1;">
        <div class="card-title">Breakdown by reason — {_CUR_PERIOD_LONG}</div>
        <table>{render_churn_reason_table()}</table>
      </div>

    </div>

  </div>
</section>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 6 — Top Customers by Issue Volume
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide6">
  <header class="slide-header">
    <span class="slide-num">06 / 06</span>
    <h1 class="slide-title">Top Customers by Issue Volume</h1>
    <span class="slide-subtitle">Who's driving most of the issues this period?</span>
  </header>

  <div class="slide-body">
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Total in-scope issues</div>
        <div class="kpi-value kpi-blue">{TOTAL_ISSUES_S6}</div>
        <div class="kpi-sub">{CUSTOMERS_COUNT_S6} customers · top 10 shown</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Customers with 5+ issues</div>
        <div class="kpi-value kpi-red">{CUSTOMERS_5PLUS}</div>
        <div class="kpi-sub">Need attention this period</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Top 10 concentration</div>
        <div class="kpi-value kpi-red">{TOP10_PCT}%</div>
        <div class="kpi-sub">Of all issues</div>
      </div>
    </div>

    <div class="card" style="flex:1;">
      <div class="card-title">Issues by customer — each tick is one issue</div>
      <div class="tick-chart" style="margin-top:8px;">
        {render_tick_chart()}
      </div>
      <div style="display:flex; gap:20px; margin-top:16px; font-size:13px; color:var(--muted);">
        <span><span class="legend-dot" style="background:var(--red);display:inline-block;width:16px;height:16px;border-radius:3px;vertical-align:middle;margin-right:6px;"></span>Bug</span>
        <span><span class="legend-dot" style="background:var(--blue);display:inline-block;width:16px;height:16px;border-radius:3px;vertical-align:middle;margin-right:6px;"></span>Feature request</span>
      </div>
    </div>
  </div>
</section>


<!-- ══════════════════════════════════════════════════════════════════════
     JAVASCRIPT
════════════════════════════════════════════════════════════════════════ -->
<script>
// Register datalabels plugin
Chart.register(ChartDataLabels);

// ── DATA ─────────────────────────────────────────────────────────────
const PERIODS        = {json.dumps(PERIODS)};
const TARGET         = {json.dumps(TARGET_LINE)};
const BUG_INTERCOM   = {json.dumps(BUG_SOURCE_INTERCOM)};
const BUG_MEETINGS   = {json.dumps(BUG_SOURCE_MEETINGS)};
const BUG_TYPES      = {json.dumps(BUG_TYPES)};
const TYPE_NAMES     = {json.dumps(BUG_TYPE_NAMES)};
const COLORS         = ['#4F8EF7','#F87171','#A78BFA','#34D399','#FBBF24'];
const RES_BY_PERIOD   = {json.dumps(RESOLUTION_BY_PERIOD)};
const CHURN_MRR       = {json.dumps(CHURN_MRR_BY_PERIOD)};
const CHURN_BY_REASON = {json.dumps(CHURN_BY_REASON)};
const TOP_CUSTOMERS   = {json.dumps(TOP_CUSTOMERS_BY_ISSUES)};
const COMM_PERIODS    = {json.dumps([c["period"] for c in COMM_RATE_TREND])};
const COMM_RATES      = {json.dumps([c["rate"] for c in COMM_RATE_TREND])};
const COMM_INFORMED   = {json.dumps([c["informed"] for c in COMM_RATE_TREND])};
const COMM_RESOLVED_N = {json.dumps([c["resolved"] for c in COMM_RATE_TREND])};

// ── CHART 1 — Stacked bars (Intercom + Meetings) + target line ───────
new Chart(document.getElementById('chart-volume'), {{
  data: {{
    labels: PERIODS,
    datasets: [
      {{
        type: 'bar',
        label: 'Intercom',
        data: BUG_INTERCOM,
        backgroundColor: 'rgba(79,142,247,0.75)',
        borderRadius: 0,
        stack: 'bugs',
        order: 1,
      }},
      {{
        type: 'bar',
        label: 'Meetings',
        data: BUG_MEETINGS,
        backgroundColor: 'rgba(167,139,250,0.75)',
        borderRadius: 6,
        stack: 'bugs',
        order: 1,
      }},
      {{
        type: 'line',
        label: '−15%/period target',
        data: TARGET,
        borderColor: '#F87171',
        borderDash: [6, 4],
        borderWidth: 2,
        pointBackgroundColor: '#F87171',
        pointRadius: 5,
        fill: false,
        tension: 0,
        order: 0,
      }}
    ]
  }},
  options: {{
    responsive: true,
    plugins: {{
      datalabels: {{ display: false }},
      legend: {{ position: 'top', labels: {{ font: {{ size: 18 }}, padding: 20 }} }}
    }},
    scales: {{
      y: {{ stacked: true, beginAtZero: true, grid: {{ color: '#f1f5f9' }},
            ticks: {{ font: {{ size: 14 }} }} }},
      x: {{ stacked: true, grid: {{ display: false }},
            ticks: {{ font: {{ size: 15 }} }} }}
    }}
  }}
}});

// ── CHART 2 — Stacked bar by bug type ────────────────────────────────
new Chart(document.getElementById('chart-stacked'), {{
  type: 'bar',
  data: {{
    labels: PERIODS,
    datasets: TYPE_NAMES.map((name, i) => ({{
      label: name,
      data: BUG_TYPES.map(p => p[i]),
      backgroundColor: COLORS[i],
      borderRadius: i === TYPE_NAMES.length - 1 ? 6 : 0,
    }}))
  }},
  options: {{
    responsive: true,
    plugins: {{
      datalabels: {{ display: false }},
      legend: {{ position: 'right', labels: {{ boxWidth: 12, padding: 14 }} }}
    }},
    scales: {{
      x: {{ stacked: true, grid: {{ display: false }} }},
      y: {{ stacked: true, beginAtZero: true, grid: {{ color: '#f1f5f9' }} }}
    }}
  }}
}});

// ── CHART 4 — Communication rate bar + 90% target line ───────────────
new Chart(document.getElementById('chart-comm-rate'), {{
  data: {{
    labels: COMM_PERIODS,
    datasets: [
      {{
        type: 'bar',
        label: 'Informed rate',
        data: COMM_RATES,
        backgroundColor: 'rgba(251,191,36,0.85)',
        borderRadius: 8,
        order: 1,
        datalabels: {{
          anchor: 'end',
          align: 'end',
          formatter: v => v + '%',
          font: {{ size: 17, weight: 'bold' }},
          color: '#1e293b',
        }},
      }},
      {{
        type: 'line',
        label: '90% target',
        data: [90, 90, 90],
        borderColor: '#F87171',
        borderDash: [6, 4],
        borderWidth: 2.5,
        pointRadius: 0,
        fill: false,
        order: 0,
        datalabels: {{ display: false }},
      }}
    ]
  }},
  options: {{
    responsive: true,
    layout: {{ padding: {{ top: 28 }} }},
    plugins: {{
      legend: {{ position: 'top', labels: {{ font: {{ size: 14 }}, padding: 16 }} }},
      tooltip: {{
        callbacks: {{
          label: function(ctx) {{
            if (ctx.dataset.type === 'line') return '90% target';
            const i = ctx.dataIndex;
            return ctx.raw + '% (' + COMM_INFORMED[i] + '/' + COMM_RESOLVED_N[i] + ' informed)';
          }}
        }}
      }}
    }},
    scales: {{
      y: {{ min: 0, max: 100, grid: {{ color: '#f1f5f9' }},
            ticks: {{ callback: v => v + '%', font: {{ size: 14 }} }} }},
      x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 15 }} }} }}
    }}
  }}
}});

// ── CHART 3 — Horizontal stacked bar (resolution by period) ──────────
new Chart(document.getElementById('chart-resolution-h'), {{
  type: 'bar',
  data: {{
    labels: PERIODS,
    datasets: [
      {{
        label: 'Open',
        data: RES_BY_PERIOD.map(p => p.Open),
        backgroundColor: '#F87171',
        stack: 'status',
      }},
      {{
        label: 'In Progress',
        data: RES_BY_PERIOD.map(p => p['In Progress']),
        backgroundColor: '#FBBF24',
        stack: 'status',
      }},
      {{
        label: 'Resolved',
        data: RES_BY_PERIOD.map(p => p.Resolved),
        backgroundColor: '#34D399',
        stack: 'status',
        borderRadius: 4,
      }},
    ]
  }},
  options: {{
    indexAxis: 'y',
    responsive: true,
    plugins: {{
      datalabels: {{ display: false }},
      legend: {{ position: 'top', labels: {{ font: {{ size: 15 }}, padding: 16 }} }}
    }},
    scales: {{
      x: {{ stacked: true, beginAtZero: true, grid: {{ color: '#f1f5f9' }} }},
      y: {{ stacked: true, grid: {{ display: false }}, ticks: {{ font: {{ size: 14 }} }} }}
    }}
  }}
}});

// ── CHART 5 — MRR stacked by customer per period ─────────────────────
const CHURN_PALETTE = ['#F87171','#FB923C','#FBBF24','#A78BFA','#34D399','#60A5FA','#F472B6'];
const churnDatasets = Object.entries(CHURN_MRR).map(([name, data], i) => ({{
  label: name,
  data: data,
  backgroundColor: CHURN_PALETTE[i % CHURN_PALETTE.length],
  stack: 'churn',
  borderRadius: 4,
  datalabels: {{
    display: function(ctx) {{ return ctx.raw > 0; }},
    anchor: 'center',
    align: 'center',
    formatter: v => '$' + v.toLocaleString(),
    font: {{ size: 11, weight: '600' }},
    color: '#fff',
  }},
}}));

new Chart(document.getElementById('chart-churn-trend'), {{
  type: 'bar',
  data: {{ labels: PERIODS, datasets: churnDatasets }},
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        callbacks: {{
          label: function(ctx) {{
            const v = ctx.raw;
            if (!v) return null;
            return ctx.dataset.label + ': $' + v.toLocaleString();
          }}
        }}
      }}
    }},
    scales: {{
      y: {{ stacked: true, min: 0, beginAtZero: true,
            ticks: {{ callback: v => '$' + v.toLocaleString() }},
            grid: {{ color: '#f1f5f9' }} }},
      x: {{ stacked: true, grid: {{ display: false }} }}
    }}
  }}
}});

// ── DOT NAV + ARROW KEYS ─────────────────────────────────────────────
const slides = ['slide1','slide2','slide3','slide4','slide5','slide6'];
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

// Arrow key navigation
let currentSlide = 0;
document.addEventListener('keydown', e => {{
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


def main():
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(HTML)
    print(f"✅  Written → {OUTPUT_FILE}")
    print("   Open in browser (or press F11 for fullscreen) to present.")


if __name__ == "__main__":
    main()
