"""
generate_meeting_report.py

WHEN TO RUN: Always run after fetch_report_data.py — never alone.
Together they are the two-step process before every weekly meeting.

Reads report_data.json and renders meeting_report.html — a 4-slide
presentation for the Bug & Churn meeting. Navigate with arrow keys
or the dot nav on the right. Best viewed fullscreen (F11).
"""

import json
import os
import shutil
import subprocess
from datetime import datetime

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
    MEETINGS_PER_PERIOD   = _data.get("meetings_per_period", [0] * len(_data["periods"]))
    BUG_ONLY_COUNT        = _data.get("bug_only_count", [])
    FEATURE_COUNT         = _data.get("feature_count", [])
    BUG_TYPES             = _data["bug_types"]
    BUG_TYPE_NAMES        = _data["bug_type_names"]
    KEY_TAKEAWAYS_S2      = _data.get("key_takeaways_s2") or _MOCK_KEY_TAKEAWAYS
    RESOLUTION_BY_PERIOD  = _data["resolution_by_period"]
    RESOLUTION_RATES      = _data["resolution_rates"]
    OPEN_BY_PRIORITY      = _data.get("open_by_priority", [])
    CHURNING_PIPELINE     = _data.get("churning_pipeline", [])
    CHURN_COMBINED        = _data.get("churn_combined", [])
    CHURN_VOLUME          = _data.get("churn_volume", [0, 0, 0])
    CHURN_CANCELED_COUNT  = _data.get("churn_canceled_count", [0, 0, 0])
    CHURN_CHURNING_COUNT  = _data.get("churn_churning_count", [0, 0, 0])
    SLIDE2_COMMENTARY     = _data.get("slide2_commentary", {})
    CATEGORY_SPLIT        = _data.get("category_split", [])
    CHURN_MRR             = _data.get("churn_mrr", [0, 0, 0])
    CURRENT_PERIOD        = _data["current_period"]
    REVIEW_PERIOD         = _data.get("review_period", CURRENT_PERIOD)
else:
    _source = "mock data"
    PERIODS = ["P1 (Feb 2–15)", "P2 (Feb 16–Mar 1)", "P3 (Mar 2–15)"]

    BUG_VOLUME = [28, 24, 21]
    TARGET_LINE = [28, round(28 * 0.85, 1), round(28 * 0.85 ** 2, 1)]

    BUG_SOURCE_INTERCOM = [18, 15, 13]
    BUG_SOURCE_MEETINGS = [10,  9,  8]
    MEETINGS_PER_PERIOD = [10, 12, 8]

    BUG_TYPES = [
        [8, 5, 7, 6],
        [7, 4, 5, 5],
        [6, 4, 4, 3],
    ]
    BUG_TYPE_NAMES = ["AI Behavior", "Platform & UI", "WhatsApp Marketing", "Integration"]

    KEY_TAKEAWAYS_S2 = _MOCK_KEY_TAKEAWAYS

    RESOLUTION_BY_PERIOD = [
        {"Open": 10, "In Progress": 8,  "Resolved": 10, "Deprioritized": 0},
        {"Open":  9, "In Progress": 5,  "Resolved": 10, "Deprioritized": 0},
        {"Open":  8, "In Progress": 6,  "Resolved": 10, "Deprioritized": 0},
    ]

    RESOLUTION_RATES = [
        [25,   40,   60,   None],
        [37,   60,   70,   None],
        [50,   80,   None, None],
    ]

    OPEN_BY_PRIORITY = [
        {"Urgent": 1, "High": 3, "Medium": 4, "Low": 2},
        {"Urgent": 2, "High": 2, "Medium": 3, "Low": 2},
        {"Urgent": 1, "High": 4, "Medium": 2, "Low": 1},
    ]

    CHURNING_PIPELINE = [
        {"name": "Pendant Publishing", "mrr_raw": 800, "cancel_date": "2026-03-05",
         "reason": "Unknown", "type": "churning", "cs_owner": "Alex"},
        {"name": "Dunder Mifflin",     "mrr_raw": 600, "cancel_date": "2026-03-12",
         "reason": "Missing features", "type": "churning", "cs_owner": "Aya"},
    ]

    CHURN_COMBINED = [
        {"name": "Globex", "mrr_raw": 1500, "reason": "Missing features", "type": "canceled",
         "days_since_contact": 5, "cs_sentiment": "", "ai_resolution_rate": None, "open_issues": 0, "cs_owner": "Alex"},
        {"name": "Initech", "mrr_raw": 900, "reason": "AI Behavior", "type": "canceled",
         "days_since_contact": 12, "cs_sentiment": "", "ai_resolution_rate": None, "open_issues": 1, "cs_owner": "Aya"},
        {"name": "Pendant Publishing", "mrr_raw": 800, "reason": "Unknown", "type": "churning",
         "days_since_contact": 8, "cs_sentiment": "", "ai_resolution_rate": None, "open_issues": 0, "cs_owner": "Alex"},
    ]

    CHURN_VOLUME         = [5, 4, 2]
    CHURN_CANCELED_COUNT = [3, 2, 1]
    CHURN_CHURNING_COUNT = [2, 2, 1]
    CHURN_MRR            = [1200, 800, 500]

    CURRENT_PERIOD = "P3"
    REVIEW_PERIOD  = CURRENT_PERIOD

print(f"📊 Rendering from: {_source}")

# ── Slide 5: churn trend KPIs ──
CHURNING_COUNT     = len(CHURNING_PIPELINE)
CHURNING_MRR_TOTAL = sum(c.get("mrr_raw", 0) for c in CHURNING_PIPELINE)

# ── Slide 1 KPI cards (computed from live data) ──
_PERIOD_LABELS = [p.split(" ")[0] for p in PERIODS]   # ["P1", "P2", "P3"]
_CUR_IDX  = _PERIOD_LABELS.index(REVIEW_PERIOD) if REVIEW_PERIOD in _PERIOD_LABELS else len(PERIODS) - 1
_PREV_IDX = max(_CUR_IDX - 1, 0)

# Long-form review period label: "P1 (Feb 16 – Mar 1)" → "Period 1 (Feb 16 – Mar 1)"
_PERIOD_NUM_MAP  = {"P1": "Period 1", "P2": "Period 2", "P3": "Period 3",
                    "W1": "Week 1", "W2": "Week 2", "W3": "Week 3", "W4": "Week 4",
                    "W5": "Week 5"}
_CUR_PERIOD_LONG = PERIODS[_CUR_IDX].replace(REVIEW_PERIOD, _PERIOD_NUM_MAP.get(REVIEW_PERIOD, REVIEW_PERIOD))

KPI_BUGS_THIS_PERIOD = BUG_VOLUME[_CUR_IDX]
KPI_BUGS_ONLY_CUR    = BUG_ONLY_COUNT[_CUR_IDX] if _CUR_IDX < len(BUG_ONLY_COUNT) else 0
KPI_FEATURES_CUR     = FEATURE_COUNT[_CUR_IDX] if _CUR_IDX < len(FEATURE_COUNT) else 0
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
        sub   = f"{p1_n} → {cur_n} ({_PERIOD_LABELS[0]} → {REVIEW_PERIOD})"
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


# ── Slide 5 KPI cards (churn trend) ──
_S5_CUR = CHURN_VOLUME[_CUR_IDX]
_S5_PREV = CHURN_VOLUME[_PREV_IDX]
_S5_MRR_CUR = CHURN_MRR[_CUR_IDX]

if _S5_PREV > 0 and _CUR_IDX != _PREV_IDX:
    _s5_wow_raw = round((_S5_CUR - _S5_PREV) / _S5_PREV * 100, 1)
    S5_WOW_STR = f"+{_s5_wow_raw}%" if _s5_wow_raw > 0 else f"{_s5_wow_raw}%"
    # For churn: DOWN is good (green), UP is bad (red)
    _s5_wow_color = "kpi-green" if _s5_wow_raw <= 0 else "kpi-red"
else:
    S5_WOW_STR, _s5_wow_color = "—", "kpi-blue"

_s5_p1 = CHURN_VOLUME[0]
if _s5_p1 > 0 and _CUR_IDX > 0:
    _s5_cum_raw = round((_S5_CUR - _s5_p1) / _s5_p1 * 100, 1)
    S5_CUM_STR = f"+{_s5_cum_raw}%" if _s5_cum_raw > 0 else f"{_s5_cum_raw}%"
    _s5_cum_color = "kpi-green" if _s5_cum_raw <= 0 else "kpi-red"
else:
    S5_CUM_STR, _s5_cum_color = "—", "kpi-blue"


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



def render_bug_feature_badges():
    """Render a row of badges showing bug vs feature split under each bar."""
    cells = []
    for i in range(len(PERIODS)):
        b = BUG_ONLY_COUNT[i] if i < len(BUG_ONLY_COUNT) else 0
        f = FEATURE_COUNT[i] if i < len(FEATURE_COUNT) else 0
        cells.append(
            f'<div style="flex:1; text-align:center;">'
            f'<span style="display:inline-block; background:#fee2e2; color:#991b1b; '
            f'border-radius:12px; padding:4px 10px; font-size:16px; font-weight:600; margin:0 3px;">'
            f'\U0001f41b {b}</span> '
            f'<span style="display:inline-block; background:#ede9fe; color:#5b21b6; '
            f'border-radius:12px; padding:4px 10px; font-size:16px; font-weight:600; margin:0 3px;">'
            f'\U0001f4a1 {f}</span></div>'
        )
    return (
        '<div style="display:flex; justify-content:space-around; '
        'margin-top:4px; padding: 0 38px 0 32px;">'
        + ''.join(cells) +
        '</div>'
    )


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


def _render_slide3_note():
    """Info bubble showing difference between category-split bugs and resolution bugs."""
    cat_bugs = sum(cs["bugs"] for cs in CATEGORY_SPLIT)
    res_last = RESOLUTION_BY_PERIOD[-1] if RESOLUTION_BY_PERIOD else {}
    res_bugs = (res_last.get("Open", 0) + res_last.get("In Progress", 0)
                + res_last.get("Resolved", 0) + res_last.get("Deprioritized", 0))
    diff = cat_bugs - res_bugs
    if diff <= 0:
        return ""
    return (
        f'<div style="margin-top:8px; padding:8px 12px; background:#f1f5f9; '
        f'border-radius:6px; font-size:12px; color:#64748b; line-height:1.4;">'
        f'Numbers differ from Issues page by <strong>{diff}</strong> — '
        f'those bugs already had a Linear ticket created in a previous period.'
        f'</div>'
    )


def _theme_row(t):
    """Render one theme entry. t may be a dict {label,total,resolved,open} or legacy str."""
    if isinstance(t, dict):
        total    = t["total"]
        resolved = t["resolved"]
        badge = (f'<span class="theme-resolved">{resolved}/{total} resolved</span>'
                 if resolved > 0 else
                 '<span class="theme-open">all open</span>')
        return f'<li>{t["label"]} ({total}) {badge}</li>'
    return f'<li>{t}</li>'


def render_key_takeaways():
    # KEY_TAKEAWAYS_S2 is a list of category blocks:
    # [{"category": str, "count": int, "color": hex, "themes": [dict|str, ...]}, ...]
    blocks = []
    for blk in KEY_TAKEAWAYS_S2:
        cat   = blk.get("category", "")
        count = blk.get("count", 0)
        color = blk.get("color", "#94A3B8")
        themes = blk.get("themes", [])
        theme_items = "".join(_theme_row(t) for t in themes)
        total_resolved = sum(t["resolved"] for t in themes if isinstance(t, dict))
        resolved_html = (
            f' <span class="takeaway-cat-resolved">{total_resolved}/{count} resolved</span>'
            if total_resolved > 0 else ""
        )
        blocks.append(
            f'<div class="takeaway-block">'
            f'<div class="takeaway-cat-header">'
            f'<span class="takeaway-dot" style="background:{color};"></span>'
            f'<strong>{cat}</strong>'
            f'<span class="takeaway-count">{count} issues{resolved_html}</span>'
            f'</div>'
            f'<ul class="takeaway-theme-list">{theme_items}</ul>'
            f'</div>'
        )
    return "".join(blocks)


# Category colors matching the chart
_COMMENTARY_COLORS = {
    "AI Behavior": "#4F8EF7",
    "Platform & UI": "#F87171",
    "WhatsApp Marketing": "#A78BFA",
    "Integration": "#34D399",
}

def render_slide2_commentary():
    """Render category commentary blocks."""
    blocks = []
    for cs in CATEGORY_SPLIT:
        cat = cs["category"]
        count = cs["total"]
        if count == 0:
            continue
        color = _COMMENTARY_COLORS.get(cat, "#94A3B8")
        comment = SLIDE2_COMMENTARY.get(cat, "")
        # Support both string (legacy) and list of bullets (new format)
        if isinstance(comment, list):
            import re as _re
            def _bold(s):
                return _re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', s)
            bullets_html = "".join(
                f'<li style="margin-bottom:6px; font-size:14px; color:var(--text); line-height:1.5;">{_bold(b)}</li>'
                for b in comment
            )
            body = f'<ul style="margin:4px 0 0 20px; padding-left:16px;">{bullets_html}</ul>'
        else:
            body = f'<p style="margin:0 0 0 20px; font-size:14px; color:var(--text); line-height:1.5;">{comment}</p>'
        blocks.append(
            f'<div style="margin-bottom:16px;">'
            f'<div style="display:flex; align-items:center; gap:8px; margin-bottom:4px;">'
            f'<span style="display:inline-block; width:12px; height:12px; border-radius:50%; background:{color};"></span>'
            f'<strong style="font-size:16px;">{cat}</strong>'
            f'<span style="color:var(--muted); font-size:14px;">{count} issues</span>'
            f'</div>'
            f'{body}'
            f'</div>'
        )
    return "".join(blocks)


def render_waterfall_chart():
    """Render an HTML/CSS waterfall chart with bug/feature split per category."""
    total = sum(cs["total"] for cs in CATEGORY_SPLIT)
    if total == 0:
        return "<p>No issues this period.</p>"
    max_val = total  # total bar is the widest

    rows = []
    cumulative = 0
    for cs in CATEGORY_SPLIT:
        cat = cs["category"]
        count = cs["total"]
        bugs = cs["bugs"]
        features = cs["features"]
        if count == 0:
            continue
        color = _COMMENTARY_COLORS.get(cat, "#94A3B8")
        offset_pct = cumulative / max_val * 100
        width_pct = count / max_val * 100
        cumulative += count

        # Connector line from previous bar
        connector = ""
        if rows:  # not first row
            connector = (
                f'<div style="position:absolute; left:{offset_pct}%; top:-8px; '
                f'width:1px; height:8px; background:#cbd5e1;"></div>'
            )

        rows.append(
            f'<div style="display:flex; align-items:center; margin-bottom:24px; height:72px;">'
            f'<div style="width:140px; text-align:right; padding-right:12px; font-size:14px; font-weight:600; color:var(--text); flex-shrink:0;">{cat}</div>'
            f'<div style="flex:1; position:relative;">'
            f'{connector}'
            f'<div style="margin-left:{offset_pct}%; width:{width_pct}%; background:{color}; '
            f'height:56px; border-radius:4px; min-width:2px;"></div>'
            f'</div>'
            f'<div style="width:200px; padding-left:10px; display:flex; align-items:center; gap:6px; flex-shrink:0;">'
            f'<span style="font-size:16px; font-weight:700; color:var(--text); min-width:28px;">{count}</span>'
            f'<span style="background:#fee2e2; color:#991b1b; border-radius:10px; padding:2px 7px; font-size:12px; font-weight:600;">'
            f'\U0001f41b {bugs}</span>'
            f'<span style="background:#ede9fe; color:#5b21b6; border-radius:10px; padding:2px 7px; font-size:12px; font-weight:600;">'
            f'\U0001f4a1 {features}</span>'
            f'</div>'
            f'</div>'
        )

    # Connector to total
    connector_total = (
        f'<div style="position:absolute; left:0; top:-8px; '
        f'width:1px; height:8px; background:#cbd5e1;"></div>'
    )

    # Total bar
    total_bugs = sum(cs["bugs"] for cs in CATEGORY_SPLIT)
    total_features = sum(cs["features"] for cs in CATEGORY_SPLIT)
    rows.append(
        f'<div style="border-top:2px solid #e2e8f0; margin-top:4px; padding-top:10px; '
        f'display:flex; align-items:center; height:72px;">'
        f'<div style="width:140px; text-align:right; padding-right:12px; font-size:14px; font-weight:700; color:var(--text); flex-shrink:0;">Total</div>'
        f'<div style="flex:1; position:relative;">'
        f'{connector_total}'
        f'<div style="width:100%; background:#cbd5e1; height:56px; border-radius:4px;"></div>'
        f'</div>'
        f'<div style="width:200px; padding-left:10px; display:flex; align-items:center; gap:6px; flex-shrink:0;">'
        f'<span style="font-size:16px; font-weight:700; color:var(--text); min-width:28px;">{total}</span>'
        f'<span style="background:#fee2e2; color:#991b1b; border-radius:10px; padding:2px 7px; font-size:12px; font-weight:600;">'
        f'\U0001f41b {total_bugs}</span>'
        f'<span style="background:#ede9fe; color:#5b21b6; border-radius:10px; padding:2px 7px; font-size:12px; font-weight:600;">'
        f'\U0001f4a1 {total_features}</span>'
        f'</div>'
        f'</div>'
    )

    return '<div style="display:flex; flex-direction:column; justify-content:center; height:100%;">' + "".join(rows) + '</div>'


_SENTIMENT_STYLE = {
    "Positive":  ("badge-green",  "😊"),
    "Neutral":   ("badge-yellow", "😐"),
    "Negative":  ("badge-red",    "😟"),
    "At Risk":   ("badge-red",    "⚠️"),
}

def _render_churn_row(c):
    """Render a single row for the combined churn table."""
    is_canceled = c.get("type") == "canceled"
    dot_color = "var(--red)" if is_canceled else "var(--yellow)"
    dot = f'<span style="color:{dot_color};font-size:16px;vertical-align:middle;">●</span>'

    mrr_fmt = f'€{c["mrr_raw"]:,.0f}' if c.get("mrr_raw") else '—'

    days = c.get("days_since_contact")
    if days is not None:
        days_int = int(days)
        color = ("color:var(--red)" if days_int > 30
                 else "color:var(--yellow)" if days_int > 14
                 else "color:var(--text)")
        days_html = f'<span style="{color};font-weight:600;">{days_int}d</span>'
    else:
        days_html = '<span style="color:var(--muted);">—</span>'

    sentiment = c.get("cs_sentiment") or ""
    if sentiment:
        badge_cls, icon = _SENTIMENT_STYLE.get(sentiment, ("badge-blue", ""))
        sentiment_html = f'<span class="badge {badge_cls}">{icon} {sentiment}</span>'
    else:
        sentiment_html = '<span style="color:var(--muted);">—</span>'

    ai_rate = c.get("ai_resolution_rate")
    if ai_rate is not None:
        pct = round(ai_rate * 100) if ai_rate <= 1 else round(ai_rate)
        ai_html = f'{pct}%'
    else:
        ai_html = '<span style="color:var(--muted);">—</span>'

    open_n = c.get("open_issues")
    if open_n is not None and open_n > 0:
        open_html = f'<span style="font-weight:700;color:var(--red);">{open_n}</span>'
    elif open_n == 0:
        open_html = '<span style="color:var(--muted);">0</span>'
    else:
        open_html = '<span style="color:var(--muted);">—</span>'

    _owner_colors = {"Alex": "badge-blue", "Aya": "badge-purple"}
    _owner_raw = c.get("cs_owner") or ""
    owner = (f'<span class="badge {_owner_colors.get(_owner_raw, "badge-blue")}">{_owner_raw}</span>'
             if _owner_raw else '<span style="color:var(--muted);">—</span>')

    return (
        f'<tr>'
        f'<td style="text-align:center;">{dot}</td>'
        f'<td>{c["name"]}</td>'
        f'<td style="text-align:right;">{mrr_fmt}</td>'
        f'<td style="text-align:center;">{days_html}</td>'
        f'<td style="text-align:center;">{ai_html}</td>'
        f'<td style="text-align:center;">{open_html}</td>'
        f'<td>{sentiment_html}</td>'
        f'<td>{owner}</td>'
        f'<td style="color:var(--muted);">{c.get("reason") or "—"}</td>'
        f'</tr>'
    )


def render_churn_combined_table():
    """Combined table of canceled + churning customers for this period."""
    header = (
        '<thead><tr>'
        '<th style="width:24px;"></th>'
        '<th>Customer</th>'
        '<th style="text-align:right;">MRR</th>'
        '<th style="text-align:center;">Days Since<br>Contact</th>'
        '<th style="text-align:center;">AI Res.<br>Rate</th>'
        '<th style="text-align:center;">Open<br>Issues</th>'
        '<th>CS Sentiment</th>'
        '<th>CS Owner</th>'
        '<th>Reason</th>'
        '</tr></thead>'
    )
    if not CHURN_COMBINED:
        return (
            header +
            '<tbody><tr><td colspan="9" style="color:var(--muted);text-align:center;">'
            'No churns this period</td></tr></tbody>'
        )
    rows = [_render_churn_row(c) for c in CHURN_COMBINED]
    return header + '<tbody>' + ''.join(rows) + '</tbody>'


# ──────────────────────────────────────────────────────────────────────────────
# HTML TEMPLATE
# ──────────────────────────────────────────────────────────────────────────────

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Bug &amp; Churn — Weekly Meeting</title>
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
  .kpi-label {{ font-size: 16px; color: var(--muted); font-weight: 500; margin-bottom: 8px; }}
  .kpi-value {{ font-size: 36px; font-weight: 800; color: var(--navy); line-height: 1; }}
  .kpi-sub {{ font-size: 13px; color: var(--muted); margin-top: 4px; }}
  .kpi-green {{ color: var(--green) !important; }}
  .kpi-blue  {{ color: var(--blue)  !important; }}
  .kpi-red   {{ color: var(--red)   !important; }}

  /* ── COMPACT KPI (slide 2) ────────────────────────────────────────── */
  .kpi-card.compact .kpi-value {{ font-size: 28px; }}

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
  .badge-purple {{ background: #EDE9FE; color: #6D28D9; }}

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
  .theme-resolved {{ color:#16a34a; font-weight:600; margin-left:6px; }}
  .theme-open     {{ color:#9ca3af; margin-left:6px; }}
  .takeaway-cat-resolved {{ color:#16a34a; font-weight:600; margin-left:8px; }}

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

</style>
</head>
<body>

<!-- ── DOT NAV ─────────────────────────────────────────────────────────── -->
<nav id="dot-nav">
  <div class="dot active" data-target="slide1" title="Bug Volume"></div>
  <div class="dot"        data-target="slide2" title="Categorization"></div>
  <div class="dot"        data-target="slide3" title="Resolution"></div>
  <div class="dot"        data-target="slide4" title="Churns"></div>
</nav>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 1 — Bug Volume & Trend
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide1">
  <header class="slide-header">
    <span class="slide-num">01 / 04</span>
    <h1 class="slide-title">Issues Volume &amp; Trend</h1>
    <span class="slide-subtitle">Target: −15% per period</span>
  </header>

  <div class="slide-body">
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Issues this period</div>
        <div class="kpi-value kpi-blue">{KPI_BUGS_THIS_PERIOD}</div>
        <div class="kpi-sub">{KPI_PERIOD_LABEL}</div>
        <div style="margin-top:6px;">
          <span style="background:#fee2e2; color:#991b1b; border-radius:12px; padding:3px 9px; font-size:14px; font-weight:600;">\U0001f41b {KPI_BUGS_ONLY_CUR}</span>
          <span style="background:#ede9fe; color:#5b21b6; border-radius:12px; padding:3px 9px; font-size:14px; font-weight:600;">\U0001f4a1 {KPI_FEATURES_CUR}</span>
        </div>
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
      <div class="card-title">Issues received per period — Intercom vs Meetings vs −15% target</div>
      <div class="chart-box">
        <canvas id="chart-volume"></canvas>
      </div>
      {render_bug_feature_badges()}
    </div>
  </div>
</section>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 2 — Bug Categorization
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide2">
  <header class="slide-header">
    <span class="slide-num">02 / 04</span>
    <h1 class="slide-title">Issue Breakdown — {REVIEW_PERIOD}</h1>
    <span class="slide-subtitle">Category split &amp; what customers complained about</span>
  </header>

  <div class="slide-body">
    <div class="row" style="flex:1; gap:24px;">
      <div style="flex:1; display:flex; flex-direction:column;">
        <div class="card" style="flex:1;">
          <div class="card-title">Issues by category — {REVIEW_PERIOD}</div>
          {render_waterfall_chart()}
        </div>
      </div>
      <div style="flex:1;">
        <div class="card" style="height:100%; overflow-y:auto;">
          <div class="card-title">What customers reported</div>
          {render_slide2_commentary()}
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
    <span class="slide-num">03 / 04</span>
    <h1 class="slide-title">Bugs Focus: Resolution Status of Engineering Tickets in Linear</h1>
    <span class="slide-subtitle">Bugs only — Open / In Progress / Resolved</span>
  </header>

  <div class="slide-body">
    <div class="row" style="flex:1; gap:24px;">
      <div class="card col" style="flex:1.4;">
        <div class="card-title">Bug status by period</div>
        <div class="chart-box">
          <canvas id="chart-resolution-h" style="max-height:220px;"></canvas>
        </div>
        {_render_slide3_note()}
      </div>
      <div class="card col" style="flex:1;">
        <div class="card-title">Resolution rate snapshots</div>
        <table>
          {render_resolution_rate_table()}
        </table>
        <div style="margin-top:12px; font-size:12px; color:var(--muted);">
          % of bugs resolved within timeframe from creation date
        </div>
      </div>
    </div>

    <div class="row" style="gap:24px; margin-top:16px;">
      <div class="card col" style="flex:1.4;">
        <div class="card-title">Open eng tickets by Linear priority</div>
        <div class="chart-box">
          <canvas id="chart-open-priority" style="max-height:220px;"></canvas>
        </div>
      </div>
      <div style="flex:1;"></div>
    </div>
  </div>
</section>

<!-- ══════════════════════════════════════════════════════════════════════
     SLIDE 4 — Weekly Churn Trend
════════════════════════════════════════════════════════════════════════ -->
<section class="slide" id="slide4">
  <header class="slide-header">
    <span class="slide-num">04 / 04</span>
    <h1 class="slide-title">Weekly Churn Trend</h1>
    <span class="slide-subtitle">Cancel-click date from Stripe</span>
  </header>

  <div class="slide-body">
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Churns this week</div>
        <div class="kpi-value kpi-red">{_S5_CUR}</div>
        <div class="kpi-sub">{KPI_PERIOD_LABEL} · €{_S5_MRR_CUR:,.0f} MRR</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Week-over-week change</div>
        <div class="kpi-value {_s5_wow_color}">{S5_WOW_STR}</div>
        <div class="kpi-sub">vs {KPI_PREV_LABEL}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Cumulative change</div>
        <div class="kpi-value {_s5_cum_color}">{S5_CUM_STR}</div>
        <div class="kpi-sub">{_PERIOD_LABELS[0]} → {REVIEW_PERIOD}</div>
      </div>
    </div>

    <div class="card" style="flex:1; margin-top:8px;">
      <div class="card-title">Cancel-clicks per week — Canceled vs Still Churning</div>
      <div class="chart-box">
        <canvas id="chart-churn-volume"></canvas>
      </div>
    </div>

    <div class="card" style="max-height:320px; overflow-y:auto;">
      <div class="card-title">
        Churns this week
        <span style="font-size:12px;font-weight:400;color:var(--muted);margin-left:8px;">
          <span style="color:var(--red);">●</span> canceled &nbsp;
          <span style="color:var(--yellow);">●</span> churning
        </span>
      </div>
      <table style="font-size:12px;">{render_churn_combined_table()}</table>
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
const BUG_ONLY_COUNT = {json.dumps(BUG_ONLY_COUNT)};
const FEATURE_COUNT  = {json.dumps(FEATURE_COUNT)};
const BUG_VOLUME     = {json.dumps(BUG_VOLUME)};
const MTG_COUNT      = {json.dumps(MEETINGS_PER_PERIOD)};
const BUG_TYPES      = {json.dumps(BUG_TYPES)};
const TYPE_NAMES     = {json.dumps(BUG_TYPE_NAMES)};
const COLORS         = ['#4F8EF7','#F87171','#A78BFA','#34D399','#FBBF24'];
const RES_BY_PERIOD   = {json.dumps(RESOLUTION_BY_PERIOD)};

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
        datalabels: {{
          labels: {{
            total: {{
              display: true,
              anchor: 'end',
              align: 'end',
              formatter: (v, ctx) => BUG_VOLUME[ctx.dataIndex],
              font: {{ size: 16, weight: 'bold' }},
              color: '#334155',
            }},
            meetingCount: {{
              display: (ctx) => MTG_COUNT[ctx.dataIndex] > 0,
              anchor: 'end',
              align: 'start',
              offset: 4,
              formatter: (v, ctx) => MTG_COUNT[ctx.dataIndex] + ' mtgs',
              font: {{ size: 11, weight: '600' }},
              color: '#7C3AED',
              backgroundColor: 'rgba(167,139,250,0.15)',
              borderRadius: 8,
              padding: {{ top: 2, bottom: 2, left: 6, right: 6 }},
            }}
          }}
        }},
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
      y: {{ stacked: true, beginAtZero: true, grid: {{ display: false }},
            ticks: {{ font: {{ size: 14 }} }} }},
      x: {{ stacked: true, grid: {{ display: false }},
            ticks: {{ font: {{ size: 15 }} }} }}
    }}
  }}
}});

// ── (Chart 2 replaced by HTML waterfall) ─────────────────────────────

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
      }},
      {{
        label: 'Deprioritized',
        data: RES_BY_PERIOD.map(p => p.Deprioritized || 0),
        backgroundColor: '#D1D5DB',
        stack: 'status',
        borderRadius: 4,
        datalabels: {{
          display: true,
          anchor: 'end',
          align: 'end',
          formatter: (v, ctx) => {{
            const p = RES_BY_PERIOD[ctx.dataIndex];
            return p.Open + p['In Progress'] + p.Resolved + (p.Deprioritized || 0);
          }},
          font: {{ size: 14, weight: 'bold' }},
          color: '#334155',
        }},
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
      x: {{ stacked: true, beginAtZero: true, grid: {{ display: false }} }},
      y: {{ stacked: true, grid: {{ display: false }}, ticks: {{ font: {{ size: 14 }} }} }}
    }}
  }}
}});

// ── CHART 3b — Open bugs by Linear priority (horizontal stacked) ────
const OPEN_BY_PRIO = {json.dumps(OPEN_BY_PRIORITY)};

new Chart(document.getElementById('chart-open-priority'), {{
  type: 'bar',
  data: {{
    labels: PERIODS,
    datasets: [
      {{
        label: 'Low',
        data: OPEN_BY_PRIO.map(p => p.Low),
        backgroundColor: '#D5DCE5',
        hoverBackgroundColor: '#A8B8CC',
        stack: 'prio',
      }},
      {{
        label: 'Medium',
        data: OPEN_BY_PRIO.map(p => p.Medium),
        backgroundColor: '#FDE68A',
        hoverBackgroundColor: '#FACC15',
        stack: 'prio',
      }},
      {{
        label: 'High',
        data: OPEN_BY_PRIO.map(p => p.High),
        backgroundColor: '#FDBA74',
        hoverBackgroundColor: '#FB923C',
        stack: 'prio',
      }},
      {{
        label: 'Urgent',
        data: OPEN_BY_PRIO.map(p => p.Urgent),
        backgroundColor: '#FCA5A5',
        hoverBackgroundColor: '#F87171',
        stack: 'prio',
        borderRadius: 4,
        datalabels: {{
          display: true,
          anchor: 'end',
          align: 'end',
          formatter: (v, ctx) => {{
            const p = OPEN_BY_PRIO[ctx.dataIndex];
            return p.Urgent + p.High + p.Medium + p.Low;
          }},
          font: {{ size: 14, weight: 'bold' }},
          color: '#334155',
        }},
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
      x: {{ stacked: true, beginAtZero: true, grid: {{ display: false }} }},
      y: {{ stacked: true, grid: {{ display: false }}, ticks: {{ font: {{ size: 14 }} }} }}
    }}
  }}
}});

// ── CHART 5 — Weekly churn trend (stacked: Canceled + Still Churning) ──
const CHURN_CANCELED  = {json.dumps(CHURN_CANCELED_COUNT)};
const CHURN_CHURNING  = {json.dumps(CHURN_CHURNING_COUNT)};
const CHURN_TOTAL     = {json.dumps(CHURN_VOLUME)};

new Chart(document.getElementById('chart-churn-volume'), {{
  data: {{
    labels: PERIODS,
    datasets: [
      {{
        type: 'bar',
        label: 'Canceled',
        data: CHURN_CANCELED,
        backgroundColor: '#F87171',
        stack: 's',
        borderRadius: 0,
        order: 1,
      }},
      {{
        type: 'bar',
        label: 'Still churning',
        data: CHURN_CHURNING,
        backgroundColor: '#FBBF24',
        stack: 's',
        borderRadius: 6,
        order: 1,
        datalabels: {{
          display: true,
          anchor: 'end',
          align: 'end',
          formatter: (v, ctx) => CHURN_TOTAL[ctx.dataIndex],
          font: {{ size: 16, weight: 'bold' }},
          color: '#334155',
        }},
      }},
      {{
        type: 'line',
        label: 'Total trend',
        data: CHURN_TOTAL.map((v, i) => (i === 0 || i === CHURN_TOTAL.length - 1) ? v : null),
        spanGaps: true,
        borderColor: '#F87171',
        borderDash: [6, 4],
        borderWidth: 2,
        pointBackgroundColor: '#F87171',
        pointRadius: 5,
        fill: false,
        tension: 0,
        order: 0,
      }},
    ]
  }},
  options: {{
    responsive: true,
    plugins: {{
      datalabels: {{ display: false }},
      legend: {{ position: 'top', labels: {{ font: {{ size: 18 }}, padding: 20 }} }}
    }},
    scales: {{
      y: {{ stacked: true, beginAtZero: true, grid: {{ display: false }},
            ticks: {{ stepSize: 1, font: {{ size: 14 }} }} }},
      x: {{ stacked: true, grid: {{ display: false }},
            ticks: {{ font: {{ size: 15 }} }} }}
    }}
  }}
}});

// ── DOT NAV + ARROW KEYS ─────────────────────────────────────────────
const slides = ['slide1','slide2','slide3','slide4'];
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


def _fmt_date(iso: str) -> str:
    """'2026-02-16' → '16Feb', '2026-03-01' → '1Mar'"""
    d = datetime.strptime(iso, "%Y-%m-%d")
    return f"{d.day}{d.strftime('%b')}"


def _archive():
    """Copy the HTML to a dated folder under meetings/ and export a PDF via headless Chrome."""
    # Resolve the current period's start/end from the JSON
    if not _data or "period_ranges" not in _data:
        print("⚠️  Skipping archive — period_ranges not found in report_data.json (re-run fetch_report_data.py)")
        return

    cur = next((p for p in _data["period_ranges"] if p["label"] == CURRENT_PERIOD), None)
    if not cur:
        print(f"⚠️  Skipping archive — no period_ranges entry for {CURRENT_PERIOD}")
        return

    folder_name = f"CS_weekly_{_fmt_date(cur['start'])}-{_fmt_date(cur['end'])}"
    project_root = os.path.dirname(os.path.abspath(__file__))
    meetings_dir = os.path.join(project_root, "meetings")
    archive_dir  = os.path.join(meetings_dir, folder_name)
    os.makedirs(archive_dir, exist_ok=True)

    # Copy HTML
    html_dest = os.path.join(archive_dir, f"{folder_name}.html")
    shutil.copy2(OUTPUT_FILE, html_dest)

    # Export PDF via headless Chrome
    pdf_dest   = os.path.join(archive_dir, f"{folder_name}.pdf")
    _CHROME_CANDIDATES = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",  # macOS
        "/usr/bin/google-chrome",        # Ubuntu (GitHub Actions)
        "/usr/bin/google-chrome-stable",
        "/snap/bin/chromium",
    ]
    chrome_bin = next((p for p in _CHROME_CANDIDATES if os.path.exists(p)), None)
    if chrome_bin:
        result = subprocess.run(
            [
                chrome_bin,
                "--headless",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                f"--print-to-pdf={pdf_dest}",
                f"file://{html_dest}",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"⚠️  Chrome PDF export failed: {result.stderr.strip()}")
    else:
        print("⚠️  Chrome not found — skipping PDF export")

    print(f"✅  Archived to meetings/{folder_name}/")


def main():
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(HTML)
    print(f"✅  Written → {OUTPUT_FILE}")
    print("   Open in browser (or press F11 for fullscreen) to present.")
    _archive()


if __name__ == "__main__":
    main()
