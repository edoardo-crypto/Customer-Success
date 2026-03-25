#!/usr/bin/env python3
"""
generate_all_checkins.py

Batch generates one HTML check-in report per active/churning customer.
Outputs to meetings/checkin/site/ for deployment to GitHub Pages.

    python3 meetings/checkin/generate_all_checkins.py

Produces:
  site/{slug}.html     — one report per customer
  site/manifest.json   — metadata for update_mct_report_urls.py
  site/index.html      — listing page linking all reports
"""

import json
import os
import re
import sys
import unicodedata

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SITE_DIR   = os.path.join(SCRIPT_DIR, "site")
RELEASES_FILE = os.path.join(SCRIPT_DIR, "releases.json")

# Import from sibling modules
sys.path.insert(0, SCRIPT_DIR)
from fetch_checkin_data import (
    fetch_all_mct, fetch_all_issues, parse_customer_issues,
    get_title, get_select, get_rich_text, extract_channels,
    fetch_all_clickhouse_metrics,
)
from generate_checkin import render_checkin_html


# ── SLUG ─────────────────────────────────────────────────────────────────────

def slugify(name):
    """'FUNDACIÓN HAKUNA REVOLUTION' → 'fundacion-hakuna-revolution'"""
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or "unnamed"


def unique_slugs(names):
    """Return {name: slug} dict, appending -2, -3 on collisions."""
    seen = {}
    result = {}
    for name in names:
        base = slugify(name)
        slug = base
        n = 1
        while slug in seen:
            n += 1
            slug = f"{base}-{n}"
        seen[slug] = True
        result[name] = slug
    return result


# ── INDEX PAGE ───────────────────────────────────────────────────────────────

def render_index_html(manifest):
    rows = []
    for entry in sorted(manifest, key=lambda e: e["name"].lower()):
        badge = ""
        if entry.get("billing_status") == "Churning":
            badge = ' <span style="color:#B91C1C; font-size:12px;">(Churning)</span>'
        rows.append(
            f'<tr>'
            f'<td><a href="{entry["slug"]}.html">{entry["name"]}</a>{badge}</td>'
            f'<td style="text-align:center;">{entry["issue_count"]}</td>'
            f'</tr>'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<title>Customer Check-in Reports</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet"/>
<style>
  body {{ font-family: 'Inter', sans-serif; background: #f1f5f9; color: #1e293b; padding: 40px; }}
  h1 {{ font-size: 28px; margin-bottom: 8px; }}
  .subtitle {{ color: #64748b; margin-bottom: 24px; }}
  table {{ width: 100%; max-width: 700px; border-collapse: collapse; background: #fff;
           border-radius: 12px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); overflow: hidden; }}
  th {{ text-align: left; font-size: 12px; font-weight: 600; color: #64748b;
       text-transform: uppercase; padding: 12px 16px; border-bottom: 2px solid #e2e8f0; }}
  td {{ padding: 10px 16px; border-bottom: 1px solid #e2e8f0; }}
  tr:last-child td {{ border-bottom: none; }}
  a {{ color: #4F8EF7; text-decoration: none; font-weight: 600; }}
  a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<h1>Customer Check-in Reports</h1>
<p class="subtitle">{len(manifest)} customers · Updated daily</p>
<table>
<thead><tr><th>Customer</th><th style="text-align:center;">Issues</th></tr></thead>
<tbody>
{"".join(rows)}
</tbody>
</table>
</body>
</html>"""


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  generate_all_checkins.py — Batch check-in report generator")
    print("=" * 60)

    # 1. Fetch all MCT pages
    mct_pages = fetch_all_mct()

    # 2. Filter to Active + Churning
    customers = []
    for page in mct_pages:
        name = get_title(page, "🏢 Company Name").strip()
        if not name:
            continue
        billing = get_select(page, "💰 Billing Status") or ""
        if billing in ("Active", "Churning"):
            customers.append({
                "page_id": page["id"],
                "name": name,
                "billing_status": billing,
                "channels": extract_channels(page),
                "stripe_id": get_rich_text(page, "🔗 Stripe Customer ID").strip(),
            })

    print(f"\n📋 {len(customers)} customers (Active + Churning)")

    # 3. Fetch all issues
    issue_pages = fetch_all_issues()

    # 3b. Fetch ClickHouse metrics for all customers (one bulk query)
    all_metrics = fetch_all_clickhouse_metrics()

    # 4. Load shared releases
    releases = {"updated": "", "items": []}
    if os.path.exists(RELEASES_FILE):
        with open(RELEASES_FILE, encoding="utf-8") as f:
            releases = json.load(f)

    # 5. Compute unique slugs
    slug_map = unique_slugs([c["name"] for c in customers])

    # 6. Create output directory
    os.makedirs(SITE_DIR, exist_ok=True)

    # 7. Generate one HTML per customer
    manifest = []
    for i, cust in enumerate(sorted(customers, key=lambda c: c["name"].lower()), 1):
        slug = slug_map[cust["name"]]
        issues = parse_customer_issues(issue_pages, cust["page_id"])

        metrics = all_metrics.get(cust.get("stripe_id")) if cust.get("stripe_id") else None
        html = render_checkin_html(cust["name"], issues, releases,
                                   metrics=metrics,
                                   channels=cust.get("channels"))
        out_path = os.path.join(SITE_DIR, f"{slug}.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)

        manifest.append({
            "slug": slug,
            "name": cust["name"],
            "page_id": cust["page_id"],
            "billing_status": cust["billing_status"],
            "issue_count": len(issues),
        })

        print(f"  [{i}/{len(customers)}] {cust['name']} → {slug}.html ({len(issues)} issues)")

    # 8. Write manifest
    manifest_path = os.path.join(SITE_DIR, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    # 9. Write index page
    index_path = os.path.join(SITE_DIR, "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(render_index_html(manifest))

    print(f"\n✅ Done — {len(manifest)} reports in {SITE_DIR}/")
    print(f"   manifest.json + index.html written")


if __name__ == "__main__":
    main()
