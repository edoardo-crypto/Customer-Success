#!/usr/bin/env python3
"""
check_customer_churn.py — Targeted churn diagnosis for specific customers

Checks "Pott Candles" and "Deeply" (and any other names you pass via --name):
  1. Scans Notion MCT for matching rows (case-insensitive substring match)
  2. Looks up their Stripe subscription status
  3. Checks whether the n8n churn webhook is active
  4. Prints a clear VERDICT per customer

Usage:
  python3 check_customer_churn.py                     # check pott + deeply
  python3 check_customer_churn.py --fix               # also patch Notion if PIPELINE BUG found
  python3 check_customer_churn.py --name "acme corp"  # check an extra customer
"""

import sys
import time
import datetime
import requests

# ── Credentials ───────────────────────────────────────────────────────────────
STRIPE_KEY           = "***REMOVED***"
NOTION_TOKEN         = "***REMOVED***"
MCT_DS_ID            = "3ceb1ad0-91f1-40db-945a-c51c58035898"
N8N_BASE             = "https://konvoai.app.n8n.cloud"
N8N_API_KEY          = "***REMOVED***"
N8N_WEBHOOK_WF_ID    = "8cLtcqxjD8DC59JG"

# ── Default search targets ─────────────────────────────────────────────────────
DEFAULT_TARGETS = ["pott", "deeply"]

# ── HTTP headers ──────────────────────────────────────────────────────────────
notion_headers = {
    "Authorization":  f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type":   "application/json",
}
stripe_headers = {
    "Authorization": f"Bearer {STRIPE_KEY}",
}
n8n_headers = {
    "X-N8N-API-KEY": N8N_API_KEY,
}


# ── Property helpers (same as audit_churn_consistency.py) ─────────────────────

def get_title(props, key):
    texts = props.get(key, {}).get("title", [])
    return texts[0].get("plain_text", "") if texts else ""


def get_select(props, key):
    sel = (props.get(key, {}).get("select") or {})
    return sel.get("name", "")


def get_rich_text(props, key):
    texts = props.get(key, {}).get("rich_text", [])
    return texts[0].get("plain_text", "") if texts else ""


def get_date(props, key):
    d = (props.get(key, {}).get("date") or {})
    return d.get("start", "")


def get_formula_string(props, key):
    f = (props.get(key, {}).get("formula") or {})
    return f.get("string", "")


# ── Step 1: check n8n webhook ─────────────────────────────────────────────────

def check_n8n_webhook():
    """Return (workflow_name, is_active) or (error_string, None)."""
    url = f"{N8N_BASE}/api/v1/workflows/{N8N_WEBHOOK_WF_ID}"
    r = requests.get(url, headers=n8n_headers)
    if r.status_code != 200:
        return f"[HTTP {r.status_code}]", None
    data = r.json()
    return data.get("name", "(unnamed)"), data.get("active", False)


# ── Step 2: scan Notion MCT for matching rows ─────────────────────────────────

def fetch_matching_rows(search_terms):
    """
    Paginate through all MCT rows and return those whose company name
    (case-insensitive) contains any of the search_terms.

    Returns: dict[term -> list[row_dict]]
    """
    matches = {term: [] for term in search_terms}
    has_more = True
    cursor = None
    page_num = 0
    total = 0

    print(f"\n[Step 2] Scanning Notion MCT for: {search_terms}")

    while has_more:
        page_num += 1
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        r = requests.post(
            f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query",
            headers=notion_headers,
            json=body,
        )
        if r.status_code != 200:
            raise RuntimeError(f"Notion query failed {r.status_code}: {r.text[:400]}")

        data     = r.json()
        batch    = data.get("results", [])
        has_more = data.get("has_more", False)
        cursor   = data.get("next_cursor")
        total   += len(batch)

        print(f"  Page {page_num}: {len(batch)} rows  (running total: {total})")

        for page in batch:
            props = page.get("properties", {})
            company = get_title(props, "🏢 Company Name")
            company_lower = company.lower()

            for term in search_terms:
                if term.lower() in company_lower:
                    row = {
                        "page_id":       page["id"],
                        "customer_name": company or "(no name)",
                        "stripe_id":     get_rich_text(props, "🔗 Stripe Customer ID"),
                        "billing_status": get_select(props, "💰 Billing Status"),
                        "churn_date":    get_date(props, "😢 Churn Date"),
                        "journey_stage": get_formula_string(props, "❤️ Journey Stage"),
                    }
                    matches[term].append(row)

        time.sleep(0.3)

    print(f"  Done. {total} rows scanned.")
    return matches


# ── Step 3: classify Stripe status for a customer ─────────────────────────────

def get_stripe_status(stripe_id):
    """
    Call Stripe and return (stripe_label, list_of_sub_dicts).
    stripe_label is one of:
      "Active in Stripe"
      "Canceled in Stripe"
      "Past Due in Stripe"
      "No subscriptions in Stripe"
      "Stripe API error"
    """
    r = requests.get(
        "https://api.stripe.com/v1/subscriptions",
        headers=stripe_headers,
        params={"customer": stripe_id, "status": "all", "limit": 10},
    )
    if r.status_code != 200:
        return f"Stripe API error ({r.status_code})", []

    subs = r.json().get("data", [])

    if not subs:
        return "No subscriptions in Stripe", []

    statuses = [s["status"] for s in subs]

    if any(s in ("active", "trialing") for s in statuses):
        return "Active in Stripe", subs
    if any(s == "canceled" for s in statuses) and not any(s in ("active", "trialing", "past_due", "unpaid") for s in statuses):
        return "Canceled in Stripe", subs
    if any(s in ("past_due", "unpaid") for s in statuses):
        return "Past Due in Stripe", subs

    # Fallback: mixed or unknown statuses
    return f"Mixed ({', '.join(set(statuses))})", subs


# ── Step 4: compute per-row verdict ──────────────────────────────────────────

def compute_verdict(billing_status, stripe_label):
    """Return a verdict string and whether --fix should apply."""
    n = billing_status or "(empty)"
    s = stripe_label

    if s == "Canceled in Stripe" and n == "Active":
        return "PIPELINE BUG — Stripe says Canceled but Notion still shows Active", True
    if s == "Canceled in Stripe" and n == "Canceled":
        return "Consistent — Stripe Canceled, Notion Canceled ✓", False
    if s == "Canceled in Stripe" and n not in ("Active", "Canceled"):
        return f"PIPELINE BUG — Stripe Canceled but Notion shows '{n}'", True
    if s == "Active in Stripe" and n == "Active":
        return "NOT CHURNED — Stripe has an active subscription ✓", False
    if s == "Active in Stripe":
        return f"Inconsistency — Stripe Active but Notion shows '{n}'", False
    if s == "Past Due in Stripe":
        return f"AT RISK — not yet churned in Stripe (past_due). Notion shows '{n}'", False
    if s == "No subscriptions in Stripe":
        return f"No Stripe subscriptions found. Notion shows '{n}'", False
    return f"Unknown — {s}. Notion shows '{n}'", False


# ── Patch billing status (reused from backfill_billing_status.py) ─────────────

def patch_billing_status(page_id):
    """Set 💰 Billing Status = 'Canceled' on a Notion MCT page."""
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers,
        json={
            "properties": {
                "💰 Billing Status": {
                    "select": {"name": "Canceled"}
                }
            }
        },
    )
    if r.status_code not in (200, 201):
        print(f"    ERROR patching page {page_id}: {r.status_code} — {r.text[:300]}")
        return False
    return True


# ── Format subscription details for display ───────────────────────────────────

def format_subs(subs):
    lines = []
    for s in subs:
        canceled_at = s.get("canceled_at")
        if canceled_at:
            ca_str = datetime.datetime.utcfromtimestamp(canceled_at).strftime("%Y-%m-%d")
        else:
            ca_str = "(not canceled)"

        period_end = s.get("current_period_end")
        pe_str = datetime.datetime.utcfromtimestamp(period_end).strftime("%Y-%m-%d") if period_end else "?"

        lines.append(
            f"    {s['id']}  status={s['status']}  "
            f"canceled_at={ca_str}  "
            f"period_end={pe_str}  "
            f"cancel_at_period_end={s.get('cancel_at_period_end', False)}"
        )
    return lines


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    fix_mode = "--fix" in sys.argv
    extra_names = []
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--name" and i + 1 < len(args):
            extra_names.append(args[i + 1].lower())
            i += 2
        else:
            i += 1
    return fix_mode, extra_names


def main():
    fix_mode, extra_names = parse_args()
    search_terms = DEFAULT_TARGETS + extra_names

    sep = "=" * 68
    print(f"\n{sep}")
    print("  check_customer_churn.py — Targeted churn diagnosis")
    if fix_mode:
        print("  Mode: DIAGNOSE + FIX (will patch Notion for PIPELINE BUG rows)")
    else:
        print("  Mode: DIAGNOSE only  (add --fix to patch Notion)")
    print(sep)

    # ── Step 1: n8n webhook status ────────────────────────────────────────────
    print(f"\n[Step 1] Checking n8n churn webhook ({N8N_WEBHOOK_WF_ID})…")
    wf_name, wf_active = check_n8n_webhook()
    if wf_active is None:
        webhook_line = f"n8n webhook ({N8N_WEBHOOK_WF_ID}): ERROR (could not fetch)"
    elif wf_active:
        webhook_line = f"n8n webhook ({N8N_WEBHOOK_WF_ID}): ACTIVE ✓  ({wf_name})"
    else:
        webhook_line = f"n8n webhook ({N8N_WEBHOOK_WF_ID}): INACTIVE ✗  ({wf_name})  ← real-time updates are silently skipped!"
    print(f"  {webhook_line}")

    # ── Step 2: scan Notion ───────────────────────────────────────────────────
    matches = fetch_matching_rows(search_terms)

    # ── Per-customer output ───────────────────────────────────────────────────
    print(f"\n{sep}")
    print(webhook_line)
    print(sep)

    all_bugs = []

    for term in search_terms:
        rows = matches[term]

        print(f"\n{'─' * 68}")
        print(f"  Search term: \"{term}\"  →  {len(rows)} Notion row(s) found")
        print(f"{'─' * 68}")

        if not rows:
            print("  (no matching customer found in Notion MCT)")
            continue

        for row in rows:
            print(f"\n  Customer : {row['customer_name']}")
            print(f"  Notion page  : {row['page_id']}")

            stripe_id = row["stripe_id"]
            if not stripe_id:
                print(f"  Stripe ID    : (not set in Notion)")
                print(f"  Notion status: {row['billing_status'] or '(empty)'}")
                print(f"  Churn date   : {row['churn_date'] or '(empty)'}")
                print(f"  Journey stage: {row['journey_stage'] or '(empty)'}")
                print()
                print(f"  VERDICT: Cannot check — no Stripe ID in Notion")
                continue

            print(f"  Stripe ID    : {stripe_id}")
            print(f"  Notion status: {row['billing_status'] or '(empty)'}")
            print(f"  Churn date   : {row['churn_date'] or '(empty)'}")
            print(f"  Journey stage: {row['journey_stage'] or '(empty)'}")

            print(f"\n  Checking Stripe subscriptions…", end=" ", flush=True)
            stripe_label, subs = get_stripe_status(stripe_id)
            print(stripe_label)
            time.sleep(0.2)

            if subs:
                print(f"\n  Stripe subscriptions ({len(subs)}):")
                for line in format_subs(subs):
                    print(line)
            else:
                print(f"\n  Stripe subscriptions: (none)")

            verdict_text, is_bug = compute_verdict(row["billing_status"], stripe_label)
            print()
            print(f"  VERDICT: {verdict_text}")

            if is_bug:
                all_bugs.append(row)
                if fix_mode:
                    print(f"           → Patching Notion…", end=" ", flush=True)
                    ok = patch_billing_status(row["page_id"])
                    if ok:
                        print("✓ Billing Status set to 'Canceled'")
                    else:
                        print("✗ PATCH failed — see error above")
                else:
                    print(f"           → run with --fix to patch Billing Status = 'Canceled'")

    # ── Final summary ─────────────────────────────────────────────────────────
    print(f"\n{sep}")
    print("  SUMMARY")
    print(sep)
    print(f"  {webhook_line}")
    print()

    if not all_bugs:
        print("  No pipeline bugs found. Pipeline is working correctly for these customers.")
    else:
        print(f"  Pipeline bugs found: {len(all_bugs)} row(s) need Notion update")
        for row in all_bugs:
            print(f"    • {row['customer_name']}  (page {row['page_id'][:8]}…  stripe={row['stripe_id']})")
        if not fix_mode:
            print()
            print("  → Re-run with --fix to apply the patches automatically")

    if not wf_active:
        print()
        print("  ATTENTION: The n8n churn webhook is INACTIVE.")
        print(f"  Open n8n UI → workflow {N8N_WEBHOOK_WF_ID} and toggle it ON")
        print("  so that future Stripe cancellations update Notion in real time.")

    print(sep)
    print()


if __name__ == "__main__":
    main()
