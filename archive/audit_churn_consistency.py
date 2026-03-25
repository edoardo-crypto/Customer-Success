#!/usr/bin/env python3
"""
audit_churn_consistency.py — Read-only churn pipeline audit

Checks two things:
  A) Notion MCT consistency: every Canceled row has a Churn Date, and every
     Churn Date belongs to a Canceled row.
  B) n8n webhook workflow 8cLtcqxjD8DC59JG is ACTIVE (the Stripe→Billing Status pipe).

Optional:
  C) --check-stripe: for each Active Notion row, verify Stripe actually has an
     active/trialing subscription (flags ghost rows).

Usage:
  python3 audit_churn_consistency.py               # Notion + n8n check only (~10s)
  python3 audit_churn_consistency.py --check-stripe  # + Stripe cross-ref (~30s extra)
"""

import sys
import time
import requests
import creds

# ── Credentials ───────────────────────────────────────────────────────────────
STRIPE_KEY      = creds.get("STRIPE_KEY")
NOTION_TOKEN    = creds.get("NOTION_TOKEN")
NOTION_DS_ID    = "3ceb1ad0-91f1-40db-945a-c51c58035898"
N8N_BASE_URL    = "https://konvoai.app.n8n.cloud"
N8N_API_KEY     = creds.get("N8N_API_KEY")
N8N_WORKFLOW_ID = "8cLtcqxjD8DC59JG"

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


# ── Property helpers ──────────────────────────────────────────────────────────

def get_title(props, key):
    texts = props.get(key, {}).get("title", [])
    return texts[0].get("plain_text", "") if texts else ""


def get_select(props, key):
    sel = props.get(key, {}).get("select") or {}
    return sel.get("name", "")


def get_rich_text(props, key):
    texts = props.get(key, {}).get("rich_text", [])
    return texts[0].get("plain_text", "") if texts else ""


def get_date(props, key):
    d = (props.get(key, {}).get("date") or {})
    return d.get("start", "")


def get_formula_string(props, key):
    f = props.get(key, {}).get("formula") or {}
    return f.get("string", "")


# ── Step 1: n8n webhook check ─────────────────────────────────────────────────

def check_n8n_webhook():
    """Return (workflow_name, is_active) for the Stripe churn webhook workflow."""
    url = f"{N8N_BASE_URL}/api/v1/workflows/{N8N_WORKFLOW_ID}"
    r = requests.get(url, headers=n8n_headers)
    if r.status_code != 200:
        return f"[HTTP {r.status_code}]", None
    data = r.json()
    return data.get("name", "(unnamed)"), data.get("active", False)


# ── Step 2: fetch all MCT rows ────────────────────────────────────────────────

def fetch_all_mct_rows():
    """
    Paginate through all MCT rows via data_sources query.
    Returns list of raw Notion page objects.
    """
    rows     = []
    has_more = True
    cursor   = None
    page_num = 0

    while has_more:
        page_num += 1
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        r = requests.post(
            f"https://api.notion.com/v1/data_sources/{NOTION_DS_ID}/query",
            headers=notion_headers,
            json=body,
        )
        if r.status_code != 200:
            raise RuntimeError(f"Notion query failed {r.status_code}: {r.text[:400]}")

        data     = r.json()
        batch    = data.get("results", [])
        has_more = data.get("has_more", False)
        cursor   = data.get("next_cursor")

        rows.extend(batch)
        print(f"  Page {page_num}: {len(batch)} rows  (running total: {len(rows)})")
        time.sleep(0.3)

    return rows


# ── Step 3: classify rows ─────────────────────────────────────────────────────

def classify_rows(rows):
    """
    Returns four buckets:
      consistent_churned — Billing Status=Canceled AND Churn Date set
      anomaly_a          — Billing Status=Canceled AND Churn Date EMPTY
      anomaly_b          — Churn Date set AND Billing Status != Canceled
      active             — Billing Status=Active
      other              — everything else (Trialing, Past Due, empty, …)
    """
    consistent_churned = []
    anomaly_a          = []
    anomaly_b          = []
    active             = []
    other              = []

    for page in rows:
        props = page.get("properties", {})

        company       = get_title(props, "🏢 Company Name")
        stripe_id     = get_rich_text(props, "🔗 Stripe Customer ID")
        billing       = get_select(props, "💰 Billing Status")
        churn_date    = get_date(props, "😢 Churn Date")
        journey_stage = get_formula_string(props, "❤️ Journey Stage")
        cs_owner      = get_select(props, "⭐ CS Owner")

        row = {
            "company":       company or "(no name)",
            "stripe_id":     stripe_id,
            "billing":       billing,
            "churn_date":    churn_date,
            "journey_stage": journey_stage,
            "cs_owner":      cs_owner,
            "page_id":       page["id"],
        }

        if billing == "Canceled" and churn_date:
            consistent_churned.append(row)
        elif billing == "Canceled" and not churn_date:
            anomaly_a.append(row)
        elif churn_date and billing != "Canceled":
            anomaly_b.append(row)
        elif billing == "Active":
            active.append(row)
        else:
            other.append(row)

    return consistent_churned, anomaly_a, anomaly_b, active, other


# ── Step 4 (optional): Stripe cross-reference ─────────────────────────────────

def check_stripe_active_rows(active_rows):
    """
    For each Notion row with Billing Status=Active, call Stripe to verify
    the customer actually has an active or trialing subscription.

    Returns list of mismatches (Notion says Active but Stripe disagrees).
    """
    mismatches = []

    for i, row in enumerate(active_rows, 1):
        sid = row["stripe_id"]
        if not sid:
            mismatches.append({**row, "stripe_note": "No Stripe ID in Notion"})
            continue

        print(f"  [{i}/{len(active_rows)}] Checking Stripe for {sid}…", end=" ", flush=True)

        r = requests.get(
            "https://api.stripe.com/v1/subscriptions",
            headers=stripe_headers,
            params={"customer": sid, "status": "all", "limit": 10},
        )
        if r.status_code != 200:
            print(f"HTTP {r.status_code}")
            mismatches.append({**row, "stripe_note": f"Stripe API error {r.status_code}"})
            time.sleep(0.15)
            continue

        subs = r.json().get("data", [])
        has_live = any(s["status"] in ("active", "trialing") for s in subs)

        if has_live:
            print("OK")
        else:
            statuses = [s["status"] for s in subs] if subs else ["no subscriptions"]
            note = ", ".join(statuses)
            print(f"MISMATCH ({note})")
            mismatches.append({**row, "stripe_note": note})

        time.sleep(0.15)

    return mismatches


# ── Print helpers ─────────────────────────────────────────────────────────────

COL_COMPANY = 40
COL_STRIPE  = 22
COL_JOURNEY = 35
COL_DATE    = 12


def print_anomaly_table(rows, title):
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  {title} ({len(rows)} rows)")
    print(sep)
    if not rows:
        print("  (none)")
        return

    header = (
        f"  {'Company':<{COL_COMPANY}}"
        f"  {'Stripe ID':<{COL_STRIPE}}"
        f"  {'Churn Date':<{COL_DATE}}"
        f"  Journey Stage"
    )
    print(header)
    print(f"  {'-'*COL_COMPANY}  {'-'*COL_STRIPE}  {'-'*COL_DATE}  {'-'*COL_JOURNEY}")

    for row in rows:
        print(
            f"  {row['company']:<{COL_COMPANY}}"
            f"  {row['stripe_id']:<{COL_STRIPE}}"
            f"  {row['churn_date'] or '(empty)':<{COL_DATE}}"
            f"  {row['journey_stage']}"
        )


def print_stripe_mismatch_table(rows):
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  STRIPE MISMATCH — Active in Notion but NOT active in Stripe ({len(rows)} rows)")
    print(sep)
    if not rows:
        print("  (none)")
        return

    header = (
        f"  {'Company':<{COL_COMPANY}}"
        f"  {'Stripe ID':<{COL_STRIPE}}"
        f"  Stripe subs status"
    )
    print(header)
    print(f"  {'-'*COL_COMPANY}  {'-'*COL_STRIPE}  {'-'*35}")

    for row in rows:
        print(
            f"  {row['company']:<{COL_COMPANY}}"
            f"  {row['stripe_id']:<{COL_STRIPE}}"
            f"  {row.get('stripe_note', '')}"
        )


# ── Verdict ───────────────────────────────────────────────────────────────────

def verdict(anomaly_a, anomaly_b, stripe_mismatches, webhook_active, check_stripe):
    n_anomalies = len(anomaly_a) + len(anomaly_b)
    n_stripe    = len(stripe_mismatches)

    if n_anomalies == 0 and webhook_active and (not check_stripe or n_stripe == 0):
        return "HEALTHY", "Pipeline is fully automatic and data is consistent. ✓"
    elif n_anomalies == 0 and webhook_active and n_stripe > 0:
        return "WARNING", (
            f"{n_stripe} customer(s) show Active in Notion but past_due/unpaid in Stripe. "
            "These are not yet churned but may need CS attention."
        )
    elif n_anomalies == 0 and not webhook_active:
        return "WARNING", "Data is consistent but webhook is INACTIVE — Stripe won't trigger updates. Re-activate in n8n UI!"
    elif n_anomalies > 0 and webhook_active:
        return "ANOMALIES", f"{n_anomalies} Notion consistency anomaly(ies) found — see tables above."
    else:
        return "ISSUES", f"{n_anomalies} Notion anomaly(ies) + webhook INACTIVE — multiple issues to fix."


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    check_stripe = "--check-stripe" in sys.argv
    sep = "=" * 80

    print(f"\n{sep}")
    print("  audit_churn_consistency.py — read-only")
    if check_stripe:
        print("  Mode: Notion + n8n + Stripe cross-reference")
    else:
        print("  Mode: Notion + n8n  (add --check-stripe for Stripe cross-reference)")
    print(sep)

    # ── Step 1: n8n webhook ──────────────────────────────────────────────────
    print(f"\n[Step 1] Checking n8n webhook workflow status ({N8N_WORKFLOW_ID})…")
    wf_name, wf_active = check_n8n_webhook()
    if wf_active is None:
        print(f"  ERROR: could not fetch workflow")
        wf_active = False
    elif wf_active:
        print(f'  ACTIVE ✓  — "{wf_name}"')
    else:
        print(f'  INACTIVE ✗  — "{wf_name}"  <- ATTENTION: webhook is not listening')

    # ── Step 2: fetch all MCT rows ───────────────────────────────────────────
    print(f"\n[Step 2] Fetching all Notion MCT rows…")
    rows = fetch_all_mct_rows()
    print(f"  Total rows fetched: {len(rows)}")

    # ── Step 3: classify ────────────────────────────────────────────────────
    print(f"\n[Step 3] Classifying rows…")
    consistent_churned, anomaly_a, anomaly_b, active_rows, other_rows = classify_rows(rows)

    # ── Step 4 (optional): Stripe check ─────────────────────────────────────
    stripe_mismatches = []
    if check_stripe:
        print(f"\n[Step 4] Stripe cross-reference for {len(active_rows)} Active Notion rows…")
        stripe_mismatches = check_stripe_active_rows(active_rows)

    # ── Print anomaly tables ─────────────────────────────────────────────────
    print_anomaly_table(
        anomaly_a,
        "ANOMALY A — Billing Status=Canceled, Churn Date EMPTY"
    )
    print_anomaly_table(
        anomaly_b,
        "ANOMALY B — Churn Date set, Billing Status ≠ Canceled"
    )
    if check_stripe:
        print_stripe_mismatch_table(stripe_mismatches)

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{sep}")
    print("  SUMMARY")
    print(sep)

    webhook_label = "ACTIVE ✓" if wf_active else "INACTIVE ✗"
    print(f"  n8n workflow {N8N_WORKFLOW_ID}:  {webhook_label}  ({wf_name})")
    print()
    print(f"  Total customers in Notion:            {len(rows)}")
    print(f"  Churned, fully consistent:            {len(consistent_churned)}")
    print(f"  Active:                               {len(active_rows)}")
    print(f"  Other (Trialing / Past Due / empty):  {len(other_rows)}")
    print()
    print(f"  ANOMALY A (Canceled, no Churn Date):  {len(anomaly_a)}   ← target: 0")
    print(f"  ANOMALY B (Churn Date, not Canceled): {len(anomaly_b)}   ← target: 0")
    if check_stripe:
        print(f"  Stripe mismatch (Active but no sub):  {len(stripe_mismatches)}   ← target: 0")

    # ── Remediation hints ────────────────────────────────────────────────────
    if anomaly_a:
        print()
        print("  Remediation for Anomaly A:")
        print("  → The Notion automation (Billing Status=Canceled → Churn Date=today) may")
        print("    not have been set up yet when these rows were updated by the backfill.")
        print("  → Open each flagged row in Notion and manually set 😢 Churn Date,")
        print("    or trigger the automation by re-saving Billing Status = Canceled.")

    if anomaly_b:
        print()
        print("  Remediation for Anomaly B:")
        print("  → These rows have a Churn Date but Billing Status is not Canceled.")
        print("  → Likely manual data entry. Review each row and correct Billing Status.")

    if stripe_mismatches:
        print()
        print("  Note on Stripe mismatches (past_due / unpaid):")
        print("  → These customers are NOT yet canceled in Stripe — they are in arrears.")
        print("  → Billing Status will be set to 'Canceled' automatically when Stripe")
        print("    fires customer.subscription.updated with status=canceled.")
        print("  → Consider reaching out to these customers about payment issues.")

    if not wf_active:
        print()
        print("  Remediation for inactive webhook:")
        print(f"  → Open n8n UI → workflow {N8N_WORKFLOW_ID}")
        print("  → Toggle the workflow ON to start receiving Stripe subscription events.")

    # ── Verdict ──────────────────────────────────────────────────────────────
    status, message = verdict(anomaly_a, anomaly_b, stripe_mismatches, wf_active, check_stripe)
    print()
    print(f"  VERDICT [{status}]: {message}")
    print(sep)
    print()

    # Exit with non-zero if anomalies detected
    if status not in ("HEALTHY",):
        sys.exit(1)


if __name__ == "__main__":
    main()
