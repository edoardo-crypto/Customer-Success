#!/usr/bin/env python3
"""
sync_billing_status.py — Authoritative Stripe → Notion billing status reconciliation

Reads every MCT row, cross-references with Stripe subscriptions, and corrects
Billing Status + date fields wherever they've drifted.

Handles all transitions:
  Active ↔ Churning ↔ Canceled (including reactivations)

Date fields populated from Stripe timestamps:
  📅 Churning Since  ← canceled_at  (decision moment)
  📅 Cancel Date     ← cancel_at    (scheduled end)
  😢 Churn Date      ← ended_at     (actual end)

Designed to run daily via GitHub Actions AFTER the n8n daily sync (which may
overwrite correct Churning status). This script is the correction safety net.

Usage:
    python3 sync_billing_status.py                  # live: fix all mismatches
    python3 sync_billing_status.py --dry-run        # preview only
    python3 sync_billing_status.py --create-missing # also create MCT rows for missing Stripe customers
"""

import os
import sys
import time
import datetime
import requests

# ── Credentials ───────────────────────────────────────────────────────────────


def _extract_block(header):
    try:
        raw = open("Credentials.md").read()
        start = raw.index(f"## {header}")
        block_start = raw.index("```", start) + 3
        if raw[block_start] not in ("\n", "\r"):
            block_start = raw.index("\n", block_start) + 1
        block_end = raw.index("```", block_start)
        return raw[block_start:block_end].strip()
    except (FileNotFoundError, ValueError):
        return None


STRIPE_KEY = os.environ.get("STRIPE_KEY") or _extract_block("Stripe")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN") or _extract_block("Notion")

if not STRIPE_KEY or not NOTION_TOKEN:
    print("ERROR: STRIPE_KEY and NOTION_TOKEN must be set (env var or Credentials.md)")
    sys.exit(1)

# ── CLI flags ─────────────────────────────────────────────────────────────────

DRY_RUN = "--dry-run" in sys.argv
CREATE_MISSING = "--create-missing" in sys.argv

# ── Constants ─────────────────────────────────────────────────────────────────

MCT_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

NOTION_HDR = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}

PLAN_MAP = {
    "prod_PiZIdx6sQck09F": "Start",
    "prod_PiZGbaJfBGRj9a": "Scale",
    "prod_Rcv25HkIvkRbhp": "All In",
}

# ── Notion helpers ────────────────────────────────────────────────────────────


def get_title(page, prop):
    p = page.get("properties", {}).get(prop)
    return "".join(t.get("plain_text", "") for t in (p or {}).get("title", []))


def get_rich_text(page, prop):
    p = page.get("properties", {}).get(prop)
    return "".join(t.get("plain_text", "") for t in (p or {}).get("rich_text", []))


def get_select(page, prop):
    p = page.get("properties", {}).get(prop)
    if not p or not p.get("select"):
        return ""
    return p["select"].get("name", "")


def get_date(page, prop):
    p = page.get("properties", {}).get(prop)
    if not p or p.get("type") != "date":
        return None
    d = p.get("date")
    return d.get("start") if d else None


def get_number(page, prop):
    p = page.get("properties", {}).get(prop)
    return p.get("number") if p else None


def fetch_all_mct():
    pages = []
    body = {"page_size": 100}
    url = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"
    while True:
        r = requests.post(url, headers=NOTION_HDR, json=body, timeout=30)
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        body["start_cursor"] = data["next_cursor"]
    return pages


def patch_mct_page(page_id, properties):
    """PATCH an MCT page. Returns True on success."""
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HDR,
        json={"properties": properties},
        timeout=20,
    )
    if r.ok:
        return True
    print(f"  [ERROR] PATCH {page_id[:8]}… failed {r.status_code}: {r.text[:300]}")
    return False


# ── Stripe helpers ────────────────────────────────────────────────────────────


def fetch_stripe_subs(status):
    """Fetch all Stripe subscriptions for a given status, paginated."""
    subs = []
    params = {"status": status, "limit": 100}
    url = "https://api.stripe.com/v1/subscriptions"
    while True:
        r = requests.get(url, auth=(STRIPE_KEY, ""), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        subs.extend(data.get("data", []))
        if not data.get("has_more"):
            break
        params["starting_after"] = data["data"][-1]["id"]
    return subs


def build_stripe_map(all_subs):
    """Build dict: customer_id -> list of subscription dicts."""
    mapping = {}
    for sub in all_subs:
        cust = sub.get("customer")
        cid = cust if isinstance(cust, str) else (cust or {}).get("id", "")
        if not cid:
            continue
        mapping.setdefault(cid, []).append(sub)
    return mapping


def classify_stripe(subs):
    """
    Given a list of subscriptions for one customer, return expected
    Notion Billing Status: "Active", "Churning", "Canceled", or None.

    Priority: active/past_due subs beat canceled ones.
    Within live subs, cancel_at_period_end=True -> Churning.
    """
    if not subs:
        return None

    live = [s for s in subs if s.get("status") in ("active", "past_due")]
    if live:
        if any(s.get("cancel_at_period_end") for s in live):
            return "Churning"
        if all(s.get("status") == "past_due" for s in live):
            return "Past Due"
        return "Active"

    if all(s.get("status") == "canceled" for s in subs):
        return "Canceled"

    return None


def _ts_to_date(ts):
    """Convert a Unix timestamp to ISO date string, or None."""
    if not ts:
        return None
    return datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")


def compute_date_fields(subs, expected_status):
    """
    Compute the expected date field values based on Stripe subscription data.

    Returns dict with keys:
        churning_since, cancel_date, churn_date
    Each value is an ISO date string or None (meaning clear the field).
    """
    if expected_status in ("Active", "Past Due"):
        # Reactivation / past due: clear all churn-related dates
        return {"churning_since": None, "cancel_date": None, "churn_date": None}

    # For Churning or Canceled, find the relevant subscription
    # Prefer live churning sub; fall back to most recently canceled
    live = [s for s in subs if s.get("status") in ("active", "past_due")]
    canceled = [s for s in subs if s.get("status") == "canceled"]

    if expected_status == "Churning":
        # Use the live sub that has cancel_at_period_end=True
        churning_subs = [s for s in live if s.get("cancel_at_period_end")]
        target = churning_subs[0] if churning_subs else (live[0] if live else subs[0])
        return {
            "churning_since": _ts_to_date(target.get("canceled_at")),
            "cancel_date": _ts_to_date(target.get("cancel_at")),
            "churn_date": None,  # not ended yet
        }

    if expected_status == "Canceled":
        # Use the most recently canceled sub (by ended_at)
        target = max(canceled, key=lambda s: s.get("ended_at") or 0) if canceled else subs[0]
        return {
            "churning_since": _ts_to_date(target.get("canceled_at")),
            "cancel_date": _ts_to_date(target.get("cancel_at")),
            "churn_date": _ts_to_date(target.get("ended_at")),
        }

    return {"churning_since": None, "cancel_date": None, "churn_date": None}


# ── MRR helpers (for --create-missing) ────────────────────────────────────────


def _tier_unit_amount_cents(tier):
    amt = tier.get("unit_amount")
    if amt is not None:
        return amt
    decimal = tier.get("unit_amount_decimal")
    return float(decimal) if decimal else 0


def _compute_tiered_amount_cents(price, quantity):
    tiers = price.get("tiers") or []
    tiers_mode = price.get("tiers_mode", "volume")

    if tiers_mode == "volume":
        for tier in tiers:
            up_to = tier.get("up_to")
            if up_to is None or quantity <= up_to:
                return _tier_unit_amount_cents(tier) * quantity + (tier.get("flat_amount") or 0)
        return 0

    total, prev = 0, 0
    for tier in tiers:
        up_to = tier.get("up_to")
        tier_end = up_to if up_to is not None else float("inf")
        units = min(quantity, tier_end) - prev
        if units <= 0:
            break
        total += _tier_unit_amount_cents(tier) * units + (tier.get("flat_amount") or 0)
        prev = tier_end
        if quantity <= tier_end:
            break
    return total


def _metered_amount_from_invoice(subscription, price_id):
    invoice = subscription.get("latest_invoice") or {}
    lines = invoice.get("lines", {}).get("data", [])
    for line in lines:
        lp = line.get("price") or line.get("plan") or {}
        if lp.get("id") == price_id:
            return line.get("amount", 0)
    return 0


def compute_mrr(subscription):
    mrr = 0
    for item in subscription.get("items", {}).get("data", []):
        price = item.get("price") or item.get("plan") or {}
        qty = item.get("quantity")
        recurring = price.get("recurring") or {}
        interval = recurring.get("interval") or (item.get("plan") or {}).get("interval") or "month"
        interval_count = recurring.get("interval_count") or (item.get("plan") or {}).get("interval_count") or 1
        usage_type = recurring.get("usage_type") or "licensed"

        if usage_type == "metered":
            amount_cents = _metered_amount_from_invoice(subscription, price.get("id"))
        elif price.get("billing_scheme") == "tiered":
            tiers = price.get("tiers") or []
            if tiers:
                amount_cents = _compute_tiered_amount_cents(price, qty or 1)
            else:
                amount_cents = _metered_amount_from_invoice(subscription, price.get("id"))
        else:
            amount_cents = (price.get("unit_amount") or 0) * (qty or 1)

        if interval == "month" and interval_count == 1:
            mrr += amount_cents / 100
        elif interval == "year" or (interval == "month" and interval_count == 12):
            mrr += amount_cents / 100 / 12
        elif interval == "month":
            mrr += amount_cents / 100 / interval_count

    return round(mrr, 2)


def enrich_tiered_prices(subs):
    """Fetch full tier data for any tiered prices missing it."""
    tiered_ids = {
        (item.get("price") or {}).get("id")
        for sub in subs
        for item in sub.get("items", {}).get("data", [])
        if (item.get("price") or {}).get("billing_scheme") == "tiered"
           and not (item.get("price") or {}).get("tiers")
    } - {None}

    if not tiered_ids:
        return

    price_tiers = {}
    for pid in tiered_ids:
        r = requests.get(
            f"https://api.stripe.com/v1/prices/{pid}",
            auth=(STRIPE_KEY, ""),
            params={"expand[]": "tiers"},
            timeout=15,
        )
        r.raise_for_status()
        price_tiers[pid] = r.json().get("tiers") or []

    for sub in subs:
        for item in sub.get("items", {}).get("data", []):
            p = item.get("price") or {}
            if p.get("id") in price_tiers:
                p["tiers"] = price_tiers[p["id"]]


# ── Create missing MCT rows ──────────────────────────────────────────────────


def fetch_stripe_customer(customer_id):
    """Fetch a single Stripe customer object."""
    r = requests.get(
        f"https://api.stripe.com/v1/customers/{customer_id}",
        auth=(STRIPE_KEY, ""),
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def fetch_subs_with_expand(customer_id):
    """Fetch subs for a customer with price + invoice expansion (for MRR)."""
    r = requests.get(
        "https://api.stripe.com/v1/subscriptions",
        auth=(STRIPE_KEY, ""),
        params={
            "customer": customer_id,
            "status": "all",
            "expand[]": [
                "data.items.data.price",
                "data.latest_invoice.lines.data",
            ],
            "limit": 10,
        },
        timeout=20,
    )
    r.raise_for_status()
    return r.json().get("data", [])


def create_mct_row(customer, subs, expected_status, date_fields):
    """Create a new MCT row in Notion for a Stripe customer."""
    # MRR from live subs only
    live_subs = [s for s in subs if s.get("status") in ("active", "past_due")]
    enrich_tiered_prices(live_subs)
    mrr = round(sum(compute_mrr(s) for s in live_subs), 2) if live_subs else 0

    # Plan tier
    plan_tier = "Custom"
    for sub in live_subs:
        for item in sub.get("items", {}).get("data", []):
            price = item.get("price") or {}
            product = price.get("product") or ""
            if isinstance(product, dict):
                product = product.get("id", "")
            if product in PLAN_MAP:
                plan_tier = PLAN_MAP[product]
                break
        else:
            continue
        break

    # Domain from email
    email = customer.get("email") or ""
    domain = email.split("@")[-1] if "@" in email else ""

    # Dates from first live sub
    sub0 = live_subs[0] if live_subs else (subs[0] if subs else {})
    contract_start = _ts_to_date(sub0.get("created"))
    renewal_date = _ts_to_date(sub0.get("current_period_end"))

    properties = {
        "🏢 Company Name": {"title": [{"text": {"content": customer.get("name") or customer["id"]}}]},
        "🏢 Domain": {"rich_text": [{"text": {"content": domain}}]},
        "🔗 Stripe Customer ID": {"rich_text": [{"text": {"content": customer["id"]}}]},
        "💰 Plan Tier": {"select": {"name": plan_tier}},
        "💰 MRR": {"number": mrr},
        "💰 Billing Status": {"select": {"name": expected_status}},
        "Meeting Scheduled": {"select": {"name": "No"}},
    }
    if contract_start:
        properties["📋 Contract Start"] = {"date": {"start": contract_start}}
    if renewal_date:
        properties["📋 Renewal Date"] = {"date": {"start": renewal_date}}
    properties["📆 Kickoff Date"] = {"date": {"start": datetime.date.today().isoformat()}}

    # Churn date fields
    if date_fields.get("churning_since"):
        properties["📅 Churning Since"] = {"date": {"start": date_fields["churning_since"]}}
    if date_fields.get("cancel_date"):
        properties["📅 Cancel Date"] = {"date": {"start": date_fields["cancel_date"]}}
    if date_fields.get("churn_date"):
        properties["😢 Churn Date"] = {"date": {"start": date_fields["churn_date"]}}

    payload = {
        "parent": {"data_source_id": MCT_DS_ID},
        "properties": properties,
    }

    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=NOTION_HDR,
        json=payload,
        timeout=20,
    )
    if not r.ok:
        print(f"  [ERROR] Create failed {r.status_code}: {r.text[:300]}")
        return False

    page = r.json()
    print(f"  Created: {customer.get('name') or customer['id']} (page {page['id'][:8]}…)")
    return True


# ── Reconciliation ────────────────────────────────────────────────────────────


def _build_patch_properties(expected_status, date_fields, current_status, current_dates):
    """
    Compare expected vs current values and return a properties dict
    with only the fields that need changing. Returns empty dict if nothing changed.
    """
    props = {}

    if current_status != expected_status:
        props["💰 Billing Status"] = {"select": {"name": expected_status}}

    # Date field comparisons
    field_map = {
        "📅 Churning Since": date_fields.get("churning_since"),
        "📅 Cancel Date": date_fields.get("cancel_date"),
        "😢 Churn Date": date_fields.get("churn_date"),
    }

    for notion_field, expected_val in field_map.items():
        current_val = current_dates.get(notion_field)
        if expected_val != current_val:
            if expected_val:
                props[notion_field] = {"date": {"start": expected_val}}
            else:
                # Clear the date field
                props[notion_field] = {"date": None}

    return props


def reconcile(pages, stripe_map):
    """Main reconciliation loop. Returns (patched, skipped, orphans, errors)."""
    mct_stripe_ids = set()
    patched = 0
    skipped = 0
    orphans = []
    errors = 0
    no_stripe_id = 0

    for page in pages:
        page_id = page["id"]
        company = get_title(page, "🏢 Company Name").strip() or "(no name)"
        current_status = get_select(page, "💰 Billing Status").strip()
        stripe_id = get_rich_text(page, "🔗 Stripe Customer ID").strip()

        if not stripe_id:
            no_stripe_id += 1
            continue

        if not stripe_id.startswith("cus_"):
            print(f"  [WARN] {company}: malformed Stripe ID '{stripe_id}' — skipping")
            skipped += 1
            continue

        mct_stripe_ids.add(stripe_id)
        subs = stripe_map.get(stripe_id, [])

        if not subs:
            orphans.append((company, stripe_id, current_status))
            continue

        expected_status = classify_stripe(subs)
        if not expected_status:
            orphans.append((company, stripe_id, current_status))
            continue

        date_fields = compute_date_fields(subs, expected_status)

        current_dates = {
            "📅 Churning Since": get_date(page, "📅 Churning Since"),
            "📅 Cancel Date": get_date(page, "📅 Cancel Date"),
            "😢 Churn Date": get_date(page, "😢 Churn Date"),
        }

        props = _build_patch_properties(expected_status, date_fields, current_status, current_dates)

        if not props:
            skipped += 1
            continue

        # Build change description
        changes = []
        if "💰 Billing Status" in props:
            changes.append(f"status: {current_status or '(empty)'} → {expected_status}")
        date_changes = [k for k in ("📅 Churning Since", "📅 Cancel Date", "😢 Churn Date") if k in props]
        if date_changes:
            changes.append(f"dates: {', '.join(date_changes)}")

        print(f"  {company}: {' | '.join(changes)}")

        if DRY_RUN:
            patched += 1
            continue

        if patch_mct_page(page_id, props):
            patched += 1
        else:
            errors += 1
        time.sleep(0.3)  # rate limit

    return patched, skipped, orphans, errors, no_stripe_id, mct_stripe_ids


def handle_missing_customers(stripe_map, mct_stripe_ids):
    """Create MCT rows for Stripe customers that don't exist in MCT yet."""
    # Only consider customers with at least one live sub
    missing = []
    for cid, subs in stripe_map.items():
        if cid in mct_stripe_ids:
            continue
        live = [s for s in subs if s.get("status") in ("active", "past_due")]
        if live:
            missing.append(cid)

    if not missing:
        print("\nNo active Stripe customers missing from MCT.")
        return 0

    print(f"\n--- Missing Stripe customers ({len(missing)}) ---")
    created = 0
    for cid in missing:
        subs = stripe_map[cid]
        expected = classify_stripe(subs)
        date_fields = compute_date_fields(subs, expected or "Active")

        # Need customer details + expanded subs for MRR
        try:
            customer = fetch_stripe_customer(cid)
        except Exception as e:
            print(f"  [ERROR] Could not fetch customer {cid}: {e}")
            continue

        name = customer.get("name") or customer.get("email") or cid
        print(f"  {name} ({cid}): {expected or 'Active'}")

        if DRY_RUN:
            created += 1
            continue

        # Fetch expanded subs for MRR computation
        try:
            expanded_subs = fetch_subs_with_expand(cid)
        except Exception as e:
            print(f"  [ERROR] Could not fetch expanded subs for {cid}: {e}")
            continue

        if create_mct_row(customer, expanded_subs, expected or "Active", date_fields):
            created += 1
        time.sleep(0.5)

    return created


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    print(f"{'='*65}")
    print(f"  sync_billing_status.py  |  {mode}  |  {datetime.date.today()}")
    print(f"{'='*65}\n")

    # ── Fetch MCT ─────────────────────────────────────────────────────────────
    print("Fetching MCT rows…")
    pages = fetch_all_mct()
    print(f"  {len(pages)} rows")

    # ── Fetch all Stripe subs ─────────────────────────────────────────────────
    print("Fetching Stripe subscriptions…")
    active_subs = fetch_stripe_subs("active")
    past_due_subs = fetch_stripe_subs("past_due")
    canceled_subs = fetch_stripe_subs("canceled")
    all_subs = active_subs + past_due_subs + canceled_subs
    print(f"  {len(active_subs)} active + {len(past_due_subs)} past_due + {len(canceled_subs)} canceled = {len(all_subs)} total")

    stripe_map = build_stripe_map(all_subs)

    # ── Reconcile ─────────────────────────────────────────────────────────────
    print(f"\nReconciling ({mode})…")
    patched, skipped, orphans, errors, no_stripe_id, mct_stripe_ids = reconcile(pages, stripe_map)

    # ── Orphans report ────────────────────────────────────────────────────────
    if orphans:
        print(f"\n--- Orphaned Stripe IDs ({len(orphans)}) ---")
        for company, sid, billing in orphans:
            print(f"  {company}: {sid} (Notion: {billing or '(empty)'})")

    # ── Missing customers ─────────────────────────────────────────────────────
    created = 0
    if CREATE_MISSING:
        created = handle_missing_customers(stripe_map, mct_stripe_ids)

    # ── Summary ───────────────────────────────────────────────────────────────
    verb = "would patch" if DRY_RUN else "patched"
    create_verb = "would create" if DRY_RUN else "created"
    print(f"\n{'='*65}")
    print(f"  {patched} {verb} | {skipped} already correct | {len(orphans)} orphans | {no_stripe_id} no Stripe ID | {errors} errors")
    if CREATE_MISSING:
        print(f"  {created} {create_verb} (missing customers)")
    print(f"{'='*65}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
