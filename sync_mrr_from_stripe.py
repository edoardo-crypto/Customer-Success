#!/usr/bin/env python3
"""
sync_mrr_from_stripe.py — Update MCT MRR from Stripe (base sub floor + charges)

MRR = max(base_subscription_price, last_month_charges)

The base plan price is always the floor. If a customer spent more than their base
plan last month (usage charges), the higher number is used as a proxy for ongoing
usage. This prevents low-usage months from producing absurdly low MRR values.

Edge cases:
  - charges > base_sub → use charges (usage-heavy month)
  - charges < base_sub → use base_sub (low-usage month)
  - no charges, has sub → use base_sub (annual billing)
  - has charges, no sub → use charges (just canceled)
  - neither            → skip, keep existing MCT MRR

Usage:
    python3 sync_mrr_from_stripe.py              # live
    DRY_RUN=true python3 sync_mrr_from_stripe.py  # preview only
"""

import os
import sys
import time
import datetime
import calendar
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

DRY_RUN = os.environ.get("DRY_RUN", "false").lower() not in ("false", "0", "no")

# ── Constants ─────────────────────────────────────────────────────────────────

MCT_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

NOTION_HDR = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}

# ── Notion helpers ────────────────────────────────────────────────────────────


def get_title(page, prop):
    p = page.get("properties", {}).get(prop)
    return "".join(t.get("plain_text", "") for t in (p or {}).get("title", []))


def get_rich_text(page, prop):
    p = page.get("properties", {}).get(prop)
    return "".join(t.get("plain_text", "") for t in (p or {}).get("rich_text", []))


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


def last_month_window():
    """Return (start_ts, end_ts) as Unix timestamps for the previous full calendar month."""
    today = datetime.date.today()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - datetime.timedelta(days=1)
    first_of_last_month = last_month_end.replace(day=1)

    start_ts = int(datetime.datetime.combine(first_of_last_month, datetime.time.min).timestamp())
    end_ts = int(datetime.datetime.combine(first_of_this_month, datetime.time.min).timestamp())
    return start_ts, end_ts, first_of_last_month, last_month_end


def fetch_all_charges(start_ts, end_ts):
    """Fetch all succeeded Stripe charges in the given time window (paginated)."""
    charges = []
    params = {
        "created[gte]": start_ts,
        "created[lt]": end_ts,
        "status": "succeeded",
        "limit": 100,
    }
    url = "https://api.stripe.com/v1/charges"
    while True:
        r = requests.get(url, auth=(STRIPE_KEY, ""), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        charges.extend(data.get("data", []))
        if not data.get("has_more"):
            break
        params["starting_after"] = data["data"][-1]["id"]
    return charges


def aggregate_charges(charges):
    """Group charges by customer, summing net amounts (amount - refunded) in EUR."""
    totals = {}
    for ch in charges:
        cust = ch.get("customer")
        if not cust:
            continue
        net_cents = (ch.get("amount") or 0) - (ch.get("amount_refunded") or 0)
        totals[cust] = totals.get(cust, 0) + net_cents
    # Convert cents to EUR
    return {cid: round(total / 100, 2) for cid, total in totals.items()}


# ── Subscription MRR helpers (from sync_billing_status.py) ────────────────────


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

        # qty=0 means a zeroed-out item (e.g. old monthly after migration to yearly)
        effective_qty = qty if qty is not None else 1

        if usage_type == "metered":
            amount_cents = _metered_amount_from_invoice(subscription, price.get("id"))
        elif price.get("billing_scheme") == "tiered":
            tiers = price.get("tiers") or []
            if tiers:
                amount_cents = _compute_tiered_amount_cents(price, effective_qty)
            else:
                amount_cents = _metered_amount_from_invoice(subscription, price.get("id"))
        else:
            amount_cents = (price.get("unit_amount") or 0) * effective_qty

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


# ── Base subscription MRR ────────────────────────────────────────────────────


def fetch_stripe_subs_expanded():
    """Fetch all active + past_due subscriptions with expansions for compute_mrr()."""
    subs = []
    for status in ("active", "past_due"):
        params = {
            "status": status,
            "limit": 100,
            "expand[]": [
                "data.items.data.price",
                "data.latest_invoice.lines.data",
            ],
        }
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


def build_base_mrr_map(subs):
    """Group subs by customer ID, sum compute_mrr() per customer → {stripe_cus_id: base_mrr}."""
    enrich_tiered_prices(subs)
    base = {}
    for sub in subs:
        cust = sub.get("customer")
        if not cust:
            continue
        base[cust] = round(base.get(cust, 0) + compute_mrr(sub), 2)
    return base


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    print(f"{'='*65}")
    print(f"  sync_mrr_from_stripe.py  |  {mode}  |  {datetime.date.today()}")
    print(f"{'='*65}\n")

    # ── 1. Compute date window ────────────────────────────────────────────────
    start_ts, end_ts, month_start, month_end = last_month_window()
    print(f"Charge window: {month_start} → {month_end}\n")

    # ── 2. Fetch all succeeded charges ────────────────────────────────────────
    print("Fetching Stripe charges…")
    charges = fetch_all_charges(start_ts, end_ts)
    print(f"  {len(charges)} succeeded charges")

    # ── 3. Aggregate by customer ──────────────────────────────────────────────
    charge_totals = aggregate_charges(charges)
    print(f"  {len(charge_totals)} unique customers with charges\n")

    # ── 4. Fetch base subscription MRR ────────────────────────────────────────
    print("Fetching Stripe subscriptions (base MRR)…")
    subs = fetch_stripe_subs_expanded()
    base_mrr_map = build_base_mrr_map(subs)
    print(f"  {len(subs)} subscriptions → {len(base_mrr_map)} customers with base MRR\n")

    # ── 5. Fetch MCT ─────────────────────────────────────────────────────────
    print("Fetching MCT rows…")
    pages = fetch_all_mct()
    print(f"  {len(pages)} rows\n")

    # ── 6. Compare and patch ──────────────────────────────────────────────────
    print(f"Reconciling MRR ({mode})…")
    patched = 0
    skipped_no_data = 0
    skipped_no_stripe = 0
    unchanged = 0
    errors = 0

    for page in pages:
        page_id = page["id"]
        company = get_title(page, "🏢 Company Name").strip() or "(no name)"
        stripe_id = get_rich_text(page, "🔗 Stripe Customer ID").strip()

        if not stripe_id or not stripe_id.startswith("cus_"):
            skipped_no_stripe += 1
            continue

        current_mrr = get_number(page, "💰 MRR") or 0
        charges_mrr = charge_totals.get(stripe_id)
        base_sub = base_mrr_map.get(stripe_id)

        if charges_mrr is None and base_sub is None:
            # No charges and no active sub — keep existing MCT MRR
            skipped_no_data += 1
            continue

        # MRR = max(base_sub, charges) — whichever is available/higher
        if charges_mrr is not None and base_sub is not None:
            if charges_mrr > base_sub:
                new_mrr = charges_mrr
                label = "charges > base"
            else:
                new_mrr = base_sub
                label = "base"
        elif base_sub is not None:
            new_mrr = base_sub
            label = "base"
        else:
            new_mrr = charges_mrr
            label = "charges"

        if abs(new_mrr - current_mrr) <= 1:
            unchanged += 1
            continue

        diff = new_mrr - current_mrr
        sign = "+" if diff > 0 else ""
        print(f"  {company}: €{current_mrr:,.2f} → €{new_mrr:,.2f} ({sign}{diff:,.2f}) [{label}]")

        if DRY_RUN:
            patched += 1
            continue

        if patch_mct_page(page_id, {"💰 MRR": {"number": new_mrr}}):
            patched += 1
        else:
            errors += 1
        time.sleep(0.3)

    # ── 7. Summary ────────────────────────────────────────────────────────────
    verb = "would patch" if DRY_RUN else "patched"
    total_charged = sum(charge_totals.values())
    total_base = sum(base_mrr_map.values())
    print(f"\n{'='*65}")
    print(f"  Stripe charges ({month_start.strftime('%b %Y')}): €{total_charged:,.2f}")
    print(f"  Stripe base MRR (active subs):       €{total_base:,.2f}")
    print(f"  {patched} {verb} | {unchanged} unchanged | {skipped_no_data} no data (kept) | {skipped_no_stripe} no Stripe ID | {errors} errors")
    print(f"{'='*65}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
