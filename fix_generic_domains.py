#!/usr/bin/env python3
"""
fix_generic_domains.py
----------------------
Phase A (DRY_RUN=True):  Look up real business domains for 29 MCT rows that
                          currently have a generic mail provider (gmail.com,
                          hotmail.com, etc.) as their domain.
                          Prints a candidate table — no writes to Notion.

Phase B (DRY_RUN=False): Patch Notion pages for HIGH-confidence findings
                          (Intercom or HubSpot). MEDIUM (DuckDuckGo) and
                          NOT FOUND rows are skipped and need manual review.

3-tier lookup per company:
  1. Intercom Companies API  → HIGH confidence
  2. HubSpot CRM Search      → HIGH confidence
  3. DuckDuckGo Instant API  → MEDIUM confidence (free, no auth)
"""

import re
import time
import urllib.parse
import urllib.request
import json
from typing import Optional

# ── Control flag ────────────────────────────────────────────────────────────
DRY_RUN = True          # Flip to False to write to Notion
SKIP_MEDIUM = True      # Skip DuckDuckGo results when patching (recommended)

# ── API credentials ─────────────────────────────────────────────────────────
INTERCOM_TOKEN = "***REMOVED***"
HUBSPOT_TOKEN  = "***REMOVED***"
NOTION_TOKEN   = "***REMOVED***"

# ── The 29 companies ─────────────────────────────────────────────────────────
COMPANIES = [
    ("WORKFIA LLC",                   "302e418f-d8c4-8106-abdc-debc8ba18846"),
    ("Juan Dersarkisian",             "302e418f-d8c4-8117-9670-cdf63eb6dfe4"),
    ("Futbolkit",                     "302e418f-d8c4-8121-9028-d749444904c5"),
    ("MONISQUI S.L.",                 "302e418f-d8c4-8121-ab7e-f060176be2aa"),
    ("CUCHY",                         "302e418f-d8c4-8131-9206-eaf5ded88f75"),
    ("Luz de Necha",                  "302e418f-d8c4-813f-9a21-d0e177f3a461"),
    ("PAYS D,OC, S.L.",               "302e418f-d8c4-8147-835e-e4b78c56e6f0"),
    ("DUKE TRADING S.L",              "302e418f-d8c4-8147-afb5-e806602625fa"),
    ("Unikare S.L.",                  "302e418f-d8c4-814b-b771-db632b3a6fec"),
    ("Old School Spain",              "302e418f-d8c4-8153-9133-e0f570989079"),
    ("JORME ONLINE SL",               "302e418f-d8c4-8160-954a-db68450b9a54"),
    ("Finca la Mesa S.L.",            "302e418f-d8c4-8164-98c3-cdca77f19ad1"),
    ("GASPAR BAJDA MOLINA",           "302e418f-d8c4-8166-9dc3-f823981d775f"),
    ("il baco da seta slu",           "302e418f-d8c4-816a-9c57-c67450aee332"),
    ("Raquel Baena Marí",             "302e418f-d8c4-816d-9112-d913acdc795c"),
    ("Rae Langworth",                 "302e418f-d8c4-8170-a478-c26ce97cf6e4"),
    ("Evercore Europe",               "302e418f-d8c4-817b-86e2-e665777ef096"),
    ("Gustavo Saralegui",             "302e418f-d8c4-8184-89ee-f1c5dee8d1f2"),
    ("DECOFLORIMPERIAL.SL",           "302e418f-d8c4-81ae-bdb5-ebb1e75d3db7"),
    ("Patricia Villarreal Bernal",    "302e418f-d8c4-81af-a259-cfdd94147216"),
    ("ODOREM MEDITERRANEA, S.L.",     "302e418f-d8c4-81ba-9f32-e25b092e3c94"),
    ("JULIO DE CASO",                 "302e418f-d8c4-81bb-b387-e3a47c6be1b7"),
    ("JORDI SOLE SOLDEVILA",          "302e418f-d8c4-81be-9519-cd7de1b51c1b"),
    ("Azulejos Solá SA",              "302e418f-d8c4-81c2-bfbf-d6760959dde4"),
    ("Mario Escobar Castro",          "302e418f-d8c4-81d3-a925-f13a977f6c31"),
    ("KLAT SAS",                      "302e418f-d8c4-81d5-82b5-eaab5b3d1277"),
    ("Rocacorba Girona SL",           "302e418f-d8c4-81e2-a6e9-f5b57fd5e8c6"),
    ("Textiles Martinez Curiel",      "302e418f-d8c4-81fc-bcde-e57dda67f714"),
    ("NICE MOOD 24 SL",               "302e418f-d8c4-81fc-bfd8-d7a6e476abbc"),
]

# Generic mail providers — domains that should NOT be used
GENERIC_DOMAINS = {
    "gmail.com", "hotmail.com", "yahoo.com", "outlook.com",
    "live.com", "icloud.com", "me.com", "msn.com", "aol.com",
    "protonmail.com", "zoho.com",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def is_generic(domain: str) -> bool:
    """Return True if domain is a generic mail provider or empty."""
    if not domain:
        return True
    return domain.lower().strip() in GENERIC_DOMAINS


def bare_domain(raw: str) -> str:
    """Strip scheme, www., trailing slash from a URL or domain string."""
    if not raw:
        return ""
    raw = raw.strip()
    # If it looks like a URL, parse it
    if "://" in raw:
        parsed = urllib.parse.urlparse(raw)
        host = parsed.hostname or ""
    else:
        # Could be "www.example.com" without scheme
        host = raw.split("/")[0]
    # Remove leading www.
    host = re.sub(r"^www\.", "", host, flags=re.IGNORECASE)
    return host.lower().strip(".")


def http_get(url: str, headers: dict) -> Optional[dict]:
    """Simple GET request, returns parsed JSON or None on error/404."""
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"    [GET error] {url[:80]}… → HTTP {e.code}")
        return None
    except Exception as e:
        print(f"    [GET error] {url[:80]}… → {e}")
        return None


def http_post(url: str, headers: dict, body: dict) -> Optional[dict]:
    """Simple POST request with JSON body, returns parsed JSON or None."""
    data = json.dumps(body).encode()
    headers = {**headers, "Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"    [POST error] {url[:80]}… → {e}")
        return None


def http_patch(url: str, headers: dict, body: dict) -> Optional[dict]:
    """Simple PATCH request with JSON body, returns parsed JSON or None."""
    data = json.dumps(body).encode()
    headers = {**headers, "Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method="PATCH")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"    [PATCH error] {url[:80]}… → {e}")
        return None


# ── Tier 1: Intercom ─────────────────────────────────────────────────────────

def lookup_intercom(name: str) -> Optional[str]:
    """Search Intercom companies by name. Return bare domain or None."""
    encoded = urllib.parse.quote(name)
    url = f"https://api.intercom.io/companies?name={encoded}"
    headers = {
        "Authorization": f"Bearer {INTERCOM_TOKEN}",
        "Intercom-Version": "2.11",
        "Accept": "application/json",
    }
    data = http_get(url, headers)
    if not data:
        return None
    results = data.get("data", []) or data.get("companies", [])
    for company in results:
        # Try website field first
        website = company.get("website") or ""
        domain = bare_domain(website)
        if domain and not is_generic(domain):
            return domain
        # Try company_domain_name (Intercom auto-extracts from employee emails)
        cdn = company.get("company_domain_name") or ""
        domain = bare_domain(cdn)
        if domain and not is_generic(domain):
            return domain
    return None


# ── Tier 2: HubSpot ──────────────────────────────────────────────────────────

def lookup_hubspot(name: str) -> Optional[str]:
    """Search HubSpot CRM companies by name. Return bare domain or None."""
    url = "https://api.hubapi.com/crm/v3/objects/companies/search"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Accept": "application/json",
    }
    body = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "name",
                "operator": "CONTAINS_TOKEN",
                "value": name,
            }]
        }],
        "properties": ["name", "domain", "website"],
        "limit": 5,
    }
    data = http_post(url, headers, body)
    if not data:
        return None
    for result in data.get("results", []):
        props = result.get("properties", {})
        # Prefer domain (clean), fall back to website
        domain = bare_domain(props.get("domain") or "")
        if domain and not is_generic(domain):
            return domain
        domain = bare_domain(props.get("website") or "")
        if domain and not is_generic(domain):
            return domain
    return None


# ── Tier 3: DuckDuckGo Instant Answer ────────────────────────────────────────

def lookup_ddg(name: str) -> Optional[str]:
    """DuckDuckGo Instant Answer API. Return bare domain or None."""
    encoded = urllib.parse.quote(f"{name} official website")
    url = f"https://api.duckduckgo.com/?q={encoded}&format=json&no_redirect=1&no_html=1"
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    data = http_get(url, headers)
    if not data:
        return None
    # AbstractURL is the Wikipedia/infobox source URL
    abstract_url = data.get("AbstractURL") or ""
    domain = bare_domain(abstract_url)
    if domain and not is_generic(domain):
        return domain
    # OfficialSite is sometimes populated
    official = data.get("OfficialSite") or ""
    domain = bare_domain(official)
    if domain and not is_generic(domain):
        return domain
    return None


# ── Notion PATCH ─────────────────────────────────────────────────────────────

def patch_notion_domain(page_id: str, domain: str) -> bool:
    """Update the 🏢 Domain rich_text property on a Notion page."""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2025-09-03",
        "Accept": "application/json",
    }
    body = {
        "properties": {
            "🏢 Domain": {
                "rich_text": [{"text": {"content": domain}}]
            }
        }
    }
    result = http_patch(url, headers, body)
    return result is not None


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 80)
    print("  fix_generic_domains.py — Phase A: Domain Discovery")
    print(f"  DRY_RUN={DRY_RUN}   SKIP_MEDIUM={SKIP_MEDIUM}")
    print("=" * 80 + "\n")

    findings = []  # (name, page_id, source, domain, confidence)

    for name, page_id in COMPANIES:
        print(f"  🔍 {name}")

        # Tier 1 — Intercom
        domain = lookup_intercom(name)
        if domain:
            print(f"     ✅ Intercom → {domain}")
            findings.append((name, page_id, "Intercom", domain, "HIGH"))
            time.sleep(0.3)
            continue

        # Tier 2 — HubSpot
        domain = lookup_hubspot(name)
        if domain:
            print(f"     ✅ HubSpot  → {domain}")
            findings.append((name, page_id, "HubSpot", domain, "HIGH"))
            time.sleep(0.3)
            continue

        # Tier 3 — DuckDuckGo
        domain = lookup_ddg(name)
        if domain:
            print(f"     ⚠️  DDG      → {domain} (needs review)")
            findings.append((name, page_id, "DDG", domain, "MEDIUM"))
            time.sleep(0.5)
            continue

        print("     ❌ Not found")
        findings.append((name, page_id, "none", "—", "NOT FOUND"))
        time.sleep(0.3)

    # ── Print results table ──────────────────────────────────────────────────
    print("\n")
    print("=" * 90)
    print(f"  {'Company Name':<35} {'Source':<10} {'Found Domain':<28} {'Confidence'}")
    print("-" * 90)
    for name, page_id, source, domain, confidence in findings:
        print(f"  {name:<35} {source:<10} {domain:<28} {confidence}")
    print("=" * 90)

    high   = [f for f in findings if f[4] == "HIGH"]
    medium = [f for f in findings if f[4] == "MEDIUM"]
    nf     = [f for f in findings if f[4] == "NOT FOUND"]
    print(f"\n  Summary: {len(high)} HIGH  |  {len(medium)} MEDIUM  |  {len(nf)} NOT FOUND")

    # ── Phase B — Patch Notion ───────────────────────────────────────────────
    if DRY_RUN:
        print("\n  ℹ️  DRY_RUN=True — no Notion writes. Flip DRY_RUN=False to patch.")
        print("  ℹ️  MEDIUM domains (DDG) still need manual verification before patching.\n")
        return

    print("\n" + "=" * 80)
    print("  Phase B: Patching Notion (HIGH confidence only)")
    print("=" * 80 + "\n")

    to_patch = high if SKIP_MEDIUM else high + medium
    patched = 0
    failed  = 0

    for name, page_id, source, domain, confidence in to_patch:
        print(f"  PATCH  {name}  →  {domain}  (via {source})")
        ok = patch_notion_domain(page_id, domain)
        if ok:
            print(f"         ✅ done")
            patched += 1
        else:
            print(f"         ❌ FAILED")
            failed += 1
        time.sleep(0.4)

    print(f"\n  Done. {patched} patched, {failed} failed.")
    if SKIP_MEDIUM and medium:
        print(f"  ⚠️  {len(medium)} MEDIUM domain(s) skipped — review manually:")
        for name, _, source, domain, _ in medium:
            print(f"       • {name}  →  {domain}")
    print()


if __name__ == "__main__":
    main()
