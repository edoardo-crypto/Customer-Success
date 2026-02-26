#!/usr/bin/env python3
"""
rename_person_to_company.py
----------------------------
23 MCT rows currently show a person's name instead of the real business name.
This script resolves the correct business name (and domain for gmail.com rows)
then patches the Notion page title.

Step 0: Loads all MCT pages from Notion to get verified page IDs by current name.
Groups:
  A — 4 gmail.com rows: business name hardcoded from screenshot; domain looked up
  B — 19 rows with real domains: business name looked up via HubSpot / Intercom /
      derived from domain

Usage:
  python3 rename_person_to_company.py             # dry-run: show resolved names, no writes
  python3 rename_person_to_company.py --apply     # apply all patches to Notion
"""

import re
import sys
import time
import urllib.parse
import urllib.request
import json
from typing import Optional

# ── Credentials ──────────────────────────────────────────────────────────────
INTERCOM_TOKEN  = "***REMOVED***"
HUBSPOT_TOKEN   = "***REMOVED***"
NOTION_TOKEN    = "***REMOVED***"

# Notion MCT data source
MCT_DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"

APPLY = "--apply" in sys.argv

# ── Group A — gmail.com rows (page IDs verified from fix_generic_domains.py) ──
# Name is provided by the user. Domain must be looked up by the new business name.
# Tuple: (page_id, current_name, new_name)
GROUP_A = [
    ("302e418f-d8c4-8166-9dc3-f823981d775f", "GASPAR BAJDA MOLINA",   "Lumara Shop"),
    ("302e418f-d8c4-81bb-b387-e3a47c6be1b7", "JULIO DE CASO",         "Perfumara"),
    ("302e418f-d8c4-81d3-a925-f13a977f6c31", "Mario Escobar Castro",  "English Path"),
    ("302e418f-d8c4-8170-a478-c26ce97cf6e4", "Rae Langworth",         "4 Patitas"),
]

# ── Group B — rows with real domains (page IDs resolved at runtime from Notion) ──
# Tuple: (current_name, domain)  — page_id is looked up dynamically
GROUP_B_LOOKUP = [
    ("Alice Hadley",                 "tomsstudio.com"),
    ("Eduardo Rodríguez Turel",      "eturel.com"),
    ("ERIKA HERNANDEZ",              "commons.mx"),
    ("Evert van der Lingen",         "multimediaconcepts.nl"),
    ("Gustavo Saralegui",            "escalamos.io"),
    ("Hugo Priego",                  "platanomelon.com"),
    ("Iker Cruz Oraá",               "ic-mediamarketing.com"),
    ("JORDI SOLE SOLDEVILA",         "badhabits.es"),
    ("Juan Dersarkisian",            "shopick.com.uy"),
    ("LUCAS GABRIEL MOTTA",          "asesoriaclientes.com"),
    ("Luis Osvaldo Garcia Cruz",     "nibiru.mx"),
    ("Marc Soler Obradors",          "healthnutritionlab.com"),
    ("María José López",             "femmeup.es"),
    ("Oriol Navarro Sancho",         "fundashogar.com"),
    ("Patricia Villarreal Bernal",   "ohmywax.com"),
    ("Raquel Baena Marí",            "huellasdeibiza.com"),
    ("SEMIR EUGENIO HOUICHI MAYOR",  "salaterradeco.com"),
    ("Xabier Yarnoz",               "vinkovaleotards.com"),
]

# ── Generic domains ───────────────────────────────────────────────────────────
GENERIC_DOMAINS = {
    "gmail.com", "hotmail.com", "yahoo.com", "outlook.com",
    "live.com", "icloud.com", "me.com", "msn.com", "aol.com",
    "protonmail.com", "zoho.com",
}

# Hosted platform suffixes — subdomains of these are not real business domains
HOSTED_SUFFIXES = ("myshopify.com", "shopify.com", "squarespace.com", "wixsite.com")

# ── Manual overrides ─────────────────────────────────────────────────────────
# Maps domain → correct company name when API lookup returns wrong/poor results.
DOMAIN_OVERRIDES = {
    "escalamos.io":           "Escalamos",           # HubSpot returned generic "ECOM"
    "huellasdeibiza.com":     "Huellas de Ibiza",    # derived "Huellasdeibiza" unreadable
    "nibiru.mx":              "Nibiru",               # HubSpot returned lowercase "nibiru"
    "healthnutritionlab.com": "Health Nutrition Lab", # derived compound word unreadable
    "ic-mediamarketing.com":  "IC Media Marketing",   # wrong case / one-word
    "asesoriaclientes.com":   "Asesoria Clientes",    # compound Spanish word
    "fundashogar.com":        "Funda Hogar",          # compound Spanish word
    "ohmywax.com":            "Oh My Wax",            # compound word
}


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def http_get(url: str, headers: dict) -> Optional[dict]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"    [GET error] {url[:80]}... → HTTP {e.code}")
        return None
    except Exception as e:
        print(f"    [GET error] {url[:80]}... → {e}")
        return None


def http_post(url: str, headers: dict, body: dict) -> Optional[dict]:
    data = json.dumps(body).encode()
    headers = {**headers, "Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"    [POST error] {url[:80]}... → {e}")
        return None


def http_patch(url: str, headers: dict, body: dict) -> Optional[dict]:
    data = json.dumps(body).encode()
    headers = {**headers, "Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method="PATCH")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"    [PATCH error] {url[:80]}... → {e}")
        return None


def bare_domain(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    if "://" in raw:
        parsed = urllib.parse.urlparse(raw)
        host = parsed.hostname or ""
    else:
        host = raw.split("/")[0]
    host = re.sub(r"^www\.", "", host, flags=re.IGNORECASE)
    return host.lower().strip(".")


def is_generic(domain: str) -> bool:
    if not domain:
        return True
    d = domain.lower().strip()
    if d in GENERIC_DOMAINS:
        return True
    if any(d.endswith("." + suffix) or d == suffix for suffix in HOSTED_SUFFIXES):
        return True
    return False


# ── Step 0: Load MCT name → page_id map from Notion ─────────────────────────

def fetch_mct_name_map() -> dict:
    """
    Query the MCT via data_sources API and return a dict mapping
    normalized company name → page_id.
    Handles pagination automatically.
    """
    url = f"https://api.notion.com/v1/data_sources/{MCT_DS_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2025-09-03",
        "Accept": "application/json",
    }
    name_map = {}
    cursor = None
    page_count = 0

    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor

        data = http_post(url, headers, body)
        if not data:
            print("  [ERROR] Failed to query MCT — cannot resolve page IDs")
            return name_map

        for page in data.get("results", []):
            page_id = page.get("id", "")
            props = page.get("properties", {})
            # Title property could be under various keys — find the title type
            title_text = ""
            for prop_name, prop_val in props.items():
                if prop_val.get("type") == "title":
                    title_arr = prop_val.get("title", [])
                    title_text = "".join(t.get("plain_text", "") for t in title_arr).strip()
                    break
            if title_text and page_id:
                name_map[title_text.lower()] = (page_id, title_text)

        page_count += len(data.get("results", []))
        if data.get("has_more") and data.get("next_cursor"):
            cursor = data["next_cursor"]
            time.sleep(0.2)
        else:
            break

    print(f"  Loaded {page_count} MCT rows ({len(name_map)} unique names)")
    return name_map


def resolve_page_id(name_map: dict, person_name: str) -> Optional[str]:
    """Look up the Notion page ID for a given person name (case-insensitive)."""
    key = person_name.lower().strip()
    match = name_map.get(key)
    if match:
        return match[0]  # page_id
    return None


# ── Name lookup helpers (Group B) ────────────────────────────────────────────

def _is_good_company_name(name: str, person_name: str = "") -> bool:
    """Return True if this name looks like a real business name, not a domain string."""
    name = name.strip()
    if not name or len(name) < 3:
        return False
    if "." in name:
        return False
    if person_name and name.lower() == person_name.lower():
        return False
    return True


def lookup_hubspot_by_domain(domain: str, person_name: str = "") -> Optional[str]:
    """Search HubSpot for a company matching this domain. Return company name or None."""
    url = "https://api.hubapi.com/crm/v3/objects/companies/search"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Accept": "application/json",
    }
    body = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "domain",
                "operator": "EQ",
                "value": domain,
            }]
        }],
        "properties": ["name", "domain", "website"],
        "limit": 3,
    }
    data = http_post(url, headers, body)
    if not data:
        return None
    for result in data.get("results", []):
        name = (result.get("properties", {}).get("name") or "").strip()
        if _is_good_company_name(name, person_name):
            return name
    return None


def lookup_intercom_by_name(person_name: str) -> Optional[str]:
    """Search Intercom companies by the person's name. Return company name or None."""
    encoded = urllib.parse.quote(person_name)
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
        name = company.get("name") or ""
        if name.strip() and name.strip().lower() != person_name.lower():
            return name.strip()
    return None


def derive_name_from_domain(domain: str) -> str:
    """Last-resort: strip TLD, split on hyphens/dots, title-case."""
    parts = domain.split(".")
    if len(parts) >= 3 and len(parts[-1]) <= 3 and len(parts[-2]) <= 3:
        base = ".".join(parts[:-2])
    elif len(parts) >= 2:
        base = ".".join(parts[:-1])
    else:
        base = domain
    words = re.split(r"[-.]", base)
    return " ".join(w.capitalize() for w in words if w)


# ── Domain lookup helpers (Group A) ──────────────────────────────────────────

def lookup_domain_by_name_hubspot(business_name: str) -> Optional[str]:
    """Search HubSpot companies by (new) business name. Return bare domain or None."""
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
                "value": business_name,
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
        domain = bare_domain(props.get("domain") or "")
        if domain and not is_generic(domain):
            return domain
        domain = bare_domain(props.get("website") or "")
        if domain and not is_generic(domain):
            return domain
    return None


def lookup_domain_by_name_intercom(business_name: str) -> Optional[str]:
    """Search Intercom companies by (new) business name. Return bare domain or None."""
    encoded = urllib.parse.quote(business_name)
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
        website = company.get("website") or ""
        domain = bare_domain(website)
        if domain and not is_generic(domain):
            return domain
        cdn = company.get("company_domain_name") or ""
        domain = bare_domain(cdn)
        if domain and not is_generic(domain):
            return domain
    return None


# ── Notion PATCH ──────────────────────────────────────────────────────────────

def patch_notion_page(page_id: str, new_name: str, new_domain: Optional[str] = None) -> bool:
    """
    PATCH the Notion MCT page:
      - Always updates 🏢 Company Name (title)
      - If new_domain is provided, also updates 🏢 Domain (rich_text)
    """
    url = f"https://api.notion.com/v1/pages/{page_id}"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2025-09-03",
        "Accept": "application/json",
    }
    props = {
        "🏢 Company Name": {
            "title": [{"text": {"content": new_name}}]
        }
    }
    if new_domain:
        props["🏢 Domain"] = {
            "rich_text": [{"text": {"content": new_domain}}]
        }
    result = http_patch(url, headers, {"properties": props})
    return result is not None


# ── Resolution ────────────────────────────────────────────────────────────────

def resolve_group_a():
    """For each Group A row, lookup domain by new business name."""
    resolved = []
    for page_id, current_name, new_name in GROUP_A:
        print(f"  🔍 {current_name}  → '{new_name}' (domain lookup)")

        domain = lookup_domain_by_name_hubspot(new_name)
        if domain:
            print(f"     ✅ HubSpot(name)   → {domain}")
            resolved.append((page_id, current_name, new_name, "gmail.com", domain, "HubSpot"))
            time.sleep(0.3)
            continue

        domain = lookup_domain_by_name_intercom(new_name)
        if domain:
            print(f"     ✅ Intercom(name)  → {domain}")
            resolved.append((page_id, current_name, new_name, "gmail.com", domain, "Intercom"))
            time.sleep(0.3)
            continue

        print(f"     ❌ Domain not found — will rename only, domain stays as-is")
        resolved.append((page_id, current_name, new_name, "gmail.com", None, "not found"))
        time.sleep(0.2)

    return resolved


def resolve_group_b(name_map: dict):
    """For each Group B row, find page ID from Notion and determine new name."""
    resolved = []
    for current_name, domain in GROUP_B_LOOKUP:
        print(f"  🔍 {current_name}  ({domain})")

        # Resolve page ID from Notion
        page_id = resolve_page_id(name_map, current_name)
        if not page_id:
            print(f"     ❌ NOT FOUND in MCT — skipping")
            resolved.append((None, current_name, "?", domain, None, "MCT_NOT_FOUND"))
            continue

        # Check manual override first
        if domain in DOMAIN_OVERRIDES:
            name = DOMAIN_OVERRIDES[domain]
            print(f"     ✅ Override         → {name}  [page={page_id}]")
            resolved.append((page_id, current_name, name, domain, None, "override"))
            time.sleep(0.1)
            continue

        # Tier 1: HubSpot by domain
        name = lookup_hubspot_by_domain(domain, person_name=current_name)
        if name:
            print(f"     ✅ HubSpot(domain) → {name}  [page={page_id}]")
            resolved.append((page_id, current_name, name, domain, None, "HubSpot"))
            time.sleep(0.3)
            continue

        # Tier 2: Intercom by person name
        name = lookup_intercom_by_name(current_name)
        if name:
            print(f"     ✅ Intercom(name)  → {name}  [page={page_id}]")
            resolved.append((page_id, current_name, name, domain, None, "Intercom"))
            time.sleep(0.3)
            continue

        # Tier 3: Derive from domain
        name = derive_name_from_domain(domain)
        print(f"     ⚠️  Derived          → {name}  [page={page_id}]")
        resolved.append((page_id, current_name, name, domain, None, "derived"))
        time.sleep(0.2)

    return resolved


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print()
    print("=" * 90)
    print("  rename_person_to_company.py")
    print(f"  Mode: {'APPLY — will write to Notion' if APPLY else 'DRY-RUN — no writes'}")
    print("=" * 90)

    # ── Step 0: Load MCT name → page_id map ─────────────────────────────────
    print("\n── Step 0: Loading MCT page IDs from Notion ──\n")
    name_map = fetch_mct_name_map()

    # ── Verify Group A page IDs ──────────────────────────────────────────────
    print("\n  Verifying Group A page IDs against MCT...")
    for page_id, current_name, _ in GROUP_A:
        mct_id = resolve_page_id(name_map, current_name)
        if mct_id and mct_id.replace("-", "") == page_id.replace("-", ""):
            print(f"    ✅ {current_name} → {page_id}")
        elif mct_id:
            print(f"    ⚠️  {current_name}: hardcoded={page_id}  MCT found={mct_id}  (using MCT)")
        else:
            print(f"    ❌ {current_name}: not found in MCT (using hardcoded ID)")

    # ── Resolve names ────────────────────────────────────────────────────────
    print("\n── Group A (4 gmail.com rows — name hardcoded, domain lookup) ──\n")
    group_a_resolved = resolve_group_a()

    print("\n── Group B (19 rows with domain — name lookup) ──\n")
    group_b_resolved = resolve_group_b(name_map)

    all_rows = group_a_resolved + group_b_resolved

    # ── Print summary table ──────────────────────────────────────────────────
    actionable = [r for r in all_rows if r[0] is not None]
    skipped    = [r for r in all_rows if r[0] is None]

    print("\n")
    print("=" * 115)
    print(f"  {'Current Name':<35} {'New Name':<30} {'New Domain':<22} {'Source':<12} {'Page ID'}")
    print("-" * 115)
    for page_id, current, new_name, old_domain, new_domain, source in actionable:
        domain_col = new_domain if new_domain else ("(keep)" if not is_generic(old_domain) else "NOT FOUND")
        pid_short = page_id[-12:] if page_id else "—"
        print(f"  {current:<35} {new_name:<30} {domain_col:<22} {source:<12} ...{pid_short}")
    print("=" * 115)

    derived   = [r for r in actionable if r[5] == "derived"]
    no_dom    = [r for r in group_a_resolved if r[4] is None]
    not_found = skipped

    print(f"\n  Totals: {len(actionable)} actionable, {len(skipped)} skipped (not in MCT)")
    if not_found:
        print(f"\n  ❌ {len(not_found)} row(s) NOT found in MCT — will be skipped:")
        for r in not_found:
            print(f"       • {r[1]}  ({r[3]})")
    if derived:
        print(f"\n  ⚠️  {len(derived)} name(s) derived from domain — review before applying:")
        for r in derived:
            print(f"       • {r[1]}  ({r[3]})  →  {r[2]}")
    if no_dom:
        print(f"\n  ⚠️  {len(no_dom)} gmail.com row(s) with no domain found — name-only patch:")
        for r in no_dom:
            print(f"       • {r[1]}  →  {r[2]}")

    if not APPLY:
        print("\n  ℹ️  DRY-RUN complete. Run with --apply to write to Notion.\n")
        return

    # ── Apply patches ────────────────────────────────────────────────────────
    print("\n")
    print("=" * 90)
    print("  Applying Notion PATCHes…")
    print("=" * 90 + "\n")

    ok_count = 0
    fail_count = 0

    for page_id, current, new_name, old_domain, new_domain, source in actionable:
        domain_str = f" + domain={new_domain}" if new_domain else ""
        print(f"  PATCH  {current}  →  {new_name}{domain_str}")
        ok = patch_notion_page(page_id, new_name, new_domain)
        if ok:
            print(f"         ✅ done")
            ok_count += 1
        else:
            print(f"         ❌ FAILED")
            fail_count += 1
        time.sleep(0.4)

    print(f"\n  Done. {ok_count} patched, {fail_count} failed.\n")


if __name__ == "__main__":
    main()
