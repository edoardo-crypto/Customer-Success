#!/usr/bin/env python3
"""
fix_mct_domains.py
-------------------
Reads mct_domain_audit.json (produced by audit_mct_domains.py) and applies
domain fixes to Notion MCT rows.

  python3 fix_mct_domains.py --dry-run   # show plan, no writes
  python3 fix_mct_domains.py             # apply (HIGH auto, MEDIUM prompted)

Behaviour by confidence level:
  HIGH        → patched automatically
  MEDIUM      → interactive y / n / q prompt per row
  UNRESOLVED  → listed for manual follow-up, never touched
  CLEAR       → skipped (no action needed)
"""

import json
import sys
import time

# Re-use the patch helper and guard from the existing enrichment script.
# patch_notion_domain uses Notion-Version 2025-09-03 (required for MCT pages).
from fix_generic_domains import is_generic, patch_notion_domain

INPUT_FILE = "mct_domain_audit.json"
DRY_RUN    = "--dry-run" in sys.argv


# ── Safety guard ──────────────────────────────────────────────────────────────

def is_safe(row) -> bool:
    """True only if the proposed domain is non-empty and not a generic provider."""
    d = row.get("proposed_domain", "")
    return bool(d) and not is_generic(d)


# ── Step 2: Plan summary ──────────────────────────────────────────────────────

def print_plan_summary(rows) -> None:
    high   = [r for r in rows if r.get("confidence") == "HIGH"   and is_safe(r)]
    medium = [r for r in rows if r.get("confidence") == "MEDIUM" and is_safe(r)]
    unres  = [r for r in rows if r.get("confidence") == "UNRESOLVED"]

    print("\n" + "=" * 72)
    print("  fix_mct_domains.py — PLAN SUMMARY")
    if DRY_RUN:
        print("  ** DRY RUN — no Notion writes will happen **")
    print("=" * 72)
    print(f"  HIGH confidence (auto-apply) : {len(high)}")
    print(f"  MEDIUM confidence (prompt)   : {len(medium)}")
    print(f"  UNRESOLVED (manual needed)   : {len(unres)}")
    print()

    if high:
        print("  HIGH confidence:")
        for r in high:
            cur  = r["current_domain"] or "(empty)"
            prop = r["proposed_domain"]
            print(f"    • {r['company_name']:<36} {cur:<22} → {prop:<22} [{r['source']}]")
        print()

    if medium:
        print("  MEDIUM confidence:")
        for r in medium:
            cur  = r["current_domain"] or "(empty)"
            prop = r["proposed_domain"]
            print(f"    • {r['company_name']:<36} {cur:<22} → {prop:<22} [{r['source']}]")
        print()

    if unres:
        print("  UNRESOLVED (manual lookup needed):")
        for r in unres:
            sid = r.get("stripe_id") or "(no Stripe ID)"
            print(f"    • {r['company_name']:<40} [{sid}]")
        print()

    print("=" * 72 + "\n")


# ── Step 4: HIGH confidence — auto-apply ──────────────────────────────────────

def apply_high_confidence(rows) -> None:
    high = [r for r in rows if r.get("confidence") == "HIGH" and is_safe(r)]

    if not high:
        print("  No HIGH confidence rows to apply.\n")
        return

    print(f"Applying {len(high)} HIGH confidence fixes ...")
    for r in high:
        name   = r["company_name"]
        domain = r["proposed_domain"]
        print(f"  PATCH  {name:<36} → {domain:<25} [{r['source']}]", end=" ... ", flush=True)
        if DRY_RUN:
            print("(dry-run)")
        else:
            ok = patch_notion_domain(r["page_id"], domain)
            print("OK" if ok else "FAILED")
            time.sleep(0.4)
    print()


# ── Step 5: MEDIUM confidence — interactive prompts ───────────────────────────

def apply_medium_confidence(rows) -> None:
    medium = [r for r in rows if r.get("confidence") == "MEDIUM" and is_safe(r)]

    if not medium:
        print("  No MEDIUM confidence rows to review.\n")
        return

    print(f"MEDIUM confidence rows — review each ({len(medium)} total).")
    print("  y = apply,  n = skip,  q = stop all MEDIUM processing\n")

    for r in medium:
        name     = r["company_name"]
        current  = r["current_domain"] or "(empty)"
        proposed = r["proposed_domain"]
        source   = r["source"]

        print(f"  Company : {name}")
        print(f"  Current : {current}")
        print(f"  Proposed: {proposed}  (via {source})")

        try:
            choice = input("  Apply? [y/n/q]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Interrupted — stopping MEDIUM loop.")
            break

        if choice == "q":
            print("  Stopped at user request.")
            break
        elif choice == "y":
            if DRY_RUN:
                print("  (dry-run — skipping write)\n")
            else:
                ok = patch_notion_domain(r["page_id"], proposed)
                print(f"  → {'OK' if ok else 'FAILED'}\n")
                time.sleep(0.4)
        else:
            print("  Skipped.\n")


# ── Step 6: Print UNRESOLVED list ─────────────────────────────────────────────

def print_unresolved(rows) -> None:
    unres = [r for r in rows if r.get("confidence") == "UNRESOLVED"]
    if not unres:
        return

    print("=" * 72)
    print(f"  UNRESOLVED — {len(unres)} rows need manual lookup in Notion:")
    for r in unres:
        sid = r.get("stripe_id") or "(no Stripe ID)"
        print(f"    • {r['company_name']:<42} [{sid}]")
    print("=" * 72 + "\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Step 1: Load the audit JSON
    try:
        with open(INPUT_FILE, encoding="utf-8") as f:
            rows = json.load(f)
    except FileNotFoundError:
        print(f"\n  ERROR: '{INPUT_FILE}' not found.")
        print("  Run audit_mct_domains.py first to generate it.\n")
        sys.exit(1)

    print(f"\n  Loaded {len(rows)} rows from {INPUT_FILE}")

    # Step 2: Plan summary (always shown)
    print_plan_summary(rows)

    if DRY_RUN:
        print("  --dry-run mode: no writes. Remove the flag to apply.\n")
        return

    # Step 4: HIGH confidence — automatic
    apply_high_confidence(rows)

    # Step 5: MEDIUM confidence — interactive
    apply_medium_confidence(rows)

    # Step 6: UNRESOLVED — list only
    print_unresolved(rows)

    print("  Done. Re-run audit_mct_domains.py to verify current state.\n")


if __name__ == "__main__":
    main()
