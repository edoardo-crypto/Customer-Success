#!/usr/bin/env python3
"""
add_linked_views.py
Phase 2 of CS Onboarding Linked Views setup.

Uses Playwright (visible browser) to add 3 linked database views
to the top 25 most recently created customer pages.

Run: python3 add_linked_views.py
- A browser window opens. Log into Notion if prompted.
- After the first test page succeeds, press Enter to continue with the rest.

Archive to archive/ after run.
"""

import time, os, sys
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

SESSION_DIR = "/tmp/notion-pw-session"
os.makedirs(SESSION_DIR, exist_ok=True)

# Top 25 most recently created Active/Churning customers
CUSTOMER_PAGES = [
    ("Grippadel",                           "313e418fd8c4801a9cc6c86a2e84db98"),
    ("Vinkova Leotards",                    "312e418fd8c48176a260c81c5f34e0a4"),
    ("Ace & Tate Spain S.L",               "311e418fd8c481dab23af7cf5d5dc9fc"),
    ("VALMAS GROUP LIMITED",                "311e418fd8c481b89019c6e97111b120"),
    ("The Cool Bottles Company SL",         "311e418fd8c481b4a675f27f13f8fee2"),
    ("BAYMO THE LABEL",                     "311e418fd8c4813fa5c6eb7b8707ae32"),
    ("CO2 YOU Limited",                     "30ce418fd8c481b795c0cef90416efb7"),
    ("Mahogany Enterprises S.L.",           "30ce418fd8c481c6a2f7c3436e51c652"),
    ("FemmeUp",                             "30ce418fd8c4818e95b5cb86333b500c"),
    ("Sherperex SL",                        "30ce418fd8c4817c97dbcb6bf539927e"),
    ("Nomade Nation SL",                    "30ae418fd8c4814b8e04c91b5acba686"),
    ("CHOCOLATES TORRAS, S.A.",             "30ae418fd8c48107af08ca0acba95ba7"),
    ("Nationaal Oogcentrum",                "302e418fd8c481529419e09112cea52a"),
    ("Indian Ocean Consulting, S.L.",       "302e418fd8c4819eb9a7daba2752492a"),
    ("The Salad Code Company",              "302e418fd8c481f5a814e7bde094ae79"),
    ("NATURAL SMART BEAUTY SL",             "302e418fd8c481f8846efb678b88e9c0"),
    ("ODOREM MEDITERRANEA, S.L.",           "302e418fd8c481ba9f32e25b092e3c94"),
    ("Live Out Solutions SL",               "302e418fd8c481b19e2fdd83438447e2"),
    ("Otso Sport",                          "302e418fd8c481fea431dc99dd2e82a0"),
    ("SINGULAR WARDROBE S.L",              "302e418fd8c4816db00ccea8586bc832"),
    ("MU Brand",                            "302e418fd8c4810bb8f7dde0fab72148"),
    ("ACUARIUM SUCS SLU",                  "302e418fd8c481a6acd1ca284ac5aa1c"),
    ("Mood Collection SA",                  "302e418fd8c481afbc7ff56477dae21e"),
    ("Valquer Laboratorios S.L.U.",         "302e418fd8c4815b9e8ee7303b0bc8ec"),
    ("San Jorge Distribuciones de Café S.L.", "302e418fd8c481c4902ee860a6123a8c"),
]

DATABASES = [
    ("CS Blockers",  "CS Blockers & Next Actions"),
    ("CS Success",   "CS Success Criteria"),
    ("CS To",        "CS To Dos"),
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def escape_all(page):
    """Dismiss any open popover / menu."""
    for _ in range(4):
        page.keyboard.press("Escape")
        time.sleep(0.2)

def click_page_body(page):
    """Click into the main content editor area (below the title)."""
    # Try placeholder text first
    placeholder_selectors = [
        "[data-placeholder=\"Type '/' for commands\"]",
        "[data-placeholder=\"Press Enter to continue with an empty page\"]",
        "[placeholder=\"Type '/' for commands\"]",
    ]
    for sel in placeholder_selectors:
        loc = page.locator(sel)
        if loc.count() > 0:
            loc.first.click()
            time.sleep(0.3)
            return

    # Fallback: click in the lower half of the viewport
    vp = page.viewport_size or {"width": 1440, "height": 900}
    page.mouse.click(vp["width"] // 2, int(vp["height"] * 0.6))
    time.sleep(0.3)


def add_one_linked_view(page, db_search, db_full_name, customer_name):
    """
    With cursor in the page body, add one linked view of db_full_name
    and filter it to customer_name.
    Returns True on success, False on failure.
    """

    # ── 1. Open slash command ──────────────────────────────────────────────────
    click_page_body(page)
    time.sleep(0.4)
    # Make sure we're at the end, create a new block
    page.keyboard.press("End")
    page.keyboard.press("Enter")
    time.sleep(0.3)

    page.keyboard.type("/linked view", delay=40)
    time.sleep(1.5)

    # ── 2. Select "Linked view of database" from slash menu ────────────────────
    found = False
    for attempt, text in enumerate([
        "Linked view of database",
        "Linked view",
        "Create linked view",
    ]):
        try:
            opt = page.locator(f'[role="option"]:has-text("{text}")').first
            opt.wait_for(state="visible", timeout=3000)
            opt.click()
            found = True
            break
        except PWTimeout:
            pass

    if not found:
        print(f"      ✗ Could not find 'Linked view' option in slash menu")
        escape_all(page)
        return False

    time.sleep(1.5)

    # ── 3. Pick the database ───────────────────────────────────────────────────
    # The database picker shows a search input
    try:
        search = page.locator('input[type="text"]').last
        search.wait_for(state="visible", timeout=4000)
        search.type(db_search, delay=40)
    except PWTimeout:
        # Might auto-focus; just type
        page.keyboard.type(db_search, delay=40)
    time.sleep(1.5)

    try:
        db_item = page.locator(f'[role="option"]:has-text("{db_full_name}")').first
        if db_item.count() == 0:
            db_item = page.locator(f'text="{db_full_name}"').first
        db_item.wait_for(state="visible", timeout=5000)
        db_item.click()
    except PWTimeout:
        print(f"      ✗ Could not find database '{db_full_name}' in picker")
        escape_all(page)
        return False

    time.sleep(2.5)  # Let the view fully render

    # ── 4. Add filter ─────────────────────────────────────────────────────────
    # Find the Filter button in the view toolbar
    filter_clicked = False
    for filter_text in ["Filter", "Filters"]:
        try:
            btn = page.locator(f'button:has-text("{filter_text}")').last
            btn.wait_for(state="visible", timeout=4000)
            btn.click()
            filter_clicked = True
            break
        except PWTimeout:
            pass

    if not filter_clicked:
        # Try scrolling to find it
        page.keyboard.press("Escape")
        try:
            btn = page.get_by_role("button", name="Filter").last
            btn.scroll_into_view_if_needed()
            btn.click()
            filter_clicked = True
        except Exception:
            print(f"      ✗ Could not find Filter button for '{db_full_name}'")
            return False

    time.sleep(0.8)

    # Click "Add a filter" (or "Add filter")
    add_filter_clicked = False
    for text in ["Add a filter", "Add filter", "Add filter rule"]:
        try:
            loc = page.locator(f'text="{text}"').first
            loc.wait_for(state="visible", timeout=3000)
            loc.click()
            add_filter_clicked = True
            break
        except PWTimeout:
            pass

    if not add_filter_clicked:
        print(f"      ✗ Could not find 'Add a filter' for '{db_full_name}'")
        escape_all(page)
        return False

    time.sleep(0.8)

    # ── 5. Select "Customer" property ─────────────────────────────────────────
    try:
        cust_prop = page.locator('[role="option"]:has-text("Customer")').first
        cust_prop.wait_for(state="visible", timeout=4000)
        cust_prop.click()
    except PWTimeout:
        print(f"      ✗ Could not find 'Customer' property in filter list")
        escape_all(page)
        return False

    time.sleep(0.8)

    # ── 6. Select the customer value (relation picker) ─────────────────────────
    # Type partial customer name to search
    search_name = customer_name[:25]
    try:
        inp = page.locator('input[type="text"]').last
        inp.wait_for(state="visible", timeout=3000)
        inp.type(search_name, delay=40)
    except PWTimeout:
        page.keyboard.type(search_name, delay=40)
    time.sleep(1.5)

    # Click the matching result
    picked = False
    # Try exact name first, then progressively shorter match
    for try_name in [customer_name, customer_name[:20], customer_name[:15], customer_name[:10]]:
        try:
            result = page.locator(f'[role="option"]:has-text("{try_name}")').first
            result.wait_for(state="visible", timeout=2500)
            result.click()
            picked = True
            break
        except PWTimeout:
            pass

    if not picked:
        print(f"      ✗ Could not pick customer '{customer_name}' from relation filter")
        escape_all(page)
        return False

    time.sleep(0.5)
    escape_all(page)

    print(f"      ✓ {db_full_name}")
    return True


def process_customer(ctx, page_id, customer_name):
    """Open customer page and add all 3 linked views."""
    url = f"https://www.notion.so/{page_id}"
    page = ctx.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        time.sleep(3)

        print(f"\n  → {customer_name}")

        for db_search, db_full_name in DATABASES:
            ok = False
            for attempt in range(2):
                try:
                    ok = add_one_linked_view(page, db_search, db_full_name, customer_name)
                    if ok:
                        break
                except Exception as e:
                    print(f"      ✗ attempt {attempt+1} failed: {e}")
                    escape_all(page)
                    time.sleep(1)
            if not ok:
                print(f"      ⚠ Skipped {db_full_name} for {customer_name}")
    finally:
        page.close()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=SESSION_DIR,
            headless=False,
            viewport={"width": 1440, "height": 900},
            args=["--window-size=1440,900"],
            slow_mo=50,
        )

        # Login check
        page = ctx.new_page()
        page.goto("https://www.notion.so", timeout=20000)
        page.wait_for_load_state("domcontentloaded")
        time.sleep(2)

        if "login" in page.url or page.url.rstrip("/") == "https://www.notion.so":
            print("\n" + "="*60)
            print("  Please log in to Notion in the browser window.")
            print("  Waiting up to 3 minutes...")
            print("="*60 + "\n")
            try:
                page.wait_for_url("https://www.notion.so/**/*", timeout=180000)
                time.sleep(2)
            except PWTimeout:
                print("Login timed out. Exiting.")
                ctx.close()
                sys.exit(1)
        else:
            print("Notion already logged in.")
        page.close()

        # ── Which pages to run — controlled by env var ─────────────────────────
        # TEST_ONLY=1  → only first customer
        # (default)    → all 25
        test_only = os.environ.get("TEST_ONLY", "") == "1"

        if test_only:
            to_process = CUSTOMER_PAGES[:1]
            print(f"\nTEST MODE: only {to_process[0][0]}")
        else:
            to_process = CUSTOMER_PAGES
            print(f"\nProcessing all {len(to_process)} customers…")

        for name, pid in to_process:
            process_customer(ctx, pid, name)

        print("\n✓ Done!")
        time.sleep(3)
        ctx.close()


if __name__ == "__main__":
    main()
