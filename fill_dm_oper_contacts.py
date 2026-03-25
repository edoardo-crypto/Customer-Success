"""
fill_dm_oper_contacts.py
------------------------
Updates 'DM - Point of contact' and 'Oper - Point of contact' email fields
on every MCT row based on the curated contact research across all 76 companies.

Matching: case-insensitive partial match on company name.
Rows that don't match any entry in CONTACTS are left untouched.

Usage:
    python3 fill_dm_oper_contacts.py [--dry-run]
"""

import sys
import time
import requests
import creds

# ── Credentials ──────────────────────────────────────────────────────────────
NOTION_TOKEN = creds.get("NOTION_TOKEN")
DS_ID = "3ceb1ad0-91f1-40db-945a-c51c58035898"
NOTION_VER = "2025-09-03"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VER,
    "Content-Type": "application/json",
}

# ── Contact data ──────────────────────────────────────────────────────────────
# (company_name_fragment, dm_email, oper_email)
# Fragment is matched case-insensitively against the MCT company name title.
# DM = decision maker (founder/CEO/owner)
# Oper = day-to-day operative user
# When DM = Oper it means only one contact was found or identified.
CONTACTS = [
    # ── Group A — domain companies ──────────────────────────────────────────
    ("Eturel",               "miryam@eturel.com",                          "miryam@eturel.com"),
    ("Pés de Cereja",        "bsuarez@pesdecereja.pt",                     "bsuarez@pesdecereja.pt"),
    ("Calzados Pablo",       "pablo.valle@pablosky.com",                   "pablo.valle@pablosky.com"),
    ("IDOL VERSION",         "web@confusionwear.com",                      "web@confusionwear.com"),
    ("Remolonas",            "carlos@remolonas.com",                       "carlos@remolonas.com"),          # CEO & Co-Founder confirmed
    ("Labienhecha",          "irene@labienhecha.com",                      "irene@labienhecha.com"),
    ("REAL EARTH STORIES",   "tina@realearthstories.es",                   "tina@realearthstories.es"),
    ("Levid Cosmetics",      "info@levidcosmetics.com",                    "info@levidcosmetics.com"),
    ("Huellas de Ibiza",     "adrianrosa@huellasdeibiza.com",              "adrianrosa@huellasdeibiza.com"),
    ("Labei Cosmetics",      "inaxio@labeicosmetics.com",                  "inaxio@labeicosmetics.com"),
    ("Endor Technologies",   "joaquin.querol@endornanotech.com",           "joaquin.querol@endornanotech.com"),
    ("HEY CHAI GIRLS",       "alejandra@sachatelier.com",                  "hello@sachatelier.com"),         # alejandra = co-founder; hello = daily ops
    ("IC Media Marketing",   "iker@ic-mediamarketing.com",                 "iker@ic-mediamarketing.com"),
    ("LOVE DIGITAL FACTORY", "alina.franco@platanomelon.com",              "alina.franco@platanomelon.com"),
    ("Health Nutrition Lab", "marc@healthnutritionlab.com",                "marc@healthnutritionlab.com"),
    ("MERCAJEANS",           "pedidos@mercajeans.com",                     "pedidos@mercajeans.com"),
    ("ELISA RIVERA",         "marketing@eduardorivera.es",                 "marketing@eduardorivera.es"),
    ("LEGEND LIFESTYLE",     "info@luxurylove.store",                      "info@luxurylove.store"),
    ("Gfa Group",            "juan@yuxus.com",                             "juan@yuxus.com"),
    ("Midnight 00.00",       "ana@neworder-midnight.com",                  "ana@neworder-midnight.com"),
    ("GIMAGUAS",             "cgarcia@gimaguas.com",                       "cgarcia@gimaguas.com"),
    ("Futboltek",            "angel.riaza@voonsports.com",                 "angel.riaza@voonsports.com"),
    ("Crea Decora Recicla",  "hola@creadecorarecicla.com",                 "hola@creadecorarecicla.com"),
    ("LELOi AB",             "ivana.hanzek@lelo.com",                      "ivana.hanzek@lelo.com"),
    ("Flor de Madre",        "liam@flordemadre.co.uk",                     "liam@flordemadre.co.uk"),
    ("The Balance Phone",    "carlos@thebalancephone.com",                 "carlos@thebalancephone.com"),
    ("English Path",         "alejandro+englishpath@ic-mediamarketing.com","alejandro+englishpath@ic-mediamarketing.com"),
    ("Tomsstudio",           "alice@tomsstudio.co.uk",                     "alice@tomsstudio.co.uk"),
    ("Asesoria Clientes",    "hola@asesoriaclientes.com",                  "hola@asesoriaclientes.com"),
    ("Nibiru",               "marco@nibiru.mx",                            "marco@nibiru.mx"),
    ("The Stage Ventures",   "maria.roibas@wowshop.com",                   "maria.roibas@wowshop.com"),     # WowShop — no CEO found, Lucas Retail email unknown
    ("International Cosmetic","admin@international-cosmetic.com",          "admin@international-cosmetic.com"),
    ("Moma Bikes",           "juan.morera@momabikes.com",                  "juan.morera@momabikes.com"),
    ("Frutas Marol",         "alvaro@frutasmarol.com",                     "alvaro@frutasmarol.com"),
    ("ATFIRSTSIGHT",         "hello@atfirstsightstudio.com",               "hello@atfirstsightstudio.com"), # Blanca Yong — no founder found
    ("Farmaciasdirect",      "enaranjo@farmaciasdirect.com",               "enaranjo@farmaciasdirect.com"),
    ("MindTravelerBcn",      "oriol@mindtravelerbcn.com",                  "oriol@mindtravelerbcn.com"),
    ("Liria Jewels",         "virginia@oratore-consulting.com",            "virginia@oratore-consulting.com"),
    ("Cachito de Zielo",     "info@cachitodezielo.es",                     "info@cachitodezielo.es"),
    ("B-ETHIC",              "lolamb@balakata.com",                        "lolamb@balakata.com"),           # Lola Martinez B., Co-founder (LinkedIn confirmed)
    ("SEISDELTRES",          "asor@seisdeltres.com",                       "asor@seisdeltres.com"),
    ("Deeply Europe",        "lflores@deeply.com",                         "lflores@deeply.com"),
    ("Simuero",              "jorge@simuero.com",                          "jorge@simuero.com"),
    ("Homologation",         "josecarlos@hostudents.com",                  "josecarlos@hostudents.com"),
    ("Tattoox",              "xavier.salvat@tattoox.io",                   "xavier.salvat@tattoox.io"),
    ("PureSkincare",         "matias@puresc.com",                          "matias@puresc.com"),
    ("La Caja Saludable",    "data@shopimasters.es",                       "data@shopimasters.es"),
    ("Wildraincosmetics",    "hello@wildraincosmetics.com",                "hello@wildraincosmetics.com"),
    ("IBG Illice",           "a.palacios@illice.com",                      "a.palacios@illice.com"),
    ("Sepiia",               "veronica@sepiia.com",                        "monica@sepiia.com"),             # veronica = advisor/DM; monica = Ecommerce Manager
    ("Redondo Brand",        "info@redondobrand.com",                      "info@redondobrand.com"),
    ("Diversual",            "fernando@diversual.com",                     "barbaramontes@diversual.com"),   # CEO found in HubSpot; barbara = ops
    ("ZZEN Labs",            "jose@zzenlabs.com",                          "jose@zzenlabs.com"),
    ("Electrotodo",          "pepe@electrotodo.es",                        "dani@electrotodo.es"),           # pepe = CEO; dani = daily user
    ("Alpha Spirit",         "export@aspiritpetfood.com",                  "almacen@aspiritpetfood.com"),    # Francisco Castillo (Head of Sales) = DM
    ("UNISA",                "erica@unisa-europa.com",                     "erica@unisa-europa.com"),
    ("Matcha Jeans",         "laura@matchajeans.com",                      "laura@matchajeans.com"),
    ("PICSIL",               "leire.rubio@picsilsport.com",                "leire.rubio@picsilsport.com"),
    ("Synsera",              "hello@synseralabs.com",                      "ainoha@synseralabs.com"),        # no founder found; ainoha = named active contact

    # ── Group B — gmail/hotmail (DM = Oper = single known contact) ──────────
    ("Perfumara",            "juligan70@gmail.com",                        "juligan70@gmail.com"),
    ("Escalamos",            "gerenteshelpfly@gmail.com",                  "gerenteshelpfly@gmail.com"),
    ("JORME ONLINE",         "sitaa934@gmail.com",                         "sitaa934@gmail.com"),
    ("DECOFLORIMPERIAL",     "decoflortoledo@gmail.com",                   "decoflortoledo@gmail.com"),
    ("DUKE TRADING",         "dukefotografiaes@gmail.com",                 "dukefotografiaes@gmail.com"),
    ("EMLIFE BIOTICS",       "emlifeorg@gmail.com",                        "emlifeorg@gmail.com"),
    ("il baco da seta",      "mlloretdp@gmail.com",                        "mlloretdp@gmail.com"),
    ("KLAT SAS",             "mmaarianaleeis@gmail.com",                   "mmaarianaleeis@gmail.com"),
    ("Oh My Wax",            "ohmywax.brand@gmail.com",                    "ohmywax.brand@gmail.com"),
    ("Finca la Mesa",        "fincalamesasl@gmail.com",                    "fincalamesasl@gmail.com"),
    ("Funda Hogar",          "orioln00@gmail.com",                         "orioln00@gmail.com"),
    ("Lumara Shop",          "carobrizuela28@gmail.com",                   "carobrizuela28@gmail.com"),
    ("NICE MOOD 24",         "nutripromungia@gmail.com",                   "nutripromungia@gmail.com"),
    ("ECOMDELSUR",           "milura1234@gmail.com",                       "milura1234@gmail.com"),
    ("Laazo 80",             "patrimj00@gmail.com",                        "patrimj00@gmail.com"),
    ("Futbolkit",            "futbolkit@hotmail.com",                      "futbolkit@hotmail.com"),
]

# ── Fetch all MCT pages ───────────────────────────────────────────────────────
def fetch_all_pages():
    pages, cursor = [], None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/data_sources/{DS_ID}/query",
            headers=HEADERS, json=body
        )
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


def get_title(page):
    for val in page.get("properties", {}).values():
        if val.get("type") == "title":
            return "".join(p.get("plain_text", "") for p in val.get("title", []))
    return ""


def patch_page(page_id, dm_email, oper_email):
    body = {
        "properties": {
            "DM - Point of contact":   {"email": dm_email},
            "Oper - Point of contact": {"email": oper_email},
        }
    }
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=HEADERS, json=body
    )
    return r.status_code


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("DRY RUN — no Notion PATCHes will be made\n")

    print("Fetching MCT pages...")
    pages = fetch_all_pages()
    print(f"  {len(pages)} pages loaded\n")

    # Build title → page_id lookup
    title_map = {get_title(p): p["id"] for p in pages}

    updated, skipped, errors = 0, 0, []

    for fragment, dm, oper in CONTACTS:
        frag_lower = fragment.lower()
        match = next((t for t in title_map if frag_lower in t.lower()), None)

        if not match:
            print(f"  [SKIP] No match for: '{fragment}'")
            skipped += 1
            continue

        diff_marker = "  ← DM≠Oper" if dm != oper else ""
        if dry_run:
            print(f"  [DRY]  {match}")
            print(f"           DM:   {dm}")
            print(f"           Oper: {oper}{diff_marker}")
            updated += 1
            continue

        status = patch_page(title_map[match], dm, oper)
        if status == 200:
            print(f"  [OK]   {match}{diff_marker}")
            updated += 1
        else:
            print(f"  [ERR]  {match} → HTTP {status}")
            errors.append(match)

        time.sleep(0.35)

    print(f"\nDone: {updated} updated, {skipped} skipped, {len(errors)} errors")
    if errors:
        print("Failed:", errors)


if __name__ == "__main__":
    main()
