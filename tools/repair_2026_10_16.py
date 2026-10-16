# -*- coding: utf-8 -*-
"""Morning fix pass, 2026-10-16.

Run on a FRESH clone of main by tools/rebase_and_verify.py, which then runs
pipeline.promote_gate_passing --apply behind it.

NOTHING IS REMOVED from Recalls or from Pending. The commit message needs
no deletion marker.

Every fact written below was read on an authority page during this pass, or
is in the row's own text/URL. Each Notes stamp says which page and what it
said. Where a page could not be opened, the row is LEFT ALONE and the
reason is reported — not guessed around.

Measured on main at base 13fc1415 (2026-10-16 07:45 UTC), Recalls 2050 rows:
FOUR tests are red.


FAULT A — FOUR RAPPELCONSO ROWS PUBLISHED IN FRENCH  (3 red tests)
==================================================================
    tests/test_product_and_firm_names_are_english.py
        ::test_no_published_product_reads_in_another_language
    tests/test_reason_is_not_wholly_foreign.py
        ::test_no_published_reason_is_wholly_foreign[Recalls]
    tests/test_language_policy.py
        ::TestPublishedWorkbook::test_every_description_field_is_english

Six rows in all, promoted 2026-10-14 and 2026-10-15. Only TWO carry
reviewer 3's "[needs cleanup: Product not in English]" stamp (fiches 23793
and 23756); the other four are the same detector miss as "foie de poulet"
— detect_language's two-hit threshold cannot see a short name, and a
Reason that opens with an English word stays under the wholly-foreign
threshold. The threshold is deliberately NOT loosened. The morning read of
every Product published in the last 26 h is what caught these.

Read this pass on the fiches themselves (rappel.conso.gouv.fr):

  23793  Dénomination "Tomes des Bauges", marque "Fruitière du massif de
         Semnoz", motif "Présence E.Colis STEC", published 15/10/2026.
  23796  Dénomination "Filet de poulet thym citron", marque "Les
         volaillers de Keranna", motif "présence de salmonelles",
         published 15/10/2026.  The published Product held NO food
         description at all — only the lot and date text. The denomination
         comes from the fiche; the lot/date detail already on the row is
         kept exactly as it was.
  23797  Dénomination "PERFETTO MOKA CARAMELLO", marque "BIALETTI", motif
         "Teneurs en ochratoxine A supérieures aux limites réglementaires
         autorisées", published 15/10/2026.  The denomination is the
         product's own commercial name, so it is kept as published (case
         normalised only) — only the Reason is translated.
  23761  Dénominations 'Mélange CURRY "COOK" 35g*', 'CORIANDRE moulue
         "COOK" 30g*', 'Mélange CURRY MADRAS "COOK" 35g*', 'Mélange
         TANDOORI "COOK" 35g*', 'CORIANDRE moulue sachet coussin "COOK"
         500g*'; marque "Cook"; motif "suite à la mise en évidence de la
         présence de salmonelles"; risque "Salmonella spp"; published
         15/10/2026.
  23788  Dénomination "TOME DES BAUGES AOP (VENDU AU RAYON COUPE)", marque
         "NOS REGIONS ONT DU TALENT", motif "Présence de E. Coli STEC
         O26:H11.", fiche réf. 2026-10-0117, published 14/10/2026.  Reason
         was already English; only the serotype case is corrected to the
         fiche's own "O26:H11" and the Product is translated.
  23756  Dénomination "Escalope de poulet milanaise", marque "sans
         marque", motif "présence de Salmonelle Infantis", published
         14/10/2026.

AOP/protected names, brand names and lot/weight/date details are kept as
written. The original French is kept in Notes so the dashboard can show it
on hover and search it.


FAULT B — FOUR RECALLS BOTH PUBLISHED AND REJECTED  (1 red test)
================================================================
    tests/test_a_recall_is_not_both_published_and_rejected.py
        ::test_every_shared_url_says_which_copy_wins[Weekly_Rejected-...]

I FIRST COUNTED EIGHT AND THAT WAS WRONG. My scan read Status,
RejectionReason and Notes — and the Rejected sheet's verdict column is
called RejectReason, not RejectionReason, so for that whole sheet the
scan was reading an absent column and seeing silence. Re-read per sheet:
the three Mattilsynet rows already say SUPERSEDED/UNRESOLVED and the four
FSAI Macroom rows already say DUPLICATE, all in RejectReason. They are
already annotated and are not touched.

FOUR rows are genuinely silent, all in Weekly_Rejected, and they are
exactly the four the failing test names. Nothing is deleted — archives are
append-only; each is annotated SUPERSEDED, naming why the published row
wins. The seven already-annotated rows are left alone by an explicit
check, printed as "already has a verdict".

ALL FOUR SHARE ONE CAUSE, and it is a code defect, fixed in the code half
of this pass:

    gap_finder/us/rules.py rejected four FSIS IMPORT-ELIGIBILITY notices
    as "allergen: Undeclared allergen — out of scope (Rule B reject)".

They are not allergen recalls. Every one is in scope and published:

    asian-america-trading   RTE fish skin crackers, China not eligible
    el-eden-import          chips w/ chicharrón, Colombia not eligible
    sempio-food             RTE chicken stew, no import reinspection
    shanghai-ravioli        produced without inspection, false EST. mark

pipeline/gap_finder/rules.py classify() carries no uninspected-product
vocabulary at all, so such a notice can only fall through to the allergen
reject (the "fish" in "Siluriformes fish products" is an allergen lexicon
word) or to "unknown". See the code half for the fix and its test.

The four Mattilsynet / FSAI URLs stay in the table below as no-op guards,
so that if a later run of this script meets one of them still silent it is
annotated rather than missed.


FAULT C — THE CFIA CLASS-1 LISTERIA CHEESE RECALL IS STUCK IN PENDING
=====================================================================
Pending row (ScrapedAt 2026-10-15T07:11:49Z) blocks on "Company is empty",
so the offline promoter cannot publish it. CFIA has had NO row in Recalls
since 2026-09-29 — 17 days.

Read this pass on the regulator's own English notice
(recalls-rappels.canada.ca/en/alert-recall/various-brands-organic-cheeses-
recalled-due-listeria-monocytogenes):

    Recalling firm   Fromagerie L'Ancêtre Inc.
    Hazard           Listeria monocytogenes
    Class            Class 1
    Reference        RA-82779  (CFIA ID 17605)
    Published        2026-10-14, last updated 2026-10-14
    Illnesses        none reported
    12 products      Farm Boy Organic Swiss 190 g; Organic Medium Cheddar
                     200 g; Organic Old Cheddar 200 g; Organic Extra Sharp
                     Cheddar 200 g; Organic 3 Year Old Cheddar 195 g.
                     Life Smart Organic Part Skim Mozzarella 200 g.
                     L'Ancêtre Organic Swiss 200 g; Organic Cheddar
                     Shredded Old 2 kg; Organic Mozza-Cheddar Shredded
                     Blend 200 g; Organic Mozzarella Shredded Partially
                     Skimmed 1 kg; Organic Swiss Shredded 1 kg; Organic
                     Cheddar Shredded Medium 2 kg.

The collector stored the FRENCH notice URL. Every other CFIA row in the
register cites the /en/ notice, so the URL is moved to the English page —
the same notice, read this pass — and the French one is kept in Notes.
The Product field held a BRAND LIST ("L'Ancêtre Farm Boy Mieux-être"), not
a product; it is replaced by the products the notice lists.


FAULT D — THREE RECALLS THE PIPELINE MISSED  (see MISSED below)
===============================================================
One FDA, three Japan CAA. Each read on the regulator's own per-recall
page this pass. Dedup checked against all five sheets by URL, company,
product and a distinctive word before being called missing.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-16"
_SWEPT = ("read on the authority page by the morning fix pass %s; "
          "dedup checked across Recalls, Pending, Weekly_Review, "
          "Weekly_Rejected and Rejected" % TODAY)

FICHE = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"

CFIA_EN = ("https://recalls-rappels.canada.ca/en/alert-recall/"
           "various-brands-organic-cheeses-recalled-due-listeria-"
           "monocytogenes")
CFIA_FR = ("https://recalls-rappels.canada.ca/fr/avis-rappel/"
           "rappel-fromages-biologiques-diverses-marques-en-raison-"
           "bacterie-listeria-monocytogenes")

CAA = "https://www.recall.caa.go.jp/result/detail.php?rcl=%s&screenkbn=01"

# Reviewer 3 / offline-promoter stamps that a translated row no longer needs.
STAMPS = ("[needs cleanup: Product not in English]",
          "[needs cleanup: Company/Brand not in English]")


# ── shared helpers (same shape as tools/repair_2026_10_14.py) ────────────
def _hdr(ws):
    return [str(c.value).strip() if c.value is not None else ""
            for c in next(ws.iter_rows(min_row=1, max_row=1))]


def _urls(ws):
    hdr = _hdr(ws)
    if "URL" not in hdr:
        return []
    col = hdr.index("URL") + 1
    return [(r, str(ws.cell(row=r, column=col).value or "").strip())
            for r in range(2, ws.max_row + 1)]


def _find(ws, url):
    """Row numbers whose URL is EXACTLY `url`, ignoring a trailing slash
    and case. Exact, never substring."""
    want = url.rstrip("/").lower()
    return [r for r, u in _urls(ws) if u.rstrip("/").lower() == want]


def _find_prefix(ws, stem):
    """Row numbers whose URL STARTS WITH `stem`. Used only where the
    stored URL is a truncated or query-string variant of a notice whose
    slug stem is unique in the sheet."""
    want = stem.rstrip("/").lower()
    return [r for r, u in _urls(ws) if u.rstrip("/").lower().startswith(want)]


def _set(ws, row, field, value):
    hdr = _hdr(ws)
    if field not in hdr:
        return False
    cell = ws.cell(row=row, column=hdr.index(field) + 1)
    if str(cell.value or "") == str(value):
        return False
    cell.value = value
    return True


def _get(ws, row, field):
    hdr = _hdr(ws)
    if field not in hdr:
        return ""
    v = ws.cell(row=row, column=hdr.index(field) + 1).value
    return "" if v is None else str(v)


def _note(ws, row, text, *, drop=()):
    prev = _get(ws, row, "Notes").strip()
    if prev.lower() == "nan":
        prev = ""
    elif prev.lower().startswith("nan "):
        prev = prev[4:].strip()
    for d in drop:
        prev = prev.replace(d, "").strip()
    while "  " in prev:
        prev = prev.replace("  ", " ")
    if text and text in prev:
        return
    _set(ws, row, "Notes", (prev + " " + text).strip())


# ── FAULT A — the six French rows ────────────────────────────────────────
# fiche -> (Product EN, Reason EN, Notes stamp)
_FRENCH = {
    "23793": (
        "Tome des Bauges (sold cut at the counter)",
        "Presence of Shiga toxin-producing E. coli (STEC)",
        'fiche read %s: dénomination "Tomes des Bauges", marque '
        '"Fruitière du massif de Semnoz", motif "Présence E.Colis STEC", '
        'published 15/10/2026. '
        '[original product: tome des bauges vendu au rayon  coupe] '
        '[original Reason (fr): "Présence e.colis stec"]',
    ),
    "23796": (
        "Chicken fillet with thyme and lemon — lot 3226192709, "
        "use-by 8 October 2026, sold between 3 and 8 October 2026",
        "Presence of Salmonella",
        'fiche read %s: dénomination "Filet de poulet thym citron", marque '
        '"Les volaillers de Keranna", motif "présence de salmonelles", '
        'published 15/10/2026. The published Product named no food at all '
        '— it held only this row\'s own lot and date text, which is kept '
        'as written. '
        '[original product: Filet de poulet thym citron]',
    ),
    "23797": (
        "Perfetto Moka Caramello",
        "Ochratoxin A levels above the authorised regulatory limits",
        'fiche read %s: dénomination "PERFETTO MOKA CARAMELLO", marque '
        '"BIALETTI", motif "Teneurs en ochratoxine A supérieures aux '
        'limites réglementaires autorisées", published 15/10/2026. The '
        'denomination is the product\'s own commercial name and is kept as '
        'published (case normalised). '
        '[original Reason (fr): "Teneurs en ochratoxine a supérieures aux '
        'limites réglementaires autorisées"]',
    ),
    "23761": (
        'CURRY blend "COOK" 35 g; ground CORIANDER "COOK" 30 g; '
        'MADRAS CURRY blend "COOK" 35 g; TANDOORI blend "COOK" 35 g; '
        'ground CORIANDER pillow sachet "COOK" 500 g',
        "La Arcadie is today withdrawing from sale certain Cook products "
        "following the detection of the presence of Salmonella.",
        'fiche read %s: five dénominations \'Mélange CURRY "COOK" 35g*\', '
        '\'CORIANDRE moulue "COOK" 30g*\', \'Mélange CURRY MADRAS "COOK" '
        '35g*\', \'Mélange TANDOORI "COOK" 35g*\', \'CORIANDRE moulue '
        'sachet coussin "COOK" 500g*\'; marque "Cook"; risque "Salmonella '
        'spp"; published 15/10/2026. '
        '[original product: mélange curry cook 35g*; coriandre moulue cook '
        '30g*; mélange curry madras cook 35g*; mélange tandoori cook 35g*; '
        'coriandre moulue sachet coussin cook 500g*] '
        '[original Reason (fr): "La arcadie procède aujourd’hui au '
        'retrait de la vente de certains produits cook suite à la mise en '
        'évidence de la présence de Salmonella."]',
    ),
    "23788": (
        "Tome des Bauges AOP (sold cut at the counter)",
        "Presence of E. coli STEC O26:H11",
        'fiche read %s: dénomination "TOME DES BAUGES AOP (VENDU AU RAYON '
        'COUPE)", marque "NOS REGIONS ONT DU TALENT", motif "Présence de '
        'E. Coli STEC O26:H11.", fiche réf. 2026-10-0117, published '
        '14/10/2026. Reason was already English; the serotype case is '
        'corrected to the fiche\'s own "O26:H11". '
        '[original product: tome des bauges aop (vendu au rayon coupe)]',
    ),
    "23756": (
        "Milanese-style chicken escalope",
        "E.Leclerc Royan is withdrawing from sale and recalling the "
        "following products following the detection of the presence of "
        "Salmonella Infantis.",
        'fiche read %s: dénomination "Escalope de poulet milanaise", '
        'marque "sans marque", motif "présence de Salmonelle Infantis", '
        'published 14/10/2026. '
        '[original product: escalope de poulet milanaise] '
        '[original Reason (fr): "E.leclerc royan procède à un retrait de '
        'la vente et à un rappel des produits suivants suite à la mise en '
        'évidence de la présence de Salmonella infantis."]',
    ),
}


def translate_french_rows(wb) -> int:
    ws = wb["Recalls"]
    n = 0
    for fiche, (prod, reason, note) in _FRENCH.items():
        rows = _find(ws, FICHE % fiche)
        if not rows:
            print(f"    fiche {fiche}: not in Recalls — LEFT ALONE")
            continue
        for r in rows:
            touched = False
            touched |= _set(ws, r, "Product", prod)
            touched |= _set(ws, r, "Reason", reason)
            if touched:
                _note(ws, r, note % TODAY, drop=STAMPS)
                n += 1
                print(f"    fiche {fiche} row {r}: Product + Reason English")
            else:
                # Already translated on an earlier run of this script;
                # still drop a stamp that no longer applies.
                before = _get(ws, r, "Notes")
                _note(ws, r, "", drop=STAMPS)
                if _get(ws, r, "Notes") != before:
                    n += 1
    return n


# ── FAULT B — eight archive rows that contradict the register ───────────
# (sheet, URL-or-stem, prefix?, why the published copy wins)
_SUPERSEDE = [
    ("Weekly_Rejected",
     "https://www.fsis.usda.gov/recalls-alerts/asian-america-trading-inc--"
     "recalls-ineligible-siluriformes-fish-produ", True,
     "the published row is correct: an FSIS IMPORT-ELIGIBILITY notice "
     "(RTE Siluriformes fish products from China, a country not eligible "
     "to export them), in scope since 2026-09-25 and filed as "
     "'Uninspected product (hazard not assessed)'. The reject reason "
     "'allergen: Undeclared allergen' is a vocabulary defect in "
     "pipeline/gap_finder/rules.py classify(), which carries no "
     "uninspected-product terms, so the word 'fish' in the notice hit the "
     "allergen lexicon. Fixed in the code half of this pass "
     "(tests/test_import_eligibility_is_in_gap_finder_scope.py)"),
    ("Weekly_Rejected",
     "https://www.fsis.usda.gov/recalls-alerts/el-eden-import-distributor-"
     "corp-recalls-ineligible-por", True,
     "the published row is correct: an FSIS IMPORT-ELIGIBILITY notice "
     "(product produced in Colombia, not eligible to export meat products "
     "to the United States). Same classify() vocabulary defect as the "
     "Asian America Trading row; fixed in the code half of this pass"),
    ("Weekly_Rejected",
     "https://www.fsis.usda.gov/recalls-alerts/sempio-food-services-inc--"
     "recalls-ready-eat-chicken-st", True,
     "the published row is correct: RTE chicken stew imported from the "
     "Republic of Korea without the benefit of import reinspection. Same "
     "classify() vocabulary defect; fixed in the code half of this pass"),
    ("Weekly_Rejected",
     "https://www.fsis.usda.gov/recalls-alerts/shanghai-ravioli-"
     "corporation-recalls-not-ready-eat-fro", True,
     "the published row is correct: product produced without the benefit "
     "of inspection and bearing a false establishment number 'EST. 18004'. "
     "Same classify() vocabulary defect; fixed in the code half of this "
     "pass"),
    ("Rejected",
     "https://www.mattilsynet.no/tilbakekallinger/sbselite-bla-valmuefro-"
     "fra-tyrkia-tilbakekalles-etter-funn-av-plan", True,
     "the published row is correct and fully enriched (Akar Food AS, blue "
     "poppy seeds, opium alkaloids above the applicable limit). The "
     "archive copy is a news-discovery stub rejected by "
     "gap_finder/no/rules.py with no reason recorded"),
    ("Rejected",
     "https://www.mattilsynet.no/tilbakekallinger/foodtrade-as-"
     "tilbakekaller-pistasjkjerner-fra-usa-etter-funn-av-mu", True,
     "the published row is correct and fully enriched (Foodtrade AS, raw "
     "pistachio kernels, aflatoxin above the applicable limit). The "
     "archive copy is a news-discovery stub with no reason recorded"),
    ("Rejected",
     "https://www.mattilsynet.no/tilbakekallinger/baza-nordic-"
     "tilbakekaller-surkal-fra-litauen-etter-funn-av-glasspa", True,
     "the published row is correct and fully enriched (Baza Nordic, "
     "sauerkraut from Lithuania, glass particles). The archive copy is a "
     "news-discovery stub with no reason recorded"),
    ("Rejected",
     "https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-specific-"
     "batch-of-various-macroom-buffal", True,
     "the published row is correct and fully enriched (Macroom Buffalo "
     "Cheese Products Ltd., mozzarella/bocconcini/burrata, Listeria "
     "monocytogenes). The archive copy was left at status "
     "'pending_enrichment' on 2026-08-28 awaiting an enrichment that the "
     "published row already carries"),
]


def supersede_contradicting_archive_rows(wb) -> int:
    n = 0
    pub = {u.rstrip("/").lower() for _, u in _urls(wb["Recalls"]) if u}
    for sheet, url, is_prefix, why in _SUPERSEDE:
        ws = wb[sheet]
        rows = _find_prefix(ws, url) if is_prefix else _find(ws, url)
        if not rows:
            print(f"    {sheet}: {url[-48:]} not found — LEFT ALONE")
            continue
        for r in rows:
            stored = _get(ws, r, "URL").rstrip("/").lower()
            if stored not in pub:
                print(f"    {sheet} row {r}: no published row on this URL "
                      f"— LEFT ALONE")
                continue
            # Only rows that are SILENT. A row that already says
            # SUPERSEDED, DUPLICATE or UNRESOLVED has a verdict; the
            # archive is append-only and an existing verdict is not ours
            # to overwrite. This is the same criterion
            # test_a_recall_is_not_both_published_and_rejected.py uses.
            verdict_blob = " ".join(
                _get(ws, r, f) for f in
                ("Status", "RejectionReason", "RejectReason", "Notes")
            ).upper()
            if any(w in verdict_blob
                   for w in ("SUPERSEDED", "DUPLICATE", "UNRESOLVED")):
                print(f"    {sheet} row {r}: already has a verdict "
                      f"— LEFT ALONE")
                continue
            stamp = ("[SUPERSEDED %s — %s. Nothing is deleted; the "
                     "published row wins.]" % (TODAY, why))
            _note(ws, r, stamp)
            # Say it in the sheet's own verdict column too, so the reader
            # of the archive does not have to open Notes.
            for field in ("RejectionReason", "RejectReason"):
                if field in _hdr(ws):
                    cur = _get(ws, r, field).strip()
                    if "SUPERSEDED" not in cur:
                        _set(ws, r, field,
                             ("SUPERSEDED %s (the published row wins) — %s"
                              % (TODAY, cur)) if cur else
                             "SUPERSEDED %s — the published row wins" % TODAY)
            if "Status" in _hdr(ws) and "SUPERSEDED" not in _get(ws, r, "Status"):
                _set(ws, r, "Status", "SUPERSEDED")
            n += 1
            print(f"    {sheet} row {r}: SUPERSEDED")
    return n


# ── FAULT C — unblock the CFIA Class-1 Listeria cheese recall ───────────
_CFIA_PRODUCTS = (
    "Farm Boy Organic Swiss Cheese 190 g; Farm Boy Organic Medium Cheddar "
    "Cheese 200 g; Farm Boy Organic Old Cheddar Cheese 200 g; Farm Boy "
    "Organic Extra Sharp Cheddar Cheese 200 g; Farm Boy Organic 3 Year Old "
    "Cheddar Cheese 195 g; Life Smart Organic Part Skim Mozzarella Cheese "
    "200 g; L'Ancêtre Organic Swiss Cheese 200 g; L'Ancêtre Organic Cheddar "
    "Cheese Shredded Old 2 kg; L'Ancêtre Organic Mozza-Cheddar Shredded "
    "Cheese Blend 200 g; L'Ancêtre Organic Mozzarella Cheese Shredded "
    "Partially Skimmed 1 kg; L'Ancêtre Organic Swiss Cheese Shredded 1 kg; "
    "L'Ancêtre Organic Cheddar Cheese Shredded Medium 2 kg"
)


def unblock_cfia_cheese_row(wb) -> int:
    ws = wb["Pending"]
    rows = _find(ws, CFIA_FR) + _find(ws, CFIA_EN)
    if not rows:
        print("    CFIA cheese row not in Pending — LEFT ALONE "
              "(it may already have been promoted)")
        return 0
    n = 0
    for r in sorted(set(rows)):
        if _get(ws, r, "Company").strip():
            continue
        _set(ws, r, "Company", "Fromagerie L'Ancêtre Inc.")
        _set(ws, r, "Brand", "Farm Boy; Life Smart; L'Ancêtre")
        _set(ws, r, "Product", _CFIA_PRODUCTS)
        _set(ws, r, "Reason",
             "Listeria monocytogenes contamination — CFIA Class 1 recall "
             "of 12 organic cheeses; no illnesses reported.")
        _set(ws, r, "URL", CFIA_EN)
        _note(ws, r,
              "[%s: Company, Brand and the 12 products read on the CFIA's "
              "own English notice RA-82779 (CFIA ID 17605), Class 1, "
              "originally published 2026-10-14, last updated 2026-10-14, "
              "which names the recalling firm 'Fromagerie L'Ancêtre Inc.' "
              "and reports no illnesses. The collector stored only a brand "
              "list in Product and no Company, which blocked the offline "
              "promoter on 'Company is empty'. URL moved from the French "
              "notice to the English one, which every other CFIA row in "
              "the register cites — same notice. "
              "[original URL (fr): %s] %s]" % (TODAY, CFIA_FR, _SWEPT))
        n += 1
        print(f"    Pending row {r}: Company + Product + URL filled "
              f"from the CFIA notice")
    return n


# ── FAULT D — recalls the pipeline missed ───────────────────────────────
MISSED = [
    {
        "Date": "2026-10-15",
        "Source": "FDA",
        "Company": "Family Farmstead Dairy, LLC (Worcester, NY)",
        "Brand": "Family Farmstead Dairy",
        "Product": ("Raw Mild Cheddar Cheese — one lot, lot code and "
                    "best-by 7/22/27"),
        "Pathogen": "Shiga toxin-producing E. coli (STEC)",
        "Reason": ("Product may be contaminated with Shiga toxin-producing "
                   "Escherichia coli O157:H7. Distributed to grocers in "
                   "New York and New Jersey; no illnesses reported."),
        "Class": "Voluntary",
        "Country": "United States",
        "Outbreak": 0,
        "URL": ("https://www.fda.gov/safety/recalls-market-withdrawals-"
                "safety-alerts/family-farmstead-dairy-announces-recall-raw-"
                "mild-cheddar-cheese-due-potential-e-coli-o157h7"),
        "Notes": ("[manual %s: morning fix pass MISSING #1. FDA recall "
                  "announcement read on the FDA's own page, which gives "
                  "the announcement and publish date 15 October 2026, the "
                  "firm 'Family Farmstead Dairy, LLC, of Worcester, NY', "
                  "'Family Farmstead Dairy Raw Mild Cheddar Cheese' with "
                  "lot code and best-by 7/22/27, the hazard 'Shiga "
                  "toxin-producing E. coli O157:H7', distribution to "
                  "grocers in New York and New Jersey, and 'No illnesses "
                  "or allergic reactions have been reported to date'. The "
                  "FDA recalls listing was also read this pass and shows "
                  "this as the only food recall dated 10/15/2026. No pack "
                  "size is published in the announcement, so none is "
                  "recorded. %s]" % (TODAY, _SWEPT)),
    },
    {
        "Date": "2026-10-15",
        "Source": "CAA (JP)",
        "Company": "Uohan",
        "Brand": "Unbranded",
        "Product": "Dried whitebait (shirasu-boshi)",
        "Pathogen": "Foreign material (unidentified)",
        "Reason": ("Foreign matter found in the product; because the "
                   "material could not be identified, the possibility that "
                   "it is pufferfish (fugu) cannot be ruled out."),
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": CAA % "00000035973",
        "Notes": ("[manual %s: morning fix pass MISSING #2. CAA food "
                  "recall index and this notice's own detail page read "
                  "this pass. Index 掲載日 2026/10/15; detail page "
                  "回収理由 「異物混入（特定できないためふぐの可能性を否定"
                  "できない）」, 商品名 「しらす干し」, 対応開始日 "
                  "2026年10月15日. The detail page carries no 事業者名 "
                  "field and the contact field is blank; the index names "
                  "the business 「魚はん（店舗）」, romanised Uohan. "
                  "Distinct from CAA rcl 00000035897 (Kintetsu Department "
                  "Store, Kusatsu, 2026-09-30) and from the 2026-10-05 "
                  "Komaki/Aichi news item held in Weekly_Rejected for "
                  "having no authority URL: different notice id, different "
                  "business, different response date. "
                  "[original company: 魚はん（店舗）] "
                  "[original product: しらす干し] "
                  "[original Reason (ja): 「異物混入（特定できないため"
                  "ふぐの可能性を否定できない）」] %s]" % (TODAY, _SWEPT)),
    },
    {
        "Date": "2026-10-15",
        "Source": "CAA (JP)",
        "Company": "Dauni",
        "Brand": "Unbranded",
        "Product": "Mirin madeleine",
        "Pathogen": "Mold",
        "Reason": "Contamination by mould.",
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": CAA % "00000035972",
        "Notes": ("[manual %s: morning fix pass MISSING #3. CAA food "
                  "recall index and this notice's own detail page read "
                  "this pass. Index 掲載日 2026/10/15; detail page "
                  "回収理由 「カビによる汚染」, 商品名 「味淋まどれーぬ」, "
                  "対応開始日 2026年10月11日. The detail page has no "
                  "事業者名 field; the index names the business 「ダウニー」 "
                  "(Hepburn romanisation Dauni — the firm publishes no "
                  "Latin-script name on the notice) and records sale at "
                  "愛知県美術館 and T-FACE A館. "
                  "[original company: ダウニー] "
                  "[original product: 味淋まどれーぬ] "
                  "[original Reason (ja): 「カビによる汚染」] %s]"
                  % (TODAY, _SWEPT)),
    },
    {
        "Date": "2026-10-16",
        "Source": "CAA (JP)",
        "Company": "Koun",
        "Brand": "Unbranded",
        "Product": ("Okayama sweet-chestnut cheesecake, small size; "
                    "Okayama sweet-chestnut cheesecake, full size; "
                    "cheesecake variety set, small size, 4 pieces; "
                    "cheesecake variety set, small size, 6 pieces"),
        "Pathogen": "Foreign material (stone-like hard object)",
        "Reason": ("Foreign matter found in the products — a hard, "
                   "stone-like foreign object."),
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": CAA % "00000035979",
        "Notes": ("[manual %s: morning fix pass MISSING #4. CAA food "
                  "recall index and this notice's own detail page read "
                  "this pass. Index 掲載日 2026/10/16; detail page "
                  "回収理由 「異物（石のような硬い異物）混入」, four "
                  "商品名 「岡山甘栗のチーズケーキ 小サイズ」, "
                  "「岡山甘栗のチーズケーキ フルサイズ」, 「チーズケーキ "
                  "バラエティ 小サイズ4個セット」, 「チーズケーキ "
                  "バラエティ 小サイズ6個セット」, 対応開始日 "
                  "2026年10月10日. The detail page has no 事業者名 field; "
                  "the index names the business 「行雲」 (Hepburn "
                  "romanisation Koun). "
                  "[original company: 行雲] "
                  "[original product: 岡山甘栗のチーズケーキ 小サイズ; "
                  "岡山甘栗のチーズケーキ フルサイズ; チーズケーキ "
                  "バラエティ 小サイズ4個セット; チーズケーキ バラエティ "
                  "小サイズ6個セット] "
                  "[original Reason (ja): 「異物（石のような硬い異物）"
                  "混入」] %s]" % (TODAY, _SWEPT)),
    },
]


def _fill_existing_pending(rows) -> list:
    """A missed recall may already be in Pending under the same URL with a
    collector's junk fields. Correct such a row IN PLACE from the authority
    page; only rows absent from every sheet go to add_manual_row (which
    would otherwise skip them as duplicate URLs and leave the junk held)."""
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Pending"]
    col = {h: i for i, h in enumerate(_hdr(ws))}
    left, changed = [], 0
    for rec in rows:
        hit = None
        for row in ws.iter_rows(min_row=2):
            if str(row[col["URL"]].value or "").strip().rstrip("/").lower() \
                    == rec["URL"].rstrip("/").lower():
                hit = row
                break
        if hit is None:
            left.append(rec)
            continue
        old = {k: hit[col[k]].value
               for k in ("Source", "Company", "Brand", "Product")
               if k in col}
        for k, v in rec.items():
            if k in col and k != "Notes":
                hit[col[k]].value = v
        hit[col["Notes"]].value = (
            rec["Notes"] + " [Pending row corrected in place; collector "
            "values were " + json.dumps(old, ensure_ascii=False) + "]").strip()
        changed += 1
        print(f"    Pending: {rec['Source']} {rec['URL']} corrected in place")
    if changed:
        wb.save(XLSX)
    wb.close()
    return left


def add_missed_recalls() -> int:
    rows = _fill_existing_pending(MISSED)
    done = len(MISSED) - len(rows)
    if not rows:
        return done
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=2)
        path = fh.name
    out = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), path],
        cwd=str(ROOT), capture_output=True, text=True)
    print((out.stdout or "").rstrip())
    if out.returncode != 0:
        print("    add_manual_row FAILED:", (out.stderr or "")[-900:])
        return done
    return done + len(rows)


def main() -> int:
    import openpyxl
    if not XLSX.exists():
        print("no workbook at", XLSX)
        return 1

    wb = openpyxl.load_workbook(XLSX)
    before = wb["Recalls"].max_row - 1
    before_pending = wb["Pending"].max_row - 1

    print("FAULT A — six RappelConso rows published in French")
    n_a = translate_french_rows(wb)
    print(f"  {n_a} row(s) translated\n")

    print("FAULT B — four recalls both published and rejected "
          "(seven more already carry a verdict and are left alone)")
    n_b = supersede_contradicting_archive_rows(wb)
    print(f"  {n_b} archive row(s) marked SUPERSEDED\n")

    print("FAULT C — the CFIA Class-1 Listeria cheese recall stuck in "
          "Pending on an empty Company")
    n_c = unblock_cfia_cheese_row(wb)
    print(f"  {n_c} row(s) unblocked\n")

    if n_a or n_b or n_c:
        wb.save(XLSX)
    wb.close()

    print("FAULT D — four recalls the pipeline missed")
    n_d = add_missed_recalls()
    print(f"  {n_d} row(s) queued into Pending\n")

    total = n_a + n_b + n_c + n_d
    if not total:
        print("nothing to do")
        print("ROWS_REMOVED=0")
        return 0

    wb2 = openpyxl.load_workbook(XLSX, read_only=True)
    after = wb2["Recalls"].max_row - 1
    after_pending = wb2["Pending"].max_row - 1
    wb2.close()
    print(f"Recalls {before} -> {after}")
    print(f"Pending {before_pending} -> {after_pending}")

    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")

    print("ROWS_REMOVED=0")      # nothing is removed from Recalls here
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
