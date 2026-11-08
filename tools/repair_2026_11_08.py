# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-08.

Base: main at 009fe0ee ("Italian gap finder: 2026-11-08 auto-update").

THE FULL SUITE WAS GREEN ON MAIN at the base sha — 5195 passed, 83 skipped,
3 xfailed. Nothing here is a red-test repair. What this pass found instead is
a recall that was about to be published for the second time, four Pending rows
blocked on a field the regulator's own page prints, one French sentence live on
a published row, and six archive rows that have been contradicting the register
since August without any test being able to see them.

Every value set below was read this morning on the regulator's own page, or is
a faithful translation of text already on the row it is written to. No row
leaves Recalls, so the commit message carries no `remove-rows`.


A. THE ROW THAT WAS ABOUT TO BE PUBLISHED TWICE
===============================================
``pipeline.promote_gate_passing`` reported exactly one Pending row passing
every publish-gate rule this morning:

    Date      2026-08-09
    Source    FSA (UK)
    Company   Greencore
    Product   three pasta products
    Pathogen  Listeria monocytogenes
    Tier      1
    URL       alerts.food.gov.uk/article/6816/Greencore-recalls-three-pasta-
              products-because-of-possible-contamination-with-Listeria-
              monocytogenes

Recalls has carried that recall since 2026-08-07:

    Date      2026-08-07
    Company   Greencore
    Product   Asda Cheese & Tomato Pasta Salad 300 g (5063089221990, use by
              10 August 2026); Asda Nacho Cheese Pasta Salad 500 g
              (5063717198311, use by 09 August 2026); Morrisons Chicken and
              Bacon Pasta 300 g (5010251790587, use by 07 August 2026)
    URL       alerts.food.gov.uk/news-alerts/alert/fsa-prin-38-2026

READ THIS MORNING on the FSA's own linked-data record,
https://data.food.gov.uk/food-alerts/id/FSA-PRIN-38-2026: notation
"FSA-PRIN-38-2026", status Published, created 2026-08-07, modified
2026-08-08T06:49:21.626Z, recalling business "Greencore", hazard "possible
contamination with Listeria monocytogenes", and those same three products with
those same three product codes and use-by dates. Its title is

    "Greencore recalls three pasta products because of possible contamination
     with Listeria monocytogenes"

which is the article slug above, word for word.

So the Pending row is a third address for one August alert, and promoting it
would have put a duplicate Tier-1 row into Recalls and moved a CLOSED month's
report with it. The live copy is removed from Pending here. It is not deleted
from the register: Rejected and Weekly_Rejected both already hold it, and both
are annotated below.

Why it got this far: it was archived on 2026-08-09 by the confirmer with
"reviewer 2 approved but Date is empty", a repairable defect. The Date has
since been filled, so the re-promotion guard correctly let it back — and
nothing downstream could tell it was the same alert, because of B.

The ``/article/<n>/<slug>`` address carries no PRIN notation, so B cannot
reach it and a future rediscovery at that address would look new again. That
is stated in the test rather than guessed at from the slug.


B. ONE FSA ALERT, THREE ADDRESSES, THREE REVIEW VERDICTS
========================================================
This is the identity-split that ``merge_master._normalize_url_for_dedup``
already fixes for admin.ch (one Swiss federal release served per language and
per department host) and for AESAN (one Spanish alert at up to seven language
addresses). On the UK agency it was unfixed, and the workbook has been paying
for it since June.

The FSA serves one alert at three hosts:

    alerts.food.gov.uk/news-alerts/alert/fsa-prin-38-2026    the public page
    data.food.gov.uk/food-alerts/id/FSA-PRIN-38-2026         the API record
    food.gov.uk/news-alerts/alert/fsa-prin-28-2026           the bare host

The notation in the path is the identity — data.food.gov.uk returns it as the
record's own ``notation`` — and the host is not. MEASURED this morning by
pushing every food.gov.uk URL in all five sheets through the function and
grouping by notation: **8 of 28 notations carried more than one dedup key**,
and six of those were PUBLISHED under one host form while an independently-
reviewed copy of the same alert sat REJECTED under another:

    fsa-prin-28-2026  Traditional Cheese Company, French Brie 160 g, Listeria
                      monocytogenes, published 2026-06-19
    fsa-prin-38-2026  Greencore, three pasta products, Listeria
                      monocytogenes, published 2026-08-07
    fsa-prin-39-2026  A.Vogel Ltd, Rapunzel bioSnacky red clover sprouting
                      seeds 30 g, STEC, published 2026-08-10
    fsa-prin-40-2026  Summit Foods, Snacksters / East Street Deli wraps,
                      STEC, published 2026-08-21
    fsa-prin-41-2026  Tesco, Tesco Finest Mozzarella Pearls & Pesto Orzo
                      Salad, STEC, published 2026-08-25
    fsa-prin-42-2026  Macroom Buffalo Cheese Products, mozzarella 125 g and
                      bocconcini 160 g, Listeria monocytogenes, published
                      2026-08-28

``tests/test_a_recall_is_not_both_published_and_rejected.py`` exists precisely
to catch a published-and-rejected contradiction, and it compares by dedup key
— so it could not see a single one of these. The six have been invisible for
between two and five months.

The fix is in ``merge_master._normalize_url_for_dedup``, host-scoped, with
``tests/test_one_fsa_alert_is_one_dedup_key.py`` holding the shape: same
notation, one key; two notations, two keys. The ``-update-N`` suffix stays IN
the key on purpose — FSA republishes an amended alert at its own slug and this
register publishes those as separate rows (prin-02 plus update-1 and update-2,
prin-28 plus update-1, prin-47 plus update-1). Whether an update is a
duplicate of its parent or an extension of it is a real question and a URL
normalizer may not answer it alone. **That question is George's** and is named
in the verdict; nothing in this pass touches an update row.

With the keys collapsed, those six archive rows become visible to that test
and must each say which copy wins. Section C says it. Two of the six already
explained themselves in prose ("metadata_endpoint_not_a_notice", naming the
promoted notice) but carried no word the test recognises; four said nothing at
all, two of them with an entirely empty RejectReason.


C. THE ARCHIVE ROWS SAY WHICH COPY WINS
=======================================
Each annotation names the published twin by company, date and notation, all
three read off the workbook and the FSA record this morning. Nothing is
deleted — ``Rejected`` is a terminal, append-only archive and these rows are
the audit trail of their own refusals.


D. FOUR PENDING ROWS BLOCKED ON A FIELD THE PAGE PRINTS
=======================================================
``promote_gate_passing`` held 14 rows this morning. Four were blocked on a
value that is published on the regulator's own page, and all four pages were
read today.

D.1 — NVWA (NL), 2026-11-06, blocked "Company is empty".
Read on https://www.nvwa.nl/actueel/nieuws/2026/11/06/veiligheidswaarschuwing-
de-oude-streek-machedoux-en-macandre-geitenkaas, byline "Nieuwsbericht
06-11-2026 | 14:32":

    "Odin waarschuwt voor De Oude Streek Machedoux en Macandré geitenkaas."
    "In deze producten is Listeria aangetroffen."
    "Om welke producten gaat het?"
      Machedoux geitenkaas — Artikelnummer 0825 — Streepjescode
      8714108860108 — Ten minste houdbaar tot (THT) 21-11-2026
      Macandré geitenkaas — Artikelnummer 2020 — Streepjescode
      8714108005028 — Ten minste houdbaar tot (THT) 21-11-2026

Odin is the party issuing the warning, so Odin is the recalling firm; the page
names no manufacturer, so none is written. "De Oude Streek" appears in the
title as the product line and goes to Brand. The product description is
translated and the Dutch kept in Notes.

PATHOGEN IS NARROWED, not filled: the row arrived carrying "Listeria
monocytogenes" and the page says only "Listeria". The species was not on the
page, so it comes off the row. "Listeria" is a Tier-1 term this register
already carries, so nothing is lost but the invented precision.

D.2 — FDA, blocked "Pathogen is empty", Date empty, Notes "FDA HTML fallback
— claude-check needs to enrich Date+Pathogen".
Read on https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/
snack-right-super-foods-inc-dba-brads-raw-chips-recalls-brads-crunchy-kale-
veggie-chips-due: FDA publish date November 03, 2026 (company announcement
November 02, 2026); "Snack Right Super Foods, Inc., doing business as Brad's
Raw Chips"; "distributed under the brand name Brad's Plant Based"; the recall
is "Due to Potential Thallium Contamination" and the reason field reads
"Product may contain thallium"; four products with UPCs; distributed
nationwide through KeHE and UNFI, about 25,000 bags; the California Department
of Public Health acted in May 2026 "after it investigated an inquiry from one
individual with elevated thallium levels".

Thallium is a heavy metal and heavy metals are in AFTS scope. IT WAS A
VOCABULARY GAP AT TWO GATES AT ONCE, and the row found both:

  * ``tools/alert_vocab.py`` "Heavy metals" held cadmium, lead, mercury,
    arsenic and molybdenum. No alert could have reached a thallium recall.
  * ``pipeline/_publish_gate.classify_hazard`` returned the EMPTY SET for
    "Thallium", so ``is_in_afts_scope`` said False and
    ``tests/test_curator_scope_2026_09_09.py`` went red the moment the row
    was promoted — the curator refused the hazard of a published row, which
    also means it could not have corrected a typo on it.

Both are closed in this commit, with ``tools/gen_alert_vocab.py --write`` run
for the first. Sweeping metal names through ``is_in_afts_scope`` the same
morning showed the shape is wider than one metal: the publish gate's chemical
class knew four metals while ``merge_master``'s own in-scope token list has
known five since it was written — it has always had "nickel" — so the two
gates have disagreed about nickel for as long as both have existed. Thallium
and nickel are added, the two with evidence; thallium bare, nickel
space-padded like cadmium. "tin", "antimony", "chromium", "copper",
"aluminium", "barium" and "cobalt" are all equally out of scope as a bare
Pathogen today and are NOT added, because no row has named one — and "tin" in
particular must never become a bare token there, since that class is matched
against Reason and "180 g tin" is all over the register.
``tests/test_a_heavy_metal_is_in_scope_at_every_gate.py`` holds both halves.

Pathogen is written as "Thallium (heavy metal)", which is the convention
Recalls already uses for "Cadmium (heavy metal)" and "Lead (heavy metal)".

The row's Reason also arrived truncated mid-sentence at the word "Due", which
is the headline cut at the slug's end; it is replaced by the hazard the page
states.

D.3 — AESAN (ES), blocked on a Product that is a search-engine snippet
("3 days ago · La Agencia Española de Seguridad Alimentaria y Nutrición
(AESAN) ha tenido conocimiento …"), and with an empty Date.
Read on https://www.aesan.gob.es/alertas/2026_80, dated 04/11/2026:

    title           "Alerta por presencia de Listeria monocytogenes en jamón
                     cocido procedente de España (Ref. ES2026/665)"
    Nombre de marca "URKABE"
    Nombre del producto "Jamón cocido"
    Aspecto         "Envase de plástico"
    Número de lote  "61027"
    Fecha de caducidad "26/11/2026"
    Peso de unidad  "175 g"
    Temperatura     "Refrigerado"
    distribution    "las comunidades autónomas de Castilla y León, Madrid,
                     Navarra y País Vasco", redistribution "no es
                     descartable"

THE ALERT NAMES NO FIRM. It has no empresa or operador field and no company
name appears anywhere on the page, so none is invented. Company takes the
marker this register already uses on exactly this shape — Recalls carries
"(not specified in AESAN alert 2026_76)", "…2026_75", "…2026_74" and
"…2026_72" on four AESAN rows published in October, each with the Marca in
Brand — and the publish gate, which requires a non-empty Company, is
satisfied by a value that says plainly that the regulator gave none.

D.4 — RappelConso (FR) fiche 23947, blocked "Company is empty", Brand
"Unbranded".
Read on https://rappel.conso.gouv.fr/fiche-rappel/23947/interne, "Fiche réf.
2026-10-0273 • Version 1":

    Nom de la marque du produit  "Sans marque"
    Noms des modèles ou références  "EMINCES DE DINDE MARINES A
                                     L'ORIENTALE vendus au rayon
                                     traditionnel"
    Motif du rappel  "Présence de Salmonelles"
    Risques encourus  "Salmonella spp (agent responsable de la salmonellose)"
    Origine de la fiche  "CARREFOUR FRANCE CARREFOUR"
    Distributeurs  "Carrefour Contact - Saint Julien en Born uniquement"
    Zone  "Départements : LANDES (40)"
    Nature juridique  "Volontaire (sans arrêté préfectoral)"

Company becomes the fiche's own originator, which is how this register
already files an unbranded service-counter recall (Recalls carries
"Intermarché Fos sur Mer", "Magasins U", "Carrefour" and "Grand Frais" on
exactly this shape). The fiche's GTIN row reads lot "Vendus entre le
12/10/2026 et le 16/10/2016" against a commercialisation field of "Du
12/10/2026 au 16/10/2026" — the page contradicts itself on that year, so
neither date is written into Product.


E. ONE FRENCH SENTENCE ON A PUBLISHED ROW
=========================================
Recalls, promoted 2026-11-07, fiche 24040:

    Company  Cooperl Arc Atlantique
    Product  Label Rouge eggs in boxes of 6 and 12, and free-range eggs sold
             loose in trays of 30
    Reason   "Présence Salmonella enteritidis"

Product was translated by yesterday's pass and Reason was not. The language
detector needs two non-English word hits and "Présence" is one word; this is
the fifth consecutive morning on which the by-hand read of every Product and
Reason published in the last 26 h found text the stamp did not mark. Reason
becomes "Presence of Salmonella Enteritidis" and the French is kept in Notes.

Every other field published in the last 26 h — 21 rows, read by hand this
morning — is already in English.


F. TWO VERIFIED MISSES, QUEUED THROUGH THE REAL PATH
====================================================
Neither is a 24-hour find: NOTHING was published anywhere on 2026-11-07 or
2026-11-08 (see the verdict). Both are 2026-11-06 items inside the window
yesterday's Daily Global Recall Search reported as covered
("window: 2026-11-06 → 2026-10-31"), and neither is in any sheet of the
register.

F.1 — CFS (HK), 2026-11-06. Read on the CFS's own press release,
https://www.cfs.gov.hk/english/press/20261106_12720.html: "Imported frozen
chicken leg sample detected with veterinary drug residue exceeding legal
limit", "Ends/Friday, November 6, 2026"; importer "Willy More Development
Limited"; doxycycline at "180 parts per billion (ppb)" against a legal limit
of "100 ppb" ("100 micrograms per kilogram" of poultry muscle); place of
origin China; "15 kilograms per carton"; manufacture date July 22, 2026;
best-before January 21, 2028; the importer "has initiated a recall of the
affected batch" under CFS instruction. The CFS prints the product name and
brand as "(Not available in English)", so Brand stays empty and that is said
in Notes rather than guessed at.

A veterinary drug residue over a legal limit is a chemical contaminant and in
AFTS scope. "veterinary drug" — the CFS's own wording — was not an alert term
("veterinary medicine" and "veterinary chemical" both were); it and
"doxycycline" are added in this commit.

CFS (HK) is NOT stale by the register's own rule: newest row 2026-10-21, 3
rows in 30 days, inside the 30-day gap this source is allowed, and today's
scrape-fleet commit exists. The page did not block this pass either — it was
read in full. So this is a plain miss by ``scrapers/asia/cfs_hk.py`` on a
press release the month page lists, and that is named in the verdict rather
than fixed here: the scraper is not what this pass measured.

F.2 — BLV (CH), 2026-11-06. The Swiss copy of the Alnatura salad-seed
Salmonella recall, which the register already carries for Croatia (HAH,
2026-11-06), France (fiche 24032, 2026-11-06), Austria (AGES, 2026-11-05) and
Germany (BVL dennree, 2026-11-05) — and not for Switzerland.

Read on the BLV's own recall list,
https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/rueckrufe-
und-oeffentliche-warnungen.html, which prints the entry as:

    "Migros ruft das Produkt «Alnatura Bio Salatkerne Mix» wegen Nachweis von
     Salmonellen zurück — 06.11.2026"

linking to https://www.recallswiss.admin.ch/customer-access/#Recalls/1185.
That viewer is a JavaScript shell — 704 characters of HTML with no recall text
in it — so no pack size and no lot number could be read and none is written.
The recallswiss address is still the row's URL: it is the regulator's own
per-recall address and the twenty-plus BLV rows already in Recalls all cite
it in that form.

Weekly_Rejected already holds two news-mirror copies of this recall (an SRF
piece and a second German-language report), both refused for having no
official regulator URL. They are left exactly as they are — the refusals were
correct on their own terms — and the official row now carries the recall.


H. A VERDICT THAT NAMED NO DEFECT, BARRING A REAL RECALL
========================================================
With D.4 repaired, ``promote_approved``'s re-promotion guard still refused
fiche 23947:

    re-promotion BLOCKED https://rappel.conso.gouv.fr/fiche-rappel/23947/
    interne — already in Weekly_Rejected (unknown: Arrived already marked
    rejected; confirmer did not re-review)

That verdict is not a finding about the recall. It says nobody looked. And it
had the same effect as a real one, which is the shape
``tests/test_a_superseded_archive_row_stops_blocking.py`` was written for in
October after a misclassified Mattilsynet kale recall made its own mistake
unappealable.

The recall is real and is in no Recalls row: RappelConso fiche 23947,
Salmonella in marinated turkey strips sold at the Carrefour Contact counter in
Saint-Julien-en-Born, published 02/11/2026, read in full this morning (D.4).
So the archive row is marked SUPERSEDED — the marker this repo already uses to
retire a verdict, which both ``_is_terminal_rejection`` and the re-promotion
guard read — rather than forced past with ``add_manual_row
--override-terminal``. The refusal is retired by evidence, in the sheet, where
an auditor can read why.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
ROWS_JSON = ROOT / "tools" / "manual_rows_2026_11_08.json"

# ---------------------------------------------------------------------------
# A. The duplicate FSA alert leaves the live sheets
# ---------------------------------------------------------------------------
GREENCORE_ARTICLE = (
    "https://alerts.food.gov.uk/article/6816/Greencore-recalls-three-pasta-"
    "products-because-of-possible-contamination-with-Listeria-monocytogenes"
)

# ---------------------------------------------------------------------------
# D. Pending: the field the regulator's own page prints
# ---------------------------------------------------------------------------
NVWA_URL = ("https://www.nvwa.nl/actueel/nieuws/2026/11/06/"
            "veiligheidswaarschuwing-de-oude-streek-machedoux-en-macandre-"
            "geitenkaas")
BRADS_URL = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-"
             "alerts/snack-right-super-foods-inc-dba-brads-raw-chips-recalls-"
             "brads-crunchy-kale-veggie-chips-due")
AESAN_URL = "https://www.aesan.gob.es/alertas/2026_80"
FICHE_23947 = "https://rappel.conso.gouv.fr/fiche-rappel/23947/interne"

PENDING_FIXES = [
    {
        "url": NVWA_URL,
        "set": {
            "Company": "Odin",
            "Brand": "De Oude Streek",
            "Product": ("Machedoux goat's cheese (article number 0825, "
                        "barcode 8714108860108, best before 21-11-2026); "
                        "Macandré goat's cheese (article number 2020, "
                        "barcode 8714108005028, best before 21-11-2026)"),
            "Pathogen": "Listeria",
            "Reason": "Listeria found in the products",
            "Date": "2026-11-06",
        },
        "note": (
            "[morning-fix 2026-11-08: Company was empty and the publish gate "
            "held the row on it. Read today on the NVWA's own news item "
            + NVWA_URL + ", byline \"Nieuwsbericht 06-11-2026 | 14:32\": "
            "\"Odin waarschuwt voor De Oude Streek Machedoux en Macandré "
            "geitenkaas.\" Odin is the party issuing the warning, so Odin is "
            "the recalling firm; the page names NO manufacturer and none is "
            "written. \"De Oude Streek\" appears in the title as the product "
            "line and goes to Brand. Products under the heading \"Om welke "
            "producten gaat het?\": Machedoux geitenkaas, Artikelnummer "
            "0825, Streepjescode 8714108860108, THT 21-11-2026; Macandré "
            "geitenkaas, Artikelnummer 2020, Streepjescode 8714108005028, "
            "THT 21-11-2026. No pack size and no partijnummer are printed, "
            "so none is written. PATHOGEN NARROWED, NOT FILLED: the row "
            "arrived carrying \"Listeria monocytogenes\" and the page says "
            "only \"In deze producten is Listeria aangetroffen\" — the "
            "species is on no page this pass read, so it comes off the row.] "
            "[original product: Machedoux geitenkaas; Macandré geitenkaas] "
            "[original company: Odin (as printed)] "
            "[original Reason (nl): \"In deze producten is Listeria "
            "aangetroffen.\"]"
        ),
    },
    {
        "url": BRADS_URL,
        "set": {
            "Date": "2026-11-03",
            "Company": "Snack Right Super Foods, Inc. (dba Brad's Raw Chips)",
            "Brand": "Brad's Plant Based",
            "Product": ("Brad's Crunchy Kale Original, 2 oz (57 g), UPC "
                        "813104021679; Brad's Crunchy Kale Cheeze It Up, "
                        "2 oz (57 g), UPC 813104021877; Brad's Crunchy Kale "
                        "Radical Ranch, 2 oz (57 g), UPC 813104020726; "
                        "Brad's Veggie Chips Kale, 3 oz (85 g), UPC "
                        "854615002054"),
            "Pathogen": "Thallium (heavy metal)",
            "Reason": ("Potential thallium contamination — the product may "
                       "contain thallium"),
            "Country": "United States",
        },
        "note": (
            "[morning-fix 2026-11-08: the row arrived from the FDA HTML "
            "fallback with Date and Pathogen empty, its Notes asking for "
            "both, and a Reason truncated mid-sentence at the word \"Due\" "
            "(the headline cut at the end of the slug). Both fields read "
            "today on the FDA's own press release, " + BRADS_URL + ": FDA "
            "publish date \"November 03, 2026\" (company announcement "
            "November 02, 2026); the recall is \"Due to Potential Thallium "
            "Contamination\" and the reason field reads \"Product may "
            "contain thallium\"; firm \"Snack Right Super Foods, Inc., doing "
            "business as Brad's Raw Chips\"; \"distributed under the brand "
            "name Brad's Plant Based\"; four products with the UPCs written "
            "into Product; lots 33525-1/-2/-3, 34525-1, 35125-2, 35025-3, "
            "29425-1, 33225-3, 33325-2, 33425-1 with best-by dates from "
            "21 December 2026 to 17 February 2027; distributed nationwide "
            "through KeHE Distributors and UNFI, about 25,000 bags (2,097 "
            "cases); the California Department of Public Health acted in May "
            "2026 \"after it investigated an inquiry from one individual "
            "with elevated thallium levels\". The page reports no illness "
            "count and none is written. Thallium is a heavy metal and heavy "
            "metals are in AFTS scope; it was reachable by no alert term, so "
            "\"thallium\" is added to tools/alert_vocab.py in this same "
            "commit.]"
        ),
    },
    {
        "url": AESAN_URL,
        "set": {
            "Date": "2026-11-04",
            "Company": "(not specified in AESAN alert 2026_80)",
            "Brand": "URKABE",
            "Product": ("Cooked ham (jamón cocido), 175 g, plastic pack, lot "
                        "61027, expiry 26/11/2026, refrigerated"),
            "Pathogen": "Listeria monocytogenes",
            "Reason": ("Presence of Listeria monocytogenes in cooked ham "
                       "from Spain (SCIRI reference ES2026/665)"),
            "Country": "Spain",
        },
        "note": (
            "[morning-fix 2026-11-08: Product was a search-engine snippet "
            "(\"3 days ago · La Agencia Española de Seguridad Alimentaria y "
            "Nutrición (AESAN) ha tenido conocimiento …\") and the publish "
            "gate held the row on it; Date was empty. Both read today on the "
            "alert itself, " + AESAN_URL + ", dated 04/11/2026: title "
            "\"Alerta por presencia de Listeria monocytogenes en jamón "
            "cocido procedente de España (Ref. ES2026/665)\"; \"Nombre de "
            "marca: URKABE\"; \"Nombre del producto: Jamón cocido\"; "
            "\"Aspecto del producto: Envase de plástico\"; \"Número de lote: "
            "61027\"; \"Fecha de caducidad: 26/11/2026\"; \"Peso de unidad: "
            "175 g\"; \"Temperatura: Refrigerado\"; initial distribution to "
            "\"las comunidades autónomas de Castilla y León, Madrid, Navarra "
            "y País Vasco\" with redistribution \"no es descartable\"; the "
            "País Vasco authorities notified AESAN through SCIRI. THE ALERT "
            "NAMES NO FIRM: it has no empresa or operador field and no "
            "company name appears anywhere on the page, so none is "
            "invented. Company carries the marker this register already uses "
            "on this shape — Recalls holds \"(not specified in AESAN alert "
            "2026_76)\", \"…2026_75\", \"…2026_74\" and \"…2026_72\" on four "
            "AESAN rows published in October, each with the Marca in Brand.] "
            "[original product: Jamón cocido] "
            "[original Reason (es): \"Alerta por presencia de Listeria "
            "monocytogenes en jamón cocido procedente de España (Ref. "
            "ES2026/665)\"]"
        ),
    },
    {
        "url": FICHE_23947,
        "set": {
            "Company": "Carrefour France",
            "Product": ("Marinated oriental-style turkey strips (éminces de "
                        "dinde marinés à l'orientale), sold at the service "
                        "counter"),
            "Reason": "Presence of Salmonella",
            "Pathogen": "Salmonella",
        },
        "note": (
            "[morning-fix 2026-11-08: Company was empty with Brand "
            "\"Unbranded\" and the publish gate held the row on it. Read "
            "today on the fiche itself, " + FICHE_23947 + ", \"Fiche réf. "
            "2026-10-0273 • Version 1\": Nom de la marque du produit \"Sans "
            "marque\"; Noms des modèles ou références \"EMINCES DE DINDE "
            "MARINES A L'ORIENTALE vendus au rayon traditionnel\"; Motif du "
            "rappel \"Présence de Salmonelles\"; Risques encourus "
            "\"Salmonella spp (agent responsable de la salmonellose)\"; "
            "Origine de la fiche \"CARREFOUR FRANCE CARREFOUR\"; "
            "Distributeurs \"Carrefour Contact - Saint Julien en Born "
            "uniquement\"; Zone \"Départements : LANDES (40)\"; Nature "
            "juridique \"Volontaire (sans arrêté préfectoral)\"; "
            "Conditionnements \"Non conditionné/Vrac\". Company is the "
            "fiche's own originator, which is how this register already "
            "files an unbranded service-counter recall (Recalls carries "
            "\"Intermarché Fos sur Mer\", \"Magasins U\", \"Carrefour\" and "
            "\"Grand Frais\" on exactly this shape). The fiche's GTIN row "
            "gives lot \"Vendus entre le 12/10/2026 et le 16/10/2016\" "
            "against a commercialisation field of \"Du 12/10/2026 au "
            "16/10/2026\" — the page contradicts itself on that year, so "
            "neither date is written into Product.] "
            "[original product: EMINCES DE DINDE MARINES A L'ORIENTALE "
            "vendus au rayon traditionnel] "
            "[original Reason (fr): \"Présence de Salmonelles\"]"
        ),
        "drop_stamps": ["[needs cleanup: Product not in English]"],
    },
    # G — the two French Reasons the writer warns about on every save.
    {
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24016/interne",
        "set": {
            "Product": "Dried morel mushrooms",
            "Reason": ("Probable contamination of a lot of morels by a toxin "
                       "liable to cause food poisoning"),
        },
        "note": (
            "[morning-fix 2026-11-08: Reason was French and carried a live "
            "\"[needs cleanup: Reason not in English]\" stamp; "
            "merge_master._write_sheet warns about this row on every save. "
            "Translated faithfully from the row's own text, which is the "
            "fiche's Motif. Product translated from the row's own text too. "
            "PATHOGEN STAYS EMPTY, which is why the row is still blocked at "
            "the publish gate and why that block is correct: the fiche says "
            "only \"une toxine\" and names no toxin, so there is nothing to "
            "write. Reviewer 1 refused the sibling fiche 24003 on 2026-11-03 "
            "with \"The hazard is not a microbial pathogen, but a risk of…\" "
            "and that reading is accepted here — the row is translated so it "
            "stops tripping the writer, not so it can publish.] "
            "[original product: champignons morilles sechees] "
            "[original Reason (fr): \"Probable contamination d'un lot de "
            "morilles par une toxine susceptible d'être à l'origine "
            "d'intoxication alimentaire\"]"
        ),
        "drop_stamps": ["[needs cleanup: Reason not in English]"],
    },
    {
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24012/interne",
        "set": {
            "Reason": ("Probable contamination of a lot of morels by a toxin "
                       "liable to cause food poisoning"),
        },
        "note": (
            "[morning-fix 2026-11-08: Reason translated under the same rule "
            "as fiche 24016 above. Product is left exactly as it is — it "
            "holds \"3760201609743; tous les lots; date limite de "
            "consommation; 2029-12-01\", which the scraper's own flag says "
            "it took from the fiche's model/reference field because that "
            "field held only lot codes. A GTIN, the French for \"all lots\" "
            "and a date are not a product description, and inventing one "
            "from the fiche title is exactly the fabrication this pass is "
            "not permitted. PATHOGEN STAYS EMPTY for the reason given on "
            "24016.] "
            "[original Reason (fr): \"Probable contamination d'un lot de "
            "morilles par une toxine susceptible d'être à l'origine "
            "d'intoxication alimentaire\"]"
        ),
        "drop_stamps": ["[needs cleanup: Reason not in English]"],
    },
]

# ---------------------------------------------------------------------------
# E. The French sentence on a published row
# ---------------------------------------------------------------------------
FICHE_24040 = "https://rappel.conso.gouv.fr/fiche-rappel/24040/Interne"

RECALLS_FIXES = [
    {
        "url": FICHE_24040,
        "set": {"Reason": "Presence of Salmonella Enteritidis"},
        "note": (
            "[morning-fix 2026-11-08: Reason was the French \"Présence "
            "Salmonella enteritidis\", live on a published row. Product was "
            "translated by yesterday's pass and Reason was not — the "
            "language detector needs two non-English word hits and "
            "\"Présence\" is one word. Translated from the row's own text; "
            "no fact added.] "
            "[original Reason (fr): \"Présence Salmonella enteritidis\"]"
        ),
    },
]

# ---------------------------------------------------------------------------
# C. Archive rows that must say which copy wins
# ---------------------------------------------------------------------------
def _dup(notation, company, product, date, stored_is_api=True):
    where = ("the FSA linked-data record for " if stored_is_api
             else "a second host form of ")
    return (
        "[DUPLICATE 2026-11-08 by the morning fix pass — the PUBLISHED copy "
        "wins. This row's URL is " + where + notation.upper() + ", and "
        "Recalls carries that alert as " + date + ", " + company + ", \"" +
        product + "\", cited at alerts.food.gov.uk/news-alerts/alert/" +
        notation + ". The notation in the path is the alert's identity — "
        "data.food.gov.uk returns it as the record's own `notation` — and "
        "the host is not, so these were always one alert at two addresses. "
        "Visible to tests/test_a_recall_is_not_both_published_and_rejected "
        "only from today, when merge_master._normalize_url_for_dedup "
        "stopped keying FSA alerts on the host; it had been contradicting "
        "the register silently since the row was archived. Kept as the "
        "audit trail of the original refusal.] "
    )


ARCHIVE_NOTES = [
    {"sheet": "Rejected",
     "url": "http://data.food.gov.uk/food-alerts/id/FSA-PRIN-39-2026",
     "column": "RejectReason", "reviewed": "DUPLICATE",
     "prefix": _dup("fsa-prin-39-2026", "A.Vogel Ltd",
                    "Rapunzel bioSnacky Das Original Red Clover — Seeds for "
                    "Sprouts and Seedlings, 30 g", "2026-08-10")},
    {"sheet": "Rejected",
     "url": "http://data.food.gov.uk/food-alerts/id/FSA-PRIN-38-2026",
     "column": "RejectReason", "reviewed": "DUPLICATE",
     "prefix": _dup("fsa-prin-38-2026", "Greencore",
                    "Asda Cheese & Tomato Pasta Salad 300 g; Asda Nacho "
                    "Cheese Pasta Salad 500 g; Morrisons Chicken and Bacon "
                    "Pasta 300 g", "2026-08-07")},
    {"sheet": "Rejected",
     "url": "http://data.food.gov.uk/food-alerts/id/FSA-PRIN-40-2026",
     "column": "RejectReason", "reviewed": "DUPLICATE",
     "prefix": _dup("fsa-prin-40-2026", "Summit Foods",
                    "Snacksters Chicken Fajita Wrap; Snacksters Hoi Sin "
                    "Wrap; East Street Deli Sweet Chilli Chicken wrap",
                    "2026-08-21")},
    {"sheet": "Rejected",
     "url": "http://data.food.gov.uk/food-alerts/id/FSA-PRIN-41-2026",
     "column": "RejectReason", "reviewed": "DUPLICATE",
     "prefix": _dup("fsa-prin-41-2026", "Tesco",
                    "Tesco Finest Mozzarella Pearls & Pesto Orzo Salad",
                    "2026-08-25")},
    {"sheet": "Rejected",
     "url": "http://data.food.gov.uk/food-alerts/id/FSA-PRIN-42-2026",
     "column": "RejectReason", "reviewed": "DUPLICATE",
     "prefix": _dup("fsa-prin-42-2026", "Macroom Buffalo Cheese Products",
                    "Macroom Buffalo Cheese Mozzarella 125 g; Macroom "
                    "Buffalo Cheese Bocconcini 160 g", "2026-08-28")},
    {"sheet": "Rejected",
     "url": "https://www.food.gov.uk/news-alerts/alert/fsa-prin-28-2026",
     "column": "RejectReason", "reviewed": "DUPLICATE",
     "prefix": _dup("fsa-prin-28-2026", "Traditional Cheese Company",
                    "French Brie, 160g", "2026-06-19",
                    stored_is_api=False)},
    # A.3 — the article form of the same alert as prin-38. It carries no
    # notation, so the normalizer cannot reach it and the test does not see
    # it; the annotation is the honest record either way.
    {"sheet": "Rejected", "url": GREENCORE_ARTICLE,
     "column": "RejectReason", "reviewed": "DUPLICATE",
     "prefix": (
         "[DUPLICATE 2026-11-08 by the morning fix pass — the PUBLISHED copy "
         "wins. This is a third address for FSA-PRIN-38-2026: the FSA's own "
         "record at data.food.gov.uk/food-alerts/id/FSA-PRIN-38-2026, read "
         "2026-11-08, has notation \"FSA-PRIN-38-2026\", created 2026-08-07, "
         "recalling business \"Greencore\", hazard \"possible contamination "
         "with Listeria monocytogenes\", and a title identical word for word "
         "to this URL's slug. Recalls has carried that alert since "
         "2026-08-07. The live Pending copy of this row was passing every "
         "publish-gate rule this morning and was removed by "
         "tools/repair_2026_11_08.py; this archive row is kept as the audit "
         "trail. NOTE: the /article/<n>/<slug> form carries no PRIN "
         "notation, so the host-form collapse added to "
         "merge_master._normalize_url_for_dedup today CANNOT reach it and a "
         "rediscovery at this address would look new again.] ")},
]

WEEKLY_REJECTED_NOTES = [
    # H — the verdict that named no defect. See section H of the docstring.
    {"sheet": "Weekly_Rejected", "url": FICHE_23947,
     "column": "RejectionReason", "reviewed": "SUPERSEDED",
     "prefix": (
         "[SUPERSEDED 2026-11-08 by the morning fix pass — the defect this "
         "row was archived for has been repaired, and the verdict named no "
         "defect in the first place. It reads \"Arrived already marked "
         "rejected; confirmer did not re-review\": a statement that nobody "
         "looked, not a finding about the recall. It nevertheless barred "
         "re-entry, so promote_approved's re-promotion guard refused the "
         "repaired row with \"already in Weekly_Rejected\". The recall is "
         "real and is in no Recalls row: RappelConso fiche 23947, \"Fiche "
         "réf. 2026-10-0273 • Version 1\", Motif \"Présence de Salmonelles\", "
         "Risques \"Salmonella spp (agent responsable de la salmonellose)\", "
         "Origine de la fiche \"CARREFOUR FRANCE CARREFOUR\", Distributeurs "
         "\"Carrefour Contact - Saint Julien en Born uniquement\", read on "
         "https://rappel.conso.gouv.fr/fiche-rappel/23947/interne on "
         "2026-11-08. The Company the publish gate held the Pending row on "
         "was read off that same page. Kept as the audit trail of the "
         "original refusal; the repaired row wins.] ")},
    {"sheet": "Weekly_Rejected", "url": GREENCORE_ARTICLE,
     "column": "RejectionReason", "reviewed": "DUPLICATE",
     "prefix": (
         "[DUPLICATE 2026-11-08 by the morning fix pass — the PUBLISHED copy "
         "wins. FSA-PRIN-38-2026, Greencore, three pasta products, Listeria "
         "monocytogenes, is published in Recalls as 2026-08-07 at "
         "alerts.food.gov.uk/news-alerts/alert/fsa-prin-38-2026. This row "
         "was archived for \"reviewer 2 approved but Date is empty\", a "
         "repairable defect, so once the Date was filled the re-promotion "
         "guard let the Pending copy back to the front of the publish gate. "
         "The defect was repairable; the recall was already in the register. "
         "Verdict recorded so the next pass does not re-promote it.] ")},
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _head(ws):
    return [c.value for c in ws[1]]


def _rowdict(ws, head, r):
    return {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}


def _set(ws, head, r, field, value):
    ws.cell(r, head.index(field) + 1).value = value


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _find_rows(ws, url):
    head = _head(ws)
    col = head.index("URL") + 1
    target = _key(url)
    return [r for r in range(2, ws.max_row + 1)
            if _key(ws.cell(r, col).value) == target]


def _find_rows_exact(ws, url):
    """Archive rows are matched on the STORED string.

    Section B is a deliberate key collapse: matching by key here would also
    hit the published row and every other host form of the same alert.
    """
    head = _head(ws)
    col = head.index("URL") + 1
    target = str(url or "").strip().lower()
    return [r for r in range(2, ws.max_row + 1)
            if str(ws.cell(r, col).value or "").strip().lower() == target]


def _apply_notes(ws, head, r, row, note, drop_stamps):
    notes = str(row.get("Notes") or "").strip()
    if notes.lower() in ("none", "nan"):
        notes = ""
    for stamp in drop_stamps or ():
        notes = notes.replace(stamp, " ")
    notes = re.sub(r"[ \t]{2,}", " ", (notes + " " + note).strip())
    _set(ws, head, r, "Notes", notes)


def _fix_sheet(wb, sheet, specs, required=True) -> int:
    if sheet not in wb.sheetnames:
        return 0
    ws = wb[sheet]
    head = _head(ws)
    done = 0
    for spec in specs:
        hit = False
        for r in _find_rows(ws, spec["url"]):
            row = _rowdict(ws, head, r)
            for field, value in spec["set"].items():
                if field not in head:
                    continue
                _set(ws, head, r, field, value)
            _apply_notes(ws, head, r, row, spec["note"],
                         spec.get("drop_stamps"))
            done += 1
            hit = True
            print(f"  {sheet}:{r}: {str(row.get('Source'))[:18]!r} "
                  f"{spec['url'][-46:]}")
        if not hit and required:
            raise SystemExit(
                f"{sheet}: no row at {spec['url']} — main has moved under "
                f"this repair script; re-read the sheet before re-running")
    return done


def annotate_archives(wb, specs) -> int:
    """C. Each contradicting archive row gets the verdict its evidence says."""
    done = 0
    for spec in specs:
        sheet = spec["sheet"]
        if sheet not in wb.sheetnames:                  # pragma: no cover
            continue
        ws = wb[sheet]
        head = _head(ws)
        rows = _find_rows_exact(ws, spec["url"])
        if not rows:
            raise SystemExit(
                f"{sheet}: no archive row stored at exactly {spec['url']} — "
                f"main has moved under this repair script")
        col = spec["column"]
        if col not in head:                             # pragma: no cover
            raise SystemExit(f"{sheet} has no {col} column")
        for r in rows:
            row = _rowdict(ws, head, r)
            cur = str(row.get(col) or "").strip()
            if cur.lower() in ("none", "nan"):
                cur = ""
            if "DUPLICATE 2026-11-08" in cur:
                print(f"  {sheet}:{r}: already annotated, left alone")
                continue
            _set(ws, head, r, col, (spec["prefix"] + cur).strip())
            if "Reviewed" in head:
                _set(ws, head, r, "Reviewed", spec["reviewed"])
            done += 1
            print(f"  {sheet}:{r}: {spec['reviewed']} "
                  f"{str(row.get('Date'))[:10]} {str(row.get('Source'))[:16]}")
    return done


def remove_duplicate_fsa_copy(wb) -> int:
    """A. The third address of FSA-PRIN-38-2026 leaves the LIVE sheets.

    Recalls is not touched — the recall is already published there, under the
    alert's own notice URL. Rejected and Weekly_Rejected keep their copies
    and both are annotated above.
    """
    removed = 0
    for sheet in ("Pending", "Weekly_Review"):
        if sheet not in wb.sheetnames:                  # pragma: no cover
            continue
        ws = wb[sheet]
        head = _head(ws)
        for r in reversed(_find_rows_exact(ws, GREENCORE_ARTICLE)):
            row = _rowdict(ws, head, r)
            print(f"  {sheet} row {r}: {str(row.get('Date'))[:10]} "
                  f"{str(row.get('Company'))!r} "
                  f"{str(row.get('Product'))!r} — removed (third address of "
                  f"FSA-PRIN-38-2026, published in Recalls since 2026-08-07)")
            ws.delete_rows(r, 1)
            removed += 1
    if not removed:
        print("  NOTE: no live copy found — if another pass already removed "
              "it, that is the expected outcome, not an error")
    # Recalls must hold the alert exactly ONCE, under the notice URL.
    ws = wb["Recalls"]
    pub = _find_rows(ws, "https://alerts.food.gov.uk/news-alerts/alert/"
                         "fsa-prin-38-2026")
    if len(pub) != 1:
        raise SystemExit(
            f"Recalls holds {len(pub)} rows for FSA-PRIN-38-2026, expected "
            f"exactly 1. The duplicate this pass was built to prevent may "
            f"already have been promoted — stop and read the sheet before "
            f"shipping anything.")
    if _find_rows_exact(ws, GREENCORE_ARTICLE):
        raise SystemExit(
            "Recalls carries the /article/6816/ copy of FSA-PRIN-38-2026 as "
            "well as the notice copy. That is the duplicate publication this "
            "pass exists to prevent, and it is already in the register — "
            "stop and read the sheet before shipping anything.")
    return removed


def queue_manual_rows() -> int:
    """F. Through the real code path, never straight into Recalls."""
    if not ROWS_JSON.exists():                          # pragma: no cover
        raise SystemExit(f"missing {ROWS_JSON}")
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
         str(ROWS_JSON)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        # The Swiss row is the fifth national copy of one Alnatura recall. If
        # the near-duplicate rule refuses it, that refusal is correct on its
        # own terms and this pass accepts it rather than forcing the row.
        print(f"  NOTE: add_manual_row exited {proc.returncode} — read the "
              f"per-row verdict lines above; a near-duplicate refusal is an "
              f"accepted outcome for the BLV (CH) row.")
    return 0


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")

    wb = openpyxl.load_workbook(XLSX)

    print("E. Recalls: the French Reason on a published row")
    recalls_fixed = _fix_sheet(wb, "Recalls", RECALLS_FIXES)
    print("E. Weekly_Review mirror (same spec; not every row is mirrored)")
    mirror_fixed = _fix_sheet(wb, "Weekly_Review", RECALLS_FIXES,
                              required=False)
    print("D. Pending: the four fields the regulators' pages print")
    pending_fixed = _fix_sheet(wb, "Pending", PENDING_FIXES)
    print("D. Weekly_Review mirror")
    pending_mirror = _fix_sheet(wb, "Weekly_Review", PENDING_FIXES,
                                required=False)
    print("C. Archive annotations: six FSA alerts published AND rejected")
    annotated = annotate_archives(wb, ARCHIVE_NOTES)
    print("C. Weekly_Rejected: the Greencore article copy")
    annotated += annotate_archives(wb, WEEKLY_REJECTED_NOTES)
    print("A. The duplicate FSA alert leaves Pending")
    removed = remove_duplicate_fsa_copy(wb)

    wb.save(XLSX)
    wb.close()

    print("F. Queue the two verified misses")
    queue_manual_rows()

    print(f"\nrepair_2026_11_08: Recalls {recalls_fixed} fixed, "
          f"Weekly_Review {mirror_fixed}+{pending_mirror} mirrored, "
          f"Pending {pending_fixed} fixed, {annotated} archive row(s) "
          f"annotated, {removed} duplicate live copy/copies removed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
