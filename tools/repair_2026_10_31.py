# -*- coding: utf-8 -*-
"""Morning fix pass 2026-10-31.

Everything here was read on a regulator page this morning or is a faithful
translation of text already on the row. ONE ROW LEAVES Recalls (section A),
so the commit message carries `remove-rows`.

A. ONE ROW REMOVED FROM Recalls — NOT A FOOD (R1).

   NAFDAC (NG), Date 2026-10-24, Company "South Africa Foods Company",
   Product "Artemether/Lumefantrine 80mg/480mg", Pathogen "Substandard and
   Falsified", Tier 3, promoted 2026-10-30:
     nafdac.gov.ng/public-alert-no-042-2026-alert-on-the-seizure-of-
     suspected-substandard-and-falsified-bppl-artemether-lumefantrine-
     80mg-480mg/

   Read on that alert this morning: artemether-lumefantrine is the
   first-line ANTIMALARIAL, and the alert is about a FALSIFIED medical
   product — fake NAFDAC registration number 02-3457, no Marketing
   Authorisation Holder on the label, purportedly manufactured by Bandaram
   Pharma Packleh Ltd (BPPL) of India, seized in Kano State during
   post-market surveillance. NAFDAC's stated risk is treatment failure and
   drug resistance.

   Three separate defects on one row:
     * not a food — this is a food recall register (R1);
     * "South Africa Foods Company" appears NOWHERE on the alert. The word
       "Foods" in a mis-parsed firm name was the only food on the row;
     * the alert was released 19 August 2026. The Date 2026-10-24 is the
       day a news mirror (punchng.com) surfaced it.

   It was published because the publish gate reads DOSAGE FORMS (the
   2026-10-04 Greenwich Rx repair) and an antimalarial tablet has none it
   may safely match — "tablets" and "capsules" are dietary-supplement pack
   forms and supplements are in scope. Four tests named four symptoms of
   this one row: tests/test_alert_vocab (no alert term can reach
   "Substandard and Falsified"), tests/test_curator_scope_2026_09_09 (the
   curator refuses the hazard), and
   tests/test_a_recall_is_not_both_published_and_rejected (the archive
   holds a rejected copy of the same URL with no verdict). The gate is
   extended at source in pipeline/_publish_gate._FALSIFIED_MEDICINE_MARKERS
   in this same upload, with tests/test_a_falsified_medicine_is_not_a_food_
   recall.py holding it; the schema sweep had already filed the row's
   hazard as HazardGroup "pathogen-bacterial".

   The archived copy at the same URL is ANNOTATED, never deleted
   (section E) — archives are append-only.

B. ONE SOURCE LABEL THAT NAMES NO REGULATOR.

   The ANVISA KF Embutidos linguiça recall of 2026-10-28 published with
   Source "National Authority":
     gov.br/anvisa/pt-br/assuntos/noticias-anvisa/2026/anvisa-suspende-
     venda-de-linguica-suina-defumada-da-marca-kf-embutidos

   The recall is real and in scope. "National Authority" is
   pipeline/gap_finder_tavily.HOST_TO_SOURCE's placeholder for "host known,
   agency not", mapped to gov.br — the whole Brazilian government — while
   pipeline/regulatory_domains.py has mapped the SAME host to ANVISA since
   it was written. One host, two tables, two answers; the gap finder read
   the one that said nothing. So canonical_source() returned
   kind='unknown', verify_urls could not host-check the row, the coverage
   denominator counted it as its own observer, and
   tools/monitored_sources did not count it (the dashboard tile was short
   by one). The registry's label for that authority is "ANVISA (BR)" and
   the register already holds two rows under it.

   Source is set to "ANVISA (BR)". The table is corrected in both copies
   and the publish gate now REFUSES any label with no host allow-list —
   this is the ninth recurrence of this defect and the first eight repairs
   were all vocabulary additions found by an already-published row. See
   tests/test_a_source_label_must_be_host_checkable.py.

C. FOURTEEN STAMPED ROWS TRANSLATED, plus FOUR the detector missed.

   Every Recalls row carrying "[needs cleanup: Product not in English]" or
   "[needs cleanup: Reason not in English]" (14 rows, 13 RappelConso and
   the ANVISA row of section B), translated faithfully with the original
   kept in Notes and the stamp dropped.

   FOUR MORE carried no stamp and were found by reading every Product and
   Reason published in the last 26 h by hand — which is why the operator
   does that and does not trust the detector:
     fiche 23977  Product "saucisse de thon lot 250"      (Reason stamped,
                                                           Product not)
     fiche 23939  Reason  "Presence liesteria"            (no stamp at all)
     fiche 23953  Reason  "Detection of listéria monocytogene"
     fiche 23946  Reason  "Presence of listéria monocytogènes"

   Brand names, protected names and lot/weight/date details are kept as
   written. Nothing is added that is not on the row or its page.

D. TWO FIELD DEFECTS READ ON THEIR OWN FICHE THIS MORNING.

   fiche 23951 — Product was "sans marque/sans modèle", published Tier 1
     against Listeria monocytogenes. The fiche's dénomination is "Salade de
     riz composée", marque "Sans marque", distributor E. Leclerc Chaumont
     (fiche origin "CHAUMONDIS E.LECLERC"), lot 16102026, published
     29/10/2026, category Alimentation / Plats préparés et snacks. The
     register named a product with no brand and no model while the real
     name sat in `libelle`, a field the scraper already reads. This is the
     THIRD instance of that class (fiche 23905 published four lot codes on
     2026-10-22; fiches 23922 and 23914 published the GTIN block on
     2026-10-24) and it is fixed at source in
     scrapers/europe_eu/rappelconso.py in this upload.

   fiche 23964 — Company AND Brand were both the product description
     ("Coquillages du groupe 3 (Huitres et Moules) Récoltés Dans L'Anse de
     Carteau (13.06.01), Dont Huîtres de Camargue et Moules"), so the
     register named a company that does not exist. Read on the fiche: no
     distributing company is named at all; the fiche's origin is the
     "COMITE REGIONAL DE LA CONCHYLICULTURE DE LA MEDITERRANEE" and the
     distributors are "producteurs en vente directe, marchés locaux ou
     régionaux, détaillants, grandes et moyennes surfaces". No commercial
     brand is named. Company therefore takes the register's own
     "(not specified in RappelConso fiche NNNNN)" form (19 rows) rather
     than a guess, and Brand becomes "Unbranded".

E. ONE ARCHIVE ROW ANNOTATED — the rejected copy of the NAFDAC URL in
   section A. Never deleted; the note records that the published copy was
   withdrawn as out of scope, so the archive stops contradicting the
   register silently.

F. TWO Pending ROWS ARCHIVED — NOT FOOD (R1), and both re-publish an old
   alert.

   NAFDAC public-alert-no-042-2024, Ryaltris Nasal Spray, Glenmark
     Pharmaceutical Inc — a prescription nasal spray, and the URL's own
     alert number is from 2024 against a Date of 2026-10-29.
   NAFDAC public-alert-no-30-2018, cosmetics withdrawn by the Portuguese
     authority — cosmetics are not food, and the alert number is from 2018
     against a Date of 2026-10-21. Company and Product are both empty.

   Both are already blocked by the publish gate, so they would never have
   published; they sit in Pending failing
   tests/test_alert_number_year_matches_date.py[Pending] for as long as
   they stay there. Archived with the reason read on their own URLs.

G. TWO RECALLS THE REGISTER DOES NOT HOLD, each read on its own authority
   page this morning. Queued through tools/add_manual_row.py — normal
   review path, no row typed into Recalls.

   RappelConso fiche 23952 — Cassoulet de porc, Hardouin, Listeria
     monocytogenes, published 29/10/2026. Absent from all five sheets.
   AGES (AT) — TAFELTRAUBEN KERNLOS HELL 500G von VILLAFRUT SRL,
     acetamiprid above the maximum residue level, 29.10.2026. Absent from
     all five sheets, and the register's newest AGES row was 2026-10-20,
     eleven days back. Dated two days before this run rather than one: it
     is reported as such, not as a 24 h find.

H. THREE Pending ROWS UNBLOCKED on a field read on their own page today,
   and ONE Pending DUPLICATE archived.

   FSANZ Maria's Pasta (foodstandards.gov.au/.../marias-pasta-various-
     products), 2026-10-30, foreign matter (metal) — a real, in-scope
     recall from the last 24 h, blocked on an EMPTY Company. Read on the
     FSANZ alert today: supplier "Maria's Pasta", six Bolognese lines in
     450 g and 500 g packs, use-by 30 Oct – 04 Nov 26, sold through
     independent retailers including IGA in Victoria and online, Problem
     "The presence of foreign matter (metal).", Food safety hazard "Food
     products containing metal may cause illness/injury if consumed.",
     recall date 30 October 2026.

     THE CAUSE IS A CHARACTER CLASS. pipeline/official_feeds/sources/
     australia.py reads the firm with two regexes over the class
     [\\w &.''\\-] — two ASCII apostrophes, where a typographic one was
     meant. FSANZ writes "Maria’s Pasta" with U+2019, so the title
     fallback stopped at the ’ and matched nothing. The same class is in
     ELEVEN places across australia.py, ireland.py, us_fsis.py and
     new_zealand.py; all are widened in this upload and
     tests/test_a_typographic_apostrophe_does_not_erase_the_firm.py holds
     it. The 2026-10-29 pass fixed the Bomberry row's shape in
     scrapers/oceania/fsanz.py and could not see this one: "Bomberry
     Various Products - 150g" is pure ASCII.

     Pathogen was the one-off string "Foreign Matter", which no alert term
     in tools/alert_vocab.py can reach — the SAME string the 2026-10-29
     pass repaired on the Bomberry row. Set to the register's canonical
     "Foreign material (metal)", the parenthetical taken from the page's
     own Problem line.

   RappelConso fiche 23937 — "boulgour fin à la libanaise 230g", Listeria
     monocytogenes, blocked on an empty Company. Read on the fiche:
     dénomination "Boulgour fin à la libanaise", marque "Sans marque",
     fiche origin "WORLDMARECHAL World Marechal", distributors Fresh,
     Grand Frais and Mon marché, lot 288, published 29/10/2026.
   RappelConso fiche 23956 — "boeuf cuit", Listeria monocytogenes, blocked
     on an empty Company. Read on the fiche: dénomination "Boeuf cuit",
     marque "SANS MARQUE", fiche origin "Bio Artisanal de Bretagne", sold
     at open-air markets in Brittany, lot 391267av (DLC 21/10/2026),
     published 29/10/2026.

   THE DUPLICATE. Pending held fiche 23937 TWICE, under two URL shapes:
     rappel.conso.gouv.fr/fiche-rappel/23937/interne          (canonical)
     rappel.conso.gouv.fr/static/fiches/8dbe652e-…-0e1ac55eb179.html
   URL is the primary dedup key and a UUID cannot be matched to a fiche
   number, so one notice entered Pending twice; the date + company +
   pathogen fallback could not catch it either, because Company was empty
   on both. No published row has ever carried the static shape (923
   RappelConso rows all cite /fiche-rappel/<id>/interne). The static copy
   is archived as a duplicate, and the shape is refused at source in
   merge_master._GENERIC_URL_PATTERNS, alongside the FDA share-link
   wrapper that is the same class of defect.

   NOT ADDED, deliberately: RappelConso fiche 23927, "filet de lieu noir
   200/400", POMONA TERRE AZUR, E.LECLERC BRIANCON, 29/10/2026. The fiche's
   motif is "Non conformité ABVT" (total volatile basic nitrogen) and its
   risque is the generic "Autres contaminants biologiques". ABVT is a
   FRESHNESS parameter and the page names no pathogen, toxin or
   contaminant, so the row is quality/spoilage — out of AFTS scope by the
   2026-07-29 policy, the same ground on which the register refuses the
   EFET HIGH PROTEIN Pudding row.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-31"
BY = "morning fix pass " + TODAY

FICHE = "https://rappel.conso.gouv.fr/fiche-rappel/%s/interne"
ANVISA_URL = ("https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa/2026/"
              "anvisa-suspende-venda-de-linguica-suina-defumada-da-marca-kf-"
              "embutidos")
NAFDAC_DRUG_URL = ("https://nafdac.gov.ng/public-alert-no-042-2026-alert-on-"
                   "the-seizure-of-suspected-substandard-and-falsified-bppl-"
                   "artemether-lumefantrine-80mg-480mg/")

PRODUCT_STAMP = "[needs cleanup: Product not in English]"
REASON_STAMP = "[needs cleanup: Reason not in English]"

MF = "[morning-fix " + TODAY + ": "


# --------------------------------------------------------------------------
# B + C + D — field fixes on published rows, keyed on the row's own URL
# --------------------------------------------------------------------------

RECALLS_FIXES = [
    # ── C. translations ────────────────────────────────────────────────
    {
        "url": FICHE % "23976",
        "set": {"Product": ("Fresh Cantal tome, ground and salted / fresh "
                            "Cantal tome, block")},
        "drop_stamps": [PRODUCT_STAMP],
        "note": (MF + "Product translated from the fiche's own French. "
                 "\"Cantal\" is a protected name (AOP) and is kept as "
                 "written.] [original product: tome fraiche de cantal "
                 "broyée et salée / tome fraiche de cantal bloc]"),
    },
    {
        "url": FICHE % "23963",
        "set": {
            "Product": "Arla Protein chocolate pudding, 200 g",
            "Reason": ("Potential risk of the presence of broken glass "
                       "fragments."),
        },
        "drop_stamps": [REASON_STAMP, PRODUCT_STAMP],
        "note": (MF + "Reason and Product translated from the fiche's own "
                 "French. The Product carried no stamp — the detector "
                 "misses short names.] [original product: arla protein "
                 "pudding chocolat 200g] [original Reason (fr): \"Risque "
                 "potentiel de présence de fragments de bris de verre.\"]"),
    },
    {
        "url": FICHE % "23978",
        "set": {"Product": "Fresh duck sausage in a vacuum pouch"},
        "drop_stamps": [PRODUCT_STAMP],
        "note": (MF + "Product translated from the fiche's own French.] "
                 "[original product: saucisse fraîche de canard en poche "
                 "sous vide]"),
    },
    {
        "url": FICHE % "23977",
        "set": {
            "Product": "Tuna sausage, lot 250",
            "Reason": ("Lot recalled for destruction following detection of "
                       "histamine"),
        },
        "drop_stamps": [REASON_STAMP, PRODUCT_STAMP],
        "note": (MF + "Reason and Product translated from the fiche's own "
                 "French. Only the Reason was stamped; the Product was "
                 "found by reading every Product published in the last "
                 "26 h.] [original product: saucisse de thon lot 250] "
                 "[original Reason (fr): \"Rappel lot pour destruction car "
                 "detection d'histamine\"]"),
    },
    {
        "url": FICHE % "23975",
        "set": {"Product": ("Minced beef steak prepared to order, sold at "
                            "the traditional service counter")},
        "drop_stamps": [PRODUCT_STAMP],
        "note": (MF + "Product translated from the fiche's own French.] "
                 "[original product: steak haché à la demande vendu au "
                 "rayon traditionnel]"),
    },
    # ── D. fiche 23964, Company and Brand were the product description ──
    {
        "url": FICHE % "23964",
        "set": {
            "Company": "(not specified in RappelConso fiche 23964)",
            "Brand": "Unbranded",
            "Product": ("Filter-feeding shellfish — group 3 shellfish "
                        "(oysters and mussels) harvested in the Anse de "
                        "Carteau (13.06.01) from 20/10/2026, including "
                        "Camargue oysters and mussels"),
            "Reason": "Escherichia coli",
        },
        "drop_stamps": [PRODUCT_STAMP],
        "note": (MF + "Company AND Brand were both the product "
                 "description, so the register named a company that does "
                 "not exist. Read on the fiche today: the dénomination is "
                 "\"Coquillages filtreurs = coquillages du groupe 3, "
                 "récoltés dans l'Anse de Carteau (13.06.01) à partir du "
                 "20/10/2026\"; the marque field repeats the same "
                 "description and the page states that no commercial brand "
                 "is named; NO distributing company is named at all — the "
                 "fiche's origin is the \"COMITE REGIONAL DE LA "
                 "CONCHYLICULTURE DE LA MEDITERRANEE\" and the "
                 "distributors are \"producteurs en vente directe, marchés "
                 "locaux ou régionaux, détaillants, grandes et moyennes "
                 "surfaces\". Company therefore takes the register's own "
                 "\"(not specified in ...)\" form rather than a guess, and "
                 "Brand becomes Unbranded. Risques encourus \"Escherichia "
                 "coli\", motif \"E.Coli\", all lots (GTIN 00000000), "
                 "published 30/10/2026, category Produits de la pêche et "
                 "d'aquaculture.] [original product: coquillages du groupe "
                 "3 (huitres et moules) récoltés dans l'anse de carteau "
                 "(13.06.01), dont huîtres de camargue et moules] "
                 "[original company: Coquillages du Groupe 3 (Huitres et "
                 "Moules) Récoltés Dans L'Anse de Carteau (13.06.01), Dont "
                 "Huîtres de Camargue et Moules]"),
    },
    {
        "url": FICHE % "23946",
        "set": {
            "Product": ("Raw white sausage for frying, traditional service "
                        "counter"),
            "Reason": "Presence of Listeria monocytogenes",
        },
        "drop_stamps": [PRODUCT_STAMP],
        "note": (MF + "Product and Reason translated from the fiche's own "
                 "French. The Reason carried no stamp.] [original product: "
                 "saucisse blanche à frire crue rayon traditionnel] "
                 "[original Reason (fr): \"Presence of listéria "
                 "monocytogènes\"]"),
    },
    {
        "url": FICHE % "23974",
        "set": {
            "Product": "Salmon pizza",
            "Reason": "Presence of a foreign body",
        },
        "drop_stamps": [REASON_STAMP, PRODUCT_STAMP],
        "note": (MF + "Reason and Product translated from the fiche's own "
                 "French.] [original product: pizza saumon] [original "
                 "Reason (fr): \"Presence of un corps étranger\"]"),
    },
    # ── D. fiche 23951, the placeholder pair ────────────────────────────
    {
        "url": FICHE % "23951",
        "set": {"Product": "Mixed rice salad"},
        "drop_stamps": [PRODUCT_STAMP],
        "note": (MF + "Product was \"sans marque/sans modèle\" — a brand/"
                 "model PLACEHOLDER pair published as the name of a Tier-1 "
                 "Listeria recall. Read on the fiche today: dénomination "
                 "\"Salade de riz composée\", marque \"Sans marque\", "
                 "distributor E. Leclerc Chaumont (fiche origin "
                 "\"CHAUMONDIS E.LECLERC\"), lot 16102026, risque Listeria "
                 "monocytogenes, published 29/10/2026, category "
                 "Alimentation / Plats préparés et snacks. The real name "
                 "sat in `libelle`, a field the scraper already reads; the "
                 "walk did not reach it because the placeholder is neither "
                 "empty nor an identifier list. Third instance of that "
                 "class (fiche 23905 on 2026-10-22, fiches 23922 and 23914 "
                 "on 2026-10-24) and fixed at source in "
                 "scrapers/europe_eu/rappelconso.py in this upload.] "
                 "[original product: sans marque/sans modèle]"),
    },
    {
        "url": FICHE % "23953",
        "set": {
            "Product": "Farmhouse fromage blanc",
            "Reason": "Detection of Listeria monocytogenes",
        },
        "drop_stamps": [PRODUCT_STAMP],
        "note": (MF + "Product and Reason translated from the fiche's own "
                 "French. The Reason carried no stamp.] [original product: "
                 "fromage blanc fermier] [original Reason (fr): "
                 "\"Detection of listéria monocytogene\"]"),
    },
    {
        "url": FICHE % "23949",
        "set": {"Product": ("Meat black pudding (boudin à la viande) sold "
                            "at the traditional service counter")},
        "drop_stamps": [PRODUCT_STAMP],
        "note": (MF + "Product translated from the fiche's own French; the "
                 "French dish name is kept in brackets as the register "
                 "does for the 2026-10-20 boudin row.] [original product: "
                 "boudin à la viande vendu au rayon traditionnel]"),
    },
    {
        "url": FICHE % "23948",
        "set": {"Product": "Fresh duck sausage in a vacuum pouch"},
        "drop_stamps": [PRODUCT_STAMP],
        "note": (MF + "Product translated from the fiche's own French.] "
                 "[original product: saucisse fraîche de canard en poche "
                 "sous vide]"),
    },
    {
        "url": FICHE % "23939",
        "set": {"Reason": "Presence of Listeria monocytogenes"},
        "note": (MF + "Reason was \"Presence liesteria\" — a misspelt, "
                 "half-French hazard line carrying no stamp, found by "
                 "reading every Reason published in the last 26 h. The "
                 "fiche's risque is Listeria monocytogenes and the row's "
                 "own Pathogen field already said so. Product "
                 "\"Tartinette\" is the fiche's own dénomination and is "
                 "left as written.] [original Reason (fr): \"Presence "
                 "liesteria\"]"),
    },
    {
        "url": FICHE % "23950",
        "set": {
            "Product": ("Kippers sold at the traditional fish counter "
                        "(smoked product)"),
            "Reason": ("Presence of Listeria monocytogenes (below 10 "
                       "CFU/g)"),
        },
        "drop_stamps": [REASON_STAMP, PRODUCT_STAMP],
        "note": (MF + "Reason and Product translated from the fiche's own "
                 "French; \"inférieur à 10 ufc/g\" is the fiche's own "
                 "measured value and is kept.] [original product: kippers "
                 "vendu au rayon poissonnerie traditionnel (produit fumé)] "
                 "[original Reason (fr): \"Presence of listéria "
                 "monocytogène (inférieur à 10 ufc/g)\"]"),
    },
    # ── B. the source label, and the Portuguese Product ────────────────
    {
        "url": ANVISA_URL,
        "set": {
            "Source": "ANVISA (BR)",
            "Product": "Smoked pork sausage",
        },
        "drop_stamps": [
            PRODUCT_STAMP,
            # the extract stamp was written twice by the same collector
            " [via Exa daily search, deterministic extract]",
        ],
        "note": (MF + "Source was \"National Authority\", which resolves "
                 "to no regulator, so nothing host-checked this row, the "
                 "coverage denominator counted it as its own observer and "
                 "tools/monitored_sources did not count it. The label is "
                 "pipeline/gap_finder_tavily's placeholder for gov.br — "
                 "the whole Brazilian government — while "
                 "pipeline/regulatory_domains has mapped that same host to "
                 "ANVISA since it was written. ANVISA is the authority "
                 "that published this notice (gov.br/anvisa, 28/10/2026 "
                 "08:22, \"Anvisa suspende venda de linguiça suína "
                 "defumada da marca KF Embutidos\"), and \"ANVISA (BR)\" "
                 "is the registry's label for it. Product translated from "
                 "the notice's own Portuguese. The duplicated "
                 "\"[via Exa daily search, deterministic extract]\" stamp "
                 "is collapsed to one. The table is corrected in both "
                 "copies and the publish gate now refuses any label with "
                 "no host allow-list.] [original product: Linguiça suína "
                 "defumada] [via Exa daily search, deterministic extract]"),
    },
]


# --------------------------------------------------------------------------
# H — Pending rows unblocked on a field read on their own page today
# --------------------------------------------------------------------------

MARIAS_URL = ("https://www.foodstandards.gov.au/food-recalls/recall-alert/"
              "marias-pasta-various-products")

PENDING_FIXES = [
    {
        "url": MARIAS_URL,
        "set": {
            "Company": "Maria’s Pasta",
            "Brand": "Maria’s Pasta",
            "Product": (
                "Maria’s Pasta Bolognese range: Traditional Bolognese "
                "Pasta Sauce 450 g (use by 04 Nov 26); Lasagna Bolognese "
                "500 g (use by 03 Nov 26); Spaghetti Bolognese 500 g "
                "(02 Nov 26); Penne Bolognese 500 g (use by 02 Nov 26); "
                "Prime Beef Ravioli in Bolognese Sauce 500 g (use by "
                "03 Nov 26); Prime Beef Tortellini in Bolognese Sauce "
                "500 g (use by 30 Oct 26 and 03 Nov 26)"),
            "Pathogen": "Foreign material (metal)",
            "Reason": (
                "The presence of foreign matter (metal). Food products "
                "containing metal may cause illness/injury if consumed."),
        },
        "note": (
            MF + "Company was EMPTY, so the row could not publish. Read on "
            "the FSANZ recall-alert page today: supplier \"Maria's Pasta\", "
            "the six Bolognese lines and their pack sizes and date "
            "markings, Problem \"The presence of foreign matter (metal).\", "
            "Food safety hazard \"Food products containing metal may cause "
            "illness/injury if consumed.\", recall date 30 October 2026, "
            "sold through independent food retailers including IGA in "
            "Victoria and online. The cause is a character class: "
            "pipeline/official_feeds/sources/australia.py read the firm "
            "with regexes over [\\w &.''\\-] — two ASCII apostrophes where "
            "a typographic one was meant — and FSANZ writes "
            "\"Maria’s Pasta\" with U+2019, so the title fallback "
            "stopped at the apostrophe. The same class is in eleven places "
            "across four collectors and all are widened in this upload. "
            "Pathogen was the one-off string \"Foreign Matter\", which no "
            "alert term in tools/alert_vocab.py can reach — the same "
            "string the 2026-10-29 pass repaired on the FSANZ Bomberry "
            "row; set to the register's canonical \"Foreign material "
            "(metal)\", the parenthetical taken from the page's own "
            "Problem line.]"),
    },
    {
        "url": FICHE % "23937",
        "set": {
            "Company": "World Marechal",
            "Product": "Fine Lebanese-style bulgur, 230 g (lot 288)",
            "Reason": "Detection of Listeria monocytogenes",
        },
        "drop_stamps": [PRODUCT_STAMP],
        "note": (
            MF + "Company was EMPTY, so the row could not publish. Read on "
            "the fiche today: dénomination \"Boulgour fin à la "
            "libanaise\", marque \"Sans marque\", motif du rappel "
            "\"Détection de Listeria monocytogenes\", risques encourus "
            "\"Infection par la bactérie Listeria monocytogenes, "
            "responsable de la listériose\", date de publication "
            "29/10/2026, lot 288, category Alimentation / Plats préparés "
            "et snacks. The fiche names NO manufacturer; its origin "
            "company is \"WORLDMARECHAL World Marechal\" and the "
            "distributors are Fresh, Grand Frais and Mon marché — Company "
            "takes the fiche's own origin firm, read from the page, and "
            "the distributors stay in Notes where the scraper put them. "
            "Product translated from the fiche's own French.] "
            "[original product: boulgour fin à la libanaise 230g]"),
    },
    {
        "url": FICHE % "23956",
        "set": {
            "Company": "Bio Artisanal de Bretagne",
            "Product": "Cooked beef (lot 391267av)",
            "Reason": "Presence of Listeria monocytogenes",
        },
        "drop_stamps": [PRODUCT_STAMP],
        "note": (
            MF + "Company was EMPTY, so the row could not publish. Read on "
            "the fiche today: dénomination \"Boeuf cuit\", marque \"SANS "
            "MARQUE\", fiche origin \"Bio Artisanal de Bretagne\", motif "
            "du rappel \"Listeria monocytogènes\", risques encourus "
            "\"Listeria monocytogenes (agent responsable de la "
            "listériose)\", date de publication 29/10/2026, lot 391267av "
            "with DLC 21/10/2026, sold at open-air markets in Brittany "
            "(Paimpol, Plérin, Saint-Brieuc, Lannion, Rennes), category "
            "Alimentation / Viandes. Product and Reason translated from "
            "the fiche's own French.] [original product: boeuf cuit] "
            "[original Reason (fr): \"Listeria monocytogènes\"]"),
    },
]


# --------------------------------------------------------------------------
# A — the one row that leaves Recalls
# --------------------------------------------------------------------------

RECALLS_REMOVE = [
    {
        "url": NAFDAC_DRUG_URL,
        "why": ("not a food (R1): a seized FALSIFIED ANTIMALARIAL. Read on "
                "the NAFDAC alert 2026-10-31 — artemether-lumefantrine, a "
                "falsified medical product with fake NAFDAC registration "
                "number 02-3457 and no Marketing Authorisation Holder, "
                "purportedly made by Bandaram Pharma Packleh Ltd (BPPL), "
                "India, seized in Kano State, released 19 August 2026. "
                "Company \"South Africa Foods Company\" appears nowhere on "
                "the alert and Date 2026-10-24 is the day a news mirror "
                "surfaced it."),
    },
]


# --------------------------------------------------------------------------
# E — the archive row at the removed row's URL
# --------------------------------------------------------------------------

NAFDAC_ARCHIVE_NOTE = (
    " [SUPERSEDED " + TODAY + " — the PUBLISHED copy of this URL was "
    "REMOVED from Recalls today as out of scope: artemether-lumefantrine is "
    "an antimalarial and the alert is about a falsified medical product, so "
    "a food recall register holds neither copy. This archive row is now the "
    "only copy and there is no contradiction left to resolve. The original "
    "rejection reason — NO_REASON_RECORDED, 'treat as UNAUDITED, not as a "
    "confirmed rejection' — was never a verdict on scope; the scope verdict "
    "is this note, read on the NAFDAC alert itself. The gate is extended so "
    "the same row cannot be re-promoted: "
    "pipeline/_publish_gate._FALSIFIED_MEDICINE_MARKERS.]")


# --------------------------------------------------------------------------
# F — Pending rows archived
# --------------------------------------------------------------------------

PENDING_ARCHIVE = [
    {
        "url": ("https://nafdac.gov.ng/public-alert-no-042-2024-alert-on-"
                "recall-of-forty-five-thousand-45000-unit-of-ryaltris-nasal-"
                "spray-manufactured-by-glenmark-pharmaceutical-inc/"),
        "reason": (
            "Not a food (R1) — Ryaltris is a prescription NASAL SPRAY by "
            "Glenmark Pharmaceutical Inc, and pipeline/_publish_gate "
            "already refuses it as a non-food dosage form. The URL's own "
            "alert number is public-alert-no-042-2024, from 2024, against "
            "a Date of 2026-10-29: the Date is the day a news mirror "
            "(punchng.com) surfaced the alert, not the day NAFDAC issued "
            "it, so the row also fails R4 and "
            "tests/test_alert_number_year_matches_date.py. Archived by the "
            + BY + " rather than left in Pending to fail that test "
            "indefinitely."),
    },
    {
        "url": ("https://nafdac.gov.ng/public-alert-no-30-2018-portuguese-"
                "authority-ordered-withdrawal-of-cosmetics-products-from-"
                "the-market/"),
        "reason": (
            "Not a food (R1) — COSMETICS withdrawn by the Portuguese "
            "authority. pipeline/_publish_gate already refuses it on its "
            "own text ('cosmetics'), and the row carries no Company, no "
            "Product and no Pathogen. The URL's own alert number is "
            "public-alert-no-30-2018, from 2018, against a Date of "
            "2026-10-21: the Date is the day a news mirror "
            "(dailytrust.com) surfaced it, so the row also fails R4 and "
            "tests/test_alert_number_year_matches_date.py. Archived by the "
            + BY + " rather than left in Pending to fail that test "
            "indefinitely."),
    },
    {
        "url": ("https://rappel.conso.gouv.fr/static/fiches/8dbe652e-ef5c-"
                "43b4-ac84-0e1ac55eb179.html"),
        "reason": (
            "DUPLICATE of RappelConso fiche 23937, already in Pending at "
            "the canonical per-notice URL rappel.conso.gouv.fr/fiche-"
            "rappel/23937/interne — same product (\"boulgour fin à la "
            "libanaise 230g\"), same brand (Unbranded), same pathogen "
            "(Listeria monocytogenes), same Date (2026-10-29), same "
            "distributors in Notes (fresh, grand frais, mon marché). This "
            "URL is RappelConso's downloadable fiche ASSET, keyed on a "
            "UUID, so the URL dedup key could not match it to the fiche "
            "number and the date+company+pathogen fallback could not "
            "either, because Company was empty on both copies. No "
            "published row has ever carried this shape — all 923 "
            "RappelConso rows in Recalls cite /fiche-rappel/<id>/interne. "
            "The shape is now refused at source in "
            "merge_master._GENERIC_URL_PATTERNS, alongside the FDA "
            "share-link wrapper, which is the same class of defect. "
            "Archived by the " + BY + "; the canonical copy is repaired "
            "and promotes normally."),
    },
]


# --------------------------------------------------------------------------
# G — recalls the register does not hold
# --------------------------------------------------------------------------

NEW_ROWS = [
    {
        "Date": "2026-10-29",
        "Source": "RappelConso (FR)",
        "Company": "Hardouin",
        "Brand": "Hardouin",
        "Product": "Pork cassoulet (cassoulet de porc), lot 26440",
        "Pathogen": "Listeria monocytogenes",
        "Reason": "Presence of Listeria monocytogenes",
        "Class": "Recall",
        "Country": "France",
        "Outbreak": 0,
        "URL": FICHE % "23952",
        "Notes": (
            "[operator review " + TODAY + ": MISSING from all five sheets. "
            "Read on the fiche today: dénomination \"Cassoulet de porc\", "
            "marque \"Hardouin\", motif du rappel \"Cassoulet de porc - "
            "Présence de Listéria monocytogenes\", risques encourus "
            "\"Listeria monocytogenes (agent responsable de la "
            "listériose)\", date de publication 29/10/2026, lot 26440, "
            "GTIN 0207827000000, DLC 22/10/2026, fiche réf. 2026-10-0277 "
            "(version 1), sold at the traditional traiteur counter of "
            "E.LECLERC Amboise (fiche origin \"LA MONTGOLFIERE "
            "E.LECLERC\") from 08/10/2026 to 21/10/2026, France entière, "
            "category Alimentation / Viandes. Company takes the brand, as "
            "every other RappelConso row with a named marque does; the "
            "retailer is recorded here and not guessed into Company.] "
            "[original product: cassoulet de porc]"),
    },
    {
        "Date": "2026-10-29",
        "Source": "AGES (AT)",
        "Company": "REWE International AG",
        "Brand": "Unbranded",
        "Product": "Seedless white table grapes, 500 g",
        "Pathogen": "Acetamiprid (pesticide residue)",
        "Reason": ("Residue of the pesticide acetamiprid above the "
                   "permitted maximum level; a health risk cannot be ruled "
                   "out, so consumers are advised not to eat the product."),
        "Class": "Recall",
        "Country": "Austria",
        "Outbreak": 0,
        "URL": ("https://www.ages.at/en/human/product-warnings-product-"
                "recalls_produkt/tafeltrauben-kernlos-hell-500g-von-"
                "villafrut-srl"),
        "Notes": (
            "[operator review " + TODAY + ": MISSING from all five sheets; "
            "the register's newest AGES (AT) row was 2026-10-20, eleven "
            "days back. Read on the AGES page today: "
            "\"TAFELTRAUBEN KERNLOS HELL 500G (von VILLAFRUT SRL)\", filed "
            "by AGES as a Rückruf (recall), not a Produktwarnung; "
            "Inverkehrbringer REWE International AG; manufacturer/producer "
            "Villafrut SRL; batch \"Pro L 2652541\"; GGN 4059883043006; "
            "recall reason \"Acetamiprid\"; dated 29.10.2026. The page "
            "states only that the maximum level was exceeded and gives no "
            "measured figure, so none is recorded. Company takes the "
            "Inverkehrbringer, as the AGES mango row of 2026-09-30 does; "
            "the page names no commercial brand, so Brand is Unbranded. "
            "DATED TWO DAYS BEFORE THIS RUN, not one — reported as such.] "
            "[original product: TAFELTRAUBEN KERNLOS HELL 500G]"),
    },
]


# --------------------------------------------------------------------------

def _head(ws):
    return [c.value for c in ws[1]]


def _rowdict(ws, head, r):
    return {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}


def _set(ws, head, r, field, value):
    ws.cell(r, head.index(field) + 1).value = value


def _norm(u):
    return str(u or "").strip().rstrip("/").lower()


def _apply_notes(ws, head, r, row, note, drop_stamps):
    notes = str(row.get("Notes") or "").strip()
    for stamp in drop_stamps or ():
        notes = notes.replace(stamp, "").strip()
    notes = re.sub(r"[ \t]{2,}", " ", (notes + " " + note).strip())
    _set(ws, head, r, "Notes", notes)


def _fix_sheet(wb, sheet, specs) -> int:
    ws = wb[sheet]
    head = _head(ws)
    done = 0
    for spec in specs:
        target = _norm(spec["url"])
        hit = False
        for r in range(2, ws.max_row + 1):
            row = _rowdict(ws, head, r)
            if _norm(row.get("URL")) != target:
                continue
            for field, value in spec["set"].items():
                if field not in head:
                    continue
                _set(ws, head, r, field, value)
            _apply_notes(ws, head, r, row, spec["note"],
                         spec.get("drop_stamps"))
            done += 1
            hit = True
            print(f"  {sheet}: fixed {row.get('Source')!r} "
                  f"{spec['url'][-52:]}")
        if not hit:
            raise SystemExit(
                f"{sheet}: no row at {spec['url']} — main has moved under "
                f"this repair script; re-read the sheet before re-running")
    return done


def _archive(arch, src_row, reason, rejected_by):
    out = []
    for h in _head(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(rejected_by)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        else:
            out.append(src_row.get(h, ""))
    arch.append(out)


def _remove_recalls_rows(wb) -> int:
    """Take a row OUT of Recalls, and record it in Rejected.

    The row is not deleted from the register's memory: a copy goes to
    Rejected with the scope verdict, which is what every other refusal
    does. Only the PUBLISHED copy goes — archives are append-only.
    """
    ws = wb["Recalls"]
    head = _head(ws)
    arch = wb["Rejected"]
    removed = 0
    for spec in RECALLS_REMOVE:
        target = _norm(spec["url"])
        hit = False
        for r in range(ws.max_row, 1, -1):
            row = _rowdict(ws, head, r)
            if _norm(row.get("URL")) != target:
                continue
            _archive(arch, row,
                     "Removed from Recalls by the " + BY + " — "
                     + spec["why"], BY)
            ws.delete_rows(r, 1)
            removed += 1
            hit = True
            print(f"  Recalls: REMOVED {row.get('Source')!r} "
                  f"{str(row.get('Product'))[:46]!r}")
        if not hit:
            raise SystemExit(
                f"Recalls: no row at {spec['url']} — main has moved under "
                f"this repair script; re-read the sheet before re-running")
    return removed


def _archive_pending(wb) -> int:
    ws = wb["Pending"]
    head = _head(ws)
    arch = wb["Rejected"]
    done = 0
    for spec in PENDING_ARCHIVE:
        target = _norm(spec["url"])
        hit = False
        for r in range(ws.max_row, 1, -1):
            row = _rowdict(ws, head, r)
            if _norm(row.get("URL")) != target:
                continue
            _archive(arch, row, spec["reason"], BY)
            ws.delete_rows(r, 1)
            done += 1
            hit = True
            print(f"  Pending -> Rejected: {row.get('Source')!r} "
                  f"{str(row.get('Product'))[:46]!r}")
        if not hit:
            raise SystemExit(
                f"Pending: no row at {spec['url']} — main has moved under "
                f"this repair script")
    return done


def _annotate_nafdac_archive(wb) -> int:
    """The archived copies of the URL whose published copy was removed.

    The verdict goes in the sheet's own REASON column, not in Notes:
    tests/test_a_recall_is_not_both_published_and_rejected.py reads
    RejectionReason / RejectReason and nothing else. URLs are compared with
    the writer's own dedup key rather than a hand-rolled strip — a trailing
    slash hid one of two rows from the repair pass of 2026-10-26.
    """
    from pipeline.merge_master import _normalize_url_for_dedup as _key

    target = _key(NAFDAC_DRUG_URL)
    done = 0
    for sheet in ("Rejected", "Weekly_Rejected"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = _head(ws)
        why = ("RejectReason" if "RejectReason" in head
               else "RejectionReason" if "RejectionReason" in head else None)
        if "URL" not in head or why is None:
            continue
        for r in range(2, ws.max_row + 1):
            row = _rowdict(ws, head, r)
            if _key(str(row.get("URL") or "")) != target:
                continue
            existing = str(row.get(why) or "").strip()
            if "superseded " + TODAY in existing.lower():
                continue
            if existing.lower() in ("", "nan", "none"):
                existing = ""
            _set(ws, head, r, why, (existing + NAFDAC_ARCHIVE_NOTE).strip())
            done += 1
            print(f"  {sheet}: scope verdict recorded in {why} — "
                  f"NAFDAC falsified antimalarial")
    if not done:
        raise SystemExit(
            "no archive row found at the NAFDAC URL — expected the copy "
            "tests/test_a_recall_is_not_both_published_and_rejected named; "
            "re-read the sheet before re-running")
    return done


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")

    wb = openpyxl.load_workbook(XLSX)
    recalls_fixed = _fix_sheet(wb, "Recalls", RECALLS_FIXES)
    pending_fixed = _fix_sheet(wb, "Pending", PENDING_FIXES)
    # The archive annotation reads the published URLs, so it runs BEFORE
    # the removal takes the published copy out.
    annotated = _annotate_nafdac_archive(wb)
    removed = _remove_recalls_rows(wb)
    archived = _archive_pending(wb)
    wb.save(XLSX)
    wb.close()

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(NEW_ROWS, fh, ensure_ascii=False, indent=2)
        rows_path = fh.name
    try:
        proc = subprocess.run(
            [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
             rows_path], cwd=str(ROOT), text=True, capture_output=True)
        print(proc.stdout.rstrip())
        if proc.stderr.strip():
            print(proc.stderr.rstrip(), file=sys.stderr)
    finally:
        Path(rows_path).unlink(missing_ok=True)
    if proc.returncode != 0:
        raise SystemExit(f"add_manual_row refused a row ({proc.returncode})")

    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"RECALLS_FIELDS_FIXED={recalls_fixed}")
    print(f"PENDING_FIELDS_FIXED={pending_fixed}")
    print(f"ARCHIVE_ROWS_ANNOTATED={annotated}")
    print(f"PENDING_ARCHIVED={archived}")
    print(f"ROWS_QUEUED={len(NEW_ROWS)}")
    print(f"ROWS_REMOVED={removed}")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
