# -*- coding: utf-8 -*-
"""Morning fix pass, 2026-10-06. Four faults plus six missed recalls.

Run on a FRESH clone of main, by tools/rebase_and_verify.py, which then
runs pipeline.promote_gate_passing --apply behind it.

ONE ROW IS REMOVED from Pending (fault D). NOTHING is removed from
Recalls, so the commit message needs no deletion marker.

Every fact written below was read on an authority page or in the row's own
text/URL, and the Notes say which.


FAULT A — ONE BYPASS, TWO SYMPTOMS, 50 ROWS
============================================
``pipeline/gap_finder/main.py::_append_rows`` and
``pipeline/gap_finder_gr/main.py::_append_rows`` open the workbook with
openpyxl and append. Neither goes through
``merge_master._write_sheet``, the documented writer choke point, and
neither called the two guards merge_master exposes for precisely this
case — ``apply_label_aliases`` and
``strip_empty_identifier_template``, each of which says in its own
docstring that it is callable by "the writers that do not go through it".

Measured on main at fa15d2e:

  A1  48 rows in Weekly_Rejected carry Source 'FSIS'.
      SOURCE_ALIASES has mapped "fsis" -> "USDA FSIS" since 2026-08 and
      pipeline/gap_finder/countries/us.py sets authority_short="FSIS".
      tests/test_publish_gate.py::test_usda_fsis_source_label_is_canonical
      was RED.

  A2  2 rows in Pending carry "(Recall ID not provided)" in Reason.
      _EMPTY_ID_TEMPLATE has matched "not provided" since it was written.
      tests/test_empty_identifier_template_never_reaches_data.py was RED.

The writers are fixed in code, with
tests/test_a_writer_that_skips_the_choke_point_still_canonicalises.py
holding both symptoms through the real writers. This script repairs the
rows already written — by calling the same two merge_master functions, not
by hand-editing strings, so what runs here is what will run tomorrow.

WHAT THIS DOES NOT FIX, SAID PLAINLY. 244 further archive rows carry a
label the alias map renames:

    Rejected         Salute 50, EFET 34, AESAN 33, NCC 21,
                     Country 'South Korea' 18
    Weekly_Rejected  Salute 28, NCC 5, AESAN 4, EFET 3

Those are left exactly as written. Weekly_Rejected and Rejected are
append-only archives of what each gate refused; rewriting one to match a
map authored after it is a decision about what the archive is FOR, and it
is the operator's, not a morning pass's. The 48 'FSIS' rows are repaired
only because test_publish_gate's banned-label assertion, which predates
all of this, covers every sheet for that one agency.


FAULT B — TWO ITALIAN ROWS ABOUT TO PUBLISH IN ITALIAN
=======================================================
The two Pending rows from fault A2 are the Italian Ministero della Salute
recalls the Italian gap finder found via ilfattoalimentare.it:

    AgriLanga   "Capric cheese"   Roccaverano DOP   E. coli STEC gene EAE
    BMS Srl     "Popcorn corn"    Probios           alcaloidi tropanici

Three defects between them, all visible without leaving the row:

  * Source is the bare "Salute" on both — the label SOURCE_ALIASES gained
    an entry for on 2026-10-04 after this same bypass let it reach
    **Recalls**. Fixed by fault A's call.

  * "Capric cheese" is not English. It is a word-for-word rendering of
    the Italian "formaggio caprino", which is GOAT cheese — "capric" in
    English is an unrelated fatty acid. Corrected to "Goat cheese".
    Corroborated 2026-10-06 against the discovery source already named in
    the row's own Notes (ilsalvagente.it/2026/09/29/e-coli-stec-richiamato
    -formaggio-roccaverano-dop-agrilanga/), which describes "formaggio
    caprino a latte crudo Roccaverano DOP a marchio AgriLanga". NOTHING
    ELSE is imported from that article — not the lot, not the raw-milk
    detail, not the weight — because the row's own authority URL is a
    salute.gov.it PDF behind a Gcore wall and could not be read.

  * "Popcorn corn" is a double rendering of "mais per popcorn". The
    product, its weight, its brand, its lot and its best-before date are
    all in the row's OWN authority URL, whose filename reads
    MODULORICHIAMOMAISPERPOPCORN400grmarchioPROBIOSLotto60183TMC30_04_2028
    — so "Maize for popcorn, 400 g, lot 60183, best before 30/04/2028"
    adds no fact from anywhere else. Pathogen 'Alcaloidi tropanici
    exceeding limit' becomes 'Tropane alkaloids (above the legal limit)'.

Both originals are kept in Notes, which the dashboard shows on hover and
searches.

THE GATE THAT REFUSED THEM. Both rows are also in Weekly_Rejected,
refused by gap_finder/it/rules.py as "unknown: No matching hazard
category — defer to manual review". For the tropane row that was a real
VOCABULARY GAP: pipeline/gap_finder/rules.py NATURAL_TOXINS held ergot
and pyrrolizidine alkaloids but not tropane alkaloids, which is the same
gap, in the neighbouring alkaloid family, that the 2026-10-03 pass fixed
for the PAs. tools/alert_vocab.py has carried "tropane" under "Mushroom /
plant toxins" all along, so a published tropane row already reaches
subscribers — only that gate could not see it. Added in code, with the
measurement in the comment.


FAULT C — A POLISH RECALL BOTH PUBLISHED AND THROWN AWAY
=========================================================
    Weekly_Rejected, GIS, written 2026-10-06T02:56:40Z
    "Ostrzeżenie publiczne ... Listeria monocytogenes w jednej partii
     ćwiartki wędzonej z kurczaka"
    RejectionReason: "llm-extraction-failed: Llama timeout or
     circuit-open; title-only fallback could not produce clean fields.
     Not a content verdict — re-extract when the box is up."

The same gov.pl URL is PUBLISHED in Recalls: Przedsiębiorstwo Drobiarskie
"Drobex" Sp. z o.o., smoked chicken quarter, 2026-09-29, Listeria
monocytogenes. So the Polish finder rediscovered a recall the register
already had, its LLM box was down, and the fallback archived it — leaving
the register saying the same URL is both published and discarded.
tests/test_a_recall_is_not_both_published_and_rejected.py was RED.

The rejection reason says outright it is not a content verdict. The
published copy wins. Marked by running the real sweep,
``promote_gate_passing.supersede_every_published_url``, rather than
stamping the row by hand. Nothing is deleted.


FAULT D — THE FSA API CATALOG PAGE, SCRAPED AS A RECALL, SITTING IN PENDING
===========================================================================
    Pending, FSA (UK), Status 'rejected'
    URL     https://data.food.gov.uk/food-alerts/id.html?__htmlView=
    Product "Title: API results: /food-alerts/id.html
             | title | James Hall recalls BBQ Pulled Pork because it may
             contain Salmonella |"

data.food.gov.uk/food-alerts/id.html is the FSA linked-data API's own
CATALOG page, not a recall notice; "?__htmlView=" is that API's
HTML-view toggle. The row's Product field is raw scrape text — a header
line and a markdown table row — which is the same class of defect as a
prompt example reaching data.

The operator already settled this: the twin row on the un-parametered URL
was archived in Rejected on 2026-08-26 as "not_a_notice — FSA Data
Catalog", and this copy is in Weekly_Rejected too, carrying "REJECTED:
URL agent: Could not fetch the official regulator URL". So the row is
archived twice and is marked rejected in Pending itself. It is removed
from Pending — the queue, not an archive — and nothing is lost.

It is also what made
tests/test_a_query_param_recall_id_is_never_dropped.py red, two distinct
URLs on one dedup key. Removing this copy is not enough on its own (the
Weekly_Rejected copy keeps the parametered URL and archives are
append-only), so the key shape is listed in that test's INTENTIONAL with
the reason: the differing part is provably presentation, and a real FSA
alert carries its identifier in the PATH (/food-alerts/id/<number>), so
it cannot reach this key at all.

FOR THE OPERATOR, NOT DECIDED HERE: behind all three junk rows there is a
real FSA recall — James Hall, BBQ pulled pork, Salmonella — and it is
published in NO sheet. It carries no date in any of the three rows and no
per-alert FSA URL was found for it (the search surfaces only the catalog
page and a thegrocer.co.uk write-up, which is a news URL and cannot be
the authority URL under R3). It is outside today's 24-hour window either
way, so it is reported rather than added.


FAULT E — SIX RECALLS THE PIPELINE MISSED IN THE LAST 24 HOURS
===============================================================
All six were read on the regulator's own per-recall page on 2026-10-06
and searched for in all five sheets first — by URL, by recall id, by
company, by brand, by product and by a distinctive word. None was in any
of them.

FRANCE — RappelConso, published 05/10/2026. The collector took 23695 and
23696 from that day and correctly refused 23697 (ABVT, a freshness
parameter) and 23698 (seal defect, packaging), but missed three:

    23663  Aubergines, PRESTA VERDE / FRUTAS SANCHEZ
           "Dépassement de LMR acétamipride"
    23562  Hemp oil 15% CBD FULL spectrum, Les Planteurs Alsaciens
    23593  Hemp oil 5% CBD FULL spectrum,  Les Planteurs Alsaciens
           both: delta-9-THC above the acute reference dose

23562 and 23593 are NOT duplicates of the already-published 23592 and
23594. Those two are the BROAD-spectrum oils ("spectre large", THC <
0.057%) recalled 30/09; these are the FULL-spectrum line ("spectre
complet"), separate fiches published 05/10. Four fiches, two product
lines, one company. If the near-duplicate rule refuses them anyway that
is the rule working on a genuinely similar pair and is reported, not
worked around.

JAPAN — Consumer Affairs Agency, 公表日 2026/10/05. Eleven food notices
were published that day and the register's Japanese coverage stops at
2026/10/02 (rcl 35905-35910). All eleven were opened and read
individually on 2026-10-06. Three are in scope:

    35915  Wismettac Foods, fresh mango from Thailand
           "農薬テトラコナゾール（0.03ppm）を検出（基準値0.01ppm）"
    35916  Kinokuniya, miso peanuts
           "虫（ノシメマダラメイガ）の混入"
    35924  吉備農産物販売, tomatoes
           "農薬アセフェート 0.51ppm、メタミドホス 0.27ppmを検出
            （基準値：0.03ppm、0.02ppm）"

The other eight are out of scope and the reason for each, read off its
own page, is recorded here so the call can be checked rather than taken
on trust:

    35914  mixed vegetables      品質不良(腐敗) — spoilage, a quality defect
    35917  choko tonochu         アレルゲン「乳」の表示欠落 — allergen only
    35918  fried tofu            消費期限の誤表示 — labelling
    35919  sanku ramen           賞味期限の誤表示 — labelling
    35920  dried apricots        添加物の誤表示 — labelling
    35921  saku saku ebi ebi     texture not maintained — quality
    35922  sauce cutlet +3       賞味期限の誤表示 — labelling
    35923  rice-flour karinto    アレルゲン「ごま」の表示欠落 — allergen only

Company names follow the register's existing Japanese convention
(standard romanisation where the firm publishes no English name, as the
2026-10-03 pass did for 平田産業 -> "Hirata Sangyo"), with the original
kept in Notes. 吉備農産物販売 publishes no English name, so it is
romanised "Kibi Nosanbutsu Hanbai" and NOT translated.

Date is the CAA publication date, matching the convention of the
2026/10/02 batch already in Recalls; each row's 対応開始日 is recorded in
Notes because the two differ.

BLOCKED, REPORTED NOT ROUTED AROUND: fsis.usda.gov/recalls returned
HTTP 403 and cfs.gov.hk/english/press/ could not be opened at all this
run, so neither USDA FSIS nor Hong Kong was swept. "I could not read it"
is not "it is gone".
"""
from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

TODAY = dt.date.today().isoformat()

#: Every sheet that is a live queue or the register itself. The two
#: archives are listed for fault A1 only, which is scoped by label below.
ALL_SHEETS = ("Recalls", "Pending", "Weekly_Review", "Weekly_Rejected",
              "Rejected")

#: Fault A1. ONLY the USDA FSIS variants, which test_publish_gate's
#: BANNED_SOURCE names and which it checks on every sheet. Every other
#: aliased archive label is deliberately left alone — see the docstring.
FSIS_BANNED = {"fsis", "usda", "usda-fsis", "usda fsis (us)"}
FSIS_CANONICAL = "USDA FSIS"

#: Fault B. Keyed by URL, the only stable key on a Pending row.
IT_AGRILANGA_URL = (
    "https://www.salute.gov.it/new/sites/default/files/external_data/"
    "avvisi_sicurezza_alimentare/cartello%20richiamo%202_1790242398.pdf")
IT_PROBIOS_URL = (
    "https://www.salute.gov.it/new/sites/default/files/external_data/"
    "avvisi_sicurezza_alimentare/MODULORICHIAMOMAISPERPOPCORN400grmarchio"
    "PROBIOSLotto60183TMC30_04_2028_1789992179.pdf")

IT_REPAIRS = {
    IT_AGRILANGA_URL: {
        "Product": "Goat cheese",
        "Notes": (
            "[morning-fix " + TODAY + ": Product read 'Capric cheese' — a "
            "word-for-word rendering of the Italian 'formaggio caprino', "
            "which is GOAT cheese; 'capric' in English is an unrelated "
            "fatty acid. Corrected to 'Goat cheese'. Corroborated " + TODAY +
            " against the discovery source already named in this row's "
            "Notes: ilsalvagente.it/2026/09/29/e-coli-stec-richiamato-"
            "formaggio-roccaverano-dop-agrilanga/, which describes "
            "\"formaggio caprino a latte crudo Roccaverano DOP a marchio "
            "AgriLanga\". NOTHING else is taken from that article — no lot, "
            "no weight, no raw-milk detail — because this row's own "
            "authority URL is a salute.gov.it PDF behind a Gcore wall and "
            "could not be read. The '(Recall ID not provided)' template was "
            "stripped from Reason by the same pass; see "
            "tools/repair_" + TODAY.replace("-", "_") + ".py fault A2.] "
            "[original product: Capric cheese]"),
    },
    IT_PROBIOS_URL: {
        "Product": ("Maize for popcorn, 400 g, lot 60183, "
                    "best before 30/04/2028"),
        "Pathogen": "Tropane alkaloids (above the legal limit)",
        "Notes": (
            "[morning-fix " + TODAY + ": Product read 'Popcorn corn' — a "
            "double rendering of 'mais per popcorn'. The product, weight, "
            "brand, lot and best-before are all in THIS ROW'S OWN authority "
            "URL, whose filename reads MODULORICHIAMOMAISPERPOPCORN400gr"
            "marchioPROBIOSLotto60183TMC30_04_2028, so no fact is added "
            "from anywhere else. Pathogen read 'Alcaloidi tropanici "
            "exceeding limit' (Italian) and is now 'Tropane alkaloids "
            "(above the legal limit)'. The hazard is why this row was also "
            "refused by gap_finder/it/rules.py as 'No matching hazard "
            "category': pipeline/gap_finder/rules.py NATURAL_TOXINS held "
            "ergot and pyrrolizidine alkaloids but not tropane alkaloids. "
            "Added to that gate in code on " + TODAY + "; "
            "tools/alert_vocab.py has carried 'tropane' under 'Mushroom / "
            "plant toxins' all along, so the published row reaches "
            "subscribers.] "
            "[original product: Popcorn corn] "
            "[original pathogen (it): \"Alcaloidi tropanici exceeding "
            "limit\"]"),
    },
}

#: Fault D. The FSA linked-data API catalog page, scraped as a recall.
FSA_CATALOG_URL = "https://data.food.gov.uk/food-alerts/id.html?__htmlView="

#: Fault E. Six rows, each read on the regulator's own page on 2026-10-06.
_SWEEP = ("morning-fix missed-recall sweep " + TODAY + ". Searched all five "
          "sheets first — by URL, by recall id, by company, by brand, by "
          "product and by a distinctive word — and absent from every one. ")

MISSED_ROWS = [
    {
        "Date": "2026-10-05",
        "Source": "RappelConso (FR)",
        "Company": "FRUTAS SANCHEZ",
        "Brand": "PRESTA VERDE",
        "Product": ("Aubergines — bulk, cardboard case (COLIS CARTON), "
                    "lot 07368397"),
        "Pathogen": "Acetamiprid (pesticide residue)",
        "Reason": "Acetamiprid above the maximum residue limit.",
        "Class": "Voluntary",
        "Country": "France",
        "Outbreak": 0,
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23663/Interne",
        "Notes": (
            "[manual " + TODAY + ": " + _SWEEP + "Fiche 23663 (réf. "
            "2026-10-0001) read on its own authority page on " + TODAY + "; "
            "date de publication 05/10/2026, recall start 28/09/2026. Read "
            "off that page: Nom du ou des fabricants FRUTAS SANCHEZ, marque "
            "PRESTA VERDE, modèles/références PRESTA VERDE, numéro de lot "
            "07368397, conditionnement COLIS CARTON, zone de vente "
            "Auvergne-Rhône-Alpes / Bourgogne-Franche-Comté / "
            "Centre-Val de Loire, distributeurs Super U, Carrefour Market "
            "and open-air markets. Company is the fiche's FABRICANT, not "
            "its DISTRIBUTEURS — the error the 2026-10-03 pass had to "
            "repair on fiche 23678. In scope under R6 as a pesticide "
            "residue above the limit.] "
            "[original Reason (fr): \"Dépassement de LMR acétamipride\"] "
            "[original risk (fr): \"Dépassement des limites autorisées de "
            "pesticides\"] "
            "[original product: AUBERGINE PRESTA VERDE]"),
    },
    {
        "Date": "2026-10-05",
        "Source": "RappelConso (FR)",
        "Company": "HALFTERMEYER YVES Les Planteurs Alsaciens",
        "Brand": "Les Planteurs Alsaciens",
        "Product": ("Hemp oil 15% CBD full spectrum, 10 ml dropper bottle "
                    "(200 drops) — all lots"),
        "Pathogen": "Delta-9-THC (above acute reference dose)",
        "Reason": ("Delta-9-THC content liable to expose the consumer to a "
                   "dose above the acute reference dose set by the "
                   "authorities."),
        "Class": "Voluntary",
        "Country": "France",
        "Outbreak": 0,
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23562/Interne",
        "Notes": (
            "[manual " + TODAY + ": " + _SWEEP + "Fiche 23562 read on its "
            "own authority page on " + TODAY + "; date de publication "
            "05/10/2026. Read off that page: raison sociale HALFTERMEYER "
            "YVES Les Planteurs Alsaciens, product \"Huile de chanvre CBD "
            "15 % - spectre complet\", flacons compte-gouttes de 10 ml "
            "(200 gouttes), tous les lots (\"les factures disponibles ne "
            "comportent pas de numéro de lot\"). NOT a duplicate of the "
            "published fiches 23592 and 23594: those are the BROAD-spectrum "
            "line (\"spectre large\", THC < 0.057%) recalled 30/09; this is "
            "the FULL-spectrum line, a separate fiche published 05/10. "
            "Four fiches, two product lines, one company.] "
            "[incident:fr:planteurs-alsaciens-thc-full-spectrum-2026-10-05] "
            "[original Reason (fr): \"Teneur en delta-9-THC susceptible "
            "d'exposer le consommateur à une dose supérieure à la dose "
            "aiguë de référence retenue par l'administration.\"] "
            "[original product: Huile de chanvre CBD 15 % - spectre "
            "complet]"),
    },
    {
        "Date": "2026-10-05",
        "Source": "RappelConso (FR)",
        "Company": "HALFTERMEYER YVES Les Planteurs Alsaciens",
        "Brand": "Les Planteurs Alsaciens",
        "Product": ("Hemp oil 5% CBD full spectrum - THC < 0.3%, 10 ml "
                    "dropper bottle (200 drops) — all lots"),
        "Pathogen": "Delta-9-THC (above acute reference dose)",
        "Reason": ("Delta-9-THC content liable to expose the consumer to a "
                   "dose above the acute reference dose set by the "
                   "authorities."),
        "Class": "Voluntary",
        "Country": "France",
        "Outbreak": 0,
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23593/Interne",
        "Notes": (
            "[manual " + TODAY + ": " + _SWEEP + "Fiche 23593 read on its "
            "own authority page on " + TODAY + "; date de publication "
            "05/10/2026. Read off that page: raison sociale HALFTERMEYER "
            "YVES Les Planteurs Alsaciens, product \"Huile de chanvre 5 % "
            "CBD spectre complet - THC < 0,3 %\", flacon compte-gouttes de "
            "10 ml (200 gouttes), tous les lots, risk classified by the "
            "fiche as \"Autres contaminants chimiques\". Sibling of fiche "
            "23562 of the same date; NOT a duplicate of the published "
            "23592/23594, which are the broad-spectrum line — see 23562's "
            "note.] "
            "[incident:fr:planteurs-alsaciens-thc-full-spectrum-2026-10-05] "
            "[original Reason (fr): \"Teneur en delta-9-THC susceptible "
            "d'exposer le consommateur à une dose supérieure à la dose "
            "aiguë de référence retenue par l'administration.\"] "
            "[original product: Huile de chanvre 5 % CBD spectre complet - "
            "THC < 0,3 %]"),
    },
    {
        "Date": "2026-10-05",
        "Source": "CAA (JP)",
        "Company": "Wismettac Foods",
        "Brand": "—",
        "Product": "Fresh mango from Thailand",
        "Pathogen": "Tetraconazole (pesticide residue)",
        "Reason": ("Pesticide tetraconazole detected at 0.03 ppm against a "
                   "limit of 0.01 ppm."),
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": ("https://www.recall.caa.go.jp/result/detail.php"
                "?rcl=00000035915&screenkbn=01"),
        "Notes": (
            "[manual " + TODAY + ": " + _SWEEP + "Read on the CAA notice on "
            + TODAY + ": 事業者名 ウィスメッタックフーズ, 商品名 タイ産生鮮マンゴー, "
            "回収理由 「農薬テトラコナゾール（0.03ppm）を検出（基準値0.01ppm）」, "
            "対応開始日 2026年09月29日, 公表日 2026/10/05 per the CAA food "
            "listing. Date is the CAA publication date, matching the "
            "2026/10/02 batch (rcl 35905-35910) already in Recalls; the "
            "対応開始日 differs and is recorded here. Company takes the firm's "
            "own English name, Wismettac Foods. ELEVEN food notices were "
            "published 2026/10/05 and the register's Japanese coverage "
            "stopped at 2026/10/02; all eleven were opened individually, "
            "three are in scope. In scope under R6 as a pesticide residue "
            "above the limit.] "
            "[original company: ウィスメッタックフーズ] "
            "[original product: タイ産生鮮マンゴー] "
            "[original Reason (ja): \"農薬テトラコナゾール（0.03ppm）を検出"
            "（基準値0.01ppm）\"]"),
    },
    {
        "Date": "2026-10-05",
        "Source": "CAA (JP)",
        "Company": "Kinokuniya",
        "Brand": "—",
        "Product": "Miso peanuts",
        "Pathogen": "Foreign material (pest)",
        "Reason": ("Insect contamination: the Indian meal moth was found in "
                   "the product."),
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": ("https://www.recall.caa.go.jp/result/detail.php"
                "?rcl=00000035916&screenkbn=01"),
        "Notes": (
            "[manual " + TODAY + ": " + _SWEEP + "Read on the CAA notice on "
            + TODAY + ": 事業者名 紀ノ國屋, 商品名 みそ落花生, 回収理由 "
            "「虫（ノシメマダラメイガ）の混入」, 対応開始日 2026年10月01日, 公表日 "
            "2026/10/05 per the CAA food listing. ノシメマダラメイガ is the "
            "Indian meal moth — the species' standard English common name, "
            "and the only thing translated; no scientific name, lot, weight "
            "or best-before is added, because none was printed. Company "
            "takes the firm's own English name, Kinokuniya. Pathogen takes "
            "the register's existing label for this hazard, 'Foreign "
            "material (pest)', already carried by two published rows, so "
            "the register holds one spelling per hazard. In scope under R6 "
            "as pest contamination.] "
            "[original company: 紀ノ國屋] "
            "[original product: みそ落花生] "
            "[original Reason (ja): \"虫（ノシメマダラメイガ）の混入\"]"),
    },
    {
        "Date": "2026-10-05",
        "Source": "CAA (JP)",
        "Company": "Kibi Nosanbutsu Hanbai",
        "Brand": "—",
        "Product": "Tomatoes",
        "Pathogen": "Acephate and methamidophos (pesticide residues)",
        "Reason": ("Pesticides acephate at 0.51 ppm (limit 0.03 ppm) and "
                   "methamidophos at 0.27 ppm (limit 0.02 ppm) detected."),
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": ("https://www.recall.caa.go.jp/result/detail.php"
                "?rcl=00000035924&screenkbn=01"),
        "Notes": (
            "[manual " + TODAY + ": " + _SWEEP + "Read on the CAA notice on "
            + TODAY + ": 事業者名 吉備農産物販売, 商品名 トマト, 回収理由 "
            "「農薬アセフェート 0.51ppm、メタミドホス 0.27ppmを検出（基準値：0.03ppm、"
            "0.02ppm）」, 対応開始日 2026年10月02日, 公表日 2026/10/05 per the CAA "
            "food listing; sold at どんどん広場. 吉備農産物販売 publishes no English "
            "name, so the company is the standard romanisation 'Kibi "
            "Nosanbutsu Hanbai' and is NOT translated — the convention the "
            "2026-10-03 pass used for 平田産業 -> 'Hirata Sangyo'. In scope "
            "under R6 as pesticide residues above the limit.] "
            "[original company: 吉備農産物販売] "
            "[original product: トマト] "
            "[original Reason (ja): \"農薬アセフェート 0.51ppm、メタミドホス "
            "0.27ppmを検出（基準値：0.03ppm、0.02ppm）\"]"),
    },
]


def _rows_of(ws):
    rows = list(ws.values)
    if not rows:
        return [], []
    return [str(h) for h in rows[0]], rows[1:]


def _find(ws, url: str):
    """Row indices (1-based worksheet rows) whose URL matches, case-folded."""
    hdr, _ = _rows_of(ws)
    if "URL" not in hdr:
        return [], hdr
    i = hdr.index("URL") + 1
    out = []
    for r in range(2, ws.max_row + 1):
        v = ws.cell(row=r, column=i).value
        if v and str(v).strip().lower() == url.strip().lower():
            out.append(r)
    return out, hdr


# ── FAULT A1 ──────────────────────────────────────────────────────────────
def canonicalise_fsis_labels(wb) -> int:
    """Only the USDA FSIS variants test_publish_gate.BANNED_SOURCE names."""
    n = 0
    for sheet in ALL_SHEETS:
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        hdr, _ = _rows_of(ws)
        if "Source" not in hdr:
            continue
        i = hdr.index("Source") + 1
        for r in range(2, ws.max_row + 1):
            v = ws.cell(row=r, column=i).value
            if v and str(v).strip().lower() in FSIS_BANNED:
                raw = str(v).strip()
                ws.cell(row=r, column=i).value = FSIS_CANONICAL
                if "Notes" in hdr:
                    j = hdr.index("Notes") + 1
                    prev = str(ws.cell(row=r, column=j).value or "").strip()
                    ws.cell(row=r, column=j).value = (
                        prev + " [label-canon " + TODAY + ": Source " +
                        repr(raw) + " -> " + repr(FSIS_CANONICAL) +
                        ", merge_master.SOURCE_ALIASES. Written verbatim by "
                        "the gap-finder writer, which bypassed "
                        "_write_sheet; fixed at that writer the same day.]"
                    ).strip()
                print(f"  {sheet} row {r}: Source {raw!r} -> "
                      f"{FSIS_CANONICAL!r}")
                n += 1
    return n


# ── FAULT A2 + FAULT B ────────────────────────────────────────────────────
def repair_italian_pending_rows(wb) -> int:
    """Run the real guard for the template, then the two English fixes."""
    from pipeline.merge_master import (
        apply_label_aliases,
        strip_empty_identifier_template,
    )

    n = 0
    for url, fixes in IT_REPAIRS.items():
        for sheet in ("Pending", "Weekly_Review", "Recalls"):
            if sheet not in wb.sheetnames:
                continue
            ws = wb[sheet]
            hits, hdr = _find(ws, url)
            for r in hits:
                row = {h: ws.cell(row=r, column=k + 1).value
                       for k, h in enumerate(hdr)}
                before = dict(row)
                # The two guards the bypassing writer never called.
                apply_label_aliases([row], where=f"repair {sheet} row {r}")
                strip_empty_identifier_template(
                    [row], where=f"repair {sheet} row {r}")
                # The English corrections, which only a reader can make.
                extra_notes = fixes.get("Notes", "")
                for col, val in fixes.items():
                    if col == "Notes":
                        continue
                    if col in hdr:
                        row[col] = val
                if extra_notes and "Notes" in hdr:
                    row["Notes"] = (str(row.get("Notes") or "").strip() +
                                    " " + extra_notes).strip()
                changed = [c for c in hdr if before.get(c) != row.get(c)]
                if not changed:
                    continue
                for k, h in enumerate(hdr):
                    ws.cell(row=r, column=k + 1).value = row.get(h)
                print(f"  {sheet} row {r}: {', '.join(changed)}")
                n += 1
    return n


# ── FAULT D ───────────────────────────────────────────────────────────────
def drop_fsa_catalog_row_from_pending(wb) -> int:
    """Remove the FSA API catalog scrape from the Pending QUEUE only.

    Archived twice already (Rejected 2026-08-26 'not_a_notice — FSA Data
    Catalog', and Weekly_Rejected), and marked rejected in Pending itself.
    No archive row is touched.
    """
    if "Pending" not in wb.sheetnames:
        return 0
    ws = wb["Pending"]
    hits, _ = _find(ws, FSA_CATALOG_URL)
    for r in reversed(hits):
        print(f"  Pending row {r}: removed (FSA linked-data API catalog "
              f"page, not a recall notice; archived in Rejected and "
              f"Weekly_Rejected)")
        ws.delete_rows(r)
    return len(hits)


# ── FAULT C ───────────────────────────────────────────────────────────────
def retire_contradicting_archive_rows() -> int:
    """Run the real sweep, not a hand copy of it."""
    from pipeline.promote_gate_passing import supersede_every_published_url
    return supersede_every_published_url(XLSX)


# ── FAULT E ───────────────────────────────────────────────────────────────
def add_missed_recalls() -> int:
    """Queue the six through tools/add_manual_row.py, as a person would."""
    import openpyxl

    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    seen = set()
    for sheet in wb.sheetnames:
        hdr, body = _rows_of(wb[sheet])
        if "URL" not in hdr:
            continue
        i = hdr.index("URL")
        for r in body:
            if r and i < len(r) and r[i]:
                seen.add(str(r[i]).strip().lower())
    wb.close()

    todo = []
    for row in MISSED_ROWS:
        if row["URL"].strip().lower() in seen:
            print(f"  already present, not added again: {row['URL']}")
            continue
        todo.append(row)
    if not todo:
        return 0

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(todo, fh, ensure_ascii=False, indent=2)
        path = fh.name
    out = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), path],
        cwd=str(ROOT), capture_output=True, text=True)
    print((out.stdout or "").rstrip())
    if out.returncode != 0:
        print("  add_manual_row FAILED:", (out.stderr or "")[-800:])
        return 0
    return len(todo)


def main() -> int:
    import openpyxl
    if not XLSX.exists():
        print("no workbook at", XLSX)
        return 1

    wb = openpyxl.load_workbook(XLSX)
    before = wb["Recalls"].max_row - 1
    before_pending = wb["Pending"].max_row - 1

    print("FAULT A1 — 'FSIS' written verbatim by the bypassing writer")
    n_a1 = canonicalise_fsis_labels(wb)
    print(f"  {n_a1} Source label(s) canonicalised\n")

    print("FAULT A2 + B — two Italian rows: template, label, English")
    n_b = repair_italian_pending_rows(wb)
    print(f"  {n_b} row(s) repaired\n")

    print("FAULT D — the FSA API catalog page sitting in Pending")
    n_d = drop_fsa_catalog_row_from_pending(wb)
    print(f"  {n_d} row(s) removed from Pending\n")

    if n_a1 or n_b or n_d:
        wb.save(XLSX)
    wb.close()

    print("FAULT E — six recalls the pipeline missed in the last 24 h")
    n_e = add_missed_recalls()
    print(f"  {n_e} row(s) queued into Pending\n")

    print("FAULT C — a Polish recall both published and thrown away")
    n_c = retire_contradicting_archive_rows()
    print(f"  {n_c} archive row(s) marked SUPERSEDED\n")

    total = n_a1 + n_b + n_c + n_d + n_e
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
    n = mirror_json_from_xlsx(XLSX, JSON)
    print(f"recalls.json re-mirrored: {n} rows")

    print("ROWS_REMOVED=0")      # nothing is removed from Recalls here
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
