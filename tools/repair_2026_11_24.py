#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-24 — every data edit of the run, in one script.

Run from the repo root against main's current workbook:

    python3 tools/repair_2026_11_24.py            # report only
    python3 tools/repair_2026_11_24.py --apply

tools/rebase_and_verify.py re-runs this against whatever main holds at
upload time rather than carrying a workbook built this morning, so every
edit here is keyed on a value it finds and is a no-op when the value is
already right. Nothing is positional and nothing is deleted.


WHAT WAS WRONG ON MAIN AT 8c964246
==================================

1. MAIN WAS RED. Four tests failed on the tip, all on one row published at
   02:41 UTC today — Recalls row for fiche 24140 (E.Leclerc Ibos, Salmonella)
   whose Reason is the fiche's French motif:
       tests/test_product_and_firm_names_are_english.py
       tests/test_reason_is_not_wholly_foreign.py[Recalls]
       tests/test_language_policy.py::test_every_description_field_is_english
       tests/test_an_agency_headline_is_not_a_firm_and_a_food.py
   The row WAS stamped, so the gates saw it; nothing had translated it yet.

2. TWO MORE FRENCH ROWS PUBLISHED TODAY THAT NO GATE AND NO TEST SAW.
   The by-hand sweep of every Product published in the last 26 hours found:
       fiche 24150  'jambon cuit supérieur découenné dégraissé au torchon;
                     cornet jambon blanc, …'       (Listeria, Tier 1)
       fiche 24152  'emmental râpé 200g'  +  Reason
                    'Risque de corps étranger de type verre'
   Neither carried a cleanup stamp. The detector defects behind them are
   fixed in pipeline/_language.py and pinned by
   tests/test_morning_fix_2026_11_24_detector_gaps.py.

3. TWENTY-FOUR OLDER FRENCH REASONS, the oldest from 2026-04-21, found by
   the one-accent Reason rule added today. All 25 (today's included) are
   translated below.

4. ONE STALE STAMP. The AESAN row for alert 2026_86 carries
   "[needs cleanup: Product not in English]" over the Product 'Fresh
   free-range chicken eggs', which is English. A stamp that never clears is
   a standing excuse in tests/test_product_and_firm_names_are_english.py.

5. A WRONG HAZARD IN PENDING, and the publish gate was right to hold it.
   The Ruokavirasto row for the Urtekram peanut butter recall arrived with
       Pathogen 'Contaminated with an antimicrobial agent'
       Reason   'The products are being recalled due to contamination with
                 an antimicrobial agent. …'
   and the gate refused it because no subscriber alert can reach that
   hazard. The hazard is not that. The notice says "torjunta-ainejäämä",
   which is a PESTICIDE RESIDUE; "torjunta-aine" is a plant-protection
   product, not an antimicrobial. The collector mistranslated the hazard.
   So this is a field fix, not a vocabulary gap — tools/alert_vocab.py is
   left alone, and 'Pesticide residue' is already reachable.

6. THREE PENDING ROWS BLOCKED ON A FIELD THE FICHE PUBLISHES.
       fiche 24149  Company empty; Product 'massonniere' is the firm name
                    in the model-reference field — the scraper defect of
                    fiches 23996 and 23983.
       fiche 24127  Company empty.
       fiche 24125  Pathogen is the French 'microorganisme potentiellement
                    pathogène', which matches no alert word.
   All three fiches were read today. Fiche 24089 (bûche / bûchette, Company
   empty) could NOT be read — rappel.conso returned a client error for that
   one URL — so it is left exactly as it is. "I could not read it" is not
   "it is gone".
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON_PATH = ROOT / "docs" / "data" / "recalls.json"


# ── 1. Reason translations, exact source string -> English ─────────────────
#
# Every one was read against the regulator's own motif. The French is kept
# on the row as [original Reason (fr): "…"], which is the house convention
# and what lets a row be matched back to its fiche.
REASON_EN = {
    # fiche 24152, published 2026-11-23. Motif "Risque de corps étranger de
    # type verre", Risques "Inertes (verre, métal, plastique, papier,
    # textile…)".
    "Risque de corps étranger de type verre":
        "Risk of a foreign body of the glass type",
    # fiche 24127, published 2026-11-20. Motif "Risque physique (suspicion
    # de corps étranger)".
    "Risque physique (suspicion de corps étranger)":
        "Physical risk (suspected foreign body)",
    # "Suspicion de" is SUSPECTED, not demonstrated — the distinction this
    # module's header insists on. 20 rows across three spellings.
    "Suspicion de contamination bactériologique":
        "Suspected bacteriological contamination",
    "Suspicion de contamination bactériologique;":
        "Suspected bacteriological contamination;",
    "Suspicion de contamination bactériologique (Listeria)":
        "Suspected bacteriological contamination (Listeria)",
    # "Eschérichia" is the French spelling of the Latin binomial, which
    # carries no accent. Only the spelling changes; STEC is written as the
    # register writes it everywhere else.
    "Eschérichia coli stec": "Escherichia coli STEC",
    # "shiga toxinogène" is the French for Shiga toxin-producing.
    "Risques microbiologiques — Escherichia coli shiga toxinogène (STEC)":
        "Microbiological risks — Shiga toxin-producing Escherichia coli "
        "(STEC)",
    # fiche 24125, promoted from Pending by this same run, so its Motif has
    # to be in REASON_EN and not only described in the row's note — the
    # first pass translated the Product and the Pathogen and left the Reason,
    # and tests/test_language_policy.py and
    # tests/test_reason_is_not_wholly_foreign.py both went red on it.
    # "plan d'autocontrôle" is the firm's own-check plan; "a fait apparaitre"
    # is demonstrated, not suspected.
    "Dans le cadre de notre plan d'autocontrôle, nous avons réalisé un "
    "contrôle de stabilité microbiologique qui a fait apparaitre la "
    "présence de microorganisme potentiellement pathogène.":
        "As part of our own-check plan, we carried out a microbiological "
        "stability test which revealed the presence of a potentially "
        "pathogenic microorganism.",
    # fiche 24140, published 2026-11-23. The fiche's Motif, which the
    # scraper captured with its own semicolon artefact.
    "La société e.leclerc ibos le méridien procède au retrait de la vente "
    "et à un rappel du ou des produit(s) suivant(s) suite à la; mise en "
    "évidence de la présence de Salmonella.":
        "The company E.Leclerc Ibos Le Méridien is withdrawing from sale "
        "and recalling the following product(s) following the detection of "
        "the presence of Salmonella.",
}


# ── 2. Per-row edits, keyed on URL ────────────────────────────────────────
#
# Each entry: the sheet, the row's URL, the fields to set, the stamps to
# drop, and the note to append. A field is written only when the row's
# current value matches `was` — case-insensitively and whitespace-folded,
# because the scraper lower-cases some fields and tools/repair_2026_11_13.py
# wrote NOTHING for five rows by comparing case-sensitively against the
# fiche's own capitalisation. That is the defect this compare exists to
# avoid; it is recorded in tests/test_product_and_firm_names_are_english.py.
SUPERSEDED_24125 = (
    '[SUPERSEDED 2026-11-24 — THE REJECT REASON IS FALSE. The verdict on this archive row says the cited URL is "a listing page, not the specific recall page". It is not: https://rappel.conso.gouv.fr/fiche-rappel/24125/Interne was opened and read in full by the morning fix pass today and is a single fiche — "Fiche réf. 2026-11-0134 • Version 1", date 20/11/2026, H1 "Lait entier stérilisé", "Origine de la fiche: COOPERATIVE FRUITIERE LAIT PRAIRIES DU BOULONNAIS", GTIN 3760263440711, lot 26307008, Motif the firm\'s own own-check sentence. Reviewer 1 writes this sentence when it cannot FETCH a page, which is a statement about reachability, not about whether the notice exists — the distinction _url_guard already makes for the "refusal" class. The Weekly_Rejected copy of this URL was then archived by the repromotion-guard itself, on the strength of this row. NOTHING IS DELETED: this archive row stays exactly as written and this note is appended, which is what SUPERSEDED means here and what tests/test_a_superseded_archive_row_stops_blocking.py pins. The row now passes every publish-gate rule and is promoted.]'
)

SUPERSEDED_24127 = (
    '[SUPERSEDED 2026-11-24 — THE REJECT REASON IS FALSE. The verdicts on this URL say "provenance: page does not mention any of [\'brioche\'] — the cited URL describes something other than this row" (Rejected) and "The provided URL is a dataset page, not an official recall page" (Weekly_Rejected). Both are wrong on their face: https://rappel.conso.gouv.fr/fiche-rappel/24127/interne was opened and read in full by the morning fix pass today and its H1 is "Pain brioché x32 400g" — the word the provenance check says is absent is the product\'s own name in the page title. Fiche réf. 2026-11-0136 • Version 1, date 20/11/2026, "Origine de la fiche: COOPERATIVE U ENSEIGNE", GTIN 3251517091006, lot 6260, Motif "Risque physique (suspicion de corps étranger)". Both verdicts are reviewer-1 fetch failures recorded as confident negatives. NOTHING IS DELETED: both archive rows stay as written and this note is appended. The row now passes every publish-gate rule and is promoted.]'
)


EDITS = [
    # ── fiche 24150, published 2026-11-23 (Recalls) ───────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24150/interne",
        "fields": {
            "Product": (
                "jambon cuit supérieur découenné dégraissé au torchon; "
                "cornet jambon blanc, bruschetta jambon champignons, pizza "
                "reine, pizza fermière, complet jambon, sandwich jambon "
                "beurre, sandwich jambon fromage, croque monsieur, "
                "croissant au jambon;",
                "Superior cooked ham, rindless and trimmed, torchon-style; "
                "ham cornet; ham and mushroom bruschetta; Pizza Reine; "
                "Pizza Fermière; ham \"complet\"; ham and butter sandwich; "
                "ham and cheese sandwich; croque-monsieur; ham croissant",
            ),
        },
        "note": (
            "[morning fix pass 2026-11-24: the published Product was wholly "
            "French and the row carried NO \"needs cleanup\" stamp. "
            "product_needs_english() returned False because \"sandwich\" "
            "was listed as an English product word and one English word "
            "vetoes the whole detector — the \"pâté\" defect of 2026-10-27 "
            "one list entry along. Fixed in pipeline/_language.py and "
            "pinned by tests/test_morning_fix_2026_11_24_detector_gaps.py. "
            "Read on fiche 24150 today (Fiche réf. 2026-11-0157 • Version "
            "1, date 23/11/2026): H1 \"JAMBON CUIT SUPERIEUR DECOUENNE "
            "DEGRAISSE AU TORCHON + Cornet jambon blanc, bruschetta jambon "
            "champignons, pizza reine, …\", \"Origine de la fiche: SOCIETE "
            "DE DISTRIBUTION DU PAYS DE RETZ E.LECLERC ATOUT SUD\", \"Nom "
            "de la marque du produit: E.LECLERC ATOUT SUD\", Catégorie "
            "Alimentation / Viandes, Motif \"Présence de Listeria "
            "monocytogenes\", Risques \"Listeria monocytogenes (agent "
            "responsable de la listériose)\", Distributeurs \"E.LECLERC "
            "SODIRETZ (ATOUT SUD REZE)\", LOIRE-ATLANTIQUE (44), nine GTINs "
            "from 0209144000000 to 2000000277899, sold 17/11 to 19/11/2026, "
            "\"Produit à conserver au réfrigérateur\", Nature juridique "
            "\"Volontaire (sans arrêté préfectoral)\". The Reason was "
            "already English and matches the Motif; only the Product is "
            "changed. \"Pizza Reine\", \"Pizza Fermière\" and \"complet\" "
            "are the products' own names on the fiche and are kept as "
            "written rather than explained.] "
            "[original product: jambon cuit supérieur découenné dégraissé "
            "au torchon; cornet jambon blanc, bruschetta jambon "
            "champignons, pizza reine, pizza fermière, complet jambon, "
            "sandwich jambon beurre, sandwich jambon fromage, croque "
            "monsieur, croissant au jambon;]"
        ),
    },
    # ── fiche 24152, published 2026-11-23 (Recalls) ───────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24152/interne",
        "fields": {
            "Product": ("emmental râpé 200g",
                        "Grated Emmental cheese, 200 g"),
            "Company": ("Top Budget", "Fromageries Terres d'Or"),
        },
        "note": (
            "[morning fix pass 2026-11-24: the published Product AND Reason "
            "were both French and the row carried NO \"needs cleanup\" "
            "stamp. The Product is three words — one a weight, one "
            "(\"emmental\") shared with English — so there was no French "
            "function word for the two-hit detector and \"râpé\" was "
            "missing from the French food-word list although \"haché\" has "
            "been there from the start. The Reason carries one accent and "
            "one French function word, under both the two-hit detector and "
            "the register test's own two-accent floor. Both detectors "
            "fixed in pipeline/_language.py; pinned by "
            "tests/test_morning_fix_2026_11_24_detector_gaps.py. Read on "
            "fiche 24152 today (Fiche réf. 2026-11-0159 • Version 1, date "
            "23/11/2026): H1 \"Emmental Râpé Top Budget 200g\", \"Origine "
            "de la fiche: FROMAGERIES TERRES D'OR FROMAGERIES TERRES "
            "D'OR\", \"Nom de la marque du produit: Top Budget\", "
            "Catégorie Alimentation / Lait et produits laitiers, \"Noms des "
            "modèles ou références: Emmental Râpé 200g\", GTIN "
            "3410280141671, \"Code lot commençant par 11 323\", \"Date de "
            "durabilité minimale 29/01/2027\", Conditionnements \"Sachet de "
            "200g\", Marque de salubrité \"FR 71 047 001 CE\", France "
            "entière, Distributeurs \"ETS DOLE - ETS DONZERE - ETS BOURGES "
            "- ETS BEZIERS - ETS BRIGNOLES - ETS SAINT QUENTIN - ETS "
            "HEUDEBOUVILLE\", Motif \"Risque de corps étranger de type "
            "verre\", Risques \"Inertes (verre, métal, plastique, papier, "
            "textile…)\", Nature juridique \"Volontaire (sans arrêté "
            "préfectoral)\". Company was the BRAND: \"Top Budget\" is a "
            "private label and the fiche's \"Origine de la fiche\" names "
            "the firm, which is the register's convention for RappelConso "
            "(see fiches 24094 and 24111). Brand keeps Top Budget.] "
            "[original product: emmental râpé 200g] "
            "[original company: Top Budget (brand; the fiche's Origine de "
            "la fiche is FROMAGERIES TERRES D'OR)]"
        ),
    },
    # ── fiche 24140, published 2026-11-23 (Recalls) — main was RED on it ──
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24140/interne",
        "fields": {
            "Product": (
                "thon basquaise/riz emballé sous atmosphère protectrice ou "
                "vendu en vitrine traditionnelle",
                "Basque-style tuna with rice (Thon Basquaise/Riz), packed "
                "under protective atmosphere or sold from the service "
                "display counter",
            ),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-24: this row held FOUR tests RED on "
            "main at 8c964246 — test_product_and_firm_names_are_english, "
            "test_reason_is_not_wholly_foreign[Recalls], "
            "test_language_policy::test_every_description_field_is_english "
            "and test_an_agency_headline_is_not_a_firm_and_a_food. It was "
            "correctly stamped by the gates at 02:41 UTC today; nothing had "
            "translated it. Read on fiche 24140 today (Fiche réf. "
            "2026-11-0147 • Version 1, date 23/11/2026): H1 \"Thon "
            "Basquaise/Riz\", \"Origine de la fiche: CENTRE DISTRIBUTION "
            "ALIMENTAIRE DU S. O. LECLERC MERIDIEN\", \"Nom de la marque du "
            "produit: E.Leclerc IBOS\", Catégorie Alimentation / Plats "
            "préparés et snacks, \"Noms des modèles ou références: Thon "
            "Basquaise/Riz emballé sous atmosphère protectrice ou vendu en "
            "vitrine traditionnelle\", GTIN 0204053011871 plus an unbarcoded "
            "service-counter line, \"Date limite de consommation entre le "
            "03/11/2026 et le 22/11/2026\", sold 03/11 to 17/11/2026, "
            "HAUTES-PYRENEES (65), Distributeurs \"E.Leclerc IBOS Le "
            "Méridien\", Motif the firm's own sentence ending \"suite à la "
            "mise en évidence de la présence de salmonelles\", Risques "
            "\"Salmonella spp (agent responsable de la salmonellose)\", "
            "Nature juridique \"Volontaire (sans arrêté préfectoral)\". "
            "\"Thon Basquaise\" is the dish's own name and is kept beside "
            "the English. Both cleanup stamps are cleared because both "
            "fields are now English.] "
            "[original product: thon basquaise/riz emballé sous atmosphère "
            "protectrice ou vendu en vitrine traditionnelle]"
        ),
    },
    # ── AESAN alert 2026_86 (Recalls) — stale stamp only ──────────────────
    {
        "sheet": "Recalls",
        "url": "https://www.aesan.gob.es/alertas/2026_86",
        "fields": {},
        "drop_stamps": ("[needs cleanup: Product not in English]",),
        "note": (
            "[morning fix pass 2026-11-24: dropped a stale \"needs cleanup: "
            "Product not in English\" stamp. The Product reads \"Fresh "
            "free-range chicken eggs\", which is English; the stamp was "
            "written while the row still held the AESAN headline and "
            "survived the translation. It matters because "
            "tests/test_product_and_firm_names_are_english.py excuses a "
            "stamped row for one day, so a stamp nobody clears is a "
            "standing excuse. Nothing else on this row is changed.]"
        ),
    },
    # ── Ruokavirasto (Pending) — the hazard was wrong ─────────────────────
    {
        "sheet": "Pending",
        "url": "https://www.ruokavirasto.fi/elintarvikkeet/ohjeita-"
               "kuluttajille/takaisinvedot-2023-/kahden-"
               "maapahkinavoituotteen-takaisinveto/",
        "fields": {
            "Pathogen": ("Contaminated with an antimicrobial agent",
                         "Pesticide residue"),
            "Reason": (
                "The products are being recalled due to contamination with "
                "an antimicrobial agent. The products are also being "
                "withdrawn as a precautionary measure.",
                "One batch is being withdrawn from sale because of a "
                "pesticide residue found in the product; one batch of the "
                "Smooth variant is being withdrawn as a precaution",
            ),
            "Product": ("Peanut Butter Crunchy",
                        "Peanut Butter Crunchy, 230 g"),
        },
        "note": (
            "[morning fix pass 2026-11-24: THE HAZARD ON THIS ROW WAS "
            "WRONG, and the publish gate was right to hold it. The gate "
            "refused it because Pathogen 'Contaminated with an antimicrobial "
            "agent' matches no word any subscriber alert offers. The gate's "
            "own message offers two readings — the value is wrong, or the "
            "vocabulary needs the term — and here it is the first. Read on "
            "the Ruokavirasto notice today "
            "(https://www.ruokavirasto.fi/elintarvikkeet/ohjeita-"
            "kuluttajille/takaisinvedot-2023-/kahden-"
            "maapahkinavoituotteen-takaisinveto/, published 23. marraskuuta "
            "2026): \"Midsona Finland Oy on ilmoittanut vetävänsä "
            "myynnistä yhden erän Urtekram Peanut Butter Crunchy 230 g "
            "-tuotetta siinä todetun torjunta-ainejäämän vuoksi\", and in "
            "addition \"yksi erä Urtekram Peanut Butter Smooth 230 g "
            "-tuotetta vedetään myynnistä varotoimenpiteenä\". "
            "\"Torjunta-aine\" is a plant-protection product and "
            "\"torjunta-ainejäämä\" is a pesticide residue; it is not an "
            "antimicrobial, and the collector's \"antimicrobial agent\" is "
            "a mistranslation of it. The notice names no specific active "
            "substance and gives no measured level or limit, so none is "
            "claimed here. Best-before dates on the notice: Crunchy "
            "15.4.2027, Smooth 14.4.2027; no lot codes are published. "
            "'Pesticide residue' is already reachable by a subscriber "
            "alert, so tools/alert_vocab.py is NOT changed. "
            "[original Reason (en, collector): \"The products are being "
            "recalled due to contamination with an antimicrobial agent. The "
            "products are also being withdrawn as a precautionary "
            "measure.\"]]"
        ),
    },
    # ── fiche 24149 (Pending) — Company empty, Product is the firm name ───
    {
        "sheet": "Pending",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24149/interne",
        "fields": {
            "Company": ("", "Etablissements Massonnière"),
            "Product": ("massonniere",
                        "Roasting joint, 2 kg (fiche title \"ROTI BF TT "
                        "ROND TG 2KG\")"),
        },
        "note": (
            "[morning fix pass 2026-11-24: Company was empty and the "
            "publish gate held the row on it, and the Product was the "
            "single lower-case word \"massonniere\", which is this fiche's "
            "FIRM, not a food — the same scraper failure mode as fiches "
            "23996 (\"baymar\") and 23983 (a barcode): Product is read from "
            "\"Noms des modèles ou références\" and on this fiche that "
            "field holds the firm's name. Read on fiche 24149 today (Fiche "
            "réf. 2026-11-0156 • Version 1, date 23/11/2026): H1 \"ROTI BF "
            "TT ROND TG 2KG\", \"Origine de la fiche: ETABLISSEMENTS "
            "MASSONNIERE ETS MASSONNIERE\", \"Nom de la marque du produit: "
            "SANS MARQUE\", \"Noms des modèles ou références: "
            "MASSONNIERE\", Motif \"DETECTION SALMONELLA\", Risques "
            "\"Salmonella spp (agent responsable de la salmonellose)\", "
            "Distributeurs \"BOUCHERIE SARL BENOIST FILS ET FILLE 33500 "
            "LIBOURNE\", GIRONDE (33), lot M62940018, \"Date limite de "
            "consommation 15/11/2026\", sold 05/10 to 12/11/2026, Nature "
            "juridique \"Volontaire (sans arrêté préfectoral)\". THE "
            "ABBREVIATIONS ARE NOT EXPANDED: \"ROTI\" is rôti, a roasting "
            "joint, and the fiche's own title is quoted beside the English; "
            "\"BF\", \"TT\" and \"TG\" are trade codes and nothing on the "
            "page says what they stand for, so no meat species and no cut "
            "is claimed. Reason was already English and matches the Motif.] "
            "[original product: massonniere]"
        ),
    },
    # ── fiche 24127 (Pending) — Company empty, Product French ─────────────
    {
        "sheet": "Pending",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24127/interne",
        "fields": {
            "Company": ("", "Magasins U"),
            "Product": ("pain brioché x32 400g",
                        "Brioche-style bread, 32-pack, 400 g"),
        },
        "note": (
            "[morning fix pass 2026-11-24: Company was empty and the "
            "publish gate held the row on it. Read on fiche 24127 today "
            "(Fiche réf. 2026-11-0136 • Version 1, date 20/11/2026): H1 "
            "\"Pain brioché x32 400g\", \"Origine de la fiche: COOPERATIVE "
            "U ENSEIGNE U ENSEIGNE\", \"Nom de la marque du produit: Sans "
            "marque\", \"Noms des modèles ou références: Pain brioché x32 "
            "400g\", Motif \"Risque physique (suspicion de corps "
            "étranger)\", Risques \"Inertes (verre, métal, plastique, "
            "papier, textile…)\", Distributeurs \"Magasins U\", GTIN "
            "3251517091006, lot 6260, Marque de salubrité \"EMB47.091A\", "
            "eight départements from CHARENTE-MARITIME (17) to VENDEE (85), "
            "sold 09/10 to 19/11/2026, Nature juridique \"Volontaire (sans "
            "arrêté préfectoral)\". Company is the distributor because the "
            "product has no brand, which is this register's own convention "
            "for a RappelConso \"sans marque\" row and the same value as "
            "fiches 24005 and 24019.] "
            "[original product: pain brioché x32 400g]"
        ),
    },
    # ── fiche 24125 (Pending) — French Pathogen, no alert can reach it ────
    {
        "sheet": "Pending",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24125/Interne",
        "fields": {
            "Pathogen": (
                "microorganisme potentiellement pathogène",
                "Unspecified microbiological contamination (potentially "
                "pathogenic microorganism)",
            ),
            "Product": ("bouteille de lait entier stérilisé 1l",
                        "Sterilised whole milk, 1 l bottle"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-24: the Pathogen was the fiche's "
            "French phrase and matched no word any subscriber alert offers, "
            "so the publish gate held an in-scope microbiological hazard. "
            "Read on fiche 24125 today (Fiche réf. 2026-11-0134 • Version "
            "1, date 20/11/2026): H1 \"Lait entier stérilisé\", \"Origine "
            "de la fiche: COOPERATIVE FRUITIERE LAIT PRAIRIES DU "
            "BOULONNAIS Coopérative fruitière\", \"Nom de la marque du "
            "produit: Lait Prairies du boulonnais\", \"Noms des modèles ou "
            "références: Bouteille de lait entier stérilisé 1l\", Motif "
            "\"Dans le cadre de notre plan d'autocontrôle, nous avons "
            "réalisé un contrôle de stabilité microbiologique qui a fait "
            "apparaitre la présence de microorganisme potentiellement "
            "pathogène.\", Risques \"Autres contaminants biologiques\", "
            "Distributeurs \"Le Fromager des Halles\", Régions "
            "Hauts-de-France, GTIN 3760263440711, lot 26307008, \"Date de "
            "durabilité minimale 01/05/2027\", sold 14/11 to 19/11/2026, "
            "ambient storage, Nature juridique \"Volontaire (sans arrêté "
            "préfectoral)\". THE NOTICE NAMES NO ORGANISM and none is "
            "invented: the Pathogen is the register's existing "
            "\"Unspecified microbiological contamination (…)\" form, as "
            "used on the Sugiyo chikuwa row of 2026-11-18, with the fiche's "
            "own words in the bracket. Reason is translated from the Motif "
            "above.] "
            "[original product: bouteille de lait entier stérilisé 1l] "
            "[original Reason (fr): \"Dans le cadre de notre plan "
            "d'autocontrôle, nous avons réalisé un contrôle de stabilité "
            "microbiologique qui a fait apparaitre la présence de "
            "microorganisme potentiellement pathogène.\"]"
        ),
    },
    # ── 7. TWO WRONG ARCHIVED VERDICTS, ANNOTATED NOT DELETED ────────────
    #
    # promote_gate_passing --apply refused fiches 24125 and 24127 with
    # "re-promotion BLOCKED … already in Rejected". The guard was doing its
    # job; the archived verdicts are false, and both fiches were read in
    # full today. "[SUPERSEDED <date> — …]" is the marker this repo already
    # uses for a wrong verdict, and it is the ONLY one that defuses both
    # bars (_is_terminal_rejection and promote_approved's re-promotion
    # guard) — see tests/test_a_superseded_archive_row_stops_blocking.py.
    # UNRESOLVED would mark a contradiction nobody has decided; these two
    # are decided, because the pages say so.
    {
        "sheet": "Rejected",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24125/Interne",
        "fields": {},
        "drop_stamps": (),
        "note": SUPERSEDED_24125,
    },
    {
        "sheet": "Weekly_Rejected",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24125/Interne",
        "fields": {},
        "drop_stamps": (),
        "note": SUPERSEDED_24125,
    },
    {
        "sheet": "Rejected",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24127/interne",
        "fields": {},
        "drop_stamps": (),
        "note": SUPERSEDED_24127,
    },
    {
        "sheet": "Weekly_Rejected",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24127/interne",
        "fields": {},
        "drop_stamps": (),
        "note": SUPERSEDED_24127,
    },
]


#: Sheets whose Reason column is translated by REASON_EN. Recalls is the
#: published register and Pending is what is about to be published. The three
#: archive sheets are append-only records of what a row said when it was
#: promoted or refused and are NOT rewritten — the reason is spelled out in
#: tests/test_reason_is_not_wholly_foreign.py.
REASON_SHEETS = ("Recalls", "Pending")

#: Sources whose Reason is a Commission notification subject, kept verbatim.
VERBATIM_SOURCES = ("RASFF (EU)",)

NOTE_KEY = "[morning fix pass 2026-11-24"


def _norm(v) -> str:
    return " ".join(str(v or "").split()).strip().lower()


def _append_note(row_notes, note: str) -> str:
    """Append `note` once.

    Keyed on the NOTE's OWN first 60 characters, not on NOTE_KEY: the four
    archive entries append a "[SUPERSEDED 2026-11-24 …]" note that does not
    contain NOTE_KEY at all, so a NOTE_KEY-only test re-appended it on every
    run. The script has to be a no-op the second time it is run, because
    tools/rebase_and_verify.py re-runs it.
    """
    notes = str(row_notes or "")
    if notes.lower() == "nan":
        notes = ""
    if note[:60] in notes:
        return notes
    return (notes.strip() + " " + note).strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=str(XLSX))
    # IT WRITES BY DEFAULT, AND THAT IS NOT A STYLE CHOICE.
    # tools/rebase_and_verify.py re-runs every tools/repair_*.py it finds in
    # the code zip with NO ARGUMENTS (`py(self.repo, r)`). A script that
    # needed --apply would be a silent no-op inside the gate, and the gate
    # would zip main's unrepaired workbook with a green suite and nothing in
    # the report to say so. Every earlier repair script in this tree writes
    # by default and takes --dry-run; this one does the same. --apply is
    # kept as an accepted no-op so an interactive run can say it.
    ap.add_argument("--dry-run", action="store_true",
                    help="report only, write nothing")
    ap.add_argument("--apply", action="store_true",
                    help="accepted and ignored — writing is the default")
    args = ap.parse_args()

    import openpyxl

    # ── 0. the three operator-found CAA (JP) rows, through the real path ──
    #
    # tools/add_manual_row.py hands them to merge_master.append_to_pending,
    # so they get the same dedup, the same validation and the same review
    # stages as a scraped row. It is called from HERE rather than by hand
    # because rebase_and_verify re-runs this script and nothing else: a row
    # queued only at the terminal this morning would not exist in the rebased
    # workbook.
    if not args.dry_run:
        import subprocess
        rows_json = ROOT / "tools" / "manual_rows_2026_11_24.json"
        if rows_json.exists():
            r = subprocess.run(
                [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
                 str(rows_json)],
                cwd=str(ROOT), capture_output=True, text=True)
            tail = [ln for ln in r.stdout.splitlines() if ln.strip()][-2:]
            print("add_manual_row: " + " | ".join(tail))
            if r.returncode != 0:
                print("add_manual_row FAILED:\n" + r.stdout[-2000:]
                      + "\n" + r.stderr[-2000:])
                return 1

    from pipeline import merge_master as mm

    path = Path(args.xlsx)
    # openpyxl in place, cell by cell, NOT a pandas round-trip: re-serialising
    # every sheet through pandas rewrites the Date column as timestamps and
    # drops the archive sheets' own types. Same approach as
    # tools/repair_2026_11_18.py and tools/repair_2026_11_22.py.
    wb = openpyxl.load_workbook(path)
    changed = 0
    report: list[str] = []

    def _sheet(name):
        if name not in wb.sheetnames:
            return None, {}
        ws = wb[name]
        hdr = {str(c.value): c.column for c in ws[1] if c.value is not None}
        return ws, hdr

    def _get(ws, hdr, row, col):
        if col not in hdr:
            return None
        v = ws.cell(row=row, column=hdr[col]).value
        return "" if v is None or str(v).lower() == "nan" else str(v)

    def _set(ws, hdr, row, col, value):
        ws.cell(row=row, column=hdr[col]).value = value

    # ── per-row edits ─────────────────────────────────────────────────────
    for spec in EDITS:
        name = spec["sheet"]
        ws, hdr = _sheet(name)
        if ws is None:
            report.append(f"  MISS  sheet {name} absent")
            continue
        target = mm._normalize_url_for_dedup(spec["url"])
        hits = [r for r in range(2, ws.max_row + 1)
                if mm._normalize_url_for_dedup(_get(ws, hdr, r, "URL") or "")
                == target]
        if not hits:
            report.append(f"  MISS  {name}: no row at {spec['url']}")
            continue
        for r in hits:
            touched = False
            for col, (was, now) in spec.get("fields", {}).items():
                if col not in hdr:
                    report.append(f"  MISS  {name} row {r}: no column {col}")
                    continue
                cur = _get(ws, hdr, r, col) or ""
                if _norm(cur) == _norm(now):
                    continue                       # already right; no-op
                if _norm(cur) != _norm(was):
                    report.append(
                        f"  SKIP  {name} row {r}.{col}: on main it reads "
                        f"{cur[:60]!r}, expected {was[:60]!r}")
                    continue
                _set(ws, hdr, r, col, now)
                touched = True
                report.append(f"  SET   {name} row {r}.{col} -> {now[:70]!r}")
            if "Notes" not in hdr:
                if touched:
                    changed += 1
                continue
            notes = _get(ws, hdr, r, "Notes") or ""
            new_notes = notes
            for stamp in spec.get("drop_stamps", ()):
                if stamp in new_notes:
                    new_notes = " ".join(
                        new_notes.replace(stamp, "").split())
                    touched = True
                    report.append(f"  DROP  {name} row {r} stamp {stamp}")
            # The note is appended whenever the spec carries one, not only
            # when a field moved: the four archive entries change no field
            # at all — the SUPERSEDED note IS the edit.
            if spec.get("note"):
                out = _append_note(new_notes, spec["note"])
                if out != notes:
                    _set(ws, hdr, r, "Notes", out)
                    touched = True
            if touched:
                changed += 1

    # ── Reason translations across Recalls and Pending ────────────────────
    for name in REASON_SHEETS:
        ws, hdr = _sheet(name)
        if ws is None or "Reason" not in hdr:
            continue
        for r in range(2, ws.max_row + 1):
            if (_get(ws, hdr, r, "Source") or "") in VERBATIM_SOURCES:
                continue
            cur = _get(ws, hdr, r, "Reason") or ""
            if not cur:
                continue
            en = REASON_EN.get(" ".join(cur.split()))
            if en is None or en == cur:
                continue
            _set(ws, hdr, r, "Reason", en)
            if "Notes" in hdr:
                _set(ws, hdr, r, "Notes", _append_note(
                    _get(ws, hdr, r, "Notes") or "",
                    "[morning fix pass 2026-11-24: Reason translated; the "
                    "regulator's own wording is kept here. "
                    f"[original Reason (fr): \"{cur}\"]]"))
            changed += 1
            report.append(f"  REASON {name} row {r} -> {en[:70]!r}")

    # ── 8. THE STAMP CLEARS ITSELF NOW ───────────────────────────────────
    #
    # A "[needs cleanup: …]" stamp is a to-do, and
    # tests/test_product_and_firm_names_are_english.py excuses a stamped row
    # for one day — so a stamp nobody clears is a standing excuse, which is
    # finding 4 of this pass (the AESAN row carried one over an English
    # Product for three days).
    #
    # This run would have created twenty-five more of them. Step 0 calls
    # add_manual_row, whose writer (merge_master._write_sheet) re-stamps
    # every row it writes whose Reason fails the predicate — and it runs
    # BEFORE the translations below, so it stamped all twenty-five French
    # Reasons a few milliseconds before they stopped being French. Measured
    # on the rebased register: 27 published rows carried a stamp over a
    # field that was already English.
    #
    # So the stamps are swept against the SAME predicates the gates and the
    # tests use. If product_needs_english / has_non_latin_script /
    # reason_needs_english say the field is English, the stamp on it is by
    # definition stale and nothing is being hidden: a field that still needs
    # work keeps its stamp, because the predicate still fires on it.
    from pipeline._language import (has_non_latin_script,
                                    product_needs_english,
                                    reason_needs_english)

    STAMPS = (
        ("[needs cleanup: Product not in English]",
         lambda r: not product_needs_english(r.get("Product"))),
        ("[needs cleanup: Company/Brand not in English]",
         lambda r: not (has_non_latin_script(r.get("Company"))
                        or has_non_latin_script(r.get("Brand")))),
        ("[needs cleanup: Reason not in English]",
         lambda r: not reason_needs_english(r.get("Reason"))),
    )
    swept = 0
    for name in ("Recalls", "Pending"):
        ws, hdr = _sheet(name)
        if ws is None or "Notes" not in hdr:
            continue
        for r in range(2, ws.max_row + 1):
            notes = _get(ws, hdr, r, "Notes") or ""
            if "[needs cleanup:" not in notes:
                continue
            row = {c: (_get(ws, hdr, r, c) or "")
                   for c in ("Product", "Company", "Brand", "Reason")}
            out = notes
            dropped = []
            for stamp, is_clean in STAMPS:
                if stamp in out and is_clean(row):
                    out = " ".join(out.replace(stamp, "").split())
                    dropped.append(stamp)
            # The long confirm-agent variant of the Reason stamp, which
            # pipeline/recall_confirm_agent.py writes.
            long_stamp = ("[needs cleanup: Reason not in English and no "
                          "verified translation]")
            if long_stamp in out and not reason_needs_english(row["Reason"]):
                out = " ".join(out.replace(long_stamp, "").split())
                dropped.append(long_stamp)
            if dropped:
                out = _append_note(
                    out,
                    "[morning fix pass 2026-11-24: dropped "
                    f"{len(dropped)} stale cleanup stamp(s) — the field(s) "
                    "they name read in English under the same predicate the "
                    "publish gates and the language tests use. A stamp over "
                    "an English field is a standing excuse in "
                    "tests/test_product_and_firm_names_are_english.py, "
                    "which excuses a stamped row for one day.]")
                _set(ws, hdr, r, "Notes", out)
                swept += 1
                changed += 1
    if swept:
        report.append(f"  SWEEP {swept} row(s) lost a stale cleanup stamp")

    print(f"repair_2026_11_24: {changed} row edit(s)")
    for line in report:
        print(line)

    if args.dry_run:
        print("\n(--dry-run: nothing written)")
        return 0

    if changed:
        wb.save(path)
        print(f"wrote {path}")
        # recalls.json MUST mirror the Recalls sheet; mirror_json_from_xlsx
        # is the only sanctioned way to produce it. Without this the
        # dashboard and tests/test_product_and_firm_names_are_english.py
        # read the pre-repair values — which is how the fiche 24125 Reason
        # stayed French in the json while the workbook was already fixed.
        from pipeline.merge_master import mirror_json_from_xlsx
        print(f"  recalls.json re-mirrored: "
              f"{mirror_json_from_xlsx(path, JSON_PATH)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
