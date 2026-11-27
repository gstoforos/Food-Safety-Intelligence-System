# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-27 — data repairs, scripted so the gate can re-run them.

Run with no arguments. tools/rebase_and_verify.py re-runs every
tools/repair_*.py it finds in the code zip WITH NO ARGUMENTS against main's
current workbook, so this script writes by default and is a no-op the second
time it runs.

WHAT WAS WRONG ON MAIN AT 1f2e319d
──────────────────────────────────
FINDING 1 — FOUR PUBLISHED PRODUCTS IN FRENCH, AND THE SUITE WAS RED.
Four RappelConso rows promoted on 2026-11-26 carried the fiche's French
`noms des modèles ou références` as the published Product, and one carried a
half-translated Reason. Four tests were RED on main:

    tests/test_an_agency_headline_is_not_a_firm_and_a_food.py
        ::TestTheLanguageDetectorSeesGerman
        ::test_no_published_product_is_left_in_german
    tests/test_language_policy.py::TestPublishedWorkbook
        ::test_every_description_field_is_english
    tests/test_product_and_firm_names_are_english.py
        ::test_no_published_product_reads_in_another_language
    tests/test_reason_is_not_wholly_foreign.py
        ::test_no_published_reason_is_wholly_foreign[Recalls]

The rows carried their `[needs cleanup: …]` stamps correctly — the offline
promoter has no model, so stamping and leaving the translation to this pass
is the designed behaviour. Nothing is broken upstream. Every fiche below was
read today and the French is kept on the row.

Two Reasons were ALSO French and NO test caught them: fiche 24191's
"Presence of Listeria monocytogène" and fiches 24182/24181's "Presence of
listéria". detect_language needs two hits and these sentences are one French
word inside English, which is exactly the threshold that protects an English
sentence quoting a foreign name. They are translated here anyway, because
"published Reason must be English" is the rule and a test that cannot see a
one-word residue is not a licence to leave it.

FINDING 2 — A BRAND WITH A DOUBLE SPACE THE FICHE DOES NOT PRINT.
Fiche 24191's Company and Brand both read "Distralis - Bergers du Haut
 Béarn" with two spaces before "Béarn". The fiche prints one. A brand name is
to be kept as written, so this is a fidelity defect, not cosmetics: the
dashboard searches Brand and "Haut Béarn" did not match it.

FINDING 3 — THREE PENDING ROWS HELD ON "Company is empty" WHOSE FIRM IS
PRINTED ON THE FICHE. Fiches 24195, 24183 and 24089 each print "Origine de
la fiche". The RappelConso scraper's company fallback did not reach it, so
all three sat behind publish-gate rule 4 with the value available on the
authority page. Read today, and filled from the page.

FINDING 4 — THE `figatellu` ROW'S PRODUCT WAS A LOT TABLE.
Fiche 24183 (Salmonella spp., Tier 1, 26/11/2026) sat in Pending as

    Product   '$lot s45021126$non concerné$$'

while the fiche heading says "figatellu". The value is the
`identification_produits` field, which is the lot table `$`-joined cell by
cell. The row's own Notes claim "Product taken from the fiche title", which
it was not. The CAUSE is fixed at the detector in scrapers/_product_text.py
(the regulator's own "non concerné" placeholder was not in the packaging
vocabulary, so "non" survived the sweep and the whole value read as a food
name, and `_first_named_product` never walked on to `libelle`); measured over
all 8,007 non-empty Product values in the workbook, the fix changes the
verdict on exactly two, both copies of this one defect. Pinned by
tests/test_morning_fix_2026_11_27.py. This script repairs the two rows
already written.

FINDING 5 — A GATE-PASSING TIER-1 ROW MADE UNPUBLISHABLE BY A VERDICT THAT
IS NO LONGER TRUE. MPI (NZ), Tip Top 6-pack Jelly Tip ice-cream, Bacillus
cereus, 2026-10-02. `pipeline.promote_gate_passing` reports it as passing
every publish-gate rule, and then:

    re-promotion BLOCKED https://www.mpi.govt.nz/news/media-releases/…
      — already in Rejected (unknown:  | REJECTED: Confirmer: row was at
      pending_gap_v2 (not reviewed by reviewer 2) and Date is empty)

The Date is NOT empty: the Pending row reads 2026-10-02, and the MPI notice
was read today and prints "Date: 02 October 2026" in the body and "Last
reviewed: 02.10.26" above the headline, so the register's Date is correct and
verified. The archived verdict was true when it was written and is false now,
and `load_rejected_urls` makes it permanent. SUPERSEDED is this repo's own
marker for exactly that (merge_master, 2026-10-11), so it is written here,
with the reading that retires it. The row is not published by this script —
it is unblocked, and the promoter publishes it through the normal path.

FINDING 6 — THE SAME SHAPE AGAIN ON fiche 24183. Its Weekly_Rejected copy
carries "Confirmer: … page does not mention any of …", which was true while
the Product was the `$`-joined lot table and is false once the Product is the
fiche's own heading. Superseded on the same grounds.

FINDING 7 (fixed in code, no data edit) — A PUBLISHED-HAZARD VOCABULARY GAP.
Mattilsynet's Sunkost cocoa-powder row (2026-11-26) was held by the gate:
"Pathogen 'PAH and Benzo(a)pyrene exceeding limit' matches no word any
subscriber alert offers". PAH and benzo(a)pyrene are chemical contaminants
with their own limits and squarely in AFTS scope. tools/alert_vocab.py now
carries them and tools/apps_script/AftsAlerts.gs is regenerated; the gate
passes the row, which then promotes through the normal path.

NOT CHANGED, DELIBERATELY
─────────────────────────
* Recalls row 63 (fiche 24125) still matches a "[needs cleanup: Reason not in
  English" search. It is a FALSE POSITIVE: the fragment sits inside a quoted
  archive verdict in a 2026-10-11 re-promotion note, truncated mid-word. The
  Reason itself was translated on 2026-11-24. An archive quote is evidence
  and is not rewritten to make a grep quieter.
* `pipeline.verify_urls` reports the same 10 structural problems it has
  reported for months — historical rows from January to April 2026 whose URL
  is a news host rather than the regulator's. None is in the last 24 h and
  none is created by today's work; re-sourcing them is a backlog of its own,
  not a morning-pass edit.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON_PATH = ROOT / "docs" / "data" / "recalls.json"

REASON_SHEETS = ("Recalls", "Pending", "Weekly_Review")

#: Per-row edits, keyed on the row's own normalised URL. `fields` is
#: {column: (expected value on main, value to write)} — the expected value is
#: checked so a row main has changed under us is SKIPPED and reported rather
#: than overwritten.
EDITS = [
    # ── FINDING 1 — fiche 24190, Recalls row 3 ────────────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24190/interne",
        "fields": {
            "Product": (
                "boudin a la viande",
                "Meat black pudding (boudin à la viande), sold at the "
                "delicatessen counter, GTIN 3505870000343, lot 630915, "
                "use-by 27/11/2026"),
            "Reason": (
                "Detection of la listéria monocytogène suite a une analyse "
                "produit",
                "Listeria monocytogenes detected following a product "
                "analysis"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-27: Product was the fiche's French "
            "reference field and the Reason was the fiche's French Motif with "
            "only its first word translated. Read on fiche 24190 today "
            "(Fiche réf. 2026-11-0196 • Version 1, date 26/11/2026): H1 "
            "\"boudin viande\" with the sub-line \"sans marque\", \"Nom de la "
            "marque du produit: sans marque\", \"Noms des modèles ou "
            "références: boudin a la viande\", \"Origine de la fiche: ARES "
            "DISTRIBUTION LECLERC\", Catégorie Alimentation / Sous-catégorie "
            "\"Autres\", GTIN \"3505870000343\" with Lot \"630915\" and "
            "\"Date limite de consommation 27/11/2026\", Conditionnements "
            "\"produit vendu en rayon traditionnel charcuterie\", \"Date "
            "début/Fin de commercialisation: Du 19/11/2026 au 23/11/2026\", "
            "\"Produit à conserver au réfrigérateur\", Zone \"Départements: "
            "GIRONDE (33)\", Distributeurs \"E LECLERC ARES\", Motif "
            "\"Détection de la listéria monocytogène suite a une analyse "
            "produit\", Risques \"Listeria monocytogenes (agent responsable "
            "de la listériose)\", Nature juridique \"Volontaire (sans arrêté "
            "préfectoral)\". No health mark is printed and none is added. "
            "Company is left as the already-published \"E.Leclerc Arès\", "
            "this register's convention for a RappelConso retailer fiche. "
            "[original product: boudin a la viande] [original Reason (fr): "
            "\"Détection de la listéria monocytogène suite a une analyse "
            "produit\"]]"),
    },
    # ── FINDINGS 1 + 2 — fiche 24191, Recalls row 4 ───────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24191/interne",
        "fields": {
            "Product": (
                "pavé fermière ail des ours au lait cru",
                "Farmhouse raw-milk wild-garlic pavé cheese (pavé fermière "
                "ail des ours au lait cru), sold loose cut from the piece, "
                "GTIN 2609898025201, lot 217, best-before 08/12/2026"),
            "Reason": ("Presence of Listeria monocytogène",
                       "Presence of Listeria monocytogenes"),
        },
        # NOT in `fields`, and that is the point. `fields` compares through
        # _norm(), which collapses runs of whitespace — so a change that is
        # ONLY whitespace reads as "already right" and silently no-ops. It
        # did, on the first run of this script: the gate's REBASE-REPORT
        # still printed the row under 'Distralis - Bergers du Haut  Béarn'.
        # `raw_fields` compares the strings exactly.
        "raw_fields": {
            "Company": ("Distralis - Bergers du Haut  Béarn",
                        "Distralis - Bergers du Haut Béarn"),
            "Brand": ("Distralis - Bergers du Haut  Béarn",
                      "Distralis - Bergers du Haut Béarn"),
        },
        "drop_stamps": ("[needs cleanup: Product not in English]",),
        "note": (
            "[morning fix pass 2026-11-27: Product was the fiche's French "
            "reference field; the Reason's last word was French too and no "
            "test saw it (one foreign word inside English is below "
            "detect_language's two-hit threshold, which is what protects an "
            "English sentence quoting a foreign name); and Company and Brand "
            "both carried a DOUBLE SPACE before \"Béarn\" that the fiche does "
            "not print, so a dashboard search for \"Haut Béarn\" did not "
            "match the row. Read on fiche 24191 today (Fiche réf. "
            "2026-11-0197 • Version 1, date 26/11/2026): H1 \"Pavé fermière "
            "ail des ours au lait cru\" with the sub-line \"Distralis - "
            "Bergers du Haut Béarn\" (ONE space), \"Nom de la marque du "
            "produit: Distralis - Bergers du Haut Béarn\", \"Noms des modèles "
            "ou références: Pavé fermière ail des ours au lait cru\", "
            "\"Origine de la fiche: SOCIETE CASTELORIENNE DE DISTRIBUTION "
            "E.LECLERC\", Catégorie Alimentation / Sous-catégorie \"Lait et "
            "produits laitiers\", GTIN \"2609898025201\" with Lot \"217\" and "
            "\"Date de durabilité minimale 08/12/2026\", Conditionnements "
            "\"Vrac\", Informations complémentaires \"Vendu au fromage à la "
            "coupe.\", Marque de salubrité \"FR64.310.111CE\", \"Date "
            "début/Fin de commercialisation: Du 03/11/2026 au 12/11/2026\", "
            "\"Produit à conserver au réfrigérateur\", Zone \"Départements: "
            "SARTHE (72)\", Distributeurs \"Leclerc Montval sur Loir\", Motif "
            "\"Présence de listeria monocytogène\", Risques \"Listeria "
            "monocytogenes (agent responsable de la listériose)\", Nature "
            "juridique \"Volontaire (sans arrêté préfectoral)\". \"pavé\" is "
            "kept as the cheese's own form name and the sub-category is what "
            "says it is a cheese; \"sold loose cut from the piece\" is the "
            "fiche's \"Vrac\" plus \"Vendu au fromage à la coupe\" and adds "
            "nothing. [original product: Pavé fermière ail des ours au lait "
            "cru] [original brand: Distralis - Bergers du Haut Béarn] "
            "[original Reason (fr): \"Présence de listeria monocytogène\"]]"),
    },
    # ── FINDING 1 — fiche 24182, Recalls row 5 ────────────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24182/interne",
        "fields": {
            "Product": (
                "ventrèche roulée séchée entière, ventrèche roulée séchée "
                "tranchée sous vide",
                "Dry-cured rolled pork belly (ventrèche roulée séchée), "
                "whole and vacuum-packed sliced, lot 130726, best-before "
                "18/12/2026"),
            "Reason": ("Presence of listéria", "Presence of Listeria"),
        },
        "drop_stamps": ("[needs cleanup: Product not in English]",),
        "note": (
            "[morning fix pass 2026-11-27: Product was the fiche's French "
            "reference field and the Reason's last word was French. Read on "
            "fiche 24182 today (Fiche réf. 2026-11-0188 • Version 1, date "
            "26/11/2026): H1 \"Ventrèche roulée séchée\" with the sub-line "
            "\"Montauzer\", \"Nom de la marque du produit: Montauzer\", "
            "\"Noms des modèles ou références: Ventrèche roulée séchée "
            "entière, ventrèche roulée séchée tranchée sous vide\", \"Origine "
            "de la fiche: MAISON MONTAUZER Maison MONTAUZER\", Catégorie "
            "Alimentation / Sous-catégorie \"Autres\", GTIN cell EMPTY (so no "
            "GTIN is added), Lot \"130726\" with \"Date de durabilité "
            "minimale 18/12/2026\", Conditionnements \"Sous vide\", Marque de "
            "salubrité \"FR 64-094-001 CE\", \"Date début/Fin de "
            "commercialisation: Du 04/09/2026 au 19/10/2026\", \"Produit à "
            "conserver au réfrigérateur\", Zone \"Régions: "
            "Nouvelle-Aquitaine\", Distributeurs \"Maison Montauzer, "
            "Carrefour market Salies de Béarn, Gamm vert Oloron Sainte "
            "Marie\", Motif \"Présence de Listéria\", Risques \"Listeria "
            "monocytogenes (agent responsable de la listériose)\", Nature "
            "juridique \"Volontaire (sans arrêté préfectoral)\". The Motif "
            "says only \"Listéria\" with no species, so the Reason says "
            "\"Listeria\" and the species is NOT read into it — the Pathogen "
            "column already carries the canonical label from the fiche's own "
            "Risques line, which does name it. \"ventrèche\" is the "
            "south-western French cut and is kept beside the English. "
            "[original product: Ventrèche roulée séchée entière, ventrèche "
            "roulée séchée tranchée sous vide] [original Reason (fr): "
            "\"Présence de Listéria\"]]"),
    },
    # ── FINDING 1 — fiche 24181, Recalls row 6 ────────────────────────────
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24181/interne",
        "fields": {
            "Product": (
                "ventrèche plate séchée entière, ventrèche plate séchée 1/2 "
                "sous vide, ventrèche plate séchée tranchée sous vide",
                "Dry-cured flat pork belly (ventrèche plate séchée), whole, "
                "vacuum-packed half and vacuum-packed sliced, lots 240726A "
                "(best-before 17/12/2026) and 240726B (best-before "
                "18/12/2026)"),
            "Reason": ("Presence of listéria", "Presence of Listeria"),
        },
        "drop_stamps": ("[needs cleanup: Product not in English]",),
        "note": (
            "[morning fix pass 2026-11-27: Product was the fiche's French "
            "reference field and the Reason's last word was French. Read on "
            "fiche 24181 today (Fiche réf. 2026-11-0187 • Version 1, date "
            "26/11/2026): H1 \"Ventrèche plate séchée\" with the sub-line "
            "\"Montauzer\", \"Nom de la marque du produit: Montauzer\", "
            "\"Noms des modèles ou références: Ventrèche plate séchée "
            "entière, ventrèche plate séchée 1/2 sous vide, ventrèche plate "
            "séchée tranchée sous vide\", \"Origine de la fiche: MAISON "
            "MONTAUZER Maison MONTAUZER\", Catégorie Alimentation / "
            "Sous-catégorie \"Autres\", GTIN cell EMPTY (so no GTIN is "
            "added), two lots — \"240726A\" with \"Date de durabilité "
            "minimale 17/12/2026\" and \"240726B\" with \"Date de durabilité "
            "minimale 18/12/2026\" — Conditionnements \"Sous vide\", Marque "
            "de salubrité \"FR 64-094-001 CE\", \"Date début/Fin de "
            "commercialisation: Du 20/08/2026 au 19/10/2026\", \"Produit à "
            "conserver au réfrigérateur\", Zone \"France entière\", "
            "Distributeurs \"Maison Montauzer, carrefour market Peyrehorade, "
            "Saint Pierre d'Irube, Salies de Béarn, Tarnos, Intermarché "
            "Bidache, Bayonne, Leclerc Biarritz.\", Motif \"Présence de "
            "Listéria\", Risques \"Listeria monocytogenes (agent responsable "
            "de la listériose)\", Nature juridique \"Volontaire (sans arrêté "
            "préfectoral)\". Same firm and same hazard as fiche 24182 but a "
            "DIFFERENT fiche, a different cut and different lots, so both "
            "rows stand: one row per regulator notice. [original product: "
            "Ventrèche plate séchée entière, ventrèche plate séchée 1/2 sous "
            "vide, ventrèche plate séchée tranchée sous vide] [original "
            "Reason (fr): \"Présence de Listéria\"]]"),
    },
    # ── FINDING 7b — the Mattilsynet row's Reason is half Norwegian ───────
    #
    # Unblocked by the vocabulary fix, this row would publish with the
    # hazard's Norwegian name in the Reason. No gate and no test objects —
    # reason_needs_english() returns False, because one foreign noun phrase
    # inside an English sentence is below detect_language's two-hit
    # threshold, which is the threshold that protects an English sentence
    # quoting a foreign name. "Published Reason must be English" is the rule
    # regardless, and the English term is the one the alert vocabulary and
    # the hazard class both now carry. The Norwegian is kept on the row.
    #
    # "(Recall ID 411599)" IS LEFT ALONE. It is not a prompt-example leak:
    # 22 rows of this register carry a "Recall ID <n>" parenthetical in
    # Reason, including published rows with the regulator's own reference
    # (FSIS 031-2024, AGES 3825082). The ONE genuine leak of that shape,
    # "Recall ID 842632", is the publish gate's own named example and is
    # already archived in Rejected. The Mattilsynet notice itself would not
    # open for this pass (permanent 4xx on
    # https://www.mattilsynet.no/tilbakekallinger/sunkost-superfood-
    # kakaopulver-tilbakekalles), and "I could not read it" is not "it is
    # wrong": the identifier stays.
    {
        "sheet": "Pending",
        "url": ("https://www.mattilsynet.no/tilbakekallinger/"
                "sunkost-superfood-kakaopulver-tilbakekalles"),
        "fields": {
            "Reason": (
                "Product contains Polysykliske aromatiske hydrokarboner "
                "(PAH) and benzo(a)pyrene above the limit for cocoa products "
                "(Recall ID 411599)",
                "Product contains polycyclic aromatic hydrocarbons (PAH) and "
                "benzo(a)pyrene above the limit for cocoa products "
                "(Recall ID 411599)"),
        },
        "note": (
            "[morning fix pass 2026-11-27: Reason's hazard name translated "
            "from Norwegian to the English term; the regulator's own wording "
            "is kept here. This row was held by the publish gate with "
            "\"Pathogen 'PAH and Benzo(a)pyrene exceeding limit' matches no "
            "word any subscriber alert offers\" — the gap is fixed in "
            "tools/alert_vocab.py (regenerating tools/apps_script/"
            "AftsAlerts.gs) and in the chemical hazard class in "
            "pipeline/_publish_gate.py, which would have refused the row the "
            "moment it published. The notice page itself returns a permanent "
            "4xx to this pass, so nothing on the row is changed beyond the "
            "hazard's English name, which is read from the row's own text. "
            "[original Reason (no): \"Product contains Polysykliske "
            "aromatiske hydrokarboner (PAH) and benzo(a)pyrene above the "
            "limit for cocoa products (Recall ID 411599)\"]]"),
    },
    # ── FINDING 8 — the Berschneider row loses half its hazard ────────────
    #
    # The lebensmittelwarnung.de notice says "In dem betroffenen Produkt
    # wurden Salmonellen und Listerien nachgewiesen." add_manual_row
    # canonicalises Pathogen and must not be hand-set, so the queued row came
    # out as "Listeria monocytogenes" alone: normalize_pathogen returns ONE
    # label. That is not wrong, it is half the notice — and the pathogen
    # haystack for a subscriber alert is the Pathogen field alone, so a
    # subscriber watching Salmonella would not hear about a Tier-1 product in
    # thirteen Bundesländer. The register's own two-hazard form is restored
    # here, after canonicalisation, exactly as the already-published ARGEL
    # row of 2026-07-10 carries it.
    {
        "sheet": "Pending",
        "url": ("https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de"
                "/Meldungen/2026/11_November/261126_27_BW_Gaensebrust/"
                "261126_27_BW_Gaensebrust.html"),
        "fields": {
            "Pathogen": ("Listeria monocytogenes",
                         "Listeria monocytogenes, Salmonella"),
        },
        "note": (
            "[morning fix pass 2026-11-27: Pathogen restored to both hazards "
            "the notice names. normalize_pathogen returns a single label, so "
            "the queued row read \"Listeria monocytogenes\" and dropped the "
            "Salmonella half of \"In dem betroffenen Produkt wurden "
            "Salmonellen und Listerien nachgewiesen.\" A subscriber alert's "
            "pathogen haystack is the Pathogen field alone, so the half that "
            "was dropped was unreachable. The two-hazard form is the one the "
            "register already publishes (ARGEL row, 2026-07-10). No species "
            "is invented for the Salmonella half: the notice names none.]"),
    },
    # ── the Cabrichou extension, recorded on the row it extends ───────────
    #
    # tools/manual_rows_2026_11_27.json originally carried a fifth row, for
    # RappelConso fiche 24184 of 26/11/2026 (LE CABRICHOU, "Buche fraiche
    # nature", "Suspicion de présence de Salmonelles"). append_to_pending
    # refused it as a near-duplicate, and the refusal is RIGHT: fiche 24108,
    # published here on 2026-11-19, carries THE SAME LOT 24432 and THE SAME
    # best-before 17/11/2026 from the same firm with the same Salmonella
    # suspicion. It is one contamination event that the regulator extended
    # from the crottin to the log, not a second recall. The row was dropped
    # from the JSON and the second fiche is recorded on the row it extends,
    # so the register says that RappelConso published twice.
    {
        "sheet": "Recalls",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24108/interne",
        "note": (
            "[morning fix pass 2026-11-27: RappelConso published a SECOND "
            "fiche for this same lot — fiche 24184 of 26/11/2026 (Fiche réf. "
            "2026-11-0190 • Version 1), read today: H1 \"Buche fraiche "
            "nature\", \"Nom de la marque du produit: LE CABRICHOU\", \"Noms "
            "des modèles ou références: Buche fraiche nature réf BUFRCA\", "
            "\"Origine de la fiche: LE CABRICHOU LE CABRICHOU\", Catégorie "
            "Alimentation / \"Lait et produits laitiers\", Lot \"24432\" with "
            "\"Date de durabilité minimale 17/11/2026\", Conditionnements "
            "\"4\", \"Du 28/10/2026 au 31/10/2026\", France entière, "
            "Distributeurs \"BRASSERIE LE COURLIS Paris CAFE BONAP' Paris\", "
            "Motif \"Suspicion de présence de Salmonelles\", Risques "
            "\"Salmonella spp (agent responsable de la salmonellose)\". SAME "
            "LOT 24432 and SAME best-before 17/11/2026 as this row, so it is "
            "the same contamination event extended to a second product line "
            "(buche fraiche nature, réf BUFRCA, pack of 4) and to two Paris "
            "outlets. The operator pass queued it and "
            "merge_master.append_to_pending refused it as a near-duplicate; "
            "the refusal is accepted and the second fiche is recorded here "
            "rather than published as a second row.]"),
    },
    # ── FINDING 3 — fiche 24195, Pending ──────────────────────────────────
    {
        "sheet": "Pending",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24195/interne",
        "fields": {
            "Company": ("", "Spécialités Antillaises Saveurs Créoles"),
            "Product": (
                "tarte 3 fromages",
                "Three-cheese tart (tarte 3 fromages), plastic tray, health "
                "mark FR 80.561.009 CE, lots 26317 (use-by 22/11/2026), "
                "26315 (use-by 20/11/2026) and 26327 (use-by 02/12/2026)"),
            "Reason": ("Detection of présence de Listeria monocytogenes",
                       "Listeria monocytogenes detected"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-27: held on publish-gate rule 4 "
            "\"Company is empty\" with the firm printed on its own fiche, and "
            "Product and Reason were both French. Read on fiche 24195 today "
            "(Fiche réf. 2026-11-0201 • Version 1, date 26/11/2026): H1 "
            "\"Tarte 3 fromages\", \"Nom de la marque du produit: Sans "
            "marque\", \"Noms des modèles ou références: Tarte 3 fromages\", "
            "\"Origine de la fiche: SPECIALITES ANTILLAISES SAVEURS CREOLES "
            "Saveurs créoles\", Catégorie Alimentation / Sous-catégorie "
            "\"Plats préparés et snacks\", NO GTIN printed, lots \"26317\" "
            "(Date limite de consommation 22/11/2026), \"26315\" "
            "(20/11/2026) and \"26327\" (02/12/2026), Marque de salubrité "
            "\"FR 80.561.009 CE\", Conditionnements \"Barquette plastique\", "
            "\"Date début/Fin de commercialisation: Du 13/11/2026 au "
            "26/11/2026\", Zone \"France entière\", Distributeurs \"Saveurs "
            "et vie\", Motif \"Détection de présence de listeria "
            "monocytogenes\", Risques \"Listeria monocytogenes (agent "
            "responsable de la listériose)\", Nature juridique \"Volontaire "
            "(sans arrêté préfectoral)\". Company is the firm half of "
            "\"Origine de la fiche\"; \"Saveurs et vie\" in this row's own "
            "Notes is the DISTRIBUTOR and is not the firm. Brand stays "
            "\"Unbranded\" — the fiche's marque field says \"Sans marque\". "
            "[original product: Tarte 3 fromages] [original company: "
            "SPECIALITES ANTILLAISES SAVEURS CREOLES Saveurs créoles] "
            "[original Reason (fr): \"Détection de présence de listeria "
            "monocytogenes\"]]"),
    },
    # ── FINDINGS 3 + 4 — fiche 24183, Pending ─────────────────────────────
    {
        "sheet": "Pending",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24183/interne",
        "fields": {
            "Company": ("", "Vanni Franck Charcuterie Tavagna"),
            "Product": ("$lot s45021126$non concerné$$",
                        "Figatellu, lot S45021126"),
        },
        "note": (
            "[morning fix pass 2026-11-27: held on publish-gate rule 4 "
            "\"Company is empty\", and the Product was the fiche's lot table "
            "`$`-joined cell by cell rather than a food — see FINDING 4 in "
            "tools/repair_2026_11_27.py for the detector fix that stops it "
            "recurring. Read on fiche 24183 today (Fiche réf. 2026-11-0189 • "
            "Version 1, date 26/11/2026): H1 \"figatellu\", \"Nom de la "
            "marque du produit: sans marque\", \"Noms des modèles ou "
            "références: Lot S45021126\" (the reference field holds ONLY the "
            "lot, which is why the heading is the denomination), \"Origine de "
            "la fiche: VANNI FRANCK CHARCUTERIE TAVAGNA\", Catégorie "
            "Alimentation / Sous-catégorie \"Viandes\", NO GTIN and NO "
            "use-by printed, lot \"lot s45021126\", \"Date début/Fin de "
            "commercialisation: Du 03/11/2026 au 06/11/2026\", \"Produit à "
            "conserver à température ambiante\", Zone \"Départements: "
            "HAUTE-CORSE (2B)\", Distributeurs \"proxi miomo ,snc luchetti , "
            "granini alimentation,far alimentation,SARL coradis temi fa\", "
            "Motif \"SALMONELLA SPP\", Risques \"Salmonella spp (agent "
            "responsable de la salmonellose)\", Nature juridique "
            "\"Volontaire (sans arrêté préfectoral)\", numéro de contact "
            "\"0611721087\". Product is the fiche's own heading and the lot, "
            "and NOTHING ELSE: \"figatellu\" is the only denomination the "
            "fiche prints, so no gloss such as \"Corsican pork sausage\" is "
            "added — the fiche does not say what meat it is. Brand stays "
            "\"Unbranded\". [original product: $lot s45021126$non concerné$$ "
            "(the `identification_produits` lot table)] [original company: "
            "VANNI FRANCK CHARCUTERIE TAVAGNA]]"),
    },
    # ── FINDING 3 — fiche 24089, Pending ──────────────────────────────────
    {
        "sheet": "Pending",
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24089/interne",
        "fields": {
            "Company": ("", "Coopérative U Enseigne"),
            "Product": (
                "bûche / bûchette roulée pralinée vanille; bûchette rocher "
                "chocolat praliné",
                "Vanilla praline rolled yule log / mini log (bûche / "
                "bûchette roulée pralinée vanille), GTIN 2000000324418, lots "
                "1/288/26 and 1/289/26; Rocher chocolate-praline mini log "
                "(bûchette Rocher chocolat praliné), GTIN 2000000278902, lot "
                "1/286/26"),
            "Reason": (
                "Risque physique : présence possible de petit corps "
                "étrangers de type cailloux dans des éclats de noisette "
                "2/4mm",
                "Physical risk: possible presence of small foreign bodies of "
                "the stone type in hazelnut pieces 2/4 mm"),
        },
        "drop_stamps": ("[needs cleanup: Reason not in English]",
                        "[needs cleanup: Product not in English]"),
        "note": (
            "[morning fix pass 2026-11-27: held on publish-gate rule 4 "
            "\"Company is empty\" with the firm printed on its own fiche, and "
            "Product and Reason were both French. Read on fiche 24089 today "
            "(Fiche réf. 2026-11-0101 • Version 1, date 16/11/2026): H1 "
            "\"Bûche / bûchette roulée pralinée vanille et Bûchette Rocher "
            "chocolat praliné\", \"Nom de la marque du produit: Sans "
            "marque\", \"Noms des modèles ou références: Bûche / bûchette "
            "roulée pralinée vanille Bûchette Rocher chocolat praliné\", "
            "\"Origine de la fiche: COOPERATIVE U ENSEIGNE U ENSEIGNE\", "
            "Catégorie Alimentation / Sous-catégorie \"Produits sucrés\", "
            "GTIN \"2000000324418\" with lots \"1/288/26 - 1/289/26\" and "
            "GTIN \"2000000278902\" with lot \"1/286/26\", NO use-by printed, "
            "Marque de salubrité \"Code emballeur : EMB 25563 A\", \"Date "
            "début/Fin de commercialisation: Du 21/10/2026 au 09/11/2026\", "
            "\"Produit à conserver au réfrigérateur\", Zone \"France "
            "entière\", Distributeurs \"Magasins U\", Motif \"RISQUE "
            "PHYSIQUE : Présence possible de petit corps étrangers de type "
            "cailloux dans des éclats de noisette 2/4mm\", Risques "
            "\"Inertes (verre, métal, plastique, papier, textile…)\", Nature "
            "juridique \"Volontaire (sans arrêté préfectoral)\", "
            "Modalités de compensation \"Remboursement\". Company is the firm "
            "half of \"Origine de la fiche\"; \"magasins u\" in this row's "
            "own Notes is the DISTRIBUTOR, which the fiche prints separately. "
            "Brand stays \"Unbranded\" — the marque field says \"Sans "
            "marque\". [original product: Bûche / bûchette roulée pralinée "
            "vanille Bûchette Rocher chocolat praliné] [original company: "
            "COOPERATIVE U ENSEIGNE U ENSEIGNE] [original Reason (fr): "
            "\"RISQUE PHYSIQUE : Présence possible de petit corps étrangers "
            "de type cailloux dans des éclats de noisette 2/4mm\"]]"),
    },
]

#: FINDINGS 5 and 6 — archive verdicts that were true when written and are
#: false now. Each entry names the sheet, the URL, a fragment that must be
#: present in the row's recorded verdict (so a row whose verdict main has
#: changed is skipped, not stamped), and the banner.
SUPERSEDE = [
    {
        "sheets": ("Rejected", "Weekly_Rejected"),
        "url": ("https://www.mpi.govt.nz/news/media-releases/tip-top-6-pack-"
                "jelly-tip-ice-creams-recalled-due-to-possible-presence-of-"
                "bacteria"),
        "expect": "Date is empty",
        "banner": (
            "[SUPERSEDED 2026-11-27 — the verdict quoted below refused this "
            "recall because \"Date is empty\". THE DATE IS NOT EMPTY: the "
            "Pending row reads 2026-10-02, and the MPI notice was read today "
            "and prints \"Date: 02 October 2026\" in the body with \"Last "
            "reviewed: 02.10.26\" above the headline, so the register's Date "
            "is correct and verified against the authority page. The notice "
            "also prints the recalling firm \"Tip Top Ice Cream Company\", "
            "the brand \"Tip Top\", the hazard Bacillus cereus (\"could "
            "contain the bacteria Bacillus cereus\"), the 6-pack cardboard "
            "box 6 x 75 ml with best-before \"30-SEP-28 03:00 L MO6252 to "
            "30-SEP-28 05:00 L MO6252\", sale at Woolworths supermarkets in "
            "the North Island, and \"New Zealand Food Safety has not "
            "received any notifications of associated illness\". "
            "pipeline.promote_gate_passing reports the Pending row as "
            "passing every publish-gate rule and was then refused by the "
            "re-promotion guard on the strength of this archived verdict "
            "alone, which load_rejected_urls treats as permanent. A verdict "
            "that has been overtaken by the facts must not be able to make "
            "itself unappealable. Kept as the audit trail of the original "
            "refusal.] "),
    },
    {
        "sheets": ("Weekly_Rejected", "Rejected"),
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24183/interne",
        "expect": "does not mention any of",
        "banner": (
            "[SUPERSEDED 2026-11-27 — the verdict quoted below refused this "
            "recall because the fiche \"does not mention any of\" the row's "
            "values. That was TRUE while the Product was "
            "'$lot s45021126$non concerné$$' — the fiche's lot table "
            "`$`-joined cell by cell, which names no food and appears nowhere "
            "on the page as a phrase — and it is FALSE now that the Product "
            "is the fiche's own heading \"figatellu\" and the Company is its "
            "own \"Origine de la fiche\", VANNI FRANCK CHARCUTERIE TAVAGNA. "
            "Fiche 24183 was read today: H1 \"figatellu\", Motif \"SALMONELLA "
            "SPP\", Risques \"Salmonella spp (agent responsable de la "
            "salmonellose)\", lot \"lot s45021126\", HAUTE-CORSE (2B), "
            "date 26/11/2026. The detector defect that produced the bad "
            "Product is fixed in scrapers/_product_text.py and pinned by "
            "tests/test_morning_fix_2026_11_27.py. Kept as the audit trail "
            "of the original refusal.] "),
    },
]

#: FINDING 5 — the resolved date-unknown stamp on the MPI row.
MPI_URL = SUPERSEDE[0]["url"]
MPI_STAMP = "[date-unknown: claude-check please verify Date]"
MPI_NOTE = (
    "[morning fix pass 2026-11-27: Date VERIFIED against the authority page "
    "and the date-unknown stamp retired. https://www.mpi.govt.nz/news/media-"
    "releases/tip-top-6-pack-jelly-tip-ice-creams-recalled-due-to-possible-"
    "presence-of-bacteria was read today and prints \"Date: 02 October 2026\" "
    "in the body, \"Last reviewed: 02.10.26\" above the headline, and "
    "2026-10-02 in the page metadata. The row's Date 2026-10-02 is right. The "
    "archived rejection that refused this row for an empty Date is marked "
    "SUPERSEDED in the same pass; nothing else about the row is changed and "
    "it publishes, if it publishes, through the ordinary promoter.]")


def _norm(v) -> str:
    return " ".join(str(v or "").split()).strip().lower()


def _append_note(row_notes, note: str) -> str:
    """Append `note` once, keyed on its own first 60 characters.

    The script has to be a no-op the second time it runs, because
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
    # IT WRITES BY DEFAULT. tools/rebase_and_verify.py re-runs every
    # tools/repair_*.py with NO ARGUMENTS; a script needing --apply would be a
    # silent no-op inside the gate and the gate would zip main's unrepaired
    # workbook with a green suite and nothing in the report to say so.
    ap.add_argument("--dry-run", action="store_true",
                    help="report only, write nothing")
    ap.add_argument("--apply", action="store_true",
                    help="accepted and ignored — writing is the default")
    args = ap.parse_args()

    import openpyxl

    # ── 0. the four operator-found rows, through the real code path ───────
    #
    # tools/add_manual_row.py hands them to merge_master.append_to_pending,
    # so they get the same dedup, the same validation and the same review
    # stages as a scraped row. Called from HERE rather than by hand because
    # rebase_and_verify re-runs this script and nothing else.
    if not args.dry_run:
        import subprocess
        rows_json = ROOT / "tools" / "manual_rows_2026_11_27.json"
        if rows_json.exists():
            r = subprocess.run(
                [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
                 str(rows_json)],
                cwd=str(ROOT), capture_output=True, text=True)
            tail = [ln for ln in r.stdout.splitlines() if ln.strip()][-7:]
            print("add_manual_row: " + " | ".join(tail))
            if r.returncode != 0:
                print("add_manual_row FAILED:\n" + r.stdout[-4000:]
                      + "\n" + r.stderr[-2000:])
                return 1

    from pipeline import merge_master as mm

    path = Path(args.xlsx)
    # openpyxl in place, cell by cell, NOT a pandas round-trip: re-serialising
    # every sheet through pandas rewrites the Date column as timestamps and
    # drops the archive sheets' own types.
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

    def _rows_at(ws, hdr, url):
        target = mm._normalize_url_for_dedup(url)
        return [r for r in range(2, ws.max_row + 1)
                if mm._normalize_url_for_dedup(_get(ws, hdr, r, "URL") or "")
                == target]

    # ── per-row edits ─────────────────────────────────────────────────────
    for spec in EDITS:
        name = spec["sheet"]
        ws, hdr = _sheet(name)
        if ws is None:
            report.append(f"  MISS  sheet {name} absent")
            continue
        hits = _rows_at(ws, hdr, spec["url"])
        if not hits:
            report.append(f"  MISS  {name}: no row at {spec['url'][:70]}")
            continue
        for r in hits:
            touched = False
            # Exact-comparison fields first — see the `raw_fields` note on
            # the fiche 24191 spec. A whitespace-only repair is invisible to
            # the normalised comparison below.
            for col, (was, now) in spec.get("raw_fields", {}).items():
                if col not in hdr:
                    report.append(f"  MISS  {name} row {r}: no column {col}")
                    continue
                cur = _get(ws, hdr, r, col) or ""
                if cur == now:
                    continue                       # already right; no-op
                if cur != was:
                    report.append(
                        f"  SKIP  {name} row {r}.{col} (raw): on main it "
                        f"reads {cur[:60]!r}, expected {was[:60]!r}")
                    continue
                _set(ws, hdr, r, col, now)
                touched = True
                report.append(f"  SET   {name} row {r}.{col} (raw) -> "
                              f"{now[:70]!r}")
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
                    new_notes = " ".join(new_notes.replace(stamp, "").split())
                    touched = True
                    report.append(f"  DROP  {name} row {r} stamp {stamp}")
            if spec.get("note"):
                out = _append_note(new_notes, spec["note"])
                if out != new_notes:
                    new_notes = out
                    touched = True
            if new_notes != notes:
                _set(ws, hdr, r, "Notes", new_notes)
            if touched:
                changed += 1

    # ── the same translated Reasons wherever else they appear ─────────────
    #
    # A fiche's row is often in Pending or Weekly_Review as well as Recalls.
    # Translating only the Recalls copy leaves the French one to be re-read by
    # the next reviewer pass and written back.
    reason_map = {}
    for spec in EDITS:
        f = spec.get("fields", {})
        if "Reason" in f:
            was, now = f["Reason"]
            reason_map[_norm(was)] = now
    for name in REASON_SHEETS:
        ws, hdr = _sheet(name)
        if ws is None or "Reason" not in hdr:
            continue
        for r in range(2, ws.max_row + 1):
            cur = _get(ws, hdr, r, "Reason") or ""
            now = reason_map.get(_norm(cur))
            if now and _norm(cur) != _norm(now):
                _set(ws, hdr, r, "Reason", now)
                changed += 1
                report.append(f"  SET   {name} row {r}.Reason (same French "
                              f"value elsewhere) -> {now[:50]!r}")

    # ── FINDINGS 5 and 6 — retire two overtaken archive verdicts ──────────
    for spec in SUPERSEDE:
        done = False
        for name in spec["sheets"]:
            ws, hdr = _sheet(name)
            if ws is None:
                continue
            rcol = ("RejectionReason" if "RejectionReason" in hdr
                    else "RejectReason" if "RejectReason" in hdr else None)
            for r in _rows_at(ws, hdr, spec["url"]):
                prior = (_get(ws, hdr, r, rcol) if rcol else "") or ""
                notes = _get(ws, hdr, r, "Notes") or ""
                haystack = prior + " " + notes
                if spec["expect"] not in haystack:
                    report.append(
                        f"  SKIP  {name} row {r}: verdict no longer quotes "
                        f"{spec['expect']!r}; not superseded")
                    continue
                if "SUPERSEDED 2026-11-27" in haystack:
                    done = True
                    continue
                # The banner goes where the verdict is recorded. On these rows
                # the reason column is blank and the verdict is in Notes, so
                # writing only to the reason column would point at nothing.
                if rcol and prior.strip():
                    _set(ws, hdr, r, rcol, spec["banner"] + prior)
                elif rcol:
                    _set(ws, hdr, r, rcol, spec["banner"])
                if "Notes" in hdr:
                    _set(ws, hdr, r, "Notes",
                         _append_note(notes, spec["banner"].strip()))
                if "Status" in hdr:
                    _set(ws, hdr, r, "Status", "SUPERSEDED")
                changed += 1
                done = True
                report.append(f"  SUPER {name} row {r} -> SUPERSEDED "
                              f"({spec['url'][-40:]})")
        if not done:
            report.append(f"  MISS  no archive row to supersede at "
                          f"{spec['url'][-50:]}")

    # ── FINDING 5 — the resolved date-unknown stamp ───────────────────────
    for name in ("Pending", "Recalls", "Weekly_Review"):
        ws, hdr = _sheet(name)
        if ws is None or "Notes" not in hdr:
            continue
        for r in _rows_at(ws, hdr, MPI_URL):
            notes = _get(ws, hdr, r, "Notes") or ""
            out = notes
            if MPI_STAMP in out:
                out = " ".join(out.replace(MPI_STAMP, "").split())
            out = _append_note(out, MPI_NOTE)
            if out != notes:
                _set(ws, hdr, r, "Notes", out)
                changed += 1
                report.append(f"  SET   {name} row {r}: date-unknown stamp "
                              f"retired, reading recorded")

    # ── FINDING 2 — every firm name with an internal whitespace run ───────
    #
    # The writer fix in pipeline/merge_master._write_sheet collapses these
    # from now on, but only when something rewrites the sheet, and a sheet
    # nothing touches keeps its spelling for ever. So every instance is
    # collapsed here, in every sheet.
    #
    # MEASURED on main at 1f2e319d: 24 Company/Brand cells carry an internal
    # whitespace run — 10 in Recalls, 3 in Weekly_Review, 11 in Rejected.
    # The published ones are fiches 24191 and 24175, "Aoste  Professionnel",
    # "Huîtres de la Zone  Pointe de Saire  50-07", and one Brand that is a
    # leaked CDC page title (a different, older defect that this does not
    # pretend to fix, only to re-space). Of the 24, the Recalls and Pending
    # cells are already collapsed by the writer fix when add_manual_row
    # rewrites those sheets at step 0 above, so this sweep reports the
    # remaining 14 in the two archive sheets that nothing else rewrites.
    #
    # Only Company and Brand, only internal runs. This cannot change which
    # characters a name is made of, so it cannot lose a fact. Pinned by
    # tests/test_morning_fix_2026_11_27.py.
    spaced = 0
    for name in wb.sheetnames:
        ws, hdr = _sheet(name)
        if ws is None:
            continue
        for col in ("Company", "Brand"):
            if col not in hdr:
                continue
            for r in range(2, ws.max_row + 1):
                cur = _get(ws, hdr, r, col) or ""
                out = " ".join(cur.split())
                if out != cur:
                    _set(ws, hdr, r, col, out)
                    spaced += 1
                    changed += 1
    if spaced:
        report.append(f"  SPACE {spaced} firm-name cell(s) lost an internal "
                      f"whitespace run")

    # ── stale cleanup stamps, swept against the gates' own predicates ─────
    #
    # add_manual_row's writer (merge_master._write_sheet) re-stamps every row
    # it writes whose field fails the predicate, and it runs BEFORE the
    # translations above. The sweep uses the SAME predicates the gates and the
    # tests use, so nothing is hidden: a field that still needs work keeps its
    # stamp, because the predicate still fires on it.
    from pipeline._language import (has_non_latin_script,
                                    product_needs_english,
                                    reason_needs_english)

    STAMPS = (
        ("[needs cleanup: Product not in English]",
         lambda row: not product_needs_english(row.get("Product"))),
        ("[needs cleanup: Company/Brand not in English]",
         lambda row: not (has_non_latin_script(row.get("Company"))
                          or has_non_latin_script(row.get("Brand")))),
        ("[needs cleanup: Reason not in English]",
         lambda row: not reason_needs_english(row.get("Reason"))),
    )
    swept = 0
    for name in ("Recalls", "Pending", "Weekly_Review"):
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
            long_stamp = ("[needs cleanup: Reason not in English and no "
                          "verified translation]")
            if long_stamp in out and not reason_needs_english(row["Reason"]):
                out = " ".join(out.replace(long_stamp, "").split())
                dropped.append(long_stamp)
            if dropped:
                out = _append_note(
                    out,
                    "[morning fix pass 2026-11-27: dropped "
                    f"{len(dropped)} stale cleanup stamp(s) — the field(s) "
                    "they name read in English under the same predicate the "
                    "publish gates and the language tests use.]")
                _set(ws, hdr, r, "Notes", out)
                swept += 1
                changed += 1
    if swept:
        report.append(f"  SWEEP {swept} row(s) lost a stale cleanup stamp")

    print(f"repair_2026_11_27: {changed} row edit(s)")
    for line in report:
        print(line)

    if args.dry_run:
        print("\n(--dry-run: nothing written)")
        return 0

    if changed:
        wb.save(path)
        print(f"wrote {path}")
        # recalls.json MUST mirror the Recalls sheet; mirror_json_from_xlsx is
        # the only sanctioned way to produce it. Without this the dashboard
        # and the language tests read the pre-repair values.
        from pipeline.merge_master import mirror_json_from_xlsx
        print(f"  recalls.json re-mirrored: "
              f"{mirror_json_from_xlsx(path, JSON_PATH)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
