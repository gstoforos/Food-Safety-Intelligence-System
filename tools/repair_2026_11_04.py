# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-04.

Base: main at 8eefd23e ("Analytical schema sweep 2026-11-04").

Every value set below was read this morning on the regulator's own page, or
is a faithful translation of text already on the row it is written to. NO
ROW LEAVES Recalls, so the commit message carries no `remove-rows` token.
One row leaves PENDING — Pending is a queue, not an archive; the row is
copied into Rejected with its verdict and nothing is deleted.

FIVE TESTS WERE RED ON MAIN AT THE BASE SHA. Four of the five sections
below are the data half of those failures; the code half is in
pipeline/recall_confirm_agent.py and pipeline/_publish_gate.py.

    test_cleanup_stamps_are_idempotent [Recalls]        -> A
    test_cleanup_stamps_are_idempotent [Weekly_Review]  -> A
    test_a_recall_is_not_both_published_and_rejected
        [Rejected-RejectReason]                         -> D
    test_a_reachability_refusal_is_not_a_verdict         -> E
    test_register_urls_pass_their_own_gate[ch]           -> C


A. TRANSLATION, AND A DUPLICATED STAMP ON FOUR ROWS
===================================================
Six published rows carried a live "[needs cleanup: …]" stamp or a
non-English published field this morning. The morning pass's standing job
is to translate them and discharge the stamp.

Two of them — RappelConso fiches 23987 and 23988, published by reviewer 3
at 04:27 UTC today — carried the Product stamp TWICE, in Recalls and again
in their Weekly_Review mirror:

    "carrefour hyper chelles uniquement
     [needs cleanup: Product not in English]
     [confirm-agent 2026-11-04: pending → pending; deterministic checks
      passed; reviewer 2 did NOT review this row]
     [needs cleanup: Product not in English]"

The writer is pipeline/recall_confirm_agent.py, which built its stamp
fragment with three bare `stamp += " [needs cleanup: …]"` and glued it onto
Notes that merge_master._write_sheet had already stamped. Fixed at the
writer in the same pass (see language_cleanup_stamps there, and the
widened source-level check in tests/test_cleanup_stamps_are_idempotent.py,
whose old regex only looked for an append to a `["Notes"]` subscript and so
could not see an accumulator).

Read on the fiches themselves this morning:

  23987  Dénomination "Thon longe"; Marque "Sans marque"; Modèles ou
         références "Thon longe nature vendu au rayon traditionnel / Thon
         longe mariné vendu en au rayon LS"; Motif "Présence d'histamine";
         Risques "Toxines endogènes : histamine"; distributeur "Carrefour
         Hyper Chelles UNIQUEMENT"; publiée le 03/11/2026.
         ("rayon traditionnel" is the served/service counter; "rayon LS"
         is libre-service, the self-service chiller.)

  23988  Dénomination "Morteau cuite"; Marque "Salaison Premier Plateau";
         Modèles ou références "5110026 - SAUCISSE DE MORTEAU CUITE IGP
         FRANCE 300G*1 - LS PF / 5110028 - SAUCISSE DE MORTEAU CUITE IGP
         FRANCE 300G*1 LS PV"; Motif "Présence de Listéria Monocytogènes";
         Risques "Listeria monocytogenes (agent responsable de la
         listériose)"; origine "PREMIER PLATEAU"; distributeurs
         "Scapalsace ITM epenotte Dole"; publiée le 03/11/2026.
         IGP is a protected designation and is kept as written.

  23983  Dénomination "Salade de museau préparée sur place"; Marque "sans
         marque"; Modèles ou références "//"; catégorie Alimentation /
         Viandes; Motif "Présence de Listeria monocytogenes"; Risques
         "Listeria monocytogenes (agent responsable de la listériose)";
         distributeur "Intermarché Fos sur mer"; GTIN "0200860000000";
         période de commercialisation "Du 19/10/2026 au 27/10/2026";
         publiée le 02/11/2026.
         THIS ROW CARRIED NO STAMP AND IS THE REASON THIS PASS SWEPT FOR
         THE SHAPE RATHER THAN THE MARKER. Its published Product was
         "0200860000000; non concerné" — a barcode and the French for "not
         applicable". The language detector cannot see that: there is no
         French WORD in it. Same defect class as fiche 23552 ("Unbranded")
         on 2026-09-18 and fiche 23951 on 2026-10-29.

  22320  Dénomination "CENDAWAN ENOKI, ENOKI MUSHROOM (champignons
         ENOKI)"; Marque "GREEN BOX Limited"; Modèles ou références "Lot
         G22014 Code barre 6936489101348"; Motif "Présence de Listeria
         monocytogenes."; publiée le 20/05/2026. Published Product was
         "Lot g22014; barcode 6936489101348" — the reference field, not a
         food. Found by the same sweep.

Translated from the row's OWN text, no page read needed and no fact added:

  23982  "rillettes pur canard au magret fume 180g" and the hybrid Reason
         "Presence of corps étranger (verre)".
  23980  "bouteille jus de pomme 1l" and "Presence of patuline".
  1182   "Mini Brie cheese, Brie Torten ½" — the FSVO's own title is
         already quoted in this row's Notes: "Eberle Spezialitäten AG ruft
         weitere Chargen der Produkte «Minibrie gefüllt und Brie Torten ½
         gefüllt» wegen Listerien zurück".

NOT TRANSLATED, AND WHY. RappelConso fiches 22886 (published Product
"dss3001021-00", 2026-07-20) and 22715 ("Lot 260226", 2026-07-02) carry
the same reference-code-as-Product defect. Both fiches served this sandbox
the site's generic all-categories listing instead of the fiche this
morning, so their Dénomination could not be read, and a Product cannot be
written from anything else. Left exactly as they are and reported.


B. TWO PENDING ROWS BLOCKED ON A FIELD THAT IS IN THEIR OWN TEXT
================================================================
B1. BLV, 2026-11-03, blv.admin.ch/de/newnsb/oBmdo4JjUtwF — Company EMPTY,
    Product "Medienmitteilung Veröffentlicht am 3" (a page crumb), status
    pending_gap_v1, so the promoter reported "Company is empty" and the
    row has sat unqueued since the overnight merge.

    Read on that page this morning: "Öffentliche Warnung: Listerien in
    weiteren Weichkäsen von Eberle Spezialitäten AG sowie in Weichkäsen
    von Strähl Käse AG, verkauft bei Coop, Migros, Volg Konsumwaren AG und
    diversen Verkaufsstellen", Medienmitteilung of 3 November 2026, headed
    "Öffentliche Warnung (2. Ergänzung)". Both firms have withdrawn the
    products and started a recall; Listeria monocytogenes was found and a
    health risk "kann nicht ausgeschlossen werden". The affected items are
    named only as "verschiedenen Weichkäsen" on the page itself — the
    item list is in a linked PDF ("Betroffene Weichkäse-Produkte") that
    this sandbox could not open — so Product is the page's own wording and
    nothing more. The page records earlier warnings on 27 and 29 October
    and says this one extends them because a further, older batch was
    found contaminated.

    THIS IS EXPECTED TO BE REFUSED AS A NEAR-DUPLICATE, and that is the
    right outcome to accept rather than to work around. Recalls already
    holds the FIRST supplement at .../de/newnsb/9ELyAwEjEEWJ, 2026-10-29,
    Company "Eberle Spezialitäten AG, Strähl Käse AG", Product "Various
    cheese products", Tier 1 — the same two firms, the same hazard and the
    same headline one supplement earlier. Company is written with the
    register's existing spelling for exactly that reason: if the row does
    publish, it must not make one firm pair into two.

B2. RappelConso fiche 24003, 2026-11-03, Champibon — Product held only lot
    codes ("3760201609743; tous les lots; date limite de conso") and the
    Reason was French. Read on the fiche this morning: réf.
    2026-11-0023 v1, Dénomination "CHAMPIGNONS MORILLES SECHEES 60G",
    Marque "CHAMPIBON", Modèles ou références "534199", catégorie
    Alimentation / Autres, Motif "Probable contamination d'un lot de
    morilles par une toxine susceptible d'être à l'origine d'intoxication
    alimentaire", Risques "Autre risque" with symptoms "diarrhées et/ou
    vomissements", GTIN "3760201609743", lot "Tous les lots", DLC
    01/12/2029, distributeur "B&M France SAS", vendue "Du 21/10/2026 au
    03/11/2026", "France entière", publiée le 03/11/2026.

    PATHOGEN IS LEFT EMPTY ON PURPOSE AND THE ROW STAYS IN PENDING. The
    fiche says "une toxine" and names none, so there is nothing to put in
    the hazard column that is not an inference about which toxin — and the
    register's hazard vocabulary has no term for an unnamed toxin
    (the nearest, "Unspecified microbiological contamination", asserts
    micro-organisms the fiche does not mention). Whether to add one is
    George's call; it is a vocabulary decision, not a data fix, and
    tools/alert_vocab.py would have to learn the term in the same change
    so every published hazard stays reachable by a subscriber alert.


C. THE BLV RECALL BOARD, QUEUED AS A RECALL FOR THE THIRD TIME
===============================================================
Pending held, at status "pending_gap" since 2026-10-29:

    Source BLV · Country Switzerland · Tier 1 · Listeria monocytogenes
    Company "Mises en garde et rappels"
    Product "aliments"
    URL     https://www.blv.admin.ch/fr/mises-en-garde-et-rappels-aliments

Company and Product are the two halves of the page title "Mises en garde
et rappels — aliments". The URL is the BLV's recall BOARD; it lists every
Swiss warning and names no single recall, which is why
pipeline/gap_finder/countries/ch.py has refused it as an item since
2026-09-23 and holds tests/test_register_urls_pass_their_own_gate[ch] RED
while it sits in a sheet.

Rejected already holds this same board URL twice, 2026-08-26 and
2026-09-13, and Weekly_Rejected once, 2026-10-29. This is the third
ingestion of one listing page; the verdict is unchanged.

MEASURED, and worth George's attention: `python -m
pipeline.promote_gate_passing` reported this row as the ONE row of ten
that passes the FULL publish gate. The only thing that kept a listing page
out of the register was merge_master's re-promotion guard finding the
earlier archive copy — a coincidence of the archive, not a gate. The gate
itself is fixed in pipeline/_publish_gate.py in this pass.

The two real recalls this board was listing on 29 October are both
accounted for: Wander AG "Ovo Crunchy Ice Mini" (wooden foreign bodies) is
published at recallswiss .../#Recalls/1181, and the Lidl Suisse cream
cheese warning of 28 October belongs to the Strähl Käse AG Listeria series
already in the register. Nothing is lost by archiving the board row.


D. FIVE ARCHIVE ROWS CONTRADICTING THE REGISTER IN SILENCE
===========================================================
Five rows in Rejected share a URL with a PUBLISHED recall and say nothing
in their RejectReason column about which copy wins.

THREE OF THEM WERE ALREADY ANNOTATED — IN THE WRONG COLUMN. The morning
passes of 2026-10-23 and 2026-10-24 wrote their resolution into **Notes**,
and one of those notes even says "see the RejectionReason banner":

  salute.gov.it .../LOTTO%20596_1792593268.pdf   Notes: "archived verdict
      superseded — another recall's allergen verdict, see the
      RejectionReason banner"            RejectReason: EMPTY
  salute.gov.it .../Tonno%20Surgelato_1792575197.pdf
      Notes: "SUPERSEDED — the published copy wins"
                                         RejectReason: EMPTY
  alerts.food.gov.uk/article/6920        Notes: "duplicate: the same alert
      … is already published"            RejectReason: EMPTY

"RejectionReason" is the Weekly_Rejected column name. The Rejected sheet
calls it "RejectReason". So the banner went to Notes, which no test and no
guard reads, and the contradiction stayed silent in the column that
matters. tests/test_a_recall_is_not_both_published_and_rejected reads the
sheet's own reason column, and it is right to.

TWO WERE NEVER ANNOTATED AT ALL, both AESAN alerts whose published copy
was enriched afterwards by another path:

  aesan.gob.es/alertas/2026_74     archive Product "23 hours ago · La
      Agencia Española de Seguridad Alimentaria …" — a search-result
      snippet. Published copy: 2026-10-23, "Pasteurised egg white, 500 ml
      plastic bottle, chilled", Salmonella.
  aesan.gob.es/fr/alertas/2026_75  archive Product "2 days ago · La
      Agencia Española …". Published copy: 2026-10-26, "Pure Sesame
      Tahini, 400 g jar", Salmonella. (/fr/ and the bare path are one
      document; merge_master._normalize_url_for_dedup collapses them,
      which is why the test sees the pair and an exact-string check does
      not.)

In all five the PUBLISHED copy wins: it carries the product the regulator
names, and the archive copy carries a news snippet or another recall's
verdict. Each is marked SUPERSEDED in its own sheet's reason column.
Nothing is deleted; archives are append-only.


E. A GREEK RECALL ARCHIVED ON A REACHABILITY REASON THAT IS NOT WHY IT
   SHOULD HAVE BEEN ARCHIVED
=======================================================================
Weekly_Rejected, 2026-10-22, EFET (GR), Tier 1, Salmonella:

    Company "FarmakeioAthens.com"
    Product "Chicken breast"
    URL     efet.gr/…/item/5390-deltio-typou-anaklisi-biftekioy-kotopoyloy-…
    RejectionReason "Confirmer: row was at pending_gap_v2 (not reviewed by
        reviewer 2) and the cited page does not exist (http_404) — the
        regulator publishes no such notice at this URL"

pipeline/_url_guard.reject_refusal reads that wording as a statement about
REACHABILITY, which it is, so the row counts as thrown away on a fetch
failure while holding a URL on efet.gr — an authority host — and passing
every publish-gate rule. That is what holds
tests/test_a_reachability_refusal_is_not_a_verdict RED, and the test's own
remedy is "run promote_gate_passing --apply", i.e. publish it.

IT MUST NOT BE PUBLISHED AS IT STANDS, AND THE REASON IS ON THE ROW.
"FarmakeioAthens.com" is a website domain, not a recalling firm — a
news-page artefact, exactly the shape of defect
pipeline/recall_review_agent._field_integrity_flags exists to catch. That
is a CONTENT verdict, available from the row's own text, and it is the
real reason this row cannot publish. The reason column is restated to say
so; reject_refusal then returns "" for it, as it does for every content
verdict, and the row stops being counted as a reachability casualty.

Nothing is lost: the confirmer's original wording is preserved verbatim in
this row's own Notes, where it already sits ("REJECTED: Confirmer: row was
at pending_gap_v2 … (http_404) …"), and the annotation says what changed.

THE RECALL IS REAL AND IS NOT IN THE REGISTER. Greek press of 22 October
2026 (iefimerida.gr, athensvoice.gr, libre.gr, alfavita.gr) reports EFET
recalling "ΜΠΙΦΤΕΚΙ ΚΟΤΟΠΟΥΛΟ HAMBURGER (κατεψυγμένο)", 850 g, lot 15626,
producer "ΜΕΑΤΚΑ Α.Ε.", after Salmonella spp. was found. That is NEWS —
discovery only — so not one word of it is written into a published field;
it is recorded here and in the row's Notes as a lead for the EFET
scraper, which is the thing that can read efet.gr. efet.gr returns a
client error to this sandbox, so the per-recall URL could not be confirmed
from here. George's call, flagged in the report.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

TODAY = "2026-11-04"
BY = "morning fix pass " + TODAY

PRODUCT_STAMP = "[needs cleanup: Product not in English]"
REASON_STAMP = "[needs cleanup: Reason not in English]"

F23987 = "https://rappel.conso.gouv.fr/fiche-rappel/23987/interne"
F23988 = "https://rappel.conso.gouv.fr/fiche-rappel/23988/interne"
F23983 = "https://rappel.conso.gouv.fr/fiche-rappel/23983/interne"
F23982 = "https://rappel.conso.gouv.fr/fiche-rappel/23982/interne"
F23980 = "https://rappel.conso.gouv.fr/fiche-rappel/23980/interne"
F24003 = "https://rappel.conso.gouv.fr/fiche-rappel/24003/interne"
F22320 = "https://rappel.conso.gouv.fr/fiche-rappel/22320/Interne"
CH1182 = "https://www.recallswiss.admin.ch/customer-access/#Recalls/1182"
BLV_NOV3 = "https://www.blv.admin.ch/de/newnsb/oBmdo4JjUtwF"
BLV_BOARD = "https://www.blv.admin.ch/fr/mises-en-garde-et-rappels-aliments"
EFET_5390 = ("https://www.efet.gr/index.php/el/enimerosi/deltia-typou/"
             "anakleiseis-cat/item/5390-deltio-typou-anaklisi-biftekioy-"
             "kotopoulou-logo-parousias-salmonella-spp")

IT_LOTTO596 = ("https://www.salute.gov.it/new/sites/default/files/"
               "external_data/avvisi_sicurezza_alimentare/"
               "cartello%20di%20richiamo%20modello%20ministero%20"
               "LOTTO%20596_1792593268.pdf")
IT_TONNO = ("https://www.salute.gov.it/new/sites/default/files/"
            "external_data/avvisi_sicurezza_alimentare/"
            "Cartello%20Richiamo%20Filetti%20di%20Tonno%20"
            "Surgelato_1792575197.pdf")
UK_6920 = ("https://alerts.food.gov.uk/article/6920/Greencore-recalls-"
           "several-products-due-to-the-presence-of-Salmonella")
ES_2074 = "https://www.aesan.gob.es/alertas/2026_74"
ES_2075 = "https://www.aesan.gob.es/fr/alertas/2026_75"


# ── A. Recalls + Weekly_Review: translate, then discharge the stamp ───────
#
# The same specs are applied to BOTH sheets. Weekly_Review is a mirror of
# what was promoted, read by the operator's Sunday review email, and two of
# these rows carry the duplicated stamp there as well — a fix applied only
# to Recalls would leave the mirror disagreeing with the register and the
# test red for [Weekly_Review].
TRANSLATIONS = [
    {
        "url": F23987,
        "set": {
            "Product": ("Tuna loin: plain tuna loin sold at the service "
                        "counter; marinated tuna loin sold in the "
                        "self-service chiller"),
        },
        "drop_stamps": [PRODUCT_STAMP],
        "note": (f"[{BY}: Product translated from fiche 23987's own French, "
                 f"read on the fiche today — Dénomination \"Thon longe\", "
                 f"Modèles ou références \"Thon longe nature vendu au rayon "
                 f"traditionnel / Thon longe mariné vendu en au rayon LS\" "
                 f"(rayon traditionnel = the served counter, rayon LS = "
                 f"libre-service, the self-service chiller). Marque \"Sans "
                 f"marque\", which is what Brand \"Unbranded\" records. "
                 f"Reason was already English. THE STAMP WAS ON THIS ROW "
                 f"TWICE: merge_master._write_sheet stamped it at the "
                 f"hourly merge and pipeline/recall_confirm_agent.py "
                 f"appended a second copy at 04:27 UTC today with three "
                 f"unguarded `stamp += \"…\"` lines. The writer is fixed in "
                 f"this pass; both copies are dropped here.] "
                 f"[original product: Thon longe nature vendu au rayon "
                 f"traditionnel / Thon longe mariné vendu en au rayon LS]"),
    },
    {
        "url": F23988,
        "set": {
            "Product": ("Cooked Morteau sausage (saucisse de Morteau cuite "
                        "IGP France), 300 g — refs 5110026 (LS PF) and "
                        "5110028 (LS PV)"),
            "Reason": "Presence of Listeria monocytogenes",
        },
        "drop_stamps": [PRODUCT_STAMP, REASON_STAMP],
        "note": (f"[{BY}: Product and Reason translated from fiche 23988's "
                 f"own French, read on the fiche today — Dénomination "
                 f"\"Morteau cuite\", Modèles ou références \"5110026 - "
                 f"SAUCISSE DE MORTEAU CUITE IGP FRANCE 300G*1 - LS PF / "
                 f"5110028 - SAUCISSE DE MORTEAU CUITE IGP FRANCE 300G*1 LS "
                 f"PV\", Motif \"Présence de Listéria Monocytogènes\", "
                 f"Risques \"Listeria monocytogenes (agent responsable de la "
                 f"listériose)\". IGP is a protected designation and is kept "
                 f"as written, as are the two reference codes and their "
                 f"shelf suffixes. The published Reason spelled the organism "
                 f"the fiche's way (\"listéria monocytogènes\"); the "
                 f"register publishes the scientific name. The Product stamp "
                 f"was on this row twice — see fiche 23987's note for the "
                 f"writer defect — and both copies are dropped.] "
                 f"[original product: 5110026 - SAUCISSE DE MORTEAU CUITE "
                 f"IGP FRANCE 300G*1 - LS PF / 5110028 - SAUCISSE DE MORTEAU "
                 f"CUITE IGP FRANCE 300G*1 LS PV] "
                 f"[original Reason (fr): \"Présence de Listéria "
                 f"Monocytogènes\"]"),
    },
    {
        "url": F23983,
        "set": {
            "Product": ("Muzzle salad (salade de museau), prepared in "
                        "store — GTIN 0200860000000"),
        },
        "note": (f"[{BY}: the published Product was \"0200860000000; non "
                 f"concerné\" — a barcode and the French for \"not "
                 f"applicable\", which is a reference field and a "
                 f"placeholder, not a food. Read on fiche 23983 today: "
                 f"Dénomination \"Salade de museau préparée sur place\", "
                 f"Marque \"sans marque\", Modèles ou références \"//\", "
                 f"catégorie Alimentation / Viandes, Motif \"Présence de "
                 f"Listeria monocytogenes\", Risques \"Listeria "
                 f"monocytogenes (agent responsable de la listériose)\", "
                 f"distributeur \"Intermarché Fos sur mer\", GTIN "
                 f"\"0200860000000\", commercialisée du 19/10/2026 au "
                 f"27/10/2026, publiée le 02/11/2026. The French dish name "
                 f"is kept in brackets because \"museau\" is the cut and has "
                 f"no single English word; no cut, species or weight is "
                 f"added that the fiche does not give. THIS ROW CARRIED NO "
                 f"\"needs cleanup\" STAMP: the detector looks for "
                 f"non-English words and there is no French WORD in a "
                 f"barcode, which is why this pass swept every published "
                 f"Product for the SHAPE rather than for the marker — the "
                 f"same hole that left fiche 23552 reading \"Unbranded\" "
                 f"from 18 September to 30 October.] "
                 f"[original product: 0200860000000; non concerné]"),
    },
    {
        "url": F23982,
        "set": {
            "Product": ("Pure duck rillettes with smoked duck breast, "
                        "180 g"),
            "Reason": "Presence of foreign material (glass)",
        },
        "drop_stamps": [PRODUCT_STAMP, REASON_STAMP],
        "note": (f"[{BY}: Product and Reason translated from this row's own "
                 f"text — no page read was needed and no fact is added. "
                 f"\"rillettes pur canard au magret fume 180g\" is pure-duck "
                 f"rillettes made with smoked duck breast (magret fumé), "
                 f"180 g. The stored Reason was a French/English hybrid, "
                 f"\"Presence of corps étranger (verre)\"; corps étranger = "
                 f"foreign material, verre = glass, which is also what the "
                 f"row's own Pathogen \"Foreign material (glass)\" already "
                 f"says.] "
                 f"[original product: rillettes pur canard au magret fume "
                 f"180g] "
                 f"[original Reason (as stored, fr/en hybrid): \"Presence of "
                 f"corps étranger (verre)\"]"),
    },
    {
        "url": F23980,
        "set": {
            "Product": "Apple juice, 1 L bottle",
            "Reason": "Presence of patulin",
        },
        "drop_stamps": [PRODUCT_STAMP, REASON_STAMP],
        "note": (f"[{BY}: Product and Reason translated from this row's own "
                 f"text. \"bouteille jus de pomme 1l\" is a 1-litre bottle "
                 f"of apple juice; \"patuline\" is the French for the "
                 f"mycotoxin patulin, which the register already publishes "
                 f"as \"Patulin\". No lot, GTIN or best-before is on the "
                 f"row, so none is added.] "
                 f"[original product: bouteille jus de pomme 1l] "
                 f"[original Reason (as stored, fr): \"Presence of "
                 f"patuline\"]"),
    },
    {
        "url": CH1182,
        "set": {
            "Product": ("Filled mini Brie cheese; filled Brie cheese "
                        "tortes, half size (Minibrie gefüllt, Brie Torten "
                        "½ gefüllt)"),
        },
        "drop_stamps": [PRODUCT_STAMP],
        "note": (f"[{BY}: Product translated from the FSVO title already "
                 f"quoted in this row's own Notes — \"Eberle Spezialitäten "
                 f"AG ruft weitere Chargen der Produkte «Minibrie gefüllt "
                 f"und Brie Torten ½ gefüllt» wegen Listerien zurück\". The "
                 f"published Product was half-German (\"Brie Torten ½\"), "
                 f"which is what reviewer 3's stamp was about. \"gefüllt\" = "
                 f"filled; the German designation is kept in brackets "
                 f"because \"Brie Torte\" is the product's own name. The "
                 f"recallswiss customer-access page is a JavaScript shell "
                 f"and serves no text to this sandbox, so nothing beyond the "
                 f"quoted title is used.] "
                 f"[original product: Minibrie gefüllt und Brie Torten ½ "
                 f"gefüllt]"),
    },
    {
        "url": F22320,
        "set": {
            "Product": ("Enoki mushrooms (CENDAWAN ENOKI / ENOKI MUSHROOM) "
                        "— lot G22014, barcode 6936489101348"),
        },
        "note": (f"[{BY}: the published Product was \"Lot g22014; barcode "
                 f"6936489101348\" — the fiche's reference field, not a "
                 f"food. Read on fiche 22320 today: Dénomination "
                 f"\"CENDAWAN ENOKI, ENOKI MUSHROOM (champignons ENOKI)\", "
                 f"Marque \"GREEN BOX Limited\", Modèles ou références "
                 f"\"Lot G22014 Code barre 6936489101348\", Motif "
                 f"\"Présence de Listeria monocytogenes.\", distributeur "
                 f"\"Multiples distributeurs\", publiée le 20/05/2026. The "
                 f"lot and barcode are kept, now after a food name. Found by "
                 f"the same shape sweep as fiche 23983. The two other rows "
                 f"that sweep found, fiches 22886 (\"dss3001021-00\") and "
                 f"22715 (\"Lot 260226\"), are NOT touched: both fiches "
                 f"served this sandbox the site's generic all-categories "
                 f"listing instead of the fiche, so their Dénomination could "
                 f"not be read, and a Product written from anything else "
                 f"would be invented.] "
                 f"[original product: Lot g22014; barcode 6936489101348]"),
    },
]


# ── B. Pending: fields that were already on the row or on its own page ───
PENDING_FIXES = [
    {
        "url": BLV_NOV3,
        "set": {
            "Company": "Eberle Spezialitäten AG, Strähl Käse AG",
            "Product": "Various soft cheeses",
            "Reason": ("Listeria monocytogenes found in further soft "
                       "cheeses; a health risk cannot be excluded"),
        },
        "note": (f"[{BY}: Company was EMPTY and Product held the page crumb "
                 f"\"Medienmitteilung Veröffentlicht am 3\", so the offline "
                 f"promoter reported \"Company is empty\" and the row sat "
                 f"unqueued. Both firms are named in this row's own stored "
                 f"title and were re-read on the BLV page today: "
                 f"\"Öffentliche Warnung: Listerien in weiteren Weichkäsen "
                 f"von Eberle Spezialitäten AG sowie in Weichkäsen von "
                 f"Strähl Käse AG, verkauft bei Coop, Migros, Volg "
                 f"Konsumwaren AG und diversen Verkaufsstellen\", "
                 f"Medienmitteilung of 3 November 2026, headed "
                 f"\"Öffentliche Warnung (2. Ergänzung)\". Listeria "
                 f"monocytogenes was found and a health risk \"kann nicht "
                 f"ausgeschlossen werden\"; both firms have withdrawn the "
                 f"products and started a recall. Product is the page's own "
                 f"wording, \"verschiedenen Weichkäsen\", and NOTHING MORE: "
                 f"the item list is in a linked PDF (\"Betroffene "
                 f"Weichkäse-Produkte\") that this sandbox could not open. "
                 f"Company is written with the spelling Recalls already uses "
                 f"for this firm pair at .../de/newnsb/9ELyAwEjEEWJ so one "
                 f"pair does not become two. EXPECTED TO BE REFUSED AS A "
                 f"NEAR-DUPLICATE of that 2026-10-29 row — same two firms, "
                 f"same hazard, same headline one supplement earlier — and "
                 f"that refusal is accepted, not worked around.] "
                 f"[original product: Medienmitteilung Veröffentlicht am 3] "
                 f"[original company field: empty]"),
    },
    {
        "url": F24003,
        "set": {
            "Product": ("Dried morel mushrooms, 60 g (CHAMPIGNONS MORILLES "
                        "SECHEES 60G) — ref 534199, GTIN 3760201609743, all "
                        "lots, use-by 01/12/2029"),
            "Reason": ("Probable contamination of a lot of morels by a "
                       "toxin liable to cause food poisoning"),
        },
        "drop_stamps": [REASON_STAMP],
        "note": (f"[{BY}: Product held only lot codes and the Reason was "
                 f"French. Read on fiche 24003 today (réf. 2026-11-0023, "
                 f"version 1): Dénomination \"CHAMPIGNONS MORILLES SECHEES "
                 f"60G\", Marque \"CHAMPIBON\", Modèles ou références "
                 f"\"534199\", catégorie Alimentation / Autres, Motif "
                 f"\"Probable contamination d'un lot de morilles par une "
                 f"toxine susceptible d'être à l'origine d'intoxication "
                 f"alimentaire\", Risques \"Autre risque\" with symptoms "
                 f"\"diarrhées et/ou vomissements\", GTIN "
                 f"\"3760201609743\", lot \"Tous les lots\", DLC "
                 f"01/12/2029, distributeur \"B&M France SAS\", vendue du "
                 f"21/10/2026 au 03/11/2026, \"France entière\", publiée le "
                 f"03/11/2026. PATHOGEN IS LEFT EMPTY AND THE ROW STAYS IN "
                 f"PENDING: the fiche says \"une toxine\" and names none, so "
                 f"any hazard value would be an inference about which toxin, "
                 f"and the register's vocabulary has no term for an unnamed "
                 f"one. George's call — a new term would have to be taught "
                 f"to tools/alert_vocab.py in the same change so every "
                 f"published hazard stays reachable by an alert.] "
                 f"[original product: 3760201609743; tous les lots; date "
                 f"limite de consommation 01/12/2029] "
                 f"[original Reason (fr): \"Probable contamination d'un lot "
                 f"de morilles par une toxine susceptible d'être à "
                 f"l'origine d'intoxication alimentaire\"]"),
    },
]


# ── C. Pending -> Rejected ────────────────────────────────────────────────
PENDING_ARCHIVE = [
    {
        "url": BLV_BOARD,
        "status": "pending_gap",
        "reason": (f"Refused by the {BY} on R3 (the regulator's OWN "
                   f"per-recall URL): this URL is the BLV's recall BOARD, "
                   f"blv.admin.ch/fr/mises-en-garde-et-rappels-aliments. It "
                   f"lists every Swiss warning and names no single recall, "
                   f"and the row's Company \"Mises en garde et rappels\" "
                   f"with Product \"aliments\" are the two halves of that "
                   f"page's title. pipeline/gap_finder/countries/ch.py has "
                   f"refused this URL as an item since 2026-09-23, naming "
                   f"this exact failure in its own comment. A DUPLICATE "
                   f"verdict: Rejected already holds this board URL on "
                   f"2026-08-26 and 2026-09-13, and Weekly_Rejected on "
                   f"2026-10-29. The two real recalls the board was listing "
                   f"on 29 October are both accounted for — Wander AG \"Ovo "
                   f"Crunchy Ice Mini\" is published at recallswiss "
                   f".../#Recalls/1181 and the Lidl Suisse cream cheese "
                   f"warning of 28 October belongs to the Strähl Käse AG "
                   f"Listeria series already in the register — so no recall "
                   f"is lost. NOTE FOR THE GATE: promote_gate_passing "
                   f"reported this row as the one row of ten passing the "
                   f"FULL publish gate; only the re-promotion guard's sight "
                   f"of the earlier archive copy kept a listing page out of "
                   f"the register. pipeline/_publish_gate.py now refuses the "
                   f"board in all of its languages."),
    },
]


# ── D. Archive verdicts: SUPERSEDED, in the sheet's own reason column ─────
SUPERSEDE = [
    {
        "url": IT_LOTTO596,
        "note": (f"[SUPERSEDED {TODAY} by the {BY}: the published copy wins. "
                 f"Recalls carries this ministry notice as 2026-10-22, "
                 f"\"Brezzi Oliviero Tartufi\", \"Truffle sauce (salsa "
                 f"tartufata), lot 596\", Botulinum toxin, on this same PDF. "
                 f"This archive row holds the ilfattoalimentare.it headline "
                 f"as its Product and \"uova\" as its Pathogen — another "
                 f"recall's allergen verdict, as the Notes below already "
                 f"record. THE BANNER WENT TO THE WRONG COLUMN ON "
                 f"2026-10-24: those Notes say \"see the RejectionReason "
                 f"banner\", but this sheet's column is RejectReason and it "
                 f"was left empty, so the contradiction stayed silent where "
                 f"it is read. Nothing is deleted.]"),
    },
    {
        "url": IT_TONNO,
        "note": (f"[SUPERSEDED {TODAY} by the {BY}: the published copy wins. "
                 f"Recalls carries this ministry notice as 2026-10-20, "
                 f"\"IN’s Mercato\", \"Tuna fillets\", Histamine / "
                 f"scombrotoxin, on this same PDF. This archive row holds "
                 f"the news headline as its Product and \"ei\" as its "
                 f"Pathogen. Its Notes already said \"SUPERSEDED — the "
                 f"published copy wins\" on 2026-10-23; the marker is now "
                 f"in this sheet's own RejectReason column, which is the one "
                 f"the guard and the test read. Nothing is deleted.]"),
    },
    {
        "url": UK_6920,
        "note": (f"[SUPERSEDED {TODAY} by the {BY}: DUPLICATE — the "
                 f"published copy wins. Recalls carries FSA alert 6920 as "
                 f"2026-10-08, Greencore, \"several products\", Salmonella, "
                 f"on this same URL. This row's own Notes already state "
                 f"\"duplicate: the same alert (same date, firm and "
                 f"pathogen on alerts.food.gov.uk) is already published\"; "
                 f"the marker is now in the RejectReason column too. "
                 f"Nothing is deleted.]"),
    },
    {
        "url": ES_2074,
        "note": (f"[SUPERSEDED {TODAY} by the {BY}: the published copy wins. "
                 f"Recalls carries AESAN alert 2026_74 as 2026-10-23, "
                 f"\"Pasteurised egg white, 500 ml plastic bottle, "
                 f"chilled\", Salmonella, on this same URL. This archive "
                 f"row's Product is a search-result snippet (\"23 hours ago "
                 f"· La Agencia Española de Seguridad Alimentaria …\"), "
                 f"which is what the confirmer rejected it for — \"Product "
                 f"looks like a headline, not a product name\" — a correct "
                 f"verdict on this COPY that was never marked as such, so "
                 f"the sheet read as a contradiction of the register. "
                 f"Nothing is deleted.]"),
    },
    {
        "url": ES_2075,
        "note": (f"[SUPERSEDED {TODAY} by the {BY}: the published copy wins. "
                 f"Recalls carries AESAN alert 2026_75 as 2026-10-26, "
                 f"\"Pure Sesame Tahini, 400 g jar\", Salmonella, at "
                 f"aesan.gob.es/alertas/2026_75 — this row cites the /fr/ "
                 f"path of the same document, which "
                 f"merge_master._normalize_url_for_dedup collapses onto the "
                 f"same key (an exact-string check does not, which is why "
                 f"this pair was invisible until the test that uses the "
                 f"writer's own key found it). This archive row's Product is "
                 f"a search-result snippet (\"2 days ago · La Agencia "
                 f"Española …\"). Its stated reason, \"The official URL does "
                 f"not point to a specific food recall page\", is wrong "
                 f"about the page — alert 2026_75 is a specific AESAN alert "
                 f"and the published row proves it — but the COPY is still "
                 f"correctly out, on its Product. Nothing is deleted.]"),
    },
]


# ── E. One archive verdict restated from reachability to content ──────────
RESTATE = [
    {
        "sheet": "Weekly_Rejected",
        "url": EFET_5390,
        "reason": (
            f"field integrity: Company \"FarmakeioAthens.com\" is a website "
            f"domain, not the recalling firm — a news-page artefact of the "
            f"kind pipeline/recall_review_agent._field_integrity_flags "
            f"exists to refuse — and Product \"Chicken breast\" does not "
            f"name what this EFET notice recalled. A verdict on the row's "
            f"own CONTENT, which is the real reason this copy cannot "
            f"publish."),
        "note": (
            f"[{BY}: RejectionReason restated, from the confirmer's "
            f"reachability wording to the content verdict that actually "
            f"disqualifies this row. NOTHING IS LOST — the original wording "
            f"is preserved verbatim in these Notes above ("
            f"\"REJECTED: Confirmer: row was at pending_gap_v2 (not reviewed "
            f"by reviewer 2) and the cited page does not exist (http_404) — "
            f"the regulator publishes no such notice at this URL\"). WHY: "
            f"pipeline/_url_guard.reject_refusal reads that wording as a "
            f"statement about REACHABILITY, which it is, so this row counted "
            f"as thrown away on a fetch failure while holding a URL on "
            f"efet.gr and passing every publish-gate rule — "
            f"tests/test_a_reachability_refusal_is_not_a_verdict was RED on "
            f"it and its remedy is to publish. It must not be published as "
            f"it stands: \"FarmakeioAthens.com\" is not a chicken processor, "
            f"and that defect is readable from the row itself. THE RECALL IS "
            f"REAL AND IS NOT IN THE REGISTER: Greek press of 22 October "
            f"2026 (iefimerida.gr, athensvoice.gr, libre.gr, alfavita.gr) "
            f"reports EFET recalling \"ΜΠΙΦΤΕΚΙ ΚΟΤΟΠΟΥΛΟ HAMBURGER "
            f"(κατεψυγμένο)\", 850 g, lot 15626, producer \"ΜΕΑΤΚΑ Α.Ε.\", "
            f"after Salmonella spp. was found. That is NEWS — discovery "
            f"only — so not one word of it is written into a published "
            f"field; it is recorded here as a lead for the EFET scraper, "
            f"which can read efet.gr. efet.gr returns a client error to this "
            f"sandbox, so the per-recall URL could not be confirmed from "
            f"here.]"),
    },
]


# --------------------------------------------------------------------------

def _head(ws):
    return [c.value for c in ws[1]]


def _rowdict(ws, head, r):
    return {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}


def _set(ws, head, r, field, value):
    ws.cell(r, head.index(field) + 1).value = value


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _apply_notes(ws, head, r, row, note, drop_stamps):
    notes = str(row.get("Notes") or "").strip()
    if notes.lower() in ("none", "nan"):
        notes = ""
    for stamp in drop_stamps or ():
        # EVERY copy, not the first: the whole point of section A is that a
        # writer appended one of these twice.
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
        target = _key(spec["url"])
        hit = False
        for r in range(2, ws.max_row + 1):
            row = _rowdict(ws, head, r)
            if _key(row.get("URL")) != target:
                continue
            want = spec.get("status")
            if want is not None and str(row.get("Status") or "").strip() != want:
                continue
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


def _archive_pending(wb) -> int:
    """Pending -> Rejected, with the verdict in the archive's own reason
    column. Pending is a queue, not an archive; Rejected is append-only and
    nothing is deleted from it."""
    ws = wb["Pending"]
    head = _head(ws)
    arch = wb["Rejected"]
    done = 0
    for spec in PENDING_ARCHIVE:
        target = _key(spec["url"])
        want = spec.get("status")
        hit = False
        for r in range(ws.max_row, 1, -1):
            row = _rowdict(ws, head, r)
            if _key(row.get("URL")) != target:
                continue
            if want is not None and str(row.get("Status") or "").strip() != want:
                continue
            _archive(arch, row, spec["reason"], BY)
            ws.delete_rows(r, 1)
            done += 1
            hit = True
            print(f"  Pending -> Rejected: {str(row.get('Source'))[:18]!r} "
                  f"{str(row.get('Product'))[:44]!r}")
        if not hit:
            raise SystemExit(
                f"Pending: no row at {spec['url']} (status={want!r}) — main "
                f"has moved under this repair script; re-read the sheet")
    return done


def _supersede_archive(wb) -> int:
    """Append "[SUPERSEDED <date> — …]" to an archive row's own reason
    column. Never deletes: archives are append-only, and this only stops a
    stale verdict reading as a live contradiction of the register."""
    done = 0
    for spec in SUPERSEDE:
        target = _key(spec["url"])
        hit = False
        for sheet in ("Weekly_Rejected", "Rejected"):
            if sheet not in wb.sheetnames:
                continue
            ws = wb[sheet]
            head = _head(ws)
            why = ("RejectionReason" if "RejectionReason" in head
                   else "RejectReason" if "RejectReason" in head else None)
            if why is None or "URL" not in head:
                continue
            for r in range(2, ws.max_row + 1):
                row = _rowdict(ws, head, r)
                if _key(row.get("URL")) != target:
                    continue
                existing = str(row.get(why) or "").strip()
                if existing.lower() in ("", "nan", "none"):
                    existing = ""
                hit = True
                if "superseded" in existing.lower():
                    continue
                _set(ws, head, r, why, (existing + " " + spec["note"]).strip())
                done += 1
                print(f"  {sheet}:{r}: verdict marked SUPERSEDED — "
                      f"{spec['url'][-46:]}")
        if not hit:
            raise SystemExit(
                f"no archive row found at {spec['url']} — the register said "
                f"one contradicts it; re-read the sheets")
    return done


def _restate_archive(wb) -> int:
    """Replace an archive verdict that names the wrong defect.

    The superseded-marker route cannot help here: reject_refusal searches
    the WHOLE reason string, so quoting the old reachability wording inside
    an annotation would keep matching it. The original wording is preserved
    verbatim in the row's Notes — this writes it there if it is not already
    — so nothing is lost by replacing the column.
    """
    done = 0
    for spec in RESTATE:
        ws = wb[spec["sheet"]]
        head = _head(ws)
        why = ("RejectionReason" if "RejectionReason" in head
               else "RejectReason")
        target = _key(spec["url"])
        hit = False
        for r in range(2, ws.max_row + 1):
            row = _rowdict(ws, head, r)
            if _key(row.get("URL")) != target:
                continue
            hit = True
            old = str(row.get(why) or "").strip()
            notes = str(row.get("Notes") or "").strip()
            keep = ""
            if old and old.lower() not in ("nan", "none") and old not in notes:
                keep = f" [original {why} verbatim: \"{old}\"]"
            _set(ws, head, r, why, spec["reason"])
            _apply_notes(ws, head, r, row, spec["note"] + keep, None)
            done += 1
            print(f"  {spec['sheet']}:{r}: verdict restated as content — "
                  f"{spec['url'][-46:]}")
        if not hit:
            raise SystemExit(
                f"{spec['sheet']}: no row at {spec['url']} — re-read the "
                f"sheet before re-running")
    return done


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")

    wb = openpyxl.load_workbook(XLSX)
    print("A. Recalls translations")
    recalls_fixed = _fix_sheet(wb, "Recalls", TRANSLATIONS)
    print("A. Weekly_Review mirror (same specs; not every row is mirrored)")
    mirror_fixed = _fix_sheet(wb, "Weekly_Review", TRANSLATIONS,
                              required=False)
    print("B. Pending field fixes")
    pending_fixed = _fix_sheet(wb, "Pending", PENDING_FIXES)
    print("C. Pending -> Rejected")
    archived = _archive_pending(wb)
    print("D. Archive verdicts superseded")
    superseded = _supersede_archive(wb)
    print("E. Archive verdict restated")
    restated = _restate_archive(wb)
    wb.save(XLSX)
    wb.close()

    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"RECALLS_FIELDS_FIXED={recalls_fixed}")
    print(f"WEEKLY_REVIEW_FIELDS_FIXED={mirror_fixed}")
    print(f"PENDING_FIELDS_FIXED={pending_fixed}")
    print(f"PENDING_ARCHIVED={archived}")
    print(f"ARCHIVE_VERDICTS_SUPERSEDED={superseded}")
    print(f"ARCHIVE_VERDICTS_RESTATED={restated}")
    print("ROWS_QUEUED=0")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
