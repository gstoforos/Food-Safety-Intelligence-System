# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-07.

Base: main at 947c883c ("Analytical schema sweep 2026-11-07").

Every value set below was read this morning on the regulator's own page, or
is a faithful translation of text already on the row it is written to. No
row leaves Recalls, so the commit message carries no `remove-rows`.

EIGHT TESTS WERE RED ON MAIN AT THE BASE SHA. Every one is a data defect
and every one is fixed here; three of them share a single row.

    test_alert_vocab::test_every_row_matches_at_least_one_pathogen_term     A
    test_curator_scope_2026_09_09::…_no_published_row_is_refused…           A
    test_a_recall_is_not_both_published_and_rejected[Weekly_Rejected]       A + D
    test_reason_is_not_wholly_foreign[Recalls]                              B
    test_language_policy::test_every_description_field_is_english           B
    test_gap_finder_rejection_stamps::…_only_dateless_rows_remain_…         E
    test_non_food_appliance_is_never_published[Pending]                     F
    test_non_food_appliance_is_never_published[Weekly_Review]               F


A. A FABRICATED PATHOGEN ON A PUBLISHED TIER-1 ROW
==================================================
Recalls carried, PUBLISHED, promoted at 03:45 UTC today by
pipeline.promote_gate_passing with the report line "passes every
publish-gate rule":

    Date      2026-11-06
    Source    BVL (DE)
    Company   Norderstedter Fleisch- und Wurstwaren Peter Faden GmbH & Co KG
    Product   Rinder-Peperonisalami, Rindfleischsalami, Rindfleischsalami
              rein Rind, Pizza Rindfleischsalami
    Pathogen  Bacillus
    Reason    Bakterienfund
    Tier      1
    URL       lebensmittelwarnung.de/.../261106_05_SH_Rindfleischsalami-
              produkte_Presse.pdf?__blob=publicationFile&v=2

"Bacillus" is a bare genus with no species. It is on no page: the gap
finder's own candidate record (docs/data/gap_finder_de/
pending_candidates.jsonl) shows where it came from — the Google News
headline "Fleisch- und Wurstwaren Peter Faden: Rindfleischsalamiprodukte
wegen Bakterienfund zurückgerufen - tagesschau.de". "Bakterienfund" is
German for "bacteria found". The extractor turned it into a genus name.

READ THIS MORNING on the regulator's own notice page,
https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/Meldungen/
2026/11_November/261106_05_SH_Rindfleischsalamiprodukte/
261106_05_SH_Rindfleischsalamiprodukte.html — which is the HTML notice
beside the PDF the row cites:

    heading            "Diverse Rindfleischsalamiprodukte"
    Grund der Meldung  "Krankheitserreger"
    Weitere Infos      "… in einer Einzeluntersuchung Shigatoxinbildende
                        Escherichia coli (STEC) nachgewiesen wurden."
                        The recall is described as precautionary.
    Produktbezeichnung 14 entries: Rinder-Peperonisalami halal 1000 g;
                        Rindfleischsalami halal Kaliber 60 in 1000 g,
                        500 g and 2x500 g; the same in Kaliber 80;
                        Rindfleischsalami rein Rind Kaliber 80 1000 g and
                        500 g; Rindfleischsalami rein Rind halal
                        Kaliber 80 Vakuum; Rindfleischsalami Kappen halal;
                        Rindfleischsalami-Scheiben, halal; Pizza
                        Rindfleischsalami halal Kaliber 60 1000 g and
                        500 g
    Hersteller         Norderstedter Fleisch- und Wurstwaren Peter Faden
                        GmbH & Co KG, Kösliner Weg 14, 22850 Norderstedt
    Betroffene Länder  15 of the 16 federal states (Saarland is not listed)
    Chargen            40 lot codes with dates from 23.10.2026 to
                        22.12.2026
    Erstveröffentlichung 06.11.2026, created by Schleswig-Holstein

The firm's own press release, which the Germany gap finder had ALREADY
FETCHED into docs/data/gap_finder_de/verified.jsonl, and the dpa copy in
the focus.de record beside it, say the same thing: "Shigatoxin-bildende
Escherichia coli (STEC)".

So the value was invented, and it was also the only hazard among 2306
published rows that no subscriber alert could reach — a Tier-1 STEC recall
across fifteen German federal states that nobody who subscribed to
anything would have been told about. Pathogen becomes the register's own
canonical spelling, which 120 published rows already carry:

    Shiga toxin-producing E. coli (STEC)

The gate half of the fix is pipeline/_publish_gate.
_unreachable_hazard_blockers (rule 10c), with
tests/test_a_published_hazard_is_reachable_by_an_alert.py.

Product and Reason are translated in the same edit, under B's rules.


B. TRANSLATION
==============
Five published rows carried French or German text this morning. Three
carried a live "[needs cleanup: …]" stamp; TWO DID NOT, and both of those
were found by reading every Product published in the last 26 h by hand:

  * fiche 24027, "saucisson à l'ail" — four French words and a French
    preposition, and the detector saw nothing. It looks for non-English
    WORDS and needs two hits; "saucisson" and "ail" are short and
    "à l'ail" is punctuation to it. Fourth consecutive morning on which
    the hand sweep found a Product the stamp did not mark (23552
    "Unbranded", 23983 a barcode, 23996 a brand, now this).
  * the BVL salami row of section A, whose Product and Reason are German.

Translated from the row's own text, cross-read against the rappel.conso
listing page https://rappel.conso.gouv.fr/categorie/1 opened this morning,
which prints each fiche's Dénomination, Marque, Motif and Risques:

  24039  Dénomination "Pâté de Sardinelle PINTON en 180 g"; Marque
         "PINTON"; Motif "Gonflement de certaines boites lié à un problème
         au sertissage. Présence de flore anaérobie élevée."; Risques
         begins "Clostridium botulinum (agent responsable du botulisme),
         Clostridium perfringens"; publiée le 06/11/2026. "Sardinelle" is
         the fish genus Sardinella and is given its English name; the
         French dish word "pâté" is kept, as the register already does.
         Pathogen is left as published — Clostridium botulinum is the
         first risk the fiche names.

  24027  Dénomination "Saucisson à l'ail"; Marque "LOUSSOUARN"; Motif
         "Présence de Listeria Monocytogenes"; Risques "Listeria
         monocytogenes (agent responsable de la listériose)"; publiée le
         06/11/2026. "Saucisson" is a dry-cured sausage and has no single
         English word, so the French is kept in brackets. Reason was
         already English.

  24032  Dénomination "Mélange de graines pour salade 100g"; Marque
         "ALNATURA"; Motif "Présence de Salmonelle détectée dans des
         pignons de pin."; Risques "Salmonella spp (agent responsable de
         la salmonellose)"; publiée le 06/11/2026. The Reason as stored
         was half-translated already ("Presence of Salmonella détectée
         dans des pignons de pin"), which is how it escaped the
         wholly-foreign detector while still failing the language policy.

  24033  Dénomination "HARICOTS VERTS EXTRA-FINS BIO"; Marque "CARREFOUR
         BIO"; Motif "Suspicion présence morceau de verre"; Risques
         "Inertes (verre, métal, plastique, papier, textile…)"; publiée le
         06/11/2026. The jar and the 660 g are on the row's own Product
         text; nothing else is added.

No lot, GTIN, weight or date is added that the row or the listing does not
give, and every original is kept in Notes.


C. FOUR STALE "needs cleanup" STAMPS
====================================
Rows 24019, 24005, 24015 and 24011 were translated by yesterday's pass and
their Notes still carry the stamp reviewer 3 wrote before it. A stamp that
survives its own repair sends the next pass back to a row that is already
right, and makes the morning count of outstanding translations wrong. The
stamps are dropped; the rows are not otherwise touched.


D. TWO ARCHIVE ROWS CONTRADICTING THE REGISTER
==============================================
Both share a URL with a PUBLISHED recall and say nothing about which copy
wins. Archives are append-only, so neither is deleted; each gets the
verdict its own evidence supports.

  1. The BVL salami row of section A, refused by the confirm agent with

         "Confirmer: row was at pending (not reviewed by reviewer 2) and
          page does not mention any of ['norderstedter', 'fleisch',
          'wurstwaren', 'peter', 'faden', 'rinder'] — the cited URL
          describes something other than this row"

     The cited URL is a PDF. pipeline/_provenance.fetch_text strips HTML
     tags out of the response body and calls anything over 300 characters
     readable; a PDF has no tags to strip, so requests decoded its bytes
     into a few thousand characters of binary noise, which cleared the
     floor as status "ok". No anchor can occur in noise. The confirmer
     reported a verdict about a document it had never read a word of —
     the js_shell and bot_wall confusion that module already guards
     against for HTML, one content type later. MEASURED: 31 rows carry
     that refusal and 6 cite a PDF (BVL twice, Hong Kong CFS three times,
     MHLW (JP) once — three regulators that publish recalls as PDF). Fixed
     in pipeline/_provenance with
     tests/test_an_unread_document_is_not_a_mismatch.py. The archive row
     is marked SUPERSEDED.

  2. AESAN (ES) alert 2026_72, archive row dated 2026-11-03 with
     Company "Noticias" and a Product holding the text of an AESAN news
     item about a Listeria conference. Its URL is
     https://www.aesan.gob.es/en/alertas/2026_72 and the PUBLISHED row's
     is https://aesan.gob.es/alertas/2026_72 — the same alert, the English
     locale path, which _normalize_url_for_dedup collapses to one key. The
     refusal itself was right (the extractor had scraped the agency's news
     listing, not the alert) but the recall is real and published, from
     2026-10-22, cooked Chilean mussels, Listeria monocytogenes. Marked a
     DUPLICATE of the published copy.


E. A GATE REFUSAL THAT LANDED FOUR BLANK COLUMNS
================================================
Weekly_Rejected carries, dated 2026-10-24:

    NAFDAC (NG) | Chigodson International Company Limited | Japata |
    Alcoholic Bitters 100 ml
    Week_Added, RejectedBy, RejectionReason, Reviewed — all empty

It was written by refuse_stale_alert_numbers, added yesterday
(2026-11-06), which archives through write_rejected_rows. That function
appends the row dict as-is, and the dict is a PENDING row: it has none of
those four keys, and _append_rows writes a blank for every header it
cannot find a key for. A blank Week_Added matches no review window, so the
refusal appears in no Sunday email; a blank RejectionReason tells an
auditor nothing about why this row went.

The four columns are filled here with the values the rule itself computed,
and the writer is fixed in pipeline/gap_finder/main._stamp_refusal so the
next refusal lands complete. tests/test_gap_finder_rejection_stamps.py
holds the shape for both refusal rules.


F. THE NON-FOOD APPLIANCE IS BACK
=================================
Yesterday's pass removed the NKFH cordless electric shaver from Recalls
and Weekly_Review, archived it to Rejected with a terminal verdict, and
added the publish-gate rule that refuses the shape. Recalls is clean this
morning. Pending is not: the row is there again, Status 'pending',
ScrapedAt 2026-11-07, with no re-promotion stamp — written overnight by
the Hungary gap finder, which reaches Pending through
gap_finder.main.write_pending_rows and consults no gate beyond "is this
URL already in Pending or Recalls". Weekly_Review mirrored it from there,
so the slice the operator's Sunday review email reads carried it for a
second day.

The two live copies are removed. The row is NOT archived again: Rejected
already holds it under "operator review 2026-11-06" with the full
verdict, and a second identical archive row each morning is noise, not an
audit trail. The durable half is two intake rules —
merge_master.validate_pending_row for every scraper and manual inject, and
gap_finder.main.refuse_non_food_appliances for every gap finder — with
tests/test_a_refused_shape_cannot_be_requeued.py.


G. A PENDING ROW BLOCKED ON A FIELD THE FICHE NAMES
===================================================
fiche 24040, French eggs, Salmonella Enteritidis, published 06/11/2026,
sat in Pending blocked on "Company is empty". READ THIS MORNING on
https://rappel.conso.gouv.fr/fiche-rappel/24040/Interne:

    Dénomination        "OEUFS LABEL ROUGE"
    Marque              "SANS MARQUE"
    Modèles/références  "6 OEUFS LABEL ROUGE 12 OEUFS LABEL ROUGE OEUFS
                         PLEIN AIR VENDUS EN VRAC"
    GTIN                3700133918889, 3700133918872, 3273620823268
    Conditionnements    boîte de 12, boîte de 6, 6 alvéoles de 30 oeufs
    Lot                 "06" on all three
    DDM                 "entre le 16/10/2026 et le 16/11/2026"
    Motif               "Présence Salmonelle Enteritidis"
    Risques             "Salmonella spp (agent responsable de la
                         salmonellose)"
    Origine de la fiche "COOPERL ARC ATLANTIQUE"
    Distributeurs       ten named, including CALIDEL, CASINO, VIVAL, SPAR
    Zone                "France entière"
    Commercialisation   "Du 30/09/2026 au 06/11/2026"
    Fiche réf.          2026-11-0053, version 1

Company is the fiche's own originator, because the product has no brand —
this register's convention for a RappelConso "sans marque" row. Product is
translated under B's rules.


H. THREE MISSED RECALLS ADDED
=============================
Swept the last 24 h against the rappel.conso listing page and found three
fiches in no sheet of the register. All three are the same firm's
expanding patulin recall, and all three were READ ON THEIR OWN FICHE this
morning. They are queued through tools/add_manual_row.py, never written
straight to Recalls.

    24024  Jus de pomme framboise 1L, La Cabane à jus, ref 2026-11-0041
    24029  Jus de pomme poire 1L,     La Cabane à jus, ref 2026-11-0042
    24030  Jus de pomme CASSIS 1L,    La Cabane à jus, ref 2026-11-0040

Already published from the same firm: fiche 23980, "Apple juice, 1 L
bottle", 2026-11-02, Pathogen "Mycotoxin", Reason "Presence of patulin".
These three are different products with different GTINs, so they are not
that row — but they ARE the same source, firm and hazard, which is what
the near-duplicate rule keys on. If it refuses one or more of them, that
refusal is correct and this pass accepts it; the verdict lines from
add_manual_row say which and why.

NOT ADDED, deliberately:

  * fiche 24031, MOULES DE BOUCHOT, MONBRUN, "PRESENCE E-COLI", published
    06/11/2026 — this is Fiche réf. 2026-11-0038 VERSION 2, and version 1
    of the same fiche reference is already published from fiche 24020
    (2026-11-05, same GTIN 3370410150013, same lot 018, same DCR
    31/10/2026). The same recall, re-issued. Not a missed recall.
  * fiche 24028, BABYCOOK EXPRESS, BEABA — category "Bébés-Enfants (hors
    alimentaire)", a baby-food blender whose blades may shed plastic into
    prepared food. An appliance, not a food: scope rule R1, and the same
    shape section F exists for.
  * fiches 24003 and 24016, dried morel mushrooms (CHAMPIBON via B&M
    France, and via Promocash), both held on an empty Pathogen. Their
    Motif names "une toxine" — a toxin, with no organism, class or
    compound. Filling Pathogen would mean composing a hazard label the
    fiche does not give, and that is the one thing this pass will not do.
    Left in Pending, and named in the morning report as George's call.


I. ONE SPELLING PER FIRM
========================
Queuing the three fiches of section H put "La Cabane à jus" — the Marque
all three fiches print — beside the already-published fiche 23980, whose
Company and Brand read "La Cabane à Jus". One name, two spellings, which
is what tests/test_firm_names_are_uniform refuses.
pipeline._firm_names.canonical_map picked the lower-case form itself, so
the published row is recased to it. Nothing else on that row changes.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
ROWS_JSON = Path(__file__).resolve().parent / "manual_rows_2026_11_07.json"
TODAY = "2026-11-07"

STAMP_PRODUCT = "[needs cleanup: Product not in English]"
STAMP_REASON = "[needs cleanup: Reason not in English]"
STAMP_COMPANY = "[needs cleanup: Company/Brand not in English]"

BVL_URL = ("https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/"
           "Meldungen/2026/11_November/261106_05_SH_Rindfleischsalamiprodukte/"
           "261106_05_SH_Rindfleischsalamiprodukte_Presse.pdf"
           "?__blob=publicationFile&v=2")
BVL_HTML = ("https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/"
            "Meldungen/2026/11_November/261106_05_SH_"
            "Rindfleischsalamiprodukte/261106_05_SH_"
            "Rindfleischsalamiprodukte.html")
AESAN_URL = "https://www.aesan.gob.es/en/alertas/2026_72"
NAFDAC_URL = ("https://nafdac.gov.ng/public-alert-no-027-2022-adulterated-"
              "lots-of-japata-alcoholic-bitters-100-ml-all-variants-"
              "produced-by-chigodson-international-company-limited-lagos/")
SHAVER = ("https://nkfh.gov.hu/hirek/termekvisszahivas-vezetek-nelkueli-"
          "elektromos-borotva-visszahivasa-sulyos-koernyezetvedelmi-"
          "kockazat-miatt-1")

CANONICAL_STEC = "Shiga toxin-producing E. coli (STEC)"


# ---------------------------------------------------------------------------
# A + B. Recalls (and the Weekly_Review mirror, where the row is mirrored)
# ---------------------------------------------------------------------------
RECALLS_FIXES = [
    {
        # A + B
        "url": BVL_URL,
        "set": {
            "Pathogen": CANONICAL_STEC,
            "Product": (
                "Beef salami products, halal: beef pepperoni salami; beef "
                "salami calibre 60 and calibre 80; pure-beef salami "
                "calibre 80, including a vacuum pack; salami end-pieces; "
                "sliced beef salami; pizza beef salami calibre 60 — "
                "500 g, 2 x 500 g and 1000 g (14 products)"),
            "Reason": (
                "Shiga toxin-producing Escherichia coli (STEC) detected in "
                "a single sample; precautionary recall"),
        },
        "note": (
            "[morning fix pass 2026-11-07: the published Pathogen was "
            "\"Bacillus\" — a bare genus, on no page. The Germany gap "
            "finder's own candidate record shows where it came from: the "
            "Google News headline \"Rindfleischsalamiprodukte wegen "
            "Bakterienfund zurueckgerufen - tagesschau.de\", and "
            "\"Bakterienfund\" is German for \"bacteria found\". Read today "
            "on the regulator's own HTML notice beside the PDF this row "
            "cites, " + BVL_HTML + " — heading \"Diverse "
            "Rindfleischsalamiprodukte\", \"Grund der Meldung: "
            "Krankheitserreger\", and \"… in einer Einzeluntersuchung "
            "Shigatoxinbildende Escherichia coli (STEC) nachgewiesen "
            "wurden\", the recall described as precautionary; Hersteller "
            "\"Norderstedter Fleisch- und Wurstwaren Peter Faden GmbH & Co "
            "KG, Koesliner Weg 14, 22850 Norderstedt\"; 14 "
            "Produktbezeichnungen (Rinder-Peperonisalami halal 1000 g; "
            "Rindfleischsalami halal Kaliber 60 and 80 in 1000 g, 500 g and "
            "2x500 g; Rindfleischsalami rein Rind Kaliber 80 1000 g and "
            "500 g; rein Rind halal Kaliber 80 Vakuum; Rindfleischsalami "
            "Kappen halal; Rindfleischsalami-Scheiben halal; Pizza "
            "Rindfleischsalami halal Kaliber 60 1000 g and 500 g); 15 of "
            "the 16 Bundeslaender (not Saarland); 40 Chargennummern with "
            "dates from 23.10.2026 to 22.12.2026; Erstveroeffentlichung "
            "06.11.2026, Meldung created by Schleswig-Holstein. The firm's "
            "own press release, which the same gap finder had already "
            "fetched into docs/data/gap_finder_de/verified.jsonl, and the "
            "dpa copy beside it, say STEC too. Pathogen is written in the "
            "register's own canonical spelling, which 120 published rows "
            "carry. It was also the ONLY hazard among 2306 published rows "
            "that no subscriber alert could reach; the gate half of that "
            "fix is pipeline/_publish_gate._unreachable_hazard_blockers. "
            "No lot, weight or state is added that the notice does not "
            "give.] "
            "[original product: Rinder-Peperonisalami, Rindfleischsalami, "
            "Rindfleischsalami rein Rind, Pizza Rindfleischsalami] "
            "[original Reason (de): \"Bakterienfund\"]"),
    },
    {
        # I — one spelling per firm (see the note at the foot of H)
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/23980/interne",
        "set": {
            "Company": "La Cabane à jus",
            "Brand": "La Cabane à jus",
        },
        "note": (
            "[morning-fix 2026-11-07: Company and Brand recased from \"La "
            "Cabane à Jus\" to \"La Cabane à jus\". The three sibling "
            "fiches of this firm's patulin recall added today (24024, "
            "24029, 24030) all print the Marque as \"La Cabane à jus\", so "
            "the register held the one name two ways and "
            "tests/test_firm_names_are_uniform went red. "
            "pipeline._firm_names.canonical_map chose this spelling "
            "itself — it is the fiche's own and now the majority. The "
            "recall is not otherwise touched.]"),
    },
    {
        # B — fiche 24039
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24039/interne",
        "set": {
            "Product": "Sardinella pâté, Pinton, 180 g tin",
            "Reason": (
                "Swelling of some tins caused by a seaming fault; high "
                "anaerobic flora found"),
        },
        "drop_stamps": (STAMP_PRODUCT, STAMP_REASON),
        "note": (
            "[morning fix pass 2026-11-07: Product and Reason translated "
            "from this row's own French, cross-read on the rappel.conso "
            "listing https://rappel.conso.gouv.fr/categorie/1 opened today "
            "— Dénomination \"Pâté de Sardinelle PINTON en 180 g\", Marque "
            "\"PINTON\", Motif \"Gonflement de certaines boites lié à un "
            "problème au sertissage. Présence de flore anaérobie élevée.\", "
            "Risques beginning \"Clostridium botulinum (agent responsable "
            "du botulisme), Clostridium perfringens\", publiée le "
            "06/11/2026. \"Sardinelle\" is the fish genus Sardinella and is "
            "given its English name; \"pâté\" is kept, as this register "
            "already does. \"Boites\" are tins, which the Motif itself "
            "names. Pathogen is left as published: Clostridium botulinum is "
            "the first risk the fiche names.] "
            "[original product: pâté de sardinelle pinton en 180 g] "
            "[original Reason (fr): \"Gonflement de certaines boites lié à "
            "un problème au sertissage.; présence de flore anaérobie "
            "élevée.\"]"),
    },
    {
        # B — fiche 24027, the row the detector did not mark
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24027/interne",
        "set": {
            "Product": "Garlic dry sausage (saucisson à l'ail)",
        },
        "note": (
            "[morning fix pass 2026-11-07: the published Product was the "
            "French \"saucisson à l'ail\". THIS ROW CARRIED NO \"needs "
            "cleanup\" STAMP: the language detector needs two non-English "
            "word hits and \"saucisson\" and \"ail\" are short tokens with "
            "\"à l'\" between them, which it reads as punctuation. Fourth "
            "consecutive morning on which the by-hand sweep of every "
            "Product published in the last 26 h found one the stamp missed "
            "— fiche 23552 published \"Unbranded\" from 18 September to 30 "
            "October, 23983 a barcode, 23996 a brand. Read on the "
            "rappel.conso listing today: Dénomination \"Saucisson à "
            "l'ail\", Marque \"LOUSSOUARN\", Motif \"Présence de Listeria "
            "Monocytogenes\", Risques \"Listeria monocytogenes (agent "
            "responsable de la listériose)\", publiée le 06/11/2026. "
            "\"Saucisson\" is a dry-cured sausage and has no single English "
            "word, so the French is kept in brackets. Reason was already "
            "English and is untouched; no weight or lot is added, because "
            "the row and the listing give none.] "
            "[original product: saucisson à l'ail]"),
    },
    {
        # B — fiche 24032
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24032/interne",
        "set": {
            "Product": "Mixed seeds for salad, 100 g",
            "Reason": "Salmonella detected in pine nuts",
        },
        "drop_stamps": (STAMP_PRODUCT, STAMP_REASON),
        "note": (
            "[morning fix pass 2026-11-07: Product and Reason translated "
            "from this row's own French, cross-read on the rappel.conso "
            "listing today — Dénomination \"Mélange de graines pour salade "
            "100g\", Marque \"ALNATURA\", Motif \"Présence de Salmonelle "
            "détectée dans des pignons de pin.\", Risques \"Salmonella spp "
            "(agent responsable de la salmonellose)\", publiée le "
            "06/11/2026. The stored Reason was half-translated already "
            "(\"Presence of Salmonella détectée dans des pignons de pin\"), "
            "which is how it passed the wholly-foreign detector while still "
            "failing the language policy.] "
            "[original product: mélange de graines pour salade 100g] "
            "[original Reason (fr): \"Presence of Salmonella détectée dans "
            "des pignons de pin\"]"),
    },
    {
        # B — fiche 24033
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24033/interne",
        "set": {
            "Product": "Organic extra-fine green beans, 660 g jar",
            "Reason": "Suspected presence of a piece of glass",
        },
        "drop_stamps": (STAMP_PRODUCT, STAMP_REASON),
        "note": (
            "[morning fix pass 2026-11-07: Product and Reason translated "
            "from this row's own French, cross-read on the rappel.conso "
            "listing today — Dénomination \"HARICOTS VERTS EXTRA-FINS "
            "BIO\", Marque \"CARREFOUR BIO\", Motif \"Suspicion présence "
            "morceau de verre\", Risques \"Inertes (verre, métal, "
            "plastique, papier, textile…)\", publiée le 06/11/2026. The jar "
            "and the 660 g are on this row's own Product text. The stamp "
            "named only the Reason; the Product was French too, which the "
            "detector missed — \"bocal\" and \"haricots\" are one hit short "
            "of its threshold once \"bio\" and the numerals are discounted.] "
            "[original product: bocal haricots verts – extra-fins bio 660g] "
            "[original Reason (fr): \"Suspicion présence morceau de "
            "verre\"]"),
    },
]

# ---------------------------------------------------------------------------
# C. Stale stamps only — no field changes
# ---------------------------------------------------------------------------
STALE_STAMPS = [
    {
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24019/interne",
        "set": {},
        "drop_stamps": (STAMP_PRODUCT,),
        "note": ("[morning-fix 2026-11-07: dropped a stale \"needs cleanup: "
                 "Product\" stamp — this Product was translated by "
                 "yesterday's pass and the original is already quoted in "
                 "these Notes]"),
    },
    {
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24005/interne",
        "set": {},
        "drop_stamps": (STAMP_PRODUCT,),
        "note": ("[morning-fix 2026-11-07: dropped a stale \"needs cleanup: "
                 "Product\" stamp — translated by yesterday's pass]"),
    },
    {
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24015/interne",
        "set": {},
        "drop_stamps": (STAMP_PRODUCT, STAMP_REASON),
        "note": ("[morning-fix 2026-11-07: dropped two stale \"needs "
                 "cleanup\" stamps — Product and Reason were both "
                 "translated by yesterday's pass]"),
    },
    {
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24011/interne",
        "set": {},
        "drop_stamps": (STAMP_PRODUCT, STAMP_REASON),
        "note": ("[morning-fix 2026-11-07: dropped two stale \"needs "
                 "cleanup\" stamps — Product and Reason were both "
                 "translated by yesterday's pass]"),
    },
]

# ---------------------------------------------------------------------------
# G. Pending field fixes
# ---------------------------------------------------------------------------
PENDING_FIXES = [
    {
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/24040/interne",
        "set": {
            "Company": "Cooperl Arc Atlantique",
            "Product": ("Label Rouge eggs in boxes of 6 and 12, and "
                        "free-range eggs sold loose in trays of 30"),
        },
        "drop_stamps": (STAMP_PRODUCT, STAMP_COMPANY),
        "note": (
            "[morning fix pass 2026-11-07: Company was empty and the "
            "publish gate held the row on it. Read today on "
            "https://rappel.conso.gouv.fr/fiche-rappel/24040/Interne — "
            "Dénomination \"OEUFS LABEL ROUGE\", Marque \"SANS MARQUE\", "
            "Modèles ou références \"6 OEUFS LABEL ROUGE 12 OEUFS LABEL "
            "ROUGE OEUFS PLEIN AIR VENDUS EN VRAC\", GTIN 3700133918889 / "
            "3700133918872 / 3273620823268, Conditionnements \"boîte de "
            "12\", \"boîte de 6\" and \"6 alvéoles de 30 oeufs\", lot "
            "\"06\" on all three, \"Date de durabilité minimale entre le "
            "16/10/2026 et le 16/11/2026\", Motif \"Présence Salmonelle "
            "Enteritidis\", Risques \"Salmonella spp (agent responsable de "
            "la salmonellose)\", Origine de la fiche \"COOPERL ARC "
            "ATLANTIQUE\", ten distributeurs including CALIDEL, CASINO, "
            "VIVAL and SPAR, France entière, commercialisée du 30/09/2026 "
            "au 06/11/2026, Fiche réf. 2026-11-0053 version 1. Company is "
            "the fiche's own originator because the product has no brand, "
            "which is this register's convention for a RappelConso \"sans "
            "marque\" row. Product is translated from the références field "
            "and the Conditionnements; no GTIN or date is written into it.] "
            "[original product: 6 oeufs label rouge; 12 oeufs label rouge; "
            "oeufs plein air vendus en vrac]"),
    },
]

# ---------------------------------------------------------------------------
# D. Archive annotations — append-only, nothing deleted
# ---------------------------------------------------------------------------
BVL_SUPERSEDED = (
    "[SUPERSEDED 2026-11-07 — the defect named below is a defect of the "
    "CHECK, not of the recall, and the recall is PUBLISHED in Recalls. The "
    "cited URL is a PDF: pipeline/_provenance.fetch_text strips HTML tags "
    "out of the response body and calls anything over 300 characters "
    "readable, and a PDF has no tags to strip, so its bytes decoded into a "
    "few thousand characters of binary noise that cleared the floor as "
    "status \"ok\". No anchor token can occur in noise, so the confirmer "
    "reported a verdict about a document it had never read a word of. "
    "MEASURED today: 31 rows carry this refusal and 6 of them cite a PDF — "
    "BVL (DE) twice, Hong Kong CFS three times, MHLW (JP) once. Fixed in "
    "pipeline/_provenance; see "
    "tests/test_an_unread_document_is_not_a_mismatch.py. This row is kept "
    "only as the audit trail of the original refusal; the Recalls copy is "
    "what the register says.] ")
AESAN_DUPLICATE = (
    "[DUPLICATE 2026-11-07 — the same AESAN alert as the row kept in "
    "Recalls. This row's URL is the English-locale path "
    "aesan.gob.es/en/alertas/2026_72 and the published row's is "
    "aesan.gob.es/alertas/2026_72; merge_master._normalize_url_for_dedup "
    "collapses the two to one key, which is why this contradiction was "
    "invisible to an exact-string comparison. The refusal below was "
    "correct — the extractor had scraped the agency's news listing, so "
    "Company read \"Noticias\" and Product held the text of an item about "
    "a Listeria conference — but the alert itself is real and is published "
    "from 2026-10-22 (cooked Chilean mussels, vacuum tray 200 g, Listeria "
    "monocytogenes). The Recalls copy is what the register says.] ")

ARCHIVE_NOTES = [
    {"sheet": "Weekly_Rejected", "url": BVL_URL,
     "column": "RejectionReason", "prefix": BVL_SUPERSEDED,
     "reviewed": "SUPERSEDED"},
    {"sheet": "Weekly_Rejected", "url": AESAN_URL,
     "column": "RejectionReason", "prefix": AESAN_DUPLICATE,
     "reviewed": "DUPLICATE"},
]

# ---------------------------------------------------------------------------
# E. The four blank columns on the NAFDAC refusal
# ---------------------------------------------------------------------------
NAFDAC_STAMPS = {
    "Week_Added": "2026-11-08",
    "RejectedBy": "gap_finder/alert_number_year_mismatch",
    "RejectionReason": (
        "alert_number_year_mismatch: the URL's own alert number "
        "('public-alert-no-027-2022') is from 2022 and the row's Date is "
        "from 2026 — the Date is the discovery day, or the row "
        "re-publishes an old alert"),
    "Reviewed": "N",
}


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
    """For an archive row whose twin is published at the SAME key.

    Section D.2 exists because _normalize_url_for_dedup collapses two
    different URLs to one key. Annotating by key there would also hit the
    published row, so the archive sheets are matched on the stored string.
    """
    head = _head(ws)
    col = head.index("URL") + 1
    target = str(url or "").strip()
    return [r for r in range(2, ws.max_row + 1)
            if str(ws.cell(r, col).value or "").strip() == target]


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


def annotate_archives(wb) -> int:
    """D. Each contradicting archive row gets the verdict its evidence says."""
    done = 0
    for spec in ARCHIVE_NOTES:
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
        for r in rows:
            row = _rowdict(ws, head, r)
            col = spec["column"]
            if col not in head:                         # pragma: no cover
                continue
            cur = str(row.get(col) or "").strip()
            if cur.lower() in ("none", "nan"):
                cur = ""
            if spec["prefix"].split("—")[0].strip() in cur:
                print(f"  {sheet}:{r}: already annotated, left alone")
                continue
            _set(ws, head, r, col, (spec["prefix"] + cur).strip())
            if "Reviewed" in head:
                _set(ws, head, r, "Reviewed", spec["reviewed"])
            done += 1
            print(f"  {sheet}:{r}: {spec['reviewed']} "
                  f"{str(row.get('Date'))[:10]} {str(row.get('Source'))[:16]}")
    return done


def stamp_nafdac_refusal(wb) -> int:
    """E. Fill the four columns the refusal path left blank."""
    ws = wb["Weekly_Rejected"]
    head = _head(ws)
    rows = _find_rows(ws, NAFDAC_URL)
    if not rows:
        raise SystemExit(
            "Weekly_Rejected: no NAFDAC public-alert-027-2022 row — main "
            "has moved under this repair script")
    done = 0
    for r in rows:
        row = _rowdict(ws, head, r)
        if str(row.get("Week_Added") or "").strip():
            print(f"  Weekly_Rejected:{r}: already stamped, left alone")
            continue
        for field, value in NAFDAC_STAMPS.items():
            if field in head:
                _set(ws, head, r, field, value)
        _apply_notes(ws, head, r, row,
                     "[morning-fix 2026-11-07: Week_Added, RejectedBy, "
                     "RejectionReason and Reviewed were all blank. The row "
                     "was archived by gap_finder.main."
                     "refuse_stale_alert_numbers, which appends a PENDING "
                     "row dict through write_rejected_rows, and "
                     "_append_rows writes a blank for every header it finds "
                     "no key for — so the refusal appeared in no Sunday "
                     "email and told an auditor nothing. The four values "
                     "here are the ones the rule itself computed; the "
                     "writer is fixed in gap_finder.main._stamp_refusal.]",
                     None)
        done += 1
        print(f"  Weekly_Rejected:{r}: stamped "
              f"{str(row.get('Date'))[:10]} NAFDAC public-alert-027-2022")
    return done


def remove_shaver_copies(wb) -> int:
    """F. The appliance leaves the two LIVE sheets. Rejected keeps it."""
    removed = 0
    for sheet in ("Pending", "Weekly_Review"):
        if sheet not in wb.sheetnames:                  # pragma: no cover
            continue
        ws = wb[sheet]
        head = _head(ws)
        for r in reversed(_find_rows(ws, SHAVER)):
            row = _rowdict(ws, head, r)
            print(f"  {sheet} row {r}: {str(row.get('Date'))[:10]} "
                  f"{str(row.get('Product'))!r} — removed (not a food; "
                  f"Rejected already holds it under 'operator review "
                  f"2026-11-06' with the full verdict)")
            ws.delete_rows(r, 1)
            removed += 1
    if not removed:
        print("  NOTE: no live shaver copy found — if another pass already "
              "removed it, that is the expected outcome, not an error")
    # Recalls must NOT have it. If it does, the publish gate regressed.
    if "Recalls" in wb.sheetnames and _find_rows(wb["Recalls"], SHAVER):
        raise SystemExit(
            "Recalls carries the NKFH shaver again — the publish-gate rule "
            "_non_food_appliance_blockers has regressed. Stop and read it "
            "before shipping anything.")
    return removed


def queue_manual_rows() -> int:
    """H. Through the real code path, never straight into Recalls."""
    if not ROWS_JSON.exists():                          # pragma: no cover
        raise SystemExit(f"missing {ROWS_JSON}")
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
         str(ROWS_JSON)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        # add_manual_row exits non-zero when ANY requested row did not land.
        # A near-duplicate refusal is an accepted outcome for these three
        # (see section H): they are the same source, firm and hazard as the
        # already-published fiche 23980.
        print(f"  NOTE: add_manual_row exited {proc.returncode} — read the "
              f"per-row verdict lines above; a near-duplicate refusal is an "
              f"accepted outcome for this pass.")
    return 0


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")

    wb = openpyxl.load_workbook(XLSX)
    print("A+B. Recalls: the fabricated Pathogen and five translations")
    recalls_fixed = _fix_sheet(wb, "Recalls", RECALLS_FIXES)
    print("A+B. Weekly_Review mirror (same specs; not every row is mirrored)")
    mirror_fixed = _fix_sheet(wb, "Weekly_Review", RECALLS_FIXES,
                              required=False)
    print("C. Recalls: stale needs-cleanup stamps")
    stamps_dropped = _fix_sheet(wb, "Recalls", STALE_STAMPS)
    print("C. Weekly_Review mirror")
    stamps_mirror = _fix_sheet(wb, "Weekly_Review", STALE_STAMPS,
                               required=False)
    print("G. Pending: the field the fiche names")
    pending_fixed = _fix_sheet(wb, "Pending", PENDING_FIXES)
    print("D. Archive annotations")
    annotated = annotate_archives(wb)
    print("E. The NAFDAC refusal's four blank columns")
    stamped = stamp_nafdac_refusal(wb)
    print("F. The non-food appliance leaves Pending and Weekly_Review")
    removed = remove_shaver_copies(wb)
    wb.save(XLSX)
    wb.close()

    print("H. Queue the three missed RappelConso recalls")
    queue_manual_rows()

    print(f"\nrepair_2026_11_07: Recalls {recalls_fixed} fixed, "
          f"Weekly_Review {mirror_fixed} mirrored, "
          f"{stamps_dropped}+{stamps_mirror} stale stamps dropped, "
          f"Pending {pending_fixed} fixed, {annotated} archive row(s) "
          f"annotated, {stamped} refusal(s) stamped, {removed} live "
          f"appliance copy/copies removed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
