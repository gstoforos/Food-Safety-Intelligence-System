# -*- coding: utf-8 -*-
"""Morning fix 2026-10-21. ONE ROW IS REMOVED from Recalls — the commit
message carries "remove-rows".

THE SUITE IS RED ON MAIN at 0ae22b9c. Three tests, one cause:

    tests/test_language_policy.py::TestPublishedWorkbook::
        test_every_description_field_is_english
    tests/test_product_and_firm_names_are_english.py::
        test_no_published_product_reads_in_another_language
    tests/test_reason_is_not_wholly_foreign.py::
        test_no_published_reason_is_wholly_foreign[Recalls]

Four RappelConso rows published 2026-10-20 and 2026-10-21 carry French
text. This is the SECOND consecutive morning on which a RappelConso batch
went to the dashboard and to subscribers in French — yesterday's pass
translated twelve of them and shipped a _language.py lexicon fix. The
lexicon is not where today's gap is.

A. WHY THE STAMPS DID NOT REACH THE MORNING PASS.

   The morning pass translates what is STAMPED "[needs cleanup: …]". Two
   writers stamped before today:

     pipeline/recall_confirm_agent.py   Reason, Product, Company/Brand
     pipeline/promote_gate_passing.py   Product, Company/Brand — NOT Reason

   and merge_master._write_sheet, the choke point EVERY write to Recalls
   goes through, stamped nothing at all. Its English-output block has
   counted non-English Reasons since 2026-08-02 and spent the count on a
   log warning nobody reads.

   Measured on the four rows:

     fiche 23874  Reason "RESULTAT ANALYSE NON CONFORME"
                  Product "sacs de 10 kgs trad, 5 kg trad,15 kg trad"
                  promoted by promote_gate_passing     NO stamp of any kind
     fiche 23873  Reason "Contamination à l'histamine"
                  promoted by promote_gate_passing     Product stamp only
     fiche 23872  Reason "Contamination du produit à l'histamine"
                  promoted by promote_gate_passing     Product stamp only
     fiche 23876  Reason "Détection de Listeria monocytogènes suite à une
                          analyse produit"
                  Product "boudin à la viande"
                  Notes = "e.leclerc arès" and NOTHING else — it arrived
                  through the hourly Pending->Recalls merge, so neither
                  stamping writer ever saw it

   Fixed in this upload, at both places, as a STAMP and never as a block:
   merge_master._write_sheet (Reason, Product, Company/Brand) and
   promote_gate_passing (Reason, for parity with its sibling), with
   tests/test_a_foreign_reason_is_stamped_at_the_writer.py.

   WHAT IS STILL NOT CAUGHT, and it is reported rather than papered over.
   detect_language("Contamination à l'histamine") returns None: the two-hit
   threshold finds only "à", because "contamination" and "histamine" are
   spelled the same in English. The stamp therefore catches three of these
   four Reasons, not four. tests/test_reason_is_not_wholly_foreign.py says
   in its own assertion message not to loosen detect_language — the
   threshold is what protects English sentences that quote foreign names —
   so that shape stays a decision for the operator, not a lexicon edit
   made in passing. Likewise product_needs_english("sacs de 10 kgs trad,
   5 kg trad,15 kg trad") is False, and rightly: that string is not French
   prose, it is a list of pack formats, and the field holding it names no
   product at all. Yesterday's _product_is_an_identifier guard catches bare
   reference codes ('484565', 'mr10ae, mr41ck') and not a pack-size list.
   Named, not widened.

B. THE FOUR ROWS, EACH READ ON ITS OWN FICHE 2026-10-21.

   Every value below is quoted from the fiche: "Nom du produit", "Marque",
   "Origine de la fiche", "Risques encourus", "Motif du rappel", the
   lot/GTIN/DLC table and "Zone de distribution". Nothing is inferred. The
   French is kept in Notes as [original product: …] / [original company: …]
   / [original Reason (fr): "…"] so the row still matches the fiche and the
   dashboard shows it on hover.

   fiche 23874  https://rappel.conso.gouv.fr/fiche-rappel/23874/interne
     Nom du produit  "MOULES DE BOUCHOT STG mytilus edulis"
     Marque          "MOULES DE BOUCHOT STG"
     Origine         "EARL BATARD-DENOUAL EARL BATARD DENOUAL"
     Risques         "Escherichia coli"
     Motif           "RESULTAT ANALYSE NON CONFORME"
     Lot "14102026" · no GTIN on the fiche · "France entière"
     Published Company AND Brand both held "Moules de Bouchot STG" — the
     fiche's Marque written into both. The fiche names a firm under
     Origine, so Company takes it and the Marque stays in Brand, exactly
     as the 2026-10-20 pass did for fiches 23842, 23852, 23859 and 23820.
     Published Product held the pack formats, not the product.

   fiche 23873  https://rappel.conso.gouv.fr/fiche-rappel/23873/interne
     Nom du produit  "CHARCUTERIE DE MER - SAUCISSE DE THON"
     Marque          "Maison de la bottarga"
     Origine         "POISCAILLE Poiscaille"
     Risques         "Autres contaminants chimiques"
     Motif           "Contamination à l'histamine"
     Lot "250" · DLC 22/10/2026 · GTIN 2274371000008 · "France entière"

   fiche 23872  https://rappel.conso.gouv.fr/fiche-rappel/23872/interne
     Same product and brand as 23873, a SEPARATE fiche with its own lot.
     Origine         "Poiscaille"
     Risques         "Autres contaminants chimiques"; the page describes
                     "Risque d'intoxication à l'histamine"
     Motif           "contamination du produit à l'histamine"
     Lot "215" · DLC 06/10/2026 · no GTIN on the fiche · "France entière"
     These two are not duplicates of each other: two fiches, two lots.

   fiche 23876  https://rappel.conso.gouv.fr/fiche-rappel/23876/interne
     Nom du produit  "Boudin Viande"
     Marque          "Sans Marque"
     Origine         "ARES DISTRIBUTION LECLERC", distributed by
                     "E.Leclerc Arès"
     Risques         "Listeria monocytogenes (agent responsable de la
                      listériose)"
     Motif           "Détection de Listeria monocytogènes suite à une
                      analyse produit"
     Lot "627814" · GTIN 3505870000343 · "GIRONDE (33)"
     Company is left as published: "E.Leclerc Arès" is on the fiche as the
     distributor, and replacing a name the fiche prints with another name
     the fiche prints buys nothing.

C. A RECALL PUBLISHED TWICE, THE SECOND COPY POINTING AT AN XML DOCUMENT.

   Recalls carried, DateAdded 2026-10-21, Source "CFIA", Date 2026-10-14,
   Tier 1, Listeria monocytogenes:

     Company  "Health and safety"
     Brand    "Health and safety"
     Product  "and alerts"
     URL      https://recalls-rappels.canada.ca/en/feed/cfia-alerts-recalls

   The three field values are the CFIA feed's own channel title, "Health
   and safety recalls and alerts", cut at the word "recalls". The row's own
   Notes carry the feed body and name the recall it was carrying:
   "Various brands of organic cheeses recalled due to Listeria
   monocytogenes https://recalls-rappels.canada.ca/en/alert-recall/
   various-brands-organic-" — and THAT recall is already published, on its
   own per-recall URL, as Recalls row 75, Fromagerie L'Ancêtre Inc., with
   the real product list (Farm Boy Organic Swiss Cheese 190 g, Farm Boy
   Organic Medium Cheddar …). So this is one recall published twice, the
   second copy naming no firm, no product, and citing a feed.

   Provenance, from its own Notes: "[via Tavily gap-finder, deterministic
   extract] [confirm-agent 2026-10-21: pending_gap → pending; deterministic
   checks passed; reviewer 2 did NOT review this row]" — commit 65aea090,
   "Tavily gap-finder (standalone): +6 rows to Pending".

   Nothing refused it, because the feed path is not in _LANDING_PATHS, is
   not a CMS index prefix, sits on the regulator's OWN host, and is long
   and slug-shaped. The gate half is url_path_is_a_feed() in
   pipeline/_publish_gate.py, scoped to regulator hosts, with
   tests/test_a_regulators_own_feed_is_not_a_recall_notice.py.

   The row is removed from Recalls and archived to Rejected as a DUPLICATE
   naming row 75 as the copy that wins; its twin in the Sunday review slice
   (Weekly_Review) goes with it so the slice stops carrying it. It is not
   deleted — archives are append-only. Rejected already holds a row on the
   REAL notice's URL marked SUPERSEDED, so the archive's account of that
   recall stays consistent.

D. ONE MISSED RECALL ADDED.

   RappelConso fiche 23846, published 19/10/2026, in no sheet of the
   register before this pass — not by fiche number, not by GTIN. Read on
   the fiche 2026-10-21:

     Nom du produit  "LOMO ESPAGNOL 20 tranches U 100g"
     Marque          "U"
     Origine         "COOPERATIVE U ENSEIGNE U ENSEIGNE"; distributed by
                     "Magasins U". No "nom de la société" field is shown.
     Risques         "Listeria monocytogenes (agent responsable de la
                      listériose)"
     Motif           "NON CONFORMITE MICROBIOLOGIQUE"
     Lot "78103" · GTIN 3256228656957 · "France entière"

   The pipeline swept 19/10 — 22 rows in Recalls carry that publish date —
   and this fiche was not among them. Added via tools/add_manual_row.py so
   the near-duplicate rule and the publish gate both get their say; if
   either refuses it, the refusal is reported and not worked around.

E. TWO PENDING ROWS WHOSE URL CITES A DIFFERENT RECALL, ONE PROMOTER RUN
   AWAY FROM PUBLICATION.

   `python -m pipeline.promote_gate_passing` reports three Pending rows that
   pass the FULL publish gate today. Two of them must not publish:

     Pending  Source "Ministero della Salute (IT)", Date 2026-10-20, Tier 1
              Company "IN's Mercato" · Brand "Selezione Piu" (sic)
              Product "Tuna fillets" · Pathogen "Istamina exceeding limit"
              Reason  "Possible presence of istamina above legal limits"
              URL     .../avvisi_sicurezza_alimentare/RICHIAMO_PediaVit_
                      Complesso_B_Sciroppo_Lotto_26040043_1792411699.pdf

     Pending  Source "Ministero della Salute (IT)", Date 2026-10-19, Tier 1
              Company "Carrefour" · Brand "Carrefour"
              Product "Cod fish fillet"
              Pathogen/Reason "Non conformity microbiological"
              URL     .../avvisi_sicurezza_alimentare/Cartello%20di%20
                      Richiamo%20Tahina%20Arkan_1792170251.pdf

   The ministry names its recall forms after the product they recall. The
   first row's own URL is the recall form for PEDIAVIT COMPLESSO B SCIROPPO,
   lot 26040043 — a vitamin-B syrup — against a row about tuna fillets and
   histamine. The second row's own URL is the recall form for TAHINA ARKAN —
   tahini — against a row about Carrefour cod fillet. Both rows' Notes say
   "Discovered via news: ilfattoalimentare.it": the fields come from a news
   article and the ministry URL was attached separately, and they do not
   describe the same recall.

   This is established from each row's OWN URL, which is the only evidence
   used. BOTH PDFs WERE REQUESTED ON 2026-10-21 AND NEITHER COULD BE READ:
   salute.gov.it served a browser-validation shell ("This process is
   automatic. You will be redirected once the validation is complete")
   instead of the document. That block is reported, not routed around, and
   nothing from those PDFs is used here. The recalls themselves may well be
   real — what is wrong is the citation, so the archive reason says the rows
   cannot publish UNTIL the ministry's own notice for each product is
   identified, and leaves them re-promotable.

   NO GATE RULE SHIPS FOR THIS, and that is a measurement, not an omission.
   The obvious rule — the PDF filename must share a content word with the
   row's Product, Brand or Company — was written and run over the register
   before being discarded: of 15 published rows on salute.gov.it recall
   PDFs it agrees with 6, has no opinion on 3 (filenames that are pure
   boilerplate, "Cartello di richiamo _5_"), and DISAGREES with 6 that are
   correct, because the pipeline translates Product into English while the
   ministry names the file in Italian — "pesce spada trancio" against
   "swordfish slices", "calamaro pulito aperto" against "Cleaned Patagonian
   squid". A gate with a 50% false-positive rate gets switched off. The
   dosage-form route is no better: "sciroppo" cannot join
   _NON_FOOD_DOSAGE_FORMS because maple, golden and corn syrup are foods.
   What binds a news-discovered row's fields to the URL attached to it is a
   rule for the operator to choose, and it is in the verdict.

NOT CHANGED, DELIBERATELY. Recalls row 110, NFSA (EG), 2026-10-11,
Yasensvit Ltd / Eggs / Salmonella Enteritidis, whose URL is a
news.google.com/rss/articles/… redirector. Its own Notes record the
operator exception it rests on — "News-authority source: see.news (no
official NFSA per-recall portal)" — but the stored URL is a Google News
redirect, not see.news's own article and not NFSA's. Resolving the
redirector means fetching it, which this sandbox cannot do without
guessing, and nothing may be invented into a published field. Whether that
row keeps a redirector URL, gets the see.news article URL, or comes out of
Recalls is the operator's call, so it is reported in the verdict and left
exactly as it is. The feed rule in this upload is scoped to regulator hosts
so that it does not decide this by accident.
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
JSON_MIRROR = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-21"
OP = "[morning fix 2026-10-21]"
READ = "read on the fiche 2026-10-21"

CLEANUP_STAMPS = (
    "[needs cleanup: Product not in English]",
    "[needs cleanup: Company/Brand not in English]",
    "[needs cleanup: Reason not in English]",
    "[needs cleanup: Reason not in English and no verified translation "
    "on file]",
)

# ── B — the four French rows, keyed on the fiche number in each row's URL ──
FICHES = {
    "fiche-rappel/23874": {
        "Company": "EARL Batard-Denoual",
        "Brand": "Moules de Bouchot STG",
        "Product": ("Bouchot mussels, STG (moules de bouchot STG, Mytilus "
                    "edulis) — trad sacks of 5 kg, 10 kg and 15 kg; lot "
                    "14102026"),
        "Reason": ("Non-compliant analysis result. Consumer risk: "
                   "Escherichia coli."),
        "_orig": {
            "product": "sacs de 10 kgs trad, 5 kg trad,15 kg trad",
            "company": "Moules de Bouchot STG",
            "reason": "RESULTAT ANALYSE NON CONFORME",
        },
        "_note": (
            "Fiche 23874 " + READ + ": Nom du produit 'MOULES DE BOUCHOT STG "
            "mytilus edulis', Marque 'MOULES DE BOUCHOT STG', Origine de la "
            "fiche 'EARL BATARD-DENOUAL EARL BATARD DENOUAL', Risques "
            "encourus 'Escherichia coli', Motif du rappel 'RESULTAT ANALYSE "
            "NON CONFORME', lot '14102026' (no GTIN on the fiche), zone de "
            "distribution 'France entière'. Company held the fiche's Marque; "
            "the fiche names a firm under Origine, so Company takes it and "
            "the Marque stays in Brand. Published Product held the pack "
            "formats, which name no product. The 5/10/15 kg 'trad' formats "
            "are the published row's own values and are kept."),
    },
    "fiche-rappel/23873": {
        "Company": "Poiscaille",
        "Brand": "Maison de la Bottarga",
        "Product": ("Seafood charcuterie — tuna sausage (charcuterie de mer "
                    "- saucisse de thon), Maison de la Bottarga — lot 250, "
                    "use-by 22/10/2026, GTIN 2274371000008"),
        "Reason": ("Histamine contamination. Consumer risk: other chemical "
                   "contaminants."),
        "_orig": {
            "product": "charcuterie de mer - saucisse de thon",
            "company": "Maison de la Bottarga",
            "reason": "Contamination à l'histamine",
        },
        "_note": (
            "Fiche 23873 " + READ + ": Nom du produit 'CHARCUTERIE DE MER - "
            "SAUCISSE DE THON', Marque 'Maison de la bottarga', Origine de "
            "la fiche 'POISCAILLE Poiscaille', Risques encourus 'Autres "
            "contaminants chimiques', Motif du rappel 'Contamination à "
            "l'histamine', lot '250', DLC '22/10/2026', GTIN "
            "'2274371000008', zone de distribution 'France entière'. A "
            "SEPARATE fiche from 23872, which recalls the same product on "
            "lot 215 — two fiches, two lots, not a duplicate."),
    },
    "fiche-rappel/23872": {
        "Company": "Poiscaille",
        "Brand": "Maison de la Bottarga",
        "Product": ("Seafood charcuterie — tuna sausage (charcuterie de mer "
                    "- saucisse de thon), Maison de la Bottarga — lot 215, "
                    "use-by 06/10/2026"),
        "Reason": ("Histamine contamination of the product. Consumer risk: "
                   "other chemical contaminants; the notice describes a risk "
                   "of histamine poisoning."),
        "_orig": {
            "product": "charcuterie de mer - saucisse de thon",
            "company": "Maison de la Bottarga",
            "reason": "Contamination du produit à l'histamine",
        },
        "_note": (
            "Fiche 23872 " + READ + ": Nom du produit 'CHARCUTERIE DE MER - "
            "SAUCISSE DE THON', Marque 'Maison de la bottarga', Origine de "
            "la fiche 'Poiscaille', Risques encourus 'Autres contaminants "
            "chimiques' with the page describing \"Risque d'intoxication à "
            "l'histamine\", Motif du rappel 'contamination du produit à "
            "l'histamine', lot '215', DLC '06/10/2026' (no GTIN on the "
            "fiche), zone de distribution 'France entière'. A SEPARATE "
            "fiche from 23873 (lot 250)."),
    },
    "fiche-rappel/23876": {
        "Product": ("Black pudding, meat (boudin viande), unbranded — lot "
                    "627814, GTIN 3505870000343"),
        "Reason": ("Listeria monocytogenes detected in a product analysis. "
                   "Consumer risk: Listeria monocytogenes, the agent of "
                   "listeriosis."),
        "_orig": {
            "product": "boudin à la viande",
            "reason": ("Detection of Listeria monocytogènes suite à une "
                       "analyse produit"),
        },
        "_note": (
            "Fiche 23876 " + READ + ": Nom du produit 'Boudin Viande', "
            "Marque 'Sans Marque', Origine de la fiche 'ARES DISTRIBUTION "
            "LECLERC' distributed by 'E.Leclerc Arès', Risques encourus "
            "'Listeria monocytogenes (agent responsable de la listériose)', "
            "Motif du rappel 'Détection de Listeria monocytogènes suite à "
            "une analyse produit', lot '627814', GTIN '3505870000343', zone "
            "de distribution 'GIRONDE (33)'. Company left as published: "
            "'E.Leclerc Arès' is the distributor the fiche prints. Brand "
            "stays 'Unbranded' — the fiche's Marque is 'Sans Marque'."),
    },
}

# ── C — the CFIA feed row ──────────────────────────────────────────────────
FEED_URL = "recalls-rappels.canada.ca/en/feed/cfia-alerts-recalls"
FEED_DUP_REASON = (
    "duplicate + not a per-recall notice: this row's URL is CFIA's "
    "SYNDICATION FEED, https://recalls-rappels.canada.ca/en/feed/"
    "cfia-alerts-recalls, and its Company ('Health and safety'), Brand "
    "('Health and safety') and Product ('and alerts') are that feed's own "
    "channel title 'Health and safety recalls and alerts' cut at the word "
    "'recalls'. The recall the feed was carrying is named in this row's own "
    "Notes and is ALREADY PUBLISHED in Recalls on CFIA's own per-recall URL "
    "(recalls-rappels.canada.ca/en/alert-recall/various-brands-organic-"
    "cheeses-recalled-due-listeria-monocytogenes, Fromagerie L'Ancêtre "
    "Inc., Date 2026-10-14), which is the copy that wins. Queued by the "
    "Tavily gap-finder (commit 65aea090) and published by the confirm agent "
    "with 'reviewer 2 did NOT review this row'. A feed is a document "
    "listing many recalls, so a row citing it names no recall (R3). The "
    "gate half is url_path_is_a_feed() in pipeline/_publish_gate.py with "
    "tests/test_a_regulators_own_feed_is_not_a_recall_notice.py. " + OP)

# ── D — the missed recall ──────────────────────────────────────────────────
LOMO_URL = "https://rappel.conso.gouv.fr/fiche-rappel/23846/Interne"
LOMO_ROW = {
    "Date": "2026-10-19",
    "Source": "RappelConso (FR)",
    "Company": "Coopérative U Enseigne",
    "Brand": "U",
    "Product": ("Spanish lomo, 20 slices, U, 100 g (LOMO ESPAGNOL 20 "
                "tranches U 100g) — lot 78103, GTIN 3256228656957"),
    "Pathogen": "Listeria monocytogenes",
    "Reason": ("Microbiological non-compliance. Consumer risk: Listeria "
               "monocytogenes, the agent of listeriosis."),
    "Class": "Recall",
    "Country": "France",
    "Outbreak": 0,
    "URL": LOMO_URL,
    "Notes": (
        OP + " Fiche 23846 " + READ + ", published 19/10/2026 and in no "
        "sheet of the register before this pass (searched by fiche number "
        "and by GTIN across Recalls, Pending, Weekly_Review, "
        "Weekly_Rejected and Rejected). Nom du produit 'LOMO ESPAGNOL 20 "
        "tranches U 100g', Marque 'U', Risques encourus 'Listeria "
        "monocytogenes (agent responsable de la listériose)', Motif du "
        "rappel 'NON CONFORMITE MICROBIOLOGIQUE', lot '78103', GTIN "
        "'3256228656957', zone de distribution 'France entière', "
        "distributors 'Magasins U'. The fiche shows NO 'nom de la société' "
        "field; Company is the firm named under Origine de la fiche, "
        "'COOPERATIVE U ENSEIGNE U ENSEIGNE'. "
        "[original product: LOMO ESPAGNOL 20 tranches U 100g] "
        "[original company: COOPERATIVE U ENSEIGNE] "
        '[original Reason (fr): "NON CONFORMITE MICROBIOLOGIQUE"]'),
}


# ── E — two Pending rows whose URL cites a different recall ───────────────
PENDING_WRONG_URL = {
    "richiamo_pediavit_complesso_b_sciroppo": (
        "url cites a different recall: this row is about TUNA FILLETS and "
        "histamine (Company \"IN's Mercato\", Brand \"Selezione Piu\", "
        "Pathogen \"Istamina exceeding limit\", Date 2026-10-20), and its "
        "own URL is the Ministero della Salute recall form for PEDIAVIT "
        "COMPLESSO B SCIROPPO, lot 26040043 — a vitamin-B syrup. The row's "
        "Notes record \"Discovered via news: ilfattoalimentare.it\": the "
        "fields come from a news article and the ministry URL was attached "
        "separately. It PASSED the full publish gate on 2026-10-21 and was "
        "one promoter run from the dashboard and the subscriber mail. This is "
        "a CONTENT verdict and not a reachability one: it rests entirely on "
        "this row's own URL, which names the recalled product, and no "
        "claim is made here about whether any page could be fetched. The "
        "tuna recall may well be real — re-promote this row once the "
        "ministry's own notice FOR THE TUNA FILLETS is cited in the URL "
        "field. See tools/repair_2026_10_21.py section E. " + OP),
    "cartello%20di%20richiamo%20tahina%20arkan": (
        "url cites a different recall: this row is about CARREFOUR COD FISH "
        "FILLET (Company and Brand \"Carrefour\", Reason \"Non conformity "
        "microbiological\", Date 2026-10-19), and its own URL is the "
        "Ministero della Salute recall form for TAHINA ARKAN — tahini. The "
        "row's Notes record \"Discovered via news: ilfattoalimentare.it\": "
        "the fields come from a news article and the ministry URL was "
        "attached separately. It PASSED the full publish gate on 2026-10-21 "
        "and was one promoter run from publication. This is a CONTENT verdict "
        "and not a reachability one: it rests entirely on this row's own "
        "URL, which names the recalled product, and no claim is made here "
        "about whether any page could be fetched. The cod recall may well "
        "be real — re-promote this row once the ministry's own notice FOR "
        "THE COD FILLET is cited in the URL field. See "
        "tools/repair_2026_10_21.py section E. " + OP),
}

# ── helpers (same shape as tools/repair_2026_10_20.py) ─────────────────────

def _head(ws):
    return [c.value for c in ws[1]]


def _cols(ws):
    return {h: i for i, h in enumerate(_head(ws))}


def _archive(arch, row, reason):
    out = []
    for h in _head(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append("operator review " + TODAY)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        else:
            out.append(row.get(h, ""))
    arch.append(out)


def _strip_stamps(notes: str) -> str:
    for stamp in CLEANUP_STAMPS:
        notes = notes.replace(stamp, "")
    return " ".join(notes.split())


def _append_originals(notes: str, orig: dict) -> str:
    bits = []
    if orig.get("product"):
        bits.append(f'[original product: {orig["product"]}]')
    if orig.get("company"):
        bits.append(f'[original company: {orig["company"]}]')
    if orig.get("brand"):
        bits.append(f'[original brand: {orig["brand"]}]')
    if orig.get("reason"):
        bits.append(f'[original Reason (fr): "{orig["reason"]}"]')
    return (notes + " " + " ".join(bits)).strip() if bits else notes


# ── the fixes ──────────────────────────────────────────────────────────────

def translate_fiches(wb) -> int:
    """B — Product/Company/Brand/Reason read off each row's own fiche.

    Applied to every sheet that holds a copy of the row, keyed on the fiche
    number in the URL so it is indifferent to row order on main.
    """
    n = 0
    for sheet in ("Recalls", "Weekly_Review", "Pending"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        col = _cols(ws)
        if "URL" not in col or "Product" not in col or "Notes" not in col:
            continue
        for row in ws.iter_rows(min_row=2):
            url = str(row[col["URL"]].value or "").lower()
            key = next((k for k in FICHES if k in url), None)
            if key is None:
                continue
            fix = FICHES[key]
            changed = False
            for field, value in fix.items():
                if field.startswith("_") or field not in col:
                    continue
                if row[col[field]].value != value:
                    row[col[field]].value = value
                    changed = True
            notes = _strip_stamps(str(row[col["Notes"]].value or ""))
            if changed:
                notes = _append_originals(notes, fix.get("_orig", {}))
                notes = (notes + " " + OP + " " + fix["_note"]).strip()
            row[col["Notes"]].value = notes
            if changed:
                if "LastUpdated" in col:
                    row[col["LastUpdated"]].value = TODAY
                n += 1
                print(f"  {sheet}: {key} -> {str(fix['Product'])[:56]}")
    return n


def remove_feed_row(wb) -> int:
    """C — one recall, one row; and a feed is not a notice."""
    ws, arch = wb["Recalls"], wb["Rejected"]
    head = _head(ws)
    ucol = head.index("URL") + 1
    n = 0
    for r in range(ws.max_row, 1, -1):
        if FEED_URL not in str(ws.cell(r, ucol).value or "").lower():
            continue
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
        _archive(arch, row, FEED_DUP_REASON)
        ws.delete_rows(r, 1)
        n += 1
        print(f"  Recalls -> Rejected (duplicate, feed URL): "
              f"{row.get('Date')} {row.get('Source')} "
              f"{str(row.get('Product'))[:36]!r}")
    for sheet in ("Weekly_Review", "Pending"):
        if sheet not in wb.sheetnames:
            continue
        wr = wb[sheet]
        col = _cols(wr)
        if "URL" not in col:
            continue
        for r in range(wr.max_row, 1, -1):
            if FEED_URL in str(wr.cell(r, col["URL"] + 1).value or "").lower():
                wr.delete_rows(r, 1)
                print(f"  {sheet}: CFIA feed row deleted")
    return n


def archive_wrong_url_pending(wb) -> int:
    """E — a row whose own URL names another recall cannot publish."""
    if "Pending" not in wb.sheetnames:
        return 0
    ws, arch = wb["Pending"], wb["Rejected"]
    head = _head(ws)
    ucol = head.index("URL") + 1
    n = 0
    for r in range(ws.max_row, 1, -1):
        url = str(ws.cell(r, ucol).value or "").lower()
        key = next((k for k in PENDING_WRONG_URL if k in url), None)
        if key is None:
            continue
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
        _archive(arch, row, PENDING_WRONG_URL[key])
        ws.delete_rows(r, 1)
        n += 1
        print(f"  Pending -> Rejected (url cites another recall): "
              f"{row.get('Date')} {str(row.get('Product'))[:30]!r}")
    return n


def add_missed_recall() -> None:
    """D — RappelConso fiche 23846, via add_manual_row so the dedup rule
    and the publish gate both get their say."""
    import openpyxl

    wb = openpyxl.load_workbook(XLSX, read_only=True)
    present = False
    try:
        for sheet in ("Recalls", "Pending"):
            if sheet not in wb.sheetnames:
                continue
            rows = wb[sheet].iter_rows(values_only=True)
            head = list(next(rows))
            if "URL" not in head:
                continue
            i = head.index("URL")
            present |= any(
                row and "fiche-rappel/23846" in str(row[i] or "").lower()
                for row in rows)
    finally:
        wb.close()
    if present:
        print("  fiche 23846 already in Recalls or Pending — left alone")
        return
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump([LOMO_ROW], fh, ensure_ascii=False)
        path = fh.name
    out = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), path],
        cwd=str(ROOT), capture_output=True, text=True)
    print((out.stdout or "").rstrip()[-700:])
    if out.returncode != 0:
        print("  add_manual_row REFUSED fiche 23846 (reported, not worked "
              "around):", (out.stderr or "")[-700:])


def main() -> int:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    n_tr = translate_fiches(wb)
    n_rm = remove_feed_row(wb)
    n_wu = archive_wrong_url_pending(wb)
    wb.save(XLSX)
    wb.close()
    print(f"fiche rows translated: {n_tr} · feed rows removed: {n_rm} · "
          f"Pending archived (wrong URL): {n_wu}")
    add_missed_recall()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON_MIRROR)} rows")
    print(f"ROWS_REMOVED={n_rm}")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
