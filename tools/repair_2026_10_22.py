# -*- coding: utf-8 -*-
"""Morning fix 2026-10-22. NO ROW LEAVES Recalls — the commit message needs
no "remove-rows". One row leaves PENDING, which is a queue, not an archive,
and its archive copy is already on file.

A. A RECALL A PERSON REJECTED YESTERDAY WAS QUEUED TO PUBLISH TODAY.
   ───────────────────────────────────────────────────────────────────
   Pending row 22 on the 2026-10-22 workbook:

       Date     2026-10-19          Source   Ministero della Salute (IT)
       Company  Carrefour           Brand    Carrefour
       Product  'Cod fish fillet'   Status   'pending'
       URL      .../avvisi_sicurezza_alimentare/
                Cartello%20di%20Richiamo%20Tahina%20Arkan_1792170251.pdf

   `python -m pipeline.promote_gate_passing` reports it as one of exactly two
   Pending rows that "pass the full publish gate". Its own URL is the
   ministry's recall form for TAHINA ARKAN — tahini. The row says cod.

   An operator had already read it and refused it. Rejected row 2204, stamped
   `operator review 2026-10-21`:

       "url cites a different recall: this row is about CARREFOUR COD FISH
        FILLET ... and its own URL is the Ministero della Salute recall form
        for TAHINA ARKAN — tahini. The row's Notes record 'Discovered via
        news: ilfattoalimentare.it': the fields come from a news article and
        the ministry URL was attached separately."

   Where the row came from is visible two sheets away: Weekly_Rejected 304
   holds the news headline the fields were cut out of — "Carrefour,
   richiamato pesce spada. Segnalata anche tahina" (Carrefour, swordfish
   recalled; tahini also reported). One article, two recalls. The collector
   took the retailer and a fish from the first and the ministry PDF from the
   second, and nothing it produced names a cod.

   This script removes the Pending copy and annotates the archive row to say
   the re-queue was found and removed. The archive row itself is kept
   verbatim — archives are append-only.

   The reason it came back at all is in pipeline/merge_master.py in this same
   upload: `_is_terminal_rejection` could recognise a human decision only
   through a machine-shaped stamp ("REJECTED — <code>"), so an operator
   writing a sentence was filed as RETRYABLE. Measured on this workbook: 70
   of the 123 archive rows whose RejectedBy names an operator review or a
   morning-fix pass. tests/test_human_rejection_in_prose_is_terminal.py.

   NOT REMOVED, deliberately: Pending row 21, the IN's Mercato / Selezione
   Più frozen tuna row (histamine, 2026-10-20), which the SAME operator
   refused on the SAME day for the same kind of defect (Rejected 2205 — its
   URL was then the ministry's form for PEDIAVIT COMPLESSO B SCIROPPO, a
   vitamin-B syrup). It is back on a DIFFERENT URL,

       .../Cartello%20Richiamo%20Filetti%20di%20Tonno%20Surgelato_1792575197.pdf

   and that filename is the ministry's own title for the frozen-tuna-fillet
   recall. The collector attached the right document this time, so the row
   is repaired, not re-queued, and it is left to publish. Its Italian
   "istamina" is translated below.

B. SIX PUBLISHED ROWS ARE NOT IN ENGLISH. Four carry reviewer 3's
   "[needs cleanup: …]" stamp; two do not, and the two are the point.
   ──────────────────────────────────────────────────────────────────
   Every fiche below was READ on 2026-10-22 and each row's note quotes the
   fiche's own fields. Nothing is inferred.

       row  3  fiche 23902  stamped
       row  4  fiche 23900  NOT STAMPED — Product is three French dish
                            names and Reason reads "Presence of listéria".
                            product_needs_english() needs a French FUNCTION
                            word or a word from _FRENCH_FOOD_WORDS; "crousty
                            poulet et riz et sauce blanche" has "et", which
                            is in neither list, and "poulet"/"riz"/"sauce"
                            are not in _FRENCH_FOOD_WORDS either.
       row  5  fiche 23898  stamped
       row  6  fiche 23784  stamped
       row  7  fiche 23905  stamped, and see C
       row 43  GIS (PL)     NOT STAMPED — Reason is one whole Polish
                            sentence. looks_non_english() did not fire on it.

   The two unstamped rows are why the operator rule says to read every
   Product published in the last 26 h by hand and not to trust the detector.

C. ROW 7 PUBLISHES FOUR LOT CODES WHERE THE FOOD NAME BELONGS.
   ───────────────────────────────────────────────────────────
       Product 'sachet 1kg: c120261010a106 - sachet 1kg: c120261010a107 -
                sachet 2kg: c120261010b306 - sachet 2kg: c120261010b307'

   The fiche's own heading is "SUCRE ROUX PURE CANNE" — pure cane brown
   sugar. RappelConso's `modeles_ou_references` field held the packaging
   line, and the scraper takes that field first and falls through to
   `libelle` (the heading) only when it is EMPTY, which a code list is not.

   The gate rule written for this shape two days ago,
   _product_is_an_identifier_blockers_2026_10_20, asks "does Product contain
   a word" — and "sachet" and "kg" are words, so it passed. The source-side
   fix and the stricter test are in scrapers/europe_eu/rappelconso.py and
   scrapers/_product_text.py in this same upload.

   SIX OLDER PUBLISHED ROWS HAVE THE SAME SHAPE and are NOT repaired here,
   because they cannot be repaired without inventing:

       row  959  2026-07-20  'dss3001021-00'                 fiche 22886
       row 1114  2026-07-02  'Lot 260226'                    fiche 22715
       row 1125  2026-07-01  'Lot n° 312'                    fiche 22705
       row 1144  2026-06-30  'Lot n° 314'                    fiche 22680
       row 1368  2026-06-01  '10156-plh'                     fiche 22406
       row 1478  2026-05-20  'Lot g22014; barcode 69364891…' fiche 22320

   Fiche 22886 was opened on 2026-10-22: RappelConso no longer serves it and
   redirects to the current category listing. The headings these rows need
   are not available from the authority any more, and a product name is not
   something to reconstruct from a lot code. Left as they are, named here,
   and named in the morning message as a decision for the operator. That is
   also why scrapers/_product_text.is_identifier_only is NOT wired into
   publish_blockers in this upload: it would refuse these six and the only
   ways to make the suite green would be to invent six names or to delete
   six published rows.

D. ONE ROW CARRIES THE SAME CLEANUP STAMP TWICE.
   ─────────────────────────────────────────────
   Row 7's Notes end "[needs cleanup: Reason not in English] … [needs
   cleanup: Reason not in English]". pipeline/promote_gate_passing.py's own
   comment claims "both are idempotent, so a row promoted here and rewritten
   there carries one stamp, not two" — merge_master._write_sheet is
   (`if _stamp not in _notes`), promote_gate_passing was not (`+=`). Fixed in
   that file in this upload, with tests/test_cleanup_stamps_are_idempotent.py;
   the duplicate already written is collapsed here for every row, not just
   row 7.

E. FIVE RECALLS THE PIPELINE MISSED, each read on the authority's own page.
   ───────────────────────────────────────────────────────────────────────
   Queued through tools/add_manual_row.py, so they enter Pending under the
   same dedup, validation and review as everything else. Provenance is in
   each row's Notes. See MANUAL_ROWS below.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

STAMPS = (
    "[needs cleanup: Product not in English]",
    "[needs cleanup: Company/Brand not in English]",
    "[needs cleanup: Reason not in English]",
)

# ── B + C: the published rows, keyed on the fiche / notice URL ─────────────
#
# `product` / `reason` are the English values to write. `note` is appended to
# Notes and carries the original text, so the row still matches the notice and
# the dashboard can show it on hover. `drop_stamps` lists the cleanup stamps
# this repair discharges.
TRANSLATIONS = {
    # row 3 — fiche 23902, read 2026-10-22
    "rappel.conso.gouv.fr/fiche-rappel/23902": {
        "product": "Sardine fillet packed by the store "
                   "(FILET DE SARDINE EMBALLE PAR LE MAGASIN)",
        "reason": "Presence of histamine above the authorised limit.",
        "note": '[original product: filet de sardine emballe par le magasin] '
                '[original Reason (fr): "Présence d\'histamine supérieure à '
                'la norme autorisée"] [fiche 23902 read 2026-10-22: heading '
                '"FILET DE SARDINE", Marque "SANS", Risque "Toxines endogènes '
                ': histamine", Origine de la fiche "SOCIETE DE DISTRIBUTION '
                'DE COUTRAS E.LECLERC"]',
        "drop_stamps": ("[needs cleanup: Product not in English]",
                        "[needs cleanup: Reason not in English]"),
    },
    # row 4 — fiche 23900, read 2026-10-22. NOT stamped by any reviewer.
    "rappel.conso.gouv.fr/fiche-rappel/23900": {
        "product": "Crousty chicken and rice with white sauce; Crousty "
                   "chicken and rice with spicy white sauce; Crousty chicken "
                   "and rice with curry sauce (Crousty poulet et riz et sauce "
                   "blanche, sauce blanche épicée, sauce curry), unbranded",
        "reason": "Presence of Listeria. Consumer risk: Listeria "
                  "monocytogenes, the agent of listeriosis.",
        "note": '[original product: crousty poulet et riz et sauce blanche, '
                'crousty poulet et riz sauce blanche épicée, crousty poulet '
                'et riz sauce curry] [original Reason (fr): "Présence de '
                'Listéria"] [fiche 23900 read 2026-10-22: réf. 2026-10-0228, '
                'Marque "sans marque", Risque "Listeria monocytogenes (agent '
                'responsable de la listériose)", Distributeurs "Carrefour '
                'hypermarché Ermont uniquement"] [morning fix 2026-10-22: '
                'this row carried NO cleanup stamp — product_needs_english() '
                'does not fire on it, see tools/repair_2026_10_22.py section B]',
        "drop_stamps": (),
    },
    # row 5 — fiche 23898, read 2026-10-22
    "rappel.conso.gouv.fr/fiche-rappel/23898": {
        "product": "Bouchot mussels STG sold at the service counter "
                   "(Moules de bouchot STG vendues au rayon traditionnel)",
        "reason": "Non-compliant analysis for E. coli.",
        "note": '[original product: moules de bouchot stg vendues au rayon '
                'traditionnel] [original Reason (fr): "Analyse non conforme '
                'en E. coli"] [fiche 23898 read 2026-10-22: Marque "Filière '
                'Qualité Carrefour", Risque "Escherichia coli", Origine de la '
                'fiche "CARREFOUR FRANCE CARREFOUR"] [STG = Spécialité '
                'Traditionnelle Garantie, a protected designation, kept as '
                'published]',
        "drop_stamps": ("[needs cleanup: Product not in English]",
                        "[needs cleanup: Reason not in English]"),
    },
    # row 6 — fiche 23784, read 2026-10-22
    "rappel.conso.gouv.fr/fiche-rappel/23784": {
        "product": "Ground beef prepared to order (Bœuf haché à la demande)",
        "reason": "Presence of Salmonella spp. (the agent of salmonellosis).",
        "note": '[original product: bœuf haché à la demande] [original Reason '
                '(fr): "Présence de Salmonella spp (agent responsable de la '
                'salmonellose)"] [fiche 23784 read 2026-10-22: Marque "Auchan '
                '(en vente assistée)", Origine de la fiche "AUCHAN RETAIL '
                'SERVICES AUCHAN", Distributeur "LE HYPERMARCHÉ AUCHAN MANTES" '
                'only, Yvelines (78)]',
        "drop_stamps": ("[needs cleanup: Product not in English]",
                        "[needs cleanup: Reason not in English]"),
    },
    # row 7 — fiche 23905, read 2026-10-22. See section C.
    "rappel.conso.gouv.fr/fiche-rappel/23905": {
        "product": "Pure cane brown sugar (SUCRE ROUX PURE CANNE), 1 kg and "
                   "2 kg bags, lots C120261010A106, C120261010A107, "
                   "C120261010B306, C120261010B307",
        "reason": "Suspected presence of white plastic foreign bodies.",
        "note": '[original product: sachet 1kg: c120261010a106 - sachet 1kg: '
                'c120261010a107 - sachet 2kg: c120261010b306 - sachet 2kg: '
                'c120261010b307] [original Reason (fr): "Suspicion de '
                'présence de corps étrangers en plastique blanc"] [fiche '
                '23905 read 2026-10-22: heading "SUCRE ROUX PURE CANNE", '
                'Marque "GARDEL SA", Risque "Inertes (verre, métal, '
                'plastique, papier, textile…)", Distributeur "LEADER PRICE", '
                'GTINs 3337708810011 (2 kg) and 3337708820027 (1 kg)] '
                '[morning fix 2026-10-22: Product held only the fiche\'s '
                '"Modèles ou références" packaging line — four lot codes and '
                'no food name. The heading is the product; the codes are kept '
                'above. Source-side fix in scrapers/europe_eu/rappelconso.py]',
        "drop_stamps": ("[needs cleanup: Product not in English]",
                        "[needs cleanup: Reason not in English]"),
    },
    # row 43 — GIS (PL), read 2026-10-22. NOT stamped by any reviewer.
    "gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-zywnosci-wykrycie-"
    "obecnosci-bakterii-listeria-monocytogenes-w-jednej-partii-salami": {
        "product": "Salami, three variants, 250 g each: muesli salami "
                   "(Salami musli), peppered salami (Salami z pieprzem), "
                   "Mexican salami (Salami meksykańskie); lot 11061",
        "reason": "Public food warning: Listeria monocytogenes detected in "
                  "one batch of salami — found in 3 of 5 samples during "
                  "official Veterinary Inspection action.",
        "note": '[original product: Salami musli 250 g; Salami z pieprzem '
                '250 g; Salami meksykańskie 250 g] [original Reason (pl): '
                '"Ostrzeżenie publiczne dotyczące żywności: wykrycie '
                'obecności bakterii Listeria monocytogenes w jednej partii '
                'salami"] [GIS notice read 2026-10-22: producer "Bell Polska '
                'Sp. z o.o., ul. Mokra, 11, 32-005 Niepołomice, WNI '
                '12190301", lot 11061 for all three variants, best-before '
                '03.01.2027 (musli) and 01.01.2027 (z pieprzem, '
                'meksykańskie), "wykryto obecność Listeria monocytogenes w 3 '
                'z 5 próbek w jednej partii produktu", published 19.10.2026. '
                'This answers the confirm-agent hold of 2026-10-21 ("producer, '
                'lot and best-before are on the detail page").] [morning fix '
                '2026-10-22: this row carried NO cleanup stamp — '
                'looks_non_english() did not fire on the Polish Reason]',
        "drop_stamps": (),
    },
}

# ── A: the re-queued row ───────────────────────────────────────────────────
PENDING_REQUEUE_OUT = (
    "cartello%20di%20richiamo%20tahina%20arkan_1792170251.pdf")

#: F — TWO PENDING ROWS THAT HOLD THE SUITE RED ON MAIN.
#:
#: Both failures reproduce on a fresh clone of main with no changes, so
#: tools/rebase_and_verify.py would refuse any upload until they are cleared:
#:
#:   test_alert_number_year_matches_date[Pending]
#:       NAFDAC (NG), Date 2026-10-21, URL
#:       .../public-alert-no-35-2025-substandard-batches-of-annmox-...
#:       The alert number in the regulator's own URL is 35/2025; the row says
#:       2026. And the product is "Amoxicillin suspensions 125mg/5ml" — a
#:       prescription antibiotic, not a food (R1). It can never publish.
#:   test_register_urls_pass_their_own_gate[us]
#:       USDA FSIS, URL https://www.fsis.usda.gov/recalls-alerts?search=026-2026
#:       A search query, not a per-recall notice. The SAME recall is already
#:       published at Recalls 2026-10-17 on FSIS's own per-recall URL
#:       (.../recalls-alerts/kayem-foods-inc--recalls-meat-and-pou...), and
#:       an operator archived this copy on 2026-10-20: "duplicate + not a
#:       per-recall notice".
#:
#: Each archive copy is already on file (Rejected 2201 / Weekly_Rejected 289
#: for Kayem, Weekly_Rejected 540 for NAFDAC), so nothing is lost. Removed
#: from the Pending QUEUE only; the archives are untouched and append-only.
#:
#: The NAFDAC archive row's stated reason is WRONG and is not the reason used
#: here: it reads "URL domain does not match Source 'NAFDAC (NG)' (expected
#: fda.gov)", which is reviewer 2 reading "fda" out of "nafdac". That is
#: fixed in pipeline/recall_review_agent.py in this same upload, with
#: tests/test_a_regulators_own_host_is_not_the_us_fdas.py. The reason the row
#: goes is on the row itself: a drug, and a 2025 alert number under a 2026
#: Date.
PENDING_UNPUBLISHABLE = {
    "public-alert-no-35-2025": (
        "not food, and the alert number in the regulator's own URL is from "
        "2025 under a 2026 Date: NAFDAC public alert 35/2025 concerns Annmox "
        "and Jawamox amoxicillin oral suspensions 125mg/5ml, a prescription "
        "antibiotic (R1 is food only), and the row's Date 2026-10-21 is the "
        "day a news article was read, not the day the alert was issued. "
        "Removed from the Pending queue by the morning fix pass 2026-10-22; "
        "the archive copy in Weekly_Rejected stands. NOTE: that archive row's "
        "own stated reason — 'URL domain does not match Source NAFDAC (NG) "
        "(expected fda.gov)' — is wrong; nafdac.gov.ng is NAFDAC's own "
        "domain, and the substring bug behind it is fixed in "
        "pipeline/recall_review_agent.py in this upload."),
    "recalls-alerts?search=026-2026": (
        "duplicate and not a per-recall notice: USDA FSIS recall 026-2026, "
        "Kayem Foods Inc., is already PUBLISHED in Recalls at 2026-10-17 on "
        "FSIS's own per-recall URL, and this row's URL is a search query "
        "whose answer can change. An operator archived this copy on "
        "2026-10-20 for exactly this; the re-queue is removed from the "
        "Pending queue by the morning fix pass 2026-10-22 and the archive "
        "copies stand."),
}

PENDING_REQUEUE_NOTE = (
    "[morning fix 2026-10-22: a copy of this row was found back in Pending "
    "with Status 'pending', reported by pipeline.promote_gate_passing as one "
    "of two rows passing the full publish gate. The Pending copy was removed; "
    "this archive row and its operator verdict stand. It returned because "
    "merge_master._is_terminal_rejection read an operator's prose rejection "
    "as retryable — fixed in the same upload, with "
    "tests/test_human_rejection_in_prose_is_terminal.py.]")

# ── A: Pending row 21, the repaired tuna row ──────────────────────────────
PENDING_TUNA = "cartello%20richiamo%20filetti%20di%20tonno%20surgelato"
PENDING_TUNA_FIX = {
    "Pathogen": "Histamine / scombrotoxin",
    "Reason": "Possible presence of histamine above legal limits.",
    "note": '[morning fix 2026-10-22: Pathogen and Reason said "istamina", '
            'the Italian. Translated; no other field touched. The row is left '
            'to publish: the operator refused its predecessor on 2026-10-21 '
            '(Rejected 2205) because the URL then pointed at the ministry\'s '
            'recall form for PEDIAVIT COMPLESSO B SCIROPPO, a vitamin-B '
            'syrup, and the URL on the row now is the ministry\'s own form '
            'for the frozen tuna fillet recall — "Cartello Richiamo Filetti '
            'di Tonno Surgelato". A different document, not the same defect.] '
            '[original Pathogen: Istamina exceeding limit] [original Reason '
            '(it, partial): "Possible presence of istamina above legal '
            'limits"]',
}

# ── E: the missed recalls ─────────────────────────────────────────────────
MANUAL_ROWS = ROOT / "tools" / "manual_rows_2026_10_22.json"


def _head(ws):
    return [str(c.value) for c in next(ws.iter_rows(min_row=1, max_row=1))]


def _collapse_repeated_stamps(notes: str) -> str:
    """D — one cleanup stamp per row, keeping the first occurrence.

    Returns the input UNCHANGED when no stamp was actually duplicated. The
    first version of this ran a whitespace normalisation over every note it
    was handed, which rewrote 108 rows whose only fault was a double space —
    churn that buries the real change in the rebase diff and asks
    rebase_and_verify to re-verify rows nothing is wrong with.
    """
    out = notes
    removed = False
    for stamp in STAMPS:
        first = out.find(stamp)
        if first < 0:
            continue
        head, tail = out[:first + len(stamp)], out[first + len(stamp):]
        if stamp in tail:
            out = head + tail.replace(stamp, "")
            removed = True
    if not removed:
        return notes
    return re.sub(r"\s{2,}", " ", out).strip()


def translate_published(wb) -> int:
    """B + C — six published rows into English, originals kept in Notes.

    Recalls AND Weekly_Review. Weekly_Review is a SLICE of Recalls — the
    Sunday review carries its own copy of each week's rows — so a row
    translated in one sheet and not the other leaves the register saying two
    different things about the same recall. Caught by this upload's own
    tests/test_product_is_not_only_identifiers.py, which found fiche 23905's
    lot line still in Weekly_Review after Recalls was repaired.
    """
    total = 0
    for sheet in ("Recalls", "Weekly_Review"):
        if sheet in wb.sheetnames:
            total += _translate_sheet(wb[sheet], sheet)
    return total


def _translate_sheet(ws, sheet: str) -> int:
    head = _head(ws)
    c_url = head.index("URL") + 1
    c_prod = head.index("Product") + 1
    c_reas = head.index("Reason") + 1
    c_note = head.index("Notes") + 1
    hit = 0
    for r in range(2, ws.max_row + 1):
        url = str(ws.cell(r, c_url).value or "").lower()
        spec = next((v for k, v in TRANSLATIONS.items() if k.lower() in url),
                    None)
        if spec is None:
            continue
        hit += 1
        notes = str(ws.cell(r, c_note).value or "")
        for stamp in spec["drop_stamps"]:
            notes = notes.replace(stamp, "")
        if spec["note"] not in notes:
            notes = (notes.strip() + " " + spec["note"]).strip()
        ws.cell(r, c_prod).value = spec["product"]
        ws.cell(r, c_reas).value = spec["reason"]
        ws.cell(r, c_note).value = _collapse_repeated_stamps(notes)
        print(f"  {sheet} row {r}: translated ({url[-28:]})")
    if sheet == "Recalls" and hit != len(TRANSLATIONS):
        raise SystemExit(
            f"expected {len(TRANSLATIONS)} rows to translate in Recalls, "
            f"matched {hit} — the register moved under this script; re-read "
            "the rows before shipping it")
    return hit


def collapse_all_duplicate_stamps(wb) -> int:
    """D — every sheet, every row: a stamp appears at most once."""
    n = 0
    for name in ("Recalls", "Pending", "Weekly_Review"):
        if name not in wb.sheetnames:
            continue
        ws = wb[name]
        head = _head(ws)
        if "Notes" not in head:
            continue
        c_note = head.index("Notes") + 1
        for r in range(2, ws.max_row + 1):
            old = str(ws.cell(r, c_note).value or "")
            if not old:
                continue
            new = _collapse_repeated_stamps(old)
            if new != old:
                ws.cell(r, c_note).value = new
                n += 1
                print(f"  {name} row {r}: duplicate cleanup stamp collapsed")
    return n


def remove_pending_requeue(wb) -> int:
    """A — the Pending copy of a row an operator rejected on 2026-10-21."""
    ws = wb["Pending"]
    head = _head(ws)
    c_url = head.index("URL") + 1
    removed = 0
    for r in range(ws.max_row, 1, -1):
        url = str(ws.cell(r, c_url).value or "").lower()
        if PENDING_REQUEUE_OUT in url:
            prod = str(ws.cell(r, head.index("Product") + 1).value or "")
            ws.delete_rows(r, 1)
            removed += 1
            print(f"  Pending row {r} REMOVED: {prod[:40]!r} — operator "
                  f"rejected 2026-10-21, archive copy stands")
    if removed != 1:
        raise SystemExit(
            f"expected exactly 1 re-queued Pending row, found {removed} — "
            "do not ship this script until that is understood")
    return removed


def annotate_requeue_archive(wb) -> int:
    """A — record on the archive row that the re-queue was removed."""
    n = 0
    for name in ("Rejected", "Weekly_Rejected"):
        ws = wb[name]
        head = _head(ws)
        if "URL" not in head or "Notes" not in head:
            continue
        c_url = head.index("URL") + 1
        c_note = head.index("Notes") + 1
        for r in range(2, ws.max_row + 1):
            url = str(ws.cell(r, c_url).value or "").lower()
            if PENDING_REQUEUE_OUT not in url:
                continue
            notes = str(ws.cell(r, c_note).value or "")
            if PENDING_REQUEUE_NOTE in notes:
                continue
            ws.cell(r, c_note).value = (
                notes.strip() + " " + PENDING_REQUEUE_NOTE).strip()
            n += 1
            print(f"  {name} row {r}: annotated (re-queue removed)")
    return n


def remove_unpublishable_pending(wb) -> int:
    """F — two rows that can never publish and hold the suite RED on main.

    The archive copies are already on file, so the row's verdict is recorded;
    what is removed is the queue entry. The reason is stamped onto the
    archive row so the next reader does not have to find this script.
    """
    ws = wb["Pending"]
    head = _head(ws)
    c_url = head.index("URL") + 1
    gone = {}
    for r in range(ws.max_row, 1, -1):
        url = str(ws.cell(r, c_url).value or "").lower()
        key = next((k for k in PENDING_UNPUBLISHABLE if k.lower() in url),
                   None)
        if key is None:
            continue
        prod = str(ws.cell(r, head.index("Product") + 1).value or "")
        ws.delete_rows(r, 1)
        gone[key] = prod
        print(f"  Pending row {r} REMOVED: {prod[:44]!r} — {key}")
    missing = set(PENDING_UNPUBLISHABLE) - set(gone)
    if missing:
        raise SystemExit(
            "expected these unpublishable Pending rows and did not find "
            f"them: {sorted(missing)} — the register moved under this script")
    # Stamp the reason onto every archive copy of each URL.
    stamped = 0
    for name in ("Rejected", "Weekly_Rejected"):
        ws2 = wb[name]
        h2 = _head(ws2)
        if "URL" not in h2 or "Notes" not in h2:
            continue
        c_u, c_n = h2.index("URL") + 1, h2.index("Notes") + 1
        for r in range(2, ws2.max_row + 1):
            url = str(ws2.cell(r, c_u).value or "").lower()
            key = next((k for k in PENDING_UNPUBLISHABLE if k.lower() in url),
                       None)
            if key is None:
                continue
            note = f"[morning fix 2026-10-22: {PENDING_UNPUBLISHABLE[key]}]"
            notes = str(ws2.cell(r, c_n).value or "")
            if note in notes:
                continue
            ws2.cell(r, c_n).value = (notes.strip() + " " + note).strip()
            stamped += 1
    print(f"  archive rows stamped with the removal reason: {stamped}")
    return len(gone)


def translate_pending_tuna(wb) -> int:
    """A — Pending row 21's Italian 'istamina'."""
    ws = wb["Pending"]
    head = _head(ws)
    c_url = head.index("URL") + 1
    c_note = head.index("Notes") + 1
    n = 0
    for r in range(2, ws.max_row + 1):
        url = str(ws.cell(r, c_url).value or "").lower()
        if PENDING_TUNA not in url:
            continue
        for field in ("Pathogen", "Reason"):
            ws.cell(r, head.index(field) + 1).value = PENDING_TUNA_FIX[field]
        notes = str(ws.cell(r, c_note).value or "")
        if PENDING_TUNA_FIX["note"] not in notes:
            ws.cell(r, c_note).value = (
                notes.strip() + " " + PENDING_TUNA_FIX["note"]).strip()
        n += 1
        print(f"  Pending row {r}: 'istamina' -> 'Histamine / scombrotoxin'")
    if n != 1:
        raise SystemExit(
            f"expected exactly 1 tuna row in Pending, found {n}")
    return n


def queue_missed_recalls() -> int:
    """E — through tools/add_manual_row.py's own code path."""
    from datetime import datetime, timezone

    from pipeline import merge_master as mm
    sys.path.insert(0, str(ROOT / "tools"))
    from add_manual_row import build_recall                  # noqa: WPS433

    specs = json.loads(MANUAL_ROWS.read_text(encoding="utf-8"))
    recalls = [build_recall(s) for s in specs]
    approved = mm.load_existing(XLSX)
    pending = mm.load_pending(XLSX)
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    updated = mm.append_to_pending(pending, approved, recalls, scraped_at)
    added = len(updated) - len(pending)
    if added:
        mm.save_xlsx_with_pending(approved, updated, XLSX)
    skipped = len(recalls) - added
    print(f"  manual rows: {added} queued to Pending, {skipped} refused by "
          f"dedup / near-duplicate (expected and reported in the message)")
    return added


def main() -> int:
    import openpyxl

    # Workbook edits first, saved, then the manual rows — add_manual_row's
    # path re-reads and re-writes the workbook itself, so it must not run
    # inside an open openpyxl handle.
    wb = openpyxl.load_workbook(XLSX)
    n_tr = translate_published(wb)
    n_rq = remove_pending_requeue(wb)
    n_an = annotate_requeue_archive(wb)
    n_up = remove_unpublishable_pending(wb)
    n_tu = translate_pending_tuna(wb)
    n_ds = collapse_all_duplicate_stamps(wb)
    wb.save(XLSX)
    wb.close()

    n_mr = queue_missed_recalls()

    print(f"\ntranslated: {n_tr} · pending re-queue removed: {n_rq} · "
          f"unpublishable pending removed: {n_up} · "
          f"archive rows annotated: {n_an} · pending tuna translated: {n_tu} · "
          f"duplicate stamps collapsed: {n_ds} · missed recalls queued: {n_mr}")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
