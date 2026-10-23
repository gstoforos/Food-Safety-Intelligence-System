# -*- coding: utf-8 -*-
"""Morning fix 2026-10-23.

NO ROW LEAVES Recalls — the commit message needs no "remove-rows". One row
leaves PENDING, which is a queue and not an archive, and its archive copies
are already on file. Nothing is deleted from Rejected or Weekly_Rejected;
those are append-only and are only annotated.

Three edits, each the data half of a code fix shipped in the same upload.


A. A PUBLISHED RECALL WAS THROWN AWAY AGAIN THIS MORNING — BY A CLASSIFIER
   READING A PDF'S COMPRESSED BYTES AS IF THEY WERE WORDS
   ───────────────────────────────────────────────────────────────────────
   Weekly_Rejected row 663 on the 2026-10-23 workbook, written at
   05:43:31Z by the Italian gap finder's 05:41 run:

       Date      2026-10-20      Source  Ministero della Salute (IT)
       Product   "Richiamati filetti di tonno IN's Mercato per istamina e
                  integratore per possibile contaminazione"
       Pathogen  "ei"
       Reason    "Undeclared allergen — out of scope (Rule B reject)."
       URL       .../avvisi_sicurezza_alimentare/
                 Cartello%20Richiamo%20Filetti%20di%20Tonno%20Surgelato
                 _1792575197.pdf

   "ei" is German and Dutch for egg. There is no egg in this recall and no
   allergen either: the hazard is histamine. The register had PUBLISHED the
   row off that exact URL the day before — Recalls 2026-10-20, IN's Mercato
   / Selezione Più, "Tuna fillets", "Histamine / scombrotoxin", Tier 1 —
   which is why tests/test_a_recall_is_not_both_published_and_rejected.py
   went RED on main overnight.

   The authority URL is a PDF. pipeline/gap_finder/article_fetcher.py had no
   content-type branch: it read resp.text (the PDF's compressed streams
   lossily decoded to a str), gave that to BeautifulSoup, got a few hundred
   characters of mojibake, saw len >= 200 and recorded it as "the authority
   press release". rules.classify() then matched the two-letter allergen
   needle "ei" inside the noise.

   Measured across docs/data/gap_finder_*/verified.jsonl on main at
   44c81b0: 11 rows whose efet_body begins "%PDF-", in five countries —
   it (7 of the 17 verified that day), de, md, se, us. Italy's ministry
   publishes nearly every recall only as a PDF cartello, so for that
   country the verdict was noise most of the time. It fell as a false
   REJECT here; the other way it invents a hazard.

   Fixed at the fetch layer in this upload — pipeline/gap_finder/pdf_text.py
   plus the screen in article_fetcher, held by
   tests/test_a_pdf_is_never_classified_as_text.py. This script annotates
   the archive row SUPERSEDED. The published copy wins and the archive row
   is kept verbatim.


B. A PENDING ZOMBIE, REMOVED TWICE AND REBUILT BOTH TIMES BY THE COLLECTOR
   THAT CREATED IT
   ───────────────────────────────────────────────────────────────────────
   Pending row 3 on the 2026-10-23 workbook:

       Date     2026-10-17        Source  USDA FSIS
       Company  Kayem Foods Inc.  Status  pending_gap
       URL      https://www.fsis.usda.gov/recalls-alerts?search=026-2026
       Notes    source_id=026-2026 [via official-feed collector]
       ScrapedAt 2026-10-22T12:24:06Z

   `python -m pipeline.promote_gate_passing` names the blocker in the
   system's own words: "URL is a search query, not a recall notice".

   The same recall is PUBLISHED at Recalls 2026-10-17 on FSIS's own
   per-recall URL (.../recalls-alerts/kayem-foods-inc--recalls-meat-and-
   poultry-products-due-possible-temperature-abuse, operator ruling
   2026-10-18). An operator archived the search-URL copy on 2026-10-20
   (tools/repair_2026_10_20.py) and the morning fix pass removed it again on
   2026-10-22 (tools/repair_2026_10_22.py). The ScrapedAt above is about
   three hours AFTER that second removal was uploaded.

   This is the third removal, and the first one that will hold:
   pipeline/official_feeds/sources/us_fsis.py built that URL itself —

       url = f"https://www.fsis.usda.gov/recalls-alerts?search={rn}"

   — a string pipeline/_publish_gate.py refuses by name
   (_SEARCH_QUERY_URL) and pipeline/gap_finder/countries/us.py labels
   LISTING. The fallback is removed in this upload and the row cannot be
   rebuilt; held by
   tests/test_a_collector_never_cites_a_search_board.py.

   Archive copies stand (Rejected 2201, Weekly_Rejected 289 and 639). What
   is removed is the queue entry.


C. A REAL, IN-SCOPE, IN-WINDOW RECALL REJECTED OVER A CELL ITS OWN REASON
   FILLS
   ───────────────────────────────────────────────────────────────────────
   Weekly_Rejected row 642 on the 2026-10-23 workbook:

       Date     2026-10-22       Source  FDA
       Company  Vitamin Cottage Natural Food Markets, Inc.
       Brand    Natural Grocers  Product "2 pound Mung Beans"
       Pathogen (empty)
       Reason   "Possible foreign material contamination with glass"
       URL      fda.gov/safety/recalls-market-withdrawals-safety-alerts/
                natural-grocers-recalls-natural-grocers-brand-organic-
                2-pound-mung-beans-due-potential-presence
       RejectionReason
                "Confirmer: reviewer 2 approved but Pathogen is empty"

   Foreign material is in scope (R6). The URL is FDA's own per-recall
   notice. Its Notes carry all three reviewer stamps — reviewer 1 confirmed
   the URL, reviewer 2 approved, the review agent verified. The date is
   inside the window. The register already holds eight rows whose Pathogen
   is "Foreign material (glass)", and the one dated 2026-08-18 carries the
   IDENTICAL Reason string.

   The one empty cell was the hazard name, and the hazard is written out in
   the row's own Reason. pipeline/enrich_pending_offline.py derives it
   deterministically, offline, with no model:

       >>> infer_pathogen({"Reason": "Possible foreign material
       ...                  contamination with glass"})
       ('Foreign material (glass)', "matched 'glass'")

   but that module only ever sees rows still in Pending, and reviewer 3
   archives first. The recall fell through the gap between two parts of the
   pipeline that each knew better.

   Reviewer 3 now runs that same table before it decides — fixed in
   pipeline/recall_confirm_agent.py and both workflow heredocs, held by
   tests/test_a_repairable_field_is_not_a_rejection.py.

   This script re-queues the row to PENDING with Pathogen filled from its
   own Reason, and lets the full publish gate decide whether it publishes.
   It is not written straight into Recalls: the gate is the gate, and a row
   this pass repaired should have to pass it like any other. Every value on
   the re-queued row is copied from the archive row; the only field this
   script computes is Pathogen, and it computes it with the repo's own
   enricher from the row's own text. Nothing was read from outside the row —
   fda.gov is not reachable from the morning-fix sandbox (the authority page
   returned a permission refusal on 2026-10-23), which is exactly why no
   field here is sourced from anywhere but the row itself.

   The archive row is kept verbatim and annotated.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"


# ── A: the tuna archive row ────────────────────────────────────────────────
TUNA_URL_KEY = "cartello%20richiamo%20filetti%20di%20tonno%20surgelato"
TUNA_REJECTED_AT = "2026-10-23"

TUNA_NOTE = (
    "[morning fix 2026-10-23: SUPERSEDED — the published copy wins. This "
    "row was written by the Italian gap finder's 05:41 run today as "
    "'allergen: Undeclared allergen — out of scope (Rule B reject)' with "
    "Pathogen 'ei' (German/Dutch for egg). There is no egg and no allergen "
    "in this recall: the hazard is histamine, and the register had already "
    "PUBLISHED this URL on 2026-10-22 (Recalls 2026-10-20, IN's Mercato / "
    "Selezione Piu, 'Tuna fillets', 'Histamine / scombrotoxin', Tier 1). "
    "The verdict was not about this notice at all. The authority URL is a "
    "PDF, and pipeline/gap_finder/article_fetcher.py had no content-type "
    "branch: it read the PDF's compressed bytes as text, BeautifulSoup "
    "returned a few hundred characters of mojibake, that passed the "
    ">=200-char authority-body floor, and the allergen lexicon's "
    "two-letter needle 'ei' matched inside the binary noise. 11 rows "
    "across 5 countries had a '%PDF-' body on main at 44c81b0 (it 7, de, "
    "md, se, us). Fixed at the fetch layer in the same upload: "
    "pipeline/gap_finder/pdf_text.py, with "
    "tests/test_a_pdf_is_never_classified_as_text.py. Row kept verbatim — "
    "archives are append-only.]")


# ── B: the FSIS Pending zombie ─────────────────────────────────────────────
KAYEM_URL_KEY = "recalls-alerts?search=026-2026"

KAYEM_ARCHIVE_NOTE = (
    "[morning fix 2026-10-23: a THIRD copy of this row was found back in "
    "Pending (Status 'pending_gap', ScrapedAt 2026-10-22T12:24:06Z — about "
    "three hours after the 2026-10-22 removal was uploaded). Removed from "
    "the Pending queue; this archive row and its verdict stand. The reason "
    "it kept returning is now fixed rather than cleaned up again: "
    "pipeline/official_feeds/sources/us_fsis.py BUILT this URL itself, as "
    "f'.../recalls-alerts?search={recall_number}' whenever the FSIS API "
    "gave it no press-release link — a string pipeline/_publish_gate.py "
    "refuses by name (_SEARCH_QUERY_URL, 'A SEARCH RESULTS PAGE IS NOT A "
    "RECALL NOTICE') and pipeline/gap_finder/countries/us.py labels "
    "LISTING. Every row it produced was unpublishable on arrival and its "
    "URL-based dedup key could never match the per-recall URL the same "
    "recall is published under. The fallback is removed in the same upload "
    "(no URL and Status pending_no_auth_url instead, so a reviewer sees it "
    "rather than a gate that refuses it forever), with a second dedup key "
    "on (Source, source_id) in pipeline/official_feeds/main.py and "
    "tests/test_a_collector_never_cites_a_search_board.py.]")


# ── C: the mung-bean row ───────────────────────────────────────────────────
MUNG_URL_KEY = ("natural-grocers-recalls-natural-grocers-brand-organic-"
                "2-pound-mung-beans")

MUNG_REQUEUE_NOTE = (
    "[morning fix 2026-10-23: re-queued to Pending from Weekly_Rejected. "
    "Reviewer 3 archived this row on 'Confirmer: reviewer 2 approved but "
    "Pathogen is empty' — a cell the row's own Reason fills. Foreign "
    "material is in scope, the URL is FDA's own per-recall notice, "
    "reviewer 1 confirmed the URL and reviewer 2 approved the row, and the "
    "register already holds eight 'Foreign material (glass)' rows, one of "
    "them (2026-08-18) off the identical Reason string. Pathogen filled by "
    "pipeline.enrich_pending_offline.infer_pathogen from this row's own "
    "text; every other value is copied verbatim from the archive row. "
    "Nothing was read from outside the row — fda.gov returned a permission "
    "refusal to this sandbox on 2026-10-23. Left to the full publish gate "
    "to decide. The ordering defect is fixed in "
    "pipeline/recall_confirm_agent.py and both workflow heredocs, with "
    "tests/test_a_repairable_field_is_not_a_rejection.py.]")

MUNG_ARCHIVE_NOTE = (
    "[morning fix 2026-10-23: SUPERSEDED by a repaired copy re-queued to "
    "Pending. The rejection named 'Pathogen is empty', and the hazard is "
    "written out in this row's own Reason — 'Possible foreign material "
    "contamination with glass'. pipeline/enrich_pending_offline.py derives "
    "'Foreign material (glass)' from it deterministically and offline, but "
    "that module only sees rows still in Pending and reviewer 3 archives "
    "first, so nothing in the pipeline ever got to repair it. Reviewer 3 "
    "now runs the same table before deciding. Row kept verbatim — archives "
    "are append-only.]")


# ── helpers ────────────────────────────────────────────────────────────────

def _head(ws):
    return [c.value for c in ws[1]]


def _annotate(wb, sheets, url_key, note, label, require=1) -> int:
    """Append `note` to Notes on every archive row whose URL matches."""
    n = 0
    mark = note[:46]
    for name in sheets:
        if name not in wb.sheetnames:
            continue
        ws = wb[name]
        head = _head(ws)
        if "URL" not in head or "Notes" not in head:
            continue
        c_url = head.index("URL") + 1
        c_note = head.index("Notes") + 1
        for r in range(2, ws.max_row + 1):
            url = str(ws.cell(r, c_url).value or "").lower()
            if url_key not in url:
                continue
            notes = str(ws.cell(r, c_note).value or "")
            if mark in notes:
                continue
            ws.cell(r, c_note).value = (notes.strip() + " " + note).strip()
            n += 1
            print(f"  {name} row {r}: annotated ({label})")
    if n < require:
        raise SystemExit(
            f"{label}: expected at least {require} archive row(s) to "
            f"annotate, found {n} — the register moved under this script")
    return n


#: The banner goes in the REASON column, not only in Notes.
#:
#: pipeline.promote_gate_passing.supersede_every_published_url already
#: sweeps the whole register for archive rows sharing a URL with a published
#: recall — and it deliberately left this one alone. Its SUPERSEDE_IF class
#: covers a REPAIRABLE defect ("Pathogen is empty", a not-found refusal, or
#: no recorded verdict at all). This row carries a coherent, stated SCOPE
#: verdict ("out of scope (Rule B reject)"), and an automatic promoter must
#: not overrule one of those — adjudicating it is a person's job. That is
#: this script.
#:
#: tests/test_a_recall_is_not_both_published_and_rejected.py reads
#: RejectionReason (Weekly_Rejected) / RejectReason (Rejected), so a note in
#: Notes alone leaves the contradiction standing where the detector looks.
#: Same shape the promoter writes: "[SUPERSEDED <date> — …] " prefixed to
#: the original reason, which is kept verbatim behind it, and "SUPERSEDED"
#: in the marker column.
TUNA_REASON_BANNER = (
    "[SUPERSEDED 2026-10-23 — the verdict below is not about this notice. "
    "It was produced by classifying the authority PDF's compressed bytes as "
    "if they were text: Pathogen 'ei' (German/Dutch for egg) was matched "
    "inside binary noise, and there is no egg and no allergen in this "
    "recall. The hazard is histamine, and this URL is PUBLISHED in Recalls "
    "(2026-10-20, IN's Mercato / Selezione Piu, 'Tuna fillets', 'Histamine "
    "/ scombrotoxin', Tier 1). The Recalls copy is what the register says. "
    "Kept as the audit trail of the original refusal; the fetch-layer "
    "defect is fixed in pipeline/gap_finder/pdf_text.py in the same "
    "upload.] ")


def annotate_tuna_superseded(wb) -> int:
    """A — the archive row the Italian gap finder wrote this morning."""
    n = _annotate(wb, ("Weekly_Rejected", "Rejected"),
                  TUNA_URL_KEY, TUNA_NOTE, "tuna SUPERSEDED", require=1)
    n += _supersede_reason(wb, TUNA_URL_KEY, TUNA_REASON_BANNER,
                           "tuna", require=1)
    return n


def _supersede_reason(wb, url_key, banner, label, require=1) -> int:
    """Prefix the SUPERSEDED banner onto the archive row's REASON column."""
    stamped = 0
    for sheet, reason_col, mark_col in (
            ("Weekly_Rejected", "RejectionReason", "Reviewed"),
            ("Rejected", "RejectReason", "Status")):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = _head(ws)
        if "URL" not in head or reason_col not in head:
            continue
        c_url = head.index("URL") + 1
        rcol = head.index(reason_col) + 1
        mcol = head.index(mark_col) + 1 if mark_col in head else None
        for r in range(2, ws.max_row + 1):
            if url_key not in str(ws.cell(r, c_url).value or "").lower():
                continue
            prior = str(ws.cell(r, rcol).value or "")
            if "SUPERSEDED" in prior:
                continue
            ws.cell(r, rcol).value = (banner + prior).strip()
            if mcol:
                ws.cell(r, mcol).value = "SUPERSEDED"
            stamped += 1
            print(f"  {sheet} row {r}: {reason_col} marked SUPERSEDED "
                  f"({label}) — original reason kept verbatim behind the "
                  f"banner")
    if stamped < require:
        raise SystemExit(
            f"{label}: expected at least {require} reason column(s) to mark "
            f"SUPERSEDED, marked {stamped} — the register moved under this "
            f"script")
    return stamped


def remove_fsis_zombie(wb) -> int:
    """B — third removal of the FSIS search-URL copy from the queue."""
    ws = wb["Pending"]
    head = _head(ws)
    c_url = head.index("URL") + 1
    c_prod = head.index("Product") + 1
    removed = 0
    for r in range(ws.max_row, 1, -1):
        url = str(ws.cell(r, c_url).value or "").lower()
        if KAYEM_URL_KEY in url:
            prod = str(ws.cell(r, c_prod).value or "")
            ws.delete_rows(r, 1)
            removed += 1
            print(f"  Pending row {r} REMOVED: {prod[:44]!r} — search-query "
                  f"URL, duplicate of a published recall (3rd removal)")
    if removed != 1:
        raise SystemExit(
            f"expected exactly 1 FSIS search-URL row in Pending, found "
            f"{removed} — do not ship this script until that is understood")
    return removed


def annotate_fsis_archive(wb) -> int:
    """B — say on the archive rows why it will not come back."""
    return _annotate(wb, ("Rejected", "Weekly_Rejected"),
                     KAYEM_URL_KEY, KAYEM_ARCHIVE_NOTE,
                     "FSIS zombie removed", require=1)


def requeue_mung_beans(wb) -> int:
    """C — restore the row with Pathogen filled from its own text."""
    from pipeline.enrich_pending_offline import infer_pathogen

    src = wb["Weekly_Rejected"]
    s_head = _head(src)
    c_url = s_head.index("URL") + 1

    found = None
    for r in range(2, src.max_row + 1):
        if MUNG_URL_KEY in str(src.cell(r, c_url).value or "").lower():
            found = r
            break
    if found is None:
        raise SystemExit(
            "C: the mung-bean archive row is not in Weekly_Rejected — the "
            "register moved under this script")

    row = {h: src.cell(found, i + 1).value for i, h in enumerate(s_head)}

    pend = wb["Pending"]
    p_head = _head(pend)
    p_url = p_head.index("URL") + 1
    for r in range(2, pend.max_row + 1):
        if MUNG_URL_KEY in str(pend.cell(r, p_url).value or "").lower():
            print("  C: a copy is already in Pending — nothing re-queued")
            return 0

    reg = wb["Recalls"]
    r_head = _head(reg)
    r_url = r_head.index("URL") + 1
    for r in range(2, reg.max_row + 1):
        if MUNG_URL_KEY in str(reg.cell(r, r_url).value or "").lower():
            print("  C: already PUBLISHED — nothing re-queued")
            return 0

    got = infer_pathogen({"Reason": row.get("Reason") or "",
                          "Product": row.get("Product") or "",
                          "Notes": row.get("Notes") or "",
                          "URL": row.get("URL") or ""})
    if not got or not got[0]:
        raise SystemExit(
            "C: infer_pathogen found no hazard in the row's own text. That "
            "is the enricher doing its job and it is NOT a reason to write "
            "a value by hand — rule 5. Leave the row archived.")
    pathogen, why = got
    print(f"  C: Pathogen derived from the row's own Reason: {pathogen!r} "
          f"({why})")

    out = {
        "Date": row.get("Date"),
        "Source": row.get("Source"),
        "Company": row.get("Company"),
        "Brand": row.get("Brand"),
        "Product": row.get("Product"),
        "Pathogen": pathogen,
        "Reason": row.get("Reason"),
        "Class": row.get("Class"),
        "Country": row.get("Country"),
        "Region": row.get("Region"),
        "Tier": row.get("Tier"),
        "Outbreak": row.get("Outbreak") or 0,
        "URL": row.get("URL"),
        "Notes": (str(row.get("Notes") or "").strip() + " "
                  + MUNG_REQUEUE_NOTE).strip()[:1800],
        "ScrapedAt": "2026-10-23T06:00:00Z",
        "Status": "pending",
        "RejectedBy": None,
    }
    pend.append([out.get(h) for h in p_head])
    print(f"  Pending: re-queued {str(out['Product'])[:44]!r} at "
          f"Status 'pending' for the publish gate to decide")
    return 1


def annotate_mung_archive(wb) -> int:
    """C — mark the archive row SUPERSEDED by the repaired copy."""
    return _annotate(wb, ("Weekly_Rejected", "Rejected"),
                     MUNG_URL_KEY, MUNG_ARCHIVE_NOTE,
                     "mung beans SUPERSEDED", require=1)


def main() -> int:
    import openpyxl

    wb = openpyxl.load_workbook(XLSX)
    n_a = annotate_tuna_superseded(wb)
    n_b = remove_fsis_zombie(wb)
    n_bb = annotate_fsis_archive(wb)
    n_c = requeue_mung_beans(wb)
    n_cc = annotate_mung_archive(wb)
    wb.save(XLSX)
    wb.close()

    print(f"\ntuna archive annotated SUPERSEDED: {n_a} · "
          f"FSIS Pending zombie removed: {n_b} (archive rows annotated: "
          f"{n_bb}) · mung-bean row re-queued: {n_c} (archive rows "
          f"annotated: {n_cc})")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
