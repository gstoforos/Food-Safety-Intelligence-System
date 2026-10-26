# -*- coding: utf-8 -*-
"""Morning fix pass 2026-10-26. Base 3d82fcfb.

WHAT WAS WRONG
==============

1. FOUR INVENTED OR TEMPLATE RECALL IDENTIFIERS, THREE OF THEM PUBLISHED.
   The extractor's rule 10 tells the model to append
   "(Recall ID <the number printed in the article>)" to Reason. When the
   article prints no number the model fills the slot anyway, and
   merge_master.strip_empty_identifier_template is the writer-side guard
   that removes the placeholder. Two spellings got past it:

       (Recall ID AGES-12345)      AGES (AT) 2026-10-16, Lidl Österreich /
                                   Milbona High Protein Pudding Chocolate,
                                   Bacillus cereus — PUBLISHED
       (Recall ID MAST-123456)     MAST (IS) 2026-10-13, Lifa ehf., Timur
                                   berry, Salmonella — PUBLISHED
       (No Recall ID provided)     NFSA (EG) 2026-10-11, Yasensvit Ltd,
                                   shell eggs, Salmonella Enteritidis —
                                   PUBLISHED
       (Recall ID)                 SZPI (CZ), Enterobacteriaceae over the
                                   limit — Weekly_Rejected

   "AGES-12345" and "MAST-123456" are dummy digit runs the guard already
   knows how to recognise; it never saw them because the model prefixed
   them with the agency's own short name, so the digits stopped being the
   whole captured group. "(No Recall ID provided)" is the same sentence the
   guard already strips as "(Recall ID not provided)" with the negation
   moved to the front of the phrase.

   A fabricated reference is worse than an admitted blank, because it looks
   checkable. Three subscribers' alert emails carried one.

   Root cause fixed in pipeline/merge_master._EMPTY_ID_WORDS (the leading
   "no/without" and the bare participle) and ._EMPTY_ID_DIGITS (an optional
   <AGENCY>- prefix in front of the placeholder digit run), with the new
   shapes and three new KEEPERS pinned in
   tests/test_empty_identifier_template_never_reaches_data.py. This script
   repairs the data through that same guard, so the register and the writer
   cannot disagree about what a placeholder is.

   DELIBERATELY NOT STRIPPED, and now pinned as KEEPERS:
     * Ministero della Salute (IT) 2026-10-13, Eurospin vitello tonnato,
       "(Recall ID L260/26)". L260/26 is the LOT, spelled in this row's own
       URL ("Modulo Richiamo Vitello tonnato gr. 180 Portamivia L260_26"),
       and already in Product. Mislabelled, but a real reference read off a
       real page: an operator's call, not a regex's.
     * GIS (PL) 2026-08-19, "(Recall ID GIS-2026-08-19-001)". The whole of
       tests/test_a_dated_recall_id_agrees_with_date.py reads that id as the
       regulator's own, and this row's Date was corrected from 2026-10-12 to
       2026-08-19 on its evidence.

2. A PENDING ROW ABOUT TO PUBLISH A PRODUCT ITS OWN NOTICE DOES NOT NAME.
   Ministero della Salute (IT), Date 2026-10-19, Carrefour, Product "Cod
   fish fillet", Pathogen "Non conformity microbiological", Status
   'pending', scraped 2026-10-26T06:45Z by the Italian gap finder — and it
   PASSES the full publish gate, so the next hourly merge would have
   published it.

   Its URL is the ministry's own PDF
       .../Cartello%20di%20Richiamo%20Tahina%20Arkan_1792170251.pdf
   which is the recall of ARKAN TAHINA, 400 g jars, Salmonella — a
   different notice about a different food. The row's own Notes place it in
   "Fiumicino", which is where the Carrefour recall's producer (Promarine
   Srl) is, and the Carrefour recall is of SWORDFISH, not cod. The gap
   finder was reading a news article that covered two ministry notices and
   paired the fields of one with the URL of the other.

   Not repaired, because repairing it would mean inventing: salute.gov.it
   answers this environment with a JavaScript bot-check shell, so the
   authority page could not be read, and the swordfish product name is
   known only from news. Archived to Rejected with the mismatch recorded.
   The row is NOT deleted — archives are append-only.

3. NOTHING, in the end — and the first count was mine, wrong.
   94 rows across Rejected and Weekly_Rejected sit on a URL that is also
   PUBLISHED in Recalls. A first pass this morning put seven of them down
   as carrying no verdict. That was a misread: the audit looked for the
   verdict in Notes and in a `RejectionReason` column, and the `Rejected`
   sheet spells it `RejectReason` — so seven rows whose verdict was
   recorded all along were counted as silent. Re-measured across every
   verdict-bearing column on both sheets: 94 overlaps, 94 annotated, 0
   silent.
   The annotation step below is kept because it is idempotent and this
   script is re-run by tools/rebase_and_verify.py against a main that
   moves all day; a new overlap arriving in between still gets a verdict.
   It is expected to report 0.

4. ONE MISSED RECALL, verified on the authority page and added through
   tools/add_manual_row.py (see MANUAL_ROWS below): RappelConso fiche
   2026-10-0248 / 23921 of 23/10/2026, GOLDEN TURTLE wasabi crackers 125 g,
   high content of glycidyl fatty acid esters. In scope as a chemical
   contaminant; four of its five sibling fiches of the same day (23918,
   23922, 23923, 23925) are published and this one is in no sheet. The
   hazard was also reachable by no subscriber alert term — fixed by adding
   "glycidyl" to tools/alert_vocab.py's existing "Industrial chemical
   contaminant" entry, so no new subscriber-facing term appears.

WHAT THIS SCRIPT DOES NOT DO
============================
No row leaves Recalls, so the commit message carries no `remove-rows`.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import openpyxl                                             # noqa: E402

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-26"

#: Sheets the reports, emails and dashboard read. `Rejected` is a terminal
#: archive; the same exclusion
#: tests/test_empty_identifier_template_never_reaches_data.LIVE_SHEETS makes.
LIVE_SHEETS = ("Recalls", "Pending", "Weekly_Review", "Weekly_Rejected")

#: The defective Italian row, identified by the three facts that make it
#: wrong together. Matching on all three means a corrected row — same URL,
#: right product — would not be touched.
BAD_IT_URL = "Cartello%20di%20Richiamo%20Tahina%20Arkan_1792170251.pdf"
BAD_IT_PRODUCT = "cod fish fillet"

BAD_IT_REASON = (
    "URL/product mismatch — the row names a product its own notice does not. "
    "Product 'Cod fish fillet', Company/Brand 'Carrefour', Pathogen 'Non "
    "conformity microbiological'; the cited salute.gov.it PDF is "
    "'Cartello di Richiamo Tahina Arkan', the recall of ARKAN tahina in 400 g "
    "jars for suspected Salmonella — a different notice about a different "
    "food. The Carrefour notice this row's locality stamp points at "
    "('Fiumicino', the plant of Promarine Srl) is a recall of SWORDFISH. One "
    "news article covered both ministry notices and the Italian gap finder "
    "paired one notice's fields with the other's URL. "
    "NOT REPAIRED, because repairing it offline would mean inventing: "
    "salute.gov.it answers this environment with a JavaScript bot-check "
    "shell, so the authority page could not be read, and the swordfish "
    "product name is known only from news, which is discovery and not a "
    "source. The row passed the full publish gate and the next hourly merge "
    "would have published it. "
    "Archived by the AFTS morning fix pass " + TODAY + "."
)

DUP_NOTE = (
    " [duplicate 2026-10-26: this archive row shares its URL with a row that "
    "is PUBLISHED in Recalls. It is the raw, unenriched sibling the "
    "reviewers rejected in favour of the published one — one notice, one "
    "published row, and this record of the discard. Kept, not deleted: "
    "archives are append-only. Annotated by the morning fix pass so the "
    "published/rejected overlap audit reports a verdict rather than a "
    "silence.]"
)

#: The missed recall, every field read on the fiche itself.
MANUAL_ROWS = [
    {
        "Date": "2026-10-23",
        "Source": "RappelConso (FR)",
        "Company": "T & T FOODS",
        "Brand": "GOLDEN TURTLE",
        "Product": ("Wasabi crackers, 125 g bag (CRACKERS AU WASABI 125G "
                    "GOLDEN TURTLE), GTIN 8717703617092, minimum durability "
                    "dates 24/02/2027, 19/03/2027 and 18/05/2027"),
        "Pathogen": "Glycidyl fatty acid esters (chemical contaminant)",
        "Reason": ("High content of glycidyl fatty acid esters; risk "
                   "category stated by the fiche: other chemical "
                   "contaminants"),
        "Class": "Voluntary",
        "Country": "France",
        "Outbreak": 0,
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23921/Interne",
        "Notes": (
            "Added by the AFTS morning fix pass 2026-10-26 (missed-recall "
            "sweep); absent from all five sheets, while four of the five "
            "RappelConso fiches published the same day (23918, 23922, 23923, "
            "23925) are in Recalls. Every field read on the fiche itself, "
            "https://rappel.conso.gouv.fr/fiche-rappel/23921/Interne, opened "
            "2026-10-26: fiche number '2026-10-0248' version 1, date de "
            "publication 23/10/2026, categorie Alimentation / sous-categorie "
            "'Plats prepares et snacks'; Company from 'Origine de la fiche: "
            "T & T FOODS' (SIREN 44037097100028) — the fiche names no "
            "separate manufacturer; Brand from 'Nom de la marque du produit: "
            "GOLDEN TURTLE'; Product, GTIN and the three minimum durability "
            "dates from the fiche's own product table; motif and risk from "
            "'Motif du rappel' and 'Risques encourus par le consommateur'. "
            "Zone geographique de vente: France entiere; commercialised "
            "19/05/2026 to 23/10/2026; nature juridique du rappel: "
            "volontaire. No lot number is printed on the fiche, so none is "
            "recorded here. "
            "[original product: CRACKERS AU WASABI 125G GOLDEN TURTLE] "
            "[original Reason (fr): \"Teneur elevee en esters "
            "glycidyliques d'acides gras\"] "
            "[original risk (fr): \"Autres contaminants chimiques\"] "
            "[scope: glycidyl fatty acid esters are a refining-process "
            "contaminant with their own limit in Reg. (EU) 2023/915, so this "
            "is a chemical contaminant over a limit and in AFTS scope. The "
            "hazard was reachable by no subscriber alert term until "
            "'glycidyl' was added to tools/alert_vocab.py's existing "
            "'Industrial chemical contaminant' entry the same morning.]"
        ),
    },
]


def _head(ws):
    return [c.value for c in ws[1]]


def _rowdict(ws, head, r):
    return {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}


def _set(ws, head, r, field, value):
    ws.cell(r, head.index(field) + 1).value = value


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


def _strip_identifier_templates(wb) -> int:
    """Run the writer's own guard over every live sheet. Returns changes."""
    from pipeline.merge_master import strip_empty_identifier_template

    fixed = 0
    for sheet in LIVE_SHEETS:
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = _head(ws)
        if "Reason" not in head:
            continue
        for r in range(2, ws.max_row + 1):
            row = _rowdict(ws, head, r)
            before = str(row.get("Reason") or "")
            if not before:
                continue
            probe = {"Reason": before, "Notes": str(row.get("Notes") or "")}
            if not strip_empty_identifier_template(
                    [probe], where=f"repair {TODAY}/{sheet}"):
                continue
            _set(ws, head, r, "Reason", probe["Reason"])
            if "Notes" in head:
                _set(ws, head, r, "Notes", probe["Notes"])
            fixed += 1
            print(f"  {sheet}: identifier template stripped from Reason — "
                  f"{row.get('Source')!r} {str(row.get('Date'))[:10]}: "
                  f"{before[-46:]!r} -> {probe['Reason'][-46:]!r}")
    return fixed


def _archive_bad_italian_row(wb) -> int:
    ws = wb["Pending"]
    head = _head(ws)
    arch = wb["Rejected"]
    done = 0
    for r in range(ws.max_row, 1, -1):
        row = _rowdict(ws, head, r)
        url = str(row.get("URL") or "")
        product = str(row.get("Product") or "").strip().lower()
        if BAD_IT_URL not in url or product != BAD_IT_PRODUCT:
            continue
        _archive(arch, row, BAD_IT_REASON,
                 "morning fix pass " + TODAY)
        ws.delete_rows(r, 1)
        done += 1
        print("  Pending -> Rejected: Ministero della Salute (IT) "
              "'Cod fish fillet' on the Tahina Arkan notice")
    return done


def _annotate_overlaps(wb) -> int:
    """Rejected rows sharing a URL with a published row, with no verdict."""
    import re

    rec = wb["Recalls"]
    rhead = _head(rec)
    ui = rhead.index("URL")
    published = {
        str(rec.cell(r, ui + 1).value or "").strip()
        for r in range(2, rec.max_row + 1)
    }
    published.discard("")

    verdict = re.compile(r"SUPERSEDED|duplicate|UNRESOLVED|also published|"
                         r"published row", re.I)
    annotated = 0
    for sheet in ("Rejected", "Weekly_Rejected"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = _head(ws)
        if "URL" not in head or "Notes" not in head:
            continue
        why = ("RejectReason" if "RejectReason" in head
               else "RejectionReason" if "RejectionReason" in head else None)
        for r in range(2, ws.max_row + 1):
            row = _rowdict(ws, head, r)
            if str(row.get("URL") or "").strip() not in published:
                continue
            blob = " ".join(str(row.get(f) or "") for f in
                            ("Notes", "RejectedBy", "Status",
                             why or "Notes"))
            if verdict.search(blob):
                continue
            _set(ws, head, r, "Notes",
                 (str(row.get("Notes") or "").strip() + DUP_NOTE).strip())
            annotated += 1
            print(f"  {sheet}: duplicate-of-published annotated — "
                  f"{row.get('Source')!r} "
                  f"{str(row.get('Product'))[:42]!r}")
    return annotated


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")

    wb = openpyxl.load_workbook(XLSX)

    fixed = _strip_identifier_templates(wb)
    if fixed < 1:
        print("  NOTE: no identifier template found to strip — already "
              "repaired upstream, which is fine")

    archived = _archive_bad_italian_row(wb)
    if archived != 1:
        raise SystemExit(
            f"expected exactly 1 Pending row matching the Tahina-Arkan "
            f"URL/product mismatch, found {archived}. If the Italian gap "
            f"finder has since corrected the row, delete section 2 of this "
            f"script rather than loosening the match.")

    annotated = _annotate_overlaps(wb)

    wb.save(XLSX)
    wb.close()

    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")

    # ── the missed recall, queued through the real code path ─────────────
    import json
    import subprocess
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(MANUAL_ROWS, fh, ensure_ascii=False, indent=2)
        rows_path = fh.name
    added = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), rows_path],
        cwd=str(ROOT), text=True, capture_output=True)
    print(added.stdout.strip())
    if added.stderr.strip():
        print(added.stderr.strip(), file=sys.stderr)
    if added.returncode != 0:
        raise SystemExit(f"add_manual_row failed ({added.returncode})")

    print(f"ROWS_REMOVED=0")
    print(f"ROWS_REPAIRED={fixed}")
    print(f"ROWS_ARCHIVED={archived}")
    print(f"ROWS_ANNOTATED={annotated}")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
