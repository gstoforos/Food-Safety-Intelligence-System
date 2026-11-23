# -*- coding: utf-8 -*-
"""Morning fix 2026-11-23 — one legislation page out of Recalls, one
Norwegian pesticide recall un-blocked.

REMOVED FROM Recalls (archived to Rejected; nothing is deleted from an archive)
------------------------------------------------------------------------------
1. Ministero della Salute (IT), https://www.salute.gov.it/new/it/sezione/
   norme-e-atti/ — queued by the DDGS gap finder at 00:14 UTC today and
   published into Recalls by the offline promoter at 04:27 UTC as:

       Date      2026-11-13
       Company   'Ministero della Salute'
       Brand     'Ministero della Salute'
       Product   'Norme e atti'
       Pathogen  'Listeria monocytogenes'
       Tier      1
       report_week W46

   "Norme e atti" is Italian for "Rules and acts" and the URL is the
   ministry's LEGISLATION section index. The row's own Notes say what the
   page actually offered: "Regolamento (UE) 2024/2895, recante modifica del
   regolamento (CE) n. 2073/2005 per quanto riguarda Listeria monocytogenes
   negli alimenti pronti al consumo - Indicazioni per l'applicazione
   nell'ambito dei controlli ufficiali (0.4 Mb)" — guidance on applying a
   food-safety regulation, a law, not a recall of anything. It names no
   firm (Company and Brand are the ministry itself), no product, no lot and
   no recalling action. R2 and R3 both fail.

   Fixed in code in the same change: a regulator's own section index in
   Italian, Portuguese, German, Polish, Croatian or English now matches the
   generic-URL pattern in all four lists that own one
   (pipeline/_url_filters.py, pipeline/merge_master.py,
   pipeline/_gap_finder_guards.py, review/url_validator.py). Only the
   Spanish /subseccion/<x>.htm and the produktwarnung.eu /rubrik/<x>/ forms
   were known before. Held by
   tests/test_section_index_url_is_not_a_recall.py.

ARCHIVE CONTRADICTION ANNOTATED (append-only; nothing is deleted)
-----------------------------------------------------------------
2. Weekly_Rejected carries, written by gap_finder/it/rules.py at 06:45 UTC
   today, a row whose URL is the Ministero della Salute LISTING index
   (/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari?tab=1&...) and
   whose Product is the page's own heading, "Avvisi e richiami di prodotti
   alimentari". That URL normalises to the same dedup key as PUBLISHED
   Recalls row 2163 (2026-04-14, La Vacje Salumerie Snc, Salame Nostrano,
   Listeria monocytogenes), so one recall reads as both published and
   thrown away, and
   tests/test_a_recall_is_not_both_published_and_rejected is RED ON MAIN.

   They are not the same recall. They collide because the published row
   cites an index page as its authority: a "URL FIX 2026-05-01" swapped a
   news-mirror URL (ilfattoalimentare.it) for the portal index when the
   specific avviso PDF could not be reached.

   The morning fix of 2026-11-18 annotated the Rejected copy with exactly
   this reading; the Italian gap finder then wrote a fresh, unannotated
   copy into Weekly_Rejected, so annotating the instance was never going to
   hold. Both are done today: the archive row gets the append-only
   UNRESOLVED CONTRADICTION note, and the listing index joins
   KNOWN_REGULATOR_LANDINGS in pipeline/_url_filters.py so the gap finder
   stops proposing it at all.

PENDING — provenance recorded, nothing claimed
----------------------------------------------
2. Mattilsynet (NO), Hewad Mohmand, "Red raisins", Pathogen "Chlorpyrifos
   exceeding limit", https://www.mattilsynet.no/tilbakekallinger/hewad-
   mohmand-kaller-tilbake-rosiner-etter-funn-av-plantevernmidler-i-
   produktet. The row has sat in Pending blocked by the publish gate's
   unreachable-hazard rule: no word in tools/alert_vocab.py reached a bare
   pesticide active substance. Chlorpyrifos is a pesticide residue and R6
   names pesticide residues explicitly, so the vocabulary was the defect,
   not the row. Named actives already present in the corpus were added to
   the vocabulary today, which lets the offline promoter publish this row.

   THIS PASS DID NOT READ THE MATTILSYNET PAGE. mattilsynet.no refuses this
   sandbox (client_error, nothing retrieved); the pipeline's own scrapers
   reach it from GitHub Actions and this row is their output. Every field
   below is the collector's, not this pass's, and the note appended says so.
   Independent corroboration that the recall is real, from a search only
   (news is discovery, never the authority): Norwegian national press on
   2026-11-18, "Afghanske rosiner trekkes: Inneholder plantevernmiddel"
   (Afghan raisins withdrawn: contains a pesticide). No field is changed.
"""
from __future__ import annotations

import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON_PATH = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-11-23"
STAMP = f"morning fix {TODAY}"

NORME_E_ATTI = "salute.gov.it/new/it/sezione/norme-e-atti"
RAISINS = "mattilsynet.no/tilbakekallinger/hewad-mohmand-kaller-tilbake-rosiner"

REMOVE_FROM_RECALLS = [
    (NORME_E_ATTI,
     "removed from Recalls — not a recall. The URL is the Italian Ministry "
     "of Health's LEGISLATION section index (\"Norme e atti\" = \"Rules and "
     "acts\"), and the document this row's own Notes quote is \"Regolamento "
     "(UE) 2024/2895, recante modifica del regolamento (CE) n. 2073/2005 "
     "per quanto riguarda Listeria monocytogenes negli alimenti pronti al "
     "consumo - Indicazioni per l'applicazione nell'ambito dei controlli "
     "ufficiali\" — guidance on applying a food-safety regulation. No firm "
     "(Company and Brand both held 'Ministero della Salute'), no product "
     "('Norme e atti'), no lot, no recalling action: R2 (a real regulatory "
     "action) and R3 (the regulator's own per-recall URL) both fail. The "
     "DDGS gap finder queued it at 00:14 UTC on 2026-11-23 and the offline "
     "promoter published it at 04:27 UTC, Tier 1, into week 46. All four "
     "generic-URL lists gained the section-index shape today so it cannot "
     "return. TERMINAL."),
]

AVVISI_INDEX = "salute.gov.it/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari"

AVVISI_NOTE = (
    f"UNRESOLVED CONTRADICTION ({STAMP}): not the same recall. This row's "
    "URL is the Ministero della Salute LISTING INDEX "
    "(/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari), which "
    "normalises to the same dedup key as the PUBLISHED Recalls row of "
    "2026-04-14 (La Vacje Salumerie Snc, Salame Nostrano ~800 g lot "
    "L6CCTD, Listeria monocytogenes, Tier 1) — a row that cites the index "
    "page as its authority URL because a \"URL FIX 2026-05-01\" swapped a "
    "news-mirror URL (ilfattoalimentare.it) for it when the ministry's own "
    "avviso PDF could not be reached. THIS row is a news-only discovery "
    "(ilmessaggero.it) with no authority URL and its refusal stands. The "
    "published row's URL is the defect and is the operator's call: swap it "
    "for the ministry's own avviso, or drop the row. The listing index "
    "joined KNOWN_REGULATOR_LANDINGS today so this shape is not proposed "
    "again. Nothing is deleted; this is an append-only annotation.] ")

RAISIN_NOTE = (
    f" [{STAMP}: this row is now publishable because tools/alert_vocab.py "
    "gained the named pesticide actives already present in the register "
    "(chlorpyrifos/klorpirifos, acetamiprid, procymidone, tolfenpyrad). "
    "Until today every published pesticide hazard reached a subscriber "
    "alert only through the words \"pesticide residue\", a parenthetical "
    "the enrichment adds and which Mattilsynet's own wording "
    "(\"Chlorpyrifos exceeding limit\") has no reason to carry — so the "
    "publish gate's unreachable-hazard rule held an in-scope hazard (R6 "
    "names pesticide residues) that was never wrong. PROVENANCE: this pass "
    "did NOT read the Mattilsynet notice — mattilsynet.no refuses this "
    "sandbox (client_error, nothing retrieved) while the pipeline's own "
    "scraper reaches it from GitHub Actions. Every field on this row is the "
    "collector's; nothing here was added, corrected or inferred by the "
    "morning pass. Corroborated only by Norwegian national press of "
    "2026-11-18, \"Afghanske rosiner trekkes: Inneholder plantevernmiddel\" "
    "— discovery, never the authority.]")


def _key(u) -> str:
    s = str(u or "").strip().lower().split("#")[0]
    for p in ("https://", "http://"):
        if s.startswith(p):
            s = s[len(p):]
    if s.startswith("www."):
        s = s[4:]
    return s.rstrip("/")


def _head(ws):
    return [str(c.value) for c in ws[1]]


def _remove_from_recalls(wb) -> int:
    rec = wb["Recalls"]
    head = _head(rec)
    iu = head.index("URL") + 1
    arch = wb["Rejected"]
    ah = _head(arch)
    gone = 0
    for frag, why in REMOVE_FROM_RECALLS:
        rows = [r for r in range(2, rec.max_row + 1)
                if frag in _key(rec.cell(r, iu).value)]
        for r in rows:
            vals = {h: rec.cell(r, i + 1).value for i, h in enumerate(head)}
            out = []
            for h in ah:
                if h == "RejectReason":
                    out.append(f"{STAMP}: {why}")
                elif h == "RejectedBy":
                    out.append(STAMP)
                elif h in ("RejectedAt", "ScrapedAt"):
                    out.append(TODAY)
                elif h == "Status":
                    out.append("rejected")
                else:
                    out.append(vals.get(h, ""))
            arch.append(out)
        for r in reversed(rows):
            rec.delete_rows(r, 1)
            gone += 1
        print(f"  Recalls: removed {len(rows)} row(s) — {frag[:70]}")
        if "Weekly_Review" in wb.sheetnames:
            wr = wb["Weekly_Review"]
            wh = _head(wr)
            wiu = wh.index("URL") + 1
            wrows = [r for r in range(2, wr.max_row + 1)
                     if frag in _key(wr.cell(r, wiu).value)]
            for r in reversed(wrows):
                wr.delete_rows(r, 1)
            if wrows:
                print(f"  Weekly_Review: removed {len(wrows)} copy/copies")
    return gone


def _annotate_avvisi_contradiction(wb) -> int:
    """Append-only note on every archive copy that still says nothing."""
    n = 0
    for sheet, col in (("Weekly_Rejected", "RejectionReason"),
                       ("Rejected", "RejectReason")):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = _head(ws)
        if "URL" not in head or col not in head:
            continue
        iu = head.index("URL") + 1
        ic = head.index(col) + 1
        for r in range(2, ws.max_row + 1):
            if AVVISI_INDEX not in _key(ws.cell(r, iu).value):
                continue
            cur = str(ws.cell(r, ic).value or "")
            if "UNRESOLVED CONTRADICTION" in cur:
                continue
            ws.cell(r, ic).value = (AVVISI_NOTE + cur)[:2000]
            if "Reviewed" in head:
                ws.cell(r, head.index("Reviewed") + 1).value = "Y"
            n += 1
            print(f"  {sheet}: annotated contradiction on row {r}")
    return n


def _note_the_raisins(wb) -> int:
    if "Pending" not in wb.sheetnames:
        return 0
    ws = wb["Pending"]
    head = _head(ws)
    if "URL" not in head or "Notes" not in head:
        return 0
    iu = head.index("URL") + 1
    ic = head.index("Notes") + 1
    n = 0
    for r in range(2, ws.max_row + 1):
        if RAISINS not in _key(ws.cell(r, iu).value):
            continue
        cur = str(ws.cell(r, ic).value or "")
        if STAMP in cur:
            continue
        ws.cell(r, ic).value = (cur.strip() + RAISIN_NOTE)[:2000]
        n += 1
        print(f"  Pending: provenance note added to row {r}")
    return n


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")
    wb = openpyxl.load_workbook(XLSX)
    gone = _remove_from_recalls(wb)
    annotated = _annotate_avvisi_contradiction(wb)
    noted = _note_the_raisins(wb)
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"  recalls.json re-mirrored: "
          f"{mirror_json_from_xlsx(XLSX, JSON_PATH)} rows")
    print(f"REMOVED={gone} ANNOTATED={annotated} NOTED={noted}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
