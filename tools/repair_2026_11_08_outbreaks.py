# -*- coding: utf-8 -*-
"""Weekly outbreak verification 2026-11-08 — three published duplicates.

Scope: Recalls rows dated 2026-09-24 .. 2026-11-08 plus all of October 2026.
Five rows were flagged Outbreak=1. Four are confirmed on authority pages
(FDA oysters, FSA/UKHSA Yasensvit eggs, Valais botulism, FDA Sierra Nevada
recall). The fifth is the same E. coli O26:H11 outbreak published a second
time by the same regulator, and the duplicate sweep found two more FSA (UK)
alerts published twice. All three are removed from Recalls to Rejected.
Nothing is deleted; archives are append-only.

1. FDA, 2026-10-09, Sierra Nevada Graziers, E. coli O26:H11, Outbreak=1, URL
   fda.gov/food/outbreaks-foodborne-illness/outbreak-investigation-e-coli-
   o26h11-raw-milk-cheese-september-2026. FDA's outbreak-investigation page
   (read 2026-11-08: body "Current Update" September 25, 2026; metadata
   10/09/2026; "Total Illnesses: 13") links to, and describes, the recall
   already published from Sierra Nevada Cheese Company's own FDA notice
   (2026-09-29, Outbreak=1, "13 illnesses identified to date"). One outbreak,
   one regulator, two URLs. The 2026-10-04 operator review had already
   removed this URL as duplicate_of_published_recall; it came back on
   2026-10-28 through merge_master's reachability exception, which read a
   stale "Unable to find the official regulator URL" in the archive row's
   Notes and ignored the duplicate verdict beside it (fixed in the same
   pass: pipeline/merge_master.py, duplicate-verdict veto). October's
   outbreak count drops 4 -> 3; it was rank #2 of October's top-10.

2. FSA (UK), 2026-10-08, Greencore, Salmonella, URL alerts.food.gov.uk/
   article/6920/... — the article-URL copy of FSA-PRIN-48-2026, which is
   published at its canonical URL alerts.food.gov.uk/news-alerts/alert/
   fsa-prin-48-2026 (same date, firm, pathogen, five products). The
   canonical row's own Notes name article/6920 as the same alert. Same
   pattern as the SuperValu/Centra article/6946 copy removed 2026-11-01.

3. FSA (UK), 2026-09-25, World of Sweets, Salmonella, URL .../fsa-prin-47-
   2026-update-1 — the update page of FSA-PRIN-47-2026, published at
   .../fsa-prin-47-2026 (2026-09-24). The audit of 2026-10-01 already
   recorded "an FSA alert and its update pages are one incident"; the
   canonical row's Reason already carries the update.

The archive rows that earlier passes marked "[SUPERSEDED … this recall is
PUBLISHED in Recalls …]" for these URLs are annotated, not rewritten.

Idempotent: a second run finds nothing in Recalls and annotates nothing.
"""
from __future__ import annotations

import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-11-08"
BY = "outbreak check " + TODAY
TAG = "[outbreak check " + TODAY

SIERRA_RECALL = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-"
                 "alerts/sierra-nevada-cheese-company-recalls-graziers-raw-milk-"
                 "cheese-because-possible-health-risk")

REMOVE = [
    {
        "url": ("https://www.fda.gov/food/outbreaks-foodborne-illness/"
                "outbreak-investigation-e-coli-o26h11-raw-milk-cheese-"
                "september-2026"),
        "why": ("duplicate_of_published_recall — the same FDA E. coli O26:H11 "
                "raw milk cheese outbreak (13 illnesses) is published from "
                "Sierra Nevada Cheese Company's own FDA recall notice at "
                + SIERRA_RECALL + " (2026-09-29, Outbreak=1). This is FDA's "
                "outbreak-investigation page for that same event, read "
                "2026-11-08: it links to that recall and states 'Total "
                "Illnesses: 13'. One outbreak, one regulator, two URLs: it "
                "counted October's outbreaks once too often (rank #2 of the "
                "October top-10). Already removed as a duplicate on "
                "2026-10-04; re-promoted 2026-10-28 by the reachability "
                "exception on stale Notes text. Never re-promote."),
    },
    {
        "url": ("https://alerts.food.gov.uk/article/6920/Greencore-recalls-"
                "several-products-due-to-the-presence-of-Salmonella"),
        "why": ("duplicate_of_published_recall — the article-URL copy of FSA "
                "alert FSA-PRIN-48-2026 (Greencore, Salmonella in pistachio "
                "ingredient, 2026-10-08), which is published at its canonical "
                "URL https://alerts.food.gov.uk/news-alerts/alert/"
                "fsa-prin-48-2026 with the same date, firm, pathogen and five "
                "products; that row's own Notes name article/6920 as the same "
                "alert. Same regulator, same alert, two URLs. Never "
                "re-promote."),
    },
    {
        "url": "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-47-2026-update-1",
        "why": ("duplicate_of_published_recall — Update 1 of FSA alert "
                "FSA-PRIN-47-2026 (World of Sweets, Alyan Dubai Style "
                "chocolate, Salmonella), which is published at "
                "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-47-2026 "
                "(2026-09-24) with the update already in its Reason. An FSA "
                "alert and its update pages are one incident (audit "
                "2026-10-01). Never re-promote."),
    },
]


def _head(ws):
    return [c.value for c in ws[1]]


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _archive(arch, row, reason):
    out = []
    for h in _head(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(BY)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        else:
            out.append(row.get(h, ""))
    arch.append(out)


def _annotate_stale_published_markers(wb, targets) -> int:
    """Append a note to archive rows whose marker says the URL is published."""
    n = 0
    for name in ("Rejected", "Weekly_Rejected"):
        if name not in wb.sheetnames:
            continue
        ws = wb[name]
        head = _head(ws)
        col = next((head.index(h) + 1 for h in ("RejectReason", "RejectionReason")
                    if h in head), None)
        if col is None or "URL" not in head:
            continue
        iu = head.index("URL") + 1
        for r in range(2, ws.max_row + 1):
            if _key(ws.cell(r, iu).value) not in targets:
                continue
            cur = str(ws.cell(r, col).value or "")
            if "PUBLISHED" not in cur or TAG in cur:
                continue
            ws.cell(r, col).value = (cur.rstrip() + " " + TAG + ": the Recalls "
                                     "copy this marker refers to was removed as "
                                     "duplicate_of_published_recall; this URL "
                                     "is NOT published and must not be "
                                     "re-promoted.]")
            n += 1
    return n


def main() -> int:
    wb = openpyxl.load_workbook(XLSX)
    ws, arch = wb["Recalls"], wb["Rejected"]
    head = _head(ws)
    removed = 0
    for spec in REMOVE:
        target = _key(spec["url"])
        hit = False
        for r in range(ws.max_row, 1, -1):
            row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
            if _key(row.get("URL")) != target:
                continue
            _archive(arch, row, "Removed from Recalls by the " + BY + " — "
                     + spec["why"])
            ws.delete_rows(r, 1)
            removed += 1
            hit = True
            print(f"  Recalls: REMOVED {row.get('Date')} {row.get('Source')!r} "
                  f"{row.get('Company')!r} Outbreak={row.get('Outbreak')}")
        if not hit:
            print(f"  Recalls: no row at {spec['url'][:70]} (already gone)")
    ann = _annotate_stale_published_markers(wb, {_key(s["url"]) for s in REMOVE})
    print(f"  archive markers annotated: {ann}")
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"ROWS_REMOVED={removed}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
