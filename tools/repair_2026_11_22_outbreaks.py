# -*- coding: utf-8 -*-
"""Weekly outbreak verification 2026-11-22 — two FSA alerts published twice.

Scope: Recalls rows dated 2026-10-01 .. 2026-11-22 (last 45 days, which
starts 2026-10-08, plus all of October 2026, the most recently closed month).

Outbreak flags in scope: 4, all kept (Little Wicomico oysters FDA, AGES
Sicilian tomatoes, FSA FAFA-03 Yasensvit eggs, BLV Le Grand'Joie terrine).
No missed outbreak found; nothing is flagged or de-flagged by this script.

DUPLICATES removed (Step 4: same Source + same firm + same pathogen, same
date, two URLs on a host that serves one FSA alert at two URL forms — the
rule recall_confirm_agent.duplicate_problems already applies):

1. FSA (UK), 2026-10-08, Greencore, Salmonella, at
   alerts.food.gov.uk/article/6920/Greencore-recalls-several-products-due-to-
   the-presence-of-Salmonella (Product "several products", Reason the bare
   word "Salmonella", a Tavily gap-finder extract). The canonical row
   alerts.food.gov.uk/news-alerts/alert/fsa-prin-48-2026 is published since
   2026-10-10, and its own Notes name article/6920 as that alert's title
   page. The morning fix of 2026-11-04 already archived this URL as a
   DUPLICATE; it came back on 2026-11-20 because that archive verdict also
   carried the word SUPERSEDED, which _is_terminal_rejection read as "not a
   verdict" (fixed in pipeline/merge_master.py in the same commit). It was
   NOT in the October report (built 2026-11-19, 390 rows); removing it
   brings the register's October count from 391 back to the published 390.

2. FSA (UK), 2026-11-21, Hotel Chocolat, Salmonella, at
   alerts.food.gov.uk/article/7000/Hotel-Chocolat-recalls-several-chocolate-
   products-because-of-the-possible-presence-of-Salmonella (Product "several
   chocolate products", Reason "Salmonella", Exa daily-search extract,
   promoted 2026-11-22 by promote_gate_passing, which never ran the two-URL
   rule — fixed in the same commit). Canonical: news-alerts/alert/
   fsa-prin-54-2026, same firm, same date, same hazard, published 2026-11-21.

Both FSA pages refused automated reads on 2026-11-22 (client_error); the
verdict rests on the register's own rows and the FSA's documented two-URL
publication, not on a page read today. Neither row carried Outbreak=1.

The same two URLs are dropped from Weekly_Review (the Sunday review list, not
an archive) so the operator email does not count them. Archives are
append-only: one Rejected row per removal, nothing deleted there. Idempotent.
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
TODAY = "2026-11-22"
BY = "outbreak check " + TODAY

REMOVE = [
    {
        "url": ("https://alerts.food.gov.uk/article/6920/Greencore-recalls-"
                "several-products-due-to-the-presence-of-Salmonella"),
        "canon": "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-48-2026",
        "why": ("duplicate_of_published_recall — the same FSA alert "
                "(FSA-PRIN-48-2026, Greencore, Salmonella, 2026-10-08) is "
                "published at its canonical URL https://alerts.food.gov.uk/"
                "news-alerts/alert/fsa-prin-48-2026, whose Notes name "
                "article/6920 as that alert's title page. Same regulator, same "
                "firm, same date, same hazard, two URLs; this copy came from a "
                "gap-finder extract (Product 'several products'). Already ruled "
                "a duplicate by the morning fix 2026-11-04 and re-published "
                "2026-11-20 through the re-promotion exception. Never "
                "re-promote."),
    },
    {
        "url": ("https://alerts.food.gov.uk/article/7000/Hotel-Chocolat-"
                "recalls-several-chocolate-products-because-of-the-possible-"
                "presence-of-Salmonella"),
        "canon": "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-54-2026",
        "why": ("duplicate_of_published_recall — the same FSA alert "
                "(FSA-PRIN-54-2026, Hotel Chocolat, Salmonella, 2026-11-21) is "
                "published at its canonical URL https://alerts.food.gov.uk/"
                "news-alerts/alert/fsa-prin-54-2026 with the six Christmas "
                "Classic box products. Same regulator, same firm, same date, "
                "same hazard, two URLs; this copy came from the Exa daily-search "
                "extract (Product 'several chocolate products'). Never "
                "re-promote."),
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


def _find(ws, url):
    head = _head(ws)
    iu = head.index("URL") + 1
    t = _key(url)
    return [r for r in range(2, ws.max_row + 1) if _key(ws.cell(r, iu).value) == t]


def main() -> int:
    wb = openpyxl.load_workbook(XLSX)
    ws, arch, wr = wb["Recalls"], wb["Rejected"], wb["Weekly_Review"]
    head = _head(ws)
    removed = wr_removed = 0

    for spec in REMOVE:
        if not _find(ws, spec["canon"]):
            print(f"  canonical row {spec['canon']} missing — duplicate NOT removed")
            continue
        rows = _find(ws, spec["url"])
        for r in sorted(rows, reverse=True):
            row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
            _archive(arch, row, "Removed from Recalls by the " + BY + " — "
                     + spec["why"])
            ws.delete_rows(r, 1)
            removed += 1
            print(f"  Recalls: REMOVED {row.get('Date')} {row.get('Source')!r} "
                  f"{row.get('Company')!r} {spec['url'][:60]}")
        if not rows:
            print(f"  Recalls: no row at {spec['url'][:70]} (already gone)")
        for r in sorted(_find(wr, spec["url"]), reverse=True):
            wr.delete_rows(r, 1)
            wr_removed += 1
            print(f"  Weekly_Review: dropped {spec['url'][:70]}")

    if removed or wr_removed:
        wb.save(XLSX)
    wb.close()
    if removed:
        from pipeline.merge_master import mirror_json_from_xlsx
        print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"ROWS_CHANGED=0 ROWS_REMOVED={removed} WEEKLY_REVIEW_DROPPED={wr_removed}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
