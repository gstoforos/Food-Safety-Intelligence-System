# -*- coding: utf-8 -*-
"""Operator review 2026-11-01 (b) — the October outbreak count.

October 2026 was published with 6 Recalls rows flagged Outbreak=1. Two of
them are not outbreaks of their own, and both are second copies of a recall
the register already carries, so every report counted them twice:

1. FSA (UK) 2026-10-21, SuperValu and Centra salmon / prawn cocktail, at
   alerts.food.gov.uk/article/6946/... — Reason "Listeria monocytogenes —
   outbreak", Outbreak=1. The Reason is the Exa gap-finder's own text; the
   row's Notes hold only the FSA page title. The same FSA alert is ALREADY
   published at its canonical URL alerts.food.gov.uk/news-alerts/alert/
   fsa-prin-50-2026 (Notes "PRIN-50-2026", Outbreak 0), and the same recall
   from Ireland is published at FSAI alert 2026.67. FSAI 2026.67, read on
   2026-11-01 ("Tuesday, 20 October 2026", Category 1 For Action), names
   the products and the hazard and reports NO illnesses, cases or outbreak.
   So this row is a duplicate carrying an outbreak no regulator stated.

2. NFSA (EG) 2026-10-11, Yasensvit Ltd eggs, Salmonella Enteritidis,
   Outbreak=1, URL a Google News redirect, Notes "News-authority source:
   see.news (no official NFSA per-recall portal)". There is no NFSA notice
   behind it (R3: the regulator's own URL is required), the supplier is the
   Ukrainian one in the FSA (UK) alert FSA-FAFA-03-2026 of 2026-10-10 that is
   already published with Outbreak=1 (36 linked cases in England), and the
   outbreak is England's, not Egypt's. A country fan-out of one alert,
   counted as a second outbreak.

Both leave Recalls for Rejected with the verdict in the archive's reason
column. Nothing is deleted; archives are append-only.
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
TODAY = "2026-11-01"
BY = "operator review " + TODAY

REMOVE = [
    {
        "url": ("https://alerts.food.gov.uk/article/6946/SuperValu-and-Centra-"
                "recall-salmon-and-prawn-cocktail-products-because-of-"
                "contamination-with-Listeria-monocytogenes"),
        "why": ("DUPLICATE, and the outbreak flag is unsupported. The same "
                "FSA (UK) alert is published at its canonical URL "
                "alerts.food.gov.uk/news-alerts/alert/fsa-prin-50-2026 "
                "(PRIN-50-2026, 2026-10-21), and the same recall from Ireland "
                "at FSAI alert 2026.67 (2026-10-20). This copy's Reason "
                "'Listeria monocytogenes — outbreak' and Outbreak=1 came from "
                "the Exa gap-finder extract, not from a regulator: FSAI "
                "2026.67, read 2026-11-01, reports no illnesses, cases or "
                "outbreak. Counted October's outbreaks once too often."),
    },
    {
        "url": ("https://news.google.com/rss/articles/CBMifkFVX3lxTFBUWmtsaG4y"
                "SWVMZTFaQzBSVUIxSENpcnpYczNWOTFGRUFWdGw0YnpPSHB0RlV4WUJ1bzJX"
                "WkZjX252MnIyZ1BxTHA4OEpCdmMtel9MNGdIRTQxRFc2NEd1ZFJPbGRyS0pG"
                "LUVZOHBaeEdxNWVnWWhCSENJenEyZw?oc=5"),
        "why": ("No regulator URL (R3): a Google News redirect, Notes "
                "'News-authority source: see.news (no official NFSA "
                "per-recall portal)'; no NFSA notice stands behind it. The "
                "Yasensvit Ltd (Ukraine) egg recall and its Salmonella "
                "Enteritidis outbreak (36 cases in England) are already "
                "published from FSA (UK) FSA-FAFA-03-2026 of 2026-10-10 with "
                "Outbreak=1; this row re-counted that outbreak under Egypt. "
                "Duplicate country fan-out of one alert."),
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
            print(f"  Recalls: REMOVED {row.get('Source')!r} {row.get('Company')!r}")
        if not hit:
            print(f"  Recalls: no row at {spec['url'][:70]} (already gone)")
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"ROWS_REMOVED={removed}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
