# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-19 (b) — a page-chrome copy of a recall
already queued from the regulator's press release.

Pending holds a row at https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/
2026_637.html (pending_gap, "via Exa daily search, deterministic extract")
with Company "Food", Brand "Food" and Product "/ Allergy Alerts - Not to
consume batch of bottled apple juice drink with patulin ..." — the CFS
"Food Alerts / Allergy Alerts" breadcrumb split into the firm fields. It passes
every publish-gate rule and would have published on the next promote.

It is the same CFS action (Issue Date 17.11.2026, bottled apple juice,
patulin) as the row added on 2026-11-18 from the CFS press release
https://www.cfs.gov.hk/english/press/20261117_12747.html (Homegrown 100% Pure
NZ Apple Juice, The Homegrown Juice Company Limited, 79 vs 50 mcg/kg). The
copy leaves the queue; one archive row goes to Rejected with the verdict.
Nothing is deleted from an archive.
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
TODAY = "2026-11-19"
URL = "https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/2026_637.html"
REASON = (
    "daily-global review 2026-11-19: DUPLICATE of the CFS (HK) row "
    "https://www.cfs.gov.hk/english/press/20261117_12747.html (2026-11-17, "
    "Homegrown 100% Pure NZ Apple Juice, The Homegrown Juice Company Limited, "
    "patulin 79 vs 50 mcg/kg). This copy was extracted from the CFS Food "
    "Alert page: Company and Brand 'Food' and Product '/ Allergy Alerts - ...' "
    "are the page breadcrumb, not the firm or the food.")


def _head(ws):
    return [c.value for c in ws[1]]


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Pending"]
    head = _head(ws)
    col = head.index("URL") + 1
    target = _key(URL)
    rows = [r for r in range(2, ws.max_row + 1)
            if _key(ws.cell(r, col).value) == target]
    if not rows:
        print("  Pending: no CFS 2026_637 copy left — nothing to do")
        return 0
    first = {h: ws.cell(rows[0], i + 1).value for i, h in enumerate(head)}
    if "Rejected" in wb.sheetnames:
        arch = wb["Rejected"]
        out = []
        for h in _head(arch):
            if h in ("RejectReason", "RejectionReason"):
                out.append(REASON)
            elif h == "RejectedBy":
                out.append("daily-global review " + TODAY)
            elif h in ("RejectedAt", "Week_Added"):
                out.append(TODAY)
            elif h == "Status":
                out.append("rejected")
            elif h == "Reviewed":
                out.append("Y")
            else:
                out.append(first.get(h, ""))
        arch.append(out)
    for r in reversed(rows):
        print(f"  Pending row {r}: CFS 2026_637 breadcrumb copy archived as DUPLICATE")
        ws.delete_rows(r, 1)
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"PENDING_REMOVED={len(rows)}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
