# -*- coding: utf-8 -*-
"""Operator review 2026-11-07 (b) — two Pending copies of one published AESAN alert.

Pending held two rows at https://www.aesan.gob.es/en/alertas/2026_72 (Status
pending_gap_v1 and pending_gap_v2, both ScrapedAt 2026-11-04T20:41:39Z),
Company "Noticias", Product "Agencia Española de Seguridad Alimentaria y
Nutrición" and Notes holding the text of an AESAN news item about a Listeria
conference — the extractor scraped the agency's news listing, not the alert.
tests/test_pending_holds_one_row_per_url went red on main.

The alert itself is real and already PUBLISHED in Recalls at
https://aesan.gob.es/alertas/2026_72 (2026-10-22, cooked Chilean mussels,
Listeria monocytogenes); merge_master._normalize_url_for_dedup collapses the
two URLs to one key. Both Pending copies leave the queue; one archive row goes
to Rejected with the verdict. Nothing is deleted from an archive.
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
TODAY = "2026-11-07"
URL = "https://www.aesan.gob.es/en/alertas/2026_72"
REASON = (
    "operator review 2026-11-07: DUPLICATE of the published Recalls row "
    "https://aesan.gob.es/alertas/2026_72 (2026-10-22, cooked Chilean "
    "mussels, Listeria monocytogenes). This copy was scraped from AESAN's "
    "news listing — Company 'Noticias', Product the agency's own name, Notes "
    "an item about a Listeria conference — and sat twice in Pending "
    "(pending_gap_v1 and pending_gap_v2).")


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
        print("  Pending: no AESAN 2026_72 copy left — nothing to do")
        return 0
    first = {h: ws.cell(rows[0], i + 1).value for i, h in enumerate(head)}
    if "Rejected" in wb.sheetnames:
        arch = wb["Rejected"]
        out = []
        for h in _head(arch):
            if h in ("RejectReason", "RejectionReason"):
                out.append(REASON)
            elif h == "RejectedBy":
                out.append("operator review " + TODAY)
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
        print(f"  Pending row {r}: AESAN 2026_72 news-listing copy removed")
        ws.delete_rows(r, 1)
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"PENDING_REMOVED={len(rows)}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
