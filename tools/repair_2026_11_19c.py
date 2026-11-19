# -*- coding: utf-8 -*-
"""Operator review 2026-11-19 (c) — Champibon dried morels, RappelConso
fiche 24047, archived out of Pending on George's decision of 2026-11-19.

The row (pending_enrichment since 2026-11-09) names its hazard only as "une
toxine"; no organism or toxin is identified on the fiche, and the ARS
Nouvelle-Aquitaine statement links a death only "potentiellement" to dried
morels (outbreak link not confirmed — George, 2026-11-15: stay at 0 until
confirmed). Sister fiches 24012 and 24016 are already in Weekly_Rejected.
One archive row goes to Rejected; nothing is deleted from an archive.
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
URL = "https://rappel.conso.gouv.fr/fiche-rappel/24047/interne"
REASON = (
    "operator review 2026-11-19 (George): archived. Hazard on fiche 24047 is "
    "only 'une toxine' — no organism or toxin named, so no publishable "
    "Pathogen; the ARS Nouvelle-Aquitaine death is only 'potentiellement liee' "
    "to dried morels and is not a confirmed link. Re-open if the toxin or the "
    "link is confirmed. Sister fiches 24012/24016 already rejected.")


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
        print("  Pending: no Champibon 24047 copy left — nothing to do")
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
        print(f"  Pending row {r}: Champibon 24047 archived (unnamed toxin)")
        ws.delete_rows(r, 1)
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"PENDING_REMOVED={len(rows)}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
