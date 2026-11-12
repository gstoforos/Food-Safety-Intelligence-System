# -*- coding: utf-8 -*-
"""Nightly operator review 2026-11-12 (b) — retire the archive verdict that
blocked RappelConso fiche 24064, then restore newest-first order in Recalls.

PHASE 1 — SUPERSEDE. Weekly_Rejected holds the confirm-agent's 2026-11-11
copy of fiche 24064 (GAEC Les Deux Savoies, farmhouse Reblochon de Savoie,
STEC) with RejectionReason "NO_REASON_RECORDED … treat as UNAUDITED". Its
Notes name the actual cause: the publish gate refused Company '/' and
Brand '/' as page debris. That defect was repaired through the curator on
2026-11-12 from the fiche itself (Company GAEC Les Deux Savoies, Brand
Unbranded, Product and Reason in English), and the row now passes the full
publish gate — but the re-promotion guard still read the archive row as a
live verdict and blocked the promotion. The repo's established way to
retire a wrong archive verdict is the "[SUPERSEDED <date> — …]" marker
(merge_master fourth exception, 2026-10-11): the row stays, its Reviewed
column says SUPERSEDED, and the reason records why. Nothing is deleted.

PHASE 2 — SORT. The 2026-11-12 promote paths appended their rows at the
bottom of the Recalls sheet (rows 2365+ dated 11-12 after a row dated
01-02), so recalls.json was not newest-first. Re-sorted with
merge_master.sort_rows; the content multiset is verified identical before
and after, and the script stops without saving if it is not.

    python tools/repair_2026_11_12b.py supersede
    python tools/repair_2026_11_12b.py sort
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-12"
URL_24064 = "https://rappel.conso.gouv.fr/fiche-rappel/24064/interne"
BANNER = (f"[SUPERSEDED {TODAY} — the defect behind this refusal (publish gate: "
          f"Company '/' and Brand '/' are page debris, named in Notes) was "
          f"repaired through the curator on {TODAY} from fiche 24064 itself "
          f"(Company GAEC Les Deux Savoies, Brand Unbranded, Product and Reason "
          f"in English; Motif 'Présence de Escherichia Coli STEC'). The row "
          f"passes the full publish gate and is PUBLISHED in Recalls; this "
          f"archive row is kept for the trail only. Operator review {TODAY}.] ")


def supersede() -> int:
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Weekly_Rejected"]
    hdr = [c.value for c in ws[1]]
    ucol, rcol, mcol = (hdr.index("URL") + 1, hdr.index("RejectionReason") + 1,
                        hdr.index("Reviewed") + 1)
    hits = [r for r in range(2, ws.max_row + 1)
            if str(ws.cell(r, ucol).value or "").strip().lower().rstrip("/")
            == URL_24064.lower()]
    if len(hits) != 1:
        print(f"STOP — expected exactly one Weekly_Rejected row for 24064, found {len(hits)}")
        return 2
    r = hits[0]
    prior = str(ws.cell(r, rcol).value or "")
    if "SUPERSEDED" in prior:
        print("already superseded; nothing written")
        return 0
    ws.cell(r, rcol).value = BANNER + prior
    ws.cell(r, mcol).value = "SUPERSEDED"
    wb.save(XLSX)
    print(f"Weekly_Rejected row {r} marked SUPERSEDED")
    return 0


def sort_recalls() -> int:
    from pipeline.merge_master import sort_rows
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Recalls"]
    hdr = [c.value for c in ws[1]]
    rows = [list(r) for r in ws.iter_rows(min_row=2, values_only=True)
            if any(v not in (None, "") for v in r)]
    before = Counter(tuple(str(v) for v in r) for r in rows)
    dicts = [dict(zip(hdr, r)) for r in rows]
    ordered = sort_rows(dicts)
    after = Counter(tuple(str(d.get(h)) for h in hdr) for d in ordered)
    if before != after:
        print("STOP — content multiset changed during sort; nothing saved")
        return 2
    if len(ordered) != ws.max_row - 1:
        print(f"STOP — {len(ordered)} rows vs sheet {ws.max_row - 1}; nothing saved")
        return 2
    moved = 0
    for i, d in enumerate(ordered, start=2):
        for j, h in enumerate(hdr, start=1):
            cell = ws.cell(i, j)
            if cell.value != d.get(h):
                cell.value = d.get(h)
                moved += 1
    wb.save(XLSX)
    print(f"Recalls re-sorted newest-first: {len(ordered)} rows, {moved} cells moved")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "supersede":
        raise SystemExit(supersede())
    if cmd == "sort":
        raise SystemExit(sort_recalls())
    print(__doc__)
    raise SystemExit(2)
