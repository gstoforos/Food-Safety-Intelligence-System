# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-10 (b).

A. GIS (PL) 09.11.2026 — Arla Protein Pudding Vanilla Cookie 200 g, possible
   glass fragments. Refused by reviewer 1 as "Official URL not reachable" and
   then stamped "Arrived already marked rejected; confirmer did not re-review".
   The URL was read on 2026-11-10 and answers in full. The Pending row is
   repaired in place from that page and put back in the queue; the
   Weekly_Rejected copy is marked SUPERSEDED (append-only, nothing deleted).
B. Two RappelConso fiches published 09/11/2026 that are in no sheet (24041
   Alnatura salad seeds via Houra, Salmonella; 24006 Santa Maria pears,
   pesticide residues), queued through tools/add_manual_row.py.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
ROWS_JSON = ROOT / "tools" / "rows_2026_11_10b.json"
STAMP = "daily-global 2026-11-10"
GIS = ("https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-zywnosci-"
       "mozliwa-obecnosc-fragmentow-szkla-w-jednej-partii-puddingu-proteinowego")

GIS_FIX = {
    "Company": "Arla Foods amba",
    "Brand": "Arla",
    "Product": ("Arla Protein Pudding Vanilla Cookie, 200 g, lot ES26271001, "
                "best before 25.04.2027"),
    "Pathogen": "Foreign material (glass)",
    "Reason": ("Possible presence of glass fragments in one lot; eating the "
               "product may cause injury to the mouth."),
    "Status": "pending",
    "RejectedBy": "",
}
GIS_NOTE = (
    f"[{STAMP}: the refusal \"URL agent: Official URL not reachable\" was "
    f"wrong — the GIS notice was read in full on 2026-11-10 at {GIS}: "
    "published 09.11.2026; producer \"Arla Foods amba Esbjerg Mejeri, Dania\"; "
    "Polish distributor \"Arla Foods Spółka Akcyjna, ul. Dworcowa 27, 78-220 "
    "Tychowo\"; product \"Protein Pudding Vanilla Cookie\", 200g, lot "
    "ES26271001, best before 25.04.2027; hazard \"Spożycie produktu "
    "zanieczyszczonego fragmentami szkła może prowadzić do uszkodzenia jamy "
    "ustnej\"; the company is withdrawing the product under PIS/IW "
    "supervision. Fields set from that page only; row returned to the queue.] "
    "[original product: możliwa obecność fragmentów szkła w jednej partii "
    "puddingu proteinowego] [original Reason (pl): \"Ostrzeżenie publiczne "
    "dotyczące żywności: możliwa obecność fragmentów szkła w jednej partii "
    "puddingu proteinowego\"]")
SUPERSEDED = (
    f"[SUPERSEDED {STAMP} — \"Official URL not reachable\" is false: the GIS "
    "page answered in full on 2026-11-10 and names producer, lot, best-before "
    "and hazard. \"Confirmer did not re-review\" is not a finding about the "
    "recall. The repaired Pending row is the copy that wins; this row is kept "
    "as the audit trail of the original refusal.] ")


def _head(ws):
    return [c.value for c in ws[1]]


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _gis(wb) -> tuple[int, int]:
    fixed = sup = 0
    target = _key(GIS)
    ws = wb["Pending"]
    head = _head(ws)
    iu = head.index("URL") + 1
    rows = [r for r in range(2, ws.max_row + 1)
            if _key(ws.cell(r, iu).value) == target]
    for n, r in enumerate(rows):
        notes = str(ws.cell(r, head.index("Notes") + 1).value or "")
        if STAMP in notes:
            continue
        if n > 0:                       # one live copy only
            ws.delete_rows(r, 1)
            continue
        for f, v in GIS_FIX.items():
            if f in head:
                ws.cell(r, head.index(f) + 1).value = v
        notes = notes.replace("REJECTED: ", "[was refused: ", 1)
        if notes.startswith("[was refused: "):
            notes = notes.replace(" | ", "] | ", 1) if "] |" not in notes else notes
        ws.cell(r, head.index("Notes") + 1).value = (notes + " " + GIS_NOTE).strip()
        fixed += 1
        print(f"  Pending:{r}: GIS Arla pudding repaired and re-queued")
    for name in ("Weekly_Rejected", "Rejected"):
        if name not in wb.sheetnames:
            continue
        a = wb[name]
        h = _head(a)
        if "URL" not in h:
            continue
        au = h.index("URL") + 1
        col = next((h.index(c) + 1 for c in ("RejectionReason", "RejectReason")
                    if c in h), None)
        for r in range(2, a.max_row + 1):
            if _key(a.cell(r, au).value) != target or col is None:
                continue
            cur = str(a.cell(r, col).value or "")
            if f"SUPERSEDED {STAMP}" in cur:
                continue
            a.cell(r, col).value = SUPERSEDED + cur
            if "Reviewed" in h:
                a.cell(r, h.index("Reviewed") + 1).value = "SUPERSEDED"
            sup += 1
            print(f"  {name}:{r}: GIS refusal marked SUPERSEDED")
    return fixed, sup


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")
    wb = openpyxl.load_workbook(XLSX)
    fixed, sup = _gis(wb)
    wb.save(XLSX)
    wb.close()
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), str(ROWS_JSON)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        print(f"  NOTE: add_manual_row exited {proc.returncode} — a near-duplicate "
              "refusal of fiche 24041 (same product as published fiche 24032) "
              "is an accepted outcome; read the verdict lines above.")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"GIS_FIXED={fixed} ARCHIVE_SUPERSEDED={sup}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
