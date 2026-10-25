# -*- coding: utf-8 -*-
"""Operator review 2026-10-25: the Brezzi salsa tartufata Pending row, before
the offline promoter publishes it (tools/repair_2026_10_24.py supersedes the
archive verdict that held it back).

On main the row read:
    Product   "Tartar sauce"        — wrong: salsa tartufata is TRUFFLE sauce
    Pathogen  "Botulism toxin (suspicion)"
    Reason    "Salsa tartufata richiamata per sospetta presenza di tossina
               botulinica (Recall ID)"  — Italian, with a dangling "(Recall ID)"
Every value below is a translation of text already on the row (its Reason and
its own URL, ".../cartello di richiamo modello ministero LOTTO 596_...pdf");
nothing is read from outside it. The originals are kept in Notes.
NOTHING IS REMOVED.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
KEY = "lotto%20596_1792593268.pdf"
SET = {
    "Product": "Truffle sauce (salsa tartufata), lot 596",
    "Pathogen": "Botulinum toxin",
    "Reason": "Truffle sauce recalled for suspected presence of botulinum toxin",
}
STAMPS = ("[needs cleanup: Reason not in English]",
          "[needs cleanup: Product not in English]")


def main() -> int:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    n = 0
    for sheet in ("Pending", "Recalls", "Weekly_Review"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = [c.value for c in ws[1]]
        if "URL" not in head:
            continue
        for r in range(2, ws.max_row + 1):
            url = str(ws.cell(r, head.index("URL") + 1).value or "").lower()
            if KEY not in url:
                continue
            row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
            orig_p, orig_r = row.get("Product") or "", row.get("Reason") or ""
            for f, v in SET.items():
                if f in head:
                    ws.cell(r, head.index(f) + 1).value = v
            if "Notes" in head:
                notes = str(row.get("Notes") or "")
                for st in STAMPS:
                    notes = notes.replace(st, "")
                mark = "[operator review 2026-10-25: truffle sauce"
                if mark not in notes:
                    notes = " ".join((notes + f" {mark} — Product was "
                                      f"\"{orig_p}\", a mistranslation of salsa "
                                      "tartufata; Reason translated; lot 596 from "
                                      "the row's own URL.] [original Reason (it): "
                                      f"\"{orig_r}\"]").split())
                ws.cell(r, head.index("Notes") + 1).value = notes
            n += 1
            print(f"  {sheet} row {r}: Brezzi salsa tartufata translated")
    if n < 1:
        print("  Brezzi salsa tartufata row not found — nothing changed")
    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
