# -*- coding: utf-8 -*-
"""Operator review 2026-11-19 (d) — BLV (CH) 16.11.2026, Migros milk rice
products with raspberries, possible foreign bodies (recallswiss #1193).

Refused in Weekly_Rejected as "URL agent: Hazard is foreign material, not a
microbial pathogen" (RejectedBy 'unknown') — wrong on the rules: foreign
material is in scope. Read on the BLV's own recall list and queued through
tools/add_manual_row.py on George's approval; the refusal is marked
SUPERSEDED (append-only, nothing deleted).
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
ROWS_JSON = ROOT / "tools" / "rows_2026_11_19d.json"


def main() -> int:
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), str(ROWS_JSON)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        raise SystemExit(f"add_manual_row exited {proc.returncode}")
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    sup = 0
    for name in ("Weekly_Rejected", "Rejected"):
        if name not in wb.sheetnames:
            continue
        a = wb[name]
        h = [c.value for c in a[1]]
        col = next((h.index(c) + 1 for c in ("RejectionReason", "RejectReason") if c in h), None)
        if col is None or "Product" not in h:
            continue
        for r in range(2, a.max_row + 1):
            prod = str(a.cell(r, h.index("Product") + 1).value or "")
            cur = str(a.cell(r, col).value or "")
            if "Milchreis" in prod and "Migros" in prod and "SUPERSEDED operator review 2026-11-19" not in cur:
                a.cell(r, col).value = ("[SUPERSEDED operator review 2026-11-19 — foreign material is in "
                    "scope; recall published from the BLV recall list, recallswiss #1193] ") + cur
                if "Reviewed" in h:
                    a.cell(r, h.index("Reviewed") + 1).value = "SUPERSEDED"
                sup += 1
    wb.save(XLSX)
    wb.close()
    print(f"MIGROS_REFUSALS_SUPERSEDED={sup}")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
