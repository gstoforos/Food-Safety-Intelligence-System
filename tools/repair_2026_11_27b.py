# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-27 (b) — three recalls read on the regulator's
own page and queued through add_manual_row.py: BVL (DE) EDEKA Herzstücke
Ackergold potatoes and RappelConso 24164 Terroirs potatoes (both
glycoalkaloids, Parmentine lot 26128774), and AESAN ES2026/712 (glass in
nuts, 15 products). Any archive row holding one of these URLs is marked
SUPERSEDED (append-only, nothing deleted) so the re-promotion guard does not
refuse the verified row.
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
ROWS_JSON = ROOT / "tools" / "rows_2026_11_27b.json"


def main() -> int:
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), str(ROWS_JSON)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        raise SystemExit(f"add_manual_row exited {proc.returncode}")
    import json as _json
    import openpyxl
    from pipeline.merge_master import _normalize_url_for_dedup as _k
    _urls = {_k(r["URL"]) for r in _json.loads(
        (ROOT / "tools" / "rows_2026_11_27b.json").read_text(encoding="utf-8"))}
    _wb = openpyxl.load_workbook(XLSX)
    _sup = 0
    for _name in ("Weekly_Rejected", "Rejected"):
        if _name not in _wb.sheetnames:
            continue
        _a = _wb[_name]
        _h = [c.value for c in _a[1]]
        _col = next((_h.index(c) + 1 for c in ("RejectionReason", "RejectReason") if c in _h), None)
        if _col is None or "URL" not in _h:
            continue
        for _r in range(2, _a.max_row + 1):
            if _k(str(_a.cell(_r, _h.index("URL") + 1).value or "")) not in _urls:
                continue
            _cur = str(_a.cell(_r, _col).value or "")
            if "SUPERSEDED daily-global 2026-11-27" in _cur:
                continue
            _a.cell(_r, _col).value = ("[SUPERSEDED daily-global 2026-11-27 — the stored URL is "
                "the regulator's own notice, read in full; the 'no official press-release URL' "
                "verdict tested the discovery article, not this URL] ") + _cur
            if "Reviewed" in _h:
                _a.cell(_r, _h.index("Reviewed") + 1).value = "SUPERSEDED"
            _sup += 1
    _wb.save(XLSX)
    _wb.close()
    print(f"ARCHIVE_SUPERSEDED={_sup}")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
