# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-08 (b) — two recalls refused for the wrong reason.

Both were read today on the regulator's own page:

1. BVL (DE) 06.11.2026 — EDEKA Bio Soft-Aprikosen 180 g, ochratoxin A. Refused
   four times on 2026-11-06 (BVL/AGES/AFSCA/NVWA gap finders) as
   no-authority-url, although the lebensmittelwarnung.de notice exists.
2. AESAN (ES) 04.11.2026, alert 2026_79 — La Pardina goat kefir, Salmonella.
   Weekly_Rejected holds the regulator's own URL refused with "The hazard is
   an undeclared allergen (Salmonella) and not a microbial pathogen". Salmonella
   is a named pathogen; the verdict is marked SUPERSEDED here (the marker the
   re-promotion guard and the published-and-rejected test both read), then the
   row is queued through tools/add_manual_row.py like any other.

Archives are append-only: the refused rows stay, annotated.
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
ROWS_JSON = ROOT / "tools" / "rows_2026_11_08b.json"
AESAN_79 = "https://www.aesan.gob.es/alertas/2026_79"
SUPERSEDED = (
    "[SUPERSEDED 2026-11-08 by the daily-global review — the verdict below "
    "calls Salmonella an undeclared allergen. Salmonella is a named microbial "
    "pathogen and in scope. AESAN alert 2026_79 was read on its own page on "
    "2026-11-08: \"Alerta por presencia de Salmonella en kéfir de cabra "
    "procedente de España (Ref. ES2026/659)\", 04/11/2026, brand La Pardina. "
    "The recall is queued again from tools/rows_2026_11_08b.json; the copy in "
    "Recalls wins. Kept as the audit trail of the original refusal.] ")


def _supersede(wb) -> int:
    done = 0
    for name in ("Weekly_Rejected", "Rejected"):
        if name not in wb.sheetnames:
            continue
        ws = wb[name]
        head = [c.value for c in ws[1]]
        if "URL" not in head:
            continue
        iu = head.index("URL") + 1
        col = next((head.index(h) + 1 for h in ("RejectionReason", "RejectReason")
                    if h in head), None)
        if col is None:
            continue
        for r in range(2, ws.max_row + 1):
            u = str(ws.cell(r, iu).value or "").strip().rstrip("/").lower()
            if u.split("://")[-1].removeprefix("www.") != \
                    AESAN_79.split("://")[-1].removeprefix("www."):
                continue
            cur = str(ws.cell(r, col).value or "")
            if "SUPERSEDED 2026-11-08" in cur:
                continue
            ws.cell(r, col).value = SUPERSEDED + cur
            if "Reviewed" in head:
                ws.cell(r, head.index("Reviewed") + 1).value = "SUPERSEDED"
            done += 1
            print(f"  {name}:{r}: AESAN 2026_79 refusal marked SUPERSEDED")
    return done


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")
    wb = openpyxl.load_workbook(XLSX)
    n = _supersede(wb)
    wb.save(XLSX)
    wb.close()
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
         str(ROWS_JSON)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        raise SystemExit(f"add_manual_row exited {proc.returncode} — read the "
                         "per-row verdict lines above")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"ARCHIVE_SUPERSEDED={n}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
