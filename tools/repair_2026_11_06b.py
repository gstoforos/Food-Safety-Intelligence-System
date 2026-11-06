# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-06 (b) — three recalls the pipeline missed.

Each was read today on the regulator's own page and is absent from Recalls
and Pending (checked against every sheet of the workbook):

1. BVL (DE) 05.11.2026 — Hohensteiner Hofkäserei (Firma Rauscher GbR) raw-milk
   cheeses "Vesperkaes" and "Bueffelkuh-Rendezvous", Listeria monocytogenes
   detected. The gap finder refused it four times on 2026-11-05 as
   no-authority-url (BVL/AGES/AFSCA/NVWA fan-out of a news article) although
   the lebensmittelwarnung.de notice exists.
2. BVL (DE) 05.11.2026 — dennree Pinienkerne 60 g, Salmonella not ruled out.
3. HAPIH (HR) 05.11.2026 — dmBio pinjoli 60 g, possible Salmonella.

Rows go through tools/add_manual_row.py (the real Pending path), never
straight into Recalls. Runs after repair_2026_11_06.py (sorted order).
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
ROWS_JSON = ROOT / "tools" / "rows_2026_11_06b.json"


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")
    if not ROWS_JSON.exists():                          # pragma: no cover
        raise SystemExit(f"missing {ROWS_JSON}")
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
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
