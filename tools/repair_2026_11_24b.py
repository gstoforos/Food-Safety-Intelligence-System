# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-24 (b) — four recalls the pipeline missed,
each read on the regulator's own page and queued through add_manual_row.py:
MPI (NZ) Pak'nSave Highland Park mince (metal) and New World Kaiapoi mince
(plastic); Ruokavirasto (FI) Corny chocolate bars (metal); RappelConso fiche
24144 Panier du Primeur potatoes (glycoalkaloids, same lot as fiche 24142).
CAA rcl 36173 names no hazard (colour and flavour complaints) and 36175
(radioactive caesium) waits on the operator's scope ruling — neither added.
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
ROWS_JSON = ROOT / "tools" / "rows_2026_11_24b.json"


def main() -> int:
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), str(ROWS_JSON)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        raise SystemExit(f"add_manual_row exited {proc.returncode}")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
