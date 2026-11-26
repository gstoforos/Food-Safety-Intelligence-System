# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-26 (b) — four recalls the pipeline missed,
each read on the regulator's own page and queued through add_manual_row.py:
CFIA Casearia il Ghiottone burattina (generic E. coli); MAPAQ Ferme des
Provins glass-jar products (C. botulinum); Ruokavirasto K-Supermarket
Tapiola fish patties (nitrite salt); CAA rcl 36197 Kane-ue kamaage chirimen
(juvenile pufferfish). AESAN ES2026/712 (glass in nuts) is listed by AESAN
but its alert page would not open — not added. CAA rcl 36190 (radioactive
caesium) waits on the operator's scope ruling.
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
ROWS_JSON = ROOT / "tools" / "rows_2026_11_26b.json"


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
