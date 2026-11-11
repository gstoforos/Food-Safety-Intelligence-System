# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-11 (b) — two recalls the pipeline missed.

1. BVL (DE) 09.11.2026 — Bakenhus Biofleisch corned beef, glass fragments.
   Refused four times by the DE/AT/BE/NL gap finders as "no official BVL
   press-release URL"; the lebensmittelwarnung.de per-Meldung page exists.
2. MAST (IS) 10.11.2026 — Golden Turtle wasabi rice crackers, glycidyl
   fatty acid esters.

Both read on the regulator's own page on 2026-11-11 and queued through
tools/add_manual_row.py.
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
ROWS_JSON = ROOT / "tools" / "rows_2026_11_11b.json"


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
