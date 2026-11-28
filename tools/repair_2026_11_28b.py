# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-28 (b) — three BVL notices of 27.11.2026 for
Berschneider GmbH smoked duck and goose breast (Norma; Netto/Metro/EDEKA and
others; Penny Best Moments), Salmonella (and Listeria on the Norma notice),
each read on lebensmittelwarnung.de and queued through add_manual_row.py.
The pipeline found the event on 27.11 and rejected it eight times as
news-only. Mattilsynet Monster Energy (L-carnitine L-tartrate, "no acute
health risk") and CFS HK soft ice-cream coliforms wait on operator rulings.
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
ROWS_JSON = ROOT / "tools" / "rows_2026_11_28b.json"


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
