# -*- coding: utf-8 -*-
"""Daily-global review 2026-11-13 (b) — two recalls the pipeline missed.

1. HAPIH (HR) 12.11.2026 — Okusi zavičaja Dalmatian bacon, Listeria
   monocytogenes (HAH is a configured source; same-source miss).
2. Fødevarestyrelsen (DK) 12.11.2026 — Kilic cracked barley groats, moth
   webbing; carried only by the RSS feed, the HTML list renders client-side.

Both read on the regulator's own page on 2026-11-13 and queued through
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
ROWS_JSON = ROOT / "tools" / "rows_2026_11_13b.json"


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
