# -*- coding: utf-8 -*-
"""2026-11-08 (z) — rebuild the closed months after today's repairs.

Runs last (sorted order). The morning fix found the day-8 monthly-updates
manifest anchored at 2026-09-30 and October's report one rebuild behind the
register (390 vs 392); the outbreak check of the same day then removed two
October duplicates (outbreaks 4 -> 3). tools/rebase_and_verify.py does not
rebuild closed months, so this runs pipeline.build_monthly_updates_check —
the day-10 updates check — against the register as it stands after every
repair, which regenerates the October report, its JSONs and the manifest.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    from pipeline import build_monthly_updates_check as m
    return int(m.main() or 0)


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
