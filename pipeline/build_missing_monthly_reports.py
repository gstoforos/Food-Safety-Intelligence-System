"""
AFTS FSIS — Monthly report gap-filler.

Reads docs/data/monthly-index.json and figures out which *closed* months
since the backfill-start are missing real stats (non-null `total`). For
each missing month, invokes the monthly builder with the correct
--month-end and lets it do its work.

NEVER overwrites an existing month with real stats. Idempotent.

Usage (called from .github/workflows/afts-monthly-report.yml):

    python -m pipeline.build_missing_monthly_reports \\
        --from 2026-03 \\
        --xlsx docs/data/recalls.xlsx \\
        --index docs/data/monthly-index.json \\
        --builder docs/build_monthly_report_afts.py
"""
from __future__ import annotations

import argparse
import calendar
import json
import logging
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("build_missing_monthly_reports")


def _parse_yyyy_mm(s: str) -> Tuple[int, int]:
    y, m = s.strip().split("-")
    return int(y), int(m)


def _last_day_of_month(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _today_utc() -> date:
    # Use UTC to decide "closed" months — avoids DST flips changing
    # which month is considered closed near the 1st-of-month boundary.
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).date()


def iter_months(start: Tuple[int, int], end_exclusive: Tuple[int, int]):
    """Yield (year, month) tuples from start (inclusive) to end_exclusive."""
    y, m = start
    while (y, m) < end_exclusive:
        yield (y, m)
        m += 1
        if m == 13:
            m = 1
            y += 1


def closed_months_since(start_ym: Tuple[int, int], today: date) -> List[Tuple[int, int]]:
    """Return every (year, month) >= start_ym whose last day is strictly
    before today. I.e. months that have definitely ended."""
    # End_exclusive = the current month (not closed until the 1st of next month).
    end_exclusive = (today.year, today.month)
    # If today IS the 1st, the previous month just closed — include it.
    # iter_months uses end_exclusive, so already correct.
    return list(iter_months(start_ym, end_exclusive))


def load_index(path: Path) -> List[dict]:
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning("monthly-index.json unreadable (%s) — treating as empty", e)
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and isinstance(raw.get("reports"), list):
        return raw["reports"]
    return []


def has_real_stats(entry: dict) -> bool:
    """An index entry counts as 'built' only if it has a real recall count.
    Legacy Jan/Feb entries have pdf_url but total=null — those count as
    pre-pipeline manual uploads and we do NOT try to regenerate them."""
    return (
        entry.get("total") is not None
        and int(entry.get("total", 0)) >= 0
    )


def month_is_covered(year: int, month: int, index: List[dict]) -> bool:
    """True if the index already has an entry for YYYY-M<MM>. Counts
    legacy null-stats entries as 'covered' — don't overwrite manual
    uploads."""
    filename = f"{year}-M{month:02d}.html"
    for e in index:
        if e.get("filename") == filename:
            # Exists. If it has real stats OR is a legacy pdf-only entry,
            # skip regeneration. Only rebuild if the entry is empty shell
            # with no pdf_url AND no stats (shouldn't happen in practice).
            if has_real_stats(e) or e.get("pdf_url"):
                return True
    return False


def run_builder(builder: Path, month_end: date, xlsx: Path) -> int:
    cmd = [
        sys.executable, str(builder),
        "--month-end", month_end.isoformat(),
        "--xlsx", str(xlsx),
    ]
    log.info("Building: %s", " ".join(cmd))
    completed = subprocess.run(cmd, capture_output=False)
    return completed.returncode


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="from_ym", default="2026-03",
                    help="Earliest YYYY-MM to consider (default 2026-03 — the first automated month)")
    ap.add_argument("--xlsx", required=True, help="Path to recalls.xlsx")
    ap.add_argument("--index", required=True, help="Path to docs/data/monthly-index.json")
    ap.add_argument("--builder", required=True, help="Path to the monthly builder script")
    args = ap.parse_args()

    xlsx = Path(args.xlsx)
    index_path = Path(args.index)
    builder = Path(args.builder)

    if not xlsx.exists():
        log.error("recalls.xlsx not found at %s", xlsx); return 2
    if not builder.exists():
        log.error("monthly builder not found at %s", builder); return 2

    try:
        start_ym = _parse_yyyy_mm(args.from_ym)
    except ValueError:
        log.error("Invalid --from (expected YYYY-MM): %s", args.from_ym); return 2

    today = _today_utc()
    index = load_index(index_path)
    log.info("Monthly index has %d entries. Today (UTC): %s", len(index), today.isoformat())

    candidates = closed_months_since(start_ym, today)
    log.info("Closed months since %s: %s",
             args.from_ym,
             ", ".join(f"{y}-{m:02d}" for y, m in candidates) or "(none)")

    missing: List[Tuple[int, int]] = [
        (y, m) for (y, m) in candidates if not month_is_covered(y, m, index)
    ]

    if not missing:
        log.info("No missing months. Nothing to build.")
        return 0

    log.info("Building %d missing month(s): %s",
             len(missing),
             ", ".join(f"{y}-{m:02d}" for y, m in missing))

    failures = 0
    for (y, m) in missing:
        month_end = _last_day_of_month(y, m)
        rc = run_builder(builder, month_end, xlsx)
        if rc != 0:
            failures += 1
            log.error("Builder failed for %d-%02d (rc=%d). Continuing.", y, m, rc)
        else:
            log.info("Built %d-M%02d OK.", y, m)

    if failures:
        log.error("%d month(s) failed to build.", failures)
        return 1

    log.info("All missing months built successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
