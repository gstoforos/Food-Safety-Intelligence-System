"""
AFTS FSIS — Weekly report gap-filler.

Scans docs/ for existing 20YY-W<NN>.html files and builds any missing
ISO weeks from --from through --this-week-end (inclusive). Each build
runs the weekly builder with --week-end <Friday>.

NEVER overwrites an existing weekly HTML. Idempotent.

Usage (called from .github/workflows/afts-weekly-report.yml):

    python -m pipeline.build_missing_weekly_reports \\
        --this-week-end 2026-04-26 \\
        --from "" \\
        --xlsx docs/data/recalls.xlsx \\
        --docs-dir docs \\
        --builder docs/build_weekly_report_afts.py

If --from is omitted or empty, defaults to the week ending 2026-04-12
(W15 — the oldest manually-created week; gap-fill starts AFTER that).
"""
from __future__ import annotations

import argparse
import logging
import re
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("build_missing_weekly_reports")

# The oldest week we'll auto-generate. W15 + W16 were George's manual
# versions — automation never touches those (PRESERVED_WEEKS).
DEFAULT_FROM_WEEK_END = date(2026, 4, 17)   # Friday W16 — first auto candidate is W17

# (iso_year, iso_week) pairs the automation MUST NOT overwrite.
PRESERVED_WEEKS: set = {
    (2026, 15),   # George's manual W15
    (2026, 16),   # George's manual W16
}


def _iso_week(d: date) -> int:
    return d.isocalendar().week


def _iso_year(d: date) -> int:
    return d.isocalendar().year


def _prev_friday(d: date) -> date:
    """Return the Friday on or before d."""
    # Monday=0 .. Friday=4 .. Sunday=6
    days_since_friday = (d.weekday() - 4) % 7  # Friday->0, Sat->1, Sun->2, Mon->3, ...
    return d - timedelta(days=days_since_friday)


def iter_week_ends(start_friday: date, end_friday: date) -> List[date]:
    """Return every Friday from start_friday to end_friday inclusive."""
    if start_friday.weekday() != 4:
        start_friday = _prev_friday(start_friday)
    if end_friday.weekday() != 4:
        end_friday = _prev_friday(end_friday)
    out = []
    cur = start_friday
    while cur <= end_friday:
        out.append(cur)
        cur += timedelta(days=7)
    return out


def existing_weeks(docs_dir: Path) -> set:
    """Return set of (iso_year, iso_week) already present as HTML."""
    pat = re.compile(r"^(20\d{2})-W(\d{2})\.html$")
    out = set()
    for p in docs_dir.glob("20*-W*.html"):
        m = pat.match(p.name)
        if m:
            out.add((int(m.group(1)), int(m.group(2))))
    return out


def run_builder(builder: Path, week_end: date, xlsx: Path, index_html: Path) -> int:
    cmd = [
        sys.executable, str(builder),
        "--week-end", week_end.isoformat(),
        "--xlsx", str(xlsx),
    ]
    log.info("Building: %s", " ".join(cmd))
    completed = subprocess.run(cmd, capture_output=False)
    return completed.returncode


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--this-week-end", required=True, help="Friday of the week we're closing now (YYYY-MM-DD)")
    ap.add_argument("--from", dest="from_date", default="",
                    help="Earliest week-ending Friday to consider (YYYY-MM-DD). "
                         "Empty -> default %s." % DEFAULT_FROM_WEEK_END.isoformat())
    ap.add_argument("--xlsx", required=True)
    ap.add_argument("--docs-dir", required=True)
    ap.add_argument("--builder", required=True)
    args = ap.parse_args()

    xlsx = Path(args.xlsx)
    docs_dir = Path(args.docs_dir)
    builder = Path(args.builder)
    index_html = docs_dir / "index.html"

    if not xlsx.exists():
        log.error("recalls.xlsx not found at %s", xlsx); return 2
    if not builder.exists():
        log.error("weekly builder not found at %s", builder); return 2
    if not docs_dir.is_dir():
        log.error("docs/ not found at %s", docs_dir); return 2

    try:
        this_week_end = date.fromisoformat(args.this_week_end)
    except ValueError:
        log.error("Invalid --this-week-end: %s", args.this_week_end); return 2

    if args.from_date.strip():
        try:
            from_week_end = date.fromisoformat(args.from_date.strip())
        except ValueError:
            log.error("Invalid --from: %s", args.from_date); return 2
    else:
        from_week_end = DEFAULT_FROM_WEEK_END

    candidates = iter_week_ends(from_week_end, this_week_end)
    log.info("Candidate week-end Fridays: %s",
             ", ".join(d.isoformat() for d in candidates) or "(none)")

    have = existing_weeks(docs_dir)
    log.info("Already present in docs/: %s",
             ", ".join(f"{y}-W{w:02d}" for (y, w) in sorted(have)) or "(none)")

    # Selection rules:
    #  1. PRESERVED weeks (W15, W16) are ALWAYS skipped — George's manuals.
    #  2. The "current" week (== this_week_end) is ALWAYS rebuilt — this
    #     Friday's run produces a fresh report for the week that just closed.
    #  3. Past weeks are built only if the HTML file is missing.
    this_week_key = (_iso_year(this_week_end), _iso_week(this_week_end))
    missing_dates: List[date] = []
    for d in candidates:
        key = (_iso_year(d), _iso_week(d))
        if key in PRESERVED_WEEKS:
            log.info("Preserving manual week %s-W%02d (skipping).", *key)
            continue
        if key == this_week_key:
            log.info("Current week %s-W%02d — will rebuild (fresh Friday run).", *key)
            missing_dates.append(d)
            continue
        if key not in have:
            missing_dates.append(d)

    if not missing_dates:
        log.info("No missing weeks. Nothing to build.")
        return 0

    log.info("Building %d missing week(s): %s",
             len(missing_dates),
             ", ".join(f"{_iso_year(d)}-W{_iso_week(d):02d}" for d in missing_dates))

    failures = 0
    for week_end in missing_dates:
        rc = run_builder(builder, week_end, xlsx, index_html)
        if rc != 0:
            failures += 1
            log.error("Builder failed for week ending %s (rc=%d). Continuing.",
                      week_end.isoformat(), rc)

    if failures:
        log.error("%d week(s) failed to build.", failures)
        return 1

    log.info("All missing weeks built successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
