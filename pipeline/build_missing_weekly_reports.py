"""
AFTS FSIS — Weekly report builder, rolling-window edition.

Rebuilds the most recent N=VISIBLE_WEEKS Fridays against the current
Recalls sheet on every run (so retroactive promotions land in the right
week), and gap-fills any older missing Friday between --from and
--this-week-end.

Selection rules (rev 2026-04-30):
  1. PRESERVED_WEEKS (W15, W16 — George's manuals) are NEVER touched.
  2. The N most recent Fridays are ALWAYS rebuilt — same model as the
     daily briefs. Default N=4, matches dashboard RICH_LIMIT in
     docs/index.html. Override via FSIS_WEEKLY_VISIBLE env var.
  3. Older Fridays inside [--from, this_week_end - N weeks] are built
     ONLY if their HTML file is missing on disk (gap-fill).

Idempotent for case 3 (won't overwrite older weeks); intentionally
non-idempotent for case 2 (the whole point is to refresh the visible
window every Friday).

Usage (called from .github/workflows/afts-weekly-report.yml):

    python -m pipeline.build_missing_weekly_reports \\
        --this-week-end 2026-05-01 \\
        --from "" \\
        --xlsx docs/data/recalls.xlsx \\
        --docs-dir docs \\
        --builder docs/build_weekly_report_afts.py

If --from is omitted or empty, defaults to the week ending 2026-04-17.
"""
from __future__ import annotations

import argparse
import logging
import os
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


# ─── Which day anchors a week (2026-09-05) ────────────────────────────────
# Mirrors anchor_for() in docs/build_weekly_report_afts.py — keep the two in
# step. Weeks up to 2026-W35 were published under the Friday rule and are
# anchored on their Friday; from 2026-W36 a week is anchored on its Sunday
# (Mon-Sun data, shipped Monday). W36 is the ten-day bridge, 28 Aug - 6 Sep.
ISO_SWITCH = (2026, 36)


def anchor_for(d: date) -> date:
    """Anchor date of the ISO week containing d: Friday before ISO_SWITCH,
    Sunday from ISO_SWITCH on."""
    iso_year, iso_week, iso_dow = d.isocalendar()
    monday = d - timedelta(days=iso_dow - 1)
    if (iso_year, iso_week) < ISO_SWITCH:
        return monday + timedelta(days=4)
    return monday + timedelta(days=6)


def _prev_friday(d: date) -> date:
    """Return the Friday on or before d (Friday-rule weeks only)."""
    # Monday=0 .. Friday=4 .. Sunday=6
    days_since_friday = (d.weekday() - 4) % 7  # Friday->0, Sat->1, Sun->2, Mon->3, ...
    return d - timedelta(days=days_since_friday)


def iter_week_ends(start: date, end: date) -> List[date]:
    """Return the anchor date of every ISO week from the one containing
    `start` to the one containing `end`, inclusive — Fridays up to W35,
    Sundays from W36."""
    out = []
    cur = anchor_for(start)
    last = anchor_for(end)
    while cur <= last:
        out.append(cur)
        cur = anchor_for(cur + timedelta(days=7))
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
    ap.add_argument("--this-week-end", required=True,
                    help="Any date in the week being closed (YYYY-MM-DD); snapped to that week's anchor — its Sunday from 2026-W36, its Friday before")
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
        this_week_end = anchor_for(date.fromisoformat(args.this_week_end))
    except ValueError:
        log.error("Invalid --this-week-end: %s", args.this_week_end); return 2
    if this_week_end.isoformat() != args.this_week_end:
        log.info("--this-week-end %s snapped to its week's anchor %s",
                 args.this_week_end, this_week_end)

    if args.from_date.strip():
        try:
            from_week_end = date.fromisoformat(args.from_date.strip())
        except ValueError:
            log.error("Invalid --from: %s", args.from_date); return 2
    else:
        from_week_end = DEFAULT_FROM_WEEK_END

    candidates = iter_week_ends(from_week_end, this_week_end)
    log.info("Candidate week anchors: %s",
             ", ".join(d.isoformat() for d in candidates) or "(none)")

    have = existing_weeks(docs_dir)
    log.info("Already present in docs/: %s",
             ", ".join(f"{y}-W{w:02d}" for (y, w) in sorted(have)) or "(none)")

    # Selection rules (rev 2026-04-30 — match dashboard RICH_LIMIT):
    #  1. PRESERVED weeks (W15, W16) are ALWAYS skipped — George's manuals.
    #  2. The N=VISIBLE_WEEKS most recent Fridays (this_week_end and the
    #     3 prior) are ALWAYS rebuilt against the current Recalls sheet —
    #     same rule as the daily briefs (data accumulates retroactively).
    #  3. Any older Friday in [from_week_end, this_week_end - N weeks]
    #     range is built ONLY if its HTML file is missing (gap-fill).
    #
    # VISIBLE_WEEKS=4 mirrors the dashboard's RICH_LIMIT in docs/index.html.
    # If you change one, change the other.
    VISIBLE_WEEKS = int(os.getenv("FSIS_WEEKLY_VISIBLE", "4"))
    rebuild_window = {
        anchor_for(this_week_end - timedelta(days=7 * i))
        for i in range(VISIBLE_WEEKS)
    }
    log.info("Rebuild window (most recent %d weeks): %s",
             VISIBLE_WEEKS,
             ", ".join(d.isoformat() for d in sorted(rebuild_window, reverse=True)))

    this_week_key = (_iso_year(this_week_end), _iso_week(this_week_end))
    missing_dates: List[date] = []
    for d in candidates:
        key = (_iso_year(d), _iso_week(d))
        if key in PRESERVED_WEEKS:
            log.info("Preserving manual week %s-W%02d (skipping).", *key)
            continue
        if d in rebuild_window:
            # In the visible window → always rebuild.
            log.info("Visible week %s-W%02d (%s) — rebuilding against current Recalls sheet.",
                     *key, d.isoformat())
            missing_dates.append(d)
            continue
        if key not in have:
            # Older than visible window AND HTML missing → gap-fill.
            log.info("Older week %s-W%02d missing on disk — gap-filling.", *key)
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
