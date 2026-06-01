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

ROBUSTNESS (added 2026-06-01 after a workflow silently no-op'd on
build_missing for M05 — gap-fill returned rc=0 yet produced zero files,
and the downstream commit step exited 0 with "No monthly-report changes
to commit." This left subscribers with no May report on June 1):

  1. subprocess.run now CAPTURES stdout+stderr from the builder, mirrors
     to our log line-by-line, and surfaces stderr at ERROR level on a
     non-zero rc. Previously capture_output=False meant any builder
     traceback could vanish into the runner's stdio buffer with no
     trace in the workflow log.

  2. After each builder invocation, we VERIFY the three expected output
     artefacts exist on disk:
         <docs>/<YYYY>-M<MM>.html              (subscriber report)
         <docs>/<YYYY>-M<MM>-all.html          (all-month companion)
         <docs>/data/monthly-summary-latest.json   (mailer payload)
     If rc=0 but any of these is missing/empty, we treat that month as
     a FAILURE and accumulate it into the final returncode. This catches
     the "silent crash mid-write" failure mode.

  3. On the 1st of any month (UTC), the previous month's report MUST
     exist after the run. If it doesn't — even because gap-fill thought
     it was already covered — we fail loudly with rc=2 so the workflow
     turns red and the operator is notified. This guards against the
     index file being mutated externally between runs.
"""
from __future__ import annotations

import argparse
import calendar
import json
import logging
import subprocess
import sys
from datetime import date, datetime, timezone
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
    return datetime.now(timezone.utc).date()


def _previous_month_ym(today: date) -> Tuple[int, int]:
    """Return (year, month) for the month immediately before `today`."""
    if today.month == 1:
        return (today.year - 1, 12)
    return (today.year, today.month - 1)


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


def expected_outputs(builder: Path, year: int, month: int) -> List[Path]:
    """Files the builder is contractually required to produce for a
    given month. The builder writes its outputs relative to its own
    directory (docs/), so we derive the docs-root from the builder path.
    """
    docs_root = builder.parent
    return [
        docs_root / f"{year}-M{month:02d}.html",
        docs_root / f"{year}-M{month:02d}-all.html",
        docs_root / "data" / "monthly-summary-latest.json",
    ]


def verify_outputs(builder: Path, year: int, month: int) -> List[Path]:
    """Return the list of expected output files that are MISSING or empty.
    Empty list means all outputs are present and non-zero-sized."""
    missing: List[Path] = []
    for p in expected_outputs(builder, year, month):
        if not p.exists() or p.stat().st_size == 0:
            missing.append(p)
    return missing


def run_builder(builder: Path, month_end: date, xlsx: Path) -> Tuple[int, str]:
    """Invoke the monthly builder as a subprocess. Captures stdout AND
    stderr (previously capture_output=False let tracebacks vanish) and
    mirrors them into our own log so the workflow run page shows what
    actually happened. Returns (returncode, stderr_text)."""
    cmd = [
        sys.executable, str(builder),
        "--month-end", month_end.isoformat(),
        "--xlsx", str(xlsx),
    ]
    log.info("Building: %s", " ".join(cmd))
    completed = subprocess.run(cmd, capture_output=True, text=True)

    # Always mirror stdout (useful even on success — shows row counts,
    # leading pathogen, output paths).
    for line in (completed.stdout or "").splitlines():
        log.info("[builder] %s", line)

    # On non-zero, surface stderr at ERROR level so the workflow page
    # shows the traceback. On success, downgrade stderr to WARNING (it
    # may contain warnings from openpyxl or jinja2 we still want visible).
    stderr_text = completed.stderr or ""
    if completed.returncode != 0:
        log.error("Builder exited with rc=%d. Stderr follows:", completed.returncode)
        for line in stderr_text.splitlines():
            log.error("[builder-err] %s", line)
    else:
        for line in stderr_text.splitlines():
            log.warning("[builder-err] %s", line)

    return completed.returncode, stderr_text


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
        log.info("No missing months according to monthly-index.json.")
    else:
        log.info("Building %d missing month(s): %s",
                 len(missing),
                 ", ".join(f"{y}-{m:02d}" for y, m in missing))

    failures = 0
    for (y, m) in missing:
        month_end = _last_day_of_month(y, m)
        rc, stderr_text = run_builder(builder, month_end, xlsx)

        if rc != 0:
            failures += 1
            log.error("Builder FAILED for %d-%02d (rc=%d). See [builder-err] lines above. "
                      "Continuing with next month (if any).", y, m, rc)
            continue

        # Builder said OK — but verify it actually wrote the expected files.
        # Previously a silent mid-write crash could leave rc=0 with no files;
        # downstream `git diff --staged --quiet` would then exit-0 the
        # workflow with no subscriber report produced.
        absent = verify_outputs(builder, y, m)
        if absent:
            failures += 1
            log.error("Builder returned rc=0 but expected outputs are MISSING for %d-%02d:",
                      y, m)
            for p in absent:
                log.error("    missing: %s", p)
            log.error("Treating as failure — the workflow run page will turn red.")
            continue

        log.info("Built %d-M%02d OK — all expected outputs verified on disk.", y, m)

    # END-OF-RUN SAFETY NET. On the 1st of any month (UTC), the previous
    # month's report MUST exist on disk by the time we exit. If it doesn't
    # — even because index claimed it was already covered, or because we
    # had failures above — escalate. This is the floor that prevents the
    # 2026-06-01 incident from recurring: a green workflow with no May
    # report committed.
    if today.day == 1:
        prev_y, prev_m = _previous_month_ym(today)
        required = expected_outputs(builder, prev_y, prev_m)
        absent = [p for p in required if not p.exists() or p.stat().st_size == 0]
        if absent:
            log.error("END-OF-RUN GUARD: today is the 1st of the month but the previous "
                      "month's outputs are missing/empty:")
            for p in absent:
                log.error("    missing: %s", p)
            log.error("This means the workflow ran but no subscriber report was "
                      "produced for %d-M%02d. Failing loudly so the operator notices.",
                      prev_y, prev_m)
            return 2

    if failures:
        log.error("%d month(s) failed to build. Exiting non-zero.", failures)
        return 1

    log.info("All requested months built and verified successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
