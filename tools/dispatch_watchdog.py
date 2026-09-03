#!/usr/bin/env python3
"""Did the Apps Script scheduler actually dispatch what it is supposed to?

WHY THIS EXISTS
---------------
Every collecting, reviewing and publishing workflow in this repository is
`workflow_dispatch`-only. They are fired by FsisScheduler.gs, a Google Apps
Script that lives outside version control, has no test, no review and no
alarm. When it stops dispatching a workflow, GitHub shows nothing: no run
fails, because no run happens. The Actions tab is green and empty.

That has already cost this project twice. Collection fell to 33% of its
trailing median in the week of 2026-08-21 and the first sign was a human
reading the weekly report. recall-confirm-agent — the only step in the
review chain that can publish a row — went 400 commits without a single run
because it had never been added to the script at all.

The fix is not to give every workflow its own GitHub cron as well. That
double-fires each slot and puts two jobs in the same writer lane. The fix is
one dispatcher and one independent auditor: this script, run from GitHub's
own cron, reads the commit record and reports any workflow that has gone
quiet past the age it should never exceed.

WHAT IT CAN AND CANNOT SEE
--------------------------
It reads `git log`, not the Actions API, so it needs no token and works from
any clone. The consequence is that it can only see workflows that COMMIT.
A workflow that ran and found nothing new commits nothing and looks
identical to one that never ran — so a quiet workflow is reported as
OVERDUE, not as FAILED, and the thresholds below are deliberately loose
enough that ordinary quiet days do not trip them.

Silence is still the signal worth having. A daily scraper that has produced
no commit in four days has either stopped running or stopped finding
anything, and both of those are worth a human's attention.

    python -m tools.dispatch_watchdog                 # report, exit 1 if overdue
    python -m tools.dispatch_watchdog --days 30       # widen the log window
    python -m tools.dispatch_watchdog --warn-only     # never exit non-zero
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (label, commit-message regex, max hours of silence before it is overdue)
#
# Thresholds are set from the observed cadence with generous headroom, so a
# slow weekend does not page anyone. They are the answer to "at what point
# is silence definitely wrong?", not "when is the next run due?".
WATCHED: tuple[tuple[str, str, int], ...] = (
    # Collection — the scrape fleet. Five dispatches a day, all committing
    # through pipeline.run_all under one message.
    ("scrape fleet (run_all)",        r"^FSIS daily update ",                     30),
    ("daily recall search",           r"^Daily recall search ",                   48),
    ("news feed",                     r"^auto news update",                       12),
    ("url guardian",                  r"^URL guardian @",                         12),

    # Gate / review / promote — the path from Pending to Recalls.
    ("gemini url-gate",               r"^FSIS Gemini URL-gate ",                  48),
    ("merge-master",                  r"^chore\(data\): hourly Pending",          36),
    ("recall review agent",           r"^Recall review agent:",                   72),

    # Publication surfaces.
    ("public xlsx (23:30 slot)",      r"^Rebuild public xlsx ",                   48),

    # Analytical schema. Nothing else writes these fourteen columns, so if
    # this goes quiet the statistical corpus silently stops growing while
    # the register keeps growing — the exact failure this watchdog was
    # extended for on 2026-08-28.
    ("analytical schema sweep",       r"^Analytical schema sweep ",               48),

    # Weekly / monthly cadence.
    ("weekly report",                 r"^AFTS Weekly Report build ",             8 * 24),
    ("weekly review wipe",            r"^Weekly_Review \+ Weekly_Rejected wipe",  9 * 24),

    # Monthly report. Added 2026-08-28 after the audit found that
    # afts-monthly-report.yml has produced no commit since May, across three
    # Day-1 dispatches (June, July, August) — while
    # scraper-health-monthly.yml, dispatched from the SAME inline block one
    # hour earlier, committed on all three. The build itself is not dead:
    # monthly-index.json carries July. What is stale is
    # docs/data/monthly-summary-latest.json, still pinned at 2026-M05.
    ("monthly report",                r"^AFTS Monthly Report build ",           34 * 24),
    ("monthly updates check",         r"^Monthly updates check ",               34 * 24),

    # Live signals tab. signal-board.yml scans the latest complete ISO week
    # every Monday and the dashboard renders docs/data/signals-board.json.
    # Added 2026-09-02 with the tab itself; before that the detector existed
    # but nothing dispatched it and no output file had ever been written.
    ("signal board",                  r"^Signal board ",                         9 * 24),
    # The fourth agent: novelty / outbreak / news / alarm hints, Monday after
    # the board. Added 2026-09-03 with the Cyclospora case as its reason.
    ("signal review agent",           r"^Signal review ",                        9 * 24),

    # The AI synthesis that fills ai_lead_paragraph. It has NEVER committed:
    # both synthesis writers carry a comment telling the operator to add them
    # to FsisScheduler.gs, and neither was ever added, so the Intelligence
    # Summary block has been empty in every weekly and monthly email.
    #
    # Note the writer swallows a failed commit and returns 0 — it logs
    # "synthesis written locally" and the run goes green with nothing
    # pushed. A green run is therefore not evidence; this line is.
    ("ai synthesis",                  r"^AI synthesis \(",                       8 * 24),
)


def _log(days: int) -> list[tuple[datetime, str]]:
    out = subprocess.run(
        ["git", "log", f"--since={days} days ago", "--pretty=%cI\x1f%s"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout
    rows = []
    for line in out.splitlines():
        stamp, _, subject = line.partition("\x1f")
        try:
            rows.append((datetime.fromisoformat(stamp), subject))
        except ValueError:
            continue
    return rows


def check(days: int = 21, now: datetime | None = None) -> list[dict]:
    now = now or datetime.now(timezone.utc)
    commits = _log(days)
    results = []
    for label, pattern, max_hours in WATCHED:
        rx = re.compile(pattern)
        seen = [c for c in commits if rx.search(c[1])]
        if seen:
            last = max(c[0] for c in seen)
            age = (now - last).total_seconds() / 3600.0
            results.append({"label": label, "count": len(seen), "age_hours": age,
                            "max_hours": max_hours, "last": last.isoformat(),
                            "overdue": age > max_hours})
        else:
            results.append({"label": label, "count": 0, "age_hours": None,
                            "max_hours": max_hours, "last": None,
                            "overdue": True})
    return results


def report(results: list[dict]) -> int:
    overdue = [r for r in results if r["overdue"]]
    print(f"Dispatch watchdog — {len(results)} workflows watched, "
          f"{len(overdue)} overdue\n")
    print(f"  {'workflow':30} {'commits':>8} {'last seen':>12} {'limit':>8}")
    print("  " + "-" * 62)
    for r in sorted(results, key=lambda x: (not x["overdue"], x["label"])):
        age = "never" if r["age_hours"] is None else f"{r['age_hours']:.0f}h ago"
        mark = "OVERDUE" if r["overdue"] else "ok"
        print(f"  {r['label']:30} {r['count']:>8} {age:>12} "
              f"{r['max_hours']:>6}h  {mark}")
    if overdue:
        print("\nOverdue means the Apps Script scheduler has not dispatched it, or")
        print("it ran and found nothing. Check FsisScheduler.gs first: a workflow")
        print("renamed or added in the repo but not added to that script never")
        print("runs, and nothing else in GitHub will say so.\n")
        for r in overdue:
            print(f"  * {r['label']}: no commit in {r['max_hours']}h")
    return 1 if overdue else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=21,
                    help="how far back to read the commit log (default 21)")
    ap.add_argument("--warn-only", action="store_true",
                    help="always exit 0; print the report only")
    a = ap.parse_args(argv)
    rc = report(check(days=a.days))
    return 0 if a.warn_only else rc


if __name__ == "__main__":
    sys.exit(main())
