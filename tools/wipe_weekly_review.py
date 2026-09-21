"""tools/wipe_weekly_review.py — close the Weekly_Review window after the
Sunday review email goes out.

WHY THIS EXISTS (audit 2026-05-08)
==================================
Every Pending → Recalls promotion mirrors into Weekly_Review, stamped with
the Sunday it will be reviewed on (Sun 17:00 Athens cutoff). The Apps
Script `sendSundayManualReview` mailer reads
`docs/data/weekly-review-latest.json` and sends the operator-only review
email at Sunday 17:00 Athens. After that email goes out the window has to
close so the new Sun→Sun window starts clean.

This script closes it. It runs from `.github/workflows/weekly-review-wipe.yml`
at Sunday 17:30 Athens — 30 minutes after the mailer.

TWO DEFECTS FIXED HERE (audit 2026-09-21)
=========================================
**1. It blanked the file the mailer reads.**

Step 4 of the old docstring read:

    "Regenerates docs/data/weekly-review-latest.json (it'll be empty,
     reflecting the new state — protection against stale data being read
     between wipe time and the next promotion)."

and the code did exactly that: `export_week_slice()` over a sheet it had
just emptied, writing `row_count: 0, rows: []` over the real capture.

That is the live cause of "0 recalls added". On main,
`weekly-review-latest.json` carries

    week_end: 2026-09-20, row_count: 0,
    generated_utc: 2026-09-20T14:37:10+00:00

14:37 UTC is **17:37 Athens** — seven minutes after the wipe fired. The
correct 15-row capture written at 14:17 UTC was overwritten by an empty
one, and the JSON then sat untouched for the rest of the week, which is
where "JSON is 335.5h old" in the Sunday email comes from.

The stale-read the old comment was defending against costs one duplicate
email if a human re-runs the mailer by hand. Blanking the file costs an
empty email every single week, automatically. The defence was more
expensive than the attack.

So: the closed week's slice is written to a dated archive
`docs/data/weekly-review-<week_end>.json`, and `weekly-review-latest.json`
is left holding **that same closed week**, stamped `sheet_wiped_utc`. A
re-run of the mailer now re-sends a true email rather than an empty one,
and `week_end` still tells any reader which window it describes.

**2. It deleted rows belonging to next week.**

The workflow's own comment says:

    "Wiping at 17:30 means rows arriving 17:00→17:30 land in NEXT
     Sunday's window, which is the desired behavior per the cutoff math."

The code did not do that. `ws.delete_rows(2, ws.max_row - 1)` removed
every data row regardless of its `Week_Added` stamp, so a promotion
landing in that 30-minute gap — `review_day_for()` has already rolled
over to the following Sunday by then — was stamped for next week and
then deleted before next week arrived. Silent loss of a published row
from the review queue.

Rows stamped for a FUTURE review Sunday are now kept. `--all` restores
the old unconditional behaviour for a manual reset.

WHAT IT DOES
============
1. Loads `docs/data/recalls.xlsx`
2. Works out the review Sunday that has just closed
   (`review_day_just_closed()`, not `review_day_for()` — see that
   function for the off-by-one-week this pair fixes)
3. Archives that week's slice to `docs/data/weekly-review-<week_end>.json`
   and leaves `weekly-review-latest.json` holding the same payload
4. Clears the closed week's rows from Weekly_Review (header preserved,
   future-stamped rows preserved)
5. Saves the xlsx

The Recalls sheet is NOT touched. Promoted rows persist there forever.
The Rejected sheet is NOT touched. The audit archive persists forever.
Only the closed Weekly_Review window gets emptied.

USAGE
=====
    python -m tools.wipe_weekly_review                # interactive (y/N)
    python -m tools.wipe_weekly_review --yes          # non-interactive
    python -m tools.wipe_weekly_review --dry-run      # report only
    python -m tools.wipe_weekly_review --all --yes    # every row, any week
    python -m tools.wipe_weekly_review --week-end 2026-09-20 --yes

EXIT CODES
==========
    0 = wipe completed (or nothing to wipe)
    1 = error (missing xlsx, can't write, etc.)
    2 = aborted by user
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "weekly-review-latest.json"


def _archive_path(week_end: str) -> Path:
    return JSON.parent / f"weekly-review-{week_end}.json"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--yes", action="store_true",
                   help="Skip interactive confirmation")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would change; do not write")
    p.add_argument("--all", action="store_true",
                   help="Wipe every data row regardless of Week_Added. "
                        "Default keeps rows stamped for a FUTURE review "
                        "Sunday — a promotion landing between the 17:00 "
                        "mailer and this 17:30 wipe belongs to next week.")
    p.add_argument("--week-end", default=None, metavar="YYYY-MM-DD",
                   help="Override the closed review Sunday. Default is "
                        "review_day_just_closed().")
    args = p.parse_args()

    if not XLSX.exists():
        print(f"ERROR: {XLSX} does not exist", file=sys.stderr)
        return 1

    # Lazy imports — keeps script startup fast
    from openpyxl import load_workbook  # noqa: E402
    from pipeline.weekly_review_capture import (  # noqa: E402
        SHEET_NAME, SHEET_COLS, export_week_slice, review_day_just_closed,
    )

    week_end = args.week_end or review_day_just_closed().isoformat()
    print(f"Closed review window: week ending {week_end}")

    wb = load_workbook(XLSX)
    if SHEET_NAME not in wb.sheetnames:
        print(f"Weekly_Review sheet does not exist in {XLSX}. "
              f"Nothing to wipe.")
        return 0

    ws = wb[SHEET_NAME]
    n_data_rows = max(0, ws.max_row - 1)  # subtract header

    if n_data_rows == 0:
        print("Weekly_Review sheet already empty (only header present).")
        return 0

    # ---------------------------------------------------------------
    # Which rows close, which roll over
    # ---------------------------------------------------------------
    headers = [str(c.value or "") for c in ws[1]]
    try:
        we_idx = headers.index("Week_Added")
    except ValueError:
        we_idx = -1
        if not args.all:
            print("  WARN: no Week_Added column — cannot tell this week's "
                  "rows from next week's. Treating every row as closed.",
                  file=sys.stderr)

    def _stamp(row_idx: int) -> str:
        if we_idx < 0:
            return week_end
        v = ws.cell(row=row_idx, column=we_idx + 1).value
        if v is None:
            return ""
        if hasattr(v, "isoformat"):
            return v.isoformat()[:10]
        return str(v)[:10]

    # A row rolls over only when it is stamped for a LATER Sunday. A row
    # with a missing or unparseable stamp closes — leaving it would make
    # it immortal and it would appear in every future email.
    closing, rolling = [], []
    for r in range(2, ws.max_row + 1):
        s = _stamp(r)
        if not args.all and we_idx >= 0 and s > week_end:
            rolling.append(r)
        else:
            closing.append(r)

    print(f"Weekly_Review has {n_data_rows} data row(s): "
          f"{len(closing)} closing, {len(rolling)} rolling over to a later "
          f"Sunday.")
    if rolling:
        # This is the row class the old code destroyed. Name them.
        for r in rolling[:10]:
            print(f"    keep row {r}: Week_Added={_stamp(r)}")

    if args.dry_run:
        print(f"[dry-run] Would archive the {week_end} slice to "
              f"{_archive_path(week_end).name}, leave {JSON.name} holding "
              f"it, and clear {len(closing)} row(s).")
        return 0

    if not args.yes:
        resp = input(f"Clear {len(closing)} row(s) from Weekly_Review? [y/N] "
                     ).strip().lower()
        if resp not in ("y", "yes"):
            print("Aborted.")
            return 2

    # ---------------------------------------------------------------
    # 1. ARCHIVE FIRST — before a single row is deleted.
    #
    # Order matters and it is the whole fix. The old code exported AFTER
    # the wipe, so it exported an empty sheet over the real capture.
    # ---------------------------------------------------------------
    archived = None
    try:
        payload = export_week_slice(xlsx_path=XLSX,
                                    json_path=_archive_path(week_end),
                                    week_end=week_end)
        archived = _archive_path(week_end)
        print(f"  ✓ Archived {archived.name}: {payload['row_count']} rows "
              f"(Tier1={payload['tier1_count']}, "
              f"Outbreak={payload['outbreak_count']}) "
              f"for week ending {payload['week_end']}")
    except Exception as e:                                   # noqa: BLE001
        print(f"  ERROR: could not archive the closed week: {e}",
              file=sys.stderr)
        print("  Refusing to wipe — the rows would be unrecoverable.",
              file=sys.stderr)
        return 1

    # ---------------------------------------------------------------
    # 2. weekly-review-latest.json keeps the week that was just mailed.
    #
    # NOT an empty slice of the sheet we are about to clear. See the
    # module docstring: emptying it here is what made every Sunday email
    # say "0 recalls added" and what left the file 335 hours stale.
    #
    # The `sheet_wiped_utc` stamp is how a reader tells "this is the
    # closed week, held for the record" from "this is the window now
    # filling". The next promotion overwrites the file via
    # record_promotions() and the stamp goes away with it.
    # ---------------------------------------------------------------
    try:
        held = dict(payload)
        held["sheet_wiped_utc"] = (datetime.now(timezone.utc)
                                   .isoformat(timespec="seconds"))
        held["note"] = (
            "Weekly_Review was cleared for this window after the Sunday "
            "17:00 Athens email. These are the rows that email covered, "
            "kept so a re-send is correct rather than empty. week_end "
            "identifies the window; the next promotion replaces this file.")
        JSON.parent.mkdir(parents=True, exist_ok=True)
        JSON.write_text(
            json.dumps(held, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8")
        print(f"  ✓ {JSON.name} holds the closed week "
              f"({held['row_count']} rows, week_end={held['week_end']})")
    except Exception as e:                                   # noqa: BLE001
        # Fall back to a straight copy of the archive rather than leaving
        # whatever was there before.
        print(f"  WARN: could not stamp {JSON.name} ({e}); copying the "
              f"archive verbatim.", file=sys.stderr)
        try:
            shutil.copyfile(archived, JSON)
        except OSError as e2:
            print(f"  WARN: copy failed too: {e2}", file=sys.stderr)

    # ---------------------------------------------------------------
    # 3. Clear the closed rows. Delete bottom-up so earlier indices stay
    #    valid as rows shift up under us.
    # ---------------------------------------------------------------
    for r in sorted(closing, reverse=True):
        ws.delete_rows(r, 1)

    # Defensive: if header is somehow missing or wrong, restore it.
    expected_headers = list(SHEET_COLS)
    actual_headers = [c.value for c in ws[1]]
    if actual_headers[:len(expected_headers)] != expected_headers:
        print("  WARN: header row was missing or different — restoring.")
        for col_idx in range(1, ws.max_column + 1):
            ws.cell(row=1, column=col_idx).value = None
        for i, h in enumerate(expected_headers, 1):
            ws.cell(row=1, column=i, value=h)

    XLSX.parent.mkdir(parents=True, exist_ok=True)
    wb.save(XLSX)
    print(f"  ✓ Cleared {len(closing)} row(s) from Weekly_Review; "
          f"{len(rolling)} kept for a later Sunday.")

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
