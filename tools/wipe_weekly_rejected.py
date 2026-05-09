"""tools/wipe_weekly_rejected.py — empty the Weekly_Rejected sheet after
the Thursday review email goes out.

WHY THIS EXISTS (audit 2026-05-09)
==================================
Architectural twin of tools/wipe_weekly_review.py. Per operator spec:
every claude-check / openrouter-check rejection mirrors into
Weekly_Rejected (Thu 17:00 Athens cutoff window, just like
Weekly_Review). The Apps Script Thursday-17:00 mailer reads
docs/data/weekly-rejected-latest.json and includes the rejection list
alongside the promotions in the operator-only review email.

After that email goes out, the Weekly_Rejected sheet must be empty so
the new Thu→Thu window starts fresh — same lifecycle as Weekly_Review.

This script does the wipe. It runs from the existing
.github/workflows/weekly-review-wipe.yml workflow at Thursday 17:30
Athens — 30 minutes after the email send to make sure the mailer has
finished. Adding to the existing workflow rather than creating a new
one keeps the wipe semantics atomic: both review sheets reset together.

WHAT IT DOES
============
1. Loads docs/data/recalls.xlsx
2. Clears all data rows from the Weekly_Rejected sheet (header preserved)
3. Saves the xlsx
4. Regenerates docs/data/weekly-rejected-latest.json (it'll be empty,
   reflecting the new state — protection against stale data being read
   between wipe time and the next rejection).

The Recalls sheet is NOT touched.
The Pending sheet is NOT touched.
The Weekly_Review sheet is NOT touched (separate wipe step).
Only Weekly_Rejected (the rolling Thu→Thu rejection queue) gets emptied.

USAGE
=====
    python -m tools.wipe_weekly_rejected                # interactive (y/N)
    python -m tools.wipe_weekly_rejected --yes          # non-interactive
    python -m tools.wipe_weekly_rejected --dry-run      # report only

EXIT CODES
==========
    0 = wipe completed (or sheet already empty)
    1 = error (missing xlsx, can't write, etc.)
    2 = aborted by user
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "weekly-rejected-latest.json"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--yes", action="store_true",
                   help="Skip interactive confirmation")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would change; do not write")
    args = p.parse_args()

    if not XLSX.exists():
        print(f"ERROR: {XLSX} does not exist", file=sys.stderr)
        return 1

    # Lazy imports — keeps script startup fast
    from openpyxl import load_workbook  # noqa: E402
    from pipeline.weekly_rejected_capture import (  # noqa: E402
        SHEET_NAME, SHEET_COLS, export_week_slice,
    )

    wb = load_workbook(XLSX)
    if SHEET_NAME not in wb.sheetnames:
        print(f"Weekly_Rejected sheet does not exist in {XLSX}. "
              f"Nothing to wipe.")
        return 0

    ws = wb[SHEET_NAME]
    n_data_rows = max(0, ws.max_row - 1)  # subtract header

    if n_data_rows == 0:
        print(f"Weekly_Rejected sheet already empty (only header present).")
        return 0

    print(f"Weekly_Rejected currently has {n_data_rows} data row(s).")

    if args.dry_run:
        print(f"[dry-run] Would wipe {n_data_rows} row(s), keeping header.")
        return 0

    if not args.yes:
        resp = input(f"Wipe {n_data_rows} row(s) from Weekly_Rejected? [y/N] "
                     ).strip().lower()
        if resp not in ("y", "yes"):
            print("Aborted.")
            return 2

    # Wipe data rows: delete every row from row 2 to the end.
    # openpyxl's delete_rows(idx, amount) takes the FIRST row index and
    # number of rows. Row 1 = header, so we delete rows 2..max_row.
    if ws.max_row >= 2:
        ws.delete_rows(2, ws.max_row - 1)

    # Defensive: if header is somehow missing or wrong, restore it.
    expected_headers = list(SHEET_COLS)
    actual_headers = [c.value for c in ws[1]]
    if actual_headers[:len(expected_headers)] != expected_headers:
        print("  WARN: header row was missing or different — restoring.")
        # Clear row 1 and rewrite
        for col_idx in range(1, ws.max_column + 1):
            ws.cell(row=1, column=col_idx).value = None
        for i, h in enumerate(expected_headers, 1):
            ws.cell(row=1, column=i, value=h)

    XLSX.parent.mkdir(parents=True, exist_ok=True)
    wb.save(XLSX)
    print(f"  ✓ Wiped {n_data_rows} row(s) from Weekly_Rejected.")

    # Regenerate the JSON snapshot to reflect the empty state.
    # This protects against the Apps Script mailer reading stale rows
    # if it happens to fire between this wipe and the next rejection.
    try:
        result = export_week_slice(xlsx_path=XLSX, json_path=JSON)
        print(f"  ✓ Regenerated {JSON.name}: {result['row_count']} rows "
              f"for week ending {result['week_end']}")
    except Exception as e:
        print(f"  WARN: JSON regenerate failed: {e}", file=sys.stderr)
        # Don't fail the script — the xlsx wipe is the canonical action.
        # Apps Script can re-derive on next email.

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
