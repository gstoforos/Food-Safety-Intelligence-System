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
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "weekly-rejected-latest.json"


def _archive_path(week_end: str) -> Path:
    return JSON.parent / f"weekly-rejected-{week_end}.json"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--yes", action="store_true",
                   help="Skip interactive confirmation")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would change; do not write")
    p.add_argument("--all", action="store_true",
                   help="Wipe every data row regardless of Week_Added. "
                        "Default keeps rows stamped for a FUTURE review "
                        "Sunday — a rejection landing between the 17:00 "
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
    from pipeline.weekly_rejected_capture import (  # noqa: E402
        SHEET_NAME, SHEET_COLS, export_week_slice, review_day_just_closed,
    )

    week_end = args.week_end or review_day_just_closed().isoformat()
    print(f"Closed review window: week ending {week_end}")

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

    # ── WHICH ROWS CLOSE, WHICH ROLL OVER (audit 2026-09-21) ───────────
    # The workflow comment says rows arriving 17:00→17:30 "land in NEXT
    # Sunday's window". The code did not do that: delete_rows(2, max_row-1)
    # took every row whatever its Week_Added stamp. Here the row still
    # reaches the permanent Rejected archive, so nothing was destroyed —
    # but it never appeared in the email it was stamped for, which is the
    # one thing the rolling sheet exists to guarantee.
    _hdr_now = [str(c.value or "") for c in ws[1]]
    try:
        _we_idx = _hdr_now.index("Week_Added")
    except ValueError:
        _we_idx = -1

    def _stamp(row_idx: int) -> str:
        if _we_idx < 0:
            return week_end
        v = ws.cell(row=row_idx, column=_we_idx + 1).value
        if v is None:
            return ""
        return v.isoformat()[:10] if hasattr(v, "isoformat") else str(v)[:10]

    # A row rolls over only when stamped for a LATER Sunday. A missing or
    # unparseable stamp closes — otherwise it would be immortal and show
    # up in every future email.
    closing, rolling = [], []
    for r in range(2, ws.max_row + 1):
        s = _stamp(r)
        if not args.all and _we_idx >= 0 and s > week_end:
            rolling.append(r)
        else:
            closing.append(r)

    print(f"Weekly_Rejected has {n_data_rows} data row(s): "
          f"{len(closing)} closing, {len(rolling)} rolling over.")
    for r in rolling[:10]:
        print(f"    keep row {r}: Week_Added={_stamp(r)}")

    if args.dry_run:
        print(f"[dry-run] Would archive the {week_end} slice to "
              f"{_archive_path(week_end).name}, leave {JSON.name} holding "
              f"it, and move+clear {len(closing)} row(s).")
        return 0

    if not args.yes:
        resp = input(f"Clear {len(closing)} row(s) from Weekly_Rejected? "
                     f"[y/N] ").strip().lower()
        if resp not in ("y", "yes"):
            print("Aborted.")
            return 2

    # ── ARCHIVE THE JSON FIRST — before a single row moves ─────────────
    # The old code called export_week_slice() AFTER the wipe, over the
    # sheet it had just emptied, writing row_count: 0 over the real
    # capture. Same defect, same file pair, as wipe_weekly_review.py —
    # see that module's docstring for the measured evidence.
    try:
        payload = export_week_slice(xlsx_path=XLSX,
                                    json_path=_archive_path(week_end),
                                    week_end=week_end)
        print(f"  ✓ Archived {_archive_path(week_end).name}: "
              f"{payload['row_count']} rows for week ending "
              f"{payload['week_end']}")
    except Exception as e:                                   # noqa: BLE001
        print(f"  ERROR: could not archive the closed week: {e}",
              file=sys.stderr)
        print("  Refusing to wipe.", file=sys.stderr)
        return 1

    try:
        held = dict(payload)
        held["sheet_wiped_utc"] = (datetime.now(timezone.utc)
                                   .isoformat(timespec="seconds"))
        held["note"] = (
            "Weekly_Rejected was cleared for this window after the Sunday "
            "17:00 Athens email. These are the rejections that email "
            "covered, kept so a re-send is correct rather than empty. The "
            "next rejection replaces this file.")
        JSON.parent.mkdir(parents=True, exist_ok=True)
        JSON.write_text(
            json.dumps(held, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8")
        print(f"  ✓ {JSON.name} holds the closed week "
              f"({held['row_count']} rows, week_end={held['week_end']})")
    except Exception as e:                                   # noqa: BLE001
        print(f"  WARN: could not stamp {JSON.name} ({e}); copying the "
              f"archive verbatim.", file=sys.stderr)
        try:
            shutil.copyfile(_archive_path(week_end), JSON)
        except OSError as e2:
            print(f"  WARN: copy failed too: {e2}", file=sys.stderr)

    # ── MOVE, DON'T DELETE (audit 2026-08-14) ──────────────────────────
    # This step used to delete the rows outright. The workflow that calls
    # it has always described a permanent archive —
    #     "Does NOT touch Rejected sheet (permanent audit archive —
    #      separate from the rolling Weekly_Rejected)"
    # — but no "Rejected" sheet has ever existed in recalls.xlsx. The
    # sheets are Recalls, Pending, Weekly_Rejected, NEWS. The archive the
    # design assumed was doing the remembering was never there, so every
    # Thursday at 17:30 Athens the reasons were destroyed.
    #
    # MEASURED DAMAGE: Weekly_Rejected went from 263 rows to 12 in one
    # wipe. Two FSANZ allergen rows removed on 2026-07-29 for a documented
    # reason lost their archive record entirely, which is what turned
    # tests/test_hazard_class_guard.py::test_they_were_archived_not_deleted
    # red on main — the only reason anyone noticed. They had to be
    # reconstructed by hand on 2026-08-14.
    #
    # It also erases the re-promotion memory: load_rejected_urls() reads
    # this sheet, so after a wipe the pipeline forgets which URLs a human
    # already turned down and re-ingests them. The USDA jalapeno public
    # health alert came back exactly this way after being archived on
    # 2026-08-13.
    #
    # The operator rule is standing and explicit: removed rows are
    # archived with a reason, NEVER silently deleted. A scheduled job is
    # not an exception to it.
    #
    # Rows now MOVE to a permanent "Rejected" sheet first. The rolling
    # Thu->Thu window still resets, which is the entire point of the wipe,
    # but the reasons survive. Dedup on (URL, Date) keeps it idempotent.
    if ws.max_row >= 2:
        archive = (wb["Rejected"] if "Rejected" in wb.sheetnames
                   else wb.create_sheet("Rejected"))
        hdr = [c.value for c in ws[1]]
        if archive.max_row < 1 or all(c.value in (None, "")
                                      for c in archive[1]):
            for i, h in enumerate(hdr, 1):
                archive.cell(row=1, column=i, value=h)
            arch_hdr = list(hdr)
        else:
            arch_hdr = [c.value for c in archive[1]]

        # KEY ON CONTENT WHEN THERE IS NO URL.
        # A plain (URL, Date) key collapses every URL-less row sharing a
        # date into one, and the URL-guardian BLANKS the URL of any row
        # whose link errors — so those rows are exactly the ones that
        # collide. Measured on the first run of this fix: 68 rows in,
        # 55 archived, 13 silently dropped. That is the same failure this
        # whole change exists to stop, reproduced inside the fix for it.
        # (weekly_rejected_capture._content_key solves it the same way.)
        def _key(row_map):
            u = str(row_map.get("URL") or "").strip().lower()
            d = str(row_map.get("Date") or "")[:10]
            if u:
                return (u, d)
            def f(name, n=None):
                v = row_map.get(name)
                if v in (None, ""):
                    return ""
                v = str(v).strip().lower()
                return v[:n] if n else v
            return ("", d, f("Source"), f("Company", 60), f("Product", 60),
                    f("Pathogen", 40), f("Reason", 60))

        seen = set()
        for t in archive.iter_rows(min_row=2, values_only=True):
            if t and not all(v in (None, "") for v in t):
                seen.add(_key(dict(zip(arch_hdr, t))))

        moved = already = 0
        # Only the CLOSING rows are archived and removed. A row stamped
        # for a later Sunday stays in the rolling sheet and is archived
        # when its own window closes.
        for r_idx in closing:
            t = tuple(c.value for c in ws[r_idx])
            if all(v in (None, "") for v in t):
                continue
            row_map = dict(zip(hdr, t))
            key = _key(row_map)
            if key in seen:
                already += 1
                continue
            seen.add(key)
            archive.append([row_map.get(h, "") for h in arch_hdr])
            moved += 1

        print(f"  ✓ Archived {moved} row(s) to the permanent 'Rejected' "
              f"sheet ({already} already present); archive now holds "
              f"{archive.max_row - 1}.")

        # Bottom-up, so earlier indices stay valid as rows shift up.
        for r_idx in sorted(closing, reverse=True):
            ws.delete_rows(r_idx, 1)

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
    print(f"  ✓ Cleared {len(closing)} row(s) from Weekly_Rejected; "
          f"{len(rolling)} kept for a later Sunday.")

    # NO JSON regenerate here. It used to run at this point, over the
    # sheet just emptied, and wrote row_count: 0 across the capture the
    # email was built from. The file was written BEFORE the wipe, above,
    # and holds the closed week deliberately.

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
