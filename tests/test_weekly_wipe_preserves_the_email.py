# -*- coding: utf-8 -*-
"""The Sunday wipe must not destroy the email it comes 30 minutes after.

THE INCIDENT (2026-09-20)
=========================
The operator review email said **"0 recalls added"** while the
Weekly_Review sheet held 15 rows, and reported the data file as
**335.5 hours old**.

Two separate faults, both in the same 30-minute window:

1. ``export_week_slice()`` filtered on ``review_day_for()`` — "which
   email will a row promoted RIGHT NOW appear in". That is the correct
   stamp to WRITE at promotion time and the wrong FILTER for the email
   being sent, because the mailer fires at Sunday 17:00 Athens, the exact
   instant ``review_day_for()`` rolls to next Sunday. The slice looked in
   a bucket that is empty by construction. Fixed by
   ``review_day_just_closed()``.

2. ``tools/wipe_weekly_review.py`` regenerated
   ``docs/data/weekly-review-latest.json`` AFTER emptying the sheet, so
   it wrote ``row_count: 0`` over the real capture — and then nothing
   touched the file until the next promotion. On main the evidence is in
   the file itself: ``generated_utc 2026-09-20T14:37:10+00:00``, which is
   17:37 Athens, seven minutes after the wipe, replacing a 15-row capture
   written at 14:17.

A third fault fell out while fixing the second: the wipe deleted every
row regardless of ``Week_Added``, including rows promoted in the
17:00→17:30 gap that the workflow's own comment promises will "land in
NEXT Sunday's window".
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

openpyxl = pytest.importorskip("openpyxl")

from pipeline.weekly_review_capture import (          # noqa: E402
    SHEET_COLS, SHEET_NAME, review_day_for, review_day_just_closed,
)

ATHENS = ZoneInfo("Europe/Athens")


def _athens(y, m, d, hh, mm=0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=ATHENS).astimezone(timezone.utc)


# --------------------------------------------------------------------------
# 1. the date math
# --------------------------------------------------------------------------

def test_the_mailer_hour_is_exactly_where_the_two_functions_diverge():
    """17:00 Athens Sunday: review_day_for rolls, just_closed does not."""
    t = _athens(2026, 9, 20, 17, 0)
    assert review_day_for(t) == date(2026, 9, 27), "the stamp rolls over"
    assert review_day_just_closed(t) == date(2026, 9, 20), (
        "the email being sent at this instant is about 2026-09-20; filtering "
        "on review_day_for() is the 0-recalls bug")


def test_before_the_cutoff_the_week_is_still_filling():
    t = _athens(2026, 9, 20, 16, 59)
    assert review_day_for(t) == date(2026, 9, 20)
    assert review_day_just_closed(t) == date(2026, 9, 13)


@pytest.mark.parametrize("dom,expected", [
    (21, date(2026, 9, 20)),   # Monday
    (23, date(2026, 9, 20)),   # Wednesday
    (26, date(2026, 9, 20)),   # Saturday
])
def test_midweek_points_at_the_sunday_just_past(dom, expected):
    assert review_day_just_closed(_athens(2026, 9, dom, 12)) == expected


def test_just_closed_is_always_a_sunday():
    t = _athens(2026, 9, 21, 9)
    for i in range(400):
        d = review_day_just_closed(t + timedelta(days=i))
        assert d.weekday() == 6, f"{d} is not a Sunday"


def test_the_rejected_twin_agrees_exactly():
    """Both sheets share one cutoff; two implementations must not drift."""
    from pipeline.weekly_rejected_capture import (
        review_day_just_closed as rj_closed)
    t = _athens(2026, 9, 21, 9)
    for i in range(0, 400, 7):
        for h in (0, 16, 17, 23):
            probe = t + timedelta(days=i, hours=h)
            assert rj_closed(probe) == review_day_just_closed(probe)


# --------------------------------------------------------------------------
# 2. the wipe, exercised end to end on a real workbook
# --------------------------------------------------------------------------

@pytest.fixture()
def workbook(tmp_path, monkeypatch):
    """A workbook holding one closed week and one row for next week."""
    import tools.wipe_weekly_review as wipe

    xlsx = tmp_path / "recalls.xlsx"
    jsn = tmp_path / "weekly-review-latest.json"

    wb = openpyxl.Workbook()
    wb.active.title = "Recalls"
    ws = wb.create_sheet(SHEET_NAME)
    ws.append(SHEET_COLS)

    def row(url, week, tier="1"):
        r = {c: "" for c in SHEET_COLS}
        r.update({"Date": "2026-09-18", "Source": "FSAI (IE)",
                  "Company": "Test Co", "Product": "Cheese",
                  "URL": url, "Tier": tier, "Week_Added": week,
                  "Reviewed": "N"})
        return [r[c] for c in SHEET_COLS]

    for i in range(3):
        ws.append(row(f"https://example.test/closed-{i}", "2026-09-20"))
    ws.append(row("https://example.test/rolled-over", "2026-09-27", tier="2"))
    wb.save(xlsx)

    monkeypatch.setattr(wipe, "XLSX", xlsx)
    monkeypatch.setattr(wipe, "JSON", jsn)
    return wipe, xlsx, jsn


def _run(wipe, argv):
    monkey = sys.argv
    sys.argv = ["wipe_weekly_review"] + argv
    try:
        return wipe.main()
    finally:
        sys.argv = monkey


def test_latest_json_is_not_blanked(workbook):
    """THE REGRESSION. The old code wrote row_count: 0 over the capture."""
    wipe, xlsx, jsn = workbook
    assert _run(wipe, ["--yes", "--week-end", "2026-09-20"]) == 0

    payload = json.loads(jsn.read_text(encoding="utf-8"))
    assert payload["row_count"] == 3, (
        "weekly-review-latest.json was emptied by the wipe — this is the "
        '"0 recalls added" email, reproduced')
    assert payload["week_end"] == "2026-09-20"
    assert payload["sheet_wiped_utc"], (
        "a reader must be able to tell a held closed week from a filling one")


def test_the_closed_week_is_archived_under_its_own_name(workbook):
    wipe, xlsx, jsn = workbook
    _run(wipe, ["--yes", "--week-end", "2026-09-20"])
    archive = jsn.parent / "weekly-review-2026-09-20.json"
    assert archive.exists(), "the closed week has no permanent record"
    assert json.loads(archive.read_text(encoding="utf-8"))["row_count"] == 3


def test_next_weeks_row_survives_the_wipe(workbook):
    """The 17:00→17:30 gap. The workflow promises these roll over."""
    wipe, xlsx, jsn = workbook
    _run(wipe, ["--yes", "--week-end", "2026-09-20"])

    wb = openpyxl.load_workbook(xlsx)
    ws = wb[SHEET_NAME]
    left = [dict(zip(SHEET_COLS, [c.value for c in r]))
            for r in ws.iter_rows(min_row=2)]
    assert len(left) == 1, f"expected the 2026-09-27 row to remain, got {left}"
    assert left[0]["URL"] == "https://example.test/rolled-over"
    assert left[0]["Week_Added"] == "2026-09-27"


def test_the_closed_rows_are_gone(workbook):
    wipe, xlsx, jsn = workbook
    _run(wipe, ["--yes", "--week-end", "2026-09-20"])
    ws = openpyxl.load_workbook(xlsx)[SHEET_NAME]
    urls = [r[SHEET_COLS.index("URL")].value for r in ws.iter_rows(min_row=2)]
    assert not any("closed" in str(u) for u in urls)


def test_all_flag_still_clears_everything(workbook):
    wipe, xlsx, jsn = workbook
    _run(wipe, ["--yes", "--all", "--week-end", "2026-09-20"])
    ws = openpyxl.load_workbook(xlsx)[SHEET_NAME]
    assert ws.max_row == 1, "--all must restore the unconditional behaviour"


def test_dry_run_writes_nothing(workbook):
    wipe, xlsx, jsn = workbook
    before = xlsx.read_bytes()
    assert _run(wipe, ["--dry-run", "--week-end", "2026-09-20"]) == 0
    assert xlsx.read_bytes() == before
    assert not jsn.exists()


def test_a_row_with_no_stamp_is_not_immortal(workbook, tmp_path):
    """A blank Week_Added must close, or it appears in every future email."""
    wipe, xlsx, jsn = workbook
    wb = openpyxl.load_workbook(xlsx)
    ws = wb[SHEET_NAME]
    r = {c: "" for c in SHEET_COLS}
    r.update({"URL": "https://example.test/no-stamp", "Week_Added": ""})
    ws.append([r[c] for c in SHEET_COLS])
    wb.save(xlsx)

    _run(wipe, ["--yes", "--week-end", "2026-09-20"])
    ws = openpyxl.load_workbook(xlsx)[SHEET_NAME]
    urls = [r[SHEET_COLS.index("URL")].value for r in ws.iter_rows(min_row=2)]
    assert "https://example.test/no-stamp" not in urls


# --------------------------------------------------------------------------
# 3. the source itself — the ordering is the whole fix
# --------------------------------------------------------------------------

def _code(name: str) -> str:
    """The module's source with its docstring removed.

    Both docstrings quote the old broken code verbatim, so a naive
    substring search over the whole file finds the defect it is
    documenting and reports the fix as absent.
    """
    import ast
    src = (ROOT / "tools" / f"{name}.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    doc = ast.get_docstring(tree, clean=False)
    return src.replace(doc, "", 1) if doc else src


@pytest.mark.parametrize("name",
                         ["wipe_weekly_review", "wipe_weekly_rejected"])
def test_the_export_happens_before_the_delete(name):
    src = _code(name)
    export_at = src.index("export_week_slice(xlsx_path=XLSX")
    delete_at = src.index("ws.delete_rows(")
    assert export_at < delete_at, (
        f"{name}.py exports after deleting — that is the defect itself, and "
        f"it writes an empty slice over the real capture")


@pytest.mark.parametrize("name",
                         ["wipe_weekly_review", "wipe_weekly_rejected"])
def test_no_export_defaults_to_the_now_empty_sheet(name):
    assert "export_week_slice(xlsx_path=XLSX, json_path=JSON)" not in _code(name), (
        f"{name}.py still regenerates the mailer's file from the sheet it "
        f"just emptied")
