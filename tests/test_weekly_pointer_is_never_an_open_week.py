"""The subscriber pointer may not name a week that is still running.

INCIDENT 2026-09-28, found by the morning fix pass of 2026-09-29.

docs/data/weekly-summary-latest.json is what the Apps Script mailer fetches
and sends. At commit 82620c48 it read:

    "filename":   "2026-W40.html"
    "week_start": "2026-09-28",  "week_end": "2026-10-04"
    "stats":      {"total": 1, "tier1": 1, "delta": -54, "delta_pct": -98}
    "generated_utc": "2026-09-28T15:25:56"

W40 began on Monday 28 September. The file was written on Monday 28
September. One day of data, framed as a week, with a 98% fall in recalls
that is nothing but the calendar — the closed week W39 (21-27 Sep) held 55.

HOW: offline-enrich-and-promote.yml calls

    python -m pipeline.build_missing_weekly_reports \
      --this-week-end "$(TZ=Europe/Athens date +%F)"

`date +%F` is today, and on any Monday-to-Saturday that is a week still
running. The argument is not itself wrong — it names the week to RENDER, and
rendering the open week as it fills is wanted. It is the pointer that must
not follow.

write_weekly_summary_json already refuses to move the pointer BACKWARDS
(incident 2026-08-07, a five-week-old briefing mailed to every subscriber).
This is that failure reflected, and the guard belongs in the same place, for
the reason written there: one place covers every caller.

tests/test_report_week.py asserts the invariant against the LIVE file, which
is how this was found. These cases assert it against the WRITER, so the file
cannot come back wrong once it is repaired.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _writer():
    """Load docs/build_weekly_report_afts.py the way its callers do."""
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "docs"))
    spec = importlib.util.spec_from_file_location(
        "wb_pointer", ROOT / "docs" / "build_weekly_report_afts.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


MOD = _writer()


def _last_closed_sunday(today: date | None = None) -> date:
    today = today or date.today()
    return today - timedelta(days=(today.weekday() + 1) % 7)


def _stats():
    return {"total": 1, "tier1": 1, "outbreaks": 0, "delta": -54,
            "delta_pct": -98, "prev_total": 55,
            "top_pathogen": ("Salmonella spp.", 1)}


def _rows(d: date):
    return [{"Date": d.isoformat(), "Country": "France",
             "Pathogen": "Salmonella spp.", "Tier": 1, "Outbreak": 0,
             "Company": "X", "Product": "cheese",
             "Source": "RappelConso (FR)",
             "URL": "https://example.invalid/a"}]


def _pointer(tmp_path):
    return tmp_path / "weekly-summary-latest.json"


def test_the_open_week_does_not_take_the_pointer(tmp_path):
    """The live case: W40 written on day one of W40."""
    open_week_end = _last_closed_sunday() + timedelta(days=7)
    MOD.write_weekly_summary_json(open_week_end, _rows(date.today()),
                                  _stats(), tmp_path)
    assert not _pointer(tmp_path).exists(), (
        "the pointer was written for a week ending "
        f"{open_week_end}, which has not closed")


def test_the_open_week_does_not_overwrite_a_good_pointer(tmp_path):
    """The damaging shape — a correct pointer replaced by an open week."""
    closed = _last_closed_sunday()
    MOD.write_weekly_summary_json(closed, _rows(closed - timedelta(days=2)),
                                  _stats(), tmp_path)
    good = json.loads(_pointer(tmp_path).read_text(encoding="utf-8"))

    MOD.write_weekly_summary_json(closed + timedelta(days=7),
                                  _rows(date.today()), _stats(), tmp_path)
    after = json.loads(_pointer(tmp_path).read_text(encoding="utf-8"))
    assert after["week_num"] == good["week_num"], (
        "an open-week build replaced the closed-week pointer")


def test_the_most_recently_closed_week_is_still_allowed(tmp_path):
    """The guard must not block the thing it is there to protect. A build of
    the just-closed week on its ship day is the normal case and must land."""
    closed = _last_closed_sunday()
    MOD.write_weekly_summary_json(closed, _rows(closed - timedelta(days=2)),
                                  _stats(), tmp_path)
    ptr = _pointer(tmp_path)
    assert ptr.exists(), "the closed week was refused the pointer"
    d = json.loads(ptr.read_text(encoding="utf-8"))
    assert date.fromisoformat(d["week_end"]) <= closed


def test_a_far_future_week_is_refused(tmp_path):
    MOD.write_weekly_summary_json(date.today() + timedelta(days=90),
                                  _rows(date.today()), _stats(), tmp_path)
    assert not _pointer(tmp_path).exists()


def test_the_live_pointer_agrees_with_the_writer():
    """Belt and braces with test_report_week: whatever is committed must be
    a week this writer would agree to write today."""
    ptr = ROOT / "docs" / "data" / "weekly-summary-latest.json"
    if not ptr.exists():
        pytest.skip("no weekly-summary-latest.json")
    d = json.loads(ptr.read_text(encoding="utf-8"))
    week_end = date.fromisoformat(str(d["week_end"])[:10])
    assert week_end <= _last_closed_sunday(), (
        f"{d.get('filename')} covers data through {week_end}; the most "
        f"recently closed week ends {_last_closed_sunday()}")
