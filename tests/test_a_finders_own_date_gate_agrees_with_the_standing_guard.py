# -*- coding: utf-8 -*-
"""A finder's private date gate may not be looser than the standing one.

WHAT THIS CAUGHT (morning-fix 2026-10-07)
=========================================
``pipeline/_gap_finder_guards.check_gap_finder_row`` is the standing
recency guard. It rejects a row whose Date leads today by more than one
day, and the official-feeds collectors and ``merge_master`` both call it.

The standalone finders do not call it. ``pipeline/gap_finder_tavily.py``
carries its own copy of the check with a THIRTY-DAY future tolerance,
which is not a sanity check on a publication date — it is a window wide
enough to admit a best-before date. Measured on main at c540527,
**Pending row 2**, written by that module (ScrapedAt 2026-10-06T19:11:42Z):

    Date   2026-10-30                              23 days in the future
    Source BVL
    URL    https://www.lebensmittelwarnung.de/.../2026/09_September/
           260904_03_BW_diverse_Kaesesorten/..._Presse_1.pdf

The notice is from 2026-09-04 — its own URL path says so — and
30.10.2026 is a shelf-life date printed inside a Listeria cheese recall.
The row then made BVL the freshest-looking source in the register:
``tests/test_scraper_output_health::test_no_source_has_a_future_last_row``
went red with ``{'BVL': '2026-10-30'}`` and "days since last row" for that
source was negative.

WHY THIS TEST SHAPE
-------------------
Not "is 2026-10-30 gone from Pending" — that is the one-off data repair and
says nothing about tomorrow's row. Not "does gap_finder_tavily reject this
one date" either, because the next finder to grow a private copy of the
check would be just as free to pick its own number. What this asserts is
the relationship: every finder that keeps its own future-date tolerance
must be no looser than the guard the rest of the pipeline enforces. A new
finder with a 30-day window fails here, and so does raising the standing
guard without raising the copies.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

#: Modules that implement their own future-date tolerance instead of
#: calling check_gap_finder_row, with the constant each one uses.
PRIVATE_GATES = [
    ("pipeline/gap_finder_tavily.py", "_FUTURE_TOLERANCE_DAYS"),
]


def _standing_tolerance_days() -> int:
    """The future slack check_gap_finder_row allows, read from its source.

    Read rather than hard-coded so that raising the standing guard makes
    this test demand the copies be raised too, instead of silently
    agreeing with a number that moved.
    """
    src = (ROOT / "pipeline" / "_gap_finder_guards.py").read_text(
        encoding="utf-8")
    m = re.search(r"if d > today \+ timedelta\(days=(\d+)\)", src)
    assert m, ("the future-date branch of check_gap_finder_row no longer "
               "looks like `if d > today + timedelta(days=N)`; this test "
               "reads N out of it")
    return int(m.group(1))


def test_the_standing_guard_still_rejects_a_future_date():
    """If this stops holding, the thing the copies must agree with is gone."""
    from pipeline._gap_finder_guards import check_gap_finder_row

    today = date(2026, 10, 7)
    row = {
        "Date": "2026-10-30",
        "Source": "BVL (DE)",
        "Country": "Germany",
        "Product": "Weinbauernkäse",
        "Company": "Test Co",
        "URL": ("https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/"
                "Meldungen/2026/09_September/260904_03_BW_diverse_Kaesesorten/"
                "260904_03_BW_diverse_Kaesesorten_Presse_1.pdf"),
    }
    ok, reason, _ = check_gap_finder_row(row, today=today)
    assert not ok and reason.startswith("future_date"), (ok, reason)


@pytest.mark.parametrize("relpath,const", PRIVATE_GATES,
                         ids=[p for p, _ in PRIVATE_GATES])
def test_a_private_gate_is_no_looser_than_the_standing_guard(relpath, const):
    import importlib

    mod = importlib.import_module(
        relpath.replace("/", ".").removesuffix(".py"))
    private = getattr(mod, const, None)
    assert private is not None, (
        f"{relpath} is listed here as keeping its own future-date "
        f"tolerance but exposes no {const}; if it now calls "
        f"check_gap_finder_row instead, remove it from PRIVATE_GATES")
    standing = _standing_tolerance_days()
    assert private <= standing, (
        f"{relpath}.{const} = {private} days of future slack, against "
        f"{standing} in pipeline/_gap_finder_guards.check_gap_finder_row. "
        f"A window wider than the standing guard admits dates that are not "
        f"publication dates — 30 days admitted a best-before date "
        f"(2026-10-30 on a 2026-09-04 BVL notice) into Pending on "
        f"2026-10-06.")


def test_the_tavily_gate_drops_the_row_that_got_through():
    """The specific date, through the constant, not through the regex."""
    from pipeline import gap_finder_tavily as gf

    today = date(2026, 10, 7)
    bad = date(2026, 10, 30)
    assert (today - bad).days < -gf._FUTURE_TOLERANCE_DAYS, (
        "2026-10-30 must fall outside the tolerance on 2026-10-07")
    # ...and a regulator publishing on its own tomorrow still passes.
    tomorrow = today + timedelta(days=1)
    assert not (today - tomorrow).days < -gf._FUTURE_TOLERANCE_DAYS, (
        "one day of slack is kept for the timezone case")
