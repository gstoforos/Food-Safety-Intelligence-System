"""
tests/test_report_week.py
==========================
Tests for merge_master.compute_report_week — the sticky-stamp rule that
decides which weekly report a recall belongs to.

RULE (locked):
    A row's "report_week" is W{nn} where nn is the ISO week number of
    the SMALLEST Friday STRICTLY AFTER the row's Date.

    Equivalently:
      - Mon-Thu → next Friday's week
      - Friday  → following Friday's week (+7 days, strict next)
      - Sat-Sun → next Friday's week

This produces a clean Friday-09:00-Athens publishing cadence: any recall
dated up to and including a given Thursday lands in THAT Friday's report,
but a recall dated ON Friday rolls over to the NEXT Friday's report
(operator's reasoning: by Friday morning when the report builds, today's
recalls haven't finished verification yet).

The non-obvious case: a Friday-dated row goes to next-week, NOT this-week.
Pre-2026-04-01 the rule was permissive (≤ Friday → this week), which
caused Friday-late recalls to slip into the report before they cleared
the dual-AI gate. Locking this behavior is essential.

Empty/malformed dates must return empty string — NOT raise. Bad data
flows through this function constantly (Tavily date-extractor returns
free-form prose sometimes); a raised exception would crash merge_master.
"""
from __future__ import annotations

import pytest

from pipeline.merge_master import compute_report_week


# ───────────────────────────────────────────────────────────────────────
# Empty / malformed input — must not raise
# ───────────────────────────────────────────────────────────────────────
class TestEmptyInputHandling:
    """Defensive: bad data must produce empty string, NEVER raise."""

    def test_empty_string_returns_empty(self):
        assert compute_report_week("") == ""

    def test_none_returns_empty(self):
        # The signature is (str) but in practice None leaks through from
        # the Tavily fallback path. compute_report_week handles it.
        assert compute_report_week(None) == ""

    def test_garbage_string_returns_empty(self):
        assert compute_report_week("not a date") == ""

    def test_partial_iso_returns_empty(self):
        assert compute_report_week("2026") == ""
        assert compute_report_week("2026-05") == ""

    def test_invalid_month_returns_empty(self):
        assert compute_report_week("2026-13-01") == ""

    def test_invalid_day_returns_empty(self):
        assert compute_report_week("2026-02-30") == ""


# ───────────────────────────────────────────────────────────────────────
# Sticky-stamp rule — each weekday lands in the right Friday's week
# ───────────────────────────────────────────────────────────────────────
class TestStickyStampRule:
    """
    Test calendar: May 2026 looks like
      Mon  4 → W19's bucket
      Tue  5 → W19's bucket
      Wed  6 → W19's bucket
      Thu  7 → W19's bucket
      Fri  8 → W19's BUCKET? NO — Fri rolls to W20 (+7 days).
      Sat  9 → W20 (next Friday is May 15 = W20)
      Sun 10 → W20
      Mon 11 → W20
      ...
      Fri 15 → W21 (rolls to May 22)

    Friday May 8, 2026 is ISO week 19. So strict "next Friday after
    Fri May 8" = Fri May 15 = ISO week 20.
    """

    def test_monday_before_friday_in_week(self):
        # Mon May 4, 2026 — next Friday is May 8 (week 19)
        assert compute_report_week("2026-05-04") == "W19"

    def test_thursday_before_friday_in_week(self):
        # Thu May 7, 2026 — next Friday is May 8 (week 19)
        assert compute_report_week("2026-05-07") == "W19"

    def test_friday_rolls_to_next_friday(self):
        # Fri May 8, 2026 — STRICT next Friday is May 15 (week 20)
        # This is THE rule that produces the sticky-stamping behavior.
        assert compute_report_week("2026-05-08") == "W20"

    def test_saturday_in_next_friday_week(self):
        # Sat May 9, 2026 — next Friday is May 15 (week 20)
        assert compute_report_week("2026-05-09") == "W20"

    def test_sunday_in_next_friday_week(self):
        # Sun May 10, 2026 — next Friday is May 15 (week 20)
        assert compute_report_week("2026-05-10") == "W20"


# ───────────────────────────────────────────────────────────────────────
# Year-boundary cases
# ───────────────────────────────────────────────────────────────────────
class TestYearBoundaries:
    """
    Cross-year edges. ISO week 52 of 2026 ends Sun Dec 27, 2026. The
    very last Friday of 2026 is Fri Dec 25 (week 52). Then the next
    Friday is Fri Jan 1, 2027 — which IS ISO week 53 of 2026 (most
    calendars include Jan 1 in the prior year's W53 if the year ended
    on Thu/Fri/Sat).

    For our purposes we just check: compute_report_week returns SOME
    valid W-stamp, doesn't crash, doesn't produce W00 or W54.
    """

    def test_year_boundary_does_not_crash(self):
        # Last week of 2026 — function must return a valid stamp.
        for d in ["2026-12-28", "2026-12-29", "2026-12-30", "2026-12-31"]:
            result = compute_report_week(d)
            assert result.startswith("W"), f"bad stamp for {d}: {result}"
            wnum = int(result[1:])
            assert 1 <= wnum <= 53, f"out-of-range week for {d}: {result}"

    def test_january_first_handled(self):
        # New Year's Day always produces a valid stamp
        result = compute_report_week("2027-01-01")
        assert result.startswith("W")


# ───────────────────────────────────────────────────────────────────────
# Format invariants
# ───────────────────────────────────────────────────────────────────────
class TestFormatInvariants:
    """All non-empty returns must be W + zero-padded 2-digit week."""

    @pytest.mark.parametrize("date_str", [
        "2026-01-01",
        "2026-01-05",
        "2026-03-15",
        "2026-05-10",
        "2026-08-31",
        "2026-12-25",
    ])
    def test_all_returns_match_w_nn_format(self, date_str):
        result = compute_report_week(date_str)
        assert result.startswith("W")
        assert len(result) == 3  # exactly W + 2 digits
        # The 2 digits must be numeric and in range 01..53
        wnum = int(result[1:])
        assert 1 <= wnum <= 53


# ───────────────────────────────────────────────────────────────────────
# Datetime input (not just date strings)
# ───────────────────────────────────────────────────────────────────────
class TestDatetimeInput:
    """
    Some callers pass ISO datetime strings (with time component). The
    function slices to [:10] so this should work transparently.
    """

    def test_iso_datetime_with_time_handled(self):
        # 2026-05-04T08:00:00Z is Monday — should stamp W19
        assert compute_report_week("2026-05-04T08:00:00Z") == "W19"

    def test_iso_datetime_with_microseconds_handled(self):
        assert compute_report_week("2026-05-08T08:00:00.000Z") == "W20"
