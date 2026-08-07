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


# ===========================================================================
# INCIDENT 2026-08-07 — a five-week-old briefing was mailed to subscribers
# ===========================================================================
# Subscribers received a Week 27 briefing on Friday 7 August. W27 ended
# 2 July and had already been sent once.
#
# The rule above worked exactly as specified. A Clostridium botulinum recall
# dated 2026-07-02 was promoted on 2026-08-06 and compute_report_week
# correctly stamped it W27 — that is the whole point of a sticky stamp: a
# late-arriving row belongs to the week it happened in, not the week it was
# found. The tests above still pass and the rule is unchanged.
#
# What broke is downstream. W27's count drifted, daily-review-agent.yml
# rebuilt W27, and the weekly builder unconditionally rewrote
# docs/data/weekly-summary-latest.json — the file the subscriber mailer
# fetches to decide which week to send. The agent's loop sorts stale weeks
# ascending and carries the comment "build W28/current last so latest
# pointer stays correct": it ASSUMES the current week is always also stale,
# so the current week is always built last and reclaims the pointer. That
# day only W27 had drifted, so the only week built was also the last week
# built.
#
# The mailer's staleness guard could not catch it: it checks generated_utc,
# which was four hours old. The JSON was fresh; its CONTENT was five weeks
# stale. Nothing compared the week number to the calendar.
#
# An assumption written in a comment in a YAML file is not a guard. The
# guard now lives at the single place the pointer is written.
class TestLatestPointerNeverMovesBackwards:
    """docs/data/weekly-summary-latest.json is a claim about the NEWEST week.

    A retro-rebuild of a closed week must update that week's HTML and its
    weekly-index.json row, and leave the subscriber pointer alone. That is
    what weekly-updates-pending.json and the Wednesday notification are for.
    """

    def _build(self, tmp_path):
        import importlib.util, json, sys
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent
        spec = importlib.util.spec_from_file_location(
            "_wkly_ptr", root / "docs" / "build_weekly_report_afts.py")
        mod = importlib.util.module_from_spec(spec)
        sys.modules["_wkly_ptr"] = mod
        spec.loader.exec_module(mod)
        return mod, json

    def _stats(self):
        return {"top_pathogen": ("Listeria monocytogenes", 20), "total": 48,
                "tier1": 40, "outbreaks": 1, "delta": -31, "delta_pct": -39}

    def test_a_retro_rebuild_does_not_take_the_pointer(self, tmp_path):
        from datetime import date
        mod, json = self._build(tmp_path)
        rows = [{"Date": "2026-08-03", "Country": "France", "Pathogen":
                 "Listeria monocytogenes", "Tier": 1, "Outbreak": 0,
                 "Company": "X", "Product": "cheese", "Source":
                 "RappelConso (FR)", "URL": "https://example.invalid/a"}]

        # Friday 2026-08-07 → W32. This is the legitimate current build.
        mod.write_weekly_summary_json(date(2026, 8, 7), rows, self._stats(),
                                      tmp_path)
        ptr = tmp_path / "weekly-summary-latest.json"
        assert json.loads(ptr.read_text())["week_num"] == 32

        # Now the incident: a rebuild of W27, five weeks earlier.
        mod.write_weekly_summary_json(date(2026, 7, 3), rows, self._stats(),
                                      tmp_path)
        after = json.loads(ptr.read_text())
        assert after["week_num"] == 32, (
            "a W27 rebuild took the subscriber pointer — this is the "
            "2026-08-07 incident, where every subscriber was mailed a "
            "five-week-old briefing")
        assert after["filename"] == "2026-W32.html"

    def test_the_current_week_can_still_claim_it(self, tmp_path):
        """The guard must not freeze the pointer. Next Friday must win."""
        from datetime import date
        mod, json = self._build(tmp_path)
        rows = [{"Date": "2026-08-10", "Country": "France", "Pathogen":
                 "Salmonella", "Tier": 1, "Outbreak": 0, "Company": "Y",
                 "Product": "z", "Source": "RappelConso (FR)",
                 "URL": "https://example.invalid/b"}]
        ptr = tmp_path / "weekly-summary-latest.json"
        mod.write_weekly_summary_json(date(2026, 8, 7), rows, self._stats(),
                                      tmp_path)
        mod.write_weekly_summary_json(date(2026, 8, 14), rows, self._stats(),
                                      tmp_path)
        assert json.loads(ptr.read_text())["week_num"] == 33

    def test_rebuilding_the_same_week_still_refreshes_it(self, tmp_path):
        """A same-week rebuild (corrections, a fixed narrative) must go
        through — otherwise a fix pushed between the 06:00 review and the
        11:00 send would never reach subscribers."""
        from datetime import date
        mod, json = self._build(tmp_path)
        rows = [{"Date": "2026-08-03", "Country": "France", "Pathogen":
                 "Listeria monocytogenes", "Tier": 1, "Outbreak": 0,
                 "Company": "X", "Product": "cheese", "Source":
                 "RappelConso (FR)", "URL": "https://example.invalid/a"}]
        ptr = tmp_path / "weekly-summary-latest.json"
        mod.write_weekly_summary_json(date(2026, 8, 7), rows, self._stats(),
                                      tmp_path)
        first = json.loads(ptr.read_text())["generated_utc"]
        mod.write_weekly_summary_json(date(2026, 8, 7), rows, self._stats(),
                                      tmp_path)
        second = json.loads(ptr.read_text())["generated_utc"]
        assert second != first, "a same-week rebuild was blocked"

    def test_the_published_pointer_is_not_a_closed_week(self):
        """The live file, as it will ship. W32 closed Thursday 2026-08-06
        and ships Friday 2026-08-07."""
        import json
        from pathlib import Path
        root = Path(__file__).resolve().parent.parent
        ptr = root / "docs" / "data" / "weekly-summary-latest.json"
        if not ptr.exists():
            return
        d = json.loads(ptr.read_text(encoding="utf-8"))
        assert d["week_num"] == 32 and d["year"] == 2026, (
            f"weekly-summary-latest.json points at {d.get('filename')} — "
            f"the subscriber mailer sends whatever this file names")
