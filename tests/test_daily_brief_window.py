"""A back-dated promotion must not widen the 7-day daily feed.

Found 2026-08-28. `update_daily_index` computes its retention cutoff from
the target_date it is handed, not from today:

    cutoff = target_date - (KEEP_DAYS - 1)

`rebuild_daily_briefs_for_promoted` looped over every date that gained a
row, today/future excluded but with no guard on the far side. Promoting a
gap-finder's catch of a June notice therefore called
update_daily_index(2026-06-19), which set the cutoff to 2026-06-13 and
retained everything after it. The rolling seven-day feed grew to nine
entries, with briefs from June and July sitting on the dashboard beside
the current week.

Nothing errored. The index was written successfully, every time.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from pipeline import merge_master as mm


def _row(d: str) -> dict:
    return {"Date": d, "Source": "RappelConso (FR)", "URL": f"https://x/{d}",
            "Product": "p", "Pathogen": "Listeria monocytogenes", "Tier": 1}


def _dates_selected(new_rows, today: date, monkeypatch) -> set[str]:
    """Which dates the rebuild would actually touch, without doing IO."""
    seen: set[str] = set()

    def fake_render(target, recalls, *a, **k):
        seen.add(target.isoformat())
        return None

    def fake_index(target, recalls, *a, **k):
        return []

    import types
    mod = types.ModuleType("pipeline.daily_recall_search")
    mod.render_daily_html = fake_render
    mod.update_daily_index = fake_index
    monkeypatch.setitem(__import__("sys").modules,
                        "pipeline.daily_recall_search", mod)

    class _FakeDT:
        @staticmethod
        def now(tz=None):
            from datetime import datetime
            return datetime(today.year, today.month, today.day, 12, 0)

    monkeypatch.setattr(mm, "datetime", _FakeDT)
    mm.rebuild_daily_briefs_for_promoted(new_rows, new_rows)
    return seen


TODAY = date(2026, 8, 28)


def test_a_back_dated_promotion_is_excluded(monkeypatch):
    rows = [_row("2026-08-27"), _row("2026-07-01"), _row("2026-06-19")]
    seen = _dates_selected(rows, TODAY, monkeypatch)
    assert "2026-08-27" in seen
    assert "2026-07-01" not in seen, (
        "a July notice promoted in August must not get a daily brief — it "
        "widens the rolling window and puts a stray card on the dashboard")
    assert "2026-06-19" not in seen


def test_today_and_future_are_still_deferred(monkeypatch):
    rows = [_row("2026-08-28"), _row("2026-08-29"), _row("2026-08-27")]
    seen = _dates_selected(rows, TODAY, monkeypatch)
    assert seen == {"2026-08-27"}


def test_the_oldest_visible_day_is_still_rebuilt(monkeypatch):
    """The feed is anchored to yesterday, so today-7 is still on screen.

    An off-by-one here silently leaves the oldest visible card stale.
    """
    rows = [_row((TODAY - timedelta(days=n)).isoformat()) for n in range(1, 9)]
    seen = _dates_selected(rows, TODAY, monkeypatch)
    assert (TODAY - timedelta(days=7)).isoformat() in seen
    assert (TODAY - timedelta(days=8)).isoformat() not in seen


def test_every_selected_date_is_inside_the_window(monkeypatch):
    rows = [_row((TODAY - timedelta(days=n)).isoformat()) for n in range(0, 30)]
    seen = _dates_selected(rows, TODAY, monkeypatch)
    lo = (TODAY - timedelta(days=7)).isoformat()
    hi = (TODAY - timedelta(days=1)).isoformat()
    assert seen, "nothing selected at all"
    for d in seen:
        assert lo <= d <= hi, f"{d} is outside [{lo}, {hi}]"
