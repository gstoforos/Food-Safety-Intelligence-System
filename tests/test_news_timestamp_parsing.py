"""Regression: the NEWS 'Published (UTC)' column must parse.

2026-08-31 — pipeline/agents/outbreak_intel.py carried its own inline
timestamp parser:

    datetime.fromisoformat(str(r[col])[:19].replace(" ", "T"))

which assumed ISO timestamps. The NEWS sheet is written by
pipeline/daily_recall_search.py as strftime("%Y-%m-%d %H:%M UTC"),
e.g. "2026-08-31 04:06 UTC". On that input the expression produced
"2026-08-31T04:06TUT" and raised ValueError, which a bare
`except Exception: continue` swallowed. Result: _load_news() returned []
against a fully-populated NEWS sheet, and the Outbreak Intel Agent
reported "no news items in window" and proposed nothing on every run.

These tests pin the ACTUAL on-disk format against BOTH parsers, so the
writer and the readers cannot drift apart again unnoticed.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pipeline.agents.outbreak_intel import _news_published_at
from pipeline.purge_old_news import _parse_dt


# The exact format pipeline/daily_recall_search.py writes, plus the
# variants that have appeared in the sheet over time.
REAL_WORLD_VALUES = [
    "2026-08-31 04:06 UTC",
    "2026-08-29 03:10 UTC",
    "2026-08-28 20:46 UTC",
    "2026-04-17 05:37 UTC",
    "2026-08-31 04:06:11 UTC",
    "2026-08-31 04:06:11",
    "2026-08-31 04:06",
    "2026-08-31T04:06:11",
    "2026-08-31T04:06:11Z",
    "2026-08-31",
]


@pytest.mark.parametrize("raw", REAL_WORLD_VALUES)
def test_agent_parser_accepts_real_news_values(raw):
    dt = _news_published_at(raw)
    assert dt is not None, f"agent parser returned None for {raw!r}"
    assert dt.tzinfo is not None, f"{raw!r} parsed without a timezone"
    assert dt.year == 2026


@pytest.mark.parametrize("raw", REAL_WORLD_VALUES)
def test_both_parsers_agree(raw):
    """The agent must not grow a second parser that drifts from purge's."""
    assert _news_published_at(raw) == _parse_dt(raw)


def test_the_exact_regression_string():
    """'2026-08-31 04:06 UTC' is the string that broke the agent."""
    dt = _news_published_at("2026-08-31 04:06 UTC")
    assert dt == datetime(2026, 8, 31, 4, 6, tzinfo=timezone.utc)


def test_old_broken_expression_would_still_fail():
    """Proves the bug was real, so this test file cannot be deleted as
    hypothetical. If this ever stops raising, the format changed."""
    raw = "2026-08-31 04:06 UTC"
    with pytest.raises(ValueError):
        datetime.fromisoformat(raw[:19].replace(" ", "T"))


def test_recent_value_is_inside_a_two_day_window():
    """The window comparison must work end to end, not just the parse."""
    raw = (datetime.now(timezone.utc) - timedelta(hours=6)
           ).strftime("%Y-%m-%d %H:%M UTC")
    dt = _news_published_at(raw)
    assert dt is not None
    assert dt >= datetime.now(timezone.utc) - timedelta(days=2)


def test_unparseable_value_returns_none_not_exception():
    assert _news_published_at("not a date at all") is None
    assert _news_published_at("") is None
    assert _news_published_at(None) is None


def test_datetime_passthrough_gets_utc():
    naive = datetime(2026, 8, 31, 4, 6)
    out = _news_published_at(naive)
    assert out is not None and out.tzinfo is not None
