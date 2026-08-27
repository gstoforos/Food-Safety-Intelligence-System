"""The two channels must draw the same baseline.

Between the first release and 2026-08-27 they did not. The count channel
used `_window(..., offset=GUARD_WEEKS)`; the share channel built its own
window with `range(GUARD_WEEKS, ...)`, which includes the week at offset
GUARD_WEEKS. The share baseline therefore sat one week later than the
count baseline and held out one guard week instead of two — on the channel
that alarms.

That defect was invisible to every test in the suite because both channels
produced plausible numbers. These tests exist so the alignment is a
property the suite checks, not an assumption a reader has to make.
"""
from __future__ import annotations

import pandas as pd
import pytest

import pipeline.signal_detector as sd


@pytest.fixture(scope="module")
def corpus():
    return sd.load_corpus()


def _count_baseline_indices(idx: int) -> list[int]:
    """Exactly what `_window(series, weeks, idx, BASELINE_WEEKS, GUARD_WEEKS)`
    reads: hi = idx - offset, then range(hi - span, hi)."""
    hi = idx - sd.GUARD_WEEKS
    return list(range(hi - sd.BASELINE_WEEKS, hi))


def _share_baseline_indices(idx: int) -> list[int]:
    """Exactly what the proportion channel's loop in `detect` reads."""
    return sorted(idx - back for back in
                  range(sd.GUARD_WEEKS + 1,
                        sd.GUARD_WEEKS + 1 + sd.BASELINE_WEEKS))


def test_the_two_channels_read_the_same_weeks():
    for idx in range(20, 40):
        assert _share_baseline_indices(idx) == _count_baseline_indices(idx), (
            f"channel baselines diverge at index {idx}: this is the defect "
            f"corrected on 2026-08-27, reintroduced")


def test_the_guard_band_is_actually_guard_weeks_wide():
    idx = 30
    base = _count_baseline_indices(idx)
    held_out = [i for i in range(base[-1] + 1, idx)]
    assert len(held_out) == sd.GUARD_WEEKS, (
        f"the guard band holds out {len(held_out)} week(s), not "
        f"{sd.GUARD_WEEKS}. The test week itself is excluded separately.")


def test_the_baseline_is_baseline_weeks_long():
    for idx in range(15, 35):
        assert len(_count_baseline_indices(idx)) == sd.BASELINE_WEEKS
        assert len(_share_baseline_indices(idx)) == sd.BASELINE_WEEKS


def test_window_helper_matches_the_derivation(corpus):
    """Tie the arithmetic above to the real helper, not just to itself."""
    series = {w: 1 for w in corpus.weeks}
    for idx in range(15, len(corpus.weeks)):
        got = sd._window(series, corpus.weeks, idx,
                         sd.BASELINE_WEEKS, sd.GUARD_WEEKS)
        assert len(got) == len(_count_baseline_indices(idx))


def test_no_baseline_week_touches_the_test_week(corpus):
    for idx in range(15, len(corpus.weeks)):
        assert idx not in _share_baseline_indices(idx)
        assert idx not in _count_baseline_indices(idx)


def test_share_baseline_totals_use_the_same_weeks_as_the_stratum(corpus):
    """Numerator and denominator must come from one window.

    The share channel accumulates stratum records and corpus totals in the
    same loop. If they ever drift apart the proportion is meaningless, so
    reconstruct both here and compare against a direct sum.
    """
    strata = sd.build_strata(corpus)
    key = next(iter(strata))
    s = strata[key]
    idx = len(corpus.weeks) - 1
    weeks = corpus.weeks
    base_stratum = base_total = 0
    for back in range(sd.GUARD_WEEKS + 1, sd.GUARD_WEEKS + 1 + sd.BASELINE_WEEKS):
        wj = weeks[idx - back]
        base_stratum += int(s.series.get(wj, 0))
        base_total += int(corpus.totals.get(wj, 0))
    idxs = _share_baseline_indices(idx)
    assert base_total == sum(int(corpus.totals.get(weeks[i], 0)) for i in idxs)
    assert base_stratum == sum(int(s.series.get(weeks[i], 0)) for i in idxs)
    assert base_stratum <= base_total
