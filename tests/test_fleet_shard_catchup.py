# -*- coding: utf-8 -*-
"""A weekly shard does not bound staleness. The catch-up does.

WHAT THIS CAUGHT (morning fix 2026-10-12)
=========================================
tools/fleet_shard.shard_for() deals the registry round-robin off a SORTED
list and keys the slice on the UTC weekday. Balanced to within one country,
every country once a week — and a country's shard is its POSITION in that
sorted list, so adding ONE config re-deals the entire fleet. Every code
sorting after the new one moves back a shard, and moving backwards means
waiting up to thirteen days instead of seven.

tests/test_no_country_goes_dark.py measured the cost::

    vn  last ran 2026-09-27 (Sunday), 14 days ago, bar is 14

Vietnam neither crashed nor lost its dispatch. th, cn, in, us and ca were
added over three weeks and each addition walked vn one shard back — Sunday,
then Wednesday, then Thursday — skipping its turn each time.

The same arithmetic delays a new config: ca.py landed 2026-10-11 on
Saturday's shard, so MAPAQ Québec would first run 2026-10-17 while two
in-scope MAPAQ notices sat unrecorded and the register's newest MAPAQ row
was 2026-04-13.

WHAT THE FIX IS, AND WHAT IT IS NOT
-----------------------------------
The shard is unchanged — it is the thing that keeps the job inside its
50-minute budget. A catch-up rides on top: any registered country whose
newest run_log record is older than CATCHUP_DAYS, or which has none at
all, joins today's run, oldest first, at most CATCHUP_MAX per day.

The cap is load-bearing. Twelve configs were added in one morning on
2026-09-23; without a cap the next day's catch-up would be twelve
countries on top of a shard of seven, the job would hit its timeout, and
most of them would end with no run_log record — which is exactly the state
this is meant to end.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools import fleet_shard as FS  # noqa: E402


# ── helpers ────────────────────────────────────────────────────────────────

def _write_log(root: Path, code: str, *stamps: str) -> None:
    d = root / f"gap_finder_{code}"
    d.mkdir(parents=True, exist_ok=True)
    with (d / "run_log.jsonl").open("w", encoding="utf-8") as fh:
        for s in stamps:
            fh.write(json.dumps({"country_code": code, "started_at": s,
                                 "finished_at": s}) + "\n")


def _iso(day: date, days_ago: int) -> str:
    return (datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
            - timedelta(days=days_ago)).isoformat().replace("+00:00", "Z")


# ── the shard itself is untouched ──────────────────────────────────────────

def test_the_shard_is_still_a_clean_weekly_partition():
    """The catch-up must not be paid for by thinning the shard."""
    codes = [f"c{i:02d}" for i in range(47)]
    seen: list[str] = []
    for wd in range(1, 8):
        seen += FS.shard_for(codes, date(2026, 10, 12) + timedelta(days=wd - 1))
    assert sorted(seen) == sorted(codes)
    sizes = [len(FS.shard_for(codes, date(2026, 10, 12) + timedelta(days=i)))
             for i in range(7)]
    assert max(sizes) - min(sizes) <= 1, f"shards unbalanced: {sizes}"


# ── the re-deal, reproduced ───────────────────────────────────────────────

def test_adding_one_config_moves_other_countries_shards():
    """The mechanism behind the vn outage, stated as a fact about the code.

    This is NOT a bug to fix in shard_for — positional dealing is what keeps
    the shards balanced. It is the reason a catch-up is needed, and this
    test fails loudly if someone "fixes" the re-deal and removes that
    reason without removing the catch-up.
    """
    before = sorted(["ae", "br", "vn", "za"])
    after = sorted(before + ["ca"])
    day = date(2026, 10, 11)                       # a Sunday
    moved = [c for c in before
             if (c in FS.shard_for(before, day, 3))
             != (c in FS.shard_for(after, day, 3))]
    assert moved, ("adding a config no longer re-deals the fleet — if that "
                   "is deliberate, re-read tests/test_fleet_shard_catchup.py "
                   "and decide whether the catch-up is still earned")


# ── the catch-up ──────────────────────────────────────────────────────────

def test_a_never_run_country_is_caught_up(tmp_path):
    day = date(2026, 10, 12)
    _write_log(tmp_path, "ae", _iso(day, 1))
    # 'ca' deliberately has NO run_log — the ca.py case.
    got = FS.catchup_codes(["ae", "ca"], day, tmp_path)
    assert got == ["ca"], got


def test_a_stale_country_is_caught_up(tmp_path):
    day = date(2026, 10, 12)
    _write_log(tmp_path, "vn", _iso(day, 15))      # the measured vn age
    _write_log(tmp_path, "ae", _iso(day, 1))
    assert FS.catchup_codes(["ae", "vn"], day, tmp_path) == ["vn"]


def test_a_fresh_country_is_not_caught_up(tmp_path):
    """A catch-up that fires on healthy countries is just a bigger shard."""
    day = date(2026, 10, 12)
    for c in ("ae", "br", "vn"):
        _write_log(tmp_path, c, _iso(day, 2))
    assert FS.catchup_codes(["ae", "br", "vn"], day, tmp_path) == []


def test_the_catchup_is_capped_and_oldest_first(tmp_path):
    """The 2026-09-23 shape: twelve new configs at once.

    Never-run countries sort ahead of merely-stale ones, the order is
    deterministic, and the day's extra load is bounded.
    """
    day = date(2026, 10, 12)
    codes = []
    for i in range(12):                            # never ran at all
        codes.append(f"n{i:02d}")
    for i, age in enumerate((40, 30, 20)):          # stale, decreasing age
        c = f"s{i}"
        codes.append(c)
        _write_log(tmp_path, c, _iso(day, age))
    got = FS.catchup_codes(codes, day, tmp_path)
    assert len(got) == FS.CATCHUP_MAX, got
    assert all(g.startswith("n") for g in got), (
        f"a never-run country must outrank a stale one: {got}")
    assert got == FS.catchup_codes(codes, day, tmp_path), "order is not stable"


def test_todays_codes_adds_the_catchup_without_duplicating(tmp_path):
    day = date(2026, 10, 12)                        # Monday
    codes = sorted(["ae", "cl", "eg", "hu", "kr", "no", "th", "jp", "vn"])
    _write_log(tmp_path, "vn", _iso(day, 15))
    for c in codes:
        if c != "vn":
            _write_log(tmp_path, c, _iso(day, 1))
    out = FS.todays_codes(codes, day, 7, tmp_path)
    assert "vn" in out, out
    assert len(out) == len(set(out)), f"duplicate codes in {out}"
    assert "jp" in out, "the DAILY extra must survive the catch-up change"


def test_the_catchup_bounds_staleness_below_the_dark_test_bar(tmp_path):
    """The catch-up only earns its place if it beats the bar that went red.

    test_no_country_goes_dark.STALE_DAYS is the bar. CATCHUP_DAYS must
    leave room for one run to land underneath it, or the fix does not fix
    the test that found the problem.
    """
    import importlib
    dark = importlib.import_module("tests.test_no_country_goes_dark")
    assert FS.CATCHUP_DAYS < dark.STALE_DAYS, (
        f"catch-up at {FS.CATCHUP_DAYS}d cannot keep a country under the "
        f"{dark.STALE_DAYS}d darkness bar")
