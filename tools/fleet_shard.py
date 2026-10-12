#!/usr/bin/env python3
"""Which countries does the gap-finder fleet run today?

WHY THIS EXISTS (audit 2026-09-23)
==================================
Coverage was spread across eight per-region workflows, each needing its
own slot in FsisScheduler.gs — which lives outside this repository. Three
of them lost their slot and nobody noticed for months::

    at be ch de hu lu nl pl   last run 2026-06-14   (Central EU)
    dk fi is no se            last run 2026-05-31   (Nordic)
    ba cz ee hr md mk         never ran             (East EU)
    eg gh ke                  never wired to any workflow at all

The code was never broken. Every one of those 22 country codes resolves
through ``countries.get()`` today. What broke was the bookkeeping: N
workflows needing N scheduler entries, maintained by hand, in a file the
repo cannot see or test.

So stop keeping a list. This module reads the country REGISTRY — the
thing that is true by construction, because a country config that does
not register cannot run at all — and deals it into shards, one per day of
the week. One scheduler slot covers the whole fleet, and a new country
config is picked up on its next shard without anyone editing a workflow,
a default list, or Apps Script.

WHY SHARD RATHER THAN RUN EVERYTHING
------------------------------------
28 countries at the observed ~6 minutes each is close to three hours in
one job, holding the ``fsis-data-writers`` concurrency lane the whole
time and blocking every reviewer and merge behind it. Seven shards of
four is roughly 25 minutes — inside the existing 50-minute budget, with
the lane free for the rest of the day.

Every country therefore runs weekly. That is a reduction for the five
that currently run daily, and it is what makes room for the 22 that
currently run never.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

N_SHARDS = 7

# Countries that run EVERY day on top of their weekly shard (2026-10-03).
# Japan's consumer agency (CAA) posts recalls daily; on its weekly rotation
# the 2026-10-02 notices — one an aflatoxin recall — reached no sheet until
# the Morning Fix found them by hand the next day.
DAILY = ("jp",)

# ── CATCH-UP (morning fix 2026-10-12) ──────────────────────────────────────
# shard_for() deals round-robin off a SORTED list, keyed on the weekday. That
# keeps the shards balanced to within one country, and it has a property the
# docstring did not state: a country's shard is its POSITION in the sorted
# list, so adding one config re-deals the whole fleet. Every code that sorts
# after the new one moves back a shard, and a country that moves backwards
# waits up to thirteen days for its next turn instead of seven.
#
# MEASURED. tests/test_no_country_goes_dark.py went red on 2026-10-12:
#
#     vn  last ran 2026-09-27 (Sunday), 14 days ago, bar is 14
#
# Vietnam did not crash and was not un-dispatched. It was on Sunday's shard,
# and the configs added since — th, cn, in, us, ca — walked it to Wednesday
# and then to Thursday, each move skipping its turn. Five additions in three
# weeks, and the country with 50 candidates and zero accepted rows is the one
# that went quiet.
#
# The same arithmetic means a BRAND-NEW config waits for its shard: ca.py
# landed 2026-10-11 and sits on Saturday, so MAPAQ Québec — whose dashboard
# tile has advertised a collector since the spring and whose last register
# row is 2026-04-13 — would first run 2026-10-17. Two in-scope MAPAQ notices
# (quebec.ca ...-72638, ...-72639) were already unrecorded when this was
# written.
#
# So the shard stays as it is, balanced, and a catch-up rides on top of it:
# any registered country whose newest run_log record is older than
# CATCHUP_DAYS — or which has none at all — joins today's run, oldest first,
# at most CATCHUP_MAX a day. That bounds staleness at CATCHUP_DAYS + 1
# whatever the re-deal does, and it is self-healing: a country that runs
# today drops off the list tomorrow.
#
# The cap is the job budget, not timidity. The fleet is 50 minutes with a
# 12-minute per-country cap; a shard of 7 plus 3 catch-ups is 10 countries
# at the observed ~6 minutes, which is the same order as a shard of 7 on a
# bad day. Without the cap, the morning after twelve configs are added in
# one go (which happened on 2026-09-23) the catch-up alone would be twelve
# countries and the job would time out with no record for most of them —
# the failure this is meant to prevent.
CATCHUP_DAYS = 9
CATCHUP_MAX = 3

#: Where gap_finder/main writes one record per exit, per country.
DATA_DIR = ROOT / "docs" / "data"


def registered_codes() -> List[str]:
    """Every country the registry knows, sorted.

    Deliberately NOT a list maintained here. A hand-maintained list is
    what let eg/gh/ke sit fully configured and never run, and what let
    the Africa workflow's header describe the East EU countries.
    """
    from pipeline.gap_finder.countries import get, all_codes
    # The registry populates lazily on first get(); any code triggers it.
    try:
        get("it")
    except KeyError:
        pass
    return sorted(all_codes())


def shard_for(codes: List[str], day: Optional[date] = None,
              n_shards: int = N_SHARDS) -> List[str]:
    """The slice of `codes` that runs on `day`.

    Deals round-robin off a sorted list, so shards stay balanced to within
    one country however many are registered. Keyed on the ISO weekday so
    the same day of the week always draws the same slice — a country that
    fails is retried on a predictable date rather than a random one.
    """
    if not codes:
        return []
    n_shards = max(1, int(n_shards))
    day = day or datetime.now(timezone.utc).date()
    idx = (day.isoweekday() - 1) % n_shards        # Mon=0 … Sun=6
    return [c for i, c in enumerate(sorted(codes)) if i % n_shards == idx]


def last_run(code: str, data_dir: Optional[Path] = None) -> Optional[str]:
    """Newest ISO timestamp in a country's run_log, or None if it never ran.

    Reads the same records tests/test_no_country_goes_dark.py reads, so the
    catch-up below and the test that measures staleness cannot disagree
    about what "last ran" means.
    """
    f = (data_dir or DATA_DIR) / f"gap_finder_{code}" / "run_log.jsonl"
    try:
        lines = f.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    best = ""
    for line in lines:
        if not line.strip():
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        t = str(r.get("finished_at") or r.get("started_at") or "")
        if t > best:
            best = t
    return best or None


def _age_days(stamp: Optional[str], now: datetime) -> float:
    """Days since `stamp`; a float infinity for a country that never ran."""
    if not stamp:
        return float("inf")
    try:
        when = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return float("inf")
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return (now - when).total_seconds() / 86400.0


def catchup_codes(codes: List[str], day: Optional[date] = None,
                  data_dir: Optional[Path] = None,
                  stale_days: int = CATCHUP_DAYS,
                  limit: int = CATCHUP_MAX) -> List[str]:
    """Registered countries overdue for a run, oldest first, capped.

    See the CATCHUP note at the top of this module for why a weekly shard
    alone does not bound staleness.
    """
    now = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) \
        if day else datetime.now(timezone.utc)
    aged = [(_age_days(last_run(c, data_dir), now), c) for c in codes]
    overdue = [(a, c) for a, c in aged if a > stale_days]
    # Oldest first, then alphabetical, so the order is deterministic and a
    # never-run country (inf) is never starved by a merely-stale one.
    overdue.sort(key=lambda t: (-t[0], t[1]))
    return [c for _, c in overdue[:max(0, int(limit))]]


def todays_codes(codes: List[str], day: Optional[date] = None,
                 n_shards: int = N_SHARDS,
                 data_dir: Optional[Path] = None) -> List[str]:
    """What the fleet runs on `day`: that weekday's shard, plus every DAILY
    country that is registered, plus up to CATCHUP_MAX overdue countries.

    shard_for() alone stays a clean partition (each country exactly once a
    week); the daily extras and the catch-up ride on top."""
    out = shard_for(codes, day, n_shards)
    for c in DAILY:
        if c in codes and c not in out:
            out.append(c)
    for c in catchup_codes(codes, day, data_dir):
        if c not in out:
            out.append(c)
    return out


def plan(n_shards: int = N_SHARDS) -> dict:
    """The whole week, for eyeballing before you trust it."""
    codes = registered_codes()
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    out = {}
    for i in range(n_shards):
        out[days[i] if i < len(days) else f"shard{i}"] = [
            c for j, c in enumerate(codes) if j % n_shards == i]
    return out


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--shards", type=int, default=N_SHARDS)
    ap.add_argument("--date", default=None,
                    help="YYYY-MM-DD; default today (UTC)")
    ap.add_argument("--plan", action="store_true",
                    help="print the whole week instead of today's slice")
    a = ap.parse_args(argv)

    if a.plan:
        p = plan(a.shards)
        total = 0
        for k, v in p.items():
            total += len(v)
            print(f"{k}: {' '.join(v)}")
        print(f"\n{total} countries over {a.shards} shards", file=sys.stderr)
        return 0

    d = date.fromisoformat(a.date) if a.date else None
    codes = registered_codes()
    todays = todays_codes(codes, d, a.shards)
    # stdout is consumed by the workflow — codes only, comma separated.
    print(",".join(todays))
    # stderr is the step summary a human reads. Name the catch-ups: a
    # country appearing off its shard is a staleness finding, and it should
    # not look like a shard that silently grew.
    caught = catchup_codes(codes, d)
    print(f"shard {(d or datetime.now(timezone.utc).date()).isoweekday()} "
          f"of {a.shards}: {len(todays)} countries", file=sys.stderr)
    if caught:
        for c in caught:
            lr = last_run(c)
            print(f"  catch-up {c}: last ran {lr[:10] if lr else 'NEVER'} "
                  f"(> {CATCHUP_DAYS} days)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
