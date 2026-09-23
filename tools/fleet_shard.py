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
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

N_SHARDS = 7


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
    todays = shard_for(registered_codes(), d, a.shards)
    # stdout is consumed by the workflow — codes only, comma separated.
    print(",".join(todays))
    print(f"shard {(d or datetime.now(timezone.utc).date()).isoweekday()} "
          f"of {a.shards}: {len(todays)} countries", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
