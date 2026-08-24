#!/usr/bin/env python3
"""Scraper health measured on OUTPUT, not on reachability.

WHY THIS EXISTS (2026-08-22)
============================
docs/data/scraper-health.json reports the fleet as healthy:

    scrapers 55 | ok 43 | failed 12 | fail_pct 21.8
    fail_threshold_pct 33.0 | exceeded_threshold False

No alarm. But "ok 43" is 6 scrapers returning data plus **37 counted as
OK_EMPTY** — reachable, and returning nothing. The existing check asks
"did the HTTP request succeed?", which a silently-broken scraper answers
yes to forever.

Measured against the register on 2026-08-22:

    33 of 55 scrapers are reported OK or OK_EMPTY while having produced
    no row for 30+ days — and 23 of those have never produced one at all.

    BVL (DE)  is marked OK. Last row 2026-06-25, 58 days earlier.
    EFET (GR) is marked OK. Last row 2026-07-25.
    Eleven sources went silent within nine days of each other in late
    June 2026 — a pattern no set of independent regulators produces.

This is the July 2026 FSA failure generalised: that scraper returned
HTTP 200 and fifty 2018 alerts every day for a month, and every
reachability check passed while zero usable rows entered the register.

So this tool asks the only question that cannot be faked: WHEN DID THIS
SOURCE LAST PUT A ROW IN THE REGISTER?

    PRODUCING   a row within `--fresh` days      (default 14)
    QUIET       last row 14-29 days ago
    DEAD        last row 30+ days ago
    NEVER       no row, ever

NEVER and DEAD are not automatically defects — a small national regulator
may genuinely issue nothing for months. What is a defect is not KNOWING,
and reporting the difference as health. This tool separates the two so a
human can look at the list and say "Denmark really is quiet" or "Denmark
has been broken since we deployed it".

Run:
    python -m tools.scraper_output_health
    python -m tools.scraper_output_health --max-dead 20   # non-zero exit
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
HEALTH = ROOT / "docs" / "data" / "scraper-health.json"

PRODUCING, QUIET, DEAD, NEVER = "PRODUCING", "QUIET", "DEAD", "NEVER"


def _norm(label: str) -> str:
    """'AESAN (ES)/Spain' and 'AESAN (ES)' -> 'aesan (es)'.

    THE COUNTRY BRACKET IS KEPT ON PURPOSE. The first version of this
    function stripped it, so "FDA (GH)", "FDA (PH)" and "FDA" all collapsed
    to "fda" — and Ghana and the Philippines inherited the US FDA's daily
    rows and were reported PRODUCING. FDA (PH) is marked FAIL_403 by the
    reachability check and has not produced a row since 2026-06-14; this
    tool would have called it healthy.

    A health check whose name matching is loose reports exactly the thing
    it exists to catch as fine, which is how the register got here.
    """
    base = str(label or "").split("/")[0]
    base = base.split(" - ")[0]                 # drop ' - aggregator (...)'
    return re.sub(r"\s+", " ", base).strip().lower()


def last_row_by_source(xlsx: Path = XLSX) -> dict:
    from pipeline.merge_master import load_existing, load_pending
    rows = load_existing(xlsx) + load_pending(xlsx)
    out: dict = {}
    for r in rows:
        src = str(r.get("Source") or "").strip()
        day = str(r.get("DateAdded") or r.get("Date") or "")[:10]
        if not src or len(day) != 10:
            continue
        key = _norm(src)
        if key not in out or day > out[key][0]:
            out[key] = (day, src)
    return out


def classify(age, fresh: int, dead_after: int) -> str:
    if age is None:
        return NEVER
    if age < fresh:
        return PRODUCING
    if age < dead_after:
        return QUIET
    return DEAD


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fresh", type=int, default=14)
    ap.add_argument("--dead-after", type=int, default=30)
    ap.add_argument("--today", type=str, default=None,
                    help="YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--max-dead", type=int, default=0,
                    help="exit non-zero if DEAD+NEVER exceeds this")
    args = ap.parse_args()

    today = (dt.date.fromisoformat(args.today) if args.today
             else dt.datetime.now(dt.timezone.utc).date())
    last = last_row_by_source()

    if HEALTH.exists():
        health = json.loads(HEALTH.read_text()).get("per_scraper", {})
    else:
        health = {}
        print("  (no scraper-health.json — reporting output only)")

    names = sorted(set(list(health) + [v[1] for v in last.values()]))
    rows = []
    for name in names:
        key = _norm(name)
        hit = last.get(key)
        age = None
        if hit:
            try:
                age = (today - dt.date.fromisoformat(hit[0])).days
            except ValueError:
                age = None
        rows.append((classify(age, args.fresh, args.dead_after), age, name,
                     health.get(name, "—")))

    order = {PRODUCING: 0, QUIET: 1, DEAD: 2, NEVER: 3}
    rows.sort(key=lambda r: (order[r[0]], -(r[1] if r[1] is not None else 0)))

    print(f"Scraper output health — {today}\n")
    print(f"{'state':10} {'days':>5}  {'reachability':13}  source")
    print("-" * 68)
    disagree = []
    for state, age, name, hstate in rows:
        d = "-" if age is None else str(age)
        mark = ""
        if state in (DEAD, NEVER) and str(hstate).startswith("OK"):
            mark = "  <-- reported OK"
            disagree.append(name)
        print(f"{state:10} {d:>5}  {hstate:13}  {name[:32]}{mark}")

    c = collections.Counter(r[0] for r in rows)
    print(f"\n  PRODUCING {c[PRODUCING]}  QUIET {c[QUIET]}  "
          f"DEAD {c[DEAD]}  NEVER {c[NEVER]}   (of {len(rows)})")

    if disagree:
        print(f"\n  {len(disagree)} source(s) are reported OK / OK_EMPTY by the "
              f"reachability check while having produced nothing for "
              f"{args.dead_after}+ days, or ever:")
        for n in disagree:
            print(f"     {n}")
        print("\n  OK_EMPTY counts toward 'ok' in scraper-health.json, so a "
              "scraper that is reachable and silent forever never moves "
              "fail_pct and never trips the threshold.")

    # Concentration — how much of the register rests on how few sources.
    from pipeline.merge_master import load_existing
    appr = load_existing(XLSX)
    per = collections.Counter(str(r.get("Source") or "").strip() for r in appr)
    top = per.most_common(3)
    share = sum(n for _, n in top) / max(1, len(appr)) * 100
    print(f"\n  concentration: top 3 sources are {share:.0f}% of the register "
          f"({', '.join(f'{s} {n}' for s, n in top)})")

    bad = c[DEAD] + c[NEVER]
    if args.max_dead and bad > args.max_dead:
        print(f"\nFAIL: {bad} DEAD/NEVER exceeds --max-dead {args.max_dead}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
