#!/usr/bin/env python3
"""Which coverage thresholds actually change the output?

All seven constants in pipeline/source_coverage.py were chosen by
judgement. This sweeps each across a plausible range, holding the others
at their defaults, and reports which ones move an output that matters —
above all the LATEST MATURITY among continuous sources, because the
analytical window in TR-2026-01 opens nine weeks after it.

Measured on the 35-week corpus, 2026-08-27:

    LOAD-BEARING (move the analytical cutoff)
      CONTINUOUS_ACTIVE_RATE   0.5 -> 10 continuous sources, cutoff 27 Apr
                               0.9 ->  1 continuous source,  cutoff 16 Mar
      ONSET_WINDOW             4   -> cutoff jumps to 15 Jun (10 weeks)
      ONSET_MIN_ACTIVE         5   -> cutoff jumps to 15 Jun
      MATURITY_REF_WEEKS       4   -> cutoff 13 Apr; 6/8/12/16 identical

    INERT (no effect anywhere in the tested range)
      OUTAGE_MIN_WEEKS         2, 3, 4, 6 all identical
      MATURITY_RATIO           0.30 .. 0.90 all identical — a threefold
                               range with no output change at all

    COSMETIC (moves counts, never the cutoff)
      SPORADIC_MIN_RECORDS     shifts the sporadic tally 23..28

Read that carefully before tuning anything: MATURITY_RATIO is the
parameter that reads as most consequential and does the least, while
CONTINUOUS_ACTIVE_RATE silently decides how much history is analysable.

    python -m tools.coverage_sensitivity
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pipeline.source_coverage as sc  # noqa: E402

BASE = dict(ONSET_WINDOW=6, ONSET_MIN_ACTIVE=4, OUTAGE_MIN_WEEKS=3,
            CONTINUOUS_ACTIVE_RATE=0.70, INTERMITTENT_ACTIVE_RATE=0.25,
            SPORADIC_MIN_RECORDS=8, MATURITY_REF_WEEKS=8, MATURITY_RATIO=0.60)

SWEEP = [("ONSET_WINDOW", [4, 5, 6, 8, 10]),
         ("ONSET_MIN_ACTIVE", [2, 3, 4, 5]),
         ("OUTAGE_MIN_WEEKS", [2, 3, 4, 6]),
         ("CONTINUOUS_ACTIVE_RATE", [0.5, 0.6, 0.7, 0.8, 0.9]),
         ("SPORADIC_MIN_RECORDS", [4, 6, 8, 12, 20]),
         ("MATURITY_REF_WEEKS", [4, 6, 8, 12, 16]),
         ("MATURITY_RATIO", [0.30, 0.45, 0.60, 0.75, 0.90])]


def _run(xlsx, **over):
    for k, v in BASE.items():
        setattr(sc, k, v)
    for k, v in over.items():
        setattr(sc, k, v)
    # refreeze: a sensitivity sweep must ignore frozen values, or every
    # run returns the same answer and the sweep measures nothing.
    reg, _ = sc.build_register(xlsx, refreeze=True)
    cont = [s for s in reg.values() if s.coverage_class == "continuous"]
    spor = sum(1 for s in reg.values() if s.coverage_class == "sporadic")
    mats = [s.mature_week for s in cont if s.mature_week]
    return len(cont), spor, (max(mats).split("/")[0] if mats else "-")


def main() -> int:
    xlsx = str(ROOT / "docs" / "data" / "recalls.xlsx")
    n0, s0, l0 = _run(xlsx)
    print(f"BASELINE  continuous={n0}  sporadic={s0}  latest_maturity={l0}")
    print("  (the analytical window opens 9 weeks after latest_maturity)\n")
    print(f"{'parameter':26} {'value':>7}  {'cont':>5} {'spor':>5} "
          f"{'latest maturity':>16}  effect")
    print("-" * 82)
    verdict = {}
    for name, vals in SWEEP:
        moves_cutoff = False
        for v in vals:
            n, s, l = _run(xlsx, **{name: v})
            flags = []
            if l != l0:
                flags.append(f"CUTOFF {l0}->{l}")
                moves_cutoff = True
            if n != n0:
                flags.append(f"continuous {n0}->{n}")
            if s != s0:
                flags.append(f"sporadic {s0}->{s}")
            star = "*" if v == BASE[name] else " "
            tail = "  <== " + "; ".join(flags) if flags else ""
            print(f"{name:26} {v:>7}{star} {n:>5} {s:>5} {l:>16}{tail}")
        verdict[name] = moves_cutoff
        print()
    print("LOAD-BEARING (move the analytical cutoff):")
    for k, v in verdict.items():
        if v:
            print(f"    {k}")
    print("\nnot load-bearing on this corpus:")
    for k, v in verdict.items():
        if not v:
            print(f"    {k}")
    print("\n* = current default")
    return 0


if __name__ == "__main__":
    sys.exit(main())
