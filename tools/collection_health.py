#!/usr/bin/env python3
"""Is the fleet still collecting? Compare this week against its own history.

WHY
---
On 2026-08-27 the W35 weekly report showed 22 recalls. The three preceding
weeks held 62, 63 and 72. Nothing failed: no workflow errored, no alert
fired, and the weekly report was correct — 22 really was all that had been
collected. The collapse was invisible because every part of the system
reported success on the work it actually did.

A 70% drop spread evenly across independent regulators is not a food-supply
signal. RappelConso 41 -> 13, RASFF 21 -> 6, FDA 5 -> 1, CFIA 3 -> 0 in one
week is a collection failure, and it is detectable from the corpus alone.

This tool makes that detectable on purpose. It is deliberately NOT wired to
the signal detector: an aberration detector must not be asked to decide
whether its own input is trustworthy.

    python -m tools.collection_health
    python -m tools.collection_health --fail-under 0.60   # CI gate
"""
from __future__ import annotations

import argparse
import statistics
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

# A source is called dark only if it was reliably producing before. Below
# this weekly volume, a zero is ordinary quiet and means nothing.
DARK_MIN_PRIOR = 3
LOOKBACK_WEEKS = 4


def _thursday_window(end: date) -> tuple[date, date]:
    """The Fri->Thu window closing on `end` (George's week runs Thu to Thu)."""
    return end - timedelta(days=6), end


def _last_thursday(today: date | None = None) -> date:
    today = today or date.today()
    return today - timedelta(days=(today.weekday() - 3) % 7)


def load(xlsx: Path) -> pd.DataFrame:
    df = pd.read_excel(xlsx, "Recalls")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df[df["Date"].notna()].copy()


def analyse(df: pd.DataFrame, end: date, lookback: int = LOOKBACK_WEEKS) -> dict:
    lo, hi = _thursday_window(end)
    cur = df[(df["Date"] >= pd.Timestamp(lo)) & (df["Date"] <= pd.Timestamp(hi))]

    prior_totals, prior_frames = [], []
    for k in range(1, lookback + 1):
        p_end = end - timedelta(days=7 * k)
        p_lo, p_hi = _thursday_window(p_end)
        sub = df[(df["Date"] >= pd.Timestamp(p_lo)) & (df["Date"] <= pd.Timestamp(p_hi))]
        prior_totals.append(len(sub))
        prior_frames.append(sub)

    baseline = statistics.median(prior_totals) if prior_totals else 0
    ratio = (len(cur) / baseline) if baseline else 1.0

    prev = prior_frames[0]["Source"].value_counts() if prior_frames else pd.Series(dtype=int)
    now = cur["Source"].value_counts()
    dark, down = [], []
    for src in prev.index:
        a, b = int(prev[src]), int(now.get(src, 0))
        if a >= DARK_MIN_PRIOR and b == 0:
            dark.append((str(src), a))
        elif a >= DARK_MIN_PRIOR and b < a * 0.5:
            down.append((str(src), a, b))

    # Days with no collection at all inside the window.
    days = {d.date(): 0 for d in pd.date_range(lo, hi)}
    for d, n in cur.groupby(cur["Date"].dt.date).size().items():
        days[d] = int(n)
    silent = [d for d, n in days.items() if n == 0]

    return {"window": (lo, hi), "current": len(cur), "prior": prior_totals,
            "baseline": baseline, "ratio": ratio, "dark": dark, "down": down,
            "days": days, "silent": silent,
            "sources_now": int(now.size), "sources_prev": int(prev.size)}


def report(a: dict, fail_under: float) -> int:
    lo, hi = a["window"]
    print(f"COLLECTION HEALTH  window {lo} -> {hi} (Fri->Thu)")
    print(f"  collected this week : {a['current']}")
    print(f"  prior {len(a['prior'])} weeks      : "
          f"{', '.join(str(x) for x in a['prior'])}  (median {a['baseline']:.0f})")
    print(f"  ratio to baseline   : {a['ratio']:.2f}")
    print(f"  sources producing   : {a['sources_now']} "
          f"(was {a['sources_prev']} last week)")
    print()
    print("  per day:")
    for d, n in sorted(a["days"].items()):
        bar = "#" * min(n, 40)
        print(f"    {d}  {n:>3}  {bar}")

    if a["dark"]:
        print("\n  DARK — produced last week, nothing this week:")
        for s, n in sorted(a["dark"], key=lambda t: -t[1]):
            print(f"    {s:<34} {n} -> 0")
    if a["down"]:
        print("\n  DOWN — more than halved:")
        for s, x, y in sorted(a["down"], key=lambda t: -t[1]):
            print(f"    {s:<34} {x} -> {y}")
    if a["silent"]:
        print(f"\n  SILENT DAYS: {', '.join(str(d) for d in sorted(a['silent']))}")

    print()
    if a["baseline"] and a["ratio"] < fail_under:
        print(f"  VERDICT: COLLECTION FAILURE — {a['ratio']:.0%} of baseline, "
              f"below the {fail_under:.0%} floor.")
        print("  A drop spread across independent regulators is a fleet")
        print("  problem, not a change in the food supply. Check that the")
        print("  scrape workflows are being dispatched before reading any")
        print("  weekly report from this window as a finding.")
        return 2
    print(f"  VERDICT: OK — {a['ratio']:.0%} of baseline.")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--xlsx", default=str(XLSX))
    ap.add_argument("--week-end", default=None,
                    help="closing Thursday, ISO; default = most recent")
    ap.add_argument("--fail-under", type=float, default=0.60,
                    help="exit 2 below this fraction of the trailing median")
    a = ap.parse_args(argv)
    end = (date.fromisoformat(a.week_end) if a.week_end else _last_thursday())
    return report(analyse(load(Path(a.xlsx)), end), a.fail_under)


if __name__ == "__main__":
    sys.exit(main())
