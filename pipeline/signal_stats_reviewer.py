#!/usr/bin/env python3
"""
pipeline/signal_stats_reviewer.py — the statistician in the loop.

WHY THIS EXISTS (audit 2026-09-21)
==================================
`signal_review_agent.py` already reviews the CONTENT of each alarm — is it
one publisher, is there news, is it an outbreak. Nothing reviewed the
STATISTICS. On 2026-09-21 a hand audit of one week's board found three
things the pipeline had no way to notice:

1. Benjamini-Hochberg was being applied over `len(candidates)` — the
   strata that had already cleared alpha — instead of over the strata
   that were tested. On 2026-09-14/20 that was 3 rather than 15, and
   `Aflatoxin - Europe` (p = 0.0252, k=3 threshold 3*0.1/15 = 0.0200)
   was published when the honest step-up drops it.

2. `Shiga toxin-producing E. coli (STEC) - France` alarmed against a
   baseline of [0, 0, 9, 1, 1, 1, 0] — SD 3.25 on a mean of 1.71. The
   exact binomial assumes independent Bernoulli draws; regulators publish
   in batches, so the real variance ran several times the binomial
   assumption and the p-value was anti-conservative.

3. The same series reads [..., 0, 2, 3, 6] — a four-week monotone rise,
   with the ramp weeks sitting inside the guard band. The strongest
   evidence on the board was the one thing the board did not test, and
   the detector scored it as an isolated one-week spike.

None of that is a bug the test suite can catch, because none of it is
wrong code. It is a set of questions a statistician asks of a result.
This module asks them every Monday evening and writes down the answers.

WHAT IT CHECKS
==============
  fdr_recompute     Redo the BH step-up from the board's own p-values and
                    `strata_tested`. Disagreement with what was published
                    is reported as a DEFECT, not a note. This is the check
                    that would have caught (1) on the day.
  overdispersion    Baseline variance against the Binomial(N, pi_hat)
                    variance the share test assumes. A ratio far above 1
                    means the p-value is optimistic.
  fragile_baseline  One week supplying most of the baseline mass.
  trend             A monotone run ending in the alarm week, including
                    weeks the guard band hides from the test.
  re_alarm          The same stratum alarming again soon after, and
                    whether its earlier, larger week is inside the guard
                    band that the current test excludes.
  publisher         Dominant source, and whether THAT publisher's own
                    share of corpus rose or fell this week. Rising is a
                    confound; falling strengthens the signal and is worth
                    saying out loud.
  corpus_volume     Whether the week's total sits inside its own band.
  coverage          Whether the baseline lies inside mature collection.

EVERY FINDING CARRIES ITS ARITHMETIC. A reader must be able to redo it
without running this file.

ADVISORY ONLY. Deterministic, no model, no network. It annotates a board
that has already been published; it never suppresses, never alarms, and
never writes to the register.

Reads:  docs/data/signals-board.json
Writes: docs/data/signals-stats-review.json
        docs/data/signals-stats-review.md   (human digest)
        docs/data/signals-stats-review.jsonl (one line per run, history)

Author: AFTS / G. Stoforos
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
BOARD = ROOT / "docs" / "data" / "signals-board.json"
OUT_JSON = ROOT / "docs" / "data" / "signals-stats-review.json"
OUT_MD = ROOT / "docs" / "data" / "signals-stats-review.md"
OUT_JSONL = ROOT / "docs" / "data" / "signals-stats-review.jsonl"

# ── thresholds ────────────────────────────────────────────────────────────
# Chosen to be loud rather than clever. Each is a round number a reader can
# argue with, and each finding prints the value beside the threshold.
OVERDISPERSION_WARN = 3.0    # baseline var / binomial var
FRAGILE_BASELINE_SHARE = 0.5  # one week holding >= this much of the mass
TREND_MIN_RUN = 3            # weeks of monotone rise worth reporting
RE_ALARM_LOOKBACK = 4        # weeks


def _load(path: Path) -> Dict[str, Any]:
    # utf-8-sig: signals-latest.json has carried a BOM before now, and a
    # reviewer that dies on a byte-order mark is a reviewer nobody runs.
    return json.loads(path.read_text(encoding="utf-8-sig"))


# ==========================================================================
# 1. FDR — recomputed from first principles
# ==========================================================================
def _bh_reject(pvals: List[float], q: float, m: int) -> List[bool]:
    """BH step-up, written out here ON PURPOSE.

    An audit that imports the function it is auditing cannot find a bug in
    that function. This is a second, independent implementation; if the two
    ever disagree, that disagreement is the finding.
    """
    n = len(pvals)
    if n == 0:
        return []
    m = max(int(m), n)
    order = sorted(range(n), key=lambda i: pvals[i])
    max_rank = 0
    for rank, i in enumerate(order, start=1):
        if pvals[i] <= (rank / m) * q:
            max_rank = rank
    keep = [False] * n
    for rank, i in enumerate(order, start=1):
        if rank <= max_rank:
            keep[i] = True
    return keep


def check_fdr(board: Dict[str, Any]) -> Dict[str, Any]:
    meta = board.get("meta") or {}
    sigs = board.get("signals") or []
    q = float(meta.get("fdr_q") or 0.1)
    tested = int(meta.get("strata_tested") or 0)
    declared_m = meta.get("fdr_m")

    share = [s for s in sigs if s.get("channel") == "proportion"]
    pv = [float(s.get("p_value") or 1.0) for s in share]

    honest = _bh_reject(pv, q, tested) if tested else []
    as_pub = [bool(s.get("fdr_pass")) for s in share]

    steps = []
    order = sorted(range(len(pv)), key=lambda i: pv[i])
    for rank, i in enumerate(order, start=1):
        thr = (rank / max(tested, len(pv))) * q if tested else float("nan")
        steps.append({
            "rank": rank,
            "label": share[i].get("label"),
            "p_value": pv[i],
            "threshold": round(thr, 6),
            "formula": f"{rank}*{q}/{max(tested, len(pv))}",
            "passes": bool(pv[i] <= thr),
        })

    disagree = [
        {"label": share[i].get("label"), "p_value": pv[i],
         "published_fdr_pass": as_pub[i], "honest_fdr_pass": honest[i]}
        for i in range(len(share)) if honest and as_pub[i] != honest[i]
    ]

    findings = []
    if declared_m is not None and int(declared_m) != tested:
        findings.append({
            "severity": "defect",
            "what": "the FDR denominator is not the number of strata tested",
            "detail": (f"meta.fdr_m = {declared_m} but meta.strata_tested = "
                       f"{tested}. BH controls the false-discovery rate over "
                       f"the family of tests; a denominator drawn from the "
                       f"survivors of the alpha screen controls nothing."),
        })
    if declared_m is None:
        findings.append({
            "severity": "note",
            "what": "the board does not publish the FDR denominator",
            "detail": ("meta.fdr_m is absent, so the step-up cannot be "
                       "reproduced from the published file alone. This "
                       "reviewer assumed strata_tested = %d." % tested),
        })
    for d in disagree:
        findings.append({
            "severity": "defect",
            "what": "published FDR verdict disagrees with an independent recompute",
            "detail": (f"{d['label']}: p = {d['p_value']:.6g}; published "
                       f"fdr_pass = {d['published_fdr_pass']}, honest step-up "
                       f"over m = {tested} gives {d['honest_fdr_pass']}."),
        })

    return {
        "q": q, "m_used_by_reviewer": tested, "m_declared_by_board": declared_m,
        "share_channel_tests": len(share),
        "steps": steps,
        "disagreements": disagree,
        "findings": findings,
    }


# ==========================================================================
# 2. per-signal statistical scrutiny
# ==========================================================================
def _variance(xs: List[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mu = sum(xs) / n
    return sum((x - mu) ** 2 for x in xs) / (n - 1)


def check_overdispersion(row: Dict[str, Any], week_total: int) -> Optional[Dict]:
    """Does the baseline vary more than Binomial(N, pi_hat) allows?

    The share channel is an exact binomial test, which assumes each record
    is an independent draw. Regulators publish in batches, so the true
    variance is larger and the p-value is optimistic — in the direction
    that produces alarms, not in the direction that suppresses them.
    """
    base = [float(x) for x in (row.get("baseline_values") or [])]
    pi = float(row.get("share_baseline") or 0.0)
    if len(base) < 3 or not week_total or pi <= 0:
        return None
    var_obs = _variance(base)
    var_binom = week_total * pi * (1.0 - pi)
    if var_binom <= 0:
        return None
    ratio = var_obs / var_binom
    if ratio < OVERDISPERSION_WARN:
        return None
    return {
        "check": "overdispersion",
        "severity": "caution",
        "baseline_values": base,
        "baseline_variance": round(var_obs, 3),
        "binomial_variance": round(var_binom, 3),
        "ratio": round(ratio, 2),
        "threshold": OVERDISPERSION_WARN,
        "reading": (f"the baseline varies {ratio:.1f}x more than "
                    f"Binomial(N={week_total}, pi={pi:.4f}) assumes, so the "
                    f"exact p is anti-conservative — the direction is "
                    f"probably right, the number is soft"),
    }


def check_fragile_baseline(row: Dict[str, Any]) -> Optional[Dict]:
    base = [float(x) for x in (row.get("baseline_values") or [])]
    tot = sum(base)
    if len(base) < 3 or tot <= 0:
        return None
    top = max(base)
    frac = top / tot
    if frac < FRAGILE_BASELINE_SHARE:
        return None
    return {
        "check": "fragile_baseline",
        "severity": "caution",
        "baseline_values": base,
        "largest_week": top,
        "share_of_baseline_mass": round(frac, 3),
        "threshold": FRAGILE_BASELINE_SHARE,
        "reading": (f"one week supplies {frac*100:.0f}% of the baseline "
                    f"({top:g} of {tot:g}); the comparison rests on a single "
                    f"prior observation and moves a lot if it is wrong"),
    }


def check_trend(row: Dict[str, Any], guard_weeks: int) -> Optional[Dict]:
    """A monotone run ending in the alarm week.

    The guard band deliberately hides the weeks just before the test, so a
    slow ramp is scored as a one-week spike. That is the right call for a
    baseline and the wrong story for a reader: a run of rises is stronger
    evidence than any single week, and nothing on the board says it.
    """
    ser = [float(x) for x in (row.get("series") or [])]
    if len(ser) < TREND_MIN_RUN + 1:
        return None
    run = 1
    i = len(ser) - 1
    while i > 0 and ser[i] > ser[i - 1]:
        run += 1
        i -= 1
    if run < TREND_MIN_RUN:
        return None
    tail = ser[-run:]
    hidden = tail[:-1][-guard_weeks:] if guard_weeks else []
    return {
        "check": "trend",
        "severity": "note",
        "run_length_weeks": run,
        "tail": tail,
        "weeks_inside_guard_band": hidden,
        "reading": (f"{run} consecutive rising weeks ending in the alarm "
                    f"week ({' -> '.join('%g' % v for v in tail)}); the last "
                    f"{len(hidden)} of them sit inside the guard band, so the "
                    f"test scored this as a single elevated week rather than "
                    f"a ramp"),
    }


def check_publisher(row: Dict[str, Any], publishers: List[Dict]) -> Optional[Dict]:
    """Dominant publisher, and whether that publisher grew this week.

    The reflexive objection to any share signal is "the regulator just
    published more". It is answerable from data already on the board, and
    the answer cuts both ways — a signal that rose while its publisher
    SHRANK is materially stronger, and nothing was saying so.
    """
    src = row.get("dominant_source")
    share = float(row.get("dominant_share") or 0.0)
    if not src:
        return None
    pub = next((p for p in publishers if p.get("source") == src), None)
    if not pub:
        return None
    now_s = float(pub.get("now_share") or 0.0)
    base_s = float(pub.get("base_share") or 0.0)
    delta = now_s - base_s
    if delta > 0.02 and share >= 0.8:
        sev, verdict = "caution", "confounded"
        reading = (f"{share*100:.0f}% of this stratum is {src}, and {src}'s "
                   f"own share of corpus ROSE {base_s*100:.1f}% -> "
                   f"{now_s*100:.1f}% this week. The stratum and its "
                   f"publisher moved together; they cannot be separated.")
    elif delta < -0.02 and share >= 0.8:
        sev, verdict = "supporting", "not-a-publisher-artefact"
        reading = (f"{share*100:.0f}% of this stratum is {src}, but {src}'s "
                   f"own share of corpus FELL {base_s*100:.1f}% -> "
                   f"{now_s*100:.1f}% ({pub.get('base_mean')} -> "
                   f"{pub.get('now')} records). The stratum rose inside a "
                   f"contracting feed, which is the opposite of a "
                   f"publication artefact.")
    else:
        sev, verdict = "note", "publisher-flat"
        reading = (f"{share*100:.0f}% of this stratum is {src}; that "
                   f"publisher's share of corpus was broadly flat "
                   f"({base_s*100:.1f}% -> {now_s*100:.1f}%).")
    return {
        "check": "publisher", "severity": sev, "verdict": verdict,
        "dominant_source": src, "dominant_share": share,
        "publisher_base_share": base_s, "publisher_now_share": now_s,
        "publisher_share_delta": round(delta, 4),
        "reading": reading,
    }


def check_re_alarm(row: Dict[str, Any], ledger: List[Dict],
                   this_week: str, guard_weeks: int) -> Optional[Dict]:
    """Did this stratum alarm recently, and is that week inside the guard?

    Salmonella - France alarmed at 15 records on 2026-08-31 and again at 10
    on 2026-09-14. The 15 sits in the guard band of the second test, so it
    is excluded from the baseline the 10 is judged against: a smaller week
    clears a lower bar precisely because a bigger one came first.
    """
    label = row.get("label")
    past = [e for e in ledger if e.get("week") != this_week]
    recent = past[-RE_ALARM_LOOKBACK:]
    hits = []
    for n, e in enumerate(recent):
        for a in (e.get("alarms") or []):
            if a.get("label") == label:
                weeks_ago = len(recent) - n
                hits.append({"week": e.get("week"),
                             "observed": a.get("observed"),
                             "weeks_ago": weeks_ago,
                             "inside_guard_band": weeks_ago <= guard_weeks})
    if not hits:
        return None
    bigger_in_guard = [h for h in hits
                       if h["inside_guard_band"]
                       and (h.get("observed") or 0) > (row.get("observed") or 0)]
    return {
        "check": "re_alarm",
        "severity": "caution" if bigger_in_guard else "note",
        "prior_alarms": hits,
        "reading": (
            (f"alarmed {len(hits)}x in the last {RE_ALARM_LOOKBACK} weeks; "
             f"a LARGER week ({bigger_in_guard[0]['observed']} on "
             f"{bigger_in_guard[0]['week']}) sits inside the guard band and "
             f"is therefore excluded from the baseline this week is judged "
             f"against — the bar is lower because of the earlier spike")
            if bigger_in_guard else
            f"alarmed {len(hits)}x in the last {RE_ALARM_LOOKBACK} weeks"),
    }


# ==========================================================================
# assembly
# ==========================================================================
def review(board: Dict[str, Any]) -> Dict[str, Any]:
    meta = board.get("meta") or {}
    ctx = board.get("context") or {}
    ledger = board.get("ledger") or []
    publishers = ctx.get("publishers") or []
    vol = ctx.get("volume_test") or {}
    week = meta.get("week")
    week_total = int(meta.get("corpus_week_total") or 0)
    guard = int(meta.get("guard_weeks") or 2)

    by_key = {r.get("stratum_key"): r for r in (board.get("board") or [])}

    fdr = check_fdr(board)

    per_signal = []
    for s in (board.get("signals") or []):
        row = dict(by_key.get(s.get("stratum_key"), {}))
        row.update({k: v for k, v in s.items() if v is not None})
        checks = [c for c in (
            check_overdispersion(row, week_total),
            check_fragile_baseline(row),
            check_trend(row, guard),
            check_publisher(row, publishers),
            check_re_alarm(row, ledger, week, guard),
        ) if c]
        per_signal.append({
            "label": s.get("label"),
            "stratum_key": s.get("stratum_key"),
            "channel": s.get("channel"),
            "observed": s.get("observed"),
            "p_value": s.get("p_value"),
            "fdr_status": s.get("fdr_status"),
            "checks": checks,
            "worst_severity": _worst([c["severity"] for c in checks]),
        })

    corpus = None
    if vol:
        c2 = float(vol.get("c2") or 0.0)
        corpus = {
            "check": "corpus_volume",
            "severity": "caution" if abs(c2) >= 2.0 else "note",
            "observed": vol.get("observed"),
            "baseline_mean": vol.get("baseline_mean"),
            "baseline_sd": vol.get("baseline_sd"),
            "c2": c2,
            "reading": (
                f"corpus is {'above' if c2 > 0 else 'below'} its own baseline "
                f"by {abs(c2):.2f} sd"
                + ("; every share this week is measured against a denominator "
                   "that has itself moved" if abs(c2) >= 2.0 else
                   " — inside the normal band, so share readings are not "
                   "confounded by a volume swing")),
        }

    cov = {
        "check": "coverage",
        "severity": "note" if meta.get("within_coverage_window") else "caution",
        "within_coverage_window": meta.get("within_coverage_window"),
        "coverage_window_start": meta.get("coverage_window_start"),
        "reading": meta.get("coverage_note") or "",
    }

    defects = list(fdr["findings"])
    return {
        "meta": {
            "week": week,
            "week_start": meta.get("week_start"),
            "week_end": meta.get("week_end"),
            "board_generated_utc": meta.get("generated_utc"),
            "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "reviewer": "signal_stats_reviewer",
            "model": None,
            "advisory_only": True,
            "corpus_week_total": week_total,
            "strata_tested": meta.get("strata_tested"),
            "strata_suppressed_sparse": meta.get("strata_suppressed_sparse"),
            "signals_reported": len(board.get("signals") or []),
        },
        "fdr": fdr,
        "signals": per_signal,
        "corpus_volume": corpus,
        "coverage": cov,
        "defects": defects,
        "verdict": ("DEFECTS FOUND" if any(d["severity"] == "defect"
                                           for d in defects)
                    else "statistics consistent with the published board"),
    }


_ORDER = {"note": 0, "supporting": 0, "caution": 1, "defect": 2}


def _worst(sevs: List[str]) -> str:
    return max(sevs, key=lambda s: _ORDER.get(s, 0)) if sevs else "none"


# ==========================================================================
# digest
# ==========================================================================
def to_markdown(r: Dict[str, Any]) -> str:
    m = r["meta"]
    L: List[str] = []
    L.append(f"# Signal statistics review — {m['week']}")
    L.append("")
    L.append(f"**{r['verdict']}**")
    L.append("")
    L.append(f"- corpus this week: **{m['corpus_week_total']}** records")
    L.append(f"- strata scored: **{m['strata_tested']}** "
             f"({m['strata_suppressed_sparse']} too sparse to test)")
    L.append(f"- signals published: **{m['signals_reported']}**")
    L.append(f"- board generated: {m['board_generated_utc']}")
    L.append("")

    f = r["fdr"]
    L.append("## Benjamini-Hochberg, recomputed independently")
    L.append("")
    L.append(f"q = {f['q']}, m = {f['m_used_by_reviewer']} "
             f"(board declared m = {f['m_declared_by_board']})")
    L.append("")
    L.append("| k | stratum | p | threshold | | verdict |")
    L.append("|--:|---|--:|--:|---|---|")
    for s in f["steps"]:
        L.append(f"| {s['rank']} | {s['label']} | {s['p_value']:.5f} | "
                 f"{s['threshold']:.5f} | `{s['formula']}` | "
                 f"{'PASS' if s['passes'] else '**FAIL**'} |")
    L.append("")
    if f["disagreements"]:
        L.append("**The published verdicts do not match this recompute:**")
        L.append("")
        for d in f["disagreements"]:
            L.append(f"- `{d['label']}` — published "
                     f"`fdr_pass={d['published_fdr_pass']}`, honest step-up "
                     f"gives `{d['honest_fdr_pass']}`")
        L.append("")

    L.append("## Per signal")
    L.append("")
    for s in r["signals"]:
        L.append(f"### {s['label']}  ·  {s['channel']}  ·  "
                 f"obs {s['observed']}  ·  p {s['p_value']:.4f}  ·  "
                 f"FDR {s['fdr_status']}")
        L.append("")
        if not s["checks"]:
            L.append("Nothing flagged.")
            L.append("")
            continue
        for c in s["checks"]:
            L.append(f"- **{c['check']}** ({c['severity']}) — {c['reading']}")
        L.append("")

    for key in ("corpus_volume", "coverage"):
        c = r.get(key)
        if c:
            L.append(f"**{c['check']}** ({c['severity']}) — {c['reading']}")
            L.append("")

    L.append("---")
    L.append("")
    L.append("Advisory. Deterministic, no model. This reviewer annotates a "
             "board that has already been published; it does not suppress, "
             "alarm, or write to the register.")
    return "\n".join(L) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--board", default=str(BOARD))
    ap.add_argument("--json-out", default=str(OUT_JSON))
    ap.add_argument("--md-out", default=str(OUT_MD))
    ap.add_argument("--jsonl-out", default=str(OUT_JSONL))
    ap.add_argument("--fail-on-defect", action="store_true",
                    help="exit 3 when a DEFECT is found (CI gate). Off by "
                         "default: the review is advisory and a red run "
                         "every Monday teaches people to ignore it.")
    a = ap.parse_args(argv)

    bp = Path(a.board)
    if not bp.exists():
        print(f"ERROR: {bp} does not exist", file=sys.stderr)
        return 1

    r = review(_load(bp))

    Path(a.json_out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.json_out).write_text(
        json.dumps(r, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")
    Path(a.md_out).write_text(to_markdown(r), encoding="utf-8")
    with Path(a.jsonl_out).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(
            {"week": r["meta"]["week"],
             "generated_utc": r["meta"]["generated_utc"],
             "verdict": r["verdict"],
             "n_defects": len(r["defects"]),
             "n_cautions": sum(1 for s in r["signals"]
                               if s["worst_severity"] == "caution"),
             "signals": [s["label"] for s in r["signals"]]},
            ensure_ascii=False) + "\n")

    print(to_markdown(r))
    if a.fail_on_defect and any(d["severity"] == "defect"
                                for d in r["defects"]):
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
