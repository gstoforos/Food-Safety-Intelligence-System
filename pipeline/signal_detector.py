#!/usr/bin/env python3
"""
signal_detector.py
==================
Aberration detection over the FSIS recall corpus.

WHAT THIS IS
------------
A retrospective surveillance detector, not a forecaster. It answers
"is stratum X running above its own recent baseline this week?" — it does
NOT predict which facility or product will be recalled next. The corpus has
no denominator (no production volumes, no negative examples), so predictive
framing is not supportable and is deliberately absent from every output
string in this module.

METHOD
------
CDC EARS C1/C2/C3 (Hutwagner et al.), chosen over Farrington/Noufaily
because those require 3-5 years of baseline for their seasonal terms and
the corpus currently holds ~33 weeks. Migrate to Farrington once >=3 years
of history exists; the stratum contract below is designed to survive that
swap without changing callers.

  C1  baseline = 7 weeks immediately preceding the test week
  C2  baseline = 7 weeks, offset by a 2-week guard band (protects the
      baseline from contamination by a slow-onset event already underway)
  C3  sum of the last 3 C2 excesses — catches sustained low-grade drift
      that no single week would trip

TWO CHANNELS
------------
  count       raw weekly count vs baseline mean/sd. Diagnostic only.
  proportion  stratum's SHARE of that week's total corpus, exact binomial
              vs pooled baseline share. THIS is the alarming channel.

Rationale: RappelConso (FR) and RASFF (EU) together supply ~75% of all
records. When a publisher dumps a backlog, every raw count in the corpus
rises simultaneously and a count-only detector alarms on all of them at
once. Shares are invariant to publisher volume. A count-channel signal
with no matching proportion signal is treated as a publication artifact
and reported as such.

SPARSITY LADDER
---------------
Most pathogen x region cells hold <1 record/week. Testing them directly
turns 1 -> 3 into a "3x jump". Each stratum is only tested at the finest
level whose baseline mean clears MIN_BASELINE_MEAN and whose observed
count clears MIN_ABSOLUTE_COUNT; otherwise it rolls up:

  country -> region -> global

MULTIPLICITY
------------
Several hundred strata tested per run at alpha=0.01 yields several false
positives every single week, which trains the reader to ignore the feed.
Benjamini-Hochberg FDR is applied across all strata within a run, and the
output is hard-capped at MAX_SIGNALS by effect size.

OUTPUT
------
docs/data/signals-latest.json    current run
docs/data/signals-history.jsonl  append-only, one run per line

Pipeline placement: after merge_master lands rows in the Recalls sheet.
Reports read the Recalls sheet ONLY — this module honours that.

CLI
---
  python -m pipeline.signal_detector
  python -m pipeline.signal_detector --asof 2026-07-24
  python -m pipeline.signal_detector --backtest --backtest-out review/signal_backtest.csv
  python -m pipeline.signal_detector --dry-run
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd

# =============================================================================
# CONFIGURATION
# =============================================================================

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XLSX_PATH = os.path.join(REPO_ROOT, "docs", "data", "recalls.xlsx")
SHEET = "Recalls"

OUT_LATEST = os.path.join(REPO_ROOT, "docs", "data", "signals-latest.json")
OUT_HISTORY = os.path.join(REPO_ROOT, "docs", "data", "signals-history.jsonl")

# --- EARS windows -----------------------------------------------------------
BASELINE_WEEKS = 7          # EARS standard
GUARD_WEEKS = 2             # C2/C3 guard band
C3_SPAN = 3                 # weeks summed for C3

# --- Alarm gates ------------------------------------------------------------
MIN_ABSOLUTE_COUNT = 4      # never alarm below this many records in the week
MIN_BASELINE_MEAN = 1.0     # finest level must average >=1/week to be tested
MIN_BASELINE_NONZERO = 3    # >=3 non-zero weeks in baseline, else roll up
ALPHA = 0.01                # per-test, before FDR
FDR_Q = 0.10                # Benjamini-Hochberg false discovery rate
MAX_SIGNALS = 12            # hard cap on reported signals per run

# Publisher concentration: if one Source contributes more than this share of
# a stratum's alarm week, the signal is annotated as publisher-driven.
PUBLISHER_DOMINANCE = 0.70

# =============================================================================
# PATHOGEN CANONICALISATION
# =============================================================================
# The Pathogen column carries free-ish text from many regulators in several
# languages. Uncorrected, one real cluster splits across several cells and
# each fragment falls below MIN_ABSOLUTE_COUNT. Order matters: first match
# on the lowercased string wins, so put specific patterns before generic.
#
# This is deliberately conservative. It merges serovars into their species
# (Salmonella Enteritidis -> Salmonella) because serovar is not reliably
# populated across sources; splitting on a field that is 90% blank produces
# a detector that tracks reporting completeness, not biology.

PATHOGEN_RULES: Tuple[Tuple[Tuple[str, ...], str], ...] = (
    (("cereulide",), "Cereulide (B. cereus toxin)"),
    (("bacillus cereus", "b. cereus"), "Bacillus cereus"),
    (("listeria", "listeriose", "listeriosis"), "Listeria monocytogenes"),
    (("salmonell",), "Salmonella"),
    (("stec", "shiga", "o157", "o104", "o121", "o26", "o45", "o103",
      "o111", "o145", "verotox", "vtec"), "Shiga toxin-producing E. coli (STEC)"),
    (("escherichia coli", "e. coli", "e.coli"), "Escherichia coli (generic)"),
    (("botulin", "botulisme"), "Clostridium botulinum"),
    (("cronobacter", "sakazakii"), "Cronobacter sakazakii"),
    (("staphyloc", "enterotox", "entérotox"), "Staphylococcus aureus / enterotoxin"),
    (("campylobact",), "Campylobacter"),
    (("hepatitis a", "hépatite a", "hav"), "Hepatitis A"),
    (("norovirus", "norwalk"), "Norovirus"),
    (("aflatox",), "Aflatoxin"),
    (("ochratox",), "Ochratoxin"),
    (("fumonisin",), "Fumonisin"),
    (("zearalenone",), "Zearalenone"),
    (("deoxynivalenol", "don ", "vomitoxin"), "Deoxynivalenol"),
    (("patulin",), "Patulin"),
    (("mycotox",), "Mycotoxin (unspecified)"),
    (("histamin", "scombro"), "Histamine / scombrotoxin"),
    (("clostridium perfringens",), "Clostridium perfringens"),
    (("vibrio",), "Vibrio"),
    (("yersinia",), "Yersinia"),
    (("shigella",), "Shigella"),
    (("cyclospora",), "Cyclospora"),
    (("hepatitis e",), "Hepatitis E"),
    (("sildenafil", "tadalafil", "undeclared drug",
      "unauthorised substance", "unauthorized substance"),
     "Undeclared pharmaceutical adulterant"),
)


def canon_pathogen(raw: object) -> str:
    """Collapse regulator-specific pathogen spellings onto one label."""
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return "Unspecified"
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "n/a", "-"):
        return "Unspecified"
    low = s.lower()
    for needles, canonical in PATHOGEN_RULES:
        for n in needles:
            if n in low:
                return canonical
    return s


# =============================================================================
# STATISTICS
# =============================================================================

def _mean_sd(xs: List[float]) -> Tuple[float, float]:
    n = len(xs)
    if n == 0:
        return 0.0, 0.0
    m = sum(xs) / n
    if n < 2:
        return m, 0.0
    var = sum((x - m) ** 2 for x in xs) / (n - 1)
    return m, math.sqrt(max(var, 0.0))


def ears_statistic(observed: float, baseline: List[float]) -> Tuple[float, float, float]:
    """
    Return (S, mean, sd) where S is the EARS standardised excess.

    S = (observed - mean) / sd, with sd floored at 1.0 as in the CDC
    implementation. The floor stops a baseline of identical values
    (sd = 0) from producing an infinite statistic — which is the single
    most common way a naive EARS port generates garbage alarms on sparse
    surveillance data.
    """
    m, sd = _mean_sd(baseline)
    sd_eff = max(sd, 1.0)
    return (observed - m) / sd_eff, m, sd


def _log_binom_coef(n: int, k: int) -> float:
    return math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)


def binom_sf(k: int, n: int, p: float) -> float:
    """
    P(X >= k) for X ~ Binomial(n, p). Exact summation.

    Used one-sided: surveillance only cares about elevation, not decline.
    Guarded against p<=0 / p>=1 which occur when a stratum has no baseline
    presence at all.
    """
    if n <= 0:
        return 1.0
    if k <= 0:
        return 1.0
    if k > n:
        return 0.0
    p = min(max(p, 1e-12), 1.0 - 1e-12)
    total = 0.0
    for i in range(k, n + 1):
        lp = _log_binom_coef(n, i) + i * math.log(p) + (n - i) * math.log1p(-p)
        total += math.exp(lp)
    return min(max(total, 0.0), 1.0)


def benjamini_hochberg(pvals: List[float], q: float) -> List[bool]:
    """Standard BH step-up. Returns a per-test rejection mask."""
    n = len(pvals)
    if n == 0:
        return []
    order = sorted(range(n), key=lambda i: pvals[i])
    keep = [False] * n
    max_rank = -1
    for rank, idx in enumerate(order, start=1):
        if pvals[idx] <= (rank / n) * q:
            max_rank = rank
    if max_rank > 0:
        for rank, idx in enumerate(order, start=1):
            if rank <= max_rank:
                keep[idx] = True
    return keep


# =============================================================================
# DATA LOADING
# =============================================================================

@dataclass
class Corpus:
    frame: pd.DataFrame
    weeks: List[pd.Period]          # complete weeks only, ascending
    totals: Dict[pd.Period, int]    # corpus-wide count per week


def load_corpus(xlsx_path: str = XLSX_PATH, sheet: str = SHEET) -> Corpus:
    """
    Read the Recalls sheet and prepare a weekly-indexed frame.

    The trailing partial week is dropped. A week that is still being
    populated always looks like a decline, and including it would both
    suppress genuine signals and poison every baseline it later enters.
    """
    df = pd.read_excel(xlsx_path, sheet)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[df["Date"].notna()].copy()

    df["pathogen_c"] = df["Pathogen"].map(canon_pathogen)
    for col in ("Region", "Country", "Source"):
        df[col] = df[col].fillna("Unknown").astype(str).str.strip()
    df["Tier"] = pd.to_numeric(df["Tier"], errors="coerce")

    df["week"] = df["Date"].dt.to_period("W-SUN")

    all_weeks = sorted(df["week"].unique())
    if not all_weeks:
        raise SystemExit("signal_detector: no dated rows in Recalls sheet")

    # Drop the trailing week if its end date is in the future (incomplete).
    today = pd.Timestamp(datetime.now(timezone.utc).date())
    complete = [w for w in all_weeks if w.end_time.normalize() < today]
    if not complete:
        raise SystemExit("signal_detector: no complete weeks available")

    df = df[df["week"].isin(complete)].copy()
    totals = df.groupby("week").size().to_dict()
    return Corpus(frame=df, weeks=complete, totals=totals)


# =============================================================================
# STRATA
# =============================================================================

@dataclass
class Stratum:
    key: str
    level: str            # "country" | "region" | "global" | "tier"
    pathogen: Optional[str]
    region: Optional[str]
    country: Optional[str]
    tier: Optional[int]
    series: Dict[pd.Period, int] = field(default_factory=dict)

    def label(self) -> str:
        if self.level == "tier":
            return f"Tier {self.tier} · {self.region}"
        if self.level == "country":
            return f"{self.pathogen} · {self.country}"
        if self.level == "region":
            return f"{self.pathogen} · {self.region}"
        return f"{self.pathogen} · global"

    def parent_key(self) -> Optional[str]:
        if self.level == "country":
            return f"region::{self.pathogen}::{self.region}"
        if self.level == "region":
            return f"global::{self.pathogen}"
        return None


def build_strata(corpus: Corpus) -> Dict[str, Stratum]:
    """Materialise every candidate stratum with its full weekly series."""
    df = corpus.frame
    strata: Dict[str, Stratum] = {}

    def _ensure(key: str, **kw) -> Stratum:
        if key not in strata:
            strata[key] = Stratum(key=key, **kw)
        return strata[key]

    for (path, region, country), grp in df.groupby(
        ["pathogen_c", "Region", "Country"], dropna=False
    ):
        counts = grp.groupby("week").size().to_dict()

        k_country = f"country::{path}::{region}::{country}"
        s = _ensure(k_country, level="country", pathogen=path,
                    region=region, country=country, tier=None)
        for w, c in counts.items():
            s.series[w] = s.series.get(w, 0) + c

        k_region = f"region::{path}::{region}"
        s = _ensure(k_region, level="region", pathogen=path,
                    region=region, country=None, tier=None)
        for w, c in counts.items():
            s.series[w] = s.series.get(w, 0) + c

        k_global = f"global::{path}"
        s = _ensure(k_global, level="global", pathogen=path,
                    region=None, country=None, tier=None)
        for w, c in counts.items():
            s.series[w] = s.series.get(w, 0) + c

    tiered = df[df["Tier"].notna()]
    for (tier, region), grp in tiered.groupby(["Tier", "Region"]):
        k = f"tier::{int(tier)}::{region}"
        s = _ensure(k, level="tier", pathogen=None, region=region,
                    country=None, tier=int(tier))
        for w, c in grp.groupby("week").size().to_dict().items():
            s.series[w] = s.series.get(w, 0) + c

    return strata


def _window(series: Dict[pd.Period, int], weeks: List[pd.Period],
            end_idx: int, span: int, offset: int) -> List[float]:
    """Extract `span` weeks ending `offset` weeks before weeks[end_idx]."""
    hi = end_idx - offset
    lo = hi - span
    if lo < 0:
        return []
    return [float(series.get(weeks[i], 0)) for i in range(lo, hi)]


# =============================================================================
# DETECTION
# =============================================================================

@dataclass
class Signal:
    label: str
    stratum_key: str
    level: str
    pathogen: Optional[str]
    region: Optional[str]
    country: Optional[str]
    tier: Optional[int]
    week: str
    observed: int
    baseline_mean: float
    baseline_sd: float
    c1: float
    c2: float
    c3: float
    share_observed: float
    share_baseline: float
    p_value: float
    fdr_pass: bool
    effect: float
    channel: str            # "proportion" | "count-only"
    dominant_source: Optional[str]
    dominant_share: float
    note: str


def _dominant_source(corpus: Corpus, s: Stratum, week: pd.Period) -> Tuple[Optional[str], float]:
    df = corpus.frame
    m = df["week"] == week
    if s.pathogen is not None:
        m &= df["pathogen_c"] == s.pathogen
    if s.country is not None:
        m &= df["Country"] == s.country
    elif s.region is not None:
        m &= df["Region"] == s.region
    if s.tier is not None:
        m &= df["Tier"] == s.tier
    sub = df[m]
    if sub.empty:
        return None, 0.0
    vc = sub["Source"].value_counts()
    return str(vc.index[0]), float(vc.iloc[0]) / float(len(sub))


def detect(corpus: Corpus, strata: Dict[str, Stratum],
           asof: Optional[pd.Period] = None) -> Tuple[List[Signal], Dict]:
    """Run one detection pass for the week `asof` (default: latest complete)."""
    weeks = corpus.weeks
    if asof is None:
        asof = weeks[-1]
    if asof not in weeks:
        raise SystemExit(f"signal_detector: week {asof} not in corpus")
    idx = weeks.index(asof)

    need = BASELINE_WEEKS + GUARD_WEEKS + C3_SPAN
    if idx < need:
        return [], {
            "status": "insufficient_history",
            "weeks_available": idx + 1,
            "weeks_required": need + 1,
        }

    week_total = corpus.totals.get(asof, 0)
    candidates: List[Signal] = []
    suppressed_sparse = 0
    rolled_up = set()

    for key, s in strata.items():
        observed = int(s.series.get(asof, 0))

        b1 = _window(s.series, weeks, idx, BASELINE_WEEKS, 0)
        b2 = _window(s.series, weeks, idx, BASELINE_WEEKS, GUARD_WEEKS)
        if not b1 or not b2:
            continue

        mean2 = sum(b2) / len(b2)
        nonzero = sum(1 for x in b2 if x > 0)

        # --- sparsity ladder ------------------------------------------------
        if mean2 < MIN_BASELINE_MEAN or nonzero < MIN_BASELINE_NONZERO:
            suppressed_sparse += 1
            pk = s.parent_key()
            if pk:
                rolled_up.add(pk)
            continue
        if observed < MIN_ABSOLUTE_COUNT:
            suppressed_sparse += 1
            continue

        c1, _, _ = ears_statistic(observed, b1)
        c2, m2, sd2 = ears_statistic(observed, b2)

        c3 = 0.0
        for back in range(C3_SPAN):
            j = idx - back
            if j < 0:
                break
            bj = _window(s.series, weeks, j, BASELINE_WEEKS, GUARD_WEEKS)
            if not bj:
                break
            sj, _, _ = ears_statistic(float(s.series.get(weeks[j], 0)), bj)
            c3 += max(sj - 1.0, 0.0)

        # --- proportion channel ---------------------------------------------
        base_stratum = 0
        base_total = 0
        for back in range(GUARD_WEEKS, GUARD_WEEKS + BASELINE_WEEKS):
            j = idx - back
            if j < 0:
                break
            wj = weeks[j]
            base_stratum += int(s.series.get(wj, 0))
            base_total += int(corpus.totals.get(wj, 0))

        share_base = (base_stratum / base_total) if base_total else 0.0
        share_obs = (observed / week_total) if week_total else 0.0
        p = binom_sf(observed, week_total, share_base) if week_total else 1.0

        # Hard gate: nothing alarms unless THIS week sits above its own
        # baseline. Without this, C3 keeps firing for weeks after a spike
        # has passed — it sums prior excesses, so a stratum can be reported
        # as "elevated" while currently running at half its baseline. That
        # single failure mode accounted for most of the noise in the first
        # backtest and is exactly what makes readers stop trusting a feed.
        if observed <= mean2:
            continue

        prop_hit = (p <= ALPHA) and (share_obs > share_base)
        count_hit = ((c2 >= 3.0) or (c1 >= 3.0) or (c3 >= 2.0)) and (observed > mean2 * 1.2)

        if not (prop_hit or count_hit):
            continue

        src, src_share = _dominant_source(corpus, s, asof)
        if prop_hit:
            channel = "proportion"
            note = ("Elevated relative to this stratum's recent share of "
                    "corpus output.")
        else:
            channel = "count-only"
            note = ("Count elevated but share of corpus output is not — "
                    "consistent with a publisher volume change rather than "
                    "a change in the underlying pattern.")
        if src_share >= PUBLISHER_DOMINANCE and src:
            note += f" {int(round(src_share * 100))}% of this week's records in this stratum come from {src}."

        effect = (share_obs / share_base) if share_base > 0 else float(observed)

        candidates.append(Signal(
            label=s.label(), stratum_key=key, level=s.level,
            pathogen=s.pathogen, region=s.region, country=s.country,
            tier=s.tier, week=str(asof), observed=observed,
            baseline_mean=round(m2, 2), baseline_sd=round(sd2, 2),
            c1=round(c1, 2), c2=round(c2, 2), c3=round(c3, 2),
            share_observed=round(share_obs, 4),
            share_baseline=round(share_base, 4),
            p_value=p, fdr_pass=False, effect=round(effect, 2),
            channel=channel, dominant_source=src,
            dominant_share=round(src_share, 2), note=note,
        ))

    # --- multiplicity control -----------------------------------------------
    keep = benjamini_hochberg([c.p_value for c in candidates], FDR_Q)
    for c, k in zip(candidates, keep):
        c.fdr_pass = bool(k)

    surviving = [c for c in candidates
                 if c.fdr_pass or c.channel == "count-only"]

    # Collapse redundant parent/child pairs. Europe supplies ~75% of the
    # corpus, so "Listeria · Europe" and "Listeria · global" fire together
    # on the same underlying event and reporting both is padding. Keep the
    # FINER level (it is the more actionable statement) and drop the parent
    # when the child accounts for most of it — i.e. the parent carries no
    # information the child does not.
    by_key = {c.stratum_key: c for c in surviving}
    drop: set = set()
    for c in surviving:
        st = strata[c.stratum_key]
        pk = st.parent_key()
        if pk and pk in by_key:
            parent = by_key[pk]
            if parent.observed > 0 and (c.observed / parent.observed) >= 0.80:
                drop.add(pk)
    deduped = [c for c in surviving if c.stratum_key not in drop]

    deduped.sort(key=lambda c: (c.channel != "proportion", -c.effect))
    final = deduped[:MAX_SIGNALS]

    meta = {
        "status": "ok",
        "week": str(asof),
        "week_start": str(asof.start_time.date()),
        "week_end": str(asof.end_time.date()),
        "corpus_week_total": week_total,
        "strata_tested": len(candidates),
        "strata_suppressed_sparse": suppressed_sparse,
        "candidates": len(candidates),
        "after_fdr": len(surviving),
        "reported": len(final),
        "method": "EARS C1/C2/C3 + exact binomial proportion channel",
        "baseline_weeks": BASELINE_WEEKS,
        "guard_weeks": GUARD_WEEKS,
        "fdr_q": FDR_Q,
        "min_absolute_count": MIN_ABSOLUTE_COUNT,
        "advisory_only": True,
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    return final, meta


# =============================================================================
# BACKTEST
# =============================================================================

def backtest(corpus: Corpus, strata: Dict[str, Stratum]) -> pd.DataFrame:
    """
    Walk-forward replay. Every week is scored using only weeks strictly
    before it — never random cross-validation, which on time-series
    surveillance data leaks the future into the baseline and produces
    beautiful, meaningless numbers.

    There is no labelled anomaly set, so this does not produce accuracy.
    It produces the alarm ledger you review by hand: each row gets marked
    genuine or artifact, and that log becomes the calibration record.
    """
    rows = []
    need = BASELINE_WEEKS + GUARD_WEEKS + C3_SPAN
    for i, w in enumerate(corpus.weeks):
        if i < need:
            continue
        sigs, meta = detect(corpus, strata, asof=w)
        if not sigs:
            rows.append({
                "week": str(w), "label": "", "observed": "",
                "baseline_mean": "", "effect": "", "p_value": "",
                "channel": "none", "verdict": "",
            })
            continue
        for s in sigs:
            rows.append({
                "week": str(w), "label": s.label, "observed": s.observed,
                "baseline_mean": s.baseline_mean, "effect": s.effect,
                "p_value": round(s.p_value, 6), "channel": s.channel,
                "verdict": "",   # fill by hand: genuine | artifact | unclear
            })
    return pd.DataFrame(rows)


# =============================================================================
# OUTPUT
# =============================================================================

def write_outputs(signals: List[Signal], meta: Dict, dry_run: bool = False) -> Dict:
    payload = {
        "meta": meta,
        "signals": [asdict(s) for s in signals],
    }
    if dry_run:
        return payload
    os.makedirs(os.path.dirname(OUT_LATEST), exist_ok=True)
    with open(OUT_LATEST, "w", encoding="utf-8-sig", newline="\r\n") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    with open(OUT_HISTORY, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return payload


def render_console(signals: List[Signal], meta: Dict) -> str:
    lines = []
    lines.append(f"FSIS signal scan — week {meta.get('week', '?')} "
                 f"({meta.get('week_start')} to {meta.get('week_end')})")
    lines.append(f"corpus this week: {meta.get('corpus_week_total', 0)} records · "
                 f"strata tested: {meta.get('strata_tested', 0)} · "
                 f"sparse-suppressed: {meta.get('strata_suppressed_sparse', 0)}")
    lines.append("")
    if meta.get("status") != "ok":
        lines.append(f"  status: {meta.get('status')}")
        return "\n".join(lines)
    if not signals:
        lines.append("  No strata elevated above baseline this week.")
        return "\n".join(lines)
    for s in signals:
        flag = "◆" if s.channel == "proportion" else "○"
        lines.append(f"  {flag} {s.label}")
        lines.append(f"      {s.observed} records (baseline mean {s.baseline_mean}) · "
                     f"share {s.share_observed:.1%} vs {s.share_baseline:.1%} · "
                     f"x{s.effect}")
        lines.append(f"      C1 {s.c1} · C2 {s.c2} · C3 {s.c3} · p={s.p_value:.2e}")
        lines.append(f"      {s.note}")
        lines.append("")
    lines.append("  ◆ share-based signal   ○ count-only (publisher artifact suspected)")
    lines.append("  Advisory only. Not a prediction of future recalls.")
    return "\n".join(lines)


# =============================================================================
# CLI
# =============================================================================

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="FSIS recall aberration detector")
    ap.add_argument("--xlsx", default=XLSX_PATH)
    ap.add_argument("--asof", default=None,
                    help="ISO date inside the target week (default: latest complete)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print only, write nothing")
    ap.add_argument("--backtest", action="store_true",
                    help="walk-forward replay across all eligible weeks")
    ap.add_argument("--backtest-out", default=None,
                    help="CSV path for the backtest alarm ledger")
    args = ap.parse_args(argv)

    corpus = load_corpus(args.xlsx)
    strata = build_strata(corpus)

    if args.backtest:
        bt = backtest(corpus, strata)
        out = args.backtest_out or os.path.join(REPO_ROOT, "review", "signal_backtest.csv")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        bt.to_csv(out, index=False, encoding="utf-8-sig")
        fired = bt[bt["channel"] != "none"]
        weeks = bt["week"].nunique()
        print(f"backtest: {weeks} weeks replayed · {len(fired)} alarms · "
              f"{len(fired) / max(weeks, 1):.2f} alarms/week")
        print(f"ledger: {out}")
        return 0

    asof = None
    if args.asof:
        asof = pd.Timestamp(args.asof).to_period("W-SUN")

    signals, meta = detect(corpus, strata, asof=asof)
    print(render_console(signals, meta))
    write_outputs(signals, meta, dry_run=args.dry_run)
    if not args.dry_run:
        print(f"\nwrote {OUT_LATEST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
