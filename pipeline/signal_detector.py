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
              It PARTIALLY normalises changes in total weekly volume. It
              does not eliminate publisher effects: it stays sensitive to
              a change in publisher MIX, and to a publisher whose hazard
              composition differs from the corpus average. Do not describe
              shares as invariant to publisher volume.

Rationale: RappelConso (FR) and RASFF (EU) together supply ~75% of all
records. When a publisher dumps a backlog, every raw count in the corpus
rises simultaneously and a count-only detector alarms on all of them at
once. Working in shares removes most of that, but not all of it. A
count-channel signal with no matching proportion signal is therefore
reported as a potentially volume-driven publication event requiring
publisher-level review — a diagnostic record, not a finding.

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
    # bare "hav" spaced 2026-08-31 — it matched the word "have", so every
    # USDA FSIS notice ("consumers who HAVe purchased") read as Hepatitis A.
    (("hepatitis a", "hépatite a", " hav ", "(hav)", "hav virus"), "Hepatitis A"),
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
    # Carried alongside `effect`, which follows the channel. Keeping both
    # means a table can show the channel-appropriate number without the
    # other becoming unrecoverable.
    effect_share: float = 0.0
    effect_count: float = 0.0


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


# =============================================================================
# COVERAGE GATE
# =============================================================================
# TR-2026-01 §2.2 states that signals are reported only where the entire
# detection baseline falls after the latest maturity date among continuously
# collected sources. Until 2026-08-27 this module contained no reference to
# source_coverage at all — the claim was true only because the window had
# been applied by hand when the report was written.
#
# The gate below makes it a property of the code. It is ADDITIVE: when the
# register is missing or unreadable, behaviour is exactly as before, and
# signals outside the window are FLAGGED rather than dropped unless the
# caller asks for enforcement. Silently changing what the detector reports
# would be a worse cure than the disease.

def coverage_window_start(register=None) -> Optional[str]:
    """First week whose full detection baseline lies after mature coverage.

    Returns a W-SUN period string, or None when no register is available.
    The offset is BASELINE_WEEKS + GUARD_WEEKS: a test week is only
    defensible once nothing in its baseline predates mature collection.
    """
    try:
        from pipeline.source_coverage import load_register
    except Exception:                                          # noqa: BLE001
        return None
    reg = register if register is not None else load_register()
    if not reg:
        return None
    mats = [sc.mature_week for sc in reg.values()
            if sc.coverage_class == "continuous" and sc.mature_week]
    if not mats:
        return None
    latest = max(mats)
    try:
        start = pd.Period(latest.split("/")[0], freq="W-SUN")
    except Exception:                                          # noqa: BLE001
        return None
    return str(start + (BASELINE_WEEKS + GUARD_WEEKS))


def coverage_status(asof: pd.Period, register=None) -> Dict:
    """Whether `asof` sits inside the defensible analytical window."""
    ws = coverage_window_start(register)
    if ws is None:
        return {"coverage_register": "absent",
                "coverage_window_start": None,
                "within_coverage_window": None,
                "coverage_note": ("No source-coverage register found. Build it "
                                  "with `python -m pipeline.source_coverage "
                                  "--build`. Without it, signals cannot be "
                                  "distinguished from collection artefacts.")}
    inside = str(asof) >= ws
    return {
        "coverage_register": "present",
        "coverage_window_start": ws,
        "within_coverage_window": bool(inside),
        "coverage_note": ("Baseline lies wholly after mature collection."
                          if inside else
                          "PART OF THIS WEEK'S BASELINE PREDATES MATURE "
                          "COLLECTION. Any elevation here may be the scraper "
                          "fleet coming online rather than a change in the "
                          "food supply. Reported for the audit ledger; not "
                          "publishable as a finding."),
    }


def detect(corpus: Corpus, strata: Dict[str, Stratum],
           asof: Optional[pd.Period] = None,
           enforce_coverage: bool = False) -> Tuple[List[Signal], Dict]:
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
    # Strata that clear the sparsity ladder and are actually scored. Before
    # 2026-09-02 the meta field `strata_tested` reported len(candidates) —
    # the number that ALARMED — so a quiet week printed "strata tested: 0"
    # and read as a detector that had tested nothing.
    tested = 0

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
        tested += 1

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
        # Guard band, corrected 2026-08-27.
        #
        # This previously read `range(GUARD_WEEKS, ...)`, which INCLUDES the
        # week at offset GUARD_WEEKS and therefore excluded only the single
        # week immediately preceding the test week. The count channel, which
        # uses `_window(..., offset=GUARD_WEEKS)` (hi = idx - offset, range
        # lo..hi-1), excludes both. The two channels drew baselines shifted
        # by one week, and the share channel — the channel that alarms —
        # was the one that did not match the documented method.
        #
        # `GUARD_WEEKS + 1` as the start makes the share baseline
        # idx-9 .. idx-3, identical to the count baseline. Asserted in
        # tests/test_guard_band.py: if these two ever diverge again the
        # suite fails rather than a reader noticing in a published table.
        for back in range(GUARD_WEEKS + 1, GUARD_WEEKS + 1 + BASELINE_WEEKS):
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

        # ── EFFECT MUST MATCH THE CHANNEL (fix 2026-08-27) ────────────────
        # This computed the SHARE ratio for every signal, including
        # count-only ones, and the published table labelled it as observed
        # over baseline. Both cannot be true, and on the 20 signals inside
        # the analytical window the two disagree on 8.
        #
        # One of those disagreements is disqualifying rather than merely
        # mislabelled: Listeria monocytogenes / France, week of 27 July, is
        # a COUNT-only signal whose share ratio is 0.85 — below one. A
        # signals table reporting an effect of 0.85 tells the reader the
        # stratum went DOWN in the same row that says it alarmed. The
        # count ratio for that row is 1.24.
        #
        # Both are now carried. `effect` follows the channel, so the number
        # beside a COUNT signal is a count ratio and the number beside a
        # SHARE signal is a share ratio. The other value stays on the
        # record, so nothing that was published becomes unrecoverable.
        eff_share = (share_obs / share_base) if share_base > 0 else float(observed)
        eff_count = (observed / m2) if m2 > 0 else float(observed)
        effect = eff_share if channel == "proportion" else eff_count

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
            effect_share=round(eff_share, 3), effect_count=round(eff_count, 3),
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

    # The primary sort key SEPARATES the channels: share rows (False) come
    # before count-only rows (True), and `-c.effect` orders within a block.
    # A share ratio is therefore never ranked against a count ratio. This
    # is load-bearing for the warning in TR-2026-01 §5.1 and is asserted in
    # tests/test_technical_report.py rather than left to a reader's trust.
    deduped.sort(key=lambda c: (c.channel != "proportion", -c.effect))
    final = deduped[:MAX_SIGNALS]

    cov = coverage_status(asof)
    if enforce_coverage and cov.get("within_coverage_window") is False:
        final = []

    meta = {
        "status": "ok",
        "week": str(asof),
        "week_start": str(asof.start_time.date()),
        "week_end": str(asof.end_time.date()),
        "corpus_week_total": week_total,
        "strata_tested": tested,
        "strata_suppressed_sparse": suppressed_sparse,
        "candidates": len(candidates),
        "after_fdr": len(surviving),
        # `after_fdr` is pre-dedup. Without `after_dedup` a reader comparing
        # after_fdr to reported would conclude MAX_SIGNALS truncated the
        # run, when the parent/child collapse did. Record both, and say
        # outright whether the cap actually bound.
        "after_dedup": len(deduped),
        "cap_binding": bool(len(deduped) > MAX_SIGNALS),
        "reported": len(final),
        "method": "EARS C1/C2/C3 + exact binomial proportion channel",
        "baseline_weeks": BASELINE_WEEKS,
        "guard_weeks": GUARD_WEEKS,
        "fdr_q": FDR_Q,
        "min_absolute_count": MIN_ABSOLUTE_COUNT,
        "advisory_only": True,
        "coverage_enforced": bool(enforce_coverage),
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    meta.update(cov)
    if cov.get("within_coverage_window") is False and not enforce_coverage:
        meta["reported"] = len(final)
        meta["status"] = "ok_outside_coverage_window"
    return final, meta


# =============================================================================
# LIVE BOARD (added 2026-09-02)
# =============================================================================
# The alarm list answers "what is elevated this week?". A live surveillance
# page also has to answer "what is being watched, and where does each
# stratum sit against its own baseline right now?" — otherwise a quiet week
# renders as an empty page and the reader cannot tell a working detector
# from a broken one. The board reports every stratum that clears the
# sparsity ladder, alarming or not, using the SAME windows and the SAME
# thresholds as detect(); it adds no statistic of its own.

BOARD_SERIES_WEEKS = 12
OUT_BOARD = os.path.join(REPO_ROOT, "docs", "data", "signals-board.json")


def build_board(corpus: Corpus, strata: Dict[str, Stratum],
                asof: pd.Period, signals: List[Signal]) -> List[Dict]:
    weeks = corpus.weeks
    idx = weeks.index(asof)
    alarmed = {s.stratum_key: s for s in signals}
    week_total = corpus.totals.get(asof, 0)
    rows: List[Dict] = []
    sparse: List[Dict] = []
    for key, s in strata.items():
        observed = int(s.series.get(asof, 0))
        b2 = _window(s.series, weeks, idx, BASELINE_WEEKS, GUARD_WEEKS)
        if not b2:
            continue
        mean2 = sum(b2) / len(b2)
        nonzero = sum(1 for x in b2 if x > 0)
        testable = (mean2 >= MIN_BASELINE_MEAN
                    and nonzero >= MIN_BASELINE_NONZERO)
        if not testable:
            if observed > 0:
                sparse.append({
                    "label": s.label(), "stratum_key": key, "level": s.level,
                    "pathogen": s.pathogen, "region": s.region,
                    "country": s.country, "tier": s.tier,
                    "observed": observed, "baseline_mean": round(mean2, 2),
                    "baseline_nonzero": nonzero,
                    "parent_key": s.parent_key(),
                })
            continue
        c2, m2, sd2 = ears_statistic(observed, b2)
        b1 = _window(s.series, weeks, idx, BASELINE_WEEKS, 0)
        c1, m1, _ = ears_statistic(observed, b1) if b1 else (0.0, 0.0, 0.0)
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
        base_stratum = base_total = 0
        for back in range(GUARD_WEEKS + 1, GUARD_WEEKS + 1 + BASELINE_WEEKS):
            j = idx - back
            if j < 0:
                break
            base_stratum += int(s.series.get(weeks[j], 0))
            base_total += int(corpus.totals.get(weeks[j], 0))
        share_base = (base_stratum / base_total) if base_total else 0.0
        share_obs = (observed / week_total) if week_total else 0.0
        # The share-channel test, for EVERY scorable stratum — not only the
        # alarms — so the page can show where each one sits against alpha.
        p_share = (binom_sf(observed, week_total, share_base)
                   if week_total and share_base > 0 else 1.0)
        lo = max(0, idx - BOARD_SERIES_WEEKS + 1)
        series = [int(s.series.get(weeks[j], 0)) for j in range(lo, idx + 1)]
        sig = alarmed.get(key)
        if sig is not None:
            status = "elevated"
        elif observed < MIN_ABSOLUTE_COUNT:
            status = "below_floor"      # tested baseline, too few this week to score
        elif c2 >= 2.0 and observed > mean2:
            status = "watch"
        elif observed > mean2:
            status = "above"
        elif c2 <= -2.0:
            # A drop of two baseline sd is as informative as a rise. On a
            # corpus fed by ~66 scrapers it is usually the fleet going
            # quiet, not the food supply going quiet — and the difference
            # matters, because a silent scraper also suppresses every
            # alarm that week. Surfaced so a quiet board is read correctly.
            status = "below"
        else:
            status = "normal"
        src, src_share = _dominant_source(corpus, s, asof)
        rows.append({
            "label": s.label(), "stratum_key": key, "level": s.level,
            "pathogen": s.pathogen, "region": s.region, "country": s.country,
            "tier": s.tier, "observed": observed,
            "baseline_mean": round(m2, 2), "baseline_sd": round(sd2, 2),
            "baseline_values": [int(x) for x in b2],
            "c1": round(c1, 2), "c2": round(c2, 2), "c3": round(c3, 2),
            "share_observed": round(share_obs, 4),
            "share_baseline": round(share_base, 4),
            "p_share": round(p_share, 6),
            "effect_count": round(observed / m2, 2) if m2 > 0 else None,
            "effect_share": (round(share_obs / share_base, 2)
                             if share_base > 0 else None),
            "series": series, "status": status,
            "channel": sig.channel if sig else None,
            "p_value": sig.p_value if sig else None,
            "fdr_pass": sig.fdr_pass if sig else None,
            "dominant_source": src, "dominant_share": round(src_share, 2),
            "parent_key": s.parent_key(),
            "note": sig.note if sig else "",
        })
    order = {"elevated": 0, "watch": 1, "below": 2, "above": 3, "normal": 4,
             "below_floor": 5}
    rows.sort(key=lambda r: (order[r["status"]],
                             -abs(r["c2"] or 0) if r["status"] == "below"
                             else -(r["c2"] or 0)))
    sparse.sort(key=lambda r: -r["observed"])
    build_board.last_sparse = sparse       # picked up by main() for the payload
    return rows


def build_context(corpus: Corpus, asof: pd.Period) -> Dict:
    """Corpus-level context the strata tests are conditioned on.

    volume       every complete week's total, with the same EARS C2 applied
                 to the TOTAL for the test week. A low C2 here is the
                 collection-artefact warning: the share channel normalises
                 volume only partially, and a silent scraper suppresses every
                 stratum at once.
    publishers   share of the corpus by source, this week vs the baseline
                 window. The docstring at the top of this module says the
                 share channel stays sensitive to a change in publisher MIX;
                 this is that mix, made visible.
    """
    weeks = corpus.weeks
    idx = weeks.index(asof)
    tot_series = {w: int(corpus.totals.get(w, 0)) for w in weeks}
    b2 = _window(tot_series, weeks, idx, BASELINE_WEEKS, GUARD_WEEKS)
    c2, m2, sd2 = ears_statistic(float(tot_series[asof]), b2) if b2 else (0.0, 0.0, 0.0)
    df = corpus.frame
    base_weeks = [weeks[idx - back]
                  for back in range(GUARD_WEEKS + 1, GUARD_WEEKS + 1 + BASELINE_WEEKS)
                  if idx - back >= 0]
    now = df[df["week"] == asof]["Source"].value_counts()
    base = df[df["week"].isin(base_weeks)]["Source"].value_counts()
    now_n = int(now.sum()) or 1
    base_n = int(base.sum()) or 1
    srcs = list(dict.fromkeys(list(now.index[:8]) + list(base.index[:8])))[:10]
    publishers = []
    for s in srcs:
        n_now = int(now.get(s, 0))
        n_base = int(base.get(s, 0))
        publishers.append({
            "source": str(s), "now": n_now,
            "now_share": round(n_now / now_n, 4),
            "base_mean": round(n_base / max(1, len(base_weeks)), 2),
            "base_share": round(n_base / base_n, 4),
        })
    publishers.sort(key=lambda p: -p["base_share"])
    return {
        "volume": [{"week": str(w), "total": tot_series[w]} for w in weeks],
        "volume_test": {"week": str(asof), "observed": tot_series[asof],
                        "baseline_mean": round(m2, 2),
                        "baseline_sd": round(sd2, 2), "c2": round(c2, 2),
                        "baseline_values": [int(x) for x in b2]},
        "publishers": publishers,
        "baseline_weeks": [str(w) for w in base_weeks],
    }


def build_ledger(corpus: Corpus, strata: Dict[str, Stratum]) -> List[Dict]:
    """Walk-forward alarm history for the page's timeline strip. Same code
    path as backtest(); this only reshapes it to JSON."""
    out: List[Dict] = []
    need = BASELINE_WEEKS + GUARD_WEEKS + C3_SPAN
    for i, w in enumerate(corpus.weeks):
        if i < need:
            continue
        sigs, _ = detect(corpus, strata, asof=w)
        out.append({
            "week": str(w),
            "week_start": str(w.start_time.date()),
            "week_end": str(w.end_time.date()),
            "corpus_total": int(corpus.totals.get(w, 0)),
            "alarms": [{
                "label": s.label, "channel": s.channel,
                "observed": s.observed, "baseline_mean": s.baseline_mean,
                "effect": s.effect, "p_value": round(s.p_value, 6),
                "fdr_pass": s.fdr_pass, "dominant_source": s.dominant_source,
            } for s in sigs],
        })
    return out


def write_board(meta: Dict, signals: List[Signal], board: List[Dict],
                ledger: List[Dict], series_weeks: List[str],
                context: Optional[Dict] = None,
                dry_run: bool = False) -> Dict:
    payload = {
        "meta": meta,
        "series_weeks": series_weeks,
        "signals": [asdict(s) for s in signals],
        "board": board,
        "sparse": getattr(build_board, "last_sparse", []),
        "ledger": ledger,
        "context": context or {},
    }
    if dry_run:
        return payload
    os.makedirs(os.path.dirname(OUT_BOARD), exist_ok=True)
    with open(OUT_BOARD, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1)
    return payload


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
    if meta.get("within_coverage_window") is False:
        lines.append("*** OUTSIDE THE MATURE-COVERAGE WINDOW — this week's "
                     "baseline predates full collection ***")
        lines.append(f"    window opens {meta.get('coverage_window_start')}; "
                     f"signals below are audit-ledger only, not publishable.")
    elif meta.get("coverage_register") == "absent":
        lines.append("*** NO COVERAGE REGISTER — run "
                     "`python -m pipeline.source_coverage --build` ***")
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
    ap.add_argument("--enforce-coverage", action="store_true",
                    help="suppress signals whose baseline predates mature "
                         "collection, instead of flagging them (see "
                         "pipeline/source_coverage.py)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print only, write nothing")
    ap.add_argument("--board", action="store_true",
                    help="also write docs/data/signals-board.json: every "
                         "tested stratum with its baseline position, plus the "
                         "walk-forward alarm ledger, for the dashboard")
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

    signals, meta = detect(corpus, strata, asof=asof,
                           enforce_coverage=args.enforce_coverage)
    print(render_console(signals, meta))
    write_outputs(signals, meta, dry_run=args.dry_run)
    if not args.dry_run:
        print(f"\nwrote {OUT_LATEST}")
    if args.board:
        week = asof if asof is not None else corpus.weeks[-1]
        board = build_board(corpus, strata, week, signals)
        ledger = build_ledger(corpus, strata)
        i = corpus.weeks.index(week)
        lo = max(0, i - BOARD_SERIES_WEEKS + 1)
        series_weeks = [str(w) for w in corpus.weeks[lo:i + 1]]
        context = build_context(corpus, week)
        write_board(meta, signals, board, ledger, series_weeks,
                    context=context, dry_run=args.dry_run)
        by = {}
        for r in board:
            by[r["status"]] = by.get(r["status"], 0) + 1
        print(f"board: {len(board)} strata on watch · "
              + " · ".join(f"{k} {v}" for k, v in by.items())
              + f" · ledger {len(ledger)} weeks")
        if not args.dry_run:
            print(f"wrote {OUT_BOARD}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
