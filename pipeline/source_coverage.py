#!/usr/bin/env python3
"""
source_coverage.py
==================
Source coverage register for the FSIS corpus.

WHY THIS EXISTS
---------------
The corpus conflates two different zeros. "No recalls published by AESAN in
week 12" and "AFTS was not yet scraping AESAN in week 12" are recorded
identically — as absence. Every statistic computed over the corpus silently
treats the second as if it were the first.

The damage is measurable. RappelConso contributed 1 record in January and
165 in April; RASFF went 4 -> 130 over the same span. Total corpus volume
went 36/month -> 364/month. None of that is food safety. It is the scraper
fleet coming online. Any seasonal or trend model fitted on the raw series
learns the deployment curve and will forecast an April surge forever.

This module records, per canonical source, WHEN it was actually producing.
Downstream code asks `coverage_weeks()` instead of assuming the whole
history is observable, and baselines are computed only over weeks where
the source was live.

It is also the provenance artifact AFSDI requires. A data-faithfulness
claim is not defensible while the corpus cannot distinguish "we looked and
found nothing" from "we did not look."

WHAT IT DOES NOT DO
-------------------
It does not fabricate coverage for periods with no evidence, and it does
not attempt seasonality. Onset inference is evidence-based and conservative;
where evidence is too thin to support a claim, the source is classed
`sporadic` and excluded from baseline denominators rather than guessed at.

CANONICALISATION
----------------
48 raw Source labels resolve to far fewer real regulators. FAVV (BE) and
AFSCA are the same Belgian agency in Dutch and French. BVL (DE) and BVL are
one German office. Salute (IT), Min. Salute (IT) and Ministero della Salute
are one Italian ministry. Coverage computed on raw labels would show three
Italian sources each with near-zero continuity.

Aggregator rows ("CFS (HK) - aggregator (RappelConso FR)") are Hong Kong
re-publishing another regulator's notice. They are marked `aggregator` and
excluded from coverage accounting entirely — counting them as independent
observations double-counts the underlying event.

OUTPUT
------
docs/data/source-coverage.json     the register
pipeline/source_coverage_overrides.json  operator corrections (authority
                                         beats inference; hand-edited)

TEMPORAL STABILITY (fix 2026-08-27)
----------------------------------
Maturity was computed against the median of the MOST RECENT eight weeks —
a reference that moves every time the corpus grows. Measured by rebuilding
the register at successive corpus cut-offs, `mature_week` did not merely
drift, it OSCILLATED:

    CFIA (CA)     2025-12-29 -> 2026-01-12 -> 2026-03-16 -> 2026-01-12
                  -> 2025-12-29 -> 2026-01-12      (five moves, ~11 weeks)
    FSAI (IE)     2026-04-06 -> 2026-06-01 -> 2026-04-06
    RASFF (EU)    2026-02-09 -> 2026-03-23 -> 2026-03-30
    RappelConso   2026-02-02 -> 2026-03-16

A source could therefore be declared mature, then immature, then mature
again. Because the analytical window in TR-2026-01 opens nine weeks after
the LATEST maturity among continuous sources, that window is a function of
when the register happened to be built:

    latest maturity 2026-04-06  ->  window opens 2026-06-08  (as published)
    latest maturity 2026-06-01  ->  window opens 2026-08-03

Both values of FSAI's maturity were produced by this code on this corpus.
Under the second, SIX of the seven signal weeks in TR-2026-01 — including
the entire French Listeria episode — fall outside the defensible window.

So maturity is now FROZEN. Once determined for a source it is written to
the register with the epoch and reference median that produced it, and it
is never recomputed. Rebuilding re-derives maturity only for sources that
do not yet have a frozen value. `--verify-stability` recomputes everything
and REPORTS what would change without writing, so drift stays visible
instead of silently rewriting published history.

This mirrors the stickiness rule already used elsewhere in FSIS for
report_week: a value that has been published does not move because more
data arrived afterwards.

CLI
---
  python -m pipeline.source_coverage --build
  python -m pipeline.source_coverage --report
  python -m pipeline.source_coverage --check-week 2026-04-06
  python -m pipeline.source_coverage --verify-stability
  python -m pipeline.source_coverage --build --refreeze   (deliberate reset)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

# =============================================================================
# PATHS
# =============================================================================

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XLSX_PATH = os.path.join(REPO_ROOT, "docs", "data", "recalls.xlsx")
SHEET = "Recalls"

OUT_REGISTER = os.path.join(REPO_ROOT, "docs", "data", "source-coverage.json")
OVERRIDES_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "source_coverage_overrides.json"
)

# =============================================================================
# ONSET / OUTAGE PARAMETERS
# =============================================================================
# Onset rule: the first week W such that, over the following ONSET_WINDOW
# weeks, at least ONSET_MIN_ACTIVE contain records. This deliberately
# ignores isolated early hits — RappelConso's single January record is a
# manual entry or a one-off, not evidence the scraper was live.
ONSET_WINDOW = 6
ONSET_MIN_ACTIVE = 4

# A continuous source that goes quiet for this many consecutive weeks is
# treated as an outage (scraper break) rather than a genuine quiet period.
OUTAGE_MIN_WEEKS = 3

# Classification thresholds, measured after onset.
CONTINUOUS_ACTIVE_RATE = 0.70   # >=70% of post-onset weeks have records
INTERMITTENT_ACTIVE_RATE = 0.25
SPORADIC_MIN_RECORDS = 8        # below this, no coverage claim is made

# Volume maturity: a source can be live but underproducing (partial scrape).
# Maturity is the first week whose 4-week rolling median reaches
# MATURITY_RATIO of the median over the most recent MATURITY_REF_WEEKS.
MATURITY_REF_WEEKS = 8
MATURITY_RATIO = 0.60

# =============================================================================
# SOURCE CANONICALISATION
# =============================================================================

SOURCE_ALIASES: Dict[str, str] = {
    # Belgium — one agency, two national languages
    "favv (be)": "FAVV/AFSCA (BE)",
    "afsca": "FAVV/AFSCA (BE)",
    "afsca (be)": "FAVV/AFSCA (BE)",
    # Germany
    "bvl (de)": "BVL (DE)",
    "bvl": "BVL (DE)",
    "lebensmittelwarnung.de": "BVL (DE)",
    # Italy — one ministry, three labels
    "salute (it)": "Min. Salute (IT)",
    "min. salute (it)": "Min. Salute (IT)",
    "ministero della salute": "Min. Salute (IT)",
    # Canada
    "cfia": "CFIA (CA)",
    "mapaq qc": "MAPAQ (QC)",
    # United States
    "fda": "FDA (US)",
    "usda fsis": "USDA FSIS (US)",
    "cdc": "CDC (US)",
    # Others normalised for consistent display
    "fsa (uk)": "FSA (UK)",
    "fsai (ie)": "FSAI (IE)",
    "rappelconso (fr)": "RappelConso (FR)",
    "rasff (eu)": "RASFF (EU)",
    "efet (gr)": "EFET (GR)",
    "aesan (es)": "AESAN (ES)",
    "blv (ch)": "BLV (CH)",
    "ages (at)": "AGES (AT)",
    "nvwa (nl)": "NVWA (NL)",
    "gis (pl)": "GIS (PL)",
    "szpi (cz)": "SZPI (CZ)",
    "fsanz (au)": "FSANZ (AU)",
    "mpi (nz)": "MPI (NZ)",
    "sfa (sg)": "SFA (SG)",
    "mfds (kr)": "MFDS (KR)",
    "cfs (hk)": "CFS (HK)",
    "fda ph": "FDA (PH)",
    "vfa": "VFA (VN)",
    "cofepris (mx)": "COFEPRIS (MX)",
    "anvisa (br)": "ANVISA (BR)",
    "anmat (ar)": "ANMAT (AR)",
    "invima (co)": "INVIMA (CO)",
    "msp (uy)": "MSP (UY)",
    "ncc (za)": "NCC (ZA)",
    "comesa": "COMESA",
    "ecdc": "ECDC (EU)",
    "efsa": "EFSA (EU)",
}

# Substring markers identifying a row as a re-publication rather than a
# primary observation. These are excluded from coverage accounting.
AGGREGATOR_MARKERS = ("- aggregator (", "aggregator(")

# Substring markers for media/secondary reporting — never a coverage source.
NEWS_MARKERS = ("food safety news", "citing ", "beaconbio", "produktwarnung",
                "ilfattoalimentare", "sedaily")


def classify_raw_source(raw: object) -> Tuple[str, str]:
    """
    Return (canonical_name, kind) where kind is one of:
      primary     — a regulator publishing its own notices
      aggregator  — re-publication of another regulator's notice
      news        — media or secondary reporting
      unknown     — blank / unusable
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return "Unknown", "unknown"
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "-"):
        return "Unknown", "unknown"

    low = s.lower()

    for marker in AGGREGATOR_MARKERS:
        if marker in low:
            return s, "aggregator"
    for marker in NEWS_MARKERS:
        if marker in low:
            return s, "news"

    return SOURCE_ALIASES.get(low, s), "primary"


# =============================================================================
# REGISTER MODEL
# =============================================================================

@dataclass
class SourceCoverage:
    source: str
    kind: str                       # primary | aggregator | news
    coverage_class: str             # continuous | intermittent | sporadic | excluded
    records: int
    first_record: Optional[str]
    last_record: Optional[str]
    onset_week: Optional[str]       # first week of sustained production
    mature_week: Optional[str]      # first week at mature production VOLUME
    onset_basis: str                # inferred | override | none
    mature_basis: str = "inferred"  # inferred | override | frozen
    mature_frozen_at: Optional[str] = None   # UTC date the value was fixed
    mature_reference_median: Optional[float] = None  # reference that produced it
    active_weeks: int = 0
    observable_weeks: int = 0
    active_rate: float = 0.0
    outages: List[Dict[str, str]] = field(default_factory=list)
    aliases_seen: List[str] = field(default_factory=list)
    note: str = ""

    def is_observable(self, week: str, require_mature: bool = True) -> bool:
        """
        True if this source was live in `week`.

        `require_mature` defaults True: a live-but-underproducing source is
        NOT observable for baseline purposes, because its zero-ish weeks
        depress the baseline and manufacture a later surge.
        """
        if self.coverage_class in ("sporadic", "excluded") or not self.onset_week:
            return False
        floor = (self.mature_week or self.onset_week) if require_mature else self.onset_week
        if week < floor:
            return False
        for o in self.outages:
            if o["start"] <= week <= o["end"]:
                return False
        return True


# =============================================================================
# INFERENCE
# =============================================================================

def _week_index(weeks: List[str]) -> Dict[str, int]:
    return {w: i for i, w in enumerate(weeks)}


def infer_onset(active: Set[str], all_weeks: List[str]) -> Optional[str]:
    """
    First week beginning a sustained run of production.

    A source is credited from the first week W where at least
    ONSET_MIN_ACTIVE of the ONSET_WINDOW weeks starting at W contain
    records, AND W itself is active. Isolated early records do not
    establish coverage — they are typically manual entries or one-off
    gap-finder hits, and treating them as onset backdates coverage by
    months and reintroduces exactly the bias this module exists to remove.
    """
    for i, w in enumerate(all_weeks):
        if w not in active:
            continue
        window = all_weeks[i:i + ONSET_WINDOW]
        if len(window) < ONSET_MIN_ACTIVE:
            break
        hits = sum(1 for x in window if x in active)
        if hits >= ONSET_MIN_ACTIVE:
            return w
    return None


def infer_maturity(volumes: Dict[str, int], all_weeks: List[str],
                   onset: Optional[str]) -> Optional[str]:
    """
    First week at which the source reached its mature production VOLUME.

    Onset alone is not enough, and this is the failure that motivates the
    whole module. RappelConso was active every week from early February —
    presence-based onset credits it from then — but produced 1, then 10,
    then 29, then 165 records per month. It was live and underproducing:
    partial scraping, or a scraper hitting only one listing page.

    A baseline drawn from the active-but-immature period is depressed, so
    the mature period reads as a sustained surge. That is precisely the
    artefact that would otherwise be sold to subscribers as a food safety
    trend.

    Rule: compare a 4-week rolling median against the mature median (median
    of the most recent MATURITY_REF_WEEKS). The first week whose rolling
    median reaches MATURITY_RATIO of that reference is the maturity date.
    """
    return _infer_maturity_full(volumes, all_weeks, onset)[0]


def _infer_maturity_full(volumes: Dict[str, int], all_weeks: List[str],
                         onset: Optional[str]
                         ) -> Tuple[Optional[str], Optional[float]]:
    """As infer_maturity, but also returns the reference median used.

    The reference is recorded in the register so a frozen maturity can be
    audited: a reader can see not just WHEN a source was judged mature but
    the volume it was judged against.
    """
    if not onset:
        return None, None
    post = [w for w in all_weeks if w >= onset]
    if len(post) < MATURITY_REF_WEEKS + 4:
        # Not enough post-onset history to judge a ramp. Onset is returned
        # as a placeholder and the caller marks it PROVISIONAL — see
        # build_register. Freezing this value would be worse than not
        # freezing at all: it locks in an answer computed from evidence
        # that does not yet exist. Measured on this corpus, freezing at the
        # first opportunity fixes RASFF at 2026-02-09, six weeks before its
        # actual ramp completed.
        return onset, None

    ref_weeks = post[-MATURITY_REF_WEEKS:]
    ref_vals = sorted(volumes.get(w, 0) for w in ref_weeks)
    mid = len(ref_vals) // 2
    ref_median = (ref_vals[mid] if len(ref_vals) % 2
                  else (ref_vals[mid - 1] + ref_vals[mid]) / 2)
    if ref_median <= 0:
        return onset, None

    target = ref_median * MATURITY_RATIO
    for i in range(len(post) - 3):
        win = sorted(volumes.get(w, 0) for w in post[i:i + 4])
        m = (win[2] + win[1]) / 2
        if m >= target:
            return post[i], float(ref_median)
    return post[-1], float(ref_median)


def infer_outages(active: Set[str], all_weeks: List[str],
                  onset: Optional[str]) -> List[Dict[str, str]]:
    """Runs of >= OUTAGE_MIN_WEEKS consecutive inactive weeks after onset."""
    if not onset:
        return []
    out: List[Dict[str, str]] = []
    run: List[str] = []
    for w in all_weeks:
        if w < onset:
            continue
        if w in active:
            if len(run) >= OUTAGE_MIN_WEEKS:
                out.append({"start": run[0], "end": run[-1], "weeks": str(len(run))})
            run = []
        else:
            run.append(w)
    # A trailing gap is NOT recorded as an outage — it may simply be the
    # current quiet period, and closing it would assert an ending we cannot
    # observe yet.
    return out


def build_register(xlsx_path: str = XLSX_PATH,
                   sheet: str = SHEET,
                   refreeze: bool = False,
                   frozen: Optional[Dict[str, SourceCoverage]] = None
                   ) -> Tuple[Dict[str, SourceCoverage], Dict]:
    """Build the register.

    `frozen` supplies previously-published maturity values, which are
    reused verbatim unless `refreeze=True`. Defaults to the register on
    disk. This is what stops a published analytical cutoff from moving
    when the corpus grows — see TEMPORAL STABILITY in the module
    docstring.
    """
    df = pd.read_excel(xlsx_path, sheet)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[df["Date"].notna()].copy()
    if df.empty:
        raise SystemExit("source_coverage: no dated rows in Recalls sheet")

    df["week"] = df["Date"].dt.to_period("W-SUN").astype(str)

    resolved = df["Source"].map(classify_raw_source)
    df["source_c"] = [r[0] for r in resolved]
    df["source_kind"] = [r[1] for r in resolved]

    all_weeks = sorted(df["week"].unique())
    overrides = load_overrides()
    prior = {} if refreeze else (frozen if frozen is not None else load_register())

    register: Dict[str, SourceCoverage] = {}
    today_utc = datetime.now(timezone.utc).date().isoformat()
    n_frozen = 0

    for (name, kind), grp in df.groupby(["source_c", "source_kind"]):
        active = set(grp["week"].unique())
        n = len(grp)
        aliases = sorted({str(x) for x in grp["Source"].dropna().unique()})

        if kind in ("aggregator", "news", "unknown"):
            register[name] = SourceCoverage(
                source=name, kind=kind, coverage_class="excluded", records=n,
                first_record=str(grp["Date"].min().date()),
                last_record=str(grp["Date"].max().date()),
                onset_week=None, mature_week=None, onset_basis="none",
                active_weeks=len(active), observable_weeks=0, active_rate=0.0,
                aliases_seen=aliases,
                note=("Re-publication of another regulator's notice; excluded "
                      "from coverage accounting to avoid double-counting."
                      if kind == "aggregator" else
                      "Secondary/media reporting; not a coverage source."),
            )
            continue

        ov = overrides.get(name, {})
        onset = ov.get("onset_week")
        basis = "override" if onset else "inferred"
        if not onset:
            onset = infer_onset(active, all_weeks)
            if not onset:
                basis = "none"

        volumes = grp.groupby("week").size().to_dict()

        # ── FROZEN MATURITY ──────────────────────────────────────────────
        # Precedence: operator override > previously frozen value > fresh
        # inference. A maturity that has been written to the register has
        # been published; recomputing it against a newer reference window
        # would retroactively move the analytical cutoff derived from it.
        # Is there enough post-onset history for the judgement to mean
        # anything? Below this, maturity is provisional and stays live.
        _post_n = len([w for w in all_weeks if onset and w >= onset])
        _ripe = bool(onset) and _post_n >= (MATURITY_REF_WEEKS + 4)

        prev = prior.get(name)
        if ov.get("mature_week"):
            mature = ov["mature_week"]
            m_basis, m_at, m_ref = "override", None, None
        elif prev is not None and prev.mature_week and prev.mature_basis in ("frozen", "inferred"):
            mature = prev.mature_week
            m_basis = "frozen"
            m_at = prev.mature_frozen_at or today_utc
            m_ref = prev.mature_reference_median
            n_frozen += 1
        else:
            mature, m_ref = _infer_maturity_full(volumes, all_weeks, onset)
            if mature and not _ripe:
                # Provisional: recomputed every build until the source has
                # MATURITY_REF_WEEKS + 4 weeks of post-onset history. This
                # is the only state in which maturity may still move, and
                # it moves only for sources too young to have been used in
                # a published analysis.
                m_basis, m_at = "provisional", None
            else:
                m_basis, m_at = "inferred", (today_utc if mature else None)

        outages = ov.get("outages") or infer_outages(active, all_weeks, onset)

        if onset:
            post = [w for w in all_weeks if w >= onset]
            outage_weeks = set()
            for o in outages:
                for w in post:
                    if o["start"] <= w <= o["end"]:
                        outage_weeks.add(w)
            observable = [w for w in post if w not in outage_weeks]
            act = sum(1 for w in observable if w in active)
            rate = act / len(observable) if observable else 0.0
        else:
            observable, act, rate = [], 0, 0.0

        if n < SPORADIC_MIN_RECORDS or not onset:
            klass = "sporadic"
            note = (f"Only {n} records; too thin to establish a coverage "
                    f"window. Excluded from baseline denominators — absence "
                    f"cannot be distinguished from non-observation.")
        elif rate >= CONTINUOUS_ACTIVE_RATE:
            klass = "continuous"
            note = f"Producing in {act}/{len(observable)} observable weeks since {onset}."
        elif rate >= INTERMITTENT_ACTIVE_RATE:
            klass = "intermittent"
            note = (f"Producing in {act}/{len(observable)} observable weeks since "
                    f"{onset}. Low-volume regulator — zero weeks are plausible "
                    f"genuine quiet, so outage inference is unreliable here.")
        else:
            klass = "sporadic"
            note = (f"Active in only {act}/{len(observable)} weeks since {onset}; "
                    f"treated as sporadic.")

        if ov.get("note"):
            note = ov["note"]

        register[name] = SourceCoverage(
            source=name, kind=kind, coverage_class=klass, records=n,
            first_record=str(grp["Date"].min().date()),
            last_record=str(grp["Date"].max().date()),
            onset_week=onset, mature_week=mature, onset_basis=basis,
            mature_basis=m_basis, mature_frozen_at=m_at,
            mature_reference_median=m_ref,
            active_weeks=len(active), observable_weeks=len(observable),
            active_rate=round(rate, 3), outages=outages,
            aliases_seen=aliases, note=note,
        )

    meta = {
        "register_version": 2,
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "maturity_frozen": n_frozen,
        "maturity_provisional": len([r for r in register.values()
                                     if r.mature_basis == "provisional"]),
        "maturity_policy": (
            "Maturity is frozen once determined, EXCEPT while provisional — "
            "a source with fewer than MATURITY_REF_WEEKS + 4 weeks of "
            "post-onset history has too little evidence to fix, and "
            "freezing it early locks in a pre-ramp answer. Values already in the "
            "register are reused verbatim; only sources without one are "
            "inferred. Rebuilding therefore cannot move a published "
            "analytical cutoff. Use --verify-stability to see what a full "
            "recomputation would change, or --refreeze to reset deliberately."),
        "weeks_in_corpus": len(all_weeks),
        "first_week": all_weeks[0],
        "last_week": all_weeks[-1],
        "raw_source_labels": int(df["Source"].nunique()),
        "canonical_sources": len([r for r in register.values() if r.kind == "primary"]),
        "onset_window": ONSET_WINDOW,
        "onset_min_active": ONSET_MIN_ACTIVE,
        "outage_min_weeks": OUTAGE_MIN_WEEKS,
        "caveat": ("Coverage onset is inferred from first sustained production, "
                   "not from deployment logs. Where operator knowledge exists it "
                   "belongs in source_coverage_overrides.json, which takes "
                   "precedence over inference."),
    }
    return register, meta


# =============================================================================
# OVERRIDES / PERSISTENCE
# =============================================================================

def load_overrides(path: str = OVERRIDES_PATH) -> Dict[str, Dict]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8-sig") as fh:
            data = json.load(fh)
        return data.get("sources", data) if isinstance(data, dict) else {}
    except Exception as exc:  # noqa: BLE001
        print(f"source_coverage: WARNING could not read overrides ({exc})",
              file=sys.stderr)
        return {}


def write_register(register: Dict[str, SourceCoverage], meta: Dict,
                   path: str = OUT_REGISTER, dry_run: bool = False) -> Dict:
    payload = {
        "meta": meta,
        "sources": {k: asdict(v) for k, v in sorted(register.items())},
    }
    if dry_run:
        return payload
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="\r\n") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    return payload


def load_register(path: str = OUT_REGISTER) -> Dict[str, SourceCoverage]:
    """Read the persisted register. Returns {} if it has not been built."""
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8-sig") as fh:
        data = json.load(fh)
    out: Dict[str, SourceCoverage] = {}
    known = set(SourceCoverage.__dataclass_fields__)
    for k, v in data.get("sources", {}).items():
        # Tolerate registers written before the freeze fields existed, and
        # ignore fields added later — a register that cannot be read is a
        # register whose frozen maturities are silently lost.
        out[k] = SourceCoverage(**{kk: vv for kk, vv in v.items() if kk in known})
    return out


# =============================================================================
# PUBLIC HELPERS — consumed by signal_detector.py
# =============================================================================

def observable_sources(week: str,
                       register: Optional[Dict[str, SourceCoverage]] = None) -> Set[str]:
    """Canonical sources that were live in `week` (period string, W-SUN)."""
    reg = register if register is not None else load_register()
    return {name for name, sc in reg.items() if sc.is_observable(week)}


def coverage_weeks(source: str, weeks: List[str],
                   register: Optional[Dict[str, SourceCoverage]] = None) -> List[str]:
    """Subset of `weeks` during which `source` was live."""
    reg = register if register is not None else load_register()
    sc = reg.get(source)
    if not sc:
        return []
    return [w for w in weeks if sc.is_observable(w)]


def stable_baseline_weeks(weeks: List[str],
                          register: Optional[Dict[str, SourceCoverage]] = None,
                          min_stability: float = 0.90) -> List[str]:
    """
    Weeks over which the observing fleet was stable enough for a baseline.

    A week qualifies when the set of live continuous sources covers at least
    `min_stability` of the fleet live in the most recent week. This is what
    a detector should use instead of "all history": comparing today against
    a period when half the scrapers did not exist produces an inflation
    signal that reflects AFTS, not the food supply.
    """
    reg = register if register is not None else load_register()
    if not reg or not weeks:
        return list(weeks)

    continuous = {n for n, sc in reg.items() if sc.coverage_class == "continuous"}
    if not continuous:
        return list(weeks)

    latest = weeks[-1]
    ref = {n for n in continuous if reg[n].is_observable(latest)}
    if not ref:
        return list(weeks)

    # Weight by record volume, not source count. A week that lost BLV (12
    # records in total corpus history) is not comparably degraded to one
    # that lost RappelConso (594). Counting sources equally lets a trivial
    # scraper outage disqualify an otherwise sound baseline week — which in
    # testing collapsed 33 weeks of history down to 6.
    weight = {n: max(reg[n].records, 1) for n in ref}
    total_w = sum(weight.values())

    out = []
    for w in weeks:
        live_w = sum(weight[n] for n in ref if reg[n].is_observable(w))
        if live_w / total_w >= min_stability:
            out.append(w)
    return out


# =============================================================================
# STABILITY VERIFICATION
# =============================================================================

def verify_stability(xlsx_path: str = XLSX_PATH) -> Tuple[List[Dict], Dict]:
    """Recompute everything from scratch and report what WOULD change.

    Writes nothing. This is how maturity drift stays visible after
    freezing: the frozen value is what the register serves, and this tells
    an operator when the evidence underneath it has moved far enough to be
    worth a deliberate --refreeze.

    Returns (drifts, meta) where each drift names the source, the frozen
    value, the value a fresh computation gives, and — for continuous
    sources — the analytical window each implies.
    """
    on_disk = load_register()
    fresh, meta = build_register(xlsx_path, refreeze=True)

    drifts: List[Dict] = []
    for name, new in sorted(fresh.items()):
        old = on_disk.get(name)
        if old is None or not old.mature_week or not new.mature_week:
            continue
        if old.mature_week != new.mature_week:
            drifts.append({
                "source": name,
                "coverage_class": new.coverage_class,
                "frozen": old.mature_week,
                "recomputed": new.mature_week,
                "frozen_at": old.mature_frozen_at or "unknown",
                "direction": ("later" if new.mature_week > old.mature_week
                              else "earlier"),
            })

    def _latest(reg: Dict[str, SourceCoverage]) -> Optional[str]:
        vals = [s.mature_week for s in reg.values()
                if s.coverage_class == "continuous" and s.mature_week]
        return max(vals) if vals else None

    meta["latest_maturity_frozen"] = _latest(on_disk) if on_disk else None
    meta["latest_maturity_recomputed"] = _latest(fresh)
    meta["cutoff_would_move"] = (
        meta["latest_maturity_frozen"] is not None
        and meta["latest_maturity_frozen"] != meta["latest_maturity_recomputed"])
    return drifts, meta


def render_stability(drifts: List[Dict], meta: Dict) -> str:
    lines = ["Maturity stability check (nothing was written)", ""]
    fr = meta.get("latest_maturity_frozen") or "—"
    rc = meta.get("latest_maturity_recomputed") or "—"
    lines.append(f"  latest continuous maturity   frozen={fr}  recomputed={rc}")
    if meta.get("cutoff_would_move"):
        lines.append("  *** A FULL RECOMPUTATION WOULD MOVE THE ANALYTICAL CUTOFF ***")
        lines.append("      Published signal windows are derived from this date.")
        lines.append("      The frozen value is being served; this is a warning,")
        lines.append("      not a change. Reset deliberately with --refreeze.")
    lines.append("")
    if not drifts:
        lines.append("  no source maturity has drifted")
        return "\n".join(lines)
    lines.append(f"  {len(drifts)} source(s) would change:")
    for d in drifts:
        lines.append(f"    {d['source']:<26} {d['frozen'].split('/')[0]} -> "
                     f"{d['recomputed'].split('/')[0]}  ({d['direction']}, "
                     f"{d['coverage_class']}, frozen {d['frozen_at']})")
    return "\n".join(lines)


# =============================================================================
# REPORTING
# =============================================================================

def render_report(register: Dict[str, SourceCoverage], meta: Dict) -> str:
    lines: List[str] = []
    lines.append("FSIS source coverage register")
    lines.append(f"  corpus: {meta['weeks_in_corpus']} weeks "
                 f"({meta['first_week']} .. {meta['last_week']})")
    lines.append(f"  {meta['raw_source_labels']} raw labels -> "
                 f"{meta['canonical_sources']} canonical primary sources")
    lines.append("")

    for klass in ("continuous", "intermittent", "sporadic", "excluded"):
        group = [s for s in register.values() if s.coverage_class == klass]
        if not group:
            continue
        lines.append(f"  [{klass.upper()}]  {len(group)}")
        for s in sorted(group, key=lambda x: -x.records):
            onset = s.onset_week.split("/")[0] if s.onset_week else "—"
            mature = s.mature_week.split("/")[0] if s.mature_week else "—"
            lag = ""
            if s.onset_week and s.mature_week and s.mature_week > s.onset_week:
                lag = "  <- ramp"
            mb = {"frozen": "F", "override": "O", "inferred": "i",
                  "provisional": "p"}.get(s.mature_basis, "?")
            lines.append(f"    {s.source:<28} n={s.records:<5} onset={onset:<12} "
                         f"mature={mature:<12} rate={s.active_rate:.2f}"
                         f"  [{s.onset_basis}/{mb}]{lag}")
            if len(s.aliases_seen) > 1:
                lines.append(f"        merged labels: {', '.join(s.aliases_seen)}")
            for o in s.outages:
                lines.append(f"        OUTAGE {o['start'].split('/')[0]} .. "
                             f"{o['end'].split('/')[0]} ({o['weeks']}w)")
        lines.append("")

    lines.append("  mature basis: F=frozen (published, never recomputed)  "
                 "O=override  i=inferred this run  p=PROVISIONAL (too little "
                 "post-onset history; still moves)")
    lines.append("  Coverage onset is inferred, not logged. Correct it in")
    lines.append("  pipeline/source_coverage_overrides.json — operator")
    lines.append("  knowledge outranks inference.")
    return "\n".join(lines)


# =============================================================================
# CLI
# =============================================================================

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="FSIS source coverage register")
    ap.add_argument("--xlsx", default=XLSX_PATH)
    ap.add_argument("--build", action="store_true", help="build and write register")
    ap.add_argument("--report", action="store_true", help="print register summary")
    ap.add_argument("--check-week", default=None,
                    help="ISO date: list sources observable that week")
    ap.add_argument("--verify-stability", action="store_true",
                    help="recompute maturity and report drift; writes nothing")
    ap.add_argument("--refreeze", action="store_true",
                    help="discard frozen maturity and re-infer from scratch "
                         "(moves the analytical cutoff — deliberate use only)")
    ap.add_argument("--dry-run", action="store_true", help="write nothing")
    args = ap.parse_args(argv)

    if args.check_week and not (args.build or args.report):
        reg = load_register()
        if not reg:
            print("No register found. Run --build first.")
            return 1
        wk = str(pd.Timestamp(args.check_week).to_period("W-SUN"))
        live = sorted(observable_sources(wk, reg))
        print(f"Week {wk} — {len(live)} sources observable:")
        for s in live:
            print(f"  {s}")
        return 0

    if args.verify_stability:
        drifts, meta = verify_stability(args.xlsx)
        print(render_stability(drifts, meta))
        return 2 if meta.get("cutoff_would_move") else 0

    register, meta = build_register(args.xlsx, refreeze=args.refreeze)
    if args.refreeze:
        print("--refreeze: frozen maturity discarded and re-inferred. "
              "Any analytical window already published from the previous "
              "value no longer matches this register.")

    if args.build or not args.report:
        write_register(register, meta, dry_run=args.dry_run)
        if not args.dry_run:
            print(f"wrote {OUT_REGISTER}")

    if args.report or not args.build:
        print(render_report(register, meta))

    return 0


if __name__ == "__main__":
    sys.exit(main())
