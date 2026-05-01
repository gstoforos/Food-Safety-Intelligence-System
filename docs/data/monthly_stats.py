"""
monthly_stats.py
================

Pure-stats foundation module for the AFTS monthly report.
Imported by docs/build_monthly_report_afts.py.

Public API (referenced by the report builder):
  - compute_monthly_signals(month_recalls, prior_months, monthly_count_history) -> Dict
  - normalise_pathogen(name) -> str

The returned signals dict has exactly seven top-level keys consumed by
the builder. Each key's exact subkey shape is documented inline below
because the builder reads them with `.get(...)` defaults but the SVG
visualisations and inline-prose builders read them with `[...]` and
will KeyError if the shape drifts.

Design rules:
  - No HTML, no Claude calls, no I/O. Deterministic given the inputs.
  - Returns native Python types only (lists/dicts/ints/floats/strs/None).
  - Numeric fields are pre-rounded for clean rendering — score=12.34
    not 12.3399999. The SVG functions assume this.
  - Every count uses `int(...)` so JSON-roundtripped numpy.int64 etc
    don't poison Counter() ordering downstream.
"""
from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# ──────────────────────────────────────────────────────────────────────
# Pathogen normalisation
# ──────────────────────────────────────────────────────────────────────
# The Recalls sheet is normalised by the URL gate / Claude check, but
# scrapers still emit minor variants ("Salmonella spp.", "Salmonella spp",
# "salmonella", "Salmonella enterica", "SALMONELLA Typhimurium"). We
# collapse to a canonical genus-level token for stats & ranking, but
# preserve common subtypes (E. coli STEC variants are kept distinct
# from generic "E. coli" because the severity profile differs).
# ──────────────────────────────────────────────────────────────────────

# Canonical labels — order matters: more-specific patterns first so
# "stec" is recognised before generic "e. coli".
_PATHOGEN_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\b(stec|shiga[\s-]?toxin|vtec)\b", re.I),         "STEC / Shiga-toxin E. coli"),
    (re.compile(r"\bo\s*157\b",                       re.I),         "STEC / Shiga-toxin E. coli"),
    (re.compile(r"\b(e\.?\s*coli|escherichia\s+coli)\b", re.I),      "E. coli"),
    (re.compile(r"\bsalmonella",                       re.I),        "Salmonella spp."),
    (re.compile(r"\blisteria",                         re.I),        "Listeria monocytogenes"),
    (re.compile(r"\b(clostridium\s+botulinum|botulin|botulism)\b", re.I), "Clostridium botulinum"),
    (re.compile(r"\bclostridium\s+perfringens\b",      re.I),        "Clostridium perfringens"),
    (re.compile(r"\bbacillus\s+cereus\b",              re.I),        "Bacillus cereus"),
    (re.compile(r"\bstaph(ylococcus)?\s+aureus\b",     re.I),        "Staphylococcus aureus"),
    (re.compile(r"\bcampylobacter\b",                  re.I),        "Campylobacter spp."),
    (re.compile(r"\byersinia\b",                       re.I),        "Yersinia enterocolitica"),
    (re.compile(r"\bvibrio\s+parahaem",                re.I),        "Vibrio parahaemolyticus"),
    (re.compile(r"\bvibrio\s+cholerae\b",              re.I),        "Vibrio cholerae"),
    (re.compile(r"\bvibrio\s+vulnif",                  re.I),        "Vibrio vulnificus"),
    (re.compile(r"\bvibrio\b",                         re.I),        "Vibrio spp."),
    (re.compile(r"\bcronobacter\b",                    re.I),        "Cronobacter sakazakii"),
    (re.compile(r"\bnorovirus|norwalk\b",              re.I),        "Norovirus"),
    (re.compile(r"\bhepatitis\s*a\b",                  re.I),        "Hepatitis A"),
    (re.compile(r"\bhepatitis\s*e\b",                  re.I),        "Hepatitis E"),
    (re.compile(r"\brotavirus\b",                      re.I),        "Rotavirus"),
    (re.compile(r"\bcryptosporidium\b",                re.I),        "Cryptosporidium"),
    (re.compile(r"\bgiardia\b",                        re.I),        "Giardia"),
    (re.compile(r"\bcyclospora\b",                     re.I),        "Cyclospora"),
    (re.compile(r"\btoxoplasma\b",                     re.I),        "Toxoplasma gondii"),
    (re.compile(r"\btrichinell",                       re.I),        "Trichinella"),
    (re.compile(r"\banisaki",                          re.I),        "Anisakis"),
    (re.compile(r"\baflatoxin",                        re.I),        "Aflatoxin"),
    (re.compile(r"\bochratoxin",                       re.I),        "Ochratoxin A"),
    (re.compile(r"\bfumonisin",                        re.I),        "Fumonisin"),
    (re.compile(r"\bdeoxyniva|\bdon\b",                re.I),        "Deoxynivalenol"),
    (re.compile(r"\bzearalenone",                      re.I),        "Zearalenone"),
    (re.compile(r"\bpatulin",                          re.I),        "Patulin"),
    (re.compile(r"\bhistamine|scombroid\b",            re.I),        "Histamine"),
    (re.compile(r"\bciguatoxin",                       re.I),        "Ciguatoxin"),
    (re.compile(r"\b(lead|pb)\b",                      re.I),        "Lead (Pb)"),
    (re.compile(r"\b(cadmium|cd)\b",                   re.I),        "Cadmium (Cd)"),
    (re.compile(r"\b(mercury|hg)\b",                   re.I),        "Mercury (Hg)"),
    (re.compile(r"\b(arsenic|as)\b",                   re.I),        "Arsenic (As)"),
    (re.compile(r"\bglass\b|\bfragment",               re.I),        "Physical hazard (glass/fragment)"),
    (re.compile(r"\bmetal\s*(piece|fragment|shard)",   re.I),        "Physical hazard (metal)"),
    (re.compile(r"\bplastic\b",                        re.I),        "Physical hazard (plastic)"),
    (re.compile(r"\b(rat\s*poison|rodenticide|brodifacoum|bromadiolone)\b", re.I),
                                                                     "Rodenticide"),
]


def normalise_pathogen(name: str) -> str:
    """Collapse pathogen variants to a canonical label.

    Returns the input (stripped) if no pattern matches — the caller is
    free to do their own grouping ("Unknown" / "Other") downstream.
    """
    if not name:
        return "Unknown"
    s = str(name).strip()
    if not s:
        return "Unknown"
    for pat, canonical in _PATHOGEN_PATTERNS:
        if pat.search(s):
            return canonical
    # Title-case for consistency in non-matching cases (e.g. "BRUCELLA" → "Brucella")
    return s if any(c.isupper() for c in s[1:]) else s.title()


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
def _safe_date(r: Dict[str, Any]) -> Optional[date]:
    """Best-effort YYYY-MM-DD extraction from a recall row."""
    raw = r.get("Date") or ""
    s = str(raw)[:10]
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _is_outbreak(r: Dict[str, Any]) -> bool:
    """An 'outbreak' here = the recall mentions illness, hospitalisation,
    or death cited. The Recalls sheet's Outbreak/IsOutbreak/Outbreak_Flag
    columns are scrapers-dependent, so we accept any truthy variant.
    """
    for k in ("Outbreak", "IsOutbreak", "Outbreak_Flag", "outbreak"):
        v = r.get(k)
        if v in (True, 1, "1", "TRUE", "True", "true", "Y", "Yes", "yes"):
            return True
    # Free-text fallback — any of these tokens in the description column
    # the builders use as `body`.
    body = " ".join(
        str(r.get(k) or "")
        for k in ("Description", "Hazard", "Reason", "Issue")
    ).lower()
    if not body:
        return False
    return any(t in body for t in (
        "illness", "hospital", "death", "outbreak", "fatal", "ill people",
    ))


def _is_tier1(r: Dict[str, Any]) -> bool:
    """Tier-1 = severity classification or explicit Class I / Tier 1."""
    for k in ("Tier", "Severity", "Class", "Tier1"):
        v = str(r.get(k) or "").strip().lower()
        if v in ("1", "tier1", "tier 1", "class i", "class 1", "i"):
            return True
        if "tier-1" in v or "tier 1" in v:
            return True
    return False


def _round(x: Optional[float], ndigits: int = 2) -> Optional[float]:
    if x is None:
        return None
    try:
        return round(float(x), ndigits)
    except (TypeError, ValueError):
        return None


# ──────────────────────────────────────────────────────────────────────
# Block 1: MoM trend
# ──────────────────────────────────────────────────────────────────────
def _mom_trend(
    monthly_count_history: Sequence[Tuple[str, int]],
    current_count: int,
) -> Dict[str, Any]:
    """Month-over-month delta + Z-score against historical baseline.

    monthly_count_history is the FULL series including the current month
    as the last element (typically 12 months).

    Returns:
      values:           the input series (preserved for sparkline SVG)
      delta_pct:        percent change current vs prior month, rounded int
      direction:        "up" | "down" | "flat"
      z_score:          Z of current count vs (n-1) historical baseline
                        — may be set to None by the baseline-size guard
      z_score_raw:      same Z, never overridden (diagnostic)
      anomaly_flag:     bool — Z >= 2 (≈ 95th percentile, two-tailed)
      baseline_n:       count of months in the baseline (excludes current)
      baseline_too_narrow: bool — baseline_n < 3
    """
    series = [(str(ym), int(c)) for ym, c in monthly_count_history]
    out: Dict[str, Any] = {
        "values": series,
        "delta_pct": None,
        "direction": "flat",
        "z_score": None,
        "z_score_raw": None,
        "anomaly_flag": False,
        "baseline_n": 0,
        "baseline_too_narrow": True,
    }
    if len(series) < 2:
        return out

    *baseline, (_, last) = series
    # MoM delta vs immediately prior month
    prev = baseline[-1][1] if baseline else 0
    if prev > 0:
        delta = (last - prev) / prev * 100.0
        out["delta_pct"] = int(round(delta))
        if   delta > 5:  out["direction"] = "up"
        elif delta < -5: out["direction"] = "down"
        else:            out["direction"] = "flat"
    elif last > 0:
        out["delta_pct"] = None  # division by zero — undefined
        out["direction"] = "up"

    # Z-score vs all historical months (excluding current)
    base_counts = [c for _, c in baseline]
    out["baseline_n"] = len(base_counts)
    out["baseline_too_narrow"] = len(base_counts) < 3
    if len(base_counts) >= 2:
        mean = sum(base_counts) / len(base_counts)
        var = sum((c - mean) ** 2 for c in base_counts) / max(1, len(base_counts) - 1)
        std = math.sqrt(var)
        if std > 1e-9:
            z = (last - mean) / std
            out["z_score"]     = _round(z, 1)
            out["z_score_raw"] = _round(z, 1)
            out["anomaly_flag"] = abs(z) >= 2.0
    return out


# ──────────────────────────────────────────────────────────────────────
# Block 2: Hotspot — Country × Pathogen heatmap with chi-square residuals
# ──────────────────────────────────────────────────────────────────────
def _hotspot(
    month_recalls: List[Dict[str, Any]],
    max_rows: int = 8,
    max_cols: int = 6,
) -> Dict[str, Any]:
    """Country × Pathogen contingency matrix + standardised residuals.

    A "hotspot" cell is one whose standardised residual (observed − expected)
    / sqrt(expected) is ≥ 2.0 — roughly the 5% tail of the chi-square
    null. The builder's heatmap SVG outlines these in red.

    Returns:
      row_labels:  top-N countries by total this month
      col_labels:  top-M pathogens by total this month
      matrix:      [[{observed, expected, stdres, ratio}, ...], ...]
                   — same shape as (rows × cols)
      hotspots:    flat list of cells with stdres ≥ 2, each:
                     {country, pathogen, observed, expected, stdres, ratio}
                   sorted by stdres descending
    """
    rows: Dict[str, int] = Counter()
    cols: Dict[str, int] = Counter()
    cell: Dict[Tuple[str, str], int] = Counter()

    for r in month_recalls:
        country = (r.get("Country") or "Unknown").strip() or "Unknown"
        path    = normalise_pathogen(r.get("Pathogen") or "")
        if not path or path == "Unknown":
            continue
        rows[country] += 1
        cols[path]    += 1
        cell[(country, path)] += 1

    if not rows or not cols:
        return {"row_labels": [], "col_labels": [], "matrix": [], "hotspots": []}

    row_labels = [c for c, _ in rows.most_common(max_rows)]
    col_labels = [p for p, _ in cols.most_common(max_cols)]

    # Recompute marginals OVER THE TRUNCATED FRAME ONLY so expected
    # values match the cells the SVG actually displays.
    grand = sum(cell[(rl, cl)] for rl in row_labels for cl in col_labels)
    if grand == 0:
        return {
            "row_labels": row_labels,
            "col_labels": col_labels,
            "matrix":     [[{"observed": 0, "expected": 0.0, "stdres": 0.0, "ratio": 0.0}
                            for _ in col_labels] for _ in row_labels],
            "hotspots":   [],
        }
    row_totals = {rl: sum(cell[(rl, cl)] for cl in col_labels) for rl in row_labels}
    col_totals = {cl: sum(cell[(rl, cl)] for rl in row_labels) for cl in col_labels}

    matrix: List[List[Dict[str, Any]]] = []
    hotspots: List[Dict[str, Any]] = []
    for rl in row_labels:
        rowcells = []
        for cl in col_labels:
            obs = int(cell[(rl, cl)])
            exp = (row_totals[rl] * col_totals[cl]) / grand if grand else 0.0
            stdres = (obs - exp) / math.sqrt(exp) if exp > 0 else 0.0
            ratio  = obs / exp if exp > 0 else 0.0
            entry = {
                "observed": obs,
                "expected": _round(exp, 2) or 0.0,
                "stdres":   _round(stdres, 2) or 0.0,
                "ratio":    _round(ratio, 2) or 0.0,
            }
            rowcells.append(entry)
            if stdres >= 2.0 and obs >= 3:  # require ≥3 obs to suppress dust
                hotspots.append({
                    "country":  rl,
                    "pathogen": cl,
                    "observed": obs,
                    "expected": entry["expected"],
                    "stdres":   entry["stdres"],
                    "ratio":    entry["ratio"],
                })
        matrix.append(rowcells)

    hotspots.sort(key=lambda h: h["stdres"], reverse=True)
    return {
        "row_labels": row_labels,
        "col_labels": col_labels,
        "matrix":     matrix,
        "hotspots":   hotspots,
    }


# ──────────────────────────────────────────────────────────────────────
# Block 3: Cluster — outbreak event clustering
# ──────────────────────────────────────────────────────────────────────
def _cluster(
    month_recalls: List[Dict[str, Any]],
    span_days: int = 14,
    min_size: int = 3,
) -> Dict[str, Any]:
    """Group outbreak events that share a pathogen and fall within a
    rolling `span_days` window into 'clusters'. A cluster needs ≥ min_size
    events to register.

    Returns:
      events:        full list of outbreak events this month
                     (each = {date, pathogen, country, company})
      event_count:   len(events)
      clusters:      [{pathogen, size, span_days, start, end, countries:[...]}, ...]
                     sorted by size desc
      cluster_count: len(clusters)
    """
    events: List[Dict[str, Any]] = []
    for r in month_recalls:
        if not _is_outbreak(r):
            continue
        d = _safe_date(r)
        if not d:
            continue
        events.append({
            "date":     d.isoformat(),
            "pathogen": normalise_pathogen(r.get("Pathogen") or ""),
            "country":  (r.get("Country") or "Unknown").strip() or "Unknown",
            "company":  (r.get("Company") or "Unknown").strip() or "Unknown",
            "_d":       d,  # kept for clustering, stripped before return
        })

    by_pathogen: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for e in events:
        by_pathogen[e["pathogen"]].append(e)

    clusters: List[Dict[str, Any]] = []
    for pathogen, evs in by_pathogen.items():
        if len(evs) < min_size:
            continue
        evs_sorted = sorted(evs, key=lambda x: x["_d"])
        # Single-pass greedy: a cluster starts at evs_sorted[i]; extend
        # while subsequent dates are within span_days. When the chain
        # breaks we emit a cluster (if size threshold met) and continue.
        i = 0
        n = len(evs_sorted)
        while i < n:
            start_e = evs_sorted[i]
            j = i
            while j + 1 < n and (evs_sorted[j + 1]["_d"] - start_e["_d"]).days <= span_days:
                j += 1
            chain = evs_sorted[i:j + 1]
            if len(chain) >= min_size:
                clusters.append({
                    "pathogen":  pathogen,
                    "size":      len(chain),
                    "span_days": (chain[-1]["_d"] - chain[0]["_d"]).days,
                    "start":     chain[0]["date"],
                    "end":       chain[-1]["date"],
                    "countries": sorted({e["country"] for e in chain}),
                })
                i = j + 1
            else:
                i += 1

    clusters.sort(key=lambda c: c["size"], reverse=True)

    # Strip the internal _d field from events before returning
    for e in events:
        e.pop("_d", None)

    return {
        "events":        events,
        "event_count":   len(events),
        "clusters":      clusters,
        "cluster_count": len(clusters),
    }


# ──────────────────────────────────────────────────────────────────────
# Block 4: Concentration — Gini + HHI
# ──────────────────────────────────────────────────────────────────────
def _gini(values: List[int]) -> float:
    """Gini coefficient on a list of non-negative integer counts."""
    n = len(values)
    if n == 0:
        return 0.0
    s = sum(values)
    if s == 0:
        return 0.0
    sorted_v = sorted(values)
    cum = 0
    for i, v in enumerate(sorted_v, start=1):
        cum += i * v
    return (2 * cum) / (n * s) - (n + 1) / n


def _hhi(values: List[int]) -> float:
    """Herfindahl-Hirschman Index — sum of squared shares × 10000."""
    s = sum(values)
    if s == 0:
        return 0.0
    return sum((v / s) ** 2 for v in values) * 10000


def _bucket_gini(g: float) -> str:
    if g >= 0.6:  return "highly concentrated"
    if g >= 0.4:  return "concentrated"
    if g >= 0.25: return "moderately diffuse"
    return "highly diffuse"


def _bucket_hhi(h: float) -> str:
    if h >= 2500: return "highly concentrated"
    if h >= 1500: return "moderately concentrated"
    return "competitive / diffuse"


def _concentration(month_recalls: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Concentration of recalls across countries (for HHI/Gini buckets)."""
    country_counts = Counter(
        (r.get("Country") or "Unknown").strip() or "Unknown"
        for r in month_recalls
    )
    source_counts = Counter(
        (r.get("Source") or r.get("Agency") or "Unknown").strip() or "Unknown"
        for r in month_recalls
    )
    cvals = list(country_counts.values())
    svals = list(source_counts.values())
    g = _gini(cvals)
    h = _hhi(cvals)
    return {
        "n_countries": len(country_counts),
        "n_sources":   len(source_counts),
        "gini":        _round(g, 3) or 0.0,
        "gini_bucket": _bucket_gini(g),
        "hhi":         _round(h, 0) or 0.0,
        "hhi_bucket":  _bucket_hhi(h),
        "top_country_share": _round(
            max(cvals) / sum(cvals) if cvals and sum(cvals) else 0.0, 3
        ) or 0.0,
    }


# ──────────────────────────────────────────────────────────────────────
# Block 5: Growth — emerging vs declining pathogens
# ──────────────────────────────────────────────────────────────────────
def _growth(
    month_recalls: List[Dict[str, Any]],
    prior_months: List[List[Dict[str, Any]]],
    min_count_for_emerging: int = 3,
) -> Dict[str, Any]:
    """For each pathogen, compare current-month share to historical mean
    share. An "emerging" pathogen has Z ≥ 2 against the historical share
    distribution AND ≥ min_count_for_emerging absolute count this month.

    Returns:
      emerging:   list of {pathogen, count, share_current, share_hist_mean,
                            z_score, growth_pct}, sorted by Z desc
      declining:  same shape, sorted by Z asc (most negative first)
    """
    cur_counts = Counter(normalise_pathogen(r.get("Pathogen") or "") for r in month_recalls)
    cur_total = sum(cur_counts.values())
    if cur_total == 0:
        return {"emerging": [], "declining": []}

    # Build per-pathogen historical share series
    hist_shares: Dict[str, List[float]] = defaultdict(list)
    for cohort in prior_months:
        ccounts = Counter(normalise_pathogen(r.get("Pathogen") or "") for r in cohort)
        ctotal  = sum(ccounts.values()) or 1
        all_paths = set(cur_counts) | set(ccounts)
        for p in all_paths:
            hist_shares[p].append(ccounts.get(p, 0) / ctotal)

    def _entry_for(p: str) -> Optional[Dict[str, Any]]:
        cur = cur_counts.get(p, 0)
        cur_share = cur / cur_total if cur_total else 0.0
        hist = hist_shares.get(p, [])
        if not hist:
            return None
        hist_mean = sum(hist) / len(hist)
        z: Optional[float] = None
        if len(hist) >= 2:
            var = sum((s - hist_mean) ** 2 for s in hist) / max(1, len(hist) - 1)
            sd = math.sqrt(var)
            if sd > 1e-9:
                z = (cur_share - hist_mean) / sd
        # Growth % = current count vs historical mean count
        hist_count_mean = hist_mean * (sum(c for cohort in prior_months for c in
                                         [sum(Counter(normalise_pathogen(r.get("Pathogen") or "") for r in cohort).values())]) / max(1, len(prior_months)))
        growth_pct = None
        if hist_count_mean > 0:
            growth_pct = int(round((cur - hist_count_mean) / hist_count_mean * 100))
        return {
            "pathogen":         p,
            "count":            cur,
            "share_current":    _round(cur_share, 3) or 0.0,
            "share_hist_mean":  _round(hist_mean, 3) or 0.0,
            "z_score":          _round(z, 1) if z is not None else None,
            "growth_pct":       growth_pct,
        }

    emerging: List[Dict[str, Any]] = []
    declining: List[Dict[str, Any]] = []
    for p in set(cur_counts) | set(hist_shares):
        e = _entry_for(p)
        if e is None or e["z_score"] is None:
            continue
        if e["z_score"] >= 2.0 and e["count"] >= min_count_for_emerging:
            emerging.append(e)
        elif e["z_score"] <= -1.5 and e["share_hist_mean"] >= 0.05:
            declining.append(e)

    emerging.sort(key=lambda x: x["z_score"], reverse=True)
    declining.sort(key=lambda x: x["z_score"])
    return {"emerging": emerging, "declining": declining}


# ──────────────────────────────────────────────────────────────────────
# Block 6: Severity index
# ──────────────────────────────────────────────────────────────────────
def _severity(month_recalls: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Composite severity index 0–100.

    Inputs:
      tier1_share  — fraction of recalls labelled Tier-1 / Class I
      outbreak_share — fraction of recalls with illness/hospital/death
      tier1_count, outbreak_count — absolute counts for the gauge label

    Score weighting: 60% tier1_share + 40% outbreak_share, all on 0–1
    scale, then × 100. Buckets:
      ≥ 65 critical | ≥ 45 elevated | ≥ 25 moderate | < 25 low
    """
    n = len(month_recalls)
    if n == 0:
        return {"score": 0.0, "bucket": "low",
                "tier1_count": 0, "outbreak_count": 0,
                "tier1_share": 0.0, "outbreak_share": 0.0}

    tier1_count    = sum(1 for r in month_recalls if _is_tier1(r))
    outbreak_count = sum(1 for r in month_recalls if _is_outbreak(r))
    tier1_share    = tier1_count / n
    outbreak_share = outbreak_count / n

    score = 100 * (0.6 * tier1_share + 0.4 * outbreak_share)
    if   score >= 65: bucket = "critical"
    elif score >= 45: bucket = "elevated"
    elif score >= 25: bucket = "moderate"
    else:             bucket = "low"

    return {
        "score":          _round(score, 1) or 0.0,
        "bucket":         bucket,
        "tier1_count":    tier1_count,
        "outbreak_count": outbreak_count,
        "tier1_share":    _round(tier1_share, 3) or 0.0,
        "outbreak_share": _round(outbreak_share, 3) or 0.0,
    }


# ──────────────────────────────────────────────────────────────────────
# Block 7: Cadence — weekly count distribution within the month
# ──────────────────────────────────────────────────────────────────────
def _cadence(month_recalls: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Bin the month's recalls into ISO weeks and report mean / std / peak.

    Returns:
      weeks:    list of {label, count} sorted by week start, e.g.
                  [{"label": "2026-W14", "count": 12}, ...]
      mean:     mean count per week (rounded 1 dp)
      std:      sample std (rounded 1 dp)
      peak_wk:  label of the busiest week or None
    """
    wk_counts: Counter = Counter()
    for r in month_recalls:
        d = _safe_date(r)
        if d is None:
            continue
        iso_year, iso_week, _ = d.isocalendar()
        wk_counts[f"{iso_year}-W{iso_week:02d}"] += 1

    if not wk_counts:
        return {"weeks": [], "mean": 0.0, "std": 0.0, "peak_wk": None}

    weeks_sorted = sorted(wk_counts.items())
    weeks = [{"label": lbl, "count": int(c)} for lbl, c in weeks_sorted]
    counts = [w["count"] for w in weeks]
    mean = sum(counts) / len(counts)
    var = sum((c - mean) ** 2 for c in counts) / max(1, len(counts) - 1)
    std = math.sqrt(var)
    peak_wk = max(weeks, key=lambda w: w["count"])["label"]

    return {
        "weeks":   weeks,
        "mean":    _round(mean, 1) or 0.0,
        "std":     _round(std, 1) or 0.0,
        "peak_wk": peak_wk,
    }


# ──────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────
def compute_monthly_signals(
    month_recalls: List[Dict[str, Any]],
    prior_months: List[List[Dict[str, Any]]],
    monthly_count_history: Sequence[Tuple[str, int]],
) -> Dict[str, Any]:
    """Bundle all seven analytical signal blocks into a single dict.

    Parameters
    ----------
    month_recalls : list of recall dicts for the current month
    prior_months  : list of cohorts (each a list of recall dicts) for
                    each historical month being used as baseline
                    for the growth Z-score
    monthly_count_history : ordered series of (year-month, count) tuples
                    INCLUDING the current month as the last element.
                    Used by the MoM trend block.
    """
    return {
        "mom_trend":     _mom_trend(monthly_count_history, len(month_recalls)),
        "hotspot":       _hotspot(month_recalls),
        "cluster":       _cluster(month_recalls),
        "concentration": _concentration(month_recalls),
        "growth":        _growth(month_recalls, prior_months),
        "severity":      _severity(month_recalls),
        "cadence":       _cadence(month_recalls),
    }


__all__ = ["compute_monthly_signals", "normalise_pathogen"]
