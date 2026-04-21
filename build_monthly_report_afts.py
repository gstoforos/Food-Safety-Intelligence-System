"""
AFTS Monthly Report — Statistical Layer
========================================

Pure-statistics module consumed by build_monthly_report_afts.py. Every
function takes recall rows (or a multi-month history) and returns a plain
dict — no HTML, no Claude calls, no side effects. This separation lets us:

  - Unit-test every metric deterministically.
  - Share the same analytics layer between monthly, quarterly, and yearly
    builders (when those exist).
  - Run stats once up front and feed the results to the AI prompt as
    authoritative signals (so Claude narrates the findings instead of
    attempting to re-derive them from raw counts).

Design notes
------------
* Every metric comes with its interpretation rule baked into the docstring.
* We never publish F-value / D-value targets or any engagement-grade
  engineering deliverables — briefing content only.
* Z-scores use the 3-month rolling mean and sample stdev. When sample size
  is too small for a meaningful Z (<3 prior months), we return z=None rather
  than a garbage number so the builder can render "insufficient history"
  instead of a fake anomaly flag.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from math import log, sqrt
from statistics import mean, pstdev, stdev
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Pathogen normalisation — mirrors the dashboard's normalizePathogen() so
# weekly, monthly, and dashboard agree on pathogen grouping.
# ---------------------------------------------------------------------------
def normalise_pathogen(raw: str) -> str:
    p = (raw or "").strip().lower()
    if not p:
        return "Unknown"
    # Defensive: anything >80 chars is boilerplate leakage, not a pathogen.
    # Group it into Unknown so monthly stats don't count it as a distinct
    # hazard class.
    if len(p) > 80:
        return "Unknown"
    if "listeria"    in p:                             return "Listeria"
    if "salmonella"  in p:                             return "Salmonella"
    if "e. coli" in p or "stec" in p or "o157" in p:   return "E. coli / STEC"
    if "botulin"     in p or "clostridium" in p:       return "C. botulinum"
    if "cereulide"   in p or "bacillus cereus" in p:   return "Bacillus cereus / Cereulide"
    if "norovirus"   in p:                             return "Norovirus"
    if "aflatoxin"   in p:                             return "Aflatoxin"
    if "ochratoxin"  in p:                             return "Ochratoxin A"
    if "patulin"     in p:                             return "Patulin"
    if "hepatit"     in p:                             return "Hepatitis A"
    if "rotavirus"   in p:                             return "Rotavirus"
    if "campylobact" in p:                             return "Campylobacter"
    if "cronobacter" in p:                             return "Cronobacter"
    if "vibrio"      in p:                             return "Vibrio"
    if "staphyloco"  in p:                             return "Staphylococcus aureus"
    if "yersinia"    in p:                             return "Yersinia"
    if "shigella"    in p:                             return "Shigella"
    if "histamine"   in p or "scombro" in p:           return "Histamine / scombrotoxin"
    if "biotoxin"    in p or "shellfish" in p or \
       "saxitoxin"   in p or "domoic" in p:            return "Marine biotoxins"
    if "cyclospora"  in p:                             return "Cyclospora"
    if "toxoplasma"  in p:                             return "Toxoplasma"
    if "rodenticide" in p or "rat poison" in p or \
       "bromadiolon" in p:                             return "Rodenticide"
    if "heavy metal" in p or "lead contamin" in p or \
       "cadmium" in p or "arsenic" in p or "mercury" in p: return "Heavy metals"
    if "glass fragm" in p or "metal fragm" in p or \
       "plastic fragm" in p or "foreign" in p:         return "Physical contaminants"
    # Short, plausibly-pathogen-like fallback only.
    cleaned = raw.split("(")[0].split(" spp")[0].strip()
    return cleaned if (cleaned and len(cleaned) <= 60) else "Unknown"


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v) if v not in (None, "") else default
    except (ValueError, TypeError):
        return default


def _iso_date(r: Dict) -> Optional[date]:
    d = str(r.get("Date", "") or "")[:10]
    if not d:
        return None
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except ValueError:
        return None


# ===========================================================================
# 1. MoM TREND + ANOMALY Z-SCORE
# ===========================================================================
def compute_mom_trend(monthly_counts: List[Tuple[str, int]]) -> Dict[str, Any]:
    """
    Month-over-month trend analysis. Input is a chronologically ordered list
    of (YYYY-MM, count) tuples — most recent month LAST. Works with as few
    as 2 months (degenerates gracefully).

    Output keys:
        values          — the same input list, preserved for sparkline render
        current         — recall count for the latest month
        prior           — recall count for the immediately prior month
        delta_abs       — current - prior
        delta_pct       — % change vs prior; None if prior is 0
        rolling_mean    — mean of all months EXCLUDING current
        rolling_std     — sample stdev of those prior months (None if n<2)
        z_score         — (current − rolling_mean) / rolling_std; None if
                          rolling_std is None or 0
        direction       — "up" | "down" | "flat"
        anomaly_flag    — True when |z| > 2 (standard stats threshold for
                          "statistically unusual")
        interpretation  — short human-readable summary for the AI prompt
    """
    if not monthly_counts:
        return {"error": "no_data"}

    values   = list(monthly_counts)
    current  = values[-1][1]
    prior    = values[-2][1] if len(values) >= 2 else 0
    history  = [c for _, c in values[:-1]]  # all months except current

    delta_abs = current - prior
    delta_pct = round((delta_abs / prior) * 100, 1) if prior else None

    rolling_mean = round(mean(history), 2) if history else None
    rolling_std  = round(stdev(history), 2) if len(history) >= 2 else None

    if rolling_mean is not None and rolling_std and rolling_std > 0:
        z_score = round((current - rolling_mean) / rolling_std, 2)
    else:
        z_score = None

    if   delta_abs > 0: direction = "up"
    elif delta_abs < 0: direction = "down"
    else:               direction = "flat"

    anomaly_flag = z_score is not None and abs(z_score) > 2
    interpretation = _mom_interpret(current, prior, delta_pct, z_score, anomaly_flag)

    return {
        "values":         values,
        "current":        current,
        "prior":          prior,
        "delta_abs":      delta_abs,
        "delta_pct":      delta_pct,
        "rolling_mean":   rolling_mean,
        "rolling_std":    rolling_std,
        "z_score":        z_score,
        "direction":      direction,
        "anomaly_flag":   anomaly_flag,
        "interpretation": interpretation,
    }


def _mom_interpret(cur, prior, delta_pct, z, anomaly):
    parts = [f"{cur} recalls"]
    if delta_pct is not None:
        sign = "+" if delta_pct > 0 else ""
        parts.append(f"{sign}{delta_pct}% vs prior month")
    if z is not None:
        parts.append(f"Z={z:+.1f}")
    if anomaly:
        parts.append("anomalous (|Z|>2)")
    return " · ".join(parts)


# ===========================================================================
# 2. COUNTRY × PATHOGEN HOTSPOT MATRIX
# ===========================================================================
def compute_hotspot_matrix(month_recalls: List[Dict],
                           top_countries: int = 6,
                           top_pathogens: int = 5) -> Dict[str, Any]:
    """
    Build a (country × pathogen) contingency table for the month. Flags
    "hotspots" — cells whose observed count is >2 standard deviations above
    what you'd expect under independence (row_total × col_total / grand_total).

    This is the classic chi-square residual test applied to individual cells:
        expected[i,j] = row_total[i] * col_total[j] / N
        stdres[i,j]   = (observed - expected) / sqrt(expected)

    Cells with |stdres| > 2 are statistically over-represented (or under-) vs
    a null model of independent country/pathogen distributions.
    """
    if not month_recalls:
        return {"matrix": {}, "hotspots": [], "row_labels": [], "col_labels": []}

    # Count everything
    country_counts  = Counter()
    pathogen_counts = Counter()
    cell_counts     = defaultdict(lambda: defaultdict(int))
    for r in month_recalls:
        c = (r.get("Country") or "Unknown").strip() or "Unknown"
        p = normalise_pathogen(r.get("Pathogen") or "")
        country_counts[c]  += 1
        pathogen_counts[p] += 1
        cell_counts[c][p]  += 1

    row_labels = [c for c, _ in country_counts.most_common(top_countries)]
    col_labels = [p for p, _ in pathogen_counts.most_common(top_pathogens)]
    N = sum(country_counts.values())

    # Build shown matrix + compute standardised residual for every cell
    matrix = []
    hotspots = []
    for c in row_labels:
        row = []
        for p in col_labels:
            observed = cell_counts[c][p]
            expected = (country_counts[c] * pathogen_counts[p]) / N if N else 0
            if expected > 0:
                stdres = (observed - expected) / sqrt(expected)
            else:
                stdres = 0.0
            row.append({
                "observed": observed,
                "expected": round(expected, 2),
                "stdres":   round(stdres, 2),
                "hotspot":  observed >= 3 and stdres > 2,
            })
            if observed >= 3 and stdres > 2:
                hotspots.append({
                    "country":  c,
                    "pathogen": p,
                    "observed": observed,
                    "expected": round(expected, 2),
                    "stdres":   round(stdres, 2),
                    "ratio":    round(observed / expected, 2) if expected else None,
                })
        matrix.append(row)

    hotspots.sort(key=lambda h: -h["stdres"])

    return {
        "row_labels": row_labels,   # top countries
        "col_labels": col_labels,   # top pathogens
        "matrix":     matrix,       # [[cell, cell, ...], ...]
        "hotspots":   hotspots,     # sorted, statistically significant only
        "N":          N,
    }


# ===========================================================================
# 3. OUTBREAK TIMELINE + CLUSTER DETECTION
# ===========================================================================
def compute_cluster_timeline(month_recalls: List[Dict],
                             window_days: int = 14,
                             min_cluster_size: int = 3) -> Dict[str, Any]:
    """
    Extract outbreak events from the month and detect temporal clusters.

    Cluster definition: ≥ `min_cluster_size` outbreaks of the SAME normalised
    pathogen falling within `window_days` of each other. A sliding-window
    pass over the sorted event list identifies the clusters.

    Returned structure includes every outbreak event (for the timeline
    render) plus the list of detected clusters with start/end dates.
    """
    events = []
    for r in month_recalls:
        if _safe_int(r.get("Outbreak")) != 1:
            continue
        d = _iso_date(r)
        if not d:
            continue
        events.append({
            "date":     d.isoformat(),
            "pathogen": normalise_pathogen(r.get("Pathogen") or ""),
            "country":  r.get("Country") or "Unknown",
            "company":  r.get("Company") or "",
            "product":  (r.get("Product") or "")[:80],
            "_d":       d,
        })
    events.sort(key=lambda e: e["_d"])

    # Sliding-window clustering on same pathogen
    clusters: List[Dict[str, Any]] = []
    by_pathogen: Dict[str, List[Dict]] = defaultdict(list)
    for e in events:
        by_pathogen[e["pathogen"]].append(e)
    for pathogen, evs in by_pathogen.items():
        if len(evs) < min_cluster_size:
            continue
        # Greedy forward scan: start cluster at evs[0], extend while the next
        # event is within window_days of the cluster start.
        i = 0
        while i <= len(evs) - min_cluster_size:
            start = evs[i]["_d"]
            members = [evs[i]]
            j = i + 1
            while j < len(evs) and (evs[j]["_d"] - start).days <= window_days:
                members.append(evs[j]); j += 1
            if len(members) >= min_cluster_size:
                clusters.append({
                    "pathogen":   pathogen,
                    "size":       len(members),
                    "start":      start.isoformat(),
                    "end":        members[-1]["_d"].isoformat(),
                    "span_days":  (members[-1]["_d"] - start).days,
                    "countries":  sorted(set(m["country"] for m in members)),
                    "members":    [{k: m[k] for k in ("date","country","company","product")}
                                    for m in members],
                })
                i = j   # skip past this cluster
            else:
                i += 1

    clusters.sort(key=lambda c: -c["size"])

    # Strip the internal _d field from the output events
    out_events = [{k: e[k] for k in e if k != "_d"} for e in events]

    return {
        "events":        out_events,
        "clusters":      clusters,
        "cluster_count": len(clusters),
        "event_count":   len(events),
    }


# ===========================================================================
# 4. CONCENTRATION INDICES
# ===========================================================================
def compute_concentration_indices(month_recalls: List[Dict],
                                  history: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """
    Three classic concentration measures applied to the month's recall
    distribution.

    * Source HHI (Herfindahl-Hirschman Index) — sum of squared market shares
      across regulatory sources, scaled ×10000. US antitrust interpretation:
        < 1500  = competitive / diverse
        1500–2500 = moderately concentrated
        > 2500  = concentrated
      In our context, a month with HHI > 2500 means 1–2 agencies are driving
      the signal; < 1500 means we're picking up broad multi-agency activity.

    * Geographic Gini — standard Gini coefficient on per-country counts.
        0   = perfectly even across countries
        1   = single country holds all recalls
      > 0.6  = heavily uneven, usually a regional event (e.g. a hotspot country).

    * Tier-1 intensity ratio — the month's Tier-1 share divided by the
      rolling-history Tier-1 share (if `history` is provided and non-empty).
      1.0 = in line with baseline; >1.2 = elevated severity; <0.8 = softer.
    """
    N = len(month_recalls)
    if N == 0:
        return {"hhi_source": None, "gini_country": None,
                "tier1_intensity_ratio": None, "tier1_share": None}

    # Source HHI
    src_counts = Counter(
        (r.get("Source") or "Unknown").strip() or "Unknown"
        for r in month_recalls
    )
    shares = [c / N for c in src_counts.values()]
    hhi = round(10000 * sum(s * s for s in shares), 0)

    # Geographic Gini (on country counts)
    ctry_counts = sorted(
        Counter((r.get("Country") or "Unknown").strip() or "Unknown"
                for r in month_recalls).values()
    )
    gini = _gini(ctry_counts)

    # Tier-1 intensity vs rolling baseline
    tier1_n = sum(1 for r in month_recalls if _safe_int(r.get("Tier")) == 1)
    tier1_share = round(tier1_n / N, 3)
    baseline_share = None
    intensity_ratio = None
    if history:
        base_t1 = sum(1 for r in history if _safe_int(r.get("Tier")) == 1)
        base_n  = len(history)
        if base_n:
            baseline_share = base_t1 / base_n
            if baseline_share > 0:
                intensity_ratio = round(tier1_share / baseline_share, 2)

    # Interpretation buckets
    def bucket_hhi(h):
        if h is None: return None
        if h < 1500:  return "diverse"
        if h < 2500:  return "moderate"
        return "concentrated"
    def bucket_gini(g):
        if g is None: return None
        if g < 0.4:   return "even"
        if g < 0.6:   return "moderate"
        return "very_uneven"

    return {
        "hhi_source":             hhi,
        "hhi_bucket":             bucket_hhi(hhi),
        "gini_country":           gini,
        "gini_bucket":            bucket_gini(gini),
        "tier1_share":            tier1_share,
        "baseline_tier1_share":   round(baseline_share, 3) if baseline_share else None,
        "tier1_intensity_ratio":  intensity_ratio,
        "n_sources":              len(src_counts),
        "n_countries":            len(ctry_counts),
    }


def _gini(sorted_counts: List[int]) -> Optional[float]:
    """Standard Gini coefficient on a sorted non-negative count list."""
    n = len(sorted_counts)
    if n == 0:
        return None
    s = sum(sorted_counts)
    if s == 0:
        return 0.0
    cum = 0.0
    for i, c in enumerate(sorted_counts, start=1):
        cum += i * c
    gini = (2 * cum) / (n * s) - (n + 1) / n
    return round(gini, 3)


# ===========================================================================
# 5. PATHOGEN GROWTH — EMERGING RISK DETECTION
# ===========================================================================
def compute_pathogen_growth(month_recalls: List[Dict],
                            prior_months: List[List[Dict]],
                            min_current: int = 3) -> Dict[str, Any]:
    """
    Per-pathogen MoM growth rate + emergence flag.

    For every pathogen with ≥ `min_current` cases in the current month, we
    compute the growth rate vs the prior month and a Z-score vs the mean
    monthly share across the whole history. Pathogens whose current share
    is >2σ above their historical mean share get tagged as "emerging".

    `prior_months` is a list-of-lists: each inner list is a month's recall
    cohort, ordered OLDEST first. The current month is NOT included in
    prior_months (pass it separately as `month_recalls`).
    """
    current_counts = Counter(normalise_pathogen(r.get("Pathogen") or "")
                             for r in month_recalls)
    N_current = len(month_recalls) or 1

    # Per-month share history for each pathogen
    pathogen_history: Dict[str, List[float]] = defaultdict(list)
    pathogen_counts_history: Dict[str, List[int]] = defaultdict(list)
    for m in prior_months:
        n_m = len(m) or 1
        month_counts = Counter(normalise_pathogen(r.get("Pathogen") or "") for r in m)
        # Record a share entry for every pathogen seen anywhere in history
        all_pathogens = set(month_counts) | set(current_counts)
        for p in all_pathogens:
            pathogen_history[p].append(month_counts.get(p, 0) / n_m)
            pathogen_counts_history[p].append(month_counts.get(p, 0))

    emerging, declining, stable = [], [], []
    for p, n in current_counts.items():
        if n < min_current:
            continue
        share_now     = n / N_current
        past_shares   = pathogen_history.get(p, [])
        past_counts   = pathogen_counts_history.get(p, [])

        hist_mean_share = round(mean(past_shares), 3) if past_shares else None
        hist_std_share  = round(stdev(past_shares), 3) if len(past_shares) >= 2 else None
        z = None
        if hist_mean_share is not None and hist_std_share and hist_std_share > 0:
            z = round((share_now - hist_mean_share) / hist_std_share, 2)

        # MoM absolute-count growth vs last month
        last_count = past_counts[-1] if past_counts else 0
        growth_abs = n - last_count
        growth_pct = round((growth_abs / last_count) * 100, 1) if last_count else None

        record = {
            "pathogen":        p,
            "count":           n,
            "share_current":   round(share_now, 3),
            "share_hist_mean": hist_mean_share,
            "share_hist_std":  hist_std_share,
            "z_score":         z,
            "last_month_count": last_count,
            "growth_abs":      growth_abs,
            "growth_pct":      growth_pct,
        }
        # Tag
        if z is not None and z > 2 and growth_abs > 0:
            emerging.append(record)
        elif z is not None and z < -2 and growth_abs < 0:
            declining.append(record)
        else:
            stable.append(record)

    emerging.sort (key=lambda x: -(x["z_score"] or 0))
    declining.sort(key=lambda x:  (x["z_score"] or 0))
    stable.sort   (key=lambda x: -x["count"])

    return {"emerging": emerging, "declining": declining, "stable": stable}


# ===========================================================================
# 6. COMPOSITE SEVERITY INDEX (0–100)
# ===========================================================================
def compute_severity_composite(month_recalls: List[Dict]) -> Dict[str, Any]:
    """
    Single 0–100 severity index summarising the month, comparable across
    months once you have a few in the book. Blend:

        0.50 × Tier-1 share (0–1)
        0.30 × outbreak rate (0–1)
        0.20 × high-severity pathogen share   — defined as the share of
               recalls whose pathogen is *C. botulinum* or *Listeria*,
               the two commodity pathogens with the highest-consequence
               process-authority implications.

    Each component is scaled to 0–100, weighted, summed. Result is rounded
    to 1 decimal. Interpretation (rough):
          0–20  quiet month
         20–40  normal
         40–60  elevated
         60–80  severe
         80–100 extreme
    """
    N = len(month_recalls)
    if N == 0:
        return {"score": None, "components": {}, "bucket": None}

    tier1_share = sum(1 for r in month_recalls if _safe_int(r.get("Tier")) == 1) / N
    outbreak_rate = sum(1 for r in month_recalls if _safe_int(r.get("Outbreak")) == 1) / N
    highsev_share = sum(
        1 for r in month_recalls
        if normalise_pathogen(r.get("Pathogen") or "") in ("C. botulinum", "Listeria")
    ) / N

    score = 100 * (0.5 * tier1_share + 0.3 * outbreak_rate + 0.2 * highsev_share)
    score = round(score, 1)

    if   score < 20: bucket = "quiet"
    elif score < 40: bucket = "normal"
    elif score < 60: bucket = "elevated"
    elif score < 80: bucket = "severe"
    else:            bucket = "extreme"

    return {
        "score": score,
        "bucket": bucket,
        "components": {
            "tier1_share":   round(tier1_share, 3),
            "outbreak_rate": round(outbreak_rate, 3),
            "highsev_share": round(highsev_share, 3),
        },
    }


# ===========================================================================
# 7. WEEKLY CADENCE WITHIN MONTH (for sparkline)
# ===========================================================================
def compute_weekly_cadence(month_recalls: List[Dict]) -> Dict[str, Any]:
    """
    Break the month's recalls down by ISO week — used for the intra-month
    sparkline and for variance-of-pace diagnostics. Empty weeks are
    included so the spark line shows gaps honestly.
    """
    by_week = defaultdict(int)
    for r in month_recalls:
        d = _iso_date(r)
        if not d:
            continue
        iso_year, iso_week, _ = d.isocalendar()
        by_week[f"{iso_year}-W{iso_week:02d}"] += 1

    series = sorted(by_week.items())
    counts = [c for _, c in series]
    return {
        "weeks":   [{"label": k, "count": v} for k, v in series],
        "mean":    round(mean(counts), 2)   if counts else None,
        "std":     round(stdev(counts), 2)  if len(counts) >= 2 else None,
        "max":     max(counts) if counts else 0,
        "peak_wk": max(series, key=lambda x: x[1])[0] if series else None,
    }


# ===========================================================================
# CONVENIENCE: BUNDLE EVERYTHING FOR THE BUILDER
# ===========================================================================
def compute_monthly_signals(month_recalls: List[Dict],
                            prior_months: List[List[Dict]],
                            monthly_count_history: List[Tuple[str, int]]) -> Dict[str, Any]:
    """
    One-shot convenience wrapper that runs every metric and returns a single
    dict. The builder passes the result straight into the AI prompt template.

    Args:
        month_recalls          — current month's recall rows
        prior_months           — list of prior months' recall rows (oldest first)
        monthly_count_history  — [(YYYY-MM, count), …] INCLUDING the current
                                 month as the last element; used for MoM trend.
    """
    prior_flat = [r for m in prior_months for r in m]
    return {
        "mom_trend":     compute_mom_trend(monthly_count_history),
        "hotspot":       compute_hotspot_matrix(month_recalls),
        "cluster":       compute_cluster_timeline(month_recalls),
        "concentration": compute_concentration_indices(month_recalls, prior_flat),
        "growth":        compute_pathogen_growth(month_recalls, prior_months),
        "severity":      compute_severity_composite(month_recalls),
        "cadence":       compute_weekly_cadence(month_recalls),
    }
