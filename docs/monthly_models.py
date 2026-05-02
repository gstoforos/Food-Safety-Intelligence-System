"""
monthly_models.py
=================

Predictive-modelling module for the AFTS monthly report.
Imported by docs/build_monthly_report_afts.py.

Public API:
  - run_all_models(monthly_counts, monthly_counts_by_pathogen) -> Dict
  - build_pathogen_history(monthly_cohorts) -> Dict[str, List[int]]   (helper)

Returns a dict with EXACTLY these eight keys (the builder iterates them):

    linear_trend     — OLS on total monthly counts
    poisson          — per-pathogen Poisson forecast for rare pathogens
    cusum            — Page-1954 tabular CUSUM change-point detection
    ols_seasonal     — OLS with month-of-year dummies
    stl              — STL decomposition (trend + seasonal + residual)
    holt_winters     — Level + trend + seasonal exponential smoothing
    sarima           — Seasonal ARIMA
    prophet          — Additive seasonality (Facebook Prophet)

Every entry is a dict with at least:
    status:  "active" | "insufficient_data" | "unavailable"
    message: human-readable status string (when not active)

The Poisson entry is special — its `status` is always one of those three
at the top level, but it ALSO carries `by_pathogen: {name: {...}}` where
each per-pathogen sub-dict has its own `status` field. The builder
checks both layers.

Data-requirement gates (n = months of history available):
    linear_trend:    n ≥ 3
    poisson:         n ≥ 6 per pathogen, current count ≤ rare_threshold
    cusum:           n ≥ 6
    ols_seasonal:    n ≥ 12
    stl / holt_winters / sarima / prophet:  n ≥ 24

These thresholds match the spec set when the report was first designed:
"activate when n≥12 months", and the 24-month gate for true seasonal
models. Stays deterministic — no scipy/statsmodels/sktime imports, all
math in pure Python so the GitHub Actions runner doesn't need extras.
"""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
def _round(x: Optional[float], ndigits: int = 2) -> Optional[float]:
    if x is None:
        return None
    try:
        return round(float(x), ndigits)
    except (TypeError, ValueError):
        return None


def _inactive(msg: str) -> Dict[str, Any]:
    """Standard 'not enough data' return shape."""
    return {"status": "insufficient_data", "message": msg}


def _unavailable(msg: str) -> Dict[str, Any]:
    """Standard 'optional dependency missing' return shape."""
    return {"status": "unavailable", "message": msg}


# ──────────────────────────────────────────────────────────────────────
# build_pathogen_history — helper used by the report builder
# ──────────────────────────────────────────────────────────────────────
def build_pathogen_history(
    monthly_cohorts: Sequence[Tuple[str, List[Dict[str, Any]]]],
) -> Dict[str, List[int]]:
    """Convert [(year-month, [recall, ...]), ...] into per-pathogen
    monthly count series.

    Result: {pathogen_name: [count_month_1, count_month_2, ...]} where
    the index order matches monthly_cohorts. Pathogens missing from a
    given month's cohort get a 0 in that slot.

    Uses monthly_stats.normalise_pathogen() so labels match the rest
    of the report.
    """
    # Local import to avoid circular dependency at module load
    from monthly_stats import normalise_pathogen

    # First pass: discover all pathogens that appeared in any cohort
    all_paths: set = set()
    per_month_counts: List[Counter] = []
    for _ym, cohort in monthly_cohorts:
        c = Counter(normalise_pathogen(r.get("Pathogen") or "") for r in cohort)
        # Drop "Unknown" so it doesn't pollute the predictive layer
        c.pop("Unknown", None)
        per_month_counts.append(c)
        all_paths.update(c.keys())

    # Second pass: build the dense series
    out: Dict[str, List[int]] = {}
    for p in sorted(all_paths):
        out[p] = [int(c.get(p, 0)) for c in per_month_counts]
    return out


# ──────────────────────────────────────────────────────────────────────
# Linear trend (OLS on total monthly counts)
# ──────────────────────────────────────────────────────────────────────
def _linear_trend(monthly_counts: Sequence[Tuple[str, int]]) -> Dict[str, Any]:
    """Simple OLS regression of count on month-index.

    Returns when active:
      status:               "active"
      n:                    number of months used
      slope_per_month:      OLS slope (recalls per month)
      intercept:            OLS intercept (recalls at month index 0)
      r_squared:            coefficient of determination
      slope_se:             standard error of the slope
      slope_significant:    bool — t-stat ≥ 2 (≈ 95% confidence)
      next_month_point:     point forecast for the month after the last
      next_month_ci95:      [lo, hi] 95% confidence interval (rounded ints)
      note:                 short interpretation string
    """
    if len(monthly_counts) < 3:
        return _inactive(f"Activates at n≥3 months (have {len(monthly_counts)}).")

    xs = list(range(len(monthly_counts)))
    ys = [int(c) for _, c in monthly_counts]
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    # OLS slope/intercept
    sxx = sum((x - mean_x) ** 2 for x in xs)
    sxy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    if sxx == 0:
        return _inactive("Degenerate x-axis (single time point).")
    slope = sxy / sxx
    intercept = mean_y - slope * mean_x

    # Residuals + R²
    fitted = [intercept + slope * x for x in xs]
    ss_res = sum((y - f) ** 2 for y, f in zip(ys, fitted))
    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # Slope SE + t-stat. With n-2 degrees of freedom, the critical t for a
    # two-sided 95% test is 4.30 at dof=2 (n=4), 2.78 at dof=4 (n=6),
    # 2.23 at dof=10 (n=12), 2.07 at dof=22 (n=24). The legacy "|t|≥2"
    # rule is only reasonable once dof is at least ~10, i.e. n≥12. Below
    # that, we explicitly refuse the significance claim and label the
    # estimate as exploratory — matches review-1 correction (a 4-month
    # series cannot be 95% significant at the |t|≥2 threshold).
    if n > 2:
        sigma2 = ss_res / (n - 2)
        slope_se = math.sqrt(sigma2 / sxx) if sxx > 0 else 0.0
    else:
        sigma2 = 0.0
        slope_se = 0.0
    t_stat = slope / slope_se if slope_se > 0 else 0.0
    MIN_N_FOR_SIGNIFICANCE = 12   # ~dof=10, where t-crit ≈ 2.23 ≈ "≥ 2"
    slope_significant = (n >= MIN_N_FOR_SIGNIFICANCE) and (abs(t_stat) >= 2.0)

    # Next-month forecast + 95% CI (point ± 1.96 × prediction-SE)
    x_next = n  # next index after last
    point = intercept + slope * x_next
    # Prediction interval SE for a new observation, simplified:
    if n > 2 and sxx > 0:
        pred_se = math.sqrt(sigma2 * (1 + 1 / n + (x_next - mean_x) ** 2 / sxx))
    else:
        pred_se = 0.0
    ci_lo = point - 1.96 * pred_se
    ci_hi = point + 1.96 * pred_se

    direction = "rising" if slope > 0 else "falling" if slope < 0 else "flat"
    if n < MIN_N_FOR_SIGNIFICANCE:
        note = (f"Direction: {direction} (slope {slope:+.1f}/month, r²={r2:.2f}). "
                f"Estimate is exploratory — only {n} monthly observations available; "
                f"a formal significance test requires n ≥ {MIN_N_FOR_SIGNIFICANCE} "
                f"(critical t at α=0.05 is approximately {2.0 if n >= 12 else 4.30 if n == 4 else 3.18 if n == 5 else 2.78 if n == 6 else 2.45 if n == 7 else 2.31 if n == 8 else 2.23 if n == 9 else 2.18:.2f} "
                f"with the current n−2 = {n-2} degrees of freedom).")
    else:
        note = (f"Direction: {direction} (slope {slope:+.1f}/month, r²={r2:.2f}, "
                f"t={t_stat:.2f}); slope "
                f"{'is statistically significant at α=0.05' if slope_significant else 'is NOT statistically significant at α=0.05'} "
                f"with n-2 = {n-2} degrees of freedom.")

    return {
        "status":            "active",
        "n":                 n,
        "n_dof":             max(0, n - 2),
        "slope_per_month":   _round(slope, 2) or 0.0,
        "intercept":         _round(intercept, 2) or 0.0,
        "r_squared":         _round(r2, 3) or 0.0,
        "slope_se":          _round(slope_se, 3) or 0.0,
        "t_stat":            _round(t_stat, 2) or 0.0,
        "slope_significant": slope_significant,
        "next_month_point":  int(round(max(0, point))),
        "next_month_ci95":   [int(round(max(0, ci_lo))), int(round(max(0, ci_hi)))],
        "note":              note,
    }


# ──────────────────────────────────────────────────────────────────────
# Poisson per-pathogen forecast (for rare pathogens)
# ──────────────────────────────────────────────────────────────────────
def _poisson_pmf_cdf(k: int, lam: float) -> float:
    """Poisson CDF P(X ≤ k) computed iteratively (no scipy)."""
    if lam <= 0:
        return 1.0 if k >= 0 else 0.0
    p = math.exp(-lam)  # P(X=0)
    cdf = p
    for i in range(1, k + 1):
        p *= lam / i
        cdf += p
    return min(1.0, cdf)


def _poisson_quantile(lam: float, q: float) -> int:
    """Return the smallest k such that P(X ≤ k) ≥ q. Iterative — fine
    for the small lam values (typically <10) we see for rare pathogens.
    """
    if lam <= 0:
        return 0
    k = 0
    while _poisson_pmf_cdf(k, lam) < q:
        k += 1
        if k > 1000:  # safety bound
            break
    return k


def _poisson_per_pathogen(
    monthly_counts_by_pathogen: Dict[str, List[int]],
    rare_threshold: int = 10,
    min_history: int = 6,
) -> Dict[str, Any]:
    """Fit Poisson(λ) per pathogen using historical mean as λ̂.
    Skip pathogens whose CURRENT (last-month) count exceeds rare_threshold.

    by_pathogen entry shape (when active):
      status:  "active"
      lambda:  λ̂ (rounded 2dp)
      last:    last-month count
      p90:     90th-percentile threshold
      p95:     95th-percentile threshold
      n:       months of history used
      flagged_p95: bool — last > p95 → unusual spike
    """
    out_by: Dict[str, Any] = {}
    for p, series in monthly_counts_by_pathogen.items():
        if len(series) < min_history:
            out_by[p] = _inactive(f"n={len(series)} < {min_history}")
            continue
        last = int(series[-1])
        if last > rare_threshold:
            out_by[p] = {
                "status":  "skipped_not_rare",
                "message": f"current count {last} > rare-threshold {rare_threshold}",
            }
            continue
        # Use historical (excluding current month) for λ̂ to avoid
        # "current spike trains itself".
        hist = series[:-1]
        lam = sum(hist) / len(hist) if hist else 0.0
        p90 = _poisson_quantile(lam, 0.90)
        p95 = _poisson_quantile(lam, 0.95)
        out_by[p] = {
            "status":      "active",
            "lambda":      _round(lam, 2) or 0.0,
            "last":        last,
            "p90":         p90,
            "p95":         p95,
            "n":           len(hist),
            "flagged_p95": last > p95,
        }

    n_active = sum(1 for v in out_by.values() if v.get("status") == "active")
    if n_active == 0:
        return {
            "status":         "insufficient_data",
            "message":        f"No pathogen has ≥{min_history} months of history & rare profile.",
            "rare_threshold": rare_threshold,
            "by_pathogen":    out_by,
        }
    return {
        "status":         "active",
        "rare_threshold": rare_threshold,
        "by_pathogen":    out_by,
    }


# ──────────────────────────────────────────────────────────────────────
# CUSUM change-point detection (Page 1954 — tabular)
# ──────────────────────────────────────────────────────────────────────
def _cusum(monthly_counts: Sequence[Tuple[str, int]],
           k_factor: float = 0.5,
           h_factor: float = 4.0) -> Dict[str, Any]:
    """Two-sided CUSUM. Slack k = k_factor × σ̂, decision threshold
    h = h_factor × σ̂.

    Returns when active:
      status:              "active"
      change_detected:     bool
      change_month:        label of first month that crossed h (None if not detected)
      direction:           "up" | "down" | None
      mu_hat, sigma_hat:   estimates from the in-control reference
      h_threshold:         decision threshold actually used
      note:                summary string
    """
    n = len(monthly_counts)
    if n < 6:
        return _inactive(f"Activates at n≥6 months (have {n}).")

    counts = [int(c) for _, c in monthly_counts]
    labels = [str(ym) for ym, _ in monthly_counts]

    # First half = "in-control reference" → estimate μ, σ
    half = max(3, n // 2)
    ref = counts[:half]
    mu = sum(ref) / len(ref)
    var = sum((c - mu) ** 2 for c in ref) / max(1, len(ref) - 1)
    sigma = math.sqrt(var) if var > 0 else 1.0
    k = k_factor * sigma
    h = h_factor * sigma

    cu_pos = 0.0
    cu_neg = 0.0
    change_idx: Optional[int] = None
    direction: Optional[str] = None
    for i, c in enumerate(counts):
        cu_pos = max(0.0, cu_pos + (c - mu) - k)
        cu_neg = max(0.0, cu_neg - (c - mu) - k)
        if cu_pos > h and change_idx is None:
            change_idx = i
            direction = "up"
            break
        if cu_neg > h and change_idx is None:
            change_idx = i
            direction = "down"
            break

    if change_idx is None:
        return {
            "status":          "active",
            "change_detected": False,
            "change_month":    None,
            "direction":       None,
            "mu_hat":          _round(mu, 2) or 0.0,
            "sigma_hat":       _round(sigma, 2) or 0.0,
            "h_threshold":     _round(h, 2) or 0.0,
            "note":            "In statistical control — no shift exceeded the CUSUM decision threshold.",
        }
    return {
        "status":          "active",
        "change_detected": True,
        "change_month":    labels[change_idx],
        "direction":       direction,
        "mu_hat":          _round(mu, 2) or 0.0,
        "sigma_hat":       _round(sigma, 2) or 0.0,
        "h_threshold":     _round(h, 2) or 0.0,
        "note":            f"Tabular CUSUM crossed the {h_factor}σ decision threshold "
                           f"in {labels[change_idx]} ({direction}-shift).",
    }


# ──────────────────────────────────────────────────────────────────────
# OLS with seasonal dummies (n ≥ 12)
# ──────────────────────────────────────────────────────────────────────
def _ols_seasonal(monthly_counts: Sequence[Tuple[str, int]]) -> Dict[str, Any]:
    """Linear trend + month-of-year dummies (Feb..Dec), Jan = baseline.

    Implementation note: 12-feature OLS solved via normal equations
    in pure Python (no numpy). For our typical n in [12, 36] this is
    numerically fine; the design matrix is well-conditioned.

    Returns when active:
      status:           "active"
      n:                months used
      intercept_Jan:    baseline (Jan) intercept
      monthly_slope:    trend slope (recalls per month)
      seasonal_effects: {month_name: effect_vs_Jan}
      r_squared:        R²
      note:             summary string
    """
    # 14-month gate: model has 13 parameters (intercept + slope + 11
    # month-dummies for Feb..Dec). With n < 14, the system is at or
    # below identification — the normal-equations solver hits rank
    # deficiency. 14 gives 1 residual df at minimum.
    if len(monthly_counts) < 14:
        return _inactive(f"Activates at n≥14 months (have {len(monthly_counts)}).")

    # Parse month-of-year from the YYYY-MM label of each entry
    months_idx: List[int] = []     # 0=Jan, 1=Feb, ..., 11=Dec
    xs: List[int] = []
    ys: List[int] = []
    for i, (ym, c) in enumerate(monthly_counts):
        try:
            mm = int(str(ym).split("-")[1])
        except (IndexError, ValueError):
            return _inactive("Could not parse month-of-year from history labels.")
        months_idx.append(mm - 1)
        xs.append(i)
        ys.append(int(c))

    n = len(ys)
    # Design matrix: [1, t, D_Feb, D_Mar, ..., D_Dec] → 13 features
    X: List[List[float]] = []
    for i, m in enumerate(months_idx):
        row = [1.0, float(xs[i])]
        for k in range(1, 12):  # k=1..11 → Feb..Dec dummies
            row.append(1.0 if m == k else 0.0)
        X.append(row)

    # Normal equations: β = (X'X)^-1 X'y. Solve via Gaussian elimination.
    p = 13
    XtX = [[sum(X[i][r] * X[i][c] for i in range(n)) for c in range(p)] for r in range(p)]
    Xty = [sum(X[i][r] * ys[i] for i in range(n)) for r in range(p)]

    # Augmented matrix and partial-pivot Gauss elimination
    aug = [row[:] + [Xty[r]] for r, row in enumerate(XtX)]
    for col in range(p):
        # pivot
        piv_row = max(range(col, p), key=lambda r: abs(aug[r][col]))
        if abs(aug[piv_row][col]) < 1e-12:
            return _inactive("Design matrix is rank-deficient (history too uniform).")
        aug[col], aug[piv_row] = aug[piv_row], aug[col]
        # eliminate
        for r in range(p):
            if r == col:
                continue
            factor = aug[r][col] / aug[col][col]
            if factor == 0:
                continue
            for cc in range(col, p + 1):
                aug[r][cc] -= factor * aug[col][cc]
    beta = [aug[r][p] / aug[r][r] for r in range(p)]

    # R²
    fitted = [sum(X[i][r] * beta[r] for r in range(p)) for i in range(n)]
    mean_y = sum(ys) / n
    ss_res = sum((ys[i] - fitted[i]) ** 2 for i in range(n))
    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    seasonal: Dict[str, float] = {"Jan": 0.0}
    for k in range(1, 12):
        seasonal[month_names[k]] = _round(beta[1 + k], 2) or 0.0

    return {
        "status":            "active",
        "n":                 n,
        "intercept_Jan":     _round(beta[0], 2) or 0.0,
        "monthly_slope":     _round(beta[1], 2) or 0.0,
        "seasonal_effects":  seasonal,
        "r_squared":         _round(r2, 3) or 0.0,
        "note":              "Linear trend + 11 month-dummies (Jan baseline).",
    }


# ──────────────────────────────────────────────────────────────────────
# n ≥ 24 models — return a shaped "insufficient_data" until we have
# 2 full seasons. The builder shows them as "Activates later" cards.
# ──────────────────────────────────────────────────────────────────────
def _two_season_gate(n: int, name: str) -> Optional[Dict[str, Any]]:
    """If n < 24, return the inactive shape. Otherwise None → caller
    proceeds with the actual fit (when implemented)."""
    if n < 24:
        return _inactive(f"{name} activates at n≥24 months (have {n}).")
    return None


def _stl(monthly_counts: Sequence[Tuple[str, int]]) -> Dict[str, Any]:
    """STL decomposition. Stub — real implementation would call
    statsmodels.tsa.seasonal.STL once we have ≥ 2 full seasons."""
    gate = _two_season_gate(len(monthly_counts), "STL")
    if gate:
        return gate
    return _unavailable("STL implementation pending — install statsmodels and re-run.")


def _holt_winters(monthly_counts: Sequence[Tuple[str, int]]) -> Dict[str, Any]:
    """Holt-Winters. Stub — needs ≥ 2 full seasonal cycles."""
    gate = _two_season_gate(len(monthly_counts), "Holt-Winters")
    if gate:
        return gate
    return _unavailable("Holt-Winters implementation pending — install statsmodels and re-run.")


def _sarima(monthly_counts: Sequence[Tuple[str, int]]) -> Dict[str, Any]:
    """SARIMA. Stub — needs ≥ 2 full seasonal cycles."""
    gate = _two_season_gate(len(monthly_counts), "SARIMA")
    if gate:
        return gate
    return _unavailable("SARIMA implementation pending — install statsmodels and re-run.")


def _prophet(monthly_counts: Sequence[Tuple[str, int]]) -> Dict[str, Any]:
    """Prophet. Stub — needs ≥ 1 year of history; we hold it to ≥ 24 to
    match the other seasonal models."""
    gate = _two_season_gate(len(monthly_counts), "Prophet")
    if gate:
        return gate
    return _unavailable("Prophet implementation pending — install prophet and re-run.")


# ──────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────
def run_all_models(
    monthly_counts: Sequence[Tuple[str, int]],
    monthly_counts_by_pathogen: Optional[Dict[str, List[int]]] = None,
) -> Dict[str, Any]:
    """Run every model. Each returns a self-describing dict.

    Parameters
    ----------
    monthly_counts : List[Tuple[str, int]]
        Ordered series of (year-month, count) tuples covering all
        history INCLUDING the current month as the last element.
    monthly_counts_by_pathogen : Dict[str, List[int]] | None
        Per-pathogen monthly count series (same time-axis as
        monthly_counts). Optional — Poisson reports
        insufficient_data if absent.

    Returns
    -------
    Dict[str, Dict] — keyed by model name, exactly the 8 keys listed
    in the module docstring.
    """
    pathogen_history = monthly_counts_by_pathogen or {}
    return {
        "linear_trend":  _linear_trend(monthly_counts),
        "poisson":       _poisson_per_pathogen(pathogen_history),
        "cusum":         _cusum(monthly_counts),
        "ols_seasonal":  _ols_seasonal(monthly_counts),
        "stl":           _stl(monthly_counts),
        "holt_winters":  _holt_winters(monthly_counts),
        "sarima":        _sarima(monthly_counts),
        "prophet":       _prophet(monthly_counts),
    }


__all__ = ["run_all_models", "build_pathogen_history"]
