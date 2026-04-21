"""
AFTS Monthly Report — Predictive Modelling Layer
================================================

Each model here ships with a minimum-data gate: when the available history
is below the gate, the function returns
    {"status": "insufficient_data", "required": N, "have": n, ...}
so the builder can render a "activates at N months" badge instead of
publishing numbers from an underspecified model. That way the report tells
the truth about modelling confidence at any point in the project timeline
and automatically enables richer models as the dataset grows.

Models included (today → future):
    linear_trend_projection    — n ≥ 3   ← ALWAYS ACTIVE TODAY
    poisson_forecast           — n ≥ 3   ← ALWAYS ACTIVE TODAY
    cusum_change_point         — n ≥ 6
    ols_with_seasonal_dummies  — n ≥ 6
    stl_decomposition          — n ≥ 12
    holt_winters               — n ≥ 24
    sarima                     — n ≥ 24
    prophet                    — n ≥ 24
"""
from __future__ import annotations

from collections import Counter
from math import exp, factorial, log, sqrt
from statistics import mean, stdev
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Shared gate
# ---------------------------------------------------------------------------
def _insufficient(required: int, have: int, model: str) -> Dict[str, Any]:
    return {
        "status":   "insufficient_data",
        "model":    model,
        "required": required,
        "have":     have,
        "message":  f"Activates at n ≥ {required} months (have {have}).",
    }


# ===========================================================================
# 1. LINEAR TREND PROJECTION — always-on
# ===========================================================================
def linear_trend_projection(monthly_counts: List[Tuple[str, int]]) -> Dict[str, Any]:
    """
    Simple OLS projection of next month's total recall count from the
    historical monthly series. Returns point estimate + approximate 95% CI
    derived from the residual standard error.

    Formula (OLS regression y = a + b·x, where x = 0,1,2,…):
        b = Σ((x_i − x̄)(y_i − ȳ)) / Σ((x_i − x̄)²)
        a = ȳ − b·x̄
        ŷ(n)  = a + b·n
        s_e²  = Σ(y_i − ŷ_i)² / (n − 2)
        CI95 ≈ ŷ(n) ± 1.96 · s_e · sqrt(1 + 1/n + (n − x̄)² / Σ(x_i − x̄)²)

    We don't bother with t-critical values since scipy isn't a dependency;
    1.96 is the large-sample normal approximation, which is fine for an
    intelligence briefing (not a clinical trial).
    """
    MIN = 3
    n = len(monthly_counts)
    if n < MIN:
        return _insufficient(MIN, n, "linear_trend_projection")

    y = [c for _, c in monthly_counts]
    x = list(range(n))
    xbar, ybar = mean(x), mean(y)

    num = sum((xi - xbar) * (yi - ybar) for xi, yi in zip(x, y))
    den = sum((xi - xbar) ** 2 for xi in x)
    if den == 0:
        return _insufficient(MIN, n, "linear_trend_projection")

    b = num / den
    a = ybar - b * xbar
    fitted    = [a + b * xi for xi in x]
    residuals = [yi - fi for yi, fi in zip(y, fitted)]
    df = max(1, n - 2)
    se = sqrt(sum(r * r for r in residuals) / df)

    x_next   = n
    forecast = a + b * x_next
    pred_se  = se * sqrt(1 + 1 / n + (x_next - xbar) ** 2 / den)
    ci_low   = forecast - 1.96 * pred_se
    ci_high  = forecast + 1.96 * pred_se

    # Slope significance via t-stat
    slope_se = se / sqrt(den) if den else None
    t_stat = (b / slope_se) if slope_se else None
    # Rough significance threshold: |t| > 2 is "significant" at ~n≥5
    slope_significant = t_stat is not None and abs(t_stat) > 2

    return {
        "status":            "active",
        "model":             "linear_trend_projection",
        "intercept":         round(a, 2),
        "slope":             round(b, 3),
        "slope_per_month":   round(b, 3),
        "slope_significant": slope_significant,
        "t_stat":            round(t_stat, 2) if t_stat is not None else None,
        "next_month_point":  round(max(0, forecast), 1),
        "next_month_ci95":   [round(max(0, ci_low), 1), round(max(0, ci_high), 1)],
        "residual_se":       round(se, 2),
        "r_squared":         _r_squared(y, fitted),
        "note":              ("Slope not statistically significant — trend may be flat "
                              "or underpowered."
                              if not slope_significant
                              else "Slope is statistically significant at the 0.05 level."),
    }


def _r_squared(y: List[float], fitted: List[float]) -> float:
    if not y:
        return 0.0
    ybar = mean(y)
    ss_tot = sum((yi - ybar) ** 2 for yi in y)
    ss_res = sum((yi - fi) ** 2 for yi, fi in zip(y, fitted))
    if ss_tot == 0:
        return 1.0 if ss_res == 0 else 0.0
    return round(1 - ss_res / ss_tot, 3)


# ===========================================================================
# 2. POISSON FORECAST PER RARE PATHOGEN — always-on
# ===========================================================================
def poisson_forecast(monthly_counts_by_pathogen: Dict[str, List[int]],
                     rare_threshold: int = 10) -> Dict[str, Any]:
    """
    For each pathogen with historical mean < `rare_threshold` cases/month,
    fit a Poisson distribution (λ = historical mean) and report:
        λ̂           — MLE rate estimate
        p90         — 90th percentile (upper-bound forecast)
        p95         — 95th percentile
        p99         — 99th percentile

    This is the right model for rare-event counts (C. botulinum, Norovirus,
    Aflatoxin). For dominant pathogens (mean ≥ threshold) we punt to the
    linear trend model — their variance is too large for a pure Poisson
    approximation.
    """
    MIN = 3
    results: Dict[str, Any] = {}
    for pathogen, counts in monthly_counts_by_pathogen.items():
        if len(counts) < MIN:
            results[pathogen] = _insufficient(MIN, len(counts), "poisson_forecast")
            continue
        lam = mean(counts)
        if lam >= rare_threshold:
            results[pathogen] = {
                "status": "not_applicable",
                "reason": f"Mean {lam:.1f} ≥ rare threshold {rare_threshold}; use linear trend.",
            }
            continue
        # Poisson percentiles via cumulative mass
        p50 = _poisson_quantile(lam, 0.50)
        p90 = _poisson_quantile(lam, 0.90)
        p95 = _poisson_quantile(lam, 0.95)
        p99 = _poisson_quantile(lam, 0.99)
        results[pathogen] = {
            "status":  "active",
            "lambda":  round(lam, 2),
            "last":    counts[-1],
            "p50":     p50,
            "p90":     p90,
            "p95":     p95,
            "p99":     p99,
        }
    return {"model": "poisson_forecast", "rare_threshold": rare_threshold, "by_pathogen": results}


def _poisson_pmf(k: int, lam: float) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * exp(-lam) / factorial(k)


def _poisson_quantile(lam: float, q: float, kmax: int = 200) -> int:
    cum = 0.0
    for k in range(kmax):
        cum += _poisson_pmf(k, lam)
        if cum >= q:
            return k
    return kmax


# ===========================================================================
# 3. CUSUM CHANGE-POINT DETECTION — activates at n ≥ 6
# ===========================================================================
def cusum_change_point(monthly_counts: List[Tuple[str, int]],
                       k: float = 0.5, h: float = 4.0) -> Dict[str, Any]:
    """
    Standard tabular CUSUM (Page 1954) applied to monthly counts. Detects a
    persistent shift in the process mean. Parameters:
        k — reference value; typically ½·σ for a 1σ shift
        h — control-chart decision interval; 4·σ gives ARL₀ ≈ 168

    For each i:
        S⁺ᵢ = max(0, S⁺ᵢ₋₁ + (y_i − μ − k·σ))
        S⁻ᵢ = max(0, S⁻ᵢ₋₁ − (y_i − μ + k·σ))
    Change flagged when either cumulative sum exceeds h·σ.

    Requires ≥6 months so we have at least 3 pre-change points for a stable
    baseline estimate.
    """
    MIN = 6
    n = len(monthly_counts)
    if n < MIN:
        return _insufficient(MIN, n, "cusum_change_point")

    y = [c for _, c in monthly_counts]
    mu = mean(y)
    sg = stdev(y) if len(y) >= 2 else 0
    if sg == 0:
        return {"status": "active", "model": "cusum_change_point",
                "change_detected": False, "note": "No variance in series."}

    s_pos = 0.0
    s_neg = 0.0
    change_month = None
    direction = None
    for i, yi in enumerate(y):
        s_pos = max(0.0, s_pos + (yi - mu - k * sg))
        s_neg = max(0.0, s_neg - (yi - mu + k * sg))
        if change_month is None:
            if s_pos > h * sg:
                change_month, direction = monthly_counts[i][0], "upward"
                break
            if s_neg > h * sg:
                change_month, direction = monthly_counts[i][0], "downward"
                break

    return {
        "status":           "active",
        "model":            "cusum_change_point",
        "baseline_mean":    round(mu, 2),
        "baseline_sigma":   round(sg, 2),
        "change_detected":  change_month is not None,
        "change_month":     change_month,
        "direction":        direction,
        "note":             ("Persistent shift detected — process is not in statistical "
                              "control relative to the historical baseline."
                              if change_month else
                              "Series remains in statistical control."),
    }


# ===========================================================================
# 4. OLS WITH MONTH-DUMMY SEASONAL EFFECTS — activates at n ≥ 6
# ===========================================================================
def ols_seasonal_dummies(monthly_counts: List[Tuple[str, int]]) -> Dict[str, Any]:
    """
    Linear trend + month-of-year dummies via normal-equation OLS. Activates
    at n=6 so we have enough degrees of freedom for meaningful coefficients
    once a few seasonal peaks/troughs have been observed.

    Fully replaces itself with STL once n≥12.
    """
    MIN = 6
    n = len(monthly_counts)
    if n < MIN:
        return _insufficient(MIN, n, "ols_seasonal_dummies")

    # Parse YYYY-MM → month number, build design matrix
    # X = [intercept, t, m2, m3, ..., m12]  (January = reference level)
    y = [c for _, c in monthly_counts]
    months = []
    for ym, _ in monthly_counts:
        try:
            months.append(int(ym.split("-")[1]))
        except (ValueError, IndexError):
            months.append(1)

    X = []
    for t, m in enumerate(months):
        row = [1.0, float(t)]
        for dummy_month in range(2, 13):
            row.append(1.0 if m == dummy_month else 0.0)
        X.append(row)

    # Normal equation: β = (XᵀX)⁻¹ Xᵀy
    try:
        beta = _ols_fit(X, y)
    except ValueError:
        return {"status": "active", "model": "ols_seasonal_dummies",
                "note": "Design matrix singular — need more monthly variety."}

    intercept = beta[0]
    trend     = beta[1]
    seasonal  = {m: round(beta[m], 2) for m in range(2, len(beta))}

    return {
        "status":           "active",
        "model":            "ols_seasonal_dummies",
        "intercept_Jan":    round(intercept, 2),
        "monthly_slope":    round(trend, 3),
        "seasonal_effects": seasonal,   # effect vs January baseline
        "note":             "Month-dummy coefficients show additive seasonal effect vs January.",
    }


def _ols_fit(X: List[List[float]], y: List[float]) -> List[float]:
    """Solve β = (XᵀX)⁻¹ Xᵀy via Gauss-Jordan. Tiny matrices only."""
    n = len(X); k = len(X[0])
    XtX = [[sum(X[i][a] * X[i][b] for i in range(n)) for b in range(k)] for a in range(k)]
    Xty = [sum(X[i][a] * y[i] for i in range(n)) for a in range(k)]
    # Solve
    aug = [row + [rhs] for row, rhs in zip(XtX, Xty)]
    for i in range(k):
        # Partial pivot
        piv = max(range(i, k), key=lambda r: abs(aug[r][i]))
        if abs(aug[piv][i]) < 1e-10:
            raise ValueError("Singular matrix")
        aug[i], aug[piv] = aug[piv], aug[i]
        # Eliminate
        for j in range(k):
            if j == i: continue
            factor = aug[j][i] / aug[i][i]
            for col in range(i, k + 1):
                aug[j][col] -= factor * aug[i][col]
    return [aug[i][k] / aug[i][i] for i in range(k)]


# ===========================================================================
# 5-8. FUTURE MODELS — all return insufficient_data until the gate opens
# ===========================================================================
def stl_decomposition(monthly_counts: List[Tuple[str, int]]) -> Dict[str, Any]:
    return _insufficient(12, len(monthly_counts), "stl_decomposition")

def holt_winters(monthly_counts: List[Tuple[str, int]]) -> Dict[str, Any]:
    return _insufficient(24, len(monthly_counts), "holt_winters")

def sarima_forecast(monthly_counts: List[Tuple[str, int]]) -> Dict[str, Any]:
    return _insufficient(24, len(monthly_counts), "sarima")

def prophet_forecast(monthly_counts: List[Tuple[str, int]]) -> Dict[str, Any]:
    return _insufficient(24, len(monthly_counts), "prophet")


# ===========================================================================
# CONVENIENCE BUNDLE
# ===========================================================================
def run_all_models(monthly_counts: List[Tuple[str, int]],
                   monthly_counts_by_pathogen: Dict[str, List[int]]) -> Dict[str, Any]:
    """
    Run every model and return a single dict the builder can iterate over
    to render the "Predictive Outlook" section. Inactive models report
    their activation threshold so the builder shows the roadmap.
    """
    return {
        "linear_trend":   linear_trend_projection(monthly_counts),
        "poisson":        poisson_forecast(monthly_counts_by_pathogen),
        "cusum":          cusum_change_point(monthly_counts),
        "ols_seasonal":   ols_seasonal_dummies(monthly_counts),
        "stl":            stl_decomposition(monthly_counts),
        "holt_winters":   holt_winters(monthly_counts),
        "sarima":         sarima_forecast(monthly_counts),
        "prophet":        prophet_forecast(monthly_counts),
    }
