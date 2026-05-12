"""
AFTS Food Safety Intelligence System — Monthly Report Generator
================================================================
Runs on the 1st of each month 07:00 UTC via GitHub Actions.

Inputs:
  docs/data/recalls.xlsx  (Recalls sheet only — never Pending or NEWS)

Outputs:
  docs/<YYYY>-M<MM>.html              — full monthly report (9 sections)
  docs/<YYYY>-M<MM>-all.html          — companion: every recall in the month
  docs/data/monthly-summary-latest.json — payload for the Apps Script mailer

Architecture:
  - Weekly builder owns shared helpers (severity taxonomy, URL grading,
    row rendering).
  - monthly_stats.py computes descriptive analytics for the month
    (MoM trend, hotspot matrix, clusters, concentration, growth, severity,
    cadence).
  - monthly_models.py runs predictive models with minimum-data gates so
    the report shows which models are active and which activate later.
  - process_authority.py fires the Process Authority 4th paragraph when
    thermal-processing hazards appear in the window.
  - pathogen_italic.italicise_prose() wraps binomial pathogen names in
    <em> across every prose paragraph (shared with weekly).

All SVG visualisations are generated inline (no JS, no CDN) so the HTML
renders identically in a browser, a PDF export, and an email client.
"""
from __future__ import annotations
import argparse
import calendar
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from html import escape as _html_escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import build_weekly_report_afts as weekly  # noqa: E402
_count_phrase = weekly._count_phrase  # plural-aware counting helper
from pathogen_italic import italicise_prose  # noqa: E402
from monthly_stats import (  # noqa: E402
    compute_monthly_signals,
    normalise_pathogen,
)
from monthly_models import run_all_models  # noqa: E402
from process_authority import (  # noqa: E402
    detect_process_authority_trigger,
    build_prompt_extension as build_pa_prompt_extension,
    deterministic_fallback as pa_deterministic_fallback,
    PROCESS_AUTHORITY_LABEL,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("monthly")

CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Brand tokens — mirror the weekly report so the HTML shares a visual identity.
# If weekly doesn't export them (older versions or current refactored state),
# fall back to the canonical AFTS palette so the monthly build never crashes
# on a missing constant.
BRAND_ORANGE = getattr(weekly, "BRAND_ORANGE", "#E8601A")
BRAND_BLACK  = getattr(weekly, "BRAND_BLACK",  "#0a0e1a")
TIER1_RED    = getattr(weekly, "TIER1_RED",    "#dc2626")
TIER2_AMBER  = getattr(weekly, "TIER2_AMBER",  "#f59e0b")
OUTBREAK_VIO = getattr(weekly, "OUTBREAK_VIO", "#7c3aed")

escape = _html_escape


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def month_bounds(year: int, month: int) -> Tuple[date, date]:
    start = date(year, month, 1)
    end = date(year, month, calendar.monthrange(year, month)[1])
    return start, end


def filter_month(recalls: List[Dict], start: date, end: date) -> List[Dict]:
    out = []
    for r in recalls:
        d = str(r.get("Date", "") or "")[:10]
        if not d:
            continue
        try:
            rd = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            continue
        if start <= rd <= end:
            out.append(r)
    return out


def bucket_by_month(recalls: List[Dict]) -> Dict[str, List[Dict]]:
    out: Dict[str, List[Dict]] = defaultdict(list)
    for r in recalls:
        ym = str(r.get("Date", "") or "")[:7]
        if ym:
            out[ym].append(r)
    return out


# ---------------------------------------------------------------------------
# Whitelist filter for the Poisson model. Applying it only to the dashboard's
# canonical pathogen list prevents junk category strings ("Mouse contamination",
# "Inadequate sterilisation") from polluting the forecast panel.
# ---------------------------------------------------------------------------
CANONICAL_PATHOGENS = {
    "Listeria", "Salmonella", "E. coli / STEC", "C. botulinum",
    "Bacillus cereus / Cereulide", "Campylobacter", "Vibrio", "Cronobacter",
    "Staphylococcus aureus", "Yersinia", "Shigella",
    "Norovirus", "Hepatitis A", "Rotavirus",
    "Aflatoxin", "Ochratoxin A", "Patulin", "Histamine / scombrotoxin",
    "Marine biotoxins",
    "Cyclospora", "Toxoplasma",
}


# ---------------------------------------------------------------------------
# Statistical guard — Z-score is mathematically valid but practically
# meaningless when the prior-month series is too short. With n=3 baseline
# months the sample σ collapses to noise and the report can show "Z=+52.8"
# for what is really "we don't have enough history yet". Set the threshold
# to the same value the CUSUM model uses (n>=6) so the report stays
# internally consistent: when CUSUM activates, Z becomes meaningful.
# ---------------------------------------------------------------------------
_MIN_BASELINE_MONTHS_FOR_Z = 6


def _apply_baseline_size_guard(signals: Dict[str, Any]) -> None:
    """Mutates signals['mom_trend'] in place. When baseline n is too small,
    moves the raw Z into a diagnostic key and nulls the public z_score so
    every consumer (AI prompt, deterministic narrative, KPI badge, meta
    block) falls through to its existing None-branch ("baseline too narrow").
    Idempotent — safe to call more than once."""
    mom = signals.get("mom_trend") or {}
    series = mom.get("values") or []
    n_baseline = len(series)
    mom["baseline_n"] = n_baseline
    if n_baseline < _MIN_BASELINE_MONTHS_FOR_Z and mom.get("z_score") is not None:
        mom["z_score_raw"] = mom["z_score"]   # preserved for diagnostics / logs
        mom["z_score"] = None
        mom["anomaly_flag"] = False
        mom["baseline_too_narrow"] = True
        log.info("Z-score suppressed: baseline n=%d < %d (raw z=%s)",
                 n_baseline, _MIN_BASELINE_MONTHS_FOR_Z, mom.get("z_score_raw"))


# ---------------------------------------------------------------------------
# Pathogen synonym consolidation — added 2026-04-27 after audit findings
# revealed splits in W17 (Rodenticide / rat poison / Rodent contamination
# all appearing as separate pathogen rows for the same HiPP outbreak) and
# in M03 (multiple STEC and aflatoxin variants). The underlying scrapers
# preserve regulator-language verbatim by design — this is a presentation-
# layer normalisation, not a data correction.
#
# Buckets are CHEMICALLY/EPIDEMIOLOGICALLY distinct, e.g. rodenticide
# (anticoagulant poisoning) is NOT folded into rodent contamination
# (live/dead pests). Salmonella serovars (Typhimurium, Newport, Enteritidis)
# are deliberately preserved — they are legitimately distinct.
# ---------------------------------------------------------------------------
_PATHOGEN_SYNONYMS = {
    # Rodenticide chemical poisoning
    "rat poison":                                          "Rodenticide",
    "Rat poison":                                          "Rodenticide",
    "Rodenticide (rat poison)":                            "Rodenticide",
    "rodenticide (rat poison)":                            "Rodenticide",
    "Rodenticide poisoning":                               "Rodenticide",
    "Bromadiolone":                                        "Rodenticide",
    "bromadiolone":                                        "Rodenticide",
    # Rodent / pest contamination (DIFFERENT category — live pests / droppings)
    "Rodent contamination (physical/microbial hazard)":    "Rodent contamination",
    "Rodent contamination (physical/biological hazard)":   "Rodent contamination",
    "Mouse contamination (physical/biological hazard)":    "Rodent contamination",
    "Mouse contamination":                                 "Rodent contamination",
    # Aflatoxin
    "Aflatoxins":                                          "Aflatoxin",
    # Bacillus cereus / cereulide
    "Bacillus cereus / cereulide":                         "Bacillus cereus / Cereulide",
    "Bacillus cereus (cereulide)":                         "Bacillus cereus / Cereulide",
    "Cereulide (B. cereus toxin)":                         "Bacillus cereus / Cereulide",
    "Cereulide":                                           "Bacillus cereus / Cereulide",
    # E. coli / STEC variants → single bucket
    "STEC (Shiga toxin-producing E. coli)":                "E. coli STEC",
    "Shiga toxin-producing E. coli (STEC)":                "E. coli STEC",
    "E. coli STEC (Shiga toxin-producing)":                "E. coli STEC",
    "STEC / E. coli O157:H7":                              "E. coli STEC",
    "E. coli O157:H7":                                     "E. coli STEC",
    "E. coli":                                             "E. coli STEC",
    # Marine biotoxins (DSP, PSP, phytoplankton — same toxin family)
    "Lipophilic biotoxins (DSP)":                          "Marine biotoxins",
    "Lipophilic biotoxins":                                "Marine biotoxins",
    "Paralytic shellfish toxins (PSP)":                    "Marine biotoxins",
    "Paralytic shellfish toxins":                          "Marine biotoxins",
    "Paralytic Shellfish Toxins (saxitoxins)":             "Marine biotoxins",
    "Phytoplankton biotoxins":                             "Marine biotoxins",
    # Salmonella — bare label only; serovars (Typhimurium, etc.) preserved
    "Salmonella":                                          "Salmonella spp.",
    # Histamine
    "Histamine":                                           "Histamine / scombrotoxin",
    "Histamine (biotoxine endogène)":                      "Histamine / scombrotoxin",
    "Scombrotoxin":                                        "Histamine / scombrotoxin",
}


def _consolidate_pathogen_label(label: str) -> str:
    """Map a raw pathogen label to its canonical bucket. Idempotent —
    canonical labels pass through unchanged."""
    if not label:
        return label
    s = label.strip()
    return _PATHOGEN_SYNONYMS.get(s, s)


def _consolidate_counter(c: Counter) -> Counter:
    """Return a new Counter with synonymous keys merged."""
    out: Counter = Counter()
    for k, v in c.items():
        out[_consolidate_pathogen_label(k)] += v
    return out


def build_pathogen_history(monthly_cohorts: List[Tuple[str, List[Dict]]]) -> Dict[str, List[int]]:
    """Per-pathogen monthly count series, filtered to canonical pathogens."""
    # Which pathogens appear anywhere in history AND are canonical?
    seen = set()
    for _, recalls in monthly_cohorts:
        for r in recalls:
            p = normalise_pathogen(r.get("Pathogen") or "")
            if p in CANONICAL_PATHOGENS:
                seen.add(p)
    # Build parallel count series for each
    series: Dict[str, List[int]] = {p: [] for p in seen}
    for _, recalls in monthly_cohorts:
        cnts = Counter(
            normalise_pathogen(r.get("Pathogen") or "") for r in recalls
        )
        for p in seen:
            series[p].append(cnts.get(p, 0))
    return series


# ---------------------------------------------------------------------------
# Month stats (shallow — just counts + top pathogen for the KPI strip)
# ---------------------------------------------------------------------------
def compute_month_stats(month_recalls: List[Dict],
                        prior_month_recalls: List[Dict]) -> Dict[str, Any]:
    total     = len(month_recalls)
    tier1     = sum(1 for r in month_recalls if weekly.safe_int(r.get("Tier")) == 1)
    outbreaks = sum(1 for r in month_recalls if weekly.safe_int(r.get("Outbreak")) == 1)

    pathogen_counts = Counter()
    pathogen_tier1  = Counter()
    for r in month_recalls:
        p = (r.get("Pathogen") or "").strip()
        if not p:
            continue
        _, canon = weekly.severity_score(p)
        pathogen_counts[canon] += 1
        if weekly.safe_int(r.get("Tier")) == 1:
            pathogen_tier1[canon] += 1

    # Synonym consolidation — merge regulator-language variants that
    # describe the same hazard (e.g. "Rodenticide", "rat poison",
    # "Rodenticide (rat poison)" → single "Rodenticide" bucket).
    pathogen_counts = _consolidate_counter(pathogen_counts)
    pathogen_tier1  = _consolidate_counter(pathogen_tier1)

    country_counts = Counter(
        (r.get("Country") or "Unknown").strip() or "Unknown"
        for r in month_recalls
    )
    source_counts = Counter()
    for r in month_recalls:
        s = (r.get("Source") or "").strip()
        if s:
            source_counts[s] += 1

    prev_total = len(prior_month_recalls)
    delta = total - prev_total
    delta_pct = round((delta / prev_total) * 100) if prev_total else None

    if pathogen_counts:
        ranked = sorted(
            pathogen_counts.items(),
            key=lambda kv: (-kv[1], -pathogen_tier1.get(kv[0], 0), kv[0]),
        )
        top_p = ranked[0]
    else:
        top_p = ("-", 0)

    return {
        "total": total,
        "tier1": tier1,
        "outbreaks": outbreaks,
        "top_pathogen": top_p,
        "pathogen_counts":  pathogen_counts.most_common(15),
        "country_counts":   country_counts.most_common(20),
        "source_counts":    source_counts.most_common(20),
        "prev_total":       prev_total,
        "delta":            delta,
        "delta_pct":        delta_pct,
    }


# ---------------------------------------------------------------------------
# AI narrative — consumes the pre-computed signals
# ---------------------------------------------------------------------------
def generate_monthly_narrative(stats: Dict[str, Any],
                               signals: Dict[str, Any],
                               models: Dict[str, Any],
                               month_recalls: List[Dict],
                               month_name: str,
                               year: int) -> str:
    """
    Write 3 paragraphs (+ optional 4th Process Authority Note). Claude is fed
    the pre-computed analytical signals as authoritative context so it
    narrates them instead of trying to re-derive from raw counts. Falls back
    to a deterministic narrative if no ANTHROPIC_API_KEY is present.
    """
    pa_trigger   = detect_process_authority_trigger(month_recalls)
    pa_extension = build_pa_prompt_extension(pa_trigger)

    if not CLAUDE_API_KEY:
        log.warning("ANTHROPIC_API_KEY missing; using fallback monthly narrative")
        return _fallback_narrative(stats, signals, models, month_name, year, pa_trigger)

    import requests

    # Compact views of signals for the prompt
    mom = signals["mom_trend"]
    hs  = signals["hotspot"]
    cl  = signals["cluster"]
    co  = signals["concentration"]
    gr  = signals["growth"]
    sv  = signals["severity"]
    lt  = models["linear_trend"]
    poi = models["poisson"]

    hotspot_lines = "\n".join(
        f"  - {h['country']} × {h['pathogen']}: observed={h['observed']} vs "
        f"expected={h['expected']} (stdres={h['stdres']:+.2f}, ratio={h['ratio']}x)"
        for h in hs.get("hotspots", [])[:3]
    ) or "  (no statistically significant hotspots — distribution matches independence baseline)"

    cluster_lines = "\n".join(
        f"  - {c['pathogen']}: {c['size']} events in {c['span_days']}d "
        f"across {len(c['countries'])} countries ({', '.join(c['countries'][:3])})"
        for c in cl.get("clusters", [])[:3]
    ) or "  (no same-pathogen temporal clusters this month)"

    emerging_lines = "\n".join(
        f"  - {e['pathogen']}: count={e['count']}, Z={e['z_score']}, MoM={e['growth_pct']}%"
        for e in gr.get("emerging", [])[:4]
    ) or "  (no pathogens with >2-sigma growth vs historical share)"

    # Poisson highlights for rare pathogens
    poisson_lines = []
    if poi.get("by_pathogen"):
        for p, f in poi["by_pathogen"].items():
            if isinstance(f, dict) and f.get("status") == "active":
                poisson_lines.append(
                    f"  - {p}: λ̂={f['lambda']}, last={f['last']}, "
                    f"p90={f['p90']}, p95={f['p95']}"
                )
    poisson_block = "\n".join(poisson_lines[:5]) or "  (no rare pathogens with active Poisson fit)"

    lt_block = (
        f"  Active: next-month point forecast={lt['next_month_point']}, "
        f"95% CI=[{lt['next_month_ci95'][0]}, {lt['next_month_ci95'][1]}], "
        f"slope={lt['slope_per_month']:+.1f}/mo, r²={lt['r_squared']}, "
        f"slope_significant={lt['slope_significant']}"
        if lt.get("status") == "active"
        else f"  Inactive: {lt.get('message','(insufficient data)')}"
    )

    prompt = f"""You are producing the AFTS monthly pathogen surveillance briefing for {month_name} {year}. Your analysis must sound like a practising process authority — not a generic AI — interpreting every finding through validated food process engineering (21 CFR 113/114, PMO, HACCP CCPs, environmental monitoring) and naming specific failure modes and control points.

PRE-COMPUTED ANALYTICAL SIGNALS — treat these as authoritative. Do NOT recompute or second-guess them.

MONTH BASELINE
  Total recalls:    {stats['total']}
  Tier-1 critical:  {stats['tier1']}
  Outbreaks:        {stats['outbreaks']}
  Leading pathogen: {stats['top_pathogen'][0]} ({stats['top_pathogen'][1]} recall incidents)

MoM TREND
  Series:           {mom.get('values')}
  Current:          {mom.get('current')}
  Rolling mean:     {mom.get('rolling_mean')}
  Z vs baseline:    {mom.get('z_score')} (|Z|>2 flagged as anomalous; {mom.get('anomaly_flag')})
  Direction:        {mom.get('direction')} ({mom.get('delta_pct')}% vs prior month)

HOTSPOT CELLS (country × pathogen with observed count >2σ above independence-baseline expected count):
{hotspot_lines}

CLUSTERS (≥3 same-pathogen outbreaks within 14 days):
{cluster_lines}

CONCENTRATION
  Source HHI:          {co.get('hhi_source')} ({co.get('hhi_bucket')})
  Geographic Gini:     {co.get('gini_country')} ({co.get('gini_bucket')})
  Tier-1 share:        {co.get('tier1_share')}
  Baseline Tier-1:     {co.get('baseline_tier1_share')}
  Tier-1 intensity:    {co.get('tier1_intensity_ratio')}x vs baseline

EMERGING PATHOGENS (>2σ MoM growth vs historical share):
{emerging_lines}

COMPOSITE SEVERITY INDEX
  Score: {sv.get('score')}/100 ({sv.get('bucket')})
  Components: {sv.get('components')}

PREDICTIVE OUTLOOK
  Linear trend projection:
{lt_block}
  Poisson per-pathogen forecasts (rare pathogens, <10/month mean):
{poisson_block}

TASK. Write exactly THREE paragraphs, each 4–6 sentences, professional-engineering tone. NO headers, NO bullets, NO markdown, NO emoji. Use UK/US business English. Reference specific numbers and named pathogens from the signals above. A separate Process Authority Note may be appended — do NOT reference scheduled-process filings or FDA Form 2541 in your three paragraphs.

Paragraph 1 — MONTH HEADLINE: Anchor on the MoM direction, the Z-score (or "inside baseline" when Z is None), the dominant pathogen and its share. Call out the single most important hotspot by name (country × pathogen combo with the highest standardised residual). Quote the composite severity score and its bucket.

Paragraph 2 — STRUCTURAL INTERPRETATION: Explain WHY the month looks the way it does using the hotspot, cluster, and concentration signals. Commit to a most-likely mechanism — is this a single-country regional event (high Gini), a coordinated multi-jurisdictional signal (low Gini, low HHI), or an agency-concentrated data artefact (high HHI)? Tie the dominant pathogen to a specific production-system failure mode (environmental harbourage, raw-material sourcing, thermal underprocess, post-process recontamination, cold-chain breach).

Paragraph 3 — FORWARD-LOOKING ENGINEERING RECOMMENDATION: Name the single highest-leverage verification step a QA director should take this month, tied to (a) the emerging-pathogen list and (b) the linear-trend and Poisson forecasts. Reference the specific predictive upper bound if material (e.g. "p95 upper bound for C. botulinum sits at N over the next month"). Be specific and commit to a concrete control (CCP re-verification, environmental monitoring intensity increase, supplier verification audit, thermocouple placement check) rather than hedging.

Return only the three paragraphs separated by a single blank line."""

    if pa_extension:
        prompt += "\n\n" + pa_extension
        log.info("Process Authority trigger fired (monthly): %s",
                 _count_phrase(pa_trigger.get("total_matches", 0), "matching incident"))

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         CLAUDE_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-sonnet-4-20250514",
                "max_tokens": 2200 if pa_extension else 1600,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=90,
        )
        if r.status_code != 200:
            log.warning("Claude %d: %s", r.status_code, r.text[:200])
            return _fallback_narrative(stats, signals, models, month_name, year, pa_trigger)
        data = r.json()
        parts = [b.get("text", "") for b in data.get("content", []) if b.get("type") == "text"]
        narrative = "\n\n".join(p for p in parts if p).strip()
        return narrative or _fallback_narrative(stats, signals, models, month_name, year, pa_trigger)
    except Exception as e:
        log.warning("Claude monthly narrative failed: %s", e)
        return _fallback_narrative(stats, signals, models, month_name, year, pa_trigger)


def _fallback_narrative(stats: Dict[str, Any], signals: Dict[str, Any],
                        models: Dict[str, Any], month_name: str, year: int,
                        pa_trigger: Dict[str, Any]) -> str:
    mom = signals["mom_trend"]
    hs  = signals["hotspot"]
    cl  = signals["cluster"]
    co  = signals["concentration"]
    sv  = signals["severity"]
    lt  = models["linear_trend"]
    top_name, top_count = stats["top_pathogen"]
    pct = round(top_count / stats["total"] * 100) if stats["total"] else 0

    hotspot_txt = ""
    if hs.get("hotspots"):
        h = hs["hotspots"][0]
        hotspot_txt = (f" The standout hotspot was {h['country']} × {h['pathogen']}, "
                       f"with {h['observed']} recalls against an independence-baseline "
                       f"expectation of {h['expected']} (stdres {h['stdres']:+.2f}).")

    z_phrase = (f", a {mom['z_score']:+.1f}-sigma anomaly vs the prior-month baseline"
                if mom.get("z_score") is not None else
                " — the rolling baseline is too narrow for a Z estimate this early in the series")

    cluster_count = cl.get("cluster_count", 0)
    if stats['outbreaks'] and cluster_count:
        outbreak_phrase = (f"{_count_phrase(stats['outbreaks'], 'outbreak-associated event')}, "
                           f"forming {_count_phrase(cluster_count, 'same-pathogen temporal cluster')}")
    elif stats['outbreaks']:
        outbreak_phrase = (f"{_count_phrase(stats['outbreaks'], 'outbreak-associated event')} "
                           f"(no temporal clustering)")
    elif cluster_count:
        outbreak_phrase = f"{_count_phrase(cluster_count, 'temporal cluster')} (no outbreak label)"
    else:
        outbreak_phrase = "no outbreak-associated events"

    p1 = (f"{month_name} {year} produced {stats['total']} food-safety hazard recall incidents "
          f"across the AFTS monitoring network, a {mom.get('delta_pct')}% move "
          f"{'above' if mom.get('direction')=='up' else 'below' if mom.get('direction')=='down' else 'flat vs'} "
          f"the prior month{z_phrase}. {top_name} dominated with {top_count} of "
          f"{stats['total']} incidents ({pct}%). The composite severity index closed at "
          f"{sv.get('score')}/100 ({sv.get('bucket')}), with {stats['tier1']} Tier-1 "
          f"critical events and {outbreak_phrase} on record.{hotspot_txt}")

    bucket_phrase = {"diverse": "signal diversity consistent with broad regulatory engagement",
                     "moderate": "moderate source concentration",
                     "concentrated": "a signal driven by one or two agencies"}.get(
        co.get("hhi_bucket"), "mixed signal concentration")
    gini_phrase = {"even": "geographically even",
                   "moderate": "moderately uneven geographically",
                   "very_uneven": "strongly concentrated in a single country"}.get(
        co.get("gini_bucket"), "")

    intensity = co.get("tier1_intensity_ratio")
    if intensity is None:
        intensity_clause = (
            "The Tier-1 intensity ratio is not yet computable — the prior-month "
            "baseline has too few observations to anchor a stable ratio."
        )
    else:
        intensity_clause = (
            f"The Tier-1 intensity ratio of {intensity:.2f}× vs the rolling baseline "
            f"indicates severity is "
            f"{'elevated' if intensity > 1.1 else 'in line' if 0.9 <= intensity <= 1.1 else 'below baseline'}."
        )

    p2 = (f"Structurally, the month reads as {gini_phrase} with {bucket_phrase} "
          f"(Source HHI {co.get('hhi_source')}, Geographic Gini {co.get('gini_country')}). "
          f"For a {top_name}-dominated month, the relevant failure modes are "
          f"post-process environmental harbourage in Zone 1 of RTE lines, sanitation SOP "
          f"drift, and cold-chain lapses — not thermal underprocess. {intensity_clause}")

    lt_txt = ""
    if lt.get("status") == "active":
        n_obs = lt.get("n", 0)
        if n_obs < 12 or not lt.get("slope_significant"):
            lt_txt = (f" The linear-trend projection for next month stands at "
                      f"{lt.get('next_month_point')} recalls (95% CI "
                      f"{lt.get('next_month_ci95')}), with r²={lt.get('r_squared')}, "
                      f"slope {lt.get('slope_per_month')}/month and t={lt.get('t_stat')} "
                      f"on {lt.get('n_dof', max(0, n_obs-2))} degrees of freedom. The estimate "
                      f"is exploratory because the series is short — only {n_obs} monthly "
                      f"observation(s) — and the slope is not statistically significant at α=0.05.")
        else:
            lt_txt = (f" The linear-trend projection for next month stands at "
                      f"{lt.get('next_month_point')} recalls (95% CI "
                      f"{lt.get('next_month_ci95')}), with r²={lt.get('r_squared')}, "
                      f"slope {lt.get('slope_per_month')}/month and t={lt.get('t_stat')} "
                      f"on {lt.get('n_dof', n_obs-2)} degrees of freedom — significant at α=0.05.")

    p3 = (f"Looking forward, operators in {top_name}-relevant commodity categories should "
          f"re-verify the single highest-leverage control this month: environmental "
          f"monitoring swab frequency on RTE deli and dairy lines, or pasteurisation "
          f"D-value validation on low-moisture commodities, whichever matches their "
          f"product mix.{lt_txt} Documentation packages should be ready for rapid "
          f"regulatory response given continued inspection intensity.")

    body = f"{p1}\n\n{p2}\n\n{p3}"

    pa_note = pa_deterministic_fallback(pa_trigger or {"fired": False})
    if pa_note:
        body = f"{body}\n\n{pa_note}"
    return body


# ---------------------------------------------------------------------------
# SVG RENDERERS — zero-JS, email-safe inline graphics
# ---------------------------------------------------------------------------
def svg_mom_sparkline(mom: Dict[str, Any], w: int = 320, h: int = 72) -> str:
    """Sparkline of month-over-month counts with current-month marker."""
    values = mom.get("values", [])
    if not values or len(values) < 2:
        return f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto;display:block;margin:0 auto;"></svg>'
    counts = [c for _, c in values]
    labels = [ym for ym, _ in values]
    mx = max(counts) or 1
    pad_x, pad_y = 24, 12
    iw, ih = w - 2 * pad_x, h - 2 * pad_y
    step = iw / max(1, len(counts) - 1)
    points = [
        (pad_x + i * step, pad_y + ih - (c / mx) * ih)
        for i, c in enumerate(counts)
    ]
    path = "M " + " L ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    area = path + f" L {points[-1][0]:.1f},{pad_y+ih:.1f} L {points[0][0]:.1f},{pad_y+ih:.1f} Z"
    last_x, last_y = points[-1]
    direction_colour = (TIER1_RED if mom.get("direction") == "up" and (mom.get("delta_pct") or 0) > 20
                        else "#059669" if mom.get("direction") == "down" else BRAND_ORANGE)

    labels_svg = "".join(
        f'<text x="{x:.1f}" y="{h-2}" text-anchor="middle" font-size="8" '
        f'font-family="DM Mono,monospace" fill="#64748b">{escape(lbl[5:])}</text>'
        for (x, _), lbl in zip(points, labels)
    )
    return (
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto;display:block;margin:0 auto;">'
        f'<defs><linearGradient id="sg" x1="0" x2="0" y1="0" y2="1">'
        f'<stop offset="0%" stop-color="{direction_colour}" stop-opacity="0.35"/>'
        f'<stop offset="100%" stop-color="{direction_colour}" stop-opacity="0"/>'
        f'</linearGradient></defs>'
        f'<path d="{area}" fill="url(#sg)"/>'
        f'<path d="{path}" fill="none" stroke="{direction_colour}" stroke-width="1.8"/>'
        f'<circle cx="{last_x:.1f}" cy="{last_y:.1f}" r="3.5" fill="{direction_colour}"/>'
        f'<text x="{last_x+6:.1f}" y="{last_y-4:.1f}" font-size="10" font-weight="700" '
        f'font-family="DM Mono,monospace" fill="{direction_colour}">{counts[-1]}</text>'
        f'{labels_svg}'
        f'</svg>'
    )


def svg_hotspot_heatmap(hs: Dict[str, Any], w: int = 620) -> str:
    """Country × Pathogen heatmap; hotspot cells (>2σ) bordered in red."""
    rows = hs.get("row_labels", [])
    cols = hs.get("col_labels", [])
    mat  = hs.get("matrix", [])
    if not rows or not cols:
        return '<div style="font-size:12px;color:#64748b;font-style:italic">No distribution data this month.</div>'

    cell_w, cell_h = 76, 40
    label_w, label_h = 160, 58
    W = label_w + cell_w * len(cols) + 10
    H = label_h + cell_h * len(rows) + 10
    max_obs = max(
        (cell["observed"] for row in mat for cell in row), default=1
    ) or 1

    out = [f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto;display:block;margin:0 auto;">']

    # Column headers (pathogens, rotated-ish — kept horizontal for email compat, truncated)
    for j, col in enumerate(cols):
        x = label_w + j * cell_w + cell_w / 2
        out.append(
            f'<text x="{x:.1f}" y="{label_h-8}" text-anchor="middle" '
            f'font-size="9" font-family="DM Mono,monospace" '
            f'font-weight="700" fill="{BRAND_BLACK}">'
            f'<tspan font-style="italic">{escape(col[:14])}</tspan></text>'
        )

    # Rows
    for i, row in enumerate(rows):
        y = label_h + i * cell_h + cell_h / 2 + 4
        out.append(
            f'<text x="{label_w-8}" y="{y:.1f}" text-anchor="end" '
            f'font-size="10" font-family="Inter,sans-serif" font-weight="600" '
            f'fill="{BRAND_BLACK}">{escape(row[:22])}</text>'
        )
        for j, col in enumerate(cols):
            cell = mat[i][j]
            obs = cell["observed"]
            intensity = obs / max_obs if max_obs else 0
            # Orange colour scale
            shade = int(245 - intensity * 145)
            fill = f'rgb(254,{max(120,shade)},{max(90,shade-30)})' if obs else "#f3f4f6"
            if obs == 0:
                text_col = "#94a3b8"
            elif intensity > 0.55:
                text_col = "#ffffff"
            else:
                text_col = BRAND_BLACK
            stroke = TIER1_RED if cell["hotspot"] else "#e5e7eb"
            sw = 2 if cell["hotspot"] else 1
            x = label_w + j * cell_w
            cy = label_h + i * cell_h
            out.append(
                f'<rect x="{x}" y="{cy}" width="{cell_w}" height="{cell_h}" '
                f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}"/>'
            )
            out.append(
                f'<text x="{x+cell_w/2:.1f}" y="{cy+cell_h/2+4:.1f}" '
                f'text-anchor="middle" font-size="13" font-weight="700" '
                f'font-family="DM Mono,monospace" fill="{text_col}">{obs}</text>'
            )
            # Small std-residual indicator under each non-zero cell
            if obs > 0:
                out.append(
                    f'<text x="{x+cell_w/2:.1f}" y="{cy+cell_h-5:.1f}" '
                    f'text-anchor="middle" font-size="7" '
                    f'font-family="DM Mono,monospace" fill="{text_col}" opacity="0.65">'
                    f'σ={cell["stdres"]:+.1f}</text>'
                )

    out.append('</svg>')
    return "".join(out)


def svg_outbreak_timeline(cl: Dict[str, Any], month_start: date, month_end: date,
                          w: int = 620, h: int = 110) -> str:
    events = cl.get("events", [])
    if not events:
        return (f'<div style="font-size:12px;color:#64748b;font-style:italic;'
                f'padding:14px 0">No outbreak events recorded in this month.</div>')
    total_days = (month_end - month_start).days or 1
    pad_x, pad_y = 36, 20
    iw = w - 2 * pad_x
    axis_y = h - 32

    # Tick marks every ~5 days
    ticks = []
    for day_offset in range(0, total_days + 1, 5):
        tx = pad_x + (day_offset / total_days) * iw
        ticks.append(
            f'<line x1="{tx:.1f}" y1="{axis_y}" x2="{tx:.1f}" y2="{axis_y+4}" '
            f'stroke="#cbd5e1" stroke-width="1"/>'
            f'<text x="{tx:.1f}" y="{axis_y+18}" text-anchor="middle" '
            f'font-size="8" font-family="DM Mono,monospace" fill="#64748b">'
            f'{(month_start + timedelta(days=day_offset)).strftime("%d %b")}</text>'
        )

    # Pathogen lane colours
    lane_map = {
        "Salmonella":              (BRAND_ORANGE, 0),
        "Listeria":                (TIER1_RED, 1),
        "Norovirus":               ("#818cf8", 2),
        "C. botulinum":            (OUTBREAK_VIO, 3),
        "E. coli / STEC":          ("#f97316", 4),
    }

    markers = []
    seen_pathogens = set()
    for ev in events:
        d_event = datetime.strptime(ev["date"], "%Y-%m-%d").date()
        offset = (d_event - month_start).days
        x = pad_x + (offset / total_days) * iw
        path = ev["pathogen"]
        colour, lane = lane_map.get(path, (BRAND_BLACK, 5))
        y = pad_y + lane * 12
        seen_pathogens.add(path)
        markers.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{colour}" '
            f'stroke="#fff" stroke-width="1.5"><title>{escape(ev["date"])} · '
            f'{escape(path)} · {escape(ev["country"])} · {escape(ev["company"][:40])}</title></circle>'
            f'<line x1="{x:.1f}" y1="{y+5:.1f}" x2="{x:.1f}" y2="{axis_y}" '
            f'stroke="{colour}" stroke-width="1" opacity="0.3"/>'
        )

    # Legend
    legend_parts = []
    lx = pad_x
    for p in sorted(seen_pathogens, key=lambda p: lane_map.get(p, (None, 99))[1]):
        colour = lane_map.get(p, (BRAND_BLACK,))[0]
        legend_parts.append(
            f'<circle cx="{lx+5:.1f}" cy="{h-4:.1f}" r="3" fill="{colour}"/>'
            f'<text x="{lx+12:.1f}" y="{h-1:.1f}" font-size="9" '
            f'font-family="DM Mono,monospace" fill="#475569">'
            f'<tspan font-style="italic">{escape(p[:18])}</tspan></text>'
        )
        lx += 12 + 8 + len(p[:18]) * 5.5 + 10

    return (
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto;display:block;margin:0 auto;">'
        f'<line x1="{pad_x}" y1="{axis_y}" x2="{w-pad_x}" y2="{axis_y}" '
        f'stroke="#94a3b8" stroke-width="1.5"/>'
        f'{"".join(ticks)}'
        f'{"".join(markers)}'
        f'{"".join(legend_parts)}'
        f'</svg>'
    )


def svg_weekly_cadence(cadence: Dict[str, Any], w: int = 300, h: int = 72) -> str:
    weeks = cadence.get("weeks", [])
    if not weeks:
        return f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto;display:block;margin:0 auto;"></svg>'
    counts = [wk["count"] for wk in weeks]
    labels = [wk["label"].split("-W")[-1] for wk in weeks]
    mx = max(counts) or 1
    pad_x, pad_y = 20, 14
    iw, ih = w - 2 * pad_x, h - 2 * pad_y
    bar_w = iw / len(counts) - 6
    bars = []
    for i, (c, lbl) in enumerate(zip(counts, labels)):
        bh = (c / mx) * ih
        x = pad_x + i * (iw / len(counts)) + 3
        y = pad_y + ih - bh
        bars.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" '
            f'fill="{BRAND_ORANGE}" opacity="0.85" rx="1"/>'
            f'<text x="{x+bar_w/2:.1f}" y="{y-3:.1f}" text-anchor="middle" '
            f'font-size="9" font-weight="700" font-family="DM Mono,monospace" '
            f'fill="{BRAND_BLACK}">{c}</text>'
            f'<text x="{x+bar_w/2:.1f}" y="{h-2:.1f}" text-anchor="middle" '
            f'font-size="8" font-family="DM Mono,monospace" fill="#64748b">W{lbl}</text>'
        )
    return f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto;display:block;margin:0 auto;">{"".join(bars)}</svg>'


def svg_severity_gauge(sv: Dict[str, Any], w: int = 200, h: int = 110) -> str:
    score  = sv.get("score") or 0
    bucket = sv.get("bucket") or "unknown"
    cx, cy, r = w / 2, h - 18, 70
    # Semi-circle gauge (180°)
    import math
    def arc_path(start_deg, end_deg, radius):
        sx = cx + radius * math.cos(math.radians(180 - start_deg))
        sy = cy - radius * math.sin(math.radians(180 - start_deg))
        ex = cx + radius * math.cos(math.radians(180 - end_deg))
        ey = cy - radius * math.sin(math.radians(180 - end_deg))
        large = 1 if (end_deg - start_deg) > 180 else 0
        return f"M {sx:.1f} {sy:.1f} A {radius} {radius} 0 {large} 1 {ex:.1f} {ey:.1f}"

    colours = [("#059669", 0, 20), (BRAND_ORANGE, 20, 40),
               (TIER2_AMBER, 40, 60), (TIER1_RED, 60, 80),
               (OUTBREAK_VIO, 80, 100)]
    tracks = "".join(
        f'<path d="{arc_path(a/100*180, b/100*180, r)}" fill="none" stroke="{c}" '
        f'stroke-width="12" opacity="0.8"/>'
        for c, a, b in colours
    )
    needle_angle = 180 * (score / 100)
    nx = cx + (r - 8) * math.cos(math.radians(180 - needle_angle))
    ny = cy - (r - 8) * math.sin(math.radians(180 - needle_angle))
    return (
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" style="max-width:100%;height:auto;display:block;margin:0 auto;">'
        f'{tracks}'
        f'<line x1="{cx}" y1="{cy}" x2="{nx:.1f}" y2="{ny:.1f}" '
        f'stroke="{BRAND_BLACK}" stroke-width="2.5"/>'
        f'<circle cx="{cx}" cy="{cy}" r="5" fill="{BRAND_BLACK}"/>'
        f'<text x="{cx}" y="{cy-35}" text-anchor="middle" font-size="22" '
        f'font-weight="800" font-family="Syne,sans-serif" fill="{BRAND_BLACK}">{score}</text>'
        f'<text x="{cx}" y="{cy-18}" text-anchor="middle" font-size="9" '
        f'font-family="DM Mono,monospace" letter-spacing="1.5" fill="#64748b" '
        f'text-transform="uppercase">{escape(bucket).upper()}</text>'
        f'</svg>'
    )


# ---------------------------------------------------------------------------
# Predictive roadmap panel — always visible, shows what's active today and
# what activates as more data accumulates
# ---------------------------------------------------------------------------
def render_models_panel(models: Dict[str, Any]) -> str:
    """HTML card list for the § 07 Predictive Outlook section."""
    order = [
        ("linear_trend",  "Linear trend projection",  "OLS on total monthly counts"),
        ("poisson",       "Poisson per-pathogen forecast", "For rare pathogens (<10 incidents/mo)"),
        ("cusum",         "CUSUM change-point detection", "Page-1954 tabular CUSUM"),
        ("ols_seasonal",  "OLS with seasonal dummies", "Linear trend + month-of-year effects"),
        ("stl",           "STL decomposition", "Trend + seasonal + residual (LOESS)"),
        ("holt_winters",  "Holt-Winters", "Level + trend + seasonal smoothing"),
        ("sarima",        "SARIMA",       "Seasonal ARIMA(p,d,q)(P,D,Q)s"),
        ("prophet",       "Prophet",      "Additive seasonality + holidays"),
    ]
    cards = []
    for key, name, subtitle in order:
        m = models.get(key, {})
        if key == "poisson":
            active = any(isinstance(v, dict) and v.get("status") == "active"
                         for v in m.get("by_pathogen", {}).values())
            status_txt = "ACTIVE" if active else "INACTIVE"
            detail = _poisson_detail(m) if active else m.get("message", "—")
        elif m.get("status") == "active":
            active = True
            status_txt = "ACTIVE"
            detail = _model_active_detail(key, m)
        else:
            active = False
            status_txt = m.get("message", "Activates later")
            detail = ""
        colour = "#059669" if active else "#94a3b8"
        bg = "rgba(5,150,105,.06)" if active else "rgba(148,163,184,.04)"
        cards.append(f"""
<div class="mdl-card" style="border-left:3px solid {colour};background:{bg}">
  <div class="mdl-hdr">
    <span class="mdl-name">{escape(name)}</span>
    <span class="mdl-status" style="color:{colour}">{escape(status_txt)}</span>
  </div>
  <div class="mdl-sub">{escape(subtitle)}</div>
  {f'<div class="mdl-detail">{detail}</div>' if detail else ''}
</div>""")
    return "".join(cards)


def _model_active_detail(key: str, m: Dict[str, Any]) -> str:
    if key == "linear_trend":
        point = m.get("next_month_point")
        ci    = m.get("next_month_ci95", [None, None])
        slope = m.get("slope_per_month")
        r2    = m.get("r_squared")
        note  = m.get("note", "")
        return (f"Next-month forecast: <strong>{point}</strong> recalls "
                f"(95% CI [{ci[0]}, {ci[1]}]); "
                f"slope <strong>{slope:+.1f}/month</strong>; "
                f"r²={r2}. {escape(note)}")
    if key == "cusum":
        if m.get("change_detected"):
            return (f"Change detected in <strong>{m.get('change_month')}</strong> "
                    f"({m.get('direction')}). {escape(m.get('note',''))}")
        return escape(m.get("note", "In statistical control."))
    if key == "ols_seasonal":
        return (f"Intercept (Jan): {m.get('intercept_Jan')}; "
                f"slope {m.get('monthly_slope'):+.2f}/month. "
                f"Seasonal effects vs Jan captured via month dummies.")
    return escape(m.get("note", "—"))


def _poisson_detail(m: Dict[str, Any]) -> str:
    rows = []
    for p, f in (m.get("by_pathogen") or {}).items():
        if not isinstance(f, dict) or f.get("status") != "active":
            continue
        rows.append(
            f'<tr><td><em>{escape(p)}</em></td>'
            f'<td class="num">{f["lambda"]}</td>'
            f'<td class="num">{f["last"]}</td>'
            f'<td class="num">{f["p90"]}</td>'
            f'<td class="num">{f["p95"]}</td></tr>'
        )
    if not rows:
        return "No rare pathogens with active Poisson fit."
    return (f'<table class="mini"><thead><tr>'
            f'<th>Pathogen</th><th>λ̂</th><th>last</th><th>p90</th><th>p95</th>'
            f'</tr></thead><tbody>{"".join(rows)}</tbody></table>')


# ---------------------------------------------------------------------------
# All-month companion file
# ---------------------------------------------------------------------------
def build_all_month_html(month_start: date, month_end: date,
                         month_recalls: List[Dict],
                         back_href: str) -> str:
    """Companion page listing every recall in the month (linked from § 08)."""
    rows = weekly.rank_top_recalls(month_recalls, n=len(month_recalls))
    body_rows = "".join(weekly.render_top5_row(i+1, r) for i, r in enumerate(rows))
    month_name = month_start.strftime("%B %Y")
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>AFTS · All Recalls · {escape(month_name)}</title>
<style>
body{{font-family:'Inter',sans-serif;background:#f5f5f7;margin:0;padding:32px 20px;color:#1f2937;}}
.wrap{{max-width:1080px;margin:0 auto;background:#fff;padding:32px 40px;border:1px solid #e5e7eb;}}
.brand{{font-family:Syne,Georgia,serif;font-weight:800;font-size:16px;letter-spacing:-0.01em;
text-transform:uppercase;color:{BRAND_BLACK};margin-bottom:8px;}}
.brand em{{color:{BRAND_ORANGE};font-style:normal;}}
h1{{font-family:Syne,Georgia,serif;font-weight:800;font-size:26px;margin:0 0 6px;}}
.sub{{font-family:'DM Mono',monospace;font-size:10px;color:#6b7280;text-transform:uppercase;
letter-spacing:0.1em;margin:0 0 20px;}}
.back{{font-family:'DM Mono',monospace;font-size:10px;letter-spacing:0.08em;
color:{BRAND_ORANGE};text-decoration:none;display:inline-block;margin-bottom:16px;
padding:8px 16px;border:1px solid #e5e7eb;border-radius:2px;text-transform:uppercase;}}
.back:hover{{background:{BRAND_ORANGE};color:#fff;border-color:{BRAND_ORANGE};}}
table.data{{width:100%;border-collapse:collapse;font-size:13px;}}
table.data th{{background:{BRAND_BLACK};color:#fff;font-family:'DM Mono',monospace;
font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;
padding:10px 8px;text-align:left;}}
table.data td{{padding:10px 8px;border-bottom:1px solid #f3f4f6;vertical-align:top;}}
table.data tr:hover{{background:rgba(232,96,26,.04);}}
.rank-num{{font-family:Syne,sans-serif;font-weight:800;font-size:22px;color:{BRAND_ORANGE};
text-align:center;white-space:nowrap;font-variant-numeric:tabular-nums;letter-spacing:-0.02em;}}
.rank-num.rank-num--multi{{font-size:18px;}}
.date-cell{{font-family:'DM Mono',monospace;font-size:11px;color:#6b7280;white-space:nowrap;}}
.path-dot{{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:7px;vertical-align:middle;}}
.path-name{{font-weight:600;color:#1f2937;font-style:italic;}}
.co-cell strong{{color:{BRAND_BLACK};font-weight:700;display:block;}}
.brand-sub{{font-size:11px;color:#6b7280;margin-top:2px;font-style:italic;}}
.juris-country{{font-weight:600;color:#1f2937;}}
.src-sub{{font-family:'DM Mono',monospace;font-size:10px;color:#6b7280;margin-top:2px;}}
.src-link{{color:{BRAND_ORANGE};font-size:11px;text-decoration:none;font-family:'DM Mono',monospace;}}
.src-na{{font-family:'DM Mono',monospace;font-size:10px;color:#94a3b8;font-style:italic;}}
.chip-tier1,.chip-tier2,.chip-outbreak{{display:inline-block;color:#fff;font-size:9px;
font-weight:700;padding:2px 6px;border-radius:2px;letter-spacing:0.06em;margin-left:5px;}}
.chip-tier1{{background:{TIER1_RED};}}
.chip-tier2{{background:{TIER2_AMBER};color:#1f2937;}}
.chip-outbreak{{background:{OUTBREAK_VIO};}}
</style></head><body><div class="wrap">
<div class="brand">Advanced Food-Tech Solutions <em>·</em> AFTS</div>
<h1>All recalls · {escape(month_name)}</h1>
<div class="sub">{month_start.strftime('%d %b %Y')} – {month_end.strftime('%d %b %Y')}
 &middot; {len(rows)} recalls</div>
<a class="back" href="{escape(back_href)}">← Back to monthly report</a>
<table class="data top5"><thead><tr>
<th>#</th><th>Date</th><th>Pathogen</th><th>Company / Brand</th>
<th>Product</th><th>Jurisdiction &amp; Source</th>
</tr></thead><tbody>{body_rows}</tbody></table>
</div></body></html>"""


# ---------------------------------------------------------------------------
# HTML EXTRACTION HELPERS  (added 2026-05-09)
# ---------------------------------------------------------------------------
# These three helpers let pipeline/build_monthly_updates_check.py reason
# about the state of a previously-built monthly HTML on disk, without
# re-running the full builder. They mirror the equivalent helpers in
# build_weekly_report_afts.py (_extract_total_from_html etc.) so the
# weekly and monthly drift-detection paths use the same patterns.
#
# Pattern matched (rendered into the masthead by build_monthly_html):
#     <strong>PUBLISHED</strong> &middot; 1 May 2026
#   or after a rebuild:
#     <strong>UPDATED</strong> &middot; 8 May 2026
# ---------------------------------------------------------------------------

# Total Recalls KPI is the FIRST <div class="kpi-value">N</div> in the
# rendered monthly HTML — the KPI strip puts Total Recalls leftmost.
_KPI_TOTAL_RE = re.compile(r'<div class="kpi-value">(\d+)</div>')

# Masthead label line: matches both PUBLISHED and UPDATED variants.
_MASTHEAD_LABEL_RE = re.compile(
    r'<strong>(PUBLISHED|UPDATED)</strong>\s*&middot;\s*([^<]+?)<'
)


def _extract_total_from_html_monthly(path: Path) -> Optional[int]:
    """Read the Total Recalls value from an existing monthly report HTML.

    Returns the int (e.g. 359) or None if the file doesn't exist or no
    KPI value can be found. The first kpi-value div in the masthead's
    KPI strip is always Total Recalls — see the build_monthly_html
    template for the layout contract.
    """
    try:
        html = Path(path).read_text(encoding="utf-8")
        m = _KPI_TOTAL_RE.search(html)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def _extract_published_from_html_monthly(path: Path) -> Optional[str]:
    """Detect whether an existing monthly HTML carries a header date
    marker (either PUBLISHED or UPDATED).

    Returns the verbatim date string (e.g. "1 May 2026") on match, or
    None. The CONTENT of the returned string is NOT used by
    build_monthly_html after the 2026-05-09 label-flip change — the FACT
    that it returned non-None is the signal "this is a rebuild, flip the
    label to UPDATED and use today's date." Successive rebuilds keep
    flipping the date forward as expected.
    """
    try:
        html = Path(path).read_text(encoding="utf-8")
        m = _MASTHEAD_LABEL_RE.search(html)
        if m:
            return m.group(2).strip()
    except Exception:
        pass
    return None


def _extract_label_from_html_monthly(path: Path) -> Optional[str]:
    """Return the literal header label — "PUBLISHED" or "UPDATED" — from
    an existing monthly HTML, or None.

    Companion to _extract_published_from_html_monthly. Used by the
    monthly-updates-check Wednesday-equivalent flow to detect the
    "stuck PUBLISHED label after legacy rebuild" case where a report's
    count matches the dataset BUT the header is still PUBLISHED because
    it was overwritten by a builder version that pre-dated this label-
    flip code. The check forces a one-shot rebuild to flip to UPDATED
    without waiting for a count change.
    """
    try:
        html = Path(path).read_text(encoding="utf-8")
        m = _MASTHEAD_LABEL_RE.search(html)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# MAIN HTML RENDER
# ---------------------------------------------------------------------------
def build_monthly_html(month_start: date, month_end: date,
                       month_recalls: List[Dict],
                       stats: Dict[str, Any],
                       signals: Dict[str, Any],
                       models: Dict[str, Any],
                       narrative: str,
                       original_published: Optional[str] = None) -> str:
    """Render the subscriber-edition monthly HTML.

    original_published:  None  → fresh publish; masthead reads
                                 "PUBLISHED · {month_end + 1}".
                         str   → rebuild; masthead reads
                                 "UPDATED · {today}". The string itself
                                 (returned by _extract_published_from_html_monthly)
                                 is just the "this is a rebuild" signal —
                                 its content is no longer used in render.

    Audit 2026-05-09 — added original_published with label-flip semantics
    matching the weekly report. Pre-this change, every rebuild silently
    re-stamped the PUBLISHED date with month_end+1 (the formulaic publish
    day-after), so the user couldn't tell a rebuilt April report from
    its first edition. Now rebuilds carry an explicit UPDATED label
    with today's date.
    """
    month_name = month_start.strftime("%B")
    year       = month_start.year
    year_m     = f"{year}-M{month_start.month:02d}"

    # Header label flips between two states (audit 2026-05-09):
    #   First publish (original_published is None) →
    #     "PUBLISHED · {month_end + 1}" — formulaic 1st-of-next-month,
    #     matching the afts-monthly-report.yml first-of-month build.
    #   Rebuild (original_published is set) →
    #     "UPDATED · {today}" — the actual rebuild date.
    # The label change is the visible signal that the user is looking
    # at a revised version of a previously-published monthly briefing.
    if original_published:
        published_label = "UPDATED"
        pub_date = datetime.now(timezone.utc).strftime("%-d %b %Y")
    else:
        published_label = "PUBLISHED"
        pub_date = (month_end + timedelta(days=1)).strftime("%-d %b %Y")

    # Paragraphs with italic pathogens + PA-note styling
    paragraphs = [p.strip() for p in narrative.split("\n\n") if p.strip()]
    pa_label_lc = PROCESS_AUTHORITY_LABEL.lower()
    analysis_parts: List[str] = []
    for p in paragraphs:
        if p.lower().startswith(pa_label_lc):
            idx = p.lower().find(pa_label_lc)
            colon = p.find(":", idx)
            if colon != -1:
                label_text = p[idx:colon].strip()
                body_text  = p[colon + 1:].strip()
                analysis_parts.append(
                    f'<p class="pa-note"><span class="pa-label">{escape(label_text)}:</span> '
                    f'{italicise_prose(escape(body_text))}</p>'
                )
            else:
                analysis_parts.append(f'<p class="pa-note">{italicise_prose(escape(p))}</p>')
        else:
            analysis_parts.append(f'<p>{italicise_prose(escape(p))}</p>')
    analysis_html = "".join(analysis_parts)

    # Component blocks
    mom  = signals["mom_trend"]
    hs   = signals["hotspot"]
    cl   = signals["cluster"]
    co   = signals["concentration"]
    gr   = signals["growth"]
    sv   = signals["severity"]
    cad  = signals["cadence"]

    # Pathogen / hazard distribution table.
    # Reconcile the displayed rows back to the headline `total`. The
    # `pathogen_counts` list comes back from compute_stats() as the most_common
    # 15 entries — which on a heavy month leaves a residual that previously
    # silently disappeared (review-1 #2: distribution summed to 221 not 236).
    # We now emit an explicit "Other / not shown" row when residual > 0.
    total_safe = stats["total"] or 1
    listed_sum = sum(c for _, c in stats["pathogen_counts"])
    residual   = max(0, stats["total"] - listed_sum)
    path_rows_html = ""
    for name, count in stats["pathogen_counts"]:
        pct = round(count / total_safe * 100)
        _, canon = weekly.severity_score(name)
        color = weekly.pathogen_badge_color(canon)
        bar_w = max(4, min(100, pct))
        path_rows_html += f"""
<tr>
<td><span class="path-dot" style="background:{color}"></span><em class="path-name">{escape(name)}</em></td>
<td class="num">{count}</td>
<td><div class="bar"><div class="bar-fill" style="width:{bar_w}%;background:{color}"></div></div></td>
<td class="num muted">{pct}%</td>
</tr>"""
    if residual > 0:
        pct = round(residual / total_safe * 100)
        bar_w = max(4, min(100, pct))
        path_rows_html += f"""
<tr>
<td><span class="path-dot" style="background:#9ca3af"></span><em class="path-name">Other / not shown above</em></td>
<td class="num">{residual}</td>
<td><div class="bar"><div class="bar-fill" style="width:{bar_w}%;background:#9ca3af"></div></div></td>
<td class="num muted">{pct}%</td>
</tr>"""

    # Top 10 table
    top10 = weekly.rank_top_recalls(month_recalls, n=10)
    top_rows_html = "".join(weekly.render_top5_row(i+1, r) for i, r in enumerate(top10))

    # Appendix — ALL recalls table (ranked by severity, same as companion page)
    all_ranked = weekly.rank_top_recalls(month_recalls, n=len(month_recalls))
    all_rows_html = "".join(weekly.render_top5_row(i+1, r) for i, r in enumerate(all_ranked))

    # MoM description
    delta_phrase = (
        f"{stats.get('delta'):+d} ({stats.get('delta_pct'):+d}%) vs prior month"
        if stats.get("delta") is not None and stats.get("delta_pct") is not None
        else "baseline month"
    )

    # Hotspot callouts
    if hs.get("hotspots"):
        hotspot_items = "".join(
            f'<li><strong>{escape(h["country"])}</strong> × <em>{escape(h["pathogen"])}</em>: '
            f'<strong>{h["observed"]}</strong> recalls observed vs '
            f'{h["expected"]} expected under independence '
            f'(σ={h["stdres"]:+.2f}, {h["ratio"]}× expected)</li>'
            for h in hs["hotspots"][:3]
        )
    else:
        hotspot_items = '<li class="empty">No statistically significant hotspot cells (σ≤2 across the whole matrix).</li>'

    # Concentration summary
    hhi_bucket   = co.get("hhi_bucket") or "unknown"
    gini_bucket  = co.get("gini_bucket") or "unknown"
    intensity    = co.get("tier1_intensity_ratio")
    intensity_txt = (f'{intensity}×' if intensity is not None else 'n/a')

    # Emerging / declining lists. The growth module collapses all Salmonella
    # serovars (Typhimurium, Bovismorbificans, etc.) to "Salmonella spp."
    # for the Z-score calculation, while §03 distribution preserves serovars
    # — so the emerging count for "Salmonella spp." here is the SUM across
    # serovars. We label it explicitly so the 29-here vs 27+2-in-§03 reads
    # as a deliberate aggregation, not a discrepancy.
    def _aggregation_hint(name: str) -> str:
        n = name.strip().lower()
        if n in ("salmonella spp.", "salmonella"):
            return " <span class=\"agg-note\">(all serovars combined)</span>"
        if n in ("e. coli stec", "stec"):
            return " <span class=\"agg-note\">(all STEC variants combined)</span>"
        return ""

    emerging_html = "".join(
        f'<li><em>{escape(e["pathogen"])}</em>{_aggregation_hint(e["pathogen"])}: '
        f'{e["count"]} recall incidents, Z={e["z_score"]:+.2f}, '
        f'MoM {e["growth_pct"]:+.0f}%</li>'
        for e in gr.get("emerging", [])[:4]
    ) or '<li class="empty">No pathogens with &gt;2σ month-over-month emergence.</li>'

    declining_html = "".join(
        f'<li><em>{escape(d["pathogen"])}</em>{_aggregation_hint(d["pathogen"])}: '
        f'{d["count"]} recall incidents, Z={d["z_score"]:+.2f}, '
        f'MoM {d["growth_pct"]:+.0f}%</li>'
        for d in gr.get("declining", [])[:3]
    ) or '<li class="empty">No pathogens with &gt;2σ month-over-month decline.</li>'

    # HHI / Gini reproducibility tables (review-1 audit guidance)
    src_total = sum(c for _, c in stats["source_counts"]) or 1
    hhi_rows_html = "".join(
        f'<tr><td class="repro-lbl">{escape(name)}</td>'
        f'<td class="repro-num">{cnt}</td>'
        f'<td class="repro-num">{cnt/src_total*100:.1f}%</td>'
        f'<td class="repro-num">{((cnt/src_total)*100)**2:.1f}</td></tr>'
        for name, cnt in stats["source_counts"][:12]
    )
    cty_total = sum(c for _, c in stats["country_counts"]) or 1
    gini_rows_html = "".join(
        f'<tr><td class="repro-lbl">{escape(name)}</td>'
        f'<td class="repro-num">{cnt}</td>'
        f'<td class="repro-num">{cnt/cty_total*100:.1f}%</td></tr>'
        for name, cnt in stats["country_counts"][:12]
    )

    # Cluster summary
    if cl.get("clusters"):
        cluster_html = "".join(
            f'<li><em>{escape(c["pathogen"])}</em> — <strong>{c["size"]} events</strong> '
            f'in {c["span_days"]} days across '
            f'{", ".join(escape(x) for x in c["countries"])}.</li>'
            for c in cl["clusters"][:3]
        )
    else:
        cluster_html = (
            '<li class="empty">No temporal clusters detected — outbreak events '
            f'({cl.get("event_count", 0)} this month) are sporadic rather than linked.</li>'
        )

    # Predictive roadmap
    models_panel_html = render_models_panel(models)

    # KPI values
    top_pathogen_name = stats["top_pathogen"][0] or "–"
    top_pathogen_pct  = round(stats["top_pathogen"][1] / total_safe * 100) if total_safe else 0
    mom_delta_label   = (f"{mom.get('delta_pct'):+.0f}%" if mom.get("delta_pct") is not None else "—")
    z_label           = (f"Z = {mom.get('z_score'):+.1f}" if mom.get("z_score") is not None else "Z = n/a")

    # MoM direction arrow / colour
    mom_colour = TIER1_RED if mom.get("direction") == "up" and (mom.get("delta_pct") or 0) > 20 \
                 else "#059669" if mom.get("direction") == "down" else BRAND_ORANGE

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AFTS Monthly · {escape(month_name)} {year}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;700&family=DM+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root{{
--bg:#ffffff; --s1:#f9fafb; --s2:#f3f4f6; --brd:#e5e7eb;
--ink:#1f2937; --black:{BRAND_BLACK}; --muted:#6b7280; --body:#374151;
--orange:{BRAND_ORANGE}; --red:{TIER1_RED}; --amber:{TIER2_AMBER};
--violet:{OUTBREAK_VIO}; --green:#059669;
}}
*{{box-sizing:border-box;}}
html,body{{margin:0;padding:0;background:#f5f5f7;font-family:'Times New Roman',Times,Georgia,serif;color:var(--ink);font-size:14.5px;line-height:1.5;}}
body{{padding:28px 16px 60px;}}
.page{{max-width:980px;margin:0 auto;background:#fff;padding:36px 44px;border:1px solid var(--brd);}}
a{{color:var(--orange);}} a:hover{{color:{BRAND_BLACK};}}

.mast{{border-bottom:1px solid var(--brd);padding-bottom:18px;margin-bottom:26px;
display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;}}
.brand{{font-family:Syne,Georgia,serif;font-weight:800;font-size:18px;color:{BRAND_BLACK};
text-transform:uppercase;letter-spacing:-0.01em;}}
.brand em{{color:{BRAND_ORANGE};font-style:normal;}}
.tagline{{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);
text-transform:uppercase;letter-spacing:0.14em;margin-top:4px;}}
.pubdate{{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);
text-transform:uppercase;letter-spacing:0.12em;margin-top:6px;}}
.pubdate strong{{color:{BRAND_BLACK};font-weight:700;}}
.pill{{background:{BRAND_BLACK};color:#fff;font-family:'DM Mono',monospace;font-size:10px;
padding:5px 12px;letter-spacing:0.12em;text-transform:uppercase;}}
h1.r-title{{font-family:Syne,Georgia,serif;font-weight:800;font-size:30px;color:{BRAND_BLACK};
letter-spacing:-0.02em;margin:14px 0 6px;}}
h1.r-title .accent{{color:{BRAND_ORANGE};}}
.sub{{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);
text-transform:uppercase;letter-spacing:0.1em;margin:0 0 26px;}}

.kpi-strip{{display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:var(--brd);
border:1px solid var(--brd);margin-bottom:28px;}}
.kpi{{background:#fff;padding:16px 14px;}}
.kpi-label{{font-family:'DM Mono',monospace;font-size:9px;font-weight:700;color:var(--muted);
text-transform:uppercase;letter-spacing:0.1em;margin-bottom:6px;}}
.kpi-value{{font-family:Syne,Georgia,serif;font-weight:800;font-size:26px;line-height:1;color:{BRAND_BLACK};}}
.kpi-value.red{{color:var(--red);}} .kpi-value.vio{{color:var(--violet);}}
.kpi-value.orange{{color:var(--orange);font-style:italic;font-size:15px;line-height:1.3;}}
.kpi-value.mom{{font-size:22px;}}
.kpi-top{{font-size:10px;color:var(--muted);margin-top:6px;}}
@media(max-width:760px){{.kpi-strip{{grid-template-columns:repeat(3,1fr);}}}}

.sec-head{{display:flex;align-items:center;gap:12px;margin:32px 0 12px;}}
.sec-num{{font-family:'DM Mono',monospace;font-size:10px;color:{BRAND_ORANGE};font-weight:700;letter-spacing:0.12em;}}
.sec-title{{font-family:Syne,Georgia,serif;font-weight:800;font-size:20px;color:{BRAND_BLACK};letter-spacing:-0.01em;margin:0;}}
.sec-rule{{flex:1;height:1px;background:var(--brd);}}
.sec-caption{{color:var(--muted);font-size:13px;margin:-4px 0 14px;}}
.sec-link{{font-family:'DM Mono',monospace;font-size:10px;color:var(--orange);letter-spacing:0.08em;text-transform:uppercase;text-decoration:none;}}
.sec-link:hover{{text-decoration:underline;}}

.analysis{{background:var(--s1);padding:24px 28px;margin-bottom:12px;}}
.analysis p{{font-size:14px;line-height:1.75;color:var(--ink);margin:0 0 14px;}}
.analysis p:last-child{{margin-bottom:0;}}
.analysis p.pa-note{{margin:18px -28px 0 -28px;padding:18px 28px 2px 28px;background:#fff;
border-top:1px solid var(--brd);font-size:13.5px;line-height:1.7;}}
.analysis p.pa-note .pa-label{{display:inline;font-family:'DM Mono',monospace;font-weight:700;
letter-spacing:0.08em;text-transform:uppercase;color:var(--red);font-size:10px;margin-right:8px;}}

/* Trend panel */
.trend-grid{{display:grid;grid-template-columns:1fr;gap:20px;margin-bottom:6px;justify-items:center;}}
.trend-grid > * {{ width:100%; max-width:680px; }}
.trend-panel{{background:var(--s1);padding:18px 22px;border-left:3px solid var(--orange);}}
.trend-num{{font-family:Syne,sans-serif;font-weight:800;font-size:30px;color:{BRAND_BLACK};}}
.trend-num.up{{color:var(--red);}} .trend-num.down{{color:var(--green);}}
.trend-lbl{{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);
letter-spacing:0.08em;text-transform:uppercase;margin-top:2px;}}
.trend-row{{display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:10px;}}

/* Heatmap */
.heat-panel{{background:var(--s1);padding:20px 22px;text-align:center;}}
.heat-panel svg{{max-width:100%;height:auto;display:block;margin:0 auto;}}
.hotspot-list{{margin:12px 0 0;padding:0;list-style:none;font-size:13px;}}
.hotspot-list li{{padding:6px 0;color:var(--ink);border-top:1px dashed var(--brd);}}
.hotspot-list li:first-child{{border-top:none;}}
.hotspot-list li.empty{{color:var(--muted);font-style:italic;}}

/* Timeline */
.timeline-panel{{background:var(--s1);padding:20px 22px;text-align:center;}}
.timeline-panel svg{{max-width:100%;height:auto;display:block;margin:0 auto;}}

/* Concentration */
.conc-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:10px;}}
@media(max-width:600px){{.conc-grid{{grid-template-columns:1fr;}}}}
.conc-card{{background:var(--s1);padding:16px 18px;border-left:3px solid var(--orange);}}
.conc-card.diverse{{border-left-color:var(--green);}}
.conc-card.concentrated{{border-left-color:var(--red);}}
.conc-card.very_uneven{{border-left-color:var(--red);}}
.conc-card.moderate{{border-left-color:var(--amber);}}
.conc-val{{font-family:Syne,sans-serif;font-weight:800;font-size:22px;color:{BRAND_BLACK};}}
.conc-lbl{{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);letter-spacing:0.08em;text-transform:uppercase;margin-bottom:4px;}}
.conc-note{{font-size:11px;color:var(--muted);margin-top:4px;}}

/* Growth lists */
.growth-grid{{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:6px;}}
@media(max-width:600px){{.growth-grid{{grid-template-columns:1fr;}}}}
.growth-panel{{background:var(--s1);padding:16px 20px;}}
.growth-panel h4{{font-family:'DM Mono',monospace;font-size:10px;margin:0 0 10px;
color:{BRAND_ORANGE};letter-spacing:0.1em;text-transform:uppercase;}}
.growth-panel ul{{margin:0;padding:0;list-style:none;font-size:12.5px;}}
.growth-panel li{{padding:5px 0;border-top:1px dashed var(--brd);}}
.growth-panel li:first-child{{border-top:none;}}
.growth-panel li.empty{{color:var(--muted);font-style:italic;}}

/* Models roadmap */
.mdl-grid{{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;}}
@media(max-width:700px){{.mdl-grid{{grid-template-columns:1fr;}}}}
.mdl-card{{padding:12px 16px;font-size:12.5px;border-radius:2px;}}
.mdl-hdr{{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;}}
.mdl-name{{font-weight:700;color:{BRAND_BLACK};}}
.mdl-status{{font-family:'DM Mono',monospace;font-size:9px;font-weight:700;letter-spacing:0.1em;}}
.mdl-sub{{font-size:11px;color:var(--muted);margin-bottom:6px;}}
.mdl-detail{{font-size:11.5px;color:var(--ink);line-height:1.55;}}
table.mini{{width:100%;font-size:11px;border-collapse:collapse;margin-top:6px;}}
table.mini th{{background:{BRAND_BLACK};color:#fff;font-family:'DM Mono',monospace;
font-size:9px;font-weight:700;padding:4px 6px;text-align:left;letter-spacing:0.06em;}}
table.mini td{{padding:4px 6px;border-bottom:1px solid var(--brd);}}
table.mini td.num{{text-align:right;font-family:'DM Mono',monospace;}}

/* Tables */
table{{width:100%;border-collapse:collapse;font-size:11.5px;}}
table.paths td{{padding:6px 8px;border-bottom:1px solid #f3f4f6;vertical-align:middle;font-size:12px;}}
table.paths td.num{{text-align:right;font-family:'DM Mono',monospace;}}
table.paths td.muted{{color:var(--muted);}}
.agg-note{{font-size:10.5px;color:var(--muted);font-style:italic;font-weight:normal;}}
table.repro{{width:100%;border-collapse:collapse;font-size:10.5px;margin-top:6px;}}
table.repro td{{padding:4px 6px;border-bottom:1px dotted #e5e7eb;vertical-align:top;}}
table.repro td.repro-lbl{{color:var(--ink);}}
table.repro td.repro-num{{text-align:right;font-family:'DM Mono',monospace;font-variant-numeric:tabular-nums;color:var(--muted);}}
.path-dot{{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:8px;vertical-align:middle;}}
.bar{{background:var(--s2);height:6px;border-radius:1px;width:100%;min-width:120px;}}
.bar-fill{{height:6px;border-radius:1px;}}

/* Top 10 table (reuses weekly .top5 styles) */
table.top5 {{ table-layout:fixed; width:100%; margin-top:8px; font-size:11px; }}
table.top5 th {{ background:{BRAND_BLACK}; color:#fff; font-family:'DM Mono',monospace; font-size:9.5px; font-weight:700; padding:8px 6px; text-align:left; letter-spacing:0.08em; }}
table.top5 td {{ padding:8px 6px; border-bottom:1px solid #f3f4f6; vertical-align:top; word-wrap:break-word; overflow-wrap:break-word; word-break:normal; hyphens:none; font-size:11px; line-height:1.35; }}
table.top5 th:nth-child(1), table.top5 td:nth-child(1) {{ width:5%; }}
table.top5 th:nth-child(2), table.top5 td:nth-child(2) {{ width:11%; }}
table.top5 th:nth-child(3), table.top5 td:nth-child(3) {{ width:21%; }}
table.top5 th:nth-child(4), table.top5 td:nth-child(4) {{ width:17%; }}
table.top5 th:nth-child(5), table.top5 td:nth-child(5) {{ width:28%; }}
table.top5 th:nth-child(6), table.top5 td:nth-child(6) {{ width:18%; }}
table.top5 td.num.rank {{ font-family:'DM Mono',monospace; font-size:13px; font-weight:700; color:{BRAND_ORANGE}; white-space:nowrap; }}
table.top5 td.date {{ font-family:'DM Mono',monospace; font-size:11px; color:var(--muted); white-space:nowrap; }}
table.top5 td.pathogen {{ word-break:normal; }}
table.top5 td.pathogen .path-name {{ white-space:normal; }}
table.top5 td.company .company-name {{ font-weight:700; color:{BRAND_BLACK}; }}
table.top5 td.company .brand {{ font-size:11px; color:var(--muted); margin-top:2px; font-style:italic; }}
table.top5 td.src .country {{ font-weight:600; color:var(--ink); font-size:11.5px; }}
table.top5 td.src .agency {{ font-family:'DM Mono',monospace; font-size:10px; color:var(--muted); margin-top:2px; }}
table.top5 td.src .agency a {{ color:{BRAND_ORANGE}; text-decoration:none; }}
.rank-num{{font-family:Syne,sans-serif;font-weight:800;font-size:22px;color:{BRAND_ORANGE};
text-align:center;white-space:nowrap;font-variant-numeric:tabular-nums;letter-spacing:-0.02em;}}
.rank-num.rank-num--multi{{font-size:18px;}}
.date-cell{{font-family:'DM Mono',monospace;font-size:11px;color:var(--muted);white-space:nowrap;}}
.path-name{{font-weight:600;color:var(--ink);font-style:italic;}}
.co-cell strong{{color:{BRAND_BLACK};font-weight:700;display:block;}}
.brand-sub{{font-size:11px;color:var(--muted);margin-top:2px;font-style:italic;}}
.juris-country{{font-weight:600;color:var(--ink);}}
.src-sub{{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);margin-top:2px;}}
.src-link{{color:{BRAND_ORANGE};font-size:11px;text-decoration:none;font-family:'DM Mono',monospace;}}
.src-na{{font-family:'DM Mono',monospace;font-size:10px;color:#94a3b8;font-style:italic;}}
.chip-tier-1,.chip-tier-2,.chip-outbreak,.chip-tier1,.chip-tier2{{display:inline-block;color:#fff;font-size:9px;
font-weight:700;padding:2px 6px;border-radius:2px;letter-spacing:0.06em;margin-left:5px;white-space:nowrap;}}
.chip-tier-1,.chip-tier1{{background:{TIER1_RED};}}
.chip-tier-2,.chip-tier2{{background:{TIER2_AMBER};color:#1f2937;}}
.chip-outbreak{{background:{OUTBREAK_VIO};}}

.meth{{background:var(--s1);padding:20px 24px;font-size:13px;line-height:1.7;color:var(--body);}}
.meth p{{margin:0 0 10px;}} .meth p:last-child{{margin:0;}}
.meth strong{{color:{BRAND_BLACK};}}

.footer{{border-top:2px solid {BRAND_BLACK};margin-top:34px;padding-top:18px;
font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);line-height:1.7;}}
.footer .fb{{font-family:Syne,Georgia,serif;font-weight:800;font-size:12px;
color:{BRAND_BLACK};text-transform:uppercase;}}
.footer .fb em{{color:{BRAND_ORANGE};font-style:normal;}}

/* Appendix */
.appendix-head{{display:flex;align-items:center;gap:12px;margin:42px 0 12px;
page-break-before:always;}}
.appendix-num{{font-family:'DM Mono',monospace;font-size:10px;color:{BRAND_ORANGE};
font-weight:700;letter-spacing:0.12em;}}
.appendix-title{{font-family:Syne,Georgia,serif;font-weight:800;font-size:20px;
color:{BRAND_BLACK};letter-spacing:-0.01em;margin:0;}}
.appendix-count{{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);
letter-spacing:0.08em;text-transform:uppercase;}}

/* ─────────────────────────────────────────────────────────────────
   PRINT / PDF — mirror the mobile HTML aesthetic.
   WeasyPrint applies @media print for PDF rendering. Goal: a clean,
   readable single-column flow at A4 (matching what users see on a
   phone), not a cramped desktop dashboard squashed onto paper.
   ───────────────────────────────────────────────────────────── */
@media print {{
  /* Page setup — A4 portrait with comfortable side margins.
     The @bottom-center page-margin box renders a footer on every
     page with brand left, page number right. WeasyPrint reads the
     CSS `counter(page)` / `counter(pages)` pseudo-counters. */
  @page {{
    size: A4 portrait;
    margin: 14mm 12mm 18mm 12mm;
    @bottom-left {{
      content: "AFTS · Food Safety Intelligence System";
      font-family: 'Times New Roman', Times, Georgia, serif;
      font-size: 8pt;
      color: #6b7280;
      padding-bottom: 4mm;
    }}
    @bottom-right {{
      content: "Page " counter(page) " / " counter(pages);
      font-family: 'DM Mono', monospace;
      font-size: 8pt;
      color: #6b7280;
      padding-bottom: 4mm;
    }}
  }}

  body {{
    background: #fff;
    padding: 0;
    font-family: 'Times New Roman', Times, Georgia, serif;
    font-size: 10.5pt;
    line-height: 1.5;
    color: var(--ink);
  }}

  .page {{
    border: none;
    padding: 0;
    max-width: none;
    background: transparent;
  }}

  /* Headings — Times Roman, generous spacing */
  h1.r-title {{ font-size: 22pt; line-height: 1.2; margin: 0 0 8px; }}
  .sub {{ font-size: 8pt; }}
  .sec-title {{ font-size: 14pt; }}
  .sec-num {{ font-size: 9pt; }}

  /* KPI strip — 3-column grid (matches mobile) */
  .kpi-strip {{ grid-template-columns: repeat(3, 1fr); gap: 8px; }}
  .kpi {{ padding: 10px 10px; }}
  .kpi-value {{ font-size: 22pt; }}
  .kpi-lbl, .kpi-top {{ font-size: 8pt; }}

  /* Concentration / growth / models / trend — 1-column flow */
  .conc-grid, .growth-grid, .mdl-grid, .trend-grid {{
    grid-template-columns: 1fr !important;
    gap: 14px;
  }}
  .conc-val {{ font-size: 18pt; }}

  /* SVG charts — bigger so they're actually readable on paper.
     380px is roughly half an A4 page in landscape orientation,
     about right for a single-chart panel. */
  svg {{
    max-width: 100%;
    max-height: 380px;
    height: auto;
    display: block;
    margin: 0 auto;
  }}
  /* Heatmap allowed even more height for matrix legibility */
  .heat-panel svg {{ max-height: 480px; }}

  /* Chart panels — keep each chart panel together as one atomic
     block. Use the ACTUAL class names that exist in the rendered
     HTML (verified via `grep class= 2026-M04.html`):
       .trend-panel    — MoM trend, weekly cadence (each one a card)
       .heat-panel     — Country × Pathogen heatmap container
       .timeline-panel — Outbreak timeline container
       .growth-panel   — Emerging / declining lists
       .conc-card      — Source HHI / Geographic Gini / Tier-1 Intensity
       .analysis       — Intelligence Analysis narrative card
       .pa-note        — Process Authority note block
       .meth           — Methodology card
       table.paths     — Hazard distribution table (16 rows)
       table.repro     — HHI/Gini reproducibility tables (12 rows each)
     Each one needs `page-break-inside: avoid` so it doesn't split. */
  .trend-panel,
  .heat-panel,
  .timeline-panel,
  .growth-panel,
  .conc-card,
  .analysis,
  .pa-note,
  .meth,
  table.paths,
  table.repro {{
    page-break-inside: avoid;
    break-inside: avoid;
  }}

  /* Section headers must stay with the content that follows.
     Without this, a §04 header lands at the bottom of page 4
     and its heatmap pushes to page 5 — exactly the bug from
     the user's page 3→4 screenshot. */
  .sec-head {{
    page-break-after: avoid;
    break-after: avoid;
    page-break-inside: avoid;
    break-inside: avoid;
  }}
  .sec-caption {{
    page-break-before: avoid;
    break-before: avoid;
  }}

  /* Top-10 + Appendix tables — smaller font, tighter padding,
     keep rows together */
  table.top5 {{ font-size: 8.5pt; }}
  table.top5 th {{ font-size: 7pt; padding: 6px 5px; }}
  table.top5 td {{ font-size: 8.5pt; padding: 6px 5px; line-height: 1.3; }}
  table.top5 tr {{ page-break-inside: avoid; break-inside: avoid; }}

  table.paths {{ font-size: 9pt; }}
  table.paths td {{ padding: 5px 7px; }}
  table.paths tr {{ page-break-inside: avoid; break-inside: avoid; }}

  /* Reproducibility tables (§09a) */
  table.repro {{ font-size: 8.5pt; }}
  table.repro td {{ padding: 3px 6px; }}
  table.repro tr {{ page-break-inside: avoid; break-inside: avoid; }}

  /* Methodology + narrative cards */
  .meth, .analysis {{ font-size: 10pt; line-height: 1.55; padding: 14px 16px; }}
  .meth p, .analysis p {{ margin: 0 0 10px; }}

  /* KPI strip stays together */
  .kpi-strip {{ page-break-inside: avoid; break-inside: avoid; }}

  /* Appendix A always starts a new page */
  .appendix-head {{
    page-break-before: always; break-before: page;
    page-break-after: avoid; break-after: avoid;
    page-break-inside: avoid; break-inside: avoid;
  }}

  /* Hide the body-level "Generated …" footer — page-numbers come
     from the @page @bottom-* margin boxes now */
  .footer {{ display: none; }}
}}

/* ─────────────────────────────────────────────────────────────────
   MOBILE — phones at ≤640px and tablets ≤900px.
   Strategy: shrink page padding, stack KPI/concentration grids
   into 1 column, scale typography, and wrap the wide Top-10 /
   Appendix tables in a horizontally-scrollable container so
   columns stay readable rather than collapsing.
   ───────────────────────────────────────────────────────────── */
@media (max-width:900px) {{
  body {{ padding:14px 10px 40px; font-size:13.5px; }}
  .page {{ padding:22px 18px; max-width:100%; }}
  h1.r-title {{ font-size:24px; }}
  .sub {{ font-size:9.5px; line-height:1.5; }}

  /* KPI strip: 6 cols → 2 cols on tablet, 1 col on phone */
  .kpi-strip {{ grid-template-columns:repeat(2,1fr); }}
  .kpi {{ padding:14px 12px; }}
  .kpi-value {{ font-size:24px; }}

  /* Concentration / growth / models stack to 1 column */
  .conc-grid, .growth-grid, .mdl-grid, .trend-grid {{ grid-template-columns:1fr !important; }}
  .conc-val {{ font-size:20px; }}

  /* Top-10 + Appendix tables get horizontal scroll on narrow
     viewports. table-layout:fixed with a min-width preserves the
     6-column structure; the wrapper provides the swipe surface. */
  table.top5 {{ display:block; overflow-x:auto; -webkit-overflow-scrolling:touch; min-width:0; }}
  table.top5 thead, table.top5 tbody {{ display:table; width:auto; min-width:680px; }}
  table.top5 thead {{ width:680px; }}
  table.top5 tbody {{ width:680px; }}
  table.top5 td {{ font-size:11.5px; padding:8px 6px; }}
  table.top5 td.date {{ font-size:10.5px; }}

  /* SVG charts scale with container */
  svg {{ max-width:100%; height:auto; }}
  .heat-panel, .timeline-panel {{ overflow-x:auto; -webkit-overflow-scrolling:touch; }}

  /* Appendix table — same scroll treatment */
  table.paths {{ font-size:12px; }}
  table.paths td {{ padding:6px 8px; }}

  /* Methodology card — tighter padding */
  .meth {{ padding:16px 18px; font-size:12.5px; }}

  /* Section headings */
  .sec-title {{ font-size:18px; }}
  .sec-num {{ font-size:11px; }}
}}

@media (max-width:640px) {{
  .kpi-strip {{ grid-template-columns:1fr; }}
  .mast {{ flex-direction:column; align-items:flex-start; gap:10px; }}
  h1.r-title {{ font-size:21px; line-height:1.2; }}
  .page {{ padding:18px 14px; }}
}}
</style></head><body><div class="page">

<div class="mast">
  <div>
    <div class="brand">Advanced Food-Tech Solutions <em>·</em> AFTS</div>
    <div class="tagline">Food Safety Intelligence System · Monthly Briefing</div>
    <div class="pubdate"><strong>{published_label}</strong> &middot; {pub_date}</div>
  </div>
  <div class="pill">{escape(month_name)} {year}</div>
</div>

<h1 class="r-title">Food Safety Hazard &amp; Pathogen Surveillance <span class="accent">·</span> {escape(month_name)} {year}</h1>
<div class="sub">{month_start.strftime('%d %b %Y')} – {month_end.strftime('%d %b %Y')}
 &middot; {stats['total']} recall incidents across {co.get('n_countries', len(stats.get('country_counts', [])))} jurisdictions
 &middot; {co.get('n_sources', len(stats.get('source_counts', [])))} regulatory sources</div>

<div class="kpi-strip">
  <div class="kpi">
    <div class="kpi-label">Total Recalls</div>
    <div class="kpi-value">{stats['total']}</div>
    <div class="kpi-top">{delta_phrase}</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Tier-1 Critical</div>
    <div class="kpi-value red">{stats['tier1']}</div>
    <div class="kpi-top">{round(stats['tier1']/total_safe*100)}% of total</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Outbreaks</div>
    <div class="kpi-value vio">{stats['outbreaks']}</div>
    <div class="kpi-top">{_count_phrase(cl.get('cluster_count',0), 'cluster')} flagged</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Leading Pathogen</div>
    <div class="kpi-value orange">{escape(top_pathogen_name)}</div>
    <div class="kpi-top">{stats['top_pathogen'][1]} recall incidents &middot; {top_pathogen_pct}%</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">MoM Change</div>
    <div class="kpi-value mom" style="color:{mom_colour}">{mom_delta_label}</div>
    <div class="kpi-top">{z_label}</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Severity Index</div>
    <div class="kpi-value">{sv.get('score','–')}</div>
    <div class="kpi-top">{escape((sv.get('bucket') or '').upper())}</div>
  </div>
</div>

<!-- § 01 Intelligence Analysis -->
<div class="sec-head"><span class="sec-num">§ 01</span><h2 class="sec-title">Intelligence Analysis</h2><span class="sec-rule"></span></div>
<div class="analysis">{analysis_html}</div>

<!-- § 02 MoM Trend -->
<div class="sec-head"><span class="sec-num">§ 02</span><h2 class="sec-title">Month-over-Month Trend</h2><span class="sec-rule"></span></div>
<div class="trend-grid">
  <div class="trend-panel">
    <div class="trend-row">
      <div>
        <div class="trend-num {mom.get('direction','')}">{mom.get('delta_pct', '—') if mom.get('delta_pct') is not None else '—'}%</div>
        <div class="trend-lbl">vs prior month</div>
      </div>
      <div style="text-align:right">
        <div style="font-family:'DM Mono',monospace;font-size:12px;font-weight:700;color:var(--muted)">{z_label}</div>
        <div class="trend-lbl">{"Anomalous (|Z|&gt;2)" if mom.get('anomaly_flag') else "Within baseline"}</div>
      </div>
    </div>
    {svg_mom_sparkline(mom)}
    <div style="font-size:12px;color:var(--muted);margin-top:8px">
      Rolling {mom.get('baseline_n', 3)}-month mean: <strong>{mom.get('rolling_mean', '—')}</strong>
      &nbsp;·&nbsp; σ: <strong>{mom.get('rolling_std', '—')}</strong>
      {f'&nbsp;·&nbsp; <span style="color:var(--orange)">baseline too narrow for Z (n &lt; {_MIN_BASELINE_MONTHS_FOR_Z})</span>' if mom.get('baseline_too_narrow') else ''}
    </div>
  </div>
  <div class="trend-panel">
    <div class="trend-lbl" style="margin-bottom:4px">Weekly cadence (recalls per ISO week)</div>
    {svg_weekly_cadence(cad)}
    <div style="font-size:11px;color:var(--muted);margin-top:6px">
      Peak week: <strong>{escape(cad.get('peak_wk') or '—')}</strong>
      &nbsp;·&nbsp; σ: <strong>{cad.get('std', '—')}</strong>
    </div>
  </div>
</div>

<!-- § 03 Hazard & Pathogen Distribution -->
<div class="sec-head"><span class="sec-num">§ 03</span><h2 class="sec-title">Hazard &amp; Pathogen Distribution</h2><span class="sec-rule"></span></div>
<p class="sec-caption">Distribution covers all reported food-safety hazards in the window — biological pathogens (Listeria, Salmonella, STEC, Campylobacter, etc.), biological toxins (marine biotoxins, histamine, cereulide), mycotoxins (aflatoxin, ochratoxin A, alternaria), heavy metals, residues, and physical/foreign-body contamination. Rows are ranked by share of total recall incidents.</p>
<table class="paths"><tbody>{path_rows_html}</tbody></table>

<!-- § 04 Country × Pathogen Hotspot Matrix -->
<div class="sec-head"><span class="sec-num">§ 04</span><h2 class="sec-title">Country × Pathogen Hotspot Matrix</h2><span class="sec-rule"></span></div>
<p class="sec-caption">Cells show observed recall counts. σ values are standardised residuals vs an independence-baseline expected count; cells with σ&gt;2 are statistically over-represented and are bordered in red. <strong>Caveat:</strong> hotspot flags are screening signals only — many cells have small expected counts, multiple country×hazard cells are tested simultaneously, and no multiple-comparison correction is applied. Treat any flag as a prompt for editorial review, not as a confirmatory epidemiological finding; for formal inference an exact / Monte-Carlo test or a Bonferroni / FDR adjustment is recommended.</p>
<div class="heat-panel">
  {svg_hotspot_heatmap(hs)}
  <div style="margin-top:16px;font-family:'DM Mono',monospace;font-size:10px;
  color:{BRAND_ORANGE};letter-spacing:0.1em;text-transform:uppercase;font-weight:700;">Hotspot alerts</div>
  <ul class="hotspot-list">{hotspot_items}</ul>
</div>

<!-- § 05 Outbreak Timeline -->
<div class="sec-head"><span class="sec-num">§ 05</span><h2 class="sec-title">Outbreak Timeline &amp; Cluster Analysis</h2><span class="sec-rule"></span></div>
<p class="sec-caption">One marker per outbreak event; hover/print for detail. Clusters are ≥3 same-pathogen outbreaks within a 14-day window.</p>
<div class="timeline-panel">
  {svg_outbreak_timeline(cl, month_start, month_end)}
  <div style="margin-top:16px;font-family:'DM Mono',monospace;font-size:10px;
  color:{BRAND_ORANGE};letter-spacing:0.1em;text-transform:uppercase;font-weight:700;">
    Detected clusters ({cl.get('cluster_count', 0)})</div>
  <ul class="hotspot-list">{cluster_html}</ul>
</div>

<!-- § 06 Regulatory Intensity + Growth -->
<div class="sec-head"><span class="sec-num">§ 06</span><h2 class="sec-title">Regulatory Intensity &amp; Concentration</h2><span class="sec-rule"></span></div>
<div class="conc-grid">
  <div class="conc-card {escape(hhi_bucket)}">
    <div class="conc-lbl">Source HHI</div>
    <div class="conc-val">{co.get('hhi_source','—')}</div>
    <div class="conc-note">{co.get('n_sources','–')} sources · <strong>{escape(hhi_bucket)}</strong> (&lt;1500 diverse · 1500–2500 moderate · &gt;2500 concentrated)</div>
  </div>
  <div class="conc-card {escape(gini_bucket)}">
    <div class="conc-lbl">Geographic Gini</div>
    <div class="conc-val">{co.get('gini_country','—')}</div>
    <div class="conc-note">{co.get('n_countries','–')} countries · <strong>{escape(gini_bucket)}</strong> (&lt;0.4 even · 0.4–0.6 moderate · &gt;0.6 uneven)</div>
  </div>
  <div class="conc-card">
    <div class="conc-lbl">Tier-1 Intensity</div>
    <div class="conc-val">{intensity_txt}</div>
    <div class="conc-note">This month: {round((co.get('tier1_share') or 0)*100)}% · baseline: {round((co.get('baseline_tier1_share') or 0)*100) if co.get('baseline_tier1_share') else '–'}%</div>
  </div>
</div>
<div class="growth-grid">
  <div class="growth-panel">
    <h4>↑ Emerging pathogens (Z &gt; 2)</h4>
    <ul>{emerging_html}</ul>
  </div>
  <div class="growth-panel">
    <h4>↓ Declining pathogens (Z &lt; −2)</h4>
    <ul>{declining_html}</ul>
  </div>
</div>

<!-- § 07 Severity + Predictive -->
<div class="sec-head"><span class="sec-num">§ 07</span><h2 class="sec-title">Predictive Outlook</h2><span class="sec-rule"></span></div>
<div style="display:flex;gap:24px;align-items:flex-start;flex-wrap:wrap;margin-bottom:20px">
  <div style="flex:0 0 auto;background:var(--s1);padding:16px 20px;text-align:center;">
    {svg_severity_gauge(sv)}
    <div style="font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);margin-top:4px;letter-spacing:0.1em;text-transform:uppercase">Composite Severity · 0–100</div>
  </div>
  <div style="flex:1;min-width:280px;font-size:12.5px;color:var(--body);line-height:1.7;">
    <strong>How to read this panel.</strong> Each card shows a predictive model
    AFTS runs on the monthly series. Active cards publish a forecast with its
    confidence envelope; dormant cards show the data threshold required to
    activate — an honest roadmap as the dataset grows. When <em>n</em> reaches
    12 months, STL decomposition unlocks; 24 months unlocks Holt-Winters,
    SARIMA, and Prophet. All models run on the same Recalls sheet you already
    subscribe to — no secondary data required.
  </div>
</div>
<div class="mdl-grid">{models_panel_html}</div>

<!-- § 08 Top 10 -->
<div class="sec-head">
  <span class="sec-num">§ 08</span>
  <h2 class="sec-title">Top 10 Critical Incidents</h2>
  <span class="sec-rule"></span>
  <a class="sec-link" href="{year_m}-all.html" target="_blank">View all {stats['total']} &rarr;</a>
</div>
<p class="sec-caption">Ranked by pathogen severity, outbreak status, and tier. The full {stats['total']}-recall list is available in the companion page linked above.</p>
<table class="top5"><thead><tr>
<th>#</th><th>Date</th><th>Pathogen</th><th>Company / Brand</th>
<th>Product</th><th>Jurisdiction &amp; Source</th>
</tr></thead><tbody>{top_rows_html}</tbody></table>

<!-- § 09 Methodology -->
<div class="sec-head"><span class="sec-num">§ 09</span><h2 class="sec-title">Methodology &amp; Sources</h2><span class="sec-rule"></span></div>
<div class="meth">
  <p><strong>Definitions.</strong> A <em>recall incident</em> is a single regulator-published recall fiche / notice covering one identified product or product family. Where a single multi-country event is reported by two regulators (for example FSAI in Ireland plus FSA in the United Kingdom), each regulator's notice counts as one incident — the incident table accordingly shows the multi-jurisdictional footprint rather than de-duplicating to a single row. <em>Tier-1</em> indicates an immediate public-health risk (Listeria, Salmonella, STEC, Campylobacter, Cronobacter, Vibrio, Hepatitis A, marine biotoxins, mycotoxins above regulatory limits, undeclared major allergens). <em>Outbreak-associated</em> means the regulator's notice cites confirmed human cases or epidemiologically-linked illnesses, not merely a positive product test.</p>

  <p><strong>Process authority.</strong> Analytical frameworks, severity rubrics, hazard classification, and the engineering interpretation of each recall are developed by the AFTS process-authority practice, drawing on in-house expertise in food process engineering, thermal processing, and regulatory compliance. Every view is grounded in validated process engineering: thermal processing (21 CFR 113/114), pasteurisation (PMO), aseptic and UHT, hold-tube and F-value lethality, and HACCP.</p>

  <p><strong>Statistical methods.</strong> Month-over-month <em>Z-scores</em> use the rolling-prior-months mean and sample standard deviation; the score is suppressed (n/a) when the baseline contains fewer than six months. The <em>hotspot matrix</em> uses standardised chi-square residuals against an independence-baseline expected count; cells with σ&gt;2 are flagged as over-represented but are screening signals only (no multiple-comparison correction; small expected counts in many cells). <em>Source concentration</em> uses the Herfindahl-Hirschman Index on agency counts (HHI = Σ s²ᵢ × 10000, where sᵢ is each agency's share; &lt;1500 diverse, 1500–2500 moderate, &gt;2500 concentrated). <em>Geographic distribution</em> uses the Gini coefficient on country counts (0 = perfectly even, 1 = single-country regime; &lt;0.4 even, 0.4–0.6 moderate, &gt;0.6 uneven). <em>Outbreak clusters</em> are detected via a sliding 14-day window over same-pathogen outbreak events (cluster threshold: ≥3 events). The <em>composite severity index</em> (0–100) is a transparent two-component blend: <strong>100 × (0.60 × Tier-1 share + 0.40 × outbreak rate)</strong>, where Tier-1 share = Tier-1 incidents ÷ total incidents and outbreak rate = outbreak-flagged incidents ÷ total incidents (using the same criterion as the headline outbreaks KPI). For April 2026 this evaluates to 100 × (0.60 × 198/236 + 0.40 × 6/236) = 51.4. Buckets: ≥65 critical, ≥45 elevated, ≥25 moderate, &lt;25 low. Predictive models are gated to activate only when data history meets the minimum size required for valid estimation; the linear-trend OLS reports a 95% CI but does not claim slope significance until n ≥ 12 monthly observations (so dof ≥ 10, where the t-critical at α = 0.05 falls below 2.23).</p>

  <p><strong>Reporting-system caveats.</strong> National recall-publication regimes differ in granularity. France's RappelConso publishes one fiche per identified product/lot configuration, often producing several fiches per single root-cause event; the headline French count therefore reflects a transparent, item-level reporting practice rather than a higher true incidence. EU member-state counts likewise reflect both the actual hazard signal and each authority's publication discipline. Treat country totals as a recall-publication signal, not as a direct food-safety league table.</p>

  <p><strong>Data &amp; AI pipeline.</strong> The system aggregates regulatory recall notices from 70+ countries and 15+ agencies (FDA, USDA FSIS, RASFF, FSA, FSANZ, CFIA, RappelConso, BVL, AESAN, EFET and national authorities) into the accumulative Recalls sheet. AI narrative is produced against AFTS process-authority prompts and edited for publication. Figures, hazard names, and source URLs are preserved verbatim from regulator data.</p>
</div>

<!-- §09a — HHI / Gini reproducibility table -->
<div class="sec-head"><span class="sec-num">§ 09a</span><h2 class="sec-title">Concentration Reproducibility</h2><span class="sec-rule"></span></div>
<p class="sec-caption">Per review-1 audit guidance — the source-count table reproduces Source HHI = {co.get('hhi_source')} on agency shares, and the country-count table reproduces Geographic Gini = {co.get('gini_country')} on country counts. Top 12 of each are shown; the residual rows behave identically and contribute proportionally smaller s².</p>
<div class="conc-grid">
  <div class="conc-card">
    <div class="conc-lbl">Source / Agency &middot; share &middot; HHI s²ᵢ contribution</div>
    <table class="repro"><tbody>
      {hhi_rows_html}
    </tbody></table>
  </div>
  <div class="conc-card">
    <div class="conc-lbl">Country &middot; count &middot; share</div>
    <table class="repro"><tbody>
      {gini_rows_html}
    </tbody></table>
  </div>
</div>

<!-- Appendix A — Complete Recall Register -->
<div class="appendix-head">
  <span class="appendix-num">APPENDIX A</span>
  <h2 class="appendix-title">Complete Recall Register</h2>
  <span class="sec-rule"></span>
  <span class="appendix-count">{stats['total']} recalls</span>
</div>
<p class="sec-caption">Every food-safety hazard recall recorded in {escape(month_name)} {year}, ranked by severity. This appendix replaces the companion page for subscriber convenience.</p>
<table class="top5"><thead><tr>
<th>#</th><th>Date</th><th>Pathogen</th><th>Company / Brand</th>
<th>Product</th><th>Jurisdiction &amp; Source</th>
</tr></thead><tbody>{all_rows_html}</tbody></table>

<div class="footer">
  <div class="fb">Advanced Food-Tech Solutions <em>·</em> AFTS</div>
  Food Process Engineering · Thermal Processing · Regulatory Compliance<br>
  advfood.tech · info@advfood.tech · Athens, Greece<br>
  Generated {(month_end + timedelta(days=1)).strftime('%d %B %Y')}
</div>

</div></body></html>"""


# ---------------------------------------------------------------------------
# Email template — short, mirrors weekly mailer contract
# ---------------------------------------------------------------------------
def build_monthly_email_html(stats: Dict[str, Any], signals: Dict[str, Any],
                             models: Dict[str, Any],
                             month_start: date, month_end: date,
                             report_url: str) -> str:
    month_name = month_start.strftime("%B %Y")
    mom = signals["mom_trend"]
    sv  = signals["severity"]
    lt  = models.get("linear_trend", {})
    hs  = signals["hotspot"]
    top_hotspot_line = ""
    if hs.get("hotspots"):
        h = hs["hotspots"][0]
        top_hotspot_line = (
            f'<div style="font-size:13px;margin-top:8px">'
            f'<strong>Top hotspot:</strong> {escape(h["country"])} × '
            f'<em>{escape(h["pathogen"])}</em> ({h["observed"]} recalls, '
            f'{h["ratio"]}× expected).</div>'
        )
    forecast_line = ""
    if lt.get("status") == "active":
        forecast_line = (
            f'<div style="font-size:13px;margin-top:8px">'
            f'<strong>Next-month forecast:</strong> {lt.get("next_month_point")} '
            f'recalls (95% CI {lt.get("next_month_ci95")}).</div>'
        )

    return f"""<!DOCTYPE html><html><body style="margin:0;padding:0;background:#f5f5f7;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#1f2937">
<div style="max-width:600px;margin:20px auto;background:#fff;border:1px solid #e5e7eb">

<div style="background:{BRAND_BLACK};color:#fff;padding:20px 24px">
<div style="font-family:Georgia,serif;font-weight:800;font-size:16px;text-transform:uppercase;letter-spacing:-0.01em">
Advanced Food-Tech Solutions <span style="color:{BRAND_ORANGE}">·</span> AFTS</div>
<div style="font-family:monospace;font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.14em;margin-top:3px">
Monthly Briefing · {escape(month_name)}</div>
</div>

<div style="padding:28px 24px 16px">
<h1 style="font-family:Georgia,serif;font-weight:800;font-size:24px;margin:0 0 8px;color:{BRAND_BLACK}">Hazard &amp; Pathogen Surveillance · {escape(month_name)}</h1>
<div style="font-family:monospace;font-size:10px;color:#6b7280;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:18px">
{month_start.strftime('%d %b')} – {month_end.strftime('%d %b %Y')} · {stats['total']} recalls · {stats['tier1']} Tier-1 · {stats['outbreaks']} outbreaks
</div>

<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;margin-bottom:20px">
<tr>
<td style="width:25%;padding:14px 12px;background:#f9fafb;border:1px solid #e5e7eb;text-align:center">
<div style="font-family:Georgia,serif;font-weight:800;font-size:26px;color:{BRAND_BLACK}">{stats['total']}</div>
<div style="font-family:monospace;font-size:9px;color:#6b7280;letter-spacing:0.08em;text-transform:uppercase;margin-top:4px">Total</div>
</td>
<td style="width:25%;padding:14px 12px;background:#f9fafb;border:1px solid #e5e7eb;text-align:center">
<div style="font-family:Georgia,serif;font-weight:800;font-size:26px;color:{TIER1_RED}">{stats['tier1']}</div>
<div style="font-family:monospace;font-size:9px;color:#6b7280;letter-spacing:0.08em;text-transform:uppercase;margin-top:4px">Tier-1</div>
</td>
<td style="width:25%;padding:14px 12px;background:#f9fafb;border:1px solid #e5e7eb;text-align:center">
<div style="font-family:Georgia,serif;font-weight:800;font-size:26px;color:{OUTBREAK_VIO}">{stats['outbreaks']}</div>
<div style="font-family:monospace;font-size:9px;color:#6b7280;letter-spacing:0.08em;text-transform:uppercase;margin-top:4px">Outbreaks</div>
</td>
<td style="width:25%;padding:14px 12px;background:#f9fafb;border:1px solid #e5e7eb;text-align:center">
<div style="font-family:Georgia,serif;font-weight:800;font-size:18px;color:{BRAND_ORANGE};font-style:italic">{escape(str(stats['top_pathogen'][0]))}</div>
<div style="font-family:monospace;font-size:9px;color:#6b7280;letter-spacing:0.08em;text-transform:uppercase;margin-top:4px">Leading</div>
</td>
</tr>
</table>

<div style="background:#f9fafb;border-left:3px solid {BRAND_ORANGE};padding:14px 18px;margin-bottom:20px">
<div style="font-family:monospace;font-size:10px;color:#6b7280;letter-spacing:0.1em;text-transform:uppercase;font-weight:700;margin-bottom:6px">Month signal</div>
<div style="font-size:14px;line-height:1.55"><strong>{mom.get('delta_pct','—')}%</strong> vs prior month
{f"· Z = {mom.get('z_score'):+.1f} (anomalous)" if mom.get('anomaly_flag') else ""}
· severity index <strong>{sv.get('score','–')}/100</strong> ({escape(sv.get('bucket','—'))}).</div>
{top_hotspot_line}{forecast_line}
</div>

<div style="text-align:center;margin:28px 0 12px">
<a href="{escape(report_url)}" target="_blank" style="display:inline-block;background:{BRAND_BLACK};color:#fff;padding:14px 28px;text-decoration:none;font-family:monospace;font-size:12px;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;border-radius:3px">View Full Monthly Report →</a>
</div>

<div style="font-size:11px;color:#94a3b8;text-align:center;margin-top:20px">
Advanced Food-Tech Solutions · advfood.tech · info@advfood.tech
</div>
</div>
</div>
</body></html>"""


# ---------------------------------------------------------------------------
# JSON summary for the Apps Script mailer
# ---------------------------------------------------------------------------
def write_monthly_summary_json(month_start: date, month_end: date,
                               stats: Dict[str, Any], signals: Dict[str, Any],
                               models: Dict[str, Any],
                               narrative: str,
                               month_recalls: List[Dict],
                               site_base_url: str, dashboard_url: str,
                               out_path: Path) -> None:
    month_name = month_start.strftime("%B")
    year = month_start.year
    year_m = f"{year}-M{month_start.month:02d}"

    top10_out = []
    for i, r in enumerate(weekly.rank_top_recalls(month_recalls, 10), 1):
        _, canon = weekly.severity_score(r.get("Pathogen") or "")
        url = (r.get("URL") or "").strip()
        top10_out.append({
            "rank":     i,
            "date":     str(r.get("Date", ""))[:10],
            "pathogen": canon,
            "pathogen_raw": r.get("Pathogen", ""),
            "company":  r.get("Company", ""),
            "brand":    r.get("Brand", ""),
            "product":  r.get("Product", ""),
            "country":  r.get("Country", ""),
            "source":   r.get("Source", ""),
            "tier":     weekly.safe_int(r.get("Tier"), 3),
            "outbreak": weekly.safe_int(r.get("Outbreak"), 0),
            "url":      url,
            "url_ok":   weekly.is_report_grade_url(url),
        })

    report_url = f"{site_base_url}/{year_m}.html"
    email_html = build_monthly_email_html(stats, signals, models,
                                          month_start, month_end, report_url)

    # Build the nested objects the Apps Script mailer expects (matches the
    # exact shape that weekly-summary-latest.json uses, so buildSubject_,
    # buildEmailHtml_, buildEmailText_ all read s.stats.total / s.stats.tier1
    # / s.stats.outbreaks and s.leading_pathogen.name without crashing).
    tp = stats.get("top_pathogen")
    if tp and len(tp) >= 2:
        leading_pathogen_obj = {
            "name": tp[0],
            "cases": tp[1],
            "pct":  round(tp[1] / max(stats["total"], 1) * 100),
        }
    else:
        leading_pathogen_obj = {"name": "Mixed", "cases": 0, "pct": 0}

    stats_obj = {
        "total":     stats["total"],
        "tier1":     stats["tier1"],
        "outbreaks": stats["outbreaks"],
        "delta":     stats.get("delta", 0),
        "delta_pct": stats.get("delta_pct", 0),
    }

    # top_threats — exactly the shape the mailer's threatRows mapper reads.
    # The mailer iterates s.top_threats (slice(0,5)) and reads .rank, .date,
    # .country, .pathogen, .company, .brand, .product, .url, .tier, .outbreak.
    # We have all of that already in top10_out, just renamed.
    top_threats_obj = [{
        "rank":     t["rank"],
        "date":     t["date"],
        "country":  t["country"],
        "pathogen": t["pathogen"],
        "company":  t["company"],
        "brand":    t["brand"],
        "product":  t["product"],
        "url":      t["url"],
        "tier":     t["tier"],
        "outbreak": t["outbreak"],
    } for t in top10_out[:5]]

    payload = {
        "month":            year_m,
        "month_tag":        year_m,           # mailer uses this to locate docs/<month_tag>.pdf
        "month_name":       month_name,
        "year":             year,
        "month_num":        month_start.month,
        "window_start":     month_start.isoformat(),
        "window_end":       month_end.isoformat(),
        "month_end":        month_end.isoformat(),  # mailer fallback for monthTag
        # Nested objects (mailer reads these — required to NOT crash)
        "stats":            stats_obj,
        "leading_pathogen": leading_pathogen_obj,
        "top_threats":      top_threats_obj,
        # Flat fields kept for backward compatibility with anything else
        # that may have been reading the flat shape (dashboard, other scripts)
        "total":            stats["total"],
        "tier1":            stats["tier1"],
        "outbreaks":        stats["outbreaks"],
        "delta":            stats.get("delta"),
        "delta_pct":        stats.get("delta_pct"),
        "top_pathogen":     list(stats["top_pathogen"]),
        "top_countries":    stats["country_counts"][:5],
        "top_sources":      stats["source_counts"][:5],
        "mom_trend":        signals.get("mom_trend"),
        "hotspots":         signals.get("hotspot", {}).get("hotspots", [])[:5],
        "clusters":         signals.get("cluster", {}).get("clusters", [])[:3],
        "concentration":    signals.get("concentration"),
        "severity":         signals.get("severity"),
        "emerging":         signals.get("growth", {}).get("emerging", [])[:5],
        "linear_trend":     models.get("linear_trend"),
        "narrative":        narrative,
        "report_url":       report_url,
        "all_month_url":    f"{site_base_url}/{year_m}-all.html",
        "dashboard_url":    dashboard_url,
        "top10":            top10_out,
        "email_html":       email_html,
        # Both keys for compatibility — weekly uses generated_utc, monthly
        # was written with generated_at; emit both so the mailer's stale
        # guard (which checks generated_utc) works in either codebase.
        "generated_utc":    datetime.now(timezone.utc).isoformat(),
        "generated_at":     datetime.now(timezone.utc).isoformat(),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Monthly summary JSON written: %s", out_path)


def update_monthly_index_json(month_start: date, month_end: date,
                              stats: Dict[str, Any],
                              index_path: Path) -> None:
    """Append-or-update this month's entry in docs/data/monthly-index.json.

    This is the file the dashboard's loadMonthlyReports() fetches. It holds
    a lean array of card metadata (NOT the full report payload) so the
    dashboard loads in one request no matter how many months have shipped.

    Idempotent: if the current month already has an entry (e.g. a re-run),
    we update in-place instead of appending a duplicate. Entries stay
    sorted newest-first by month_end.
    """
    year_m = f"{month_start.year}-M{month_start.month:02d}"
    month_name = month_start.strftime("%B %Y")      # e.g. "March 2026"

    top_pathogen = (
        stats["top_pathogen"][0]
        if stats.get("top_pathogen") and stats["top_pathogen"][0]
        else "—"
    )
    entry = {
        "filename":     f"{year_m}.html",
        "year":         month_start.year,
        "month_num":    month_start.month,
        "month_name":   month_name,
        "month_start":  month_start.isoformat(),
        "month_end":    month_end.isoformat(),
        "total":        stats["total"],
        "tier1":        stats["tier1"],
        "outbreaks":    stats["outbreaks"],
        "top_pathogen": top_pathogen,
        "summary": (
            f"{month_name}: {stats['total']} recalls, {stats['tier1']} Tier-1, "
            f"{stats['outbreaks']} outbreak(s). Leading pathogen: {top_pathogen}."
        ),
    }

    # Load existing index (if any). Accept either a bare array or a
    # {"reports":[...]} wrapper — matches the dashboard's tolerant parser.
    entries: List[Dict[str, Any]] = []
    if index_path.exists():
        try:
            raw = json.loads(index_path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                entries = raw
            elif isinstance(raw, dict) and isinstance(raw.get("reports"), list):
                entries = raw["reports"]
        except (json.JSONDecodeError, OSError) as e:
            log.warning("monthly-index.json unreadable, starting fresh: %s", e)

    # Drop any pre-existing entry for this month, then insert the new one.
    # Filename is the unique key (matches year_m exactly).
    entries = [e for e in entries if e.get("filename") != entry["filename"]]
    entries.append(entry)

    # Sort newest-first by month_end so the dashboard can slice() from the
    # top without re-sorting. String comparison works because ISO dates sort
    # lexicographically.
    entries.sort(key=lambda e: e.get("month_end", ""), reverse=True)

    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(
        json.dumps(entries, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("Monthly index JSON updated: %s (%d entries)", index_path, len(entries))


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="AFTS monthly intelligence briefing")
    ap.add_argument("--month-end", required=True,
                    help="Last day of the month to report on (YYYY-MM-DD)")
    ap.add_argument("--xlsx", default=str(ROOT / "data" / "recalls.xlsx"))
    ap.add_argument("--output", default=None)
    ap.add_argument("--all-output", default=None,
                    help="Path for the companion all-recalls HTML")
    ap.add_argument("--site-url",
                    default="https://gstoforos.github.io/Food-Safety-Intelligence-System")
    ap.add_argument("--dashboard-url",
                    default="https://www.advfood.tech/fsis-recalls")
    ap.add_argument("--summary-json",
                    default=str(ROOT / "data" / "monthly-summary-latest.json"))
    ap.add_argument("--monthly-index",
                    default=str(ROOT / "data" / "monthly-index.json"),
                    help="Running index of all monthly reports — the dashboard's "
                         "loadMonthlyReports() fetches this on first render.")
    args = ap.parse_args()

    try:
        month_end = datetime.strptime(args.month_end, "%Y-%m-%d").date()
    except ValueError:
        log.error("Invalid --month-end: %s", args.month_end); return 2

    month_start, month_end_full = month_bounds(month_end.year, month_end.month)
    log.info("AFTS monthly report | %s %d (%s – %s)",
             month_start.strftime("%B"), month_start.year,
             month_start.isoformat(), month_end_full.isoformat())

    all_recalls = weekly.load_recalls(Path(args.xlsx))
    if not all_recalls:
        log.error("No recalls loaded"); return 3

    month_recalls = filter_month(all_recalls, month_start, month_end_full)
    log.info("This month: %d recalls", len(month_recalls))

    # Build multi-month cohort ordered oldest → newest, current month LAST
    all_bucket = bucket_by_month(all_recalls)
    available_months = sorted(k for k in all_bucket if k <= month_start.strftime("%Y-%m"))
    cohorts = [(ym, all_bucket[ym]) for ym in available_months]
    prior_cohorts = cohorts[:-1] if cohorts and cohorts[-1][0] == month_start.strftime("%Y-%m") else cohorts
    prior_month_recalls = prior_cohorts[-1][1] if prior_cohorts else []
    monthly_count_history = [(ym, len(c)) for ym, c in cohorts]

    # Shallow stats (KPI strip + distribution + top 10)
    stats = compute_month_stats(month_recalls, prior_month_recalls)

    # Analytical signals (stats module) + predictive models (models module)
    signals = compute_monthly_signals(
        month_recalls=month_recalls,
        prior_months=[c for _, c in prior_cohorts],
        monthly_count_history=monthly_count_history,
    )
    # Suppress Z-score when baseline n is too small (sample-σ collapse).
    # Without this, March 2026 with n=3 baseline shows "Z=+52.8" — a
    # mathematical artifact of σ=0.71, not a real anomaly signal.
    _apply_baseline_size_guard(signals)
    pathogen_history = build_pathogen_history(cohorts) if cohorts else {}
    models = run_all_models(
        monthly_counts=monthly_count_history,
        monthly_counts_by_pathogen=pathogen_history,
    )

    # AI narrative
    narrative = generate_monthly_narrative(
        stats, signals, models, month_recalls, month_start.strftime("%B"), month_start.year
    )

    # HTML + companion page
    out_path = Path(args.output) if args.output else (ROOT / f"{month_start.year}-M{month_start.month:02d}.html")

    # Auto-detect rebuild vs fresh publish (audit 2026-05-09):
    # If a previous build of this month already exists on disk, this
    # invocation is a REBUILD — the masthead label flips to "UPDATED · {today}".
    # Fresh first-time builds (no prior file) render "PUBLISHED · {1st of next month}".
    #
    # The signal is FILE EXISTENCE, not the presence of a date marker —
    # legacy HTMLs built before this label-flip code lack the marker
    # entirely, but they ARE prior builds and should be treated as such.
    is_rebuild = out_path.exists() and _extract_total_from_html_monthly(out_path) is not None
    if is_rebuild:
        prior_label_date = _extract_published_from_html_monthly(out_path)
        log.info("Existing monthly HTML detected at %s (prior label='%s') — "
                 "rebuild will render UPDATED label",
                 out_path, prior_label_date or "<legacy: no label>")
    else:
        log.info("No prior monthly HTML at %s — first publish, PUBLISHED label", out_path)

    # Pass a non-empty string when this is a rebuild — build_monthly_html only
    # checks truthiness of original_published; the value's content is ignored.
    html = build_monthly_html(month_start, month_end_full, month_recalls,
                              stats, signals, models, narrative,
                              original_published=("rebuild" if is_rebuild else None))
    out_path.write_text(html, encoding="utf-8")
    log.info("Monthly report: %s (%d bytes)", out_path, len(html))

    all_path = Path(args.all_output) if args.all_output else (ROOT / f"{month_start.year}-M{month_start.month:02d}-all.html")
    all_html = build_all_month_html(month_start, month_end_full, month_recalls,
                                    back_href=out_path.name)
    all_path.write_text(all_html, encoding="utf-8")
    log.info("All-month companion: %s (%d bytes)", all_path, len(all_html))

    # Summary JSON for Apps Script (only when the month has closed)
    if month_end_full <= date.today():
        write_monthly_summary_json(
            month_start, month_end_full, stats, signals, models, narrative,
            month_recalls, args.site_url, args.dashboard_url, Path(args.summary_json),
        )
        # Running index for the dashboard's Monthly tab. Updated on every
        # closed-month run so the index always reflects the latest build
        # without requiring a manual edit to docs/index.html.
        update_monthly_index_json(
            month_start, month_end_full, stats, Path(args.monthly_index),
        )
    else:
        log.info("Skipping summary JSON (month not yet closed)")

    log.info("Done | Total=%d | Tier1=%d | Outbreaks=%d | Top=%s",
             stats["total"], stats["tier1"], stats["outbreaks"], stats["top_pathogen"][0])
    return 0


if __name__ == "__main__":
    sys.exit(main())
