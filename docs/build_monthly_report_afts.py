"""
AFTS Food Safety Intelligence System — Monthly Report Generator
================================================================
Runs on the 1st of each month 07:00 UTC via GitHub Actions.
Writes:
  docs/<YYYY>-M<MM>.html              — full monthly report HTML
  docs/data/monthly-summary-latest.json — compact summary consumed by the
                                          Google Apps Script subscriber mailer

Reads from: docs/data/recalls.xlsx (Recalls sheet only — never the Pending tab)

Architecture alignment:
  - xlsx is the single source of truth; this script is READ-ONLY for the xlsx
  - monthly-summary-latest.json shape mirrors weekly-summary-latest.json but
    swaps the week_* keys for month_* keys, matching what the Apps Script
    mailer already expects (see CONFIG.MONTHLY_SUMMARY_PATH and its consumers
    buildSubject_, buildEmailHtml_, buildEmailText_).
  - Reuses the weekly builder's helpers (severity_score, safe_int, etc.) so
    pathogen tiebreaking, URL grading and formatting stay identical.

The full monthly HTML is intentionally lighter than the weekly — one KPI strip,
leading pathogen, AI narrative, top 10 incidents, pathogen distribution table.
Trend predictions and deep retrospective analysis live in the quarterly/yearly
builders.
"""
from __future__ import annotations
import argparse
import calendar
import json
import logging
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

# We import the weekly module so we reuse exactly the same severity taxonomy,
# URL-grading, pathogen tiebreaker, and narrative clients. If the weekly
# helpers ever change, monthly inherits the fix for free.
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import build_weekly_report_afts as weekly  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("monthly")

# Brand tokens — mirror the weekly report so the HTML shares a visual identity.
BRAND_ORANGE = weekly.BRAND_ORANGE
BRAND_BLACK  = weekly.BRAND_BLACK
TIER1_RED    = weekly.TIER1_RED
TIER2_AMBER  = weekly.TIER2_AMBER
OUTBREAK_VIO = weekly.OUTBREAK_VIO


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def month_bounds(year: int, month: int) -> Tuple[date, date]:
    """Inclusive first/last day of the given month."""
    start = date(year, month, 1)
    end = date(year, month, calendar.monthrange(year, month)[1])
    return start, end


def filter_month(recalls: List[Dict], start: date, end: date) -> List[Dict]:
    out = []
    for r in recalls:
        d = r.get("Date", "")
        if not d:
            continue
        try:
            rd = datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue
        if start <= rd <= end:
            out.append(r)
    return out


# ---------------------------------------------------------------------------
# Statistics — mirrors compute_stats but for a month window. The leading-
# pathogen tiebreaker (cases -> tier1 count -> outbreak count -> name) is the
# same as the weekly builder, so reports stay consistent across cadences.
# ---------------------------------------------------------------------------
def compute_month_stats(month_recalls: List[Dict],
                        prev_month_recalls: List[Dict]) -> Dict[str, Any]:
    total = len(month_recalls)
    tier1 = sum(1 for r in month_recalls if weekly.safe_int(r.get("Tier")) == 1)
    outbreaks = sum(1 for r in month_recalls if weekly.safe_int(r.get("Outbreak")) == 1)

    pathogen_counts = Counter()
    pathogen_tier1 = Counter()
    pathogen_outbreak = Counter()
    for r in month_recalls:
        p = (r.get("Pathogen") or "").strip()
        if p:
            _, canon = weekly.severity_score(p)
            pathogen_counts[canon] += 1
            if weekly.safe_int(r.get("Tier")) == 1:
                pathogen_tier1[canon] += 1
            if weekly.safe_int(r.get("Outbreak")) == 1:
                pathogen_outbreak[canon] += 1

    country_counts = Counter(
        (r.get("Country") or "Unknown").strip() for r in month_recalls
    )
    source_counts = Counter()
    for r in month_recalls:
        s = (r.get("Source") or "").strip()
        if s:
            source_counts[s] += 1

    prev_total = len(prev_month_recalls)
    delta = total - prev_total
    delta_pct = round((delta / prev_total) * 100) if prev_total else None

    if pathogen_counts:
        ranked = sorted(
            pathogen_counts.items(),
            key=lambda kv: (-kv[1],
                            -pathogen_tier1.get(kv[0], 0),
                            -pathogen_outbreak.get(kv[0], 0),
                            kv[0]),
        )
        top_p = ranked[0]
    else:
        top_p = ("-", 0)

    return {
        "total": total,
        "tier1": tier1,
        "outbreaks": outbreaks,
        "top_pathogen": top_p,
        "pathogen_counts": pathogen_counts.most_common(15),
        "pathogen_tier1": dict(pathogen_tier1),
        "pathogen_outbreak": dict(pathogen_outbreak),
        "country_counts": country_counts.most_common(20),
        "source_counts": source_counts.most_common(20),
        "prev_total": prev_total,
        "delta": delta,
        "delta_pct": delta_pct,
    }


def rank_top_monthly(recalls: List[Dict], n: int = 10) -> List[Dict]:
    """Rank by URL quality first, then severity, outbreak, tier, date (same as weekly)."""
    def key(r):
        url_ok = 0 if weekly.is_report_grade_url(r.get("URL") or "") else 1
        sev, _ = weekly.severity_score(r.get("Pathogen") or "")
        outbreak = -weekly.safe_int(r.get("Outbreak"))
        tier = weekly.safe_int(r.get("Tier"), 3)
        # Newer first within ties
        d = str(r.get("Date") or "")
        return (url_ok, -sev, outbreak, tier, d[::-1] if d else "")
    ranked = sorted(recalls, key=key)
    return ranked[:n]


# ---------------------------------------------------------------------------
# AI narrative — reuse the same Claude + OpenAI clients as weekly, but re-prompt
# for monthly framing. Falls back to a deterministic paragraph if AI is missing.
# ---------------------------------------------------------------------------
def generate_monthly_narrative(stats: Dict[str, Any],
                               month_recalls: List[Dict],
                               month_name: str,
                               year: int) -> str:
    """
    Three paragraphs of analysis (plus optional 4th Process Authority Note).
    Falls through to a deterministic summary if the ANTHROPIC_API_KEY isn't set.
    """
    # Detect Process Authority trigger for this month. Same module as weekly,
    # so prompt + keywords stay in sync between cadences.
    from process_authority import (
        detect_process_authority_trigger,
        build_prompt_extension,
        deterministic_fallback,
    )
    pa_trigger = detect_process_authority_trigger(month_recalls)
    pa_extension = build_prompt_extension(pa_trigger)

    if not os.getenv("ANTHROPIC_API_KEY", "").strip():
        log.warning("ANTHROPIC_API_KEY missing; using fallback monthly narrative")
        return generate_monthly_fallback(stats, month_name, year, pa_trigger)

    import requests  # lazy import so --skip-ai runs without network deps

    top_incidents = []
    for r in rank_top_monthly(month_recalls, 10):
        top_incidents.append({
            "date":    str(r.get("Date"))[:10],
            "country": r.get("Country", ""),
            "source":  r.get("Source", ""),
            "company": (r.get("Company") or "")[:60],
            "product": (r.get("Product") or "")[:120],
            "pathogen": r.get("Pathogen", ""),
            "tier":    weekly.safe_int(r.get("Tier"), 3),
            "outbreak": weekly.safe_int(r.get("Outbreak"), 0),
        })

    prompt = f"""You are a senior food safety analyst writing the monthly intelligence briefing for {month_name} {year}.

MONTH STATS
Total incidents: {stats['total']}
Tier-1 (critical): {stats['tier1']}
Outbreaks: {stats['outbreaks']}
Δ vs prior month: {stats.get('delta')} ({stats.get('delta_pct')}%)

PATHOGEN DISTRIBUTION (top 10)
{dict(stats['pathogen_counts'][:10])}

GEOGRAPHIC DISTRIBUTION (top 10)
{dict(stats['country_counts'][:10])}

TOP 10 INCIDENTS
{json.dumps(top_incidents, indent=2)}

Write exactly THREE paragraphs, each 4–6 sentences, professional-engineering tone.
Paragraph 1 — month-scale headline + contextual framing (compare to prior month, call out any notable shifts).
Paragraph 2 — pathogen-specific analysis (what dominated, what was notable about the mix, any cluster/outbreak detail).
Paragraph 3 — forward-looking regulatory + operator implications for the coming month.

NO headers, NO bullet points, NO emoji, NO markdown. Use UK/US business English. Reference specific numbers and named pathogens.
Return the paragraphs separated by a single blank line. No preamble.
"""
    # Append the Process Authority Note prompt only when the trigger fired.
    if pa_extension:
        prompt += "\n\n" + pa_extension
        log.info("Process Authority trigger fired (monthly): %d matching incident(s) — %s",
                 pa_trigger["total_matches"], ", ".join(pa_trigger["keywords_hit"][:6]))

    try:
        r = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': os.getenv("ANTHROPIC_API_KEY", "").strip(),
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': 'claude-sonnet-4-20250514',
                'max_tokens': 2200,
                'messages': [{'role': 'user', 'content': prompt}],
            },
            timeout=90,
        )
        if r.status_code != 200:
            log.warning("Claude %d: %s", r.status_code, r.text[:200])
            return generate_monthly_fallback(stats, month_name, year, pa_trigger)
        data = r.json()
        parts = [b.get('text', '') for b in data.get('content', []) if b.get('type') == 'text']
        narrative = "\n\n".join(p for p in parts if p).strip()
        return narrative or generate_monthly_fallback(stats, month_name, year, pa_trigger)
    except Exception as e:
        log.warning("Claude monthly narrative failed: %s", e)
        return generate_monthly_fallback(stats, month_name, year, pa_trigger)


def generate_monthly_fallback(stats: Dict[str, Any], month_name: str, year: int,
                              pa_trigger: Dict[str, Any] = None) -> str:
    from process_authority import deterministic_fallback
    top_name, top_count = stats.get("top_pathogen", ("-", 0))
    pct = round(top_count / stats["total"] * 100) if stats["total"] else 0
    delta = stats.get("delta")
    delta_pct = stats.get("delta_pct")
    delta_phrase = (
        f"a {delta_pct}% shift versus the prior month"
        if delta is not None and delta_pct is not None
        else "a baseline month with no prior comparator"
    )
    body = (
        f"During {month_name} {year} the AFTS monitoring network recorded "
        f"{stats['total']} pathogen-related recall incidents across {len(stats.get('country_counts', []))} "
        f"jurisdictions, with {stats['tier1']} classified as Tier-1 critical and "
        f"{stats['outbreaks']} linked to confirmed outbreak clusters — {delta_phrase}.\n\n"
        f"{top_name} dominated the surveillance window, accounting for {top_count} of "
        f"{stats['total']} incidents ({pct}%). The pathogen mix warrants routine review of "
        f"HACCP critical limits, sanitation verification schedules, and environmental monitoring "
        f"intensities in facilities producing ready-to-eat and low-acid preserved products.\n\n"
        f"Operators should read the month's Tier-1 share as a planning signal for the next "
        f"30 days: expect continued regulatory attention on microbial hazards, tightened "
        f"documentation on thermal-process deviations, and elevated demand for third-party "
        f"lethality validation on any formulation or equipment changes underway."
    )
    # Optional 4th paragraph: Process Authority Note (when trigger fired)
    pa_note = deterministic_fallback(pa_trigger or {"fired": False})
    if pa_note:
        body = f"{body}\n\n{pa_note}"
    return body


# ---------------------------------------------------------------------------
# HTML rendering — compact, built for Friday weekly consumers who want a
# monthly-level summary. Leans on the weekly builder's CSS conventions so the
# brand is visually consistent.
# ---------------------------------------------------------------------------
def build_monthly_html(month_start: date, month_end: date,
                       month_recalls: List[Dict],
                       stats: Dict[str, Any],
                       narrative: str) -> str:
    month_name = month_start.strftime("%B")
    year = month_start.year
    paragraphs = [p for p in narrative.split("\n\n") if p.strip()]

    # Render paragraphs — 4th (when present) is the Process Authority Note and
    # gets distinctive red-accent styling so thermal-process hazards stand out.
    from process_authority import PROCESS_AUTHORITY_LABEL
    _pa_label_lc = PROCESS_AUTHORITY_LABEL.lower()
    analysis_parts: List[str] = []
    for p in paragraphs:
        is_pa = p.lower().lstrip().startswith(_pa_label_lc)
        if is_pa:
            idx = p.lower().find(_pa_label_lc)
            colon = p.find(":", idx) if idx != -1 else -1
            if colon != -1:
                label_text = p[idx:colon].strip()
                body_text = p[colon + 1:].strip()
                analysis_parts.append(
                    f'<p class="pa-note"><span class="pa-label">{weekly.escape(label_text)}:</span> '
                    f'{weekly.escape(body_text)}</p>'
                )
            else:
                analysis_parts.append(f'<p class="pa-note">{weekly.escape(p)}</p>')
        else:
            analysis_parts.append(f'<p>{weekly.escape(p)}</p>')
    analysis_html = "".join(analysis_parts)

    # Pathogen table
    total_safe = stats["total"] or 1
    path_rows_html = ""
    for name, count in stats["pathogen_counts"]:
        pct = round(count / total_safe * 100)
        _, canon = weekly.severity_score(name)
        color = weekly.pathogen_badge_color(canon)
        bar_w = max(4, min(100, pct))
        path_rows_html += f"""
        <tr>
          <td><span class="path-dot" style="background:{color}"></span>{weekly.escape(name)}</td>
          <td class="num">{count}</td>
          <td><div class="bar"><div class="bar-fill" style="width:{bar_w}%;background:{color}"></div></div></td>
          <td class="num muted">{pct}%</td>
        </tr>"""

    # Top 10 threats
    top10 = rank_top_monthly(month_recalls, 10)
    top_rows_html = "".join(weekly.render_top5_row(i + 1, r) for i, r in enumerate(top10))

    delta_phrase = (
        f"{stats.get('delta'):+d} ({stats.get('delta_pct'):+d}%) vs prior month"
        if stats.get("delta") is not None and stats.get("delta_pct") is not None
        else "baseline month"
    )

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>AFTS Monthly · {month_name} {year}</title>
<style>
body{{margin:0;padding:0;background:#f5f5f5;font-family:'DM Sans',-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;color:#1f2937;}}
.wrap{{max-width:900px;margin:28px auto;background:#fff;padding:32px 40px;border:1px solid #e5e7eb;}}
.mast{{border-bottom:1px solid #e5e7eb;padding-bottom:14px;margin-bottom:22px;display:flex;justify-content:space-between;align-items:flex-start;}}
.mast .brand{{font-family:Syne,Georgia,serif;font-weight:800;font-size:17px;color:{BRAND_BLACK};text-transform:uppercase;letter-spacing:-0.01em;}}
.mast .brand .accent{{color:{BRAND_ORANGE};}}
.mast .tagline{{font-family:'DM Mono',monospace;font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.14em;margin-top:4px;}}
.mast .pill{{background:{BRAND_BLACK};color:#fff;font-family:'DM Mono',monospace;font-size:10px;padding:4px 10px;letter-spacing:0.12em;text-transform:uppercase;}}
h1.r-title{{font-family:Syne,Georgia,serif;font-weight:800;font-size:28px;color:{BRAND_BLACK};letter-spacing:-0.02em;margin:0 0 6px;}}
h1.r-title .accent{{color:{BRAND_ORANGE};}}
.sub{{font-family:'DM Mono',monospace;font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.1em;margin:0 0 22px;}}
.kpi-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:#e5e7eb;border:1px solid #e5e7eb;margin-bottom:26px;}}
.kpi{{background:#fff;padding:16px 14px;}}
.kpi-label{{font-family:'DM Mono',monospace;font-size:9px;font-weight:700;color:#6b7280;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:6px;}}
.kpi-value{{font-family:Syne,Georgia,serif;font-weight:800;font-size:26px;line-height:1;color:{BRAND_BLACK};}}
.kpi-value.red{{color:{TIER1_RED};}}
.kpi-value.vio{{color:{OUTBREAK_VIO};}}
.kpi-value.orange{{color:{BRAND_ORANGE};}}
.kpi-top{{font-size:10px;color:#6b7280;margin-top:6px;}}
.sec-head{{display:flex;align-items:center;gap:10px;margin:24px 0 10px;}}
.sec-num{{font-family:'DM Mono',monospace;font-size:10px;color:{BRAND_ORANGE};font-weight:700;letter-spacing:0.1em;}}
.sec-title{{font-family:Syne,Georgia,serif;font-weight:800;font-size:18px;letter-spacing:-0.01em;color:{BRAND_BLACK};margin:0;}}
.analysis p{{font-size:13.5px;line-height:1.75;color:#1f2937;margin:0 0 12px;}}
.analysis p.pa-note{{margin:14px -18px 0 -18px;padding:14px 18px;background:#fff;border-top:1px solid #e5e7eb;font-size:13px;line-height:1.7;}}
.analysis p.pa-note .pa-label{{display:inline;font-family:'DM Mono',ui-monospace,monospace;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:{TIER1_RED};font-size:10px;margin-right:6px;}}
table{{width:100%;border-collapse:collapse;font-size:12.5px;}}
table.paths td{{padding:7px 10px;border-bottom:1px solid #f3f4f6;vertical-align:middle;}}
table.paths td.num{{text-align:right;font-family:'DM Mono',monospace;}}
table.paths td.muted{{color:#6b7280;}}
.path-dot{{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:8px;vertical-align:middle;}}
.bar{{background:#f3f4f6;height:6px;border-radius:1px;width:100%;min-width:120px;}}
.bar-fill{{height:6px;border-radius:1px;}}
.chip-tier1,.chip-tier2,.chip-outbreak{{display:inline-block;color:#fff;font-size:9px;font-weight:700;padding:2px 6px;border-radius:2px;letter-spacing:0.06em;margin-left:5px;}}
.chip-tier1{{background:{TIER1_RED};}}
.chip-tier2{{background:{TIER2_AMBER};color:#1f2937;}}
.chip-outbreak{{background:{OUTBREAK_VIO};}}
.footer{{border-top:2px solid {BRAND_BLACK};margin-top:28px;padding-top:18px;font-family:'DM Mono',monospace;font-size:10px;color:#6b7280;line-height:1.7;}}
.footer .fb{{font-family:Syne,Georgia,serif;font-weight:800;font-size:12px;color:{BRAND_BLACK};text-transform:uppercase;}}
.footer .fb .accent{{color:{BRAND_ORANGE};}}
</style></head><body><div class="wrap">

<div class="mast">
  <div>
    <div class="brand">Advanced Food-Tech Solutions <span class="accent">·</span> AFTS</div>
    <div class="tagline">Food Safety Intelligence System · Monthly Briefing</div>
  </div>
  <div class="pill">{month_name} {year}</div>
</div>

<h1 class="r-title">Pathogen Surveillance <span class="accent">·</span> {month_name} {year}</h1>
<div class="sub">{month_start.strftime('%d %b %Y')} – {month_end.strftime('%d %b %Y')} &middot; {stats['total']} recalls across {len(stats.get('country_counts', []))} jurisdictions</div>

<div class="kpi-grid">
  <div class="kpi">
    <div class="kpi-label">Total Recalls</div>
    <div class="kpi-value">{stats['total']}</div>
    <div class="kpi-top">{delta_phrase}</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Tier-1 Critical</div>
    <div class="kpi-value red">{stats['tier1']}</div>
    <div class="kpi-top">Immediate public-health risk</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Outbreaks</div>
    <div class="kpi-value vio">{stats['outbreaks']}</div>
    <div class="kpi-top">Confirmed clusters</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Leading Pathogen</div>
    <div class="kpi-value orange" style="font-size:15px;line-height:1.3;">{weekly.escape(stats['top_pathogen'][0])}</div>
    <div class="kpi-top">{stats['top_pathogen'][1]} cases · {round(stats['top_pathogen'][1] / (stats['total'] or 1) * 100)}% of total</div>
  </div>
</div>

<div class="sec-head"><span class="sec-num">§ 01</span><h2 class="sec-title">Intelligence Analysis</h2></div>
<div class="analysis">{analysis_html}</div>

<div class="sec-head"><span class="sec-num">§ 02</span><h2 class="sec-title">Pathogen Distribution</h2></div>
<table class="paths"><tbody>{path_rows_html}</tbody></table>

<div class="sec-head"><span class="sec-num">§ 03</span><h2 class="sec-title">Top {len(top10)} Critical Incidents</h2></div>
<table class="top5"><tbody>{top_rows_html}</tbody></table>

<div class="footer">
  <div class="fb">Advanced Food-Tech Solutions <span class="accent">·</span> AFTS</div>
  Food Process Engineering · Thermal Processing · Regulatory Compliance<br>
  advfood.tech · info@advfood.tech · Athens, Greece<br>
  Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
</div>

</div></body></html>"""
    return html


# ---------------------------------------------------------------------------
# monthly-summary-latest.json — matches the Apps Script mailer contract
# (see CONFIG.MONTHLY_SUMMARY_PATH and the buildEmail* functions).
# ---------------------------------------------------------------------------
def write_monthly_summary_json(month_start: date, month_end: date,
                               stats: Dict[str, Any], narrative: str,
                               month_recalls: List[Dict],
                               site_base_url: str, dashboard_url: str,
                               out_path: Path) -> None:
    month_name = month_start.strftime("%B")
    month_short = month_start.strftime("%b")
    year = month_start.year

    top10_out = []
    for i, r in enumerate(rank_top_monthly(month_recalls, 10), 1):
        _, canon = weekly.severity_score(r.get("Pathogen") or "")
        url = (r.get("URL") or "").strip()
        good_url = weekly.is_report_grade_url(url)
        top10_out.append({
            "rank":    i,
            "date":    weekly.fmt_date(r.get("Date")),
            "pathogen":     canon,
            "pathogen_raw": r.get("Pathogen") or "",
            "tier":    weekly.safe_int(r.get("Tier"), 3),
            "outbreak": bool(weekly.safe_int(r.get("Outbreak"), 0)),
            "company": (r.get("Company") or "")[:80],
            "brand":   (r.get("Brand") or "")[:60],
            "product": (r.get("Product") or "")[:140],
            "country": r.get("Country") or "",
            "source":  (r.get("Source") or "").strip(),
            "url":     url if good_url else "",
        })

    top_pathogen_name, top_pathogen_count = stats.get("top_pathogen", ("-", 0))
    total_safe = stats["total"] or 1
    site_base = site_base_url.rstrip("/")

    first_para = (narrative.split("\n\n", 1)[0] if narrative else "").strip()

    # Extract the optional Process Authority Note paragraph (4th) so the
    # Apps Script monthly mailer can render it as a dedicated call-out block.
    from process_authority import PROCESS_AUTHORITY_LABEL
    _pa_label_lc = PROCESS_AUTHORITY_LABEL.lower()
    pa_note_out = ""
    for p in (narrative.split("\n\n") if narrative else []):
        if p.lower().lstrip().startswith(_pa_label_lc):
            pa_note_out = p.strip()
            break

    summary = {
        "filename":      f"{year}-M{month_start.month:02d}.html",
        "report_url":    f"{site_base}/{year}-M{month_start.month:02d}.html",
        "dashboard_url": dashboard_url,
        "month_tag":     f"{year}-{month_start.month:02d}",
        "month_num":     month_start.month,
        "month_name":    f"{month_name} {year}",
        "month_name_short": month_short,
        "year":          year,
        "month_start":   month_start.strftime("%Y-%m-%d"),
        "month_end":     month_end.strftime("%Y-%m-%d"),
        "month_start_display": month_start.strftime("%d %b"),
        "month_end_display":   month_end.strftime("%d %b %Y"),
        "generated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "stats": {
            "total":     stats["total"],
            "tier1":     stats["tier1"],
            "outbreaks": stats["outbreaks"],
            "delta":     stats.get("delta"),
            "delta_pct": stats.get("delta_pct"),
        },
        "leading_pathogen": {
            "name":  top_pathogen_name,
            "cases": top_pathogen_count,
            "pct":   round(top_pathogen_count / total_safe * 100) if stats["total"] else 0,
        },
        "ai_lead_paragraph": first_para,
        # Process Authority Note — empty string when trigger didn't fire
        "process_authority_note": pa_note_out,
        "top_threats": top10_out,
        "country_count": len(stats.get("country_counts", [])),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Monthly summary JSON written: %s", out_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="AFTS monthly food safety intelligence briefing")
    ap.add_argument("--month-end", required=True,
                    help="Last day of the month to report on, YYYY-MM-DD")
    ap.add_argument("--xlsx", default=str(ROOT / "data" / "recalls.xlsx"))
    ap.add_argument("--output", default=None,
                    help="Output HTML path (default: <year>-M<month>.html next to index.html)")
    ap.add_argument("--index", default=str(ROOT / "index.html"))
    ap.add_argument("--site-url",
                    default="https://gstoforos.github.io/Food-Safety-Intelligence-System",
                    help="Public base URL where the docs/ folder is served")
    ap.add_argument("--dashboard-url",
                    default="https://www.advfood.tech/food-safety-intelligence")
    ap.add_argument("--summary-json",
                    default=str(ROOT / "data" / "monthly-summary-latest.json"))
    args = ap.parse_args()

    try:
        month_end = datetime.strptime(args.month_end, "%Y-%m-%d").date()
    except ValueError:
        log.error("Invalid --month-end: %s", args.month_end)
        return 2

    month_start, month_end_full = month_bounds(month_end.year, month_end.month)
    log.info("AFTS monthly report | %s %s (%s – %s)",
             month_start.strftime("%B"), month_start.year,
             month_start.isoformat(), month_end_full.isoformat())

    xlsx_path = Path(args.xlsx)
    all_recalls = weekly.load_recalls(xlsx_path)
    log.info("Loaded %d recalls from %s", len(all_recalls), xlsx_path)
    if not all_recalls:
        log.error("No recalls loaded; aborting.")
        return 3

    month_recalls = filter_month(all_recalls, month_start, month_end_full)
    # Prior month for delta
    prev_start = (month_start - timedelta(days=1)).replace(day=1)
    prev_end = month_start - timedelta(days=1)
    prev_recalls = filter_month(all_recalls, prev_start, prev_end)
    log.info("This month: %d | prior month: %d", len(month_recalls), len(prev_recalls))

    stats = compute_month_stats(month_recalls, prev_recalls)
    narrative = generate_monthly_narrative(
        stats, month_recalls, month_start.strftime("%B"), month_start.year,
    )
    html = build_monthly_html(month_start, month_end_full, month_recalls, stats, narrative)

    out_path = Path(args.output) if args.output else (
        ROOT / f"{month_start.year}-M{month_start.month:02d}.html"
    )
    out_path.write_text(html, encoding="utf-8")
    log.info("Monthly report written: %s (%d bytes)", out_path, len(html))

    # Write monthly summary JSON only once the month is actually closed —
    # prevents the Apps Script mailer from sending an email for a month
    # that's still in progress.
    if month_end_full <= date.today():
        summary_path = Path(args.summary_json)
        write_monthly_summary_json(
            month_start, month_end_full, stats, narrative, month_recalls,
            args.site_url, args.dashboard_url, summary_path,
        )
    else:
        log.info("Monthly summary JSON skipped (month %s not yet closed)",
                 month_start.strftime("%Y-%m"))

    log.info("Done | Total=%d | Tier1=%d | Outbreaks=%d | Top=%s",
             stats["total"], stats["tier1"], stats["outbreaks"],
             stats["top_pathogen"][0] if stats["top_pathogen"] else "-")
    return 0


if __name__ == "__main__":
    sys.exit(main())
