"""
AFTS Food Safety Intelligence System — Weekly Report Generator
================================================================
Runs every Friday 08:00 UTC via GitHub Actions.
Pipeline: Excel (recalls.xlsx) -> stats -> Claude AI analysis -> OpenAI polish -> HTML report.

Output:
  - <year>-W<week>.html  (report, committed to repo root)
  - index.html           (dashboard embedded reports array, appended)

Brand: Advanced Food-Tech Solutions (AFTS)
Palette: black #0a0e1a / white #fff / orange #E8601A (AFTS brand)
Typography: Syne (display) + DM Sans (body) + DM Mono (data)
"""

import json
import logging
import os
import re
import sys
import argparse
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from collections import Counter
from typing import List, Dict, Any, Tuple

import requests
from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CLAUDE_API_KEY = os.getenv('ANTHROPIC_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# --- AFTS brand tokens -------------------------------------------------------
BRAND_ORANGE = "#E8601A"
BRAND_BLACK  = "#0a0e1a"
TIER1_RED    = "#dc2626"
TIER2_AMBER  = "#f59e0b"
OUTBREAK_VIO = "#9333ea"

# --- Regulatory authority map (country -> primary agency) -------------------
AUTHORITY_MAP = {
    "United States": "FDA / USDA FSIS",
    "USA": "FDA / USDA FSIS",
    "US": "FDA / USDA FSIS",
    "Canada": "CFIA",
    "United Kingdom": "FSA (UK)",
    "UK": "FSA (UK)",
    "France": "RappelConso / DGCCRF",
    "Germany": "BVL",
    "Italy": "Ministero della Salute",
    "Spain": "AESAN",
    "Netherlands": "NVWA",
    "Belgium": "FAVV-AFSCA",
    "Ireland": "FSAI",
    "Portugal": "ASAE",
    "Switzerland": "BLV",
    "Austria": "AGES",
    "Sweden": "Livsmedelsverket",
    "Denmark": "Foedevarestyrelsen",
    "Norway": "Mattilsynet",
    "Finland": "Ruokavirasto",
    "Greece": "EFET",
    "Poland": "GIS",
    "Australia": "FSANZ",
    "New Zealand": "MPI / FSANZ",
    "Japan": "MHLW",
    "Hong Kong": "CFS (HK)",
    "Singapore": "SFA",
    "South Korea": "MFDS",
    "Brazil": "ANVISA",
    "Mexico": "COFEPRIS",
    "Argentina": "ANMAT",
    "Chile": "ACHIPIA / ISP",
    "South Africa": "DALRRD",
}

def authority_for(country: str) -> str:
    if not country:
        return "Multiple / Unknown"
    return AUTHORITY_MAP.get(country.strip(), "National Authority")

# --- Pathogen severity (Tier-1 first) ---------------------------------------
PATHOGEN_SEVERITY = [
    ("clostridium botulinum", 1, "Clostridium botulinum"),
    ("botulinum",             1, "Clostridium botulinum"),
    ("listeria",              2, "Listeria monocytogenes"),
    ("stec",                  3, "STEC / E. coli O157:H7"),
    ("o157",                  3, "STEC / E. coli O157:H7"),
    ("escherichia coli",      3, "E. coli"),
    ("e. coli",               3, "E. coli"),
    ("salmonella",            4, "Salmonella spp."),
    ("cronobacter",           5, "Cronobacter sakazakii"),
    ("vibrio",                6, "Vibrio spp."),
    ("hepatitis",             7, "Hepatitis A"),
    ("norovirus",             8, "Norovirus"),
    ("campylobacter",         9, "Campylobacter"),
    ("staphylococcus",       10, "Staphylococcus aureus"),
    ("bacillus cereus",      11, "Bacillus cereus / Cereulide"),
    ("cereulide",            11, "Bacillus cereus / Cereulide"),
    ("clostridium perfringens", 12, "C. perfringens"),
]

def severity_score(pathogen: str) -> Tuple[int, str]:
    """Returns (severity_rank, canonical_name). Lower rank = more severe."""
    p = (pathogen or "").lower()
    for key, rank, canon in PATHOGEN_SEVERITY:
        if key in p:
            return rank, canon
    return 99, (pathogen or "Unknown")

# --- URL quality (local-only, no HTTP) --------------------------------------
# A URL is "report-grade" if it's non-empty, starts with http(s), isn't a known
# generic landing page, and has at least 2 path segments (i.e., points to a
# specific recall, not a category or homepage). This check runs at report time
# so bad URLs that slipped past the guardian can't reach the Top-5.
_GENERIC_URL_PATTERNS = [
    r"/categorie/\d+/?$",
    r"/categorie/0/\d+/[a-z]+/?$",
    r"/anakleiseis-cat/?$",
    r"/alertas_alimentarias/?$",
    r"/liste/lebensmittel/bundesweit/?$",
    r"/portal/news/p3_2_1_3\.jsp",
    r"/food-recalls/?$",
    r"/recalls?/?$",
    r"/alerts?/?$",
    r"/news/?$",
    r"/category/[^/]+/?$",
    r"/tag/[^/]+/?$",
    r"/search/?",
    r"^https?://[^/]+/?$",
    # FDA / USDA FSIS / CFIA / FSA / FSAI landing pages (mirror url_validator.py)
    r"/safety/recalls-market-withdrawals-safety-alerts/?$",
    r"/safety/recalls/?$",
    r"/recalls-alerts/?$",
    r"/recalls-public-health-alerts/?$",
    r"/food-recall-warnings/?$",
    r"/food-recall-warnings-and-allergy-alerts/?$",
    r"/news-alerts/?$",
    r"/consumer/food-alerts/?$",
    r"/recall-and-advice-list/?$",
    r"/list-of-recalls/?$",
    r"/food-alert-list/?$",
]

def is_report_grade_url(url: str) -> bool:
    if not url or len(url) < 20:
        return False
    u = url.strip()
    if not u.lower().startswith(("http://", "https://")):
        return False
    for pat in _GENERIC_URL_PATTERNS:
        if re.search(pat, u, re.I):
            return False
    try:
        from urllib.parse import urlparse
        p = urlparse(u)
    except Exception:
        return False
    if not p.netloc:
        return False
    segments = [s for s in (p.path or "").split("/") if s]
    return len(segments) >= 2

# --- Data loading -----------------------------------------------------------
def load_recalls(xlsx_path: Path) -> List[Dict[str, Any]]:
    if not xlsx_path.exists():
        log.error("XLSX file not found: %s", xlsx_path)
        return []
    wb = load_workbook(xlsx_path, data_only=True)
    if "Recalls" not in wb.sheetnames:
        log.error("Recalls sheet not found. Available: %s", wb.sheetnames)
        return []
    ws = wb["Recalls"]
    headers = [c.value for c in ws[1]]
    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        rec = {h: (v if v is not None else "") for h, v in zip(headers, row)}
        out.append(rec)
    return out

def filter_week(recalls: List[Dict], week_end: date) -> List[Dict]:
    """Recalls dated between (week_end - 6 days) and week_end inclusive."""
    week_start = week_end - timedelta(days=6)
    out = []
    for r in recalls:
        d = r.get("Date", "")
        if not d:
            continue
        try:
            rd = datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue
        if week_start <= rd <= week_end:
            out.append(r)
    return out

# --- Statistics -------------------------------------------------------------
def safe_int(v, default=0):
    try:
        return int(v)
    except (ValueError, TypeError):
        return default

def compute_stats(week_recalls: List[Dict], prev_week_recalls: List[Dict]) -> Dict[str, Any]:
    total = len(week_recalls)
    tier1 = sum(1 for r in week_recalls if safe_int(r.get("Tier")) == 1)
    outbreaks = sum(1 for r in week_recalls if safe_int(r.get("Outbreak")) == 1)

    pathogen_counts = Counter()
    for r in week_recalls:
        p = (r.get("Pathogen") or "").strip()
        if p:
            _, canon = severity_score(p)
            pathogen_counts[canon] += 1

    country_counts = Counter()
    for r in week_recalls:
        c = (r.get("Country") or "Unknown").strip()
        country_counts[c] += 1

    source_counts = Counter()
    for r in week_recalls:
        s = (r.get("Source") or "").strip()
        if s:
            source_counts[s] += 1

    prev_total = len(prev_week_recalls)
    delta = total - prev_total
    delta_pct = round((delta / prev_total) * 100) if prev_total else None

    top_p = pathogen_counts.most_common(1)[0] if pathogen_counts else ("-", 0)

    return {
        "total": total,
        "tier1": tier1,
        "outbreaks": outbreaks,
        "top_pathogen": top_p,
        "pathogen_counts": pathogen_counts.most_common(10),
        "country_counts": country_counts.most_common(10),
        "source_counts": source_counts.most_common(10),
        "prev_total": prev_total,
        "delta": delta,
        "delta_pct": delta_pct,
    }

def rank_top_recalls(week_recalls: List[Dict], n: int = 5) -> List[Dict]:
    """
    Rank by: URL quality (verifiable first), then severity, outbreak, tier, date.
    A Top-5 entry that would appear in an executive briefing MUST link to a specific
    recall page, never a homepage or category listing. Records without a report-grade
    URL fall to the bottom and only appear if we need to fill slots.
    """
    def score(r):
        has_good_url = is_report_grade_url(r.get("URL") or "")
        sev, _ = severity_score(r.get("Pathogen") or "")
        tier = safe_int(r.get("Tier"), 3)
        outbreak = safe_int(r.get("Outbreak"), 0)
        d = str(r.get("Date") or "")[:10]
        # URL-first so a verifiable Tier-2 beats an unverifiable Tier-1 in the Top-5
        return (0 if has_good_url else 1, sev, -outbreak, tier, d)
    ranked = sorted(week_recalls, key=score, reverse=False)
    # If there are enough good-URL rows to fill n, drop the unverifiable ones entirely
    good = [r for r in ranked if is_report_grade_url(r.get("URL") or "")]
    if len(good) >= n:
        return good[:n]
    # Otherwise include unverifiable rows at the bottom but mark them
    return ranked[:n]

# --- AI analysis ------------------------------------------------------------
def generate_report_with_claude(stats: Dict[str, Any], week_recalls: List[Dict]) -> str:
    if not CLAUDE_API_KEY:
        log.warning("ANTHROPIC_API_KEY missing; using fallback narrative")
        return generate_fallback_analysis(stats)

    top_incidents = []
    for r in rank_top_recalls(week_recalls, n=5):
        top_incidents.append({
            "pathogen": r.get("Pathogen", ""),
            "company":  (r.get("Company") or "")[:80],
            "product":  (r.get("Product") or "")[:120],
            "country":  r.get("Country", ""),
            "tier":     r.get("Tier", ""),
            "outbreak": r.get("Outbreak", 0),
        })

    delta_txt = ""
    if stats["delta_pct"] is not None:
        direction = "increase" if stats["delta"] > 0 else ("decrease" if stats["delta"] < 0 else "no change")
        delta_txt = f"Week-over-week {direction}: {stats['delta']:+d} recalls ({stats['delta_pct']:+d}%)."

    prompt = f"""You are producing the weekly pathogen surveillance briefing for Advanced Food-Tech Solutions (AFTS), a food process engineering firm. Your analysis MUST sound like it comes from a practising process authority - not a generic AI. That means: name specific process-control failure modes, cite validated engineering frameworks (F-value lethality, D-value, hold-tube residence time, FDA 21 CFR 113/114, PMO, HACCP CCPs, pre-op sanitation, environmental monitoring programmes), and tie every pathogen to the PROCESS that failed to eliminate it.

This is what differentiates AFTS from pure data platforms (e.g. Foodakai): we interpret recalls through validated food process engineering, not just count them.

DATA SUMMARY (this week):
- Total pathogen recalls: {stats['total']}
- Tier-1 (critical public-health risk): {stats['tier1']}
- Active outbreaks: {stats['outbreaks']}
- Leading pathogen: {stats['top_pathogen'][0]} ({stats['top_pathogen'][1]} cases)
- {delta_txt}

PATHOGEN DISTRIBUTION:
{dict(stats['pathogen_counts'])}

GEOGRAPHIC DISTRIBUTION:
{dict(stats['country_counts'])}

TOP 5 INCIDENTS:
{json.dumps(top_incidents, indent=2)}

Write exactly THREE paragraphs, each 3-4 sentences, in an authoritative professional-engineering tone. NO headers, NO bullet points, NO emoji, NO markdown. Use UK/US business English. Reference specific numbers and named pathogens.

Paragraph 1 - EXECUTIVE SUMMARY: Frame the week's activity quantitatively (total, Tier-1, outbreaks, week-over-week). Name the leading pathogen and at least one specific product category or commodity it appeared in this week.

Paragraph 2 - PROCESS-FAILURE ANALYSIS: Diagnose the likely failure mode(s) behind this week's dominant pathogen. Be specific: is this a thermal underprocess, a post-pasteurisation recontamination, an environmental monitoring gap, raw-material sourcing, sanitation SOP failure, validation drift, or a cold-chain breach? Reference the relevant engineering standard (e.g. "minimum 6-log Listeria lethality per 21 CFR 113", "HTST 72 C / 15 s per PMO", "aseptic hold-tube residence time validation") and name the food category most at risk. Do not hedge - a process authority would commit to a most-likely mechanism.

Paragraph 3 - ENGINEERING RECOMMENDATION: Close with a concrete recommendation a VP of QA at a food manufacturer could act on this week. Name the specific control(s) to re-verify (e.g. "revalidate hold-tube residence time under current production flow rates", "increase Zone 1 environmental swab frequency on RTE deli lines", "verify post-retort thermocouple placement"). Tie it to the week's regulatory pattern (RASFF, FDA, CFIA, FSA).

Return only the three paragraphs separated by a single blank line."""

    try:
        response = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'Content-Type': 'application/json',
                'x-api-key': CLAUDE_API_KEY,
                'anthropic-version': '2023-06-01',
            },
            json={
                'model': 'claude-sonnet-4-20250514',
                'max_tokens': 1400,
                'messages': [{'role': 'user', 'content': prompt}]
            },
            timeout=60,
        )
        if response.status_code == 200:
            return response.json()['content'][0]['text'].strip()
        log.error("Claude API %d: %s", response.status_code, response.text[:300])
    except Exception as e:
        log.error("Claude API exception: %s", e)
    return generate_fallback_analysis(stats)

def generate_fallback_analysis(stats: Dict[str, Any]) -> str:
    top_pathogen, count = stats['top_pathogen']
    total = stats['total']
    tier1 = stats['tier1']
    outbreaks = stats['outbreaks']
    pct = round((count / total) * 100) if total > 0 else 0

    p1 = (f"This week produced {total} pathogen-related recall incidents across the AFTS monitoring "
          f"network, with {tier1} classified as Tier-1 and {outbreaks} confirmed outbreak event(s). "
          f"{top_pathogen} dominated the surveillance window, accounting for {count} of {total} "
          f"incidents ({pct}%). The elevated Tier-1 ratio indicates sustained regulatory pressure "
          f"and should be read by food manufacturers as a signal of tightening enforcement.")

    # Tailor paragraph 2 to the leading pathogen
    p_lower = (top_pathogen or "").lower()
    if "listeria" in p_lower:
        p2 = (f"{top_pathogen} at this concentration points to post-process recontamination in "
              f"ready-to-eat deli, dairy, and cooked-meat lines rather than thermal underprocess. "
              f"The likely failure modes are Zone 1 environmental harbourage, sanitation SOP drift, "
              f"and post-lethality recontamination. 21 CFR 117 environmental monitoring and the "
              f"6-log Listeria lethality requirement (21 CFR 113/114 where applicable) are the "
              f"relevant frameworks for review.")
    elif "salmonella" in p_lower:
        p2 = (f"{top_pathogen} at this volume typically traces to raw-material contamination, "
              f"insufficient thermal lethality, or post-process handling. Validate pasteurisation "
              f"D-values against current product formulations, confirm hold-tube residence time "
              f"under production flow rates, and audit supplier verification protocols for "
              f"high-risk commodities (poultry, eggs, produce, low-moisture products).")
    elif "e. coli" in p_lower or "stec" in p_lower:
        p2 = (f"{top_pathogen} in RTE products indicates either inadequate cook step or "
              f"post-cook cross-contamination. Re-verify core temperature achievement against "
              f"USDA Appendix A lethality tables, confirm hot-hold temperatures at or above "
              f"60 C, and audit raw/cooked segregation on the processing line.")
    elif "botulinum" in p_lower:
        p2 = (f"{top_pathogen} recalls are process-authority events by definition. Verify scheduled "
              f"process adequacy under 21 CFR 113 (LACF) or 114 (acidified foods), revalidate "
              f"F-value delivery on the slowest-heating particle, and confirm container integrity "
              f"across the retort cycle. Any deviation from the filed scheduled process triggers "
              f"immediate process-authority review.")
    else:
        p2 = (f"{top_pathogen} at {pct}% of this week's total warrants review of both thermal "
              f"lethality validation and post-process hygiene controls. Re-verify CCP monitoring "
              f"records for the affected product categories and confirm environmental monitoring "
              f"coverage.")

    p3 = (f"Regulatory activity this week spanned multiple jurisdictions (RASFF, FDA, CFIA, FSA, "
          f"and national authorities), signalling continued inspection intensity. AFTS recommends "
          f"that food manufacturers use this briefing as a prompt to re-verify the single "
          f"highest-leverage control for their commodity this week and to confirm documentation "
          f"packages are ready for rapid regulatory response.")
    return f"{p1}\n\n{p2}\n\n{p3}"

def review_with_openai(report_content: str) -> str:
    if not OPENAI_API_KEY:
        return report_content

    prompt = f"""You are a senior editor for a food industry intelligence publication. Polish the following three-paragraph executive briefing for an audience of Fortune-500 food safety directors.

Rules:
- Preserve every number, pathogen name, and factual claim exactly.
- Keep exactly three paragraphs separated by a blank line.
- Tighten the language, remove redundancy, and ensure the tone is measured, authoritative, and non-alarmist.
- Use UK/US business English. No headers, no bullets, no emoji, no markdown.
- Max ~180 words per paragraph.

BRIEFING:
{report_content}

Return only the polished three paragraphs."""
    try:
        response = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {OPENAI_API_KEY}',
                'Content-Type': 'application/json'
            },
            json={
                'model': 'gpt-4o-mini',
                'messages': [{'role': 'user', 'content': prompt}],
                'max_tokens': 1000,
                'temperature': 0.3,
            },
            timeout=60,
        )
        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content'].strip()
        log.error("OpenAI API %d: %s", response.status_code, response.text[:300])
    except Exception as e:
        log.error("OpenAI API exception: %s", e)
    return report_content

# --- HTML rendering ---------------------------------------------------------
def escape(s: Any) -> str:
    if s is None:
        return ""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))

def fmt_date(s: Any) -> str:
    if not s:
        return "-"
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").strftime("%d %b %Y")
    except (ValueError, TypeError):
        return str(s)[:10] or "-"

def pathogen_badge_color(canon: str) -> str:
    rank, _ = severity_score(canon)
    if rank <= 1:  return OUTBREAK_VIO
    if rank <= 4:  return TIER1_RED
    return TIER2_AMBER

def render_top5_row(i: int, r: Dict) -> str:
    pathogen = r.get("Pathogen") or "Unknown"
    _, canon = severity_score(pathogen)
    badge_color = pathogen_badge_color(canon)
    tier = safe_int(r.get("Tier"), 3)
    outbreak = safe_int(r.get("Outbreak"), 0)
    company = (r.get("Company") or "-")[:55]
    brand = (r.get("Brand") or "").strip()
    product = (r.get("Product") or "-")[:85]
    country = r.get("Country") or "-"
    source = (r.get("Source") or "").strip() or authority_for(country)
    url = (r.get("URL") or "").strip()
    date_str = fmt_date(r.get("Date"))

    outbreak_chip = '<span class="chip-outbreak">OUTBREAK</span>' if outbreak else ''
    tier_chip = f'<span class="chip-tier{tier}">T{tier}</span>' if tier in (1, 2) else ''
    brand_line = f'<div class="brand-sub">{escape(brand)}</div>' if brand else ''

    if url and is_report_grade_url(url):
        link_cell = f'<a class="src-link" href="{escape(url)}" target="_blank" rel="noopener">View source &rarr;</a>'
    else:
        link_cell = '<span class="src-na" title="No verified specific-recall URL available">unverified</span>'

    return f"""
    <tr>
      <td class="rank-num" data-label="#">{i}</td>
      <td class="date-cell" data-label="Date">{escape(date_str)}</td>
      <td data-label="Pathogen">
        <span class="path-dot" style="background:{badge_color}"></span>
        <span class="path-name">{escape(canon)}</span>
        {tier_chip}{outbreak_chip}
      </td>
      <td class="co-cell" data-label="Company"><strong>{escape(company)}</strong>{brand_line}</td>
      <td class="prod-cell" data-label="Product">{escape(product)}</td>
      <td class="juris-cell" data-label="Jurisdiction">
        <div class="juris-country">{escape(country)}</div>
        <div class="src-sub">{escape(source)}</div>
        <div class="juris-link">{link_cell}</div>
      </td>
    </tr>"""

def build_html(week_end: date, recalls: List[Dict], prev_week: List[Dict]) -> Tuple[str, Dict[str, Any]]:
    stats = compute_stats(recalls, prev_week)
    week_start = week_end - timedelta(days=6)
    wnum = week_end.isocalendar()[1]
    year = week_end.year
    top5 = rank_top_recalls(recalls, n=5)

    # AI pipeline
    ai_raw = generate_report_with_claude(stats, recalls)
    analysis = review_with_openai(ai_raw)
    paragraphs = [p.strip() for p in analysis.split("\n\n") if p.strip()]
    while len(paragraphs) < 3:
        paragraphs.append("")

    # Stash the analysis and top5 for main() to use when writing the summary JSON
    stats["_first_paragraph"] = paragraphs[0]
    stats["_top5"] = top5

    # Top 5 rows
    if top5:
        top5_rows = "".join(render_top5_row(i, r) for i, r in enumerate(top5, 1))
    else:
        top5_rows = '<tr><td colspan="6" class="empty">No recalls recorded this reporting period.</td></tr>'

    # Pathogen distribution
    total_safe = stats['total'] or 1
    path_rows = ""
    for pathogen, count in stats['pathogen_counts']:
        pct = round((count / total_safe) * 100)
        _, canon = severity_score(pathogen)
        color = pathogen_badge_color(canon)
        bar_w = max(4, min(100, pct))
        path_rows += f"""
        <tr>
          <td><span class="path-dot" style="background:{color}"></span>{escape(pathogen)}</td>
          <td class="num">{count}</td>
          <td class="num">{pct}%</td>
          <td><div class="bar-track"><div class="bar-fill" style="width:{bar_w}%;background:{color}"></div></div></td>
        </tr>"""
    if not path_rows:
        path_rows = '<tr><td colspan="4" class="empty">No pathogen data.</td></tr>'

    # Country distribution
    country_rows = ""
    for country, count in stats['country_counts']:
        pct = round((count / total_safe) * 100)
        country_rows += f"""
        <tr>
          <td>{escape(country)}</td>
          <td>{escape(authority_for(country))}</td>
          <td class="num">{count}</td>
          <td class="num">{pct}%</td>
        </tr>"""
    if not country_rows:
        country_rows = '<tr><td colspan="4" class="empty">No geographic data.</td></tr>'

    # KPI delta block
    if stats['delta_pct'] is not None:
        arrow = "&#9650;" if stats['delta'] > 0 else ("&#9660;" if stats['delta'] < 0 else "&#9644;")
        color = TIER1_RED if stats['delta'] > 0 else ("#059669" if stats['delta'] < 0 else "#6b7280")
        delta_html = f'<div class="kpi-delta" style="color:{color}">{arrow} {stats["delta"]:+d} ({stats["delta_pct"]:+d}%) vs prior week</div>'
    else:
        delta_html = '<div class="kpi-delta" style="color:#6b7280">- baseline week</div>'

    top_pathogen_pct = round(stats['top_pathogen'][1] / total_safe * 100) if stats['total'] else 0
    generated = datetime.now(timezone.utc).strftime('%d %b %Y &middot; %H:%M UTC')

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AFTS Pathogen Intelligence Briefing &middot; Week {wnum}, {year}</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
:root {{
  --black:{BRAND_BLACK}; --orange:{BRAND_ORANGE};
  --ink:#111827; --body:#1f2937; --muted:#6b7280; --dim:#9ca3af;
  --bg:#ffffff; --s1:#f9fafb; --s2:#f3f4f6; --brd:#e5e7eb;
  --red:{TIER1_RED}; --amber:{TIER2_AMBER}; --violet:{OUTBREAK_VIO}; --green:#059669;
}}
* {{ box-sizing:border-box; }}
html, body {{ margin:0; padding:0; background:var(--bg); }}
body {{
  font-family:'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  color:var(--body); font-size:14px; line-height:1.65;
  max-width:1180px; margin:0 auto; padding:0 40px 60px;
}}
a {{ color:var(--orange); text-decoration:none; }}
a:hover {{ text-decoration:underline; }}

.masthead {{
  border-top:6px solid var(--black);
  padding:28px 0 22px;
  display:flex; justify-content:space-between; align-items:flex-start;
  border-bottom:1px solid var(--brd);
  margin-bottom:32px;
}}
.brand-block .brand {{
  font-family:'Syne', sans-serif; font-weight:800; font-size:24px;
  color:var(--black); letter-spacing:-0.01em; text-transform:uppercase;
  line-height:1.1;
}}
.brand-block .brand em {{ color:var(--orange); font-style:normal; font-weight:800; }}
.brand-block .tagline {{
  font-family:'DM Mono', monospace; font-size:10px; font-weight:600;
  color:var(--muted); text-transform:uppercase; letter-spacing:0.14em;
  margin-top:8px;
}}
.mast-right {{ text-align:right; }}
.report-label {{
  display:inline-block; background:var(--black); color:#fff;
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  padding:5px 11px; letter-spacing:0.12em; text-transform:uppercase;
  margin-bottom:10px;
}}
.report-meta {{
  font-family:'DM Mono', monospace; font-size:11px;
  color:var(--muted); line-height:1.8;
}}
.report-meta strong {{ color:var(--ink); font-weight:700; }}

.r-title {{
  font-family:'Syne', sans-serif; font-weight:800; font-size:38px;
  color:var(--black); letter-spacing:-0.02em; line-height:1.15;
  margin:2px 0 10px;
}}
.r-title .accent {{ color:var(--orange); }}
.r-kicker {{
  font-family:'Syne', sans-serif; font-weight:800; font-size:13px;
  color:var(--black); letter-spacing:0.08em; text-transform:uppercase;
  margin:8px 0 6px;
}}
.r-kicker-dot {{ color:var(--orange); font-style:normal; margin:0 2px; }}
.r-sub {{
  color:var(--muted); font-size:14px; margin-bottom:16px;
}}
.r-sub strong {{ color:var(--ink); font-weight:600; }}
.r-authority {{
  display:flex; align-items:center; gap:10px; flex-wrap:wrap;
  padding:10px 14px; background:var(--s1); border-left:3px solid var(--orange);
  font-family:'DM Mono', monospace; font-size:11px; color:var(--ink);
  margin-bottom:30px;
}}
.auth-label {{
  font-size:9px; font-weight:700; color:var(--orange);
  text-transform:uppercase; letter-spacing:0.14em;
  border-right:1px solid var(--brd); padding-right:10px;
}}

.kpi-strip {{
  display:grid; grid-template-columns:repeat(4, 1fr);
  gap:1px; background:var(--brd); border:1px solid var(--brd);
  margin-bottom:32px;
}}
.kpi {{ background:#fff; padding:22px 20px; }}
.kpi-label {{
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  color:var(--muted); text-transform:uppercase; letter-spacing:0.1em;
  margin-bottom:8px;
}}
.kpi-value {{
  font-family:'Syne', sans-serif; font-weight:800; font-size:42px;
  color:var(--black); line-height:1; letter-spacing:-0.02em;
}}
.kpi-value.red {{ color:var(--red); }}
.kpi-value.violet {{ color:var(--violet); }}
.kpi-value.orange {{ color:var(--orange); font-size:20px; line-height:1.2; }}
.kpi-delta {{
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  margin-top:10px; letter-spacing:0.04em;
}}
.kpi-top {{ font-size:11px; color:var(--muted); margin-top:10px; font-style:italic; }}

.sec-head {{
  display:flex; align-items:baseline; gap:14px;
  margin:40px 0 16px;
}}
.sec-num {{
  font-family:'DM Mono', monospace; font-size:11px; font-weight:700;
  color:var(--orange); letter-spacing:0.12em;
}}
.sec-title {{
  font-family:'Syne', sans-serif; font-weight:800; font-size:22px;
  color:var(--black); letter-spacing:-0.01em;
}}
.sec-rule {{ flex:1; height:1px; background:var(--brd); }}
.sec-caption {{ color:var(--muted); font-size:13px; margin:-4px 0 14px; }}
.sec-caption em {{ color:var(--ink); font-style:italic; }}

.analysis {{
  background:var(--s1); border-left:4px solid var(--orange);
  padding:26px 30px; margin-bottom:10px;
}}
.analysis p {{ margin:0 0 14px; font-size:14.5px; line-height:1.75; }}
.analysis p:last-child {{ margin-bottom:0; }}

table.data {{
  width:100%; border-collapse:collapse; margin:0 0 10px;
  background:#fff; border:1px solid var(--brd);
  font-size:13px;
}}
table.data th {{
  background:var(--black); color:#fff;
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  text-transform:uppercase; letter-spacing:0.1em;
  padding:12px 12px; text-align:left; border-bottom:2px solid var(--orange);
}}
table.data td {{
  padding:14px 12px; border-bottom:1px solid var(--brd);
  vertical-align:top;
}}
table.data tr:last-child td {{ border-bottom:none; }}
table.data tr:nth-child(even) td {{ background:#fafbfc; }}
table.data td.num {{
  font-family:'DM Mono', monospace; font-weight:600; text-align:right;
  white-space:nowrap;
}}
table.data td.empty {{
  text-align:center; color:var(--muted); padding:28px; font-style:italic;
}}

/* Top 5 column sizing - keeps table within A4 and desktop viewport */
table.top5 {{ table-layout:fixed; width:100%; }}
table.top5 th:nth-child(1), table.top5 td:nth-child(1) {{ width:5%;  }}  /* # */
table.top5 th:nth-child(2), table.top5 td:nth-child(2) {{ width:9%;  }}  /* Date */
table.top5 th:nth-child(3), table.top5 td:nth-child(3) {{ width:19%; }}  /* Pathogen */
table.top5 th:nth-child(4), table.top5 td:nth-child(4) {{ width:18%; }}  /* Company */
table.top5 th:nth-child(5), table.top5 td:nth-child(5) {{ width:30%; }}  /* Product */
table.top5 th:nth-child(6), table.top5 td:nth-child(6) {{ width:19%; }}  /* Jurisdiction+Source */
table.top5 td {{ word-wrap:break-word; overflow-wrap:break-word; }}

.rank-num {{
  font-family:'Syne', sans-serif; font-weight:800; font-size:22px;
  color:var(--orange); text-align:center;
}}
.date-cell {{
  font-family:'DM Mono', monospace; font-size:11px; color:var(--muted);
}}
.path-dot {{
  display:inline-block; width:9px; height:9px; border-radius:50%;
  margin-right:7px; vertical-align:middle;
}}
.path-name {{ font-weight:600; color:var(--ink); }}
.co-cell strong {{ color:var(--black); font-weight:700; display:block; }}
.brand-sub {{ font-size:11px; color:var(--muted); margin-top:2px; font-style:italic; }}
.prod-cell {{ color:var(--body); }}
.juris-country {{ font-weight:600; color:var(--ink); }}
.src-sub {{
  font-family:'DM Mono', monospace; font-size:10px;
  color:var(--muted); margin-top:3px;
}}
.juris-link {{ margin-top:6px; }}
.chip-tier1 {{
  display:inline-block; background:var(--red); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:6px; letter-spacing:0.06em;
}}
.chip-tier2 {{
  display:inline-block; background:var(--amber); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:6px; letter-spacing:0.06em;
}}
.chip-outbreak {{
  display:inline-block; background:var(--violet); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:4px; letter-spacing:0.06em;
}}
.src-link {{
  font-family:'DM Mono', monospace; font-size:11px; font-weight:700;
  color:var(--orange); letter-spacing:0.02em;
}}
.src-na {{ color:var(--dim); font-family:'DM Mono', monospace; font-size:10px; font-style:italic; }}

.dist-grid {{
  display:grid; grid-template-columns:1fr 1fr; gap:24px; margin-bottom:10px;
}}
.dist-grid h3 {{
  font-family:'DM Mono', monospace; font-size:11px; color:var(--muted);
  text-transform:uppercase; letter-spacing:0.1em; margin:0 0 10px;
}}
.bar-track {{
  width:100%; height:8px; background:var(--s2);
  border-radius:1px; overflow:hidden;
}}
.bar-fill {{ height:100%; }}

.cta-box {{
  margin:40px 0 30px;
  padding:26px 30px;
  background:var(--black); color:#fff;
  display:flex; justify-content:space-between; align-items:center;
  flex-wrap:wrap; gap:18px;
}}
.cta-text {{ flex:1; min-width:280px; }}
.cta-text h3 {{
  font-family:'Syne', sans-serif; font-weight:800; font-size:20px;
  margin:0 0 6px; color:#fff; letter-spacing:-0.01em;
}}
.cta-text p {{ margin:0; color:#d1d5db; font-size:13px; }}
.cta-btn {{
  background:var(--orange); color:#fff; font-family:'DM Mono', monospace;
  font-size:11px; font-weight:700; padding:14px 22px;
  text-transform:uppercase; letter-spacing:0.1em;
  border:none; cursor:pointer; white-space:nowrap;
}}
.cta-btn:hover {{ background:#d35416; text-decoration:none; color:#fff; }}

.meth {{
  background:var(--s1); border:1px solid var(--brd);
  padding:22px 26px; margin-bottom:24px; font-size:13px;
  color:var(--body);
}}
.meth strong {{ color:var(--black); }}
.meth p {{ margin:0 0 10px; }}
.meth p:last-child {{ margin-bottom:0; }}

.footer {{
  margin-top:50px; padding-top:26px; border-top:2px solid var(--black);
  display:flex; justify-content:space-between; align-items:flex-start;
  flex-wrap:wrap; gap:20px; font-size:12px;
}}
.foot-brand {{
  font-family:'Syne', sans-serif; font-weight:800; font-size:15px;
  color:var(--black); text-transform:uppercase; letter-spacing:0.02em;
}}
.foot-brand em {{ color:var(--orange); font-style:normal; }}
.foot-meta {{
  font-family:'DM Mono', monospace; font-size:10px;
  color:var(--muted); line-height:1.8; margin-top:6px;
}}
.foot-legal {{
  font-size:11px; color:var(--muted); max-width:440px;
  text-align:right; line-height:1.6;
}}

@media print {{
  /* Running footer on every printed page: process-authority attribution
     anchors the AFTS differentiator visually throughout the document. */
  @page {{
    size: A4;
    margin: 14mm 14mm 18mm 14mm;
    @bottom-left {{
      content: "AFTS · Process Validation Intelligence · under AFTS process authority";
      font-family: 'DM Mono', monospace; font-size: 8pt; color: #6b7280;
      letter-spacing: 0.04em;
    }}
    @bottom-right {{
      content: "Page " counter(page) " / " counter(pages);
      font-family: 'DM Mono', monospace; font-size: 8pt; color: #6b7280;
      letter-spacing: 0.04em;
    }}
  }}
  body {{ max-width:none; padding:0; margin:0; font-size:11px; }}
  .cta-box {{ display:none; }}

  /* Lock print-mode layout: even if the browser's print page is narrow,
     these must not collapse into mobile responsive layouts. */
  .masthead {{ flex-direction:row !important; }}
  .mast-right {{ text-align:right !important; }}
  .kpi-strip {{ grid-template-columns:repeat(4, 1fr) !important; }}
  .dist-grid {{ display:block !important; grid-template-columns:1fr !important; gap:0 !important; }}
  .dist-grid > div {{ width:100% !important; display:block !important; }}
  .dist-grid > div:nth-child(2) {{ page-break-before:always !important; break-before:page !important; margin-top:0 !important; }}

  /* Page 1 compression: tighten the above-the-fold so the first Intelligence
     Analysis paragraph opens on page 1 rather than orphaning the heading. */
  .masthead {{ border-top-width:4px; padding:18px 0 12px; margin-bottom:22px; }}
  .brand-block .brand {{ font-size:18px; }}
  .brand-block .tagline {{ font-size:10px; margin-top:5px; letter-spacing:0.12em; }}
  .report-label {{ font-size:9px; padding:4px 10px; margin-bottom:8px; }}
  .report-meta {{ font-size:10px; line-height:1.7; }}
  .r-kicker {{ font-size:12px; margin:6px 0 5px; letter-spacing:0.07em; }}
  .r-title {{ font-size:26px; margin:2px 0 8px; }}
  .r-sub {{ font-size:13px; margin-bottom:12px; line-height:1.55; }}
  .r-authority {{ padding:9px 12px; font-size:11px; margin-bottom:22px; }}
  .auth-label {{ font-size:8px; padding-right:9px; }}
  .kpi-strip {{ margin-bottom:24px; }}
  .kpi {{ padding:16px 14px; }}
  .kpi-label {{ font-size:9px; margin-bottom:6px; }}
  .kpi-value {{ font-size:28px; }}
  .kpi-value.orange {{ font-size:18px; }}
  .kpi-delta {{ font-size:9px; margin-top:7px; }}
  .kpi-top {{ font-size:10px; margin-top:7px; }}
  .sec-head {{ margin:28px 0 12px; page-break-after:avoid; break-after:avoid; }}
  .sec-num {{ font-size:10px; }}
  .sec-title {{ font-size:20px; white-space:nowrap; }}
  .analysis {{ padding:22px 26px; }}
  .analysis p {{ font-size:13px; margin:0 0 12px; line-height:1.7; }}

  table.data th {{ background:var(--black) !important; color:#fff !important; -webkit-print-color-adjust:exact; print-color-adjust:exact; }}
  /* Prevent any table row from splitting across a page break */
  table.data tr {{ page-break-inside:avoid; break-inside:avoid; }}
  .analysis {{ border-left-width:3px; }}
  /* Top-5 print tightening - fit all 6 columns on A4 */
  table.top5 {{ font-size:9px; page-break-inside:avoid; }}
  table.top5 th {{ padding:6px 5px; font-size:8px; }}
  table.top5 td {{ padding:6px 5px; line-height:1.35; }}
  table.top5 tr {{ page-break-inside:avoid; }}
  table.top5 .rank-num {{ font-size:14px; }}
  table.top5 .path-name {{ font-size:9px; }}
  table.top5 .date-cell {{ font-size:8px; }}
  table.top5 .prod-cell {{ font-size:9px; line-height:1.35; }}
  table.top5 .co-cell strong {{ font-size:9px; }}
  table.top5 .juris-country {{ font-size:9px; }}
  table.top5 .brand-sub, table.top5 .src-sub {{ font-size:8px; margin-top:1px; }}
  table.top5 .chip-tier1, table.top5 .chip-tier2, table.top5 .chip-outbreak {{ font-size:7px; padding:1px 3px; margin-left:3px; }}
  table.top5 .src-link {{ font-size:8px; }}
  table.top5 .juris-link {{ margin-top:3px; }}
  /* Tighten the caption above the Top 5 so more room for rows */
  .sec-caption {{ font-size:10px; margin:-2px 0 8px; }}
  /* Force section boundaries on page breaks for clean 4-page distribution:
     P1 = masthead + KPI + § 01 Analysis
     P2 = § 02 Top 5
     P3 = § 03 Distribution
     P4 = § 04 Methodology + Footer */
  section.page-break, div.page-break {{ page-break-before:always; }}
  .sec-head.break-before {{ page-break-before:always; break-before:page; }}

  /* Footer: switch from flex to a clean vertical stack for print.
     WeasyPrint and some browser print engines overlap the two halves
     when flex wraps at narrow widths - block layout avoids it entirely.
     page-break-inside: avoid keeps brand block + disclaimer together on one page. */
  .footer {{
    display:block !important;
    margin-top:26px;
    page-break-inside:avoid;
    break-inside:avoid;
  }}
  .footer > div {{ display:block !important; width:auto !important; }}
  .footer > div:first-child {{ margin-bottom:12px; }}
  .foot-legal {{
    text-align:left !important;
    max-width:none !important;
    padding-top:10px;
    border-top:1px solid var(--brd);
  }}
  /* Keep the methodology section with its adjacent section intact */
  .meth {{ page-break-inside:avoid; break-inside:avoid; }}
}}

@media screen and (max-width:900px) {{
  body {{ padding:0 20px 40px; }}
  .kpi-strip {{ grid-template-columns:repeat(2,1fr); }}
  .dist-grid {{ grid-template-columns:1fr; }}
  .masthead {{ flex-direction:column; gap:16px; }}
  .mast-right {{ text-align:left; }}
  .r-title {{ font-size:28px; }}
}}

/* Mobile Top-5: switch from a 6-column table to stacked cards.
   On phones, a horizontal table would either scroll sideways (bad UX) or
   compress columns into unreadable widths. Instead, each row becomes a
   card with labeled fields - all data visible, no horizontal scroll. */
@media screen and (max-width:700px) {{
  table.top5, table.top5 thead, table.top5 tbody, table.top5 tr, table.top5 td {{
    display:block; width:auto !important;
  }}
  /* Kill all fixed column widths - they would make card-mode cells unreadably narrow */
  table.top5 th:nth-child(1), table.top5 td:nth-child(1),
  table.top5 th:nth-child(2), table.top5 td:nth-child(2),
  table.top5 th:nth-child(3), table.top5 td:nth-child(3),
  table.top5 th:nth-child(4), table.top5 td:nth-child(4),
  table.top5 th:nth-child(5), table.top5 td:nth-child(5),
  table.top5 th:nth-child(6), table.top5 td:nth-child(6) {{
    width:auto !important;
  }}
  table.top5 {{ border:none; table-layout:auto !important; }}
  table.top5 thead {{ display:none; }}
  table.top5 tr {{
    border:1px solid var(--brd); border-left:4px solid var(--orange);
    background:#fff; margin-bottom:12px; padding:8px 4px;
    position:relative;
  }}
  table.top5 tr:nth-child(even) td {{ background:transparent; }}
  table.top5 td {{
    border:none !important; padding:7px 14px 7px 108px !important;
    position:relative; min-height:28px;
    word-wrap:normal; overflow-wrap:normal;
  }}
  table.top5 td::before {{
    content:attr(data-label);
    position:absolute; left:14px; top:7px; width:88px;
    font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
    color:var(--muted); text-transform:uppercase; letter-spacing:0.08em;
  }}
  /* Rank number sits in top-right corner as an orange badge */
  table.top5 .rank-num {{
    position:absolute; top:8px; right:14px; padding:0 !important;
    font-size:28px; min-height:0; text-align:right;
  }}
  table.top5 .rank-num::before {{ display:none; }}
  table.top5 .date-cell {{ font-size:11px; }}
  table.top5 .path-name {{ font-size:13px; }}
  table.top5 .co-cell strong {{ font-size:13px; }}
  table.top5 .prod-cell {{ line-height:1.45; font-size:13px; }}
  table.top5 .juris-country {{ font-size:13px; }}
  table.top5 .juris-link {{ margin-top:6px; }}
}}

@media screen and (max-width:480px) {{
  body {{ padding:0 14px 30px; }}
  .kpi-strip {{ grid-template-columns:1fr 1fr; }}
  .kpi {{ padding:16px 14px; }}
  .kpi-value {{ font-size:28px; }}
  .r-title {{ font-size:24px; }}
  .analysis {{ padding:18px 20px; }}
  .analysis p {{ font-size:13px; }}
}}
</style>
</head>
<body>

<header class="masthead">
  <div class="brand-block">
    <div class="brand">Advanced Food-Tech Solutions <em>&middot;</em> AFTS</div>
    <div class="tagline">Food Safety Intelligence System &middot; Weekly Briefing</div>
  </div>
  <div class="mast-right">
    <div class="report-label">Subscribers Edition</div>
    <div class="report-meta">
      <strong>ISSUE</strong> &middot; Week {wnum:02d}, {year}<br>
      <strong>PERIOD</strong> &middot; {week_start.strftime('%d %b')} &ndash; {week_end.strftime('%d %b %Y')}<br>
      <strong>PUBLISHED</strong> &middot; {generated}
    </div>
  </div>
</header>

<div class="r-kicker">AFTS <span class="r-kicker-dot">&middot;</span> Process Validation Intelligence</div>
<h1 class="r-title">Pathogen Surveillance <span class="accent">&middot;</span> Week {wnum:02d}</h1>
<p class="r-sub">
  AI-powered analysis of <strong>{stats['total']}</strong> regulatory recall actions across
  <strong>{len(stats['country_counts'])}</strong> jurisdictions, aggregated from 66 primary sources
  monitored continuously by the AFTS intelligence platform.
</p>
<div class="r-authority">
  <span class="auth-label">Process Authority</span>
  Food Process Engineering &middot; Thermal Processing &middot; Regulatory Compliance
</div>

<div class="kpi-strip">
  <div class="kpi">
    <div class="kpi-label">Total Recalls</div>
    <div class="kpi-value">{stats['total']}</div>
    {delta_html}
  </div>
  <div class="kpi">
    <div class="kpi-label">Tier-1 Critical</div>
    <div class="kpi-value red">{stats['tier1']}</div>
    <div class="kpi-delta" style="color:var(--muted)">Immediate public-health risk</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Active Outbreaks</div>
    <div class="kpi-value violet">{stats['outbreaks']}</div>
    <div class="kpi-delta" style="color:var(--muted)">Confirmed cluster events</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Leading Pathogen</div>
    <div class="kpi-value orange">{escape(stats['top_pathogen'][0])}</div>
    <div class="kpi-top">{stats['top_pathogen'][1]} cases &middot; {top_pathogen_pct}% of total</div>
  </div>
</div>

<div class="sec-head">
  <span class="sec-num">&sect; 01</span>
  <h2 class="sec-title">Intelligence Analysis</h2>
  <span class="sec-rule"></span>
</div>
<div class="analysis">
  <p>{escape(paragraphs[0])}</p>
  <p>{escape(paragraphs[1])}</p>
  <p>{escape(paragraphs[2])}</p>
</div>

<div class="sec-head">
  <span class="sec-num">&sect; 02</span>
  <h2 class="sec-title">Top 5 Critical Threats</h2>
  <span class="sec-rule"></span>
</div>
<p class="sec-caption">
  Ranked by pathogen severity (<em>C. botulinum</em> &rarr; <em>Listeria</em> &rarr; STEC &rarr; <em>Salmonella</em>), outbreak status, and tier classification.
  Each row links to the originating regulatory notice.
</p>
<table class="data top5">
  <thead>
    <tr>
      <th>#</th><th>Date</th><th>Pathogen</th><th>Company / Brand</th><th>Product</th><th>Jurisdiction &amp; Source</th>
    </tr>
  </thead>
  <tbody>
    {top5_rows}
  </tbody>
</table>

<div class="sec-head">
  <span class="sec-num">&sect; 03</span>
  <h2 class="sec-title">Distribution Analysis</h2>
  <span class="sec-rule"></span>
</div>
<div class="dist-grid">
  <div>
    <h3>Pathogen Profile</h3>
    <table class="data">
      <thead>
        <tr><th>Pathogen</th><th class="num">Cases</th><th class="num">%</th><th>Share</th></tr>
      </thead>
      <tbody>
        {path_rows}
      </tbody>
    </table>
  </div>
  <div>
    <h3>Geographic &middot; Regulatory</h3>
    <table class="data">
      <thead>
        <tr><th>Country</th><th>Authority</th><th class="num">Cases</th><th class="num">%</th></tr>
      </thead>
      <tbody>
        {country_rows}
      </tbody>
    </table>
  </div>
</div>

<div class="cta-box">
  <div class="cta-text">
    <h3>Live Dashboard &middot; Full Dataset Access</h3>
    <p>Filter by pathogen, country, tier, and source. Download the accumulative XLSX dataset. Set custom alerts.</p>
  </div>
  <a class="cta-btn" href="https://www.advfood.tech/food-safety-intelligence" target="_blank" rel="noopener">Access Portal &rarr;</a>
</div>

<div class="sec-head">
  <span class="sec-num">&sect; 04</span>
  <h2 class="sec-title">Methodology &amp; Sources</h2>
  <span class="sec-rule"></span>
</div>
<div class="meth">
  <p>
    <strong>Process authority.</strong> Analytical frameworks, severity rubrics, pathogen
    classification, and the engineering interpretation of each recall are developed under the
    process authority of AFTS, drawing on in-house expertise in food process engineering,
    thermal processing, and regulatory compliance. Every view is grounded in validated
    process engineering: thermal processing (21 CFR 113/114), pasteurisation (PMO), aseptic
    and UHT, hold-tube and F-value lethality, and HACCP. This is what the AFTS platform brings
    that pure data feeds do not &mdash; data under engineering authority.
  </p>
  <p>
    <strong>Data &amp; AI pipeline.</strong> The system aggregates regulatory recall notices from
    66 primary sources across 60+ countries (FDA, USDA FSIS, RASFF, FSA, FSANZ, CFIA, RappelConso,
    BVL, AESAN, EFET, and national authorities) and processes each record through Gemini
    (extraction), OpenAI GPT (normalisation), and Claude (Tier-1 validation). Records are
    de-duplicated and harmonised into the accumulative dataset.
  </p>
  <p>
    <strong>This briefing.</strong> Statistical analysis filters the accumulative dataset to the
    reporting week ({week_start.strftime('%d %b')} &ndash; {week_end.strftime('%d %b %Y')}).
    AI-generated narrative is produced against AFTS process-authority prompts and edited for
    publication. Figures and pathogen names are preserved verbatim from source data.
  </p>
</div>

<footer class="footer">
  <div>
    <div class="foot-brand">Advanced Food-Tech Solutions <em>&middot;</em> AFTS</div>
    <div class="foot-meta">
      Food Process Engineering &middot; Thermal Processing &middot; Regulatory Compliance<br>
      advfood.tech &middot; info@advfood.tech &middot; Athens, Greece<br>
      &copy; {year} Advanced Food Tech Solutions
    </div>
  </div>
  <div class="foot-legal">
    This briefing is provided for informational purposes only and does not constitute regulatory, legal,
    or medical advice. Subscribers should verify recall status with the originating regulatory authority
    before taking action. Next issue: {(week_end + timedelta(days=7)).strftime('%A, %d %b %Y')}.
  </div>
</footer>

</body>
</html>"""

    return html, stats

# --- Dashboard update (accumulative) ----------------------------------------
def update_dashboard_data(week_end: date, stats: Dict[str, Any], index_path: Path):
    """Append/replace this week's entry in the embedded reports array; preserve history."""
    if not index_path.exists():
        log.warning("index.html not found at %s; skipping dashboard update", index_path)
        return

    wnum = week_end.isocalendar()[1]
    year = week_end.year
    week_start = week_end - timedelta(days=6)

    entry = {
        "filename": f"{year}-W{wnum:02d}.html",
        "week_num": wnum,
        "year": year,
        "week_start": week_start.strftime('%Y-%m-%d'),
        "week_end":   week_end.strftime('%Y-%m-%d'),
        "generated":  datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "total":      stats['total'],
        "tier1":      stats['tier1'],
        "outbreaks":  stats['outbreaks'],
        "top_pathogen": stats['top_pathogen'][0] if stats['top_pathogen'] else None,
        "summary": (f"Week {wnum}: {stats['total']} recalls, "
                    f"{stats['tier1']} Tier-1, {stats['outbreaks']} outbreak(s). "
                    f"Leading pathogen: {stats['top_pathogen'][0] if stats['top_pathogen'] else 'n/a'}."),
    }

    content = index_path.read_text(encoding='utf-8')

    # Find the reports array with a balanced match across newlines.
    m = re.search(r'const\s+reports\s*=\s*(\[.*?\]);', content, flags=re.DOTALL)
    if not m:
        log.warning("Could not locate `const reports = [...]` in index.html")
        return

    try:
        existing = json.loads(m.group(1))
        if not isinstance(existing, list):
            existing = []
    except json.JSONDecodeError:
        log.warning("Existing reports array not valid JSON; starting fresh")
        existing = []

    # Replace by filename if it exists; otherwise insert
    # ALSO prune any future-dated entries that slipped in from manual test runs.
    # A weekly briefing is "published" the moment its week_end has been reached —
    # before that, the report file may exist in the repo (so the Friday cron has
    # something to regenerate from) but it must not appear in the public list.
    today_iso = date.today().isoformat()
    existing = [
        r for r in existing
        if r.get("filename") != entry["filename"]
        and (r.get("week_end", "") or "") <= today_iso
    ]
    if entry["week_end"] <= today_iso:
        existing.insert(0, entry)
        log.info("Dashboard updated: %d total reports (published)", len(existing))
    else:
        log.info("Dashboard: entry %s is future-dated (%s > %s); "
                 "report file written but NOT listed publicly yet",
                 entry["filename"], entry["week_end"], today_iso)
    existing.sort(key=lambda r: r.get("week_end", ""), reverse=True)

    new_block = f'const reports = {json.dumps(existing, indent=4)};'
    updated = content[:m.start()] + new_block + content[m.end():]
    index_path.write_text(updated, encoding='utf-8')


# --- Summary JSON for the subscriber mailer ---------------------------------
# The Google Apps Script subscriber mailer fetches this file every Friday
# and builds the email body from it. Keeping the email data in a small, stable
# JSON contract decouples the email format from the HTML report format.
def write_weekly_summary_json(week_end: date, stats: Dict[str, Any],
                              site_base_url: str, out_path: Path):
    wnum = week_end.isocalendar()[1]
    year = week_end.year
    week_start = week_end - timedelta(days=6)

    # Top 5 incidents, stripped down to email-relevant fields
    top5_out = []
    for i, r in enumerate(stats.get("_top5", []), 1):
        _, canon = severity_score(r.get("Pathogen") or "")
        url = (r.get("URL") or "").strip()
        good_url = is_report_grade_url(url)
        top5_out.append({
            "rank": i,
            "date": fmt_date(r.get("Date")),
            "pathogen": canon,
            "pathogen_raw": r.get("Pathogen") or "",
            "tier": safe_int(r.get("Tier"), 3),
            "outbreak": bool(safe_int(r.get("Outbreak"), 0)),
            "company": (r.get("Company") or "")[:80],
            "brand":   (r.get("Brand") or "")[:60],
            "product": (r.get("Product") or "")[:140],
            "country": r.get("Country") or "",
            "source":  (r.get("Source") or "").strip(),
            "url": url if good_url else "",
        })

    top_pathogen_name, top_pathogen_count = stats.get("top_pathogen", ("-", 0))
    total_safe = stats["total"] or 1
    site_base = site_base_url.rstrip("/")

    summary = {
        "filename": f"{year}-W{wnum:02d}.html",
        "report_url":   f"{site_base}/{year}-W{wnum:02d}.html",
        "dashboard_url": f"{site_base}/",
        "week_num": wnum,
        "year": year,
        "week_start": week_start.strftime("%Y-%m-%d"),
        "week_end":   week_end.strftime("%Y-%m-%d"),
        "week_start_display": week_start.strftime("%d %b"),
        "week_end_display":   week_end.strftime("%d %b %Y"),
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
        "ai_lead_paragraph": stats.get("_first_paragraph", ""),
        "top_threats": top5_out,
        "country_count": len(stats.get("country_counts", [])),
    }

    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Summary JSON written: %s", out_path)

# --- Main -------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="AFTS weekly food safety intelligence briefing")
    ap.add_argument("--week-end", required=True, help="Friday date YYYY-MM-DD")
    ap.add_argument("--xlsx", default=str(ROOT / "data" / "recalls.xlsx"))
    ap.add_argument("--output", default=None, help="Output HTML path (default: <year>-W<week>.html in repo root)")
    ap.add_argument("--index", default=str(ROOT / "index.html"))
    ap.add_argument("--site-url",
                    default="https://gstoforos.github.io/Food-Safety-Intelligence-System/docs",
                    help="Public base URL where the docs/ folder is served (used in email report_url)")
    ap.add_argument("--summary-json",
                    default=str(ROOT / "data" / "weekly-summary-latest.json"),
                    help="Path for the companion JSON summary (consumed by the subscriber mailer)")
    args = ap.parse_args()

    try:
        week_end = datetime.strptime(args.week_end, "%Y-%m-%d").date()
    except ValueError:
        log.error("Invalid --week-end: %s (expected YYYY-MM-DD)", args.week_end)
        return 2

    week_start = week_end - timedelta(days=6)
    prev_end = week_end - timedelta(days=7)

    log.info("AFTS weekly report | week %s -> %s (W%02d, %d)",
             week_start, week_end, week_end.isocalendar()[1], week_end.year)

    xlsx_path = Path(args.xlsx)
    all_recalls = load_recalls(xlsx_path)
    log.info("Loaded %d recalls from %s", len(all_recalls), xlsx_path)

    if not all_recalls:
        log.error("No recalls loaded; aborting to avoid empty report.")
        return 3

    week_recalls = filter_week(all_recalls, week_end)
    prev_recalls = filter_week(all_recalls, prev_end)
    log.info("This week: %d | prior week: %d", len(week_recalls), len(prev_recalls))

    html, stats = build_html(week_end, week_recalls, prev_recalls)

    # FIX: compute filename here from week_end (NOT inside build_html scope)
    wnum = week_end.isocalendar()[1]
    year = week_end.year
    out_path = Path(args.output) if args.output else (ROOT / f"{year}-W{wnum:02d}.html")

    out_path.write_text(html, encoding='utf-8')
    log.info("Report written: %s (%d bytes)", out_path, len(html))

    update_dashboard_data(week_end, stats, Path(args.index))

    # Emit the companion JSON summary ONLY for weeks that have been published
    # (week_end <= today). This prevents the Apps Script mailer from sending an
    # email for a report that isn't live yet.
    if week_end <= date.today():
        summary_path = Path(args.summary_json)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        write_weekly_summary_json(week_end, stats, args.site_url, summary_path)
    else:
        log.info("Summary JSON: skipping (week_end %s is in the future)", week_end)

    log.info("Done | Total=%d | Tier1=%d | Outbreaks=%d | Top=%s",
             stats['total'], stats['tier1'], stats['outbreaks'],
             stats['top_pathogen'][0] if stats['top_pathogen'] else '-')

    return 0

if __name__ == "__main__":
    sys.exit(main())
