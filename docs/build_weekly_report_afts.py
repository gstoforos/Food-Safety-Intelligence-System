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

    prompt = f"""You are the lead food safety analyst at Advanced Food-Tech Solutions (AFTS), a specialised food process engineering firm. Write the weekly pathogen surveillance briefing for Fortune-500 food safety executives, QA directors, and regulatory affairs teams.

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

Write exactly THREE paragraphs, each 3-4 sentences, in professional executive-briefing tone. NO headers, NO bullet points, NO emoji, NO markdown. Use UK/US business English. Reference specific numbers and named pathogens.

Paragraph 1 - EXECUTIVE SUMMARY: Frame the week's pathogen activity, quantify the risk posture, note the week-over-week trend.

Paragraph 2 - PATHOGEN RISK FOCUS: Analyse the dominant pathogen's implications for thermal processing, supply-chain hygiene, or sanitation controls. Name specific food categories or process vulnerabilities where relevant (aseptic/UHT, RTE, fresh produce, dairy, deli).

Paragraph 3 - GEOGRAPHIC & REGULATORY ASSESSMENT: Interpret the geographic distribution, note which regulatory regimes are most active (RASFF, FDA, CFIA, FSA, etc.), and close with a concrete recommendation for food manufacturers.

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

    p1 = (f"This week produced {total} pathogen-related recall incidents across the AFTS monitoring network, "
          f"with {tier1} classified as Tier-1 and {outbreaks} confirmed outbreak event(s). "
          f"The overall risk posture remains elevated, driven primarily by {top_pathogen} activity. "
          f"Food manufacturers should treat the week's volume as indicative of sustained surveillance pressure from regulators.")
    p2 = (f"{top_pathogen} accounted for {count} of {total} incidents ({pct}%), consistent with its status as a "
          f"persistent risk in ready-to-eat, deli, dairy, and fresh-produce categories. Sanitation programmes targeting "
          f"post-process contamination, as well as validated lethality steps (F-value, pasteurisation schedules), warrant "
          f"review this week. Facilities operating under FDA 21 CFR 113/114 or PMO should confirm compliance margins.")
    p3 = (f"Regulatory activity spanned multiple jurisdictions, indicating coordinated enforcement across RASFF, FDA, CFIA, "
          f"and FSA channels. Exporters should anticipate continued inspection intensity and prepare documentation packages "
          f"for rapid response. AFTS recommends reviewing supplier verification protocols and confirming process-deviation "
          f"procedures before the next production cycle.")
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
    company = (r.get("Company") or "-")[:60]
    brand = (r.get("Brand") or "").strip()
    product = (r.get("Product") or "-")[:90]
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
      <td class="rank-num">{i}</td>
      <td class="date-cell">{escape(date_str)}</td>
      <td>
        <span class="path-dot" style="background:{badge_color}"></span>
        <span class="path-name">{escape(canon)}</span>
        {tier_chip}{outbreak_chip}
      </td>
      <td class="co-cell"><strong>{escape(company)}</strong>{brand_line}</td>
      <td class="prod-cell">{escape(product)}</td>
      <td>{escape(country)}<div class="src-sub">{escape(source)}</div></td>
      <td class="link-cell">{link_cell}</td>
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

    # Top 5 rows
    if top5:
        top5_rows = "".join(render_top5_row(i, r) for i, r in enumerate(top5, 1))
    else:
        top5_rows = '<tr><td colspan="7" class="empty">No recalls recorded this reporting period.</td></tr>'

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
  margin:8px 0 10px;
}}
.r-title .accent {{ color:var(--orange); }}
.r-sub {{
  color:var(--muted); font-size:14px; margin-bottom:30px;
  padding-bottom:22px; border-bottom:1px solid var(--brd);
}}
.r-sub strong {{ color:var(--ink); font-weight:600; }}

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

.rank-num {{
  font-family:'Syne', sans-serif; font-weight:800; font-size:22px;
  color:var(--orange); text-align:center; width:48px;
}}
.date-cell {{
  font-family:'DM Mono', monospace; font-size:11px; color:var(--muted);
  white-space:nowrap; width:96px;
}}
.path-dot {{
  display:inline-block; width:9px; height:9px; border-radius:50%;
  margin-right:7px; vertical-align:middle;
}}
.path-name {{ font-weight:600; color:var(--ink); }}
.co-cell strong {{ color:var(--black); font-weight:700; display:block; }}
.brand-sub {{ font-size:11px; color:var(--muted); margin-top:2px; font-style:italic; }}
.prod-cell {{ color:var(--body); max-width:260px; }}
.src-sub {{
  font-family:'DM Mono', monospace; font-size:10px;
  color:var(--muted); margin-top:3px;
}}
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
.link-cell {{ white-space:nowrap; }}
.src-link {{
  font-family:'DM Mono', monospace; font-size:11px; font-weight:700;
  color:var(--orange); letter-spacing:0.02em;
}}
.src-na {{ color:var(--dim); font-family:'DM Mono', monospace; font-size:11px; }}

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
  body {{ max-width:none; padding:20px; font-size:11px; }}
  .cta-box {{ display:none; }}
  .masthead {{ border-top-width:4px; }}
  .kpi-value {{ font-size:32px; }}
  .r-title {{ font-size:28px; }}
  table.data th {{ background:var(--black) !important; color:#fff !important; -webkit-print-color-adjust:exact; print-color-adjust:exact; }}
  .analysis {{ border-left-width:3px; }}
}}

@media (max-width:900px) {{
  body {{ padding:0 20px 40px; }}
  .kpi-strip {{ grid-template-columns:repeat(2,1fr); }}
  .dist-grid {{ grid-template-columns:1fr; }}
  .masthead {{ flex-direction:column; gap:16px; }}
  .mast-right {{ text-align:left; }}
  .r-title {{ font-size:28px; }}
  table.data {{ font-size:11px; }}
  table.data th, table.data td {{ padding:8px; }}
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

<h1 class="r-title">Pathogen Surveillance <span class="accent">&middot;</span> Week {wnum:02d}</h1>
<p class="r-sub">
  AI-powered analysis of <strong>{stats['total']}</strong> regulatory recall actions across
  <strong>{len(stats['country_counts'])}</strong> jurisdictions, aggregated from 66 primary sources
  monitored continuously by the AFTS intelligence platform.
</p>

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
<table class="data">
  <thead>
    <tr>
      <th>#</th><th>Date</th><th>Pathogen</th><th>Company / Brand</th><th>Product</th><th>Jurisdiction</th><th>Source</th>
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
    <strong>Data ingestion.</strong> The AFTS Food Safety Intelligence System aggregates regulatory recall notices
    from 66 primary sources covering 60+ countries, including FDA, USDA FSIS, EU RASFF, FSA (UK), FSANZ, CFIA,
    RappelConso, and national authorities across Europe, Asia-Pacific, and the Americas.
  </p>
  <p>
    <strong>AI enrichment.</strong> Each record is processed through a multi-stage pipeline: Gemini (initial extraction),
    OpenAI GPT (normalisation and quality control), and Claude (Tier-1 safety validation and severity tagging).
    Records are de-duplicated and harmonised before entering the accumulative dataset.
  </p>
  <p>
    <strong>This briefing.</strong> Statistical analysis draws from the accumulative <em>recalls.xlsx</em> dataset
    filtered to the reporting week ({week_start.strftime('%d %b')} &ndash; {week_end.strftime('%d %b %Y')}).
    Narrative analysis is generated by Claude Sonnet 4 and edited by GPT-4o-mini for publication. All figures
    and pathogen names are preserved verbatim from source data.
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

# --- Main -------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="AFTS weekly food safety intelligence briefing")
    ap.add_argument("--week-end", required=True, help="Friday date YYYY-MM-DD")
    ap.add_argument("--xlsx", default=str(ROOT / "data" / "recalls.xlsx"))
    ap.add_argument("--output", default=None, help="Output HTML path (default: <year>-W<week>.html in repo root)")
    ap.add_argument("--index", default=str(ROOT / "index.html"))
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

    log.info("Done | Total=%d | Tier1=%d | Outbreaks=%d | Top=%s",
             stats['total'], stats['tier1'], stats['outbreaks'],
             stats['top_pathogen'][0] if stats['top_pathogen'] else '-')

    return 0

if __name__ == "__main__":
    sys.exit(main())
