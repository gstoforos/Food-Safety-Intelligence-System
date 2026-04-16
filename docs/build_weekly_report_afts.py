"""
AFTS Food Safety Intelligence System — Weekly Report Generator
Generated every Friday 18:00 UTC.
Uses Claude AI + OpenAI pipeline for professional reports.
Output: reports/YYYY-WW.html + updates index.html embedded data
"""

import json, logging, os, requests, sys, argparse
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from collections import Counter
from typing import List, Dict, Any, Tuple
from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# AI API Configuration
CLAUDE_API_KEY = os.getenv('ANTHROPIC_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

def load_recalls(xlsx_path: Path) -> List[Dict[str, Any]]:
    """Load recalls from Excel file"""
    wb = load_workbook(xlsx_path, data_only=True)
    if "Recalls" not in wb.sheetnames:
        return []
    ws = wb["Recalls"]
    headers = [c.value for c in ws[1]]
    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        rec = {h: (v if v is not None else "") for h, v in zip(headers, row)}
        out.append(rec)
    return out

def filter_week(recalls: List[Dict], week_end: date) -> List[Dict]:
    """Filter to recalls dated between (week_end - 6 days) and week_end inclusive."""
    week_start = week_end - timedelta(days=6)
    out = []
    for r in recalls:
        d = r.get("Date", "")
        if not d:
            continue
        try:
            rd = datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        if week_start <= rd <= week_end:
            out.append(r)
    return out

def compute_stats(week_recalls: List[Dict], prev_week_recalls: List[Dict]) -> Dict[str, Any]:
    """Compute headline numbers and breakdowns."""
    def safe_int(v, default=0):
        try:
            return int(v)
        except (ValueError, TypeError):
            return default

    total = len(week_recalls)
    tier1 = sum(1 for r in week_recalls if safe_int(r.get("Tier")) == 1)
    outbreaks = sum(1 for r in week_recalls if safe_int(r.get("Outbreak")) == 1)

    pathogen_counts = Counter()
    for r in week_recalls:
        p = (r.get("Pathogen") or "").strip()
        if p:
            short = p.split("(")[0].strip()
            pathogen_counts[short] += 1

    country_counts = Counter()
    for r in week_recalls:
        c = r.get("Country", "") or "Unknown"
        country_counts[c] += 1

    return {
        "total": total,
        "tier1": tier1,
        "outbreaks": outbreaks,
        "top_pathogen": pathogen_counts.most_common(1)[0] if pathogen_counts else ("—", 0),
        "pathogen_counts": pathogen_counts.most_common(10),
        "country_counts": country_counts.most_common(10),
        "prev_total": len(prev_week_recalls),
    }

def rank_top_recalls(week_recalls: List[Dict], n: int = 5) -> List[Dict]:
    """Rank by pathogen severity: C. botulinum > Listeria > E. coli > Salmonella > others"""
    severity_order = {
        'Clostridium botulinum': 1,
        'Listeria monocytogenes': 2, 
        'E. coli': 3, 'STEC': 3,
        'Salmonella': 4,
        'Cereulide': 5,
    }
    
    def score(r):
        pathogen = (r.get("Pathogen") or "").lower()
        severity = 99  # default
        for key, val in severity_order.items():
            if key.lower() in pathogen:
                severity = val
                break
        
        tier = int(r.get("Tier", 2)) if str(r.get("Tier", "")).isdigit() else 2
        outbreak = int(r.get("Outbreak", 0)) if str(r.get("Outbreak", "")).isdigit() else 0
        
        return (
            severity,  # pathogen severity first
            -outbreak,  # outbreaks higher
            tier,  # tier 1 higher
        )
    
    return sorted(week_recalls, key=score)[:n]

def generate_report_with_claude(data: Dict[str, Any], week_recalls: List[Dict]) -> str:
    """Use Claude AI to generate comprehensive weekly report analysis"""
    
    prompt = f"""You are an AI food safety analyst for Advanced Food-Tech Solutions (AFTS). Generate a professional weekly pathogen surveillance analysis.

DATA SUMMARY:
- Total recalls this week: {data['total']}
- Tier-1 critical: {data['tier1']} 
- Active outbreaks: {data['outbreaks']}
- Top pathogen: {data['top_pathogen'][0]} ({data['top_pathogen'][1]} cases)

PATHOGEN DISTRIBUTION:
{dict(data['pathogen_counts'])}

COUNTRY DISTRIBUTION:
{dict(data['country_counts'])}

Generate a professional 3-paragraph analysis:
1. Executive Summary (current week overview)
2. Key Risk Factors (pathogen-specific threats)  
3. Geographic Assessment (distribution patterns)

Use professional business language. NO emojis. Focus on actionable intelligence for food safety executives.
Return only the content paragraphs, no headers or formatting."""

    try:
        response = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'Content-Type': 'application/json',
                'x-api-key': CLAUDE_API_KEY
            },
            json={
                'model': 'claude-sonnet-4-20250514',
                'max_tokens': 1000,
                'messages': [{'role': 'user', 'content': prompt}]
            }
        )
        
        if response.status_code == 200:
            return response.json()['content'][0]['text']
        else:
            log.error(f"Claude API error: {response.status_code}")
            return generate_fallback_analysis(data)
            
    except Exception as e:
        log.error(f"Error calling Claude API: {e}")
        return generate_fallback_analysis(data)

def generate_fallback_analysis(data: Dict[str, Any]) -> str:
    """Fallback analysis if AI APIs fail"""
    top_pathogen, count = data['top_pathogen']
    total = data['total']
    tier1 = data['tier1']
    outbreaks = data['outbreaks']
    
    return f"""This week demonstrated significant pathogen activity with {total} total recall incidents requiring regulatory attention. Of particular concern, {tier1} incidents were classified as Tier-1 critical, indicating immediate public health risk requiring swift containment measures.

{top_pathogen} emerged as the dominant pathogen threat with {count} documented cases, representing {round(count/total*100) if total > 0 else 0}% of total incidents. This concentration suggests potential systematic contamination issues requiring enhanced surveillance protocols across affected product categories and manufacturing facilities.

Geographic distribution analysis reveals regulatory activity spanning multiple jurisdictions, with {outbreaks} confirmed outbreak events documented during the surveillance period. The distributed nature of these incidents indicates the need for continued vigilance across global supply chains and enhanced coordination between international food safety authorities."""

def review_with_openai(report_content: str) -> str:
    """Use OpenAI for final quality check and enhancement"""
    
    prompt = f"""Review this food safety intelligence report for executive stakeholders. Ensure it's:
1. Professional and business-appropriate tone
2. Factually accurate and well-structured
3. Clear actionable insights
4. Free of grammatical errors
5. Appropriately urgent without being alarmist

REPORT TO REVIEW:
{report_content}

Return the polished version maintaining the same structure and key facts."""

    try:
        response = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {OPENAI_API_KEY}',
                'Content-Type': 'application/json'
            },
            json={
                'model': 'gpt-4o',
                'messages': [{'role': 'user', 'content': prompt}],
                'max_tokens': 1000
            }
        )
        
        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content']
        else:
            log.error(f"OpenAI API error: {response.status_code}")
            return report_content
            
    except Exception as e:
        log.error(f"Error calling OpenAI API: {e}")
        return report_content

def escape(s: Any) -> str:
    """HTML escape utility"""
    if s is None: return ""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))

def build_html(week_end: date, recalls: List[Dict], prev_week: List[Dict]) -> Tuple[str, Dict[str, Any]]:
    """Build professional AFTS HTML report"""
    
    stats = compute_stats(recalls, prev_week)
    week_start = week_end - timedelta(days=6)
    wnum = week_end.isocalendar()[1]
    year = week_end.year
    top = rank_top_recalls(recalls, n=5)
    
    # Generate AI content
    ai_analysis = generate_report_with_claude(stats, recalls)
    final_analysis = review_with_openai(ai_analysis)
    
    # Build top recalls table
    top_recalls_html = ""
    for i, r in enumerate(top, 1):
        tier = int(r.get("Tier", 2)) if str(r.get("Tier", "")).isdigit() else 2
        outbreak = int(r.get("Outbreak", 0)) if str(r.get("Outbreak", "")).isdigit() else 0
        severity_class = "critical" if "botulinum" in (r.get("Pathogen") or "").lower() else "tier1"
        outbreak_badge = '<span class="outbreak-badge">OUTBREAK</span>' if outbreak else ''
        
        top_recalls_html += f"""
        <tr class="{severity_class}">
            <td><strong>{i}</strong></td>
            <td class="pathogen"><strong>{escape(r.get('Pathogen', 'Unknown'))}</strong></td>
            <td>{escape((r.get('Company') or '')[:50])}{outbreak_badge}</td>
            <td class="product">{escape((r.get('Product') or '')[:60])}</td>
            <td>{escape(r.get('Country', ''))}</td>
        </tr>"""
    
    # Build pathogen distribution table
    pathogen_table_html = ""
    total = stats['total']
    for pathogen, count in stats['pathogen_counts']:
        pct = round((count/total)*100) if total > 0 else 0
        pathogen_table_html += f"""
        <tr>
            <td class="pathogen">{escape(pathogen)}</td>
            <td>{count}</td>
            <td>{pct}%</td>
            <td>Tier-1</td>
        </tr>"""
    
    # Build country distribution table
    country_table_html = ""
    for country, count in stats['country_counts']:
        pct = round((count/total)*100) if total > 0 else 0
        country_table_html += f"""
        <tr>
            <td>{escape(country)}</td>
            <td>Multiple Sources</td>
            <td>{count}</td>
            <td>{pct}%</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AFTS Weekly Pathogen Report - Week {wnum}</title>
<style>
body{{font-family:'Segoe UI',Arial,sans-serif;line-height:1.6;margin:30px;color:#333;background:#fff;}}
.header{{border-bottom:3px solid #333;margin-bottom:30px;padding-bottom:20px;}}
.logo{{font-size:28px;font-weight:800;color:#333;margin-bottom:5px;}}
.logo .green{{color:#333;}}
.subtitle{{color:#666;font-size:14px;margin-bottom:15px;}}
.contact{{font-size:12px;color:#666;}}
.contact a{{color:#333;text-decoration:none;}}
h1{{color:#333;font-size:24px;margin:30px 0 20px 0;border-bottom:2px solid #eee;padding-bottom:10px;}}
h2{{color:#333;font-size:18px;margin:25px 0 15px 0;}}
.stats-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:20px;margin:20px 0;}}
.stat-box{{background:#f8f9fa;padding:20px;text-align:center;border:1px solid #dee2e6;border-radius:8px;}}
.stat-number{{font-size:28px;font-weight:bold;color:#dc3545;margin-bottom:5px;}}
.stat-number.tier1{{color:#dc3545;}} .stat-number.outbreak{{color:#fd7e14;}}
.stat-label{{font-size:14px;color:#666;text-transform:uppercase;}}
table{{width:100%;border-collapse:collapse;margin:15px 0;font-size:13px;}}
th{{background:#f8f9fa;padding:12px 8px;border:1px solid #dee2e6;font-weight:600;text-align:left;}}
td{{padding:10px 8px;border:1px solid #dee2e6;}}
tr:nth-child(even){{background:#f9f9f9;}}
.pathogen{{font-weight:500;}}
.critical{{background:#ffebee;color:#c62828;}}
.tier1{{background:#ffeaa7;}}
.outbreak-badge{{background:rgba(232,96,26,.2);color:#E8601A;font-size:9px;padding:1px 5px;border-radius:2px;font-family:monospace;margin-left:4px;}}
.summary{{background:#e3f2fd;padding:20px;border-radius:8px;margin:20px 0;}}
.footer{{margin-top:40px;border-top:1px solid #eee;padding-top:20px;font-size:12px;color:#666;text-align:center;}}
.dashboard-link{{background:#333;color:#fff;padding:10px 20px;text-decoration:none;border-radius:5px;display:inline-block;margin:15px 0;}}
@media print{{body{{margin:15px;}} .dashboard-link{{background:#333 !important;}}}}
</style>
</head><body>

<div class="header">
  <div class="logo">Advanced Food-Tech Solutions <span class="green">AFTS</span></div>
  <div class="subtitle">Food Safety Intelligence System - Weekly Pathogen Surveillance Report</div>
  <div class="contact">
    <strong>Contact:</strong> info@advfood.tech | 
    <a href="https://www.advfood.tech">www.advfood.tech</a> | 
    <a href="https://advfood.tech/food-safety-intelligence">Dashboard Portal</a>
  </div>
</div>

<h1>Week {wnum} Pathogen Surveillance Report</h1>
<p><strong>Reporting Period:</strong> {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')} | <strong>Generated:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>

<div class="stats-grid">
  <div class="stat-box">
    <div class="stat-number">{stats['total']}</div>
    <div class="stat-label">Total Recalls</div>
  </div>
  <div class="stat-box">
    <div class="stat-number tier1">{stats['tier1']}</div>
    <div class="stat-label">Tier-1 Critical</div>
  </div>
  <div class="stat-box">
    <div class="stat-number outbreak">{stats['outbreaks']}</div>
    <div class="stat-label">Outbreak Events</div>
  </div>
</div>

<div class="summary">
  <h2>Intelligence Analysis</h2>
  <p>{final_analysis}</p>
</div>

<h2>Top 5 Critical Threats - Prioritized by Pathogen Severity</h2>
<table>
  <thead>
    <tr><th>Priority</th><th>Pathogen</th><th>Company</th><th>Product</th><th>Country</th></tr>
  </thead>
  <tbody>{top_recalls_html or '<tr><td colspan="5" style="text-align:center;color:#555;padding:20px;">No critical threats this week</td></tr>'}</tbody>
</table>

<h2>Current Pathogen Distribution</h2>
<table>
  <thead>
    <tr><th>Pathogen</th><th>Cases</th><th>% of Total</th><th>Classification</th></tr>
  </thead>
  <tbody>{pathogen_table_html or '<tr><td colspan="4" style="text-align:center;color:#555;padding:20px;">No pathogen data</td></tr>'}</tbody>
</table>

<h2>Geographic Distribution</h2>
<table>
  <thead>
    <tr><th>Country</th><th>Regulatory Authority</th><th>Cases</th><th>% of Total</th></tr>
  </thead>
  <tbody>{country_table_html or '<tr><td colspan="4" style="text-align:center;color:#555;padding:20px;">No geographic data</td></tr>'}</tbody>
</table>

<a href="https://advfood.tech/food-safety-intelligence" class="dashboard-link" target="_blank">
  Access Live Dashboard Portal
</a>

<div class="footer">
  <p><strong>Advanced Food-Tech Solutions (AFTS)</strong><br>
  AI-Powered Global Pathogen Recall Monitoring • 66 Sources • 60+ Countries<br>
  Report generated by FSIS Intelligence System | Next update: {(week_end + timedelta(days=7)).strftime('%A, %B %d, %Y')}</p>
</div>

</body></html>"""
    
    return html, stats

def update_dashboard_data(week_end: date, stats: Dict[str, Any]):
    """Update index.html embedded data to match the new weekly report"""
    
    wnum = week_end.isocalendar()[1]
    year = week_end.year
    week_start = week_end - timedelta(days=6)
    
    # Create new report entry
    report_entry = {
        "filename": f"{year}-W{wnum:02d}.html",
        "week_num": wnum,
        "year": year,
        "week_start": week_start.strftime('%Y-%m-%d'),
        "week_end": week_end.strftime('%Y-%m-%d'),
        "generated": datetime.utcnow().isoformat() + "Z",
        "total": stats['total'],
        "tier1": stats['tier1'],
        "outbreaks": stats['outbreaks'],
        "top_pathogen": stats['top_pathogen'][0] if stats['top_pathogen'] else None,
        "summary": f"Week {wnum} saw {stats['total']} pathogen recalls with {stats['top_pathogen'][0] if stats['top_pathogen'] else 'mixed pathogens'} as primary concern."
    }
    
    # Read and update index.html
    index_path = ROOT / "index.html"
    try:
        if index_path.exists():
            content = index_path.read_text()
            
            # Replace the embedded reports array
            import re
            pattern = r'const reports = \[([^\]]*)\];'
            replacement = f'const reports = {json.dumps([report_entry], indent=4)};'
            
            updated_content = re.sub(pattern, replacement, content, flags=re.DOTALL)
            
            index_path.write_text(updated_content)
            log.info("Dashboard embedded data updated successfully")
        else:
            log.warning("index.html not found, skipping dashboard update")
            
    except Exception as e:
        log.error(f"Error updating dashboard data: {e}")

def main():
    """Main CLI function"""
    ap = argparse.ArgumentParser(description="Build AFTS weekly food safety intelligence report")
    ap.add_argument("--week-end", required=True, help="Friday date YYYY-MM-DD")
    ap.add_argument("--xlsx", default=str(ROOT / "data" / "recalls.xlsx"))
    ap.add_argument("--output", default=None, help="Output HTML path (default: reports/<year>-W<num>.html)")
    args = ap.parse_args()

    week_end = datetime.strptime(args.week_end, "%Y-%m-%d").date()
    week_start = week_end - timedelta(days=6)
    prev_week_end = week_end - timedelta(days=7)

    log.info("Building AFTS report for week %s to %s", week_start, week_end)

    all_recalls = load_recalls(Path(args.xlsx))
    log.info("Loaded %d total recalls from %s", len(all_recalls), args.xlsx)

    week_recalls = filter_week(all_recalls, week_end)
    prev_recalls = filter_week(all_recalls, prev_week_end)
    log.info("Week %s: %d recalls", week_end, len(week_recalls))

    html, stats = build_html(week_end, week_recalls, prev_recalls)

    # Save report with original naming format
    output = Path(args.output) if args.output else Path(f"{year}-W{wnum:02d}.html")
    output.write_text(html, encoding="utf-8")
    log.info("Report written to %s (%d bytes)", output, len(html))

    # Update dashboard embedded data
    update_dashboard_data(week_end, stats)

    return 0

if __name__ == "__main__":
    sys.exit(main())
