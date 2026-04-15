"""
FSIS Global — Weekly Subscriber Report
Generated every Monday 07:00 UTC.
Covers the last 7 days across all 50+ sources.
Output: docs/weekly/YYYY-WW.html + docs/weekly/index.html
"""
import json, logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from database import get_recalls, get_stats_for_period, get_monthly_trend

log = logging.getLogger("report.weekly")


def _esc(s):
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def _pathogen_color(p):
    p = (p or "").lower()
    if "listeria" in p: return "#ff8c5a"
    if "salmonella" in p: return "#7ab8e8"
    if "e. coli" in p or "stec" in p: return "#f0c040"
    if "botulinum" in p or "botulism" in p: return "#ce93d8"
    if "norovirus" in p: return "#80d4a8"
    if "aflatoxin" in p or "mycotoxin" in p: return "#ffcc80"
    if "campylobacter" in p: return "#f48fb1"
    return "#888"

def _severity_badge(sev):
    colors = {"CRITICAL":("#E8601A","#fff"),"MODERATE":("#d4a017","#111"),"LOW":("#4caf80","#fff")}
    bg, fg = colors.get(sev, ("#333","#888"))
    return f'<span style="background:{bg};color:{fg};padding:2px 7px;border-radius:3px;font-size:10px;font-weight:700;font-family:monospace;">{sev or "—"}</span>'

def _source_flag(src):
    flags = {"FDA":"🇺🇸","USDA":"🇺🇸","CFIA":"🇨🇦","RASFF":"🇪🇺","FSA_UK":"🇬🇧",
             "EFET_GR":"🇬🇷","FSANZ":"🇦🇺","MHLW_JP":"🇯🇵","SFA_SG":"🇸🇬",
             "DGCCRF_FR":"🇫🇷","BVL_DE":"🇩🇪","MSAL_IT":"🇮🇹","AESAN_ES":"🇪🇸",
             "FSN":"📰","OUTBREAK_NEWS":"📰","FOOD_POISON_JOURNAL":"📰"}
    return flags.get(src, "🌍")

def generate():
    now      = datetime.now(timezone.utc)
    week_num = now.strftime("%Y-W%V")
    date_to  = now.strftime("%Y-%m-%d")
    date_from= (now - timedelta(days=7)).strftime("%Y-%m-%d")
    year     = now.year

    recalls = get_recalls(days=7)
    st      = get_stats_for_period(date_from, date_to)

    # Compare with previous week
    prev_from = (now - timedelta(days=14)).strftime("%Y-%m-%d")
    prev_to   = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    prev_st   = get_stats_for_period(prev_from, prev_to)
    delta     = st["total"] - prev_st["total"]
    delta_str = f"+{delta}" if delta > 0 else str(delta)
    delta_col = "#ef5350" if delta > 0 else "#4caf80" if delta < 0 else "#888"

    # Top pathogens table
    path_rows = ""
    for p in st["by_path"][:8]:
        name = p.get("p") or "Unknown"
        col  = _pathogen_color(name)
        path_rows += f'<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #1e1e1e;"><span style="color:{col};font-size:13px;">{_esc(name)}</span><span style="color:#E8601A;font-weight:700;">{p["c"]}</span></div>'

    # Source breakdown
    src_rows = ""
    for s in st["by_src"]:
        flag = _source_flag(s["source"])
        src_rows += f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #1e1e1e;"><span style="font-size:12px;">{flag} {_esc(s["source"])}</span><span style="color:#E8601A;font-weight:700;font-size:12px;">{s["c"]}</span></div>'

    # Country breakdown
    country_rows = ""
    for c in st["by_country"][:8]:
        country_rows += f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #1e1e1e;"><span style="font-size:12px;">{_esc(c["country"])}</span><span style="color:#E8601A;font-weight:700;font-size:12px;">{c["c"]}</span></div>'

    # Recall table
    recall_rows = ""
    for r in sorted(recalls, key=lambda x: x.get("severity","") == "CRITICAL", reverse=True)[:20]:
        pathogen = r.get("pathogen_ai") or r.get("pathogen") or "—"
        col = _pathogen_color(pathogen)
        outbreak = '<span style="background:rgba(232,96,26,.2);color:#E8601A;font-size:9px;padding:1px 5px;border-radius:2px;font-family:monospace;margin-left:4px;">OUTBREAK</span>' if r.get("is_outbreak") else ""
        recall_rows += f"""<tr>
<td style="font-family:monospace;font-size:11px;color:#666;">{_esc(r.get('recall_date',''))}</td>
<td style="font-size:11px;">{_source_flag(r.get('source',''))} {_esc(r.get('source',''))}</td>
<td style="font-weight:500;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px;" title="{_esc(r.get('firm',''))}">{_esc(r.get('firm','')[:50])}{outbreak}</td>
<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#bbb;font-size:11px;" title="{_esc(r.get('product',''))}">{_esc(r.get('product','')[:60])}</td>
<td style="color:{col};font-weight:700;font-size:11px;font-family:monospace;">{_esc(pathogen)}</td>
<td>{_severity_badge(r.get('severity',''))}</td>
<td style="font-size:11px;color:#666;">{_esc(r.get('country',''))}</td>
</tr>"""

    # Outbreaks / critical callouts
    critical_recalls = [r for r in recalls if r.get("severity") == "CRITICAL" or r.get("is_outbreak")]
    callout_html = ""
    for r in critical_recalls[:4]:
        pathogen = r.get("pathogen_ai") or r.get("pathogen") or "Unknown"
        col = _pathogen_color(pathogen)
        callout_html += f"""<div style="background:#1a0800;border:1px solid #4a2000;border-left:3px solid {col};border-radius:4px;padding:12px 16px;margin-bottom:10px;">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
  <span style="color:{col};font-weight:700;font-family:monospace;font-size:12px;">{_esc(pathogen)}</span>
  <span style="font-size:10px;color:#666;font-family:monospace;">{_esc(r.get('recall_date',''))} · {_esc(r.get('source',''))}</span>
</div>
<div style="font-size:13px;font-weight:500;margin-bottom:4px;">{_esc(r.get('firm','')[:80])}</div>
<div style="font-size:12px;color:#bbb;">{_esc(r.get('product','')[:100])}</div>
<div style="font-size:11px;color:#888;margin-top:4px;">{_esc(r.get('risk_summary','') or r.get('reason','')[:120])}</div>
</div>"""

    if not callout_html:
        callout_html = '<div style="color:#555;font-size:12px;padding:8px;">No critical alerts this week</div>'

    html = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AFTS Weekly Food Safety Report — {week_num}</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:#0e0e0e;color:#f0f0f0;font-family:'DM Sans',sans-serif;font-size:14px;line-height:1.6;}}
.hdr{{background:#161616;border-bottom:2px solid #E8601A;padding:20px 40px;}}
.week-label{{font-family:'DM Mono',monospace;font-size:11px;color:#E8601A;letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px;}}
h1{{font-family:'Syne',sans-serif;font-size:26px;font-weight:800;}}
.period{{font-family:'DM Mono',monospace;font-size:11px;color:#666;margin-top:4px;}}
.wrap{{max-width:900px;margin:0 auto;padding:32px 40px;}}
.kpi-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:28px;}}
.kpi{{background:#161616;border:1px solid #2a2a2a;border-radius:6px;padding:16px;border-top:2px solid #E8601A;}}
.kpi.r{{border-top-color:#ef5350;}}.kpi.a{{border-top-color:#d4a017;}}.kpi.g{{border-top-color:#4caf80;}}
.kv{{font-family:'Syne',sans-serif;font-size:32px;font-weight:800;color:#E8601A;}}
.kpi.r .kv{{color:#ef5350;}}.kpi.a .kv{{color:#d4a017;}}.kpi.g .kv{{color:#4caf80;}}
.kl{{font-size:10px;color:#666;letter-spacing:.08em;text-transform:uppercase;margin-top:4px;}}
h2{{font-family:'Syne',sans-serif;font-size:11px;font-weight:700;letter-spacing:.12em;color:#E8601A;text-transform:uppercase;margin-bottom:14px;padding-bottom:6px;border-bottom:1px solid #222;}}
.panel{{background:#161616;border:1px solid #2a2a2a;border-radius:6px;padding:20px;margin-bottom:20px;}}
.grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px;}}
.grid-3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px;margin-bottom:20px;}}
.tbl-wrap{{overflow-x:auto;}}
table{{width:100%;border-collapse:collapse;font-size:12px;}}
thead tr{{border-bottom:1px solid #2a2a2a;}}
th{{text-align:left;padding:8px;font-size:10px;color:#555;letter-spacing:.08em;}}
td{{padding:7px 8px;border-bottom:1px solid #161616;vertical-align:middle;}}
tr:hover td{{background:rgba(255,255,255,.025);}}
.footer{{text-align:center;padding:24px;font-size:11px;color:#555;border-top:1px solid #1e1e1e;font-family:monospace;}}
.footer a{{color:#E8601A;text-decoration:none;}}
@media(max-width:600px){{.kpi-row,.grid-2,.grid-3{{grid-template-columns:1fr;}}.wrap{{padding:20px;}}}}
</style></head><body>

<div class="hdr">
  <div class="week-label">Weekly Intelligence Report</div>
  <h1>Food Safety Week {week_num}</h1>
  <div class="period">{date_from} to {date_to} &nbsp;·&nbsp; All global sources &nbsp;·&nbsp; AFTS Food Safety Intelligence</div>
</div>

<div class="wrap">
  <div class="kpi-row" style="margin-top:28px;">
    <div class="kpi"><div class="kv">{st['total']}</div><div class="kl">Recalls This Week</div></div>
    <div class="kpi r"><div class="kv">{st['critical']}</div><div class="kl">Class I Critical</div></div>
    <div class="kpi a"><div class="kv">{st['outbreaks']}</div><div class="kl">Active Outbreaks</div></div>
    <div class="kpi" style="border-top-color:{delta_col};"><div class="kv" style="color:{delta_col};">{delta_str}</div><div class="kl">vs Previous Week</div></div>
  </div>

  <div class="panel">
    <h2>Critical Alerts & Outbreaks</h2>
    {callout_html}
  </div>

  <div class="grid-3">
    <div class="panel"><h2>Top Pathogens</h2>{path_rows or '<div style="color:#555;font-size:12px;">No data</div>'}</div>
    <div class="panel"><h2>By Source</h2>{src_rows or '<div style="color:#555;font-size:12px;">No data</div>'}</div>
    <div class="panel"><h2>By Country</h2>{country_rows or '<div style="color:#555;font-size:12px;">No data</div>'}</div>
  </div>

  <div class="panel">
    <h2>All Recalls This Week ({len(recalls)} records)</h2>
    <div class="tbl-wrap">
      <table><thead><tr>
        <th>Date</th><th>Source</th><th>Firm</th><th>Product</th>
        <th>Pathogen</th><th>Severity</th><th>Country</th>
      </tr></thead>
      <tbody>{recall_rows or '<tr><td colspan="7" style="text-align:center;color:#555;padding:20px;">No pathogen recalls this week</td></tr>'}</tbody>
      </table>
    </div>
  </div>
</div>

<div class="footer">
  <a href="https://advfood.tech">AFTS · Advanced Food-Tech Solutions</a> &nbsp;·&nbsp;
  <a href="../index.html">Live Dashboard</a> &nbsp;·&nbsp;
  <a href="../alerts/">Alerts</a> &nbsp;·&nbsp;
  Data: FDA · USDA · RASFF · CFIA · 50+ sources &nbsp;·&nbsp; {year}
</div>
</body></html>"""

    # Save report
    Path("docs/weekly").mkdir(parents=True, exist_ok=True)
    out = Path(f"docs/weekly/{week_num}.html")
    out.write_text(html, encoding="utf-8")

    # Update weekly index
    _update_index("docs/weekly/index.html", out.name, f"Week {week_num} ({date_from} to {date_to})", st["total"])

    log.info(f"Weekly report → {out} ({st['total']} recalls)")
    return str(out)


def _update_index(index_path, filename, label, count):
    p = Path(index_path)
    link = f'<li style="padding:6px 0;border-bottom:0.5px solid #222;display:flex;justify-content:space-between;"><a href="{filename}" style="color:#E8601A;text-decoration:none;">{label}</a><span style="color:#666;font-size:12px;">{count} recalls</span></li>'
    if p.exists():
        content = p.read_text()
        content = content.replace("</ul>", f"{link}\n</ul>", 1)
        p.write_text(content)
    else:
        p.write_text(f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>Weekly Reports | AFTS</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@800&family=DM+Sans&display=swap" rel="stylesheet">
<style>body{{background:#0e0e0e;color:#f0f0f0;font-family:'DM Sans',sans-serif;padding:40px;max-width:700px;margin:0 auto;}}
h1{{font-family:'Syne',sans-serif;font-size:22px;color:#E8601A;margin-bottom:24px;}}
ul{{list-style:none;padding:0;}}</style></head>
<body><h1>AFTS · Weekly Reports</h1><ul>{link}</ul></body></html>""")
