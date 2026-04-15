"""
FSIS Global — Yearly Intelligence Report
5-year trend analysis + current year deep dive.
Generated January 1st each year.
"""
import json, logging, statistics
from datetime import datetime, timezone
from pathlib import Path
from database import get_recalls, get_stats_for_period, get_historical_baseline, conn

log = logging.getLogger("report.yearly")


def _esc(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def _pc(p):
    p = (p or "").lower()
    if "listeria" in p: return "#ff8c5a"
    if "salmonella" in p: return "#7ab8e8"
    if "e. coli" in p or "stec" in p: return "#f0c040"
    if "botulinum" in p: return "#ce93d8"
    if "norovirus" in p: return "#80d4a8"
    if "aflatoxin" in p or "mycotoxin" in p: return "#ffcc80"
    if "campylobacter" in p: return "#f48fb1"
    return "#888"


def _get_yearly_data(year: int) -> dict:
    date_from = f"{year}-01-01"
    date_to   = f"{year}-12-31"
    return get_stats_for_period(date_from, date_to)


def _five_year_trend():
    """Monthly recall counts for last 5 years."""
    db = conn()
    rows = db.execute("""
        SELECT substr(recall_date,1,7) m, COUNT(*) c
        FROM recalls WHERE superseded=0
        AND recall_date >= date('now','-5 years')
        GROUP BY m ORDER BY m""").fetchall()
    db.close()
    return [dict(r) for r in rows]


def _sparkline(months_data, height=80, highlight_year=None):
    if not months_data: return ""
    counts = [m["c"] for m in months_data]
    months = [m["m"] for m in months_data]
    mx = max(counts) if counts else 1
    w, h = 700, height
    bw = w / max(len(counts), 1)
    bars = labels = ""
    for i, (mon, cnt) in enumerate(zip(months, counts)):
        bh = int((cnt/mx)*(h-25))+2
        x = i*bw
        is_hl = highlight_year and mon.startswith(str(highlight_year))
        col = "#E8601A" if is_hl else "#444"
        bars += f'<rect x="{x+0.5}" y="{h-bh-15}" width="{bw-1}" height="{bh}" fill="{col}" rx="1"><title>{mon}: {cnt}</title></rect>'
        if i % 6 == 0:
            labels += f'<text x="{x+bw/2}" y="{h-1}" text-anchor="middle" font-size="8" fill="#555">{mon[2:]}</text>'
    return f'<svg viewBox="0 0 {w} {h}" width="100%" xmlns="http://www.w3.org/2000/svg">{bars}{labels}</svg>'


def generate():
    now  = datetime.now(timezone.utc)
    year = now.year - 1  # report for the just-completed year
    now_str = now.strftime("%Y-%m-%d %H:%M UTC")

    log.info(f"Yearly report — {year}")

    # This year data
    st = _get_yearly_data(year)
    recalls = get_recalls(year=year)
    baseline = get_historical_baseline()
    five_yr  = _five_year_trend()

    # Multi-year comparison
    yearly_comparison = []
    for y in range(year-4, year+1):
        yst = _get_yearly_data(y)
        yearly_comparison.append({"year": y, "total": yst["total"],
                                   "critical": yst["critical"], "outbreaks": yst["outbreaks"]})

    # Pathogen 5-year ranking
    db = conn()
    path_5yr = db.execute("""
        SELECT COALESCE(pathogen_ai,pathogen,'Unknown') p, COUNT(*) c
        FROM recalls WHERE superseded=0
        AND recall_date >= date('now','-5 years')
        GROUP BY p ORDER BY c DESC LIMIT 15""").fetchall()
    path_5yr = [dict(r) for r in path_5yr]

    # Source 5-year
    src_5yr = db.execute("""
        SELECT source, COUNT(*) c FROM recalls WHERE superseded=0
        AND recall_date >= date('now','-5 years')
        GROUP BY source ORDER BY c DESC""").fetchall()
    src_5yr = [dict(r) for r in src_5yr]

    # Monthly seasonality for current year
    monthly_cur = db.execute("""
        SELECT substr(recall_date,1,7) m, COUNT(*) c FROM recalls
        WHERE superseded=0 AND data_year=? GROUP BY m ORDER BY m""",
        (year,)).fetchall()
    monthly_cur = [dict(r) for r in monthly_cur]

    # Peak month
    peak_month = max(monthly_cur, key=lambda x: x["c"])["m"] if monthly_cur else "N/A"

    # Category breakdown
    cat_5yr = db.execute("""
        SELECT COALESCE(hazard_category,'Bacteria') h, COUNT(*) c FROM recalls
        WHERE superseded=0 AND recall_date >= date('now','-5 years')
        GROUP BY h ORDER BY c DESC""").fetchall()
    cat_5yr = [dict(r) for r in cat_5yr]
    db.close()

    # ── HTML sections ─────────────────────────────────────────────────────────

    def year_comparison_table():
        rows = ""
        for y in yearly_comparison:
            delta = ""
            if yearly_comparison.index(y) > 0:
                prev = yearly_comparison[yearly_comparison.index(y)-1]["total"]
                d = y["total"] - prev
                col = "#ef5350" if d > 0 else "#4caf80"
                delta = f'<span style="color:{col};font-size:11px;font-family:monospace;">({"+"+str(d) if d > 0 else str(d)})</span>'
            bold = ' style="font-weight:700;color:#f0f0f0;"' if y["year"] == year else ''
            rows += f'<tr{bold}><td style="font-family:monospace;{bold[8:-1] if bold else ""}">{y["year"]}</td><td>{y["total"]} {delta}</td><td style="color:#ef5350;">{y["critical"]}</td><td style="color:#d4a017;">{y["outbreaks"]}</td></tr>'
        return rows

    def path_5yr_rows():
        total = sum(p["c"] for p in path_5yr) or 1
        return "".join(
            f'<div style="margin-bottom:10px;">'
            f'<div style="display:flex;justify-content:space-between;margin-bottom:3px;">'
            f'<span style="color:{_pc(p["p"])};font-size:13px;">{_esc(p["p"] or "Unknown")}</span>'
            f'<span style="color:#E8601A;font-weight:700;">{p["c"]} &nbsp;<span style="color:#555;font-size:11px;">({round(p["c"]/total*100,1)}%)</span></span></div>'
            f'<div style="background:#1e1e1e;border-radius:2px;height:3px;">'
            f'<div style="background:{_pc(p["p"])};width:{round(p["c"]/total*100)}%;height:100%;border-radius:2px;"></div></div></div>'
            for p in path_5yr[:10]
        )

    def cat_rows():
        return "".join(
            f'<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #1e1e1e;">'
            f'<span style="font-size:12px;">{_esc(c["h"])}</span>'
            f'<span style="color:#E8601A;font-weight:700;">{c["c"]}</span></div>'
            for c in cat_5yr
        )

    def top_recalls_rows():
        top = sorted(recalls, key=lambda r: r.get("severity","") == "CRITICAL", reverse=True)[:15]
        return "".join(
            f'<tr><td style="font-family:monospace;font-size:11px;color:#666;">{_esc(r.get("recall_date",""))}</td>'
            f'<td style="font-size:11px;">{_esc(r.get("source",""))}</td>'
            f'<td style="font-weight:500;font-size:12px;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{_esc(r.get("firm","")[:50])}</td>'
            f'<td style="font-size:11px;color:#bbb;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{_esc(r.get("product","")[:55])}</td>'
            f'<td style="color:{_pc(r.get("pathogen_ai") or r.get("pathogen",""))};font-weight:700;font-size:11px;font-family:monospace;">{_esc(r.get("pathogen_ai") or r.get("pathogen","—"))}</td>'
            f'<td style="font-size:11px;color:#666;">{_esc(r.get("country",""))}</td></tr>'
            for r in top
        )

    html = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AFTS Yearly Report {year}</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:#0e0e0e;color:#f0f0f0;font-family:'DM Sans',sans-serif;line-height:1.6;}}
.hdr{{background:#161616;border-bottom:2px solid #E8601A;padding:24px 40px;}}
.rt{{font-family:'DM Mono',monospace;font-size:11px;color:#E8601A;letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px;}}
h1{{font-family:'Syne',sans-serif;font-size:32px;font-weight:800;}}
.meta{{font-family:'DM Mono',monospace;font-size:11px;color:#666;margin-top:6px;}}
.wrap{{max-width:960px;margin:0 auto;padding:36px 40px;}}
.kpi-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:32px;}}
.kpi{{background:#161616;border:1px solid #2a2a2a;border-radius:8px;padding:18px;border-top:2px solid #E8601A;}}
.kpi.r{{border-top-color:#ef5350;}}.kpi.a{{border-top-color:#d4a017;}}.kpi.b{{border-top-color:#5b9bd5;}}
.kv{{font-family:'Syne',sans-serif;font-size:36px;font-weight:800;color:#E8601A;line-height:1;}}
.kpi.r .kv{{color:#ef5350;}}.kpi.a .kv{{color:#d4a017;}}.kpi.b .kv{{color:#5b9bd5;}}
.kl{{font-size:10px;color:#666;letter-spacing:.08em;text-transform:uppercase;margin-top:5px;}}
h2{{font-family:'Syne',sans-serif;font-size:11px;font-weight:700;letter-spacing:.12em;color:#E8601A;text-transform:uppercase;margin-bottom:14px;padding-bottom:7px;border-bottom:1px solid #222;}}
.panel{{background:#161616;border:1px solid #2a2a2a;border-radius:8px;padding:22px;margin-bottom:22px;}}
.grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:22px;margin-bottom:22px;}}
.grid-3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:22px;margin-bottom:22px;}}
table{{width:100%;border-collapse:collapse;font-size:12px;}}
thead tr{{border-bottom:1px solid #2a2a2a;}}
th{{text-align:left;padding:8px;font-size:10px;color:#555;letter-spacing:.08em;}}
td{{padding:7px 8px;border-bottom:1px solid #161616;vertical-align:middle;}}
tr:hover td{{background:rgba(255,255,255,.025);}}
.footer{{text-align:center;padding:28px;font-size:11px;color:#555;border-top:1px solid #1e1e1e;font-family:monospace;}}
.footer a{{color:#E8601A;text-decoration:none;}}
@media(max-width:600px){{.kpi-row,.grid-2,.grid-3{{grid-template-columns:1fr;}}.wrap{{padding:20px;}}}}
</style></head><body>

<div class="hdr">
  <div class="rt">Annual Intelligence Report</div>
  <h1>Food Safety Year in Review — {year}</h1>
  <div class="meta">5-year trend analysis · 50+ global sources · Generated: {now_str}</div>
</div>

<div class="wrap">
  <div class="kpi-row" style="margin-top:30px;">
    <div class="kpi"><div class="kv">{st['total']}</div><div class="kl">{year} Total Recalls</div></div>
    <div class="kpi r"><div class="kv">{st['critical']}</div><div class="kl">Class I Critical</div></div>
    <div class="kpi a"><div class="kv">{st['outbreaks']}</div><div class="kl">Outbreaks</div></div>
    <div class="kpi b"><div class="kv">{peak_month[5:] if len(peak_month)>4 else "—"}</div><div class="kl">Peak Month</div></div>
  </div>

  <div class="panel">
    <h2>5-Year Recall Trend (orange = {year})</h2>
    {_sparkline(five_yr, height=110, highlight_year=year)}
  </div>

  <div class="grid-3">
    <div class="panel">
      <h2>Year-over-Year Comparison</h2>
      <table><thead><tr><th>Year</th><th>Total</th><th>Critical</th><th>Outbreaks</th></tr></thead>
      <tbody>{year_comparison_table()}</tbody></table>
    </div>
    <div class="panel">
      <h2>Top Pathogens (5 Years)</h2>
      {path_5yr_rows()}
    </div>
    <div class="panel">
      <h2>By Hazard Category (5 Years)</h2>
      {cat_rows()}
    </div>
  </div>

  <div class="panel">
    <h2>Top Recall Events — {year}</h2>
    <div style="overflow-x:auto;">
      <table><thead><tr>
        <th>Date</th><th>Source</th><th>Firm</th><th>Product</th><th>Pathogen</th><th>Country</th>
      </tr></thead>
      <tbody>{top_recalls_rows()}</tbody></table>
    </div>
  </div>
</div>

<div class="footer">
  <a href="https://advfood.tech">AFTS · Advanced Food-Tech Solutions</a> &nbsp;·&nbsp;
  <a href="../index.html">Live Dashboard</a> &nbsp;·&nbsp;
  <a href="../monthly/">Monthly Reports</a> &nbsp;·&nbsp;
  <a href="../weekly/">Weekly Reports</a> &nbsp;·&nbsp; {year}
</div>
</body></html>"""

    Path("docs/yearly").mkdir(parents=True, exist_ok=True)
    out = Path(f"docs/yearly/{year}.html")
    out.write_text(html, encoding="utf-8")
    _update_index(year, out.name, st["total"])
    log.info(f"Yearly report → {out}")
    return str(out)


def _update_index(year, filename, count):
    p = Path("docs/yearly/index.html")
    link = f'<li style="padding:6px 0;border-bottom:0.5px solid #222;display:flex;justify-content:space-between;"><a href="{filename}" style="color:#E8601A;text-decoration:none;">Year {year}</a><span style="color:#666;font-size:12px;">{count} recalls</span></li>'
    if p.exists():
        c = p.read_text(); c = c.replace("</ul>", f"{link}\n</ul>", 1); p.write_text(c)
    else:
        p.write_text(f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Yearly Reports</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@800&family=DM+Sans&display=swap" rel="stylesheet">
<style>body{{background:#0e0e0e;color:#f0f0f0;font-family:'DM Sans',sans-serif;padding:40px;max-width:700px;margin:0 auto;}}
h1{{font-family:'Syne',sans-serif;font-size:22px;color:#E8601A;margin-bottom:24px;}}ul{{list-style:none;padding:0;}}</style></head>
<body><h1>AFTS · Yearly Reports</h1><ul>{link}</ul></body></html>""")
