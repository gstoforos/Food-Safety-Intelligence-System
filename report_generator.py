"""
FSIS Agent – HTML Report Generator (Full Dashboard)
Website = entire searchable/filterable recall list
Weekly/Monthly = separate summary reports
"""
import os
from datetime import datetime, timezone
from pathlib import Path

from config import (
    REPORT_OUTPUT, REPORT_TITLE, BRAND_NAME, BRAND_URL,
    COLOR_BG, COLOR_SURFACE, COLOR_ORANGE, COLOR_WHITE, COLOR_MUTED,
    COLOR_CRITICAL, COLOR_MODERATE, COLOR_LOW, CLASS_SEVERITY
)
from database import get_recent_recalls, get_stats, get_conn


def _esc(s):
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"','&quot;')

def _badge_html(classification):
    sev = CLASS_SEVERITY.get(classification, "UNKNOWN")
    colors = {
        "CRITICAL": (COLOR_CRITICAL, "#fff"),
        "MODERATE": (COLOR_MODERATE, "#111"),
        "LOW":      (COLOR_LOW,      "#fff"),
        "UNKNOWN":  ("#444",         "#aaa"),
    }
    bg, fg = colors.get(sev, ("#444", "#aaa"))
    return f'<span class="badge" style="background:{bg};color:{fg};">{sev}</span>'

def _sparkline(monthly):
    if not monthly:
        return "<div style='color:#444;font-size:12px;'>No trend data</div>"
    counts = [m["cnt"] for m in monthly]
    months = [m["month"] for m in monthly]
    mx = max(counts) if counts else 1
    w, h = 600, 100
    bar_w = w / max(len(counts), 1)
    bars = labels = ""
    for i, (mon, cnt) in enumerate(zip(months, counts)):
        bh = int((cnt / mx) * (h - 25)) + 2
        x  = i * bar_w
        y  = h - bh - 18
        bars += (f'<rect x="{x+1}" y="{y}" width="{bar_w-2}" height="{bh}" '
                 f'fill="#E8601A" rx="2" opacity="0.85"><title>{mon}: {cnt}</title></rect>')
        if i % 2 == 0:
            short = mon[2:].replace("-", "/")
            labels += (f'<text x="{x+bar_w/2}" y="{h-2}" text-anchor="middle" '
                       f'font-size="8" fill="#888">{short}</text>')
    return (f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" '
            f'xmlns="http://www.w3.org/2000/svg">{bars}{labels}</svg>')

def _donut(by_class):
    palette = [COLOR_CRITICAL, COLOR_MODERATE, COLOR_LOW, "#555"]
    r, cx, cy, sw = 48, 68, 68, 26
    total = sum(d["cnt"] for d in by_class) or 1
    circ  = 2 * 3.14159 * r
    offset_val = 0
    slices = legend = ""
    for i, d in enumerate(by_class[:4]):
        frac  = d["cnt"] / total
        dash  = frac * circ
        color = palette[i % len(palette)]
        slices += (f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" stroke="{color}" '
                   f'stroke-width="{sw}" stroke-dasharray="{dash:.1f} {circ:.1f}" '
                   f'stroke-dashoffset="-{offset_val:.1f}" transform="rotate(-90 {cx} {cy})">'
                   f'<title>{d["classification"]}: {d["cnt"]}</title></circle>')
        offset_val += dash
        pct = int(frac * 100)
        legend += (f'<div style="display:flex;align-items:center;gap:6px;margin:2px 0;">'
                   f'<span style="width:9px;height:9px;border-radius:2px;background:{color};flex-shrink:0;"></span>'
                   f'<span style="font-size:11px;color:#888;">{_esc(d["classification"])} – {d["cnt"]} ({pct}%)</span></div>')
    svg = (f'<svg viewBox="0 0 136 136" width="130" height="130" xmlns="http://www.w3.org/2000/svg">'
           f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="#1a1a1a"/>{slices}'
           f'<text x="{cx}" y="{cy+5}" text-anchor="middle" font-size="14" fill="#F5F5F5" font-weight="700">{total}</text>'
           f'<text x="{cx}" y="{cy+17}" text-anchor="middle" font-size="7" fill="#888">RECALLS</text></svg>')
    return f'<div style="display:flex;align-items:center;gap:16px;">{svg}<div>{legend}</div></div>'

def get_all_recalls():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM recalls ORDER BY recall_date DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def _build_table_rows(recalls):
    rows = ""
    for r in recalls:
        pathogen  = _esc(r.get("pathogen") or "—")
        conf      = r.get("pathogen_confidence","")
        conf_col  = {"HIGH":COLOR_CRITICAL,"MEDIUM":COLOR_MODERATE,"LOW":COLOR_LOW}.get(conf, COLOR_MUTED)
        risk      = _esc(r.get("risk_summary") or r.get("reason","")[:100])
        product   = _esc(r.get("product","")[:80])
        firm      = _esc(r.get("firm","")[:45])
        dist      = _esc(r.get("distribution","")[:60])
        date      = _esc(r.get("recall_date",""))
        sev       = CLASS_SEVERITY.get(r.get("classification",""), "UNKNOWN")
        search_str= _esc((r.get("product","")+" "+r.get("firm","")+" "+r.get("reason","")).lower())
        rows += (f'<tr data-pathogen="{pathogen.lower()}" data-class="{sev.lower()}" data-search="{search_str}">'
                 f'<td class="td-date">{date}</td>'
                 f'<td class="td-product" title="{product}">{product}</td>'
                 f'<td class="td-firm">{firm}</td>'
                 f'<td class="td-pathogen" style="color:{conf_col};font-weight:700;font-family:monospace;">{pathogen}</td>'
                 f'<td>{_badge_html(r.get("classification",""))}</td>'
                 f'<td class="td-dist" title="{dist}">{dist}</td>'
                 f'<td class="td-summary" title="{risk}">{risk}</td>'
                 f'</tr>\n')
    return rows

def generate_report(days: int = 30):
    Path("docs").mkdir(exist_ok=True)
    stats       = get_stats(365)
    all_recalls = get_all_recalls()
    recent_30   = get_recent_recalls(30)
    now_utc     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    year        = datetime.now().year
    total_all   = len(all_recalls)
    critical_30 = sum(1 for r in recent_30 if CLASS_SEVERITY.get(r.get("classification","")) == "CRITICAL")
    enriched    = sum(1 for r in all_recalls if r.get("enriched"))
    pathogens_seen = sorted(set(r.get("pathogen","Unknown") for r in all_recalls
                                if r.get("pathogen") and r.get("pathogen") != "Unknown"))
    path_items = "".join(
        f'<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #1e1e1e;">'
        f'<span style="font-size:12px;">{_esc(p["pathogen"])}</span>'
        f'<span style="color:#E8601A;font-weight:700;font-size:12px;">{p["cnt"]}</span></div>'
        for p in stats["by_pathogen"])
    table_rows = _build_table_rows(all_recalls)
    pathogen_opts = '<option value="">All Pathogens</option>' + "".join(
        f'<option value="{p.lower()}">{p}</option>' for p in pathogens_seen)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{REPORT_TITLE}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:#111;color:#F5F5F5;font-family:'DM Sans',sans-serif;font-size:14px;line-height:1.5;}}
.header{{background:#1a1a1a;border-bottom:2px solid #E8601A;padding:14px 24px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:100;}}
.brand{{font-family:'Syne',sans-serif;font-size:17px;font-weight:800;}}.brand span{{color:#E8601A;}}
.updated{{font-family:'DM Mono',monospace;font-size:10px;color:#888;}}
.container{{padding:20px 24px;max-width:1400px;margin:0 auto;}}
.section-title{{font-family:'Syne',sans-serif;font-size:11px;font-weight:700;letter-spacing:.12em;color:#E8601A;text-transform:uppercase;margin-bottom:12px;padding-bottom:5px;border-bottom:1px solid #2a2a2a;}}
.panel{{background:#1a1a1a;border:1px solid #222;border-radius:6px;padding:18px;margin-bottom:18px;}}
.grid-3{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:18px;}}
.grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:18px;}}
.stat-card{{background:#1a1a1a;border:1px solid #222;border-radius:6px;padding:14px 18px;}}
.stat-card .val{{font-family:'Syne',sans-serif;font-size:34px;font-weight:800;color:#E8601A;line-height:1;}}
.stat-card .lbl{{font-size:10px;color:#888;letter-spacing:.08em;text-transform:uppercase;margin-top:4px;}}
.filters{{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-bottom:14px;}}
.filters input,.filters select{{background:#1e1e1e;border:1px solid #333;color:#F5F5F5;padding:7px 12px;border-radius:4px;font-size:12px;font-family:'DM Mono',monospace;outline:none;}}
.filters input{{flex:1;min-width:200px;}}.filters input::placeholder{{color:#555;}}
.filters select{{cursor:pointer;}}
.filter-count{{font-family:'DM Mono',monospace;font-size:11px;color:#888;margin-left:auto;}}
.table-wrap{{overflow-x:auto;}}
table{{width:100%;border-collapse:collapse;font-size:12px;}}
thead tr{{border-bottom:1px solid #2a2a2a;}}
th{{text-align:left;padding:8px;color:#888;font-size:10px;font-weight:600;letter-spacing:.08em;white-space:nowrap;cursor:pointer;user-select:none;}}
th:hover{{color:#F5F5F5;}}
th.sort-asc::after{{content:" ▲";color:#E8601A;}}th.sort-desc::after{{content:" ▼";color:#E8601A;}}
td{{padding:7px 8px;border-bottom:1px solid #1a1a1a;vertical-align:middle;}}
tr:hover td{{background:rgba(255,255,255,.025);}}
.td-date{{font-family:'DM Mono',monospace;font-size:11px;color:#888;white-space:nowrap;}}
.td-product{{max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}}
.td-firm{{max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#ccc;}}
.td-dist{{max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#888;font-size:11px;}}
.td-summary{{max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#888;font-size:11px;}}
.badge{{padding:2px 7px;border-radius:3px;font-size:10px;font-weight:700;letter-spacing:.05em;white-space:nowrap;}}
.pagination{{display:flex;justify-content:space-between;align-items:center;margin-top:14px;flex-wrap:wrap;gap:8px;}}
.page-btns{{display:flex;gap:6px;flex-wrap:wrap;}}
.page-btn{{background:#1e1e1e;border:1px solid #333;color:#888;padding:5px 10px;border-radius:3px;cursor:pointer;font-size:12px;font-family:'DM Mono',monospace;}}
.page-btn:hover,.page-btn.active{{background:#E8601A;color:#fff;border-color:#E8601A;}}
.page-info{{font-family:'DM Mono',monospace;font-size:11px;color:#888;}}
.footer{{text-align:center;padding:18px;font-size:11px;color:#888;border-top:1px solid #1e1e1e;}}
.footer a{{color:#E8601A;text-decoration:none;}}
@media(max-width:700px){{.grid-3,.grid-2{{grid-template-columns:1fr;}}.header{{flex-direction:column;gap:6px;align-items:flex-start;}}.td-dist,.td-summary{{display:none;}}}}
</style>
</head>
<body>
<div class="header">
  <div class="brand">AFTS <span>·</span> Food Safety Intelligence</div>
  <div class="updated">FDA Pathogen Recalls &nbsp;·&nbsp; Updated: {now_utc}</div>
</div>
<div class="container">
  <div class="grid-3" style="margin-top:18px;">
    <div class="stat-card"><div class="val">{total_all}</div><div class="lbl">Total Pathogen Recalls in Database</div></div>
    <div class="stat-card"><div class="val" style="color:#E8601A;">{critical_30}</div><div class="lbl">Class I (Critical) – Last 30 Days</div></div>
    <div class="stat-card"><div class="val">{len(pathogens_seen)}</div><div class="lbl">Distinct Pathogens Tracked</div></div>
  </div>
  <div class="grid-2">
    <div class="panel"><div class="section-title">Monthly Trend (12 Months)</div>{_sparkline(stats['by_month'])}</div>
    <div class="panel"><div class="section-title">By Classification</div>{_donut(stats['by_class'])}</div>
  </div>
  <div class="grid-2">
    <div class="panel"><div class="section-title">Top Pathogens (All Time)</div>{path_items or '<div style="color:#444;font-size:12px;">No data yet</div>'}</div>
    <div class="panel"><div class="section-title">Data Coverage</div>
      <div style="line-height:2.2;font-size:12px;color:#888;">
        <div>Source &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;: <span style="color:#F5F5F5;">FDA openFDA API</span></div>
        <div>Total records &nbsp;: <span style="color:#F5F5F5;">{total_all} pathogen recalls</span></div>
        <div>AI-enriched &nbsp;&nbsp;: <span style="color:#F5F5F5;">{enriched} / {total_all}</span></div>
        <div>Excluded &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;: <span style="color:#F5F5F5;">allergens, label errors, drugs</span></div>
        <div>AI engine &nbsp;&nbsp;&nbsp;&nbsp;: <span style="color:#E8601A;">Gemini 2.0 Flash</span></div>
        <div>Auto-update &nbsp;&nbsp;: <span style="color:#F5F5F5;">daily 06:00 UTC</span></div>
      </div>
    </div>
  </div>
  <div class="panel">
    <div class="section-title">All Pathogen Recalls — Complete Database (<span id="visible-count">{total_all}</span> records)</div>
    <div class="filters">
      <input type="text" id="search-input" placeholder="Search product, firm, reason...">
      <select id="pathogen-filter">{pathogen_opts}</select>
      <select id="class-filter">
        <option value="">All Classes</option>
        <option value="critical">CRITICAL (Class I)</option>
        <option value="moderate">MODERATE (Class II)</option>
        <option value="low">LOW (Class III)</option>
      </select>
      <select id="rows-per-page">
        <option value="25">25 / page</option>
        <option value="50" selected>50 / page</option>
        <option value="100">100 / page</option>
        <option value="9999">All</option>
      </select>
      <span class="filter-count" id="filter-count"></span>
    </div>
    <div class="table-wrap">
      <table id="recalls-table">
        <thead><tr>
          <th data-col="date">DATE</th>
          <th data-col="product">PRODUCT</th>
          <th data-col="firm">FIRM</th>
          <th data-col="pathogen">PATHOGEN</th>
          <th data-col="class">CLASS</th>
          <th data-col="dist">DISTRIBUTION</th>
          <th data-col="summary">SUMMARY</th>
        </tr></thead>
        <tbody id="table-body">{table_rows}</tbody>
      </table>
    </div>
    <div class="pagination">
      <span class="page-info" id="page-info"></span>
      <div class="page-btns" id="page-btns"></div>
    </div>
  </div>
</div>
<div class="footer">
  <a href="{BRAND_URL}" target="_blank">{BRAND_NAME}</a> &nbsp;·&nbsp;
  Data: <a href="https://open.fda.gov/apis/food/enforcement/" target="_blank">openFDA</a>
  &nbsp;·&nbsp; Pathogen recalls only · allergens &amp; label errors excluded &nbsp;·&nbsp; {year}
</div>
<script>
(function(){{
  const tbody=document.getElementById('table-body');
  const allRows=Array.from(tbody.querySelectorAll('tr'));
  const searchInp=document.getElementById('search-input');
  const pathFilter=document.getElementById('pathogen-filter');
  const classFilter=document.getElementById('class-filter');
  const rppSel=document.getElementById('rows-per-page');
  const pageInfo=document.getElementById('page-info');
  const pageBtns=document.getElementById('page-btns');
  const visCount=document.getElementById('visible-count');
  const filterCount=document.getElementById('filter-count');
  let currentPage=1,filtered=allRows;
  function applyFilters(){{
    const q=searchInp.value.toLowerCase().trim();
    const pf=pathFilter.value.toLowerCase();
    const cf=classFilter.value.toLowerCase();
    filtered=allRows.filter(row=>{{
      if(q&&!row.dataset.search.includes(q)&&!row.dataset.pathogen.includes(q))return false;
      if(pf&&!row.dataset.pathogen.includes(pf))return false;
      if(cf&&!row.dataset.class.includes(cf))return false;
      return true;
    }});
    currentPage=1;render();
  }}
  function render(){{
    const rpp=parseInt(rppSel.value);
    const total=filtered.length;
    const pages=Math.max(1,Math.ceil(total/rpp));
    currentPage=Math.min(currentPage,pages);
    const start=(currentPage-1)*rpp;
    const end=Math.min(start+rpp,total);
    allRows.forEach(r=>r.style.display='none');
    filtered.slice(start,end).forEach(r=>r.style.display='');
    visCount.textContent=total;
    filterCount.textContent=total!==allRows.length?total+' of '+allRows.length+' records':allRows.length+' records';
    pageInfo.textContent=total>0?'Showing '+(start+1)+'–'+end+' of '+total:'No records match';
    pageBtns.innerHTML='';
    if(pages<=1)return;
    const addBtn=(label,pg,active)=>{{
      const b=document.createElement('button');
      b.className='page-btn'+(active?' active':'');
      b.textContent=label;
      b.onclick=()=>{{currentPage=pg;render();}};
      pageBtns.appendChild(b);
    }};
    addBtn('«',1,false);
    if(currentPage>1)addBtn('‹',currentPage-1,false);
    let lo=Math.max(1,currentPage-2),hi=Math.min(pages,lo+4);
    lo=Math.max(1,hi-4);
    for(let i=lo;i<=hi;i++)addBtn(i,i,i===currentPage);
    if(currentPage<pages)addBtn('›',currentPage+1,false);
    addBtn('»',pages,false);
  }}
  searchInp.addEventListener('input',applyFilters);
  pathFilter.addEventListener('change',applyFilters);
  classFilter.addEventListener('change',applyFilters);
  rppSel.addEventListener('change',applyFilters);
  document.querySelectorAll('th[data-col]').forEach(th=>{{
    let asc=true;
    th.addEventListener('click',()=>{{
      document.querySelectorAll('th').forEach(t=>t.classList.remove('sort-asc','sort-desc'));
      th.classList.add(asc?'sort-asc':'sort-desc');
      const col=th.dataset.col;
      filtered.sort((a,b)=>{{
        let av='',bv='';
        const cellIdx={{'date':0,'product':1,'firm':2,'pathogen':3,'class':4,'dist':5,'summary':6}};
        const idx=cellIdx[col]??0;
        av=a.cells[idx]?.textContent||'';
        bv=b.cells[idx]?.textContent||'';
        return asc?av.localeCompare(bv):bv.localeCompare(av);
      }});
      asc=!asc;currentPage=1;render();
    }});
  }});
  render();
}})();
</script>
</body>
</html>"""

    out = Path(REPORT_OUTPUT)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"Report written → {REPORT_OUTPUT}  ({len(all_recalls)} recalls)")
    return str(out.resolve())

if __name__ == "__main__":
    generate_report()
