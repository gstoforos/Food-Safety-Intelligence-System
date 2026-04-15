"""
FSIS Global v3 — Dashboard Report Generator
Reads ALL records from SQLite DB and generates docs/index.html
This is what runs after every scrape cycle.
"""
import json, logging, sqlite3
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("report.dashboard")

# ── Inline DB read (no import needed if run standalone) ──────────────────────
def _get_all(db_path="fsis_global.db"):
    if not Path(db_path).exists():
        return []
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    rows = c.execute("""
        SELECT * FROM recalls
        WHERE superseded=0
        ORDER BY recall_date DESC
    """).fetchall()
    c.close()
    return [dict(r) for r in rows]

def _stats(rows):
    paths = {}
    srcs  = {}
    countries = set()
    outbreaks = 0
    for r in rows:
        p = r.get("pathogen_ai") or r.get("pathogen") or "Unknown"
        p = p.split("/")[0].strip()
        paths[p] = paths.get(p,0)+1
        srcs[r.get("source","?")] = srcs.get(r.get("source","?"),0)+1
        if r.get("country"): countries.add(r["country"])
        if r.get("is_outbreak"): outbreaks+=1
    tier1 = sum(1 for r in rows if _tier(r.get("pathogen_ai") or r.get("pathogen",""))==1)
    return {
        "total": len(rows),
        "tier1": tier1,
        "outbreaks": outbreaks,
        "countries": len(countries),
        "top_paths": sorted(paths.items(), key=lambda x:-x[1])[:10],
        "top_srcs":  sorted(srcs.items(),  key=lambda x:-x[1])[:12],
    }

TIER = {
    "Clostridium botulinum":1,"C. botulinum":1,"Botulinum":1,
    "E. coli O157":1,"E. coli O157:H7":1,"STEC":1,
    "Listeria monocytogenes":1,"Aflatoxin B1":1,"Aflatoxin":1,
    "BSE":1,"Hepatitis A":1,"Cereulide":1,
    "Salmonella":2,"Listeria":2,"Campylobacter":2,"Vibrio":2,
    "Norovirus":2,"Cyclospora":2,"Bacillus cereus":2,
    "E. coli":3,"Shigella":3,"Yersinia":3,
}

def _tier(p):
    p = (p or "").strip()
    for k,v in TIER.items():
        if k.lower() in p.lower(): return v
    return 3

PBADGE = {
    1: ("Listeria monocytogenes","b-l"),
    2: ("Salmonella","b-s"),
    3: ("E. coli","b-e"),
    4: ("C. botulinum","b-b"),
    5: ("Norovirus","b-n"),
    6: ("Aflatoxin","b-a"),
    7: ("Cereulide","b-c"),
}
def _pb(p):
    p=(p or "").lower()
    if "listeria" in p: return "b-l"
    if "salmonella" in p: return "b-s"
    if "e. coli" in p or "stec" in p or "o157" in p: return "b-e"
    if "botulinum" in p or "botulism" in p: return "b-b"
    if "norovirus" in p: return "b-n"
    if "aflatoxin" in p or "mycotoxin" in p: return "b-a"
    if "cereulide" in p or "bacillus cereus" in p: return "b-c"
    return "b-o"

SRC_CSS = {
    "FDA":      "background:rgba(91,155,213,.12);color:#5b9bd5;border:1px solid rgba(91,155,213,.25);",
    "USDA":     "background:rgba(91,155,213,.12);color:#5b9bd5;border:1px solid rgba(91,155,213,.25);",
    "USDA-FSIS":"background:rgba(91,155,213,.12);color:#5b9bd5;border:1px solid rgba(91,155,213,.25);",
    "RASFF":    "background:rgba(76,175,128,.12);color:#80d4a8;border:1px solid rgba(76,175,128,.25);",
    "RASFF (EU)":"background:rgba(76,175,128,.12);color:#80d4a8;border:1px solid rgba(76,175,128,.25);",
    "EFET_GR":  "background:rgba(30,136,229,.12);color:#64b5f6;border:1px solid rgba(30,136,229,.25);",
    "AFSCA_BE": "background:rgba(255,193,7,.12);color:#ffd54f;border:1px solid rgba(255,193,7,.25);",
    "BVL_DE":   "background:rgba(244,67,54,.12);color:#ef9a9a;border:1px solid rgba(244,67,54,.25);",
    "RAPPELCONSO_FR":"background:rgba(206,147,216,.12);color:#ce93d8;border:1px solid rgba(206,147,216,.25);",
    "CFIA":     "background:rgba(255,112,67,.12);color:#ffab91;border:1px solid rgba(255,112,67,.25);",
    "FSANZ":    "background:rgba(77,182,172,.12);color:#80cbc4;border:1px solid rgba(77,182,172,.25);",
    "FSA_UK":   "background:rgba(158,158,158,.12);color:#bdbdbd;border:1px solid rgba(158,158,158,.25);",
    "FSN":      "background:rgba(255,255,255,.05);color:#777;border:1px solid #2a2a2a;",
    "NEWS":     "background:rgba(255,255,255,.05);color:#777;border:1px solid #2a2a2a;",
}
def _src_badge(s):
    css = SRC_CSS.get(s, "background:rgba(255,255,255,.05);color:#666;border:1px solid #2a2a2a;")
    label = s.replace("_GR","").replace("_FR","").replace("_BE","").replace("_DE","").replace("_UK","").replace("_EU","").replace(" (EU)","")
    return f'<span style="display:inline-block;padding:2px 6px;border-radius:3px;font-size:9px;font-family:monospace;font-weight:700;{css}">{label}</span>'

def _tier_badge(p):
    t=_tier(p)
    if t==1: return'<span style="background:rgba(229,57,53,.1);color:#ef5350;border:1px solid rgba(229,57,53,.25);padding:2px 6px;border-radius:3px;font-size:9px;font-family:monospace;font-weight:700;">TIER-1</span>'
    if t==2: return'<span style="background:rgba(232,96,26,.1);color:#E8601A;border:1px solid rgba(232,96,26,.25);padding:2px 6px;border-radius:3px;font-size:9px;font-family:monospace;">TIER-2</span>'
    return'<span style="background:rgba(212,160,23,.1);color:#d4a017;padding:2px 6px;border-radius:3px;font-size:9px;font-family:monospace;">TIER-3</span>'

def _esc(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")[:120]


def generate(db_path="fsis_global.db"):
    rows = _get_all(db_path)
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Sort: Tier-1 first, then by date desc
    rows.sort(key=lambda r: (
        _tier(r.get("pathogen_ai") or r.get("pathogen","")),
        -int((r.get("recall_date") or "").replace("-","") or 0),
    ))

    st = _stats(rows)

    # KPI cards
    kpis = f"""
    <div class="kpi-row">
      <div class="kpi"><div class="kv">{st['total']}</div><div class="kl">Total Recalls in DB</div></div>
      <div class="kpi r"><div class="kv">{st['tier1']}</div><div class="kl">Tier-1 Critical</div></div>
      <div class="kpi a"><div class="kv">{st['outbreaks']}</div><div class="kl">Active Outbreaks</div></div>
      <div class="kpi b"><div class="kv">{len(set((r.get('pathogen_ai') or r.get('pathogen','')).split('/')[0].strip() for r in rows if r.get('pathogen_ai') or r.get('pathogen')))}</div><div class="kl">Distinct Pathogens</div></div>
      <div class="kpi g"><div class="kv">{st['countries']}</div><div class="kl">Countries</div></div>
      <div class="kpi p"><div class="kv">63</div><div class="kl">Sources</div></div>
    </div>"""

    # Side panels
    path_rows = "".join(
        f'<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #1e1e1e;">'
        f'<span style="font-size:12px;">{_esc(p[0])}</span>'
        f'<span style="color:#E8601A;font-weight:700;font-family:monospace;">{p[1]}</span></div>'
        for p in st["top_paths"]
    )
    src_rows = "".join(
        f'<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #1e1e1e;">'
        f'<span style="font-size:11px;">{_esc(s[0])}</span>'
        f'<span style="color:#E8601A;font-weight:700;font-family:monospace;font-size:11px;">{s[1]}</span></div>'
        for s in st["top_srcs"]
    )

    # Table rows — ALL records, JS handles pagination
    js_data = []
    for r in rows:
        pathogen = r.get("pathogen_ai") or r.get("pathogen") or "—"
        product  = (r.get("product") or "")[:80]
        firm     = (r.get("firm") or "")[:60]
        src      = r.get("source","")
        country  = r.get("country","")
        date     = r.get("recall_date","")
        url      = r.get("url","") or "#"
        is_ob    = 1 if r.get("is_outbreak") else 0
        tier     = _tier(pathogen)
        js_data.append([date, src, firm, product, pathogen, country, is_ob, url, tier])

    js_data_json = json.dumps(js_data)

    html = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AFTS · Global Food Safety Intelligence</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:#0e0e0e;color:#f0f0f0;font-family:'DM Sans',sans-serif;font-size:14px;}}
.hdr{{background:#161616;border-bottom:2px solid #E8601A;padding:14px 24px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:100;}}
.brand{{font-family:'Syne',sans-serif;font-size:17px;font-weight:800;}}.brand span{{color:#E8601A;}}
.meta{{font-family:'DM Mono',monospace;font-size:10px;color:#666;text-align:right;line-height:1.8;}}
.ticker{{background:#120800;border-bottom:1px solid #3a1800;padding:8px 24px;display:flex;gap:20px;flex-wrap:wrap;align-items:center;}}
.dot{{width:6px;height:6px;border-radius:50%;background:#E8601A;box-shadow:0 0 5px #E8601A;animation:blink 1.5s infinite;flex-shrink:0;}}
@keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
.wrap{{padding:18px 24px;max-width:1600px;margin:0 auto;}}
.kpi-row{{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:16px;}}
.kpi{{background:#161616;border:1px solid #222;border-radius:6px;padding:12px 16px;border-top:2px solid #E8601A;}}
.kpi.r{{border-top-color:#ef5350;}}.kpi.a{{border-top-color:#d4a017;}}.kpi.b{{border-top-color:#5b9bd5;}}.kpi.g{{border-top-color:#4caf80;}}.kpi.p{{border-top-color:#ce93d8;}}
.kv{{font-family:'Syne',sans-serif;font-size:26px;font-weight:800;color:#E8601A;line-height:1;}}
.kpi.r .kv{{color:#ef5350;}}.kpi.a .kv{{color:#d4a017;}}.kpi.b .kv{{color:#5b9bd5;}}.kpi.g .kv{{color:#4caf80;}}.kpi.p .kv{{color:#ce93d8;}}
.kl{{font-size:10px;color:#666;letter-spacing:.08em;text-transform:uppercase;margin-top:3px;}}
.grid2{{display:grid;grid-template-columns:2fr 1fr;gap:14px;margin-bottom:16px;}}
.panel{{background:#161616;border:1px solid #222;border-radius:6px;padding:16px;}}
.side-grid{{display:grid;grid-template-columns:1fr 1fr;gap:8px;}}
.sec{{font-family:'Syne',sans-serif;font-size:10px;font-weight:700;letter-spacing:.12em;color:#E8601A;text-transform:uppercase;margin-bottom:10px;padding-bottom:5px;border-bottom:1px solid #222;}}
.filters{{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:12px;align-items:center;}}
.filters input,.filters select{{background:#1e1e1e;border:1px solid #2a2a2a;color:#f0f0f0;padding:7px 10px;border-radius:4px;font-size:12px;font-family:'DM Mono',monospace;outline:none;}}
.filters input{{flex:1;min-width:200px;}}.filters input::placeholder{{color:#444;}}
.tbl-wrap{{overflow-x:auto;border:1px solid #1e1e1e;border-radius:6px;}}
table{{width:100%;border-collapse:collapse;font-size:12px;}}
thead{{background:#1a1a1a;}}
th{{text-align:left;padding:9px 10px;font-size:10px;font-weight:600;color:#555;letter-spacing:.1em;white-space:nowrap;cursor:pointer;}}
th:hover{{color:#f0f0f0;}}
td{{padding:7px 10px;border-bottom:1px solid #161616;vertical-align:middle;}}
tr:last-child td{{border-bottom:none;}}tr:hover td{{background:rgba(232,96,26,.04);}}
.badge{{display:inline-block;padding:2px 7px;border-radius:3px;font-size:10px;font-weight:700;white-space:nowrap;font-family:monospace;}}
.b-l{{background:rgba(232,96,26,.15);color:#ff8c5a;border:1px solid rgba(232,96,26,.3);}}
.b-e{{background:rgba(212,160,23,.15);color:#f0c040;border:1px solid rgba(212,160,23,.3);}}
.b-s{{background:rgba(91,155,213,.15);color:#7ab8e8;border:1px solid rgba(91,155,213,.3);}}
.b-b{{background:rgba(156,39,176,.15);color:#ce93d8;border:1px solid rgba(156,39,176,.3);}}
.b-n{{background:rgba(76,175,128,.15);color:#80d4a8;border:1px solid rgba(76,175,128,.3);}}
.b-a{{background:rgba(255,152,0,.15);color:#ffb74d;border:1px solid rgba(255,152,0,.3);}}
.b-c{{background:rgba(233,30,99,.15);color:#f48fb1;border:1px solid rgba(233,30,99,.3);}}
.b-o{{background:rgba(255,255,255,.05);color:#666;border:1px solid #2a2a2a;}}
.ob{{display:inline-block;background:rgba(232,96,26,.15);border:1px solid rgba(232,96,26,.4);color:#E8601A;font-size:9px;padding:1px 5px;border-radius:2px;font-family:monospace;font-weight:700;margin-left:4px;}}
.nav{{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px;font-size:11px;font-family:monospace;}}
.nav a{{color:#666;text-decoration:none;padding:4px 10px;border:1px solid #2a2a2a;border-radius:3px;}}
.nav a:hover,.nav a.on{{color:#E8601A;border-color:#E8601A;}}
.pag{{display:flex;justify-content:space-between;align-items:center;margin-top:12px;flex-wrap:wrap;gap:8px;}}
.pbtns{{display:flex;gap:4px;flex-wrap:wrap;}}
.pb{{background:#1e1e1e;border:1px solid #2a2a2a;color:#666;padding:5px 9px;border-radius:3px;cursor:pointer;font-size:11px;font-family:monospace;}}
.pb:hover,.pb.on{{background:#E8601A;color:#fff;border-color:#E8601A;}}
.pi{{font-family:monospace;font-size:11px;color:#555;}}
.footer{{text-align:center;padding:16px;font-size:11px;color:#555;border-top:1px solid #1e1e1e;font-family:monospace;}}
.footer a{{color:#E8601A;text-decoration:none;}}
@media(max-width:700px){{.kpi-row{{grid-template-columns:repeat(2,1fr);}}.grid2{{grid-template-columns:1fr;}}}}
</style></head><body>

<div class="hdr">
  <div class="brand">AFTS <span>·</span> Food Safety Intelligence</div>
  <div class="meta">63 Sources · FDA · USDA · RASFF · CFIA · ΕΦΕΤ · 50+ countries · Refresh: 4h<br>Last update: {now_utc}</div>
</div>
<div class="ticker">
  <span style="font-family:monospace;font-size:10px;color:#E8601A;font-weight:700;letter-spacing:.1em;">LIVE</span>
  <div style="display:flex;align-items:center;gap:6px;"><div class="dot"></div><span style="color:#E8601A;font-size:11px;font-weight:700;font-family:monospace;">TIER-1</span><span style="font-size:11px;color:#ccc;">ByHeart Infant Formula · C. botulinum · 51 infants hospitalized · USA</span></div>
  <div style="display:flex;align-items:center;gap:6px;"><div class="dot"></div><span style="color:#E8601A;font-size:11px;font-weight:700;font-family:monospace;">TIER-1</span><span style="font-size:11px;color:#ccc;">RAW FARM LLC · E. coli O157:H7 · 9 ill, 3 hosp · Active · USA</span></div>
  <div style="display:flex;align-items:center;gap:6px;"><div class="dot" style="background:#d4a017;box-shadow:0 0 5px #d4a017;"></div><span style="color:#d4a017;font-size:11px;font-weight:700;font-family:monospace;">INVESTIGATION</span><span style="font-size:11px;color:#ccc;">ΕΦΕΤ + ΕΟΔΥ · Φέτα Βυτίνας ΠΟΠ · Listeria · Greece</span></div>
  <div style="display:flex;align-items:center;gap:6px;"><div class="dot" style="background:#ce93d8;box-shadow:0 0 5px #ce93d8;"></div><span style="color:#ce93d8;font-size:11px;font-weight:700;font-family:monospace;">TIER-1</span><span style="font-size:11px;color:#ccc;">SMA / Aptamil / Danone Global · Cereulide (B. cereus) · Infant Formula · UK/SG/IE/DE/TW/AU</span></div>
</div>

<div class="wrap">
  <div class="nav" style="margin-top:14px;">
    <a href="index.html" class="on">🔴 Live Dashboard</a>
    <a href="weekly/">📊 Weekly</a>
    <a href="monthly/">📈 Monthly + AI</a>
    <a href="yearly/">📅 Yearly</a>
    <a href="alerts/">🔔 Alerts</a>
    <a href="alerts/feed.json" style="color:#4caf80;border-color:#4caf80;">↓ JSON Feed</a>
  </div>

  {kpis}

  <div class="grid2">
    <div class="panel">
      <div class="sec">Top Pathogens (All Sources)</div>
      {path_rows}
    </div>
    <div class="side-grid">
      <div class="panel">
        <div class="sec">By Source</div>
        {src_rows}
      </div>
      <div class="panel">
        <div class="sec">Bio Risk Tiers</div>
        <div style="background:#1a0800;border:1px solid #4a2000;border-left:3px solid #ef5350;border-radius:4px;padding:10px;margin-bottom:8px;">
          <div style="color:#ef5350;font-weight:700;font-size:11px;font-family:monospace;">TIER-1 CRITICAL</div>
          <div style="font-size:11px;color:#aaa;margin-top:4px;">C. botulinum · Listeria monocytogenes · E. coli O157 · Aflatoxin B1 · Cereulide · Hepatitis A</div>
        </div>
        <div style="background:#1a0a00;border:1px solid #3a1e00;border-left:3px solid #E8601A;border-radius:4px;padding:10px;margin-bottom:8px;">
          <div style="color:#E8601A;font-weight:700;font-size:11px;font-family:monospace;">TIER-2 SERIOUS</div>
          <div style="font-size:11px;color:#aaa;margin-top:4px;">Salmonella · Campylobacter · Vibrio · Norovirus · Cyclospora</div>
        </div>
        <div style="background:#181400;border:1px solid #2a2200;border-left:3px solid #d4a017;border-radius:4px;padding:10px;">
          <div style="color:#d4a017;font-weight:700;font-size:11px;font-family:monospace;">TIER-3 MODERATE</div>
          <div style="font-size:11px;color:#aaa;margin-top:4px;">Generic E. coli · Shigella · Yersinia · Low-dose Aflatoxin</div>
        </div>
      </div>
    </div>
  </div>

  <div class="panel">
    <div class="sec">All Pathogen Recalls — <span id="vc"></span></div>
    <div class="filters">
      <input type="text" id="srch" placeholder="Search firm, product, country, pathogen, lot...">
      <select id="srcf">
        <option value="">All Sources</option>
        <option value="fda">FDA (USA)</option>
        <option value="usda">USDA (USA)</option>
        <option value="cfia">CFIA (Canada)</option>
        <option value="rasff">RASFF (EU)</option>
        <option value="efet">ΕΦΕΤ (Greece)</option>
        <option value="afsca">AFSCA (Belgium)</option>
        <option value="bvl">BVL (Germany)</option>
        <option value="rappelconso">RappelConso (France)</option>
        <option value="fsa">FSA (UK)</option>
        <option value="fsanz">FSANZ (AU/NZ)</option>
        <option value="news">News</option>
      </select>
      <select id="pathf">
        <option value="">All Pathogens</option>
        <option value="listeria">Listeria</option>
        <option value="salmonella">Salmonella</option>
        <option value="e. coli">E. coli / STEC</option>
        <option value="o157">E. coli O157</option>
        <option value="botulinum">C. botulinum</option>
        <option value="aflatoxin">Aflatoxin</option>
        <option value="norovirus">Norovirus</option>
        <option value="cereulide">Cereulide / B. cereus</option>
        <option value="campylobacter">Campylobacter</option>
        <option value="cyclospora">Cyclospora</option>
      </select>
      <select id="tierf">
        <option value="">All Tiers</option>
        <option value="1">Tier-1 Critical only</option>
        <option value="2">Tier-2 Serious only</option>
        <option value="3">Tier-3 Moderate only</option>
      </select>
      <select id="rpp">
        <option value="50">50/page</option>
        <option value="100">100/page</option>
        <option value="9999">All records</option>
      </select>
      <span class="pi" id="fcnt"></span>
    </div>
    <div class="tbl-wrap">
      <table><thead><tr>
        <th>Date</th><th>Source</th><th>Firm / Producer</th><th>Product</th>
        <th>Pathogen</th><th>Bio Risk</th><th>Country</th><th>Link</th>
      </tr></thead>
      <tbody id="tbody"></tbody></table>
    </div>
    <div class="pag"><span class="pi" id="pi"></span><div class="pbtns" id="pbtns"></div></div>
  </div>
</div>

<div class="footer">
  <a href="https://advfood.tech">AFTS · Advanced Food-Tech Solutions</a> &nbsp;·&nbsp;
  FDA · USDA · RASFF · CFIA · ΕΦΕΤ · FSA UK · FSANZ · BVL · RappelConso · AFSCA · 63+ sources
  &nbsp;·&nbsp; <a href="weekly/">Weekly</a> · <a href="monthly/">Monthly</a> · <a href="yearly/">Yearly</a>
  &nbsp;·&nbsp; Bio Risk Tier: Tier-1 > FDA Class I
</div>

<script>
const ALL={js_data_json};
const SRC_CSS={{
  "FDA":"background:rgba(91,155,213,.12);color:#5b9bd5;border:1px solid rgba(91,155,213,.25);",
  "USDA":"background:rgba(91,155,213,.12);color:#5b9bd5;border:1px solid rgba(91,155,213,.25);",
  "USDA-FSIS":"background:rgba(91,155,213,.12);color:#5b9bd5;border:1px solid rgba(91,155,213,.25);",
  "RASFF":"background:rgba(76,175,128,.12);color:#80d4a8;border:1px solid rgba(76,175,128,.25);",
  "RASFF (EU)":"background:rgba(76,175,128,.12);color:#80d4a8;border:1px solid rgba(76,175,128,.25);",
  "EFET_GR":"background:rgba(30,136,229,.12);color:#64b5f6;border:1px solid rgba(30,136,229,.25);",
  "AFSCA_BE":"background:rgba(255,193,7,.12);color:#ffd54f;border:1px solid rgba(255,193,7,.25);",
  "BVL_DE":"background:rgba(244,67,54,.12);color:#ef9a9a;border:1px solid rgba(244,67,54,.25);",
  "RAPPELCONSO_FR":"background:rgba(206,147,216,.12);color:#ce93d8;border:1px solid rgba(206,147,216,.25);",
  "CFIA":"background:rgba(255,112,67,.12);color:#ffab91;border:1px solid rgba(255,112,67,.25);",
  "FSANZ":"background:rgba(77,182,172,.12);color:#80cbc4;border:1px solid rgba(77,182,172,.25);",
  "FSA_UK":"background:rgba(158,158,158,.12);color:#bdbdbd;border:1px solid rgba(158,158,158,.25);",
}};
const PBADGE={{"Listeria":"b-l","Salmonella":"b-s","E. coli":"b-e","STEC":"b-e","O157":"b-e","botulinum":"b-b","Norovirus":"b-n","Aflatoxin":"b-a","Cereulide":"b-c","Bacillus":"b-c"}};
function pb(p){{p=(p||"").toLowerCase();if(p.includes("listeria"))return"b-l";if(p.includes("salmonella"))return"b-s";if(p.includes("o157")||p.includes("stec")||p.includes("e. coli"))return"b-e";if(p.includes("botulinum"))return"b-b";if(p.includes("norovirus"))return"b-n";if(p.includes("aflatoxin"))return"b-a";if(p.includes("cereulide")||p.includes("bacillus"))return"b-c";return"b-o";}}
function tb(t){{if(t==1)return'<span style="background:rgba(229,57,53,.1);color:#ef5350;border:1px solid rgba(229,57,53,.25);padding:2px 6px;border-radius:3px;font-size:9px;font-family:monospace;font-weight:700;">TIER-1</span>';if(t==2)return'<span style="background:rgba(232,96,26,.1);color:#E8601A;border:1px solid rgba(232,96,26,.25);padding:2px 6px;border-radius:3px;font-size:9px;font-family:monospace;">TIER-2</span>';return'<span style="background:rgba(212,160,23,.1);color:#d4a017;padding:2px 6px;border-radius:3px;font-size:9px;font-family:monospace;">TIER-3</span>';}}
function sc(s){{const css=SRC_CSS[s]||"background:rgba(255,255,255,.05);color:#666;border:1px solid #2a2a2a;";const l=s.replace("_GR","").replace("_FR","").replace("_BE","").replace("_DE","").replace("_UK","").replace(" (EU)","");return`<span style="display:inline-block;padding:2px 6px;border-radius:3px;font-size:9px;font-family:monospace;font-weight:700;${{css}}">${{l}}</span>`;}}

let filtered=[...ALL],cur=1;

document.getElementById('vc').textContent=`(${{ALL.length}} total records · Tier-1 sorted first)`;

function applyFilters(){{
  const q=document.getElementById('srch').value.toLowerCase();
  const sf=document.getElementById('srcf').value.toLowerCase();
  const pf=document.getElementById('pathf').value.toLowerCase();
  const tf=document.getElementById('tierf').value;
  filtered=ALL.filter(r=>{{
    const hay=(r[2]+' '+r[3]+' '+r[4]+' '+r[5]).toLowerCase();
    if(q&&!hay.includes(q))return false;
    if(sf&&!r[1].toLowerCase().includes(sf))return false;
    if(pf&&!r[4].toLowerCase().includes(pf))return false;
    if(tf&&String(r[8])!==tf)return false;
    return true;
  }});
  cur=1;render();
}}

function render(){{
  const n=parseInt(document.getElementById('rpp').value);
  const tot=filtered.length;
  const pages=Math.max(1,Math.ceil(tot/n));
  cur=Math.min(cur,pages);
  const s=(cur-1)*n,e=Math.min(s+n,tot);
  document.getElementById('fcnt').textContent=`${{tot}} records`;
  const tbody=document.getElementById('tbody');
  tbody.innerHTML=filtered.slice(s,e).map(r=>{{
    const ob=r[6]?'<span class="ob">OUTBREAK</span>':'';
    const link=r[7]&&r[7]!='#'?`<a href="${{r[7]}}" target="_blank" style="color:#E8601A;font-size:11px;text-decoration:none;">↗</a>`:'—';
    return`<tr>
<td style="font-family:monospace;font-size:11px;color:#666;white-space:nowrap;">${{r[0]}}</td>
<td>${{sc(r[1])}}</td>
<td style="font-weight:500;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px;" title="${{r[2]}}">${{r[2]}}${{ob}}</td>
<td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#bbb;font-size:11px;" title="${{r[3]}}">${{r[3]}}</td>
<td><span class="badge ${{pb(r[4])}}">${{r[4]}}</span></td>
<td>${{tb(r[8])}}</td>
<td style="font-size:11px;color:#888;">${{r[5]}}</td>
<td>${{link}}</td>
</tr>`;
  }}).join('');
  document.getElementById('pi').textContent=tot>0?`${{s+1}}–${{e}} of ${{tot}}`:'No matches';
  const pb2=document.getElementById('pbtns');pb2.innerHTML='';
  if(pages<=1)return;
  const btn=(l,p,a)=>{{const b=document.createElement('button');b.className='pb'+(a?' on':'');b.textContent=l;b.onclick=()=>{{cur=p;render();}};pb2.appendChild(b);}};
  btn('«',1,false);if(cur>1)btn('‹',cur-1,false);
  let lo=Math.max(1,cur-2),hi=Math.min(pages,lo+4);lo=Math.max(1,hi-4);
  for(let i=lo;i<=hi;i++)btn(i,i,i===cur);
  if(cur<pages)btn('›',cur+1,false);btn('»',pages,false);
}}

['srch','srcf','pathf','tierf'].forEach(id=>document.getElementById(id).addEventListener(id==='srch'?'input':'change',applyFilters));
document.getElementById('rpp').addEventListener('change',applyFilters);
render();
</script>
</body></html>"""

    html = html.replace("{js_data_json}", js_data_json)

    Path("docs").mkdir(exist_ok=True)
    Path("docs/index.html").write_text(html, encoding="utf-8")
    log.info(f"Dashboard generated: {len(rows)} records → docs/index.html")
    return len(rows)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    db = sys.argv[1] if len(sys.argv) > 1 else "fsis_global.db"
    n = generate(db)
    print(f"Dashboard: {n} records")
