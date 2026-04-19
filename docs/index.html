<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AFTS · Food Safety Intelligence System</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
<style>
:root{--bg:#0a0e1a;--s1:#0f1420;--s2:#141a2a;--brd:#1d2436;--text:#e8edf5;--white:#fff;--muted:#9aa3b5;--dim:#5d6679;--green:#00ff88;--red:#ef4444;--orange:#f97316;--cyan:#06b6d4;--purple:#a78bfa;}
*{box-sizing:border-box;}
html,body{margin:0;padding:0;overflow-x:hidden;height:100%;overflow-y:auto;}
body{background:var(--bg);color:var(--text);font-family:'DM Sans',sans-serif;font-size:14px;}
.hdr{padding:18px 24px;background:var(--s1);border-bottom:1px solid var(--brd);display:flex;justify-content:space-between;align-items:flex-start;position:relative;}
.hdr::after{content:'';position:absolute;left:0;right:0;bottom:-1px;height:2px;background:linear-gradient(90deg,var(--green) 0%,transparent 35%);}
.brand{font-family:'Syne',sans-serif;font-weight:800;font-size:18px;letter-spacing:.02em;color:var(--white);text-transform:uppercase;}
.brand em{color:var(--green);font-style:normal;}
.brand small{display:block;font-family:'DM Mono',monospace;font-weight:400;font-size:9px;color:var(--dim);text-transform:uppercase;letter-spacing:.18em;margin-top:4px;}
.hdr-right{text-align:right;}
.live-pill{display:inline-block;background:rgba(0,255,136,.1);color:var(--green);font-family:'DM Mono',monospace;font-size:9px;font-weight:700;padding:4px 10px;border-radius:2px;letter-spacing:.1em;border:1px solid rgba(0,255,136,.3);margin-bottom:8px;}
.hdr-meta{font-family:'DM Mono',monospace;font-size:9px;color:var(--dim);text-align:right;line-height:1.9;}
.ticker{background:var(--s2);border-bottom:1px solid var(--brd);padding:8px 24px;overflow:hidden;white-space:nowrap;}
.ticker-inner{display:inline-block;animation:tick 60s linear infinite;}
@keyframes tick{0%{transform:translateX(0);}100%{transform:translateX(-50%);}}
.tick-item{display:inline-block;margin-right:36px;font-size:11px;color:var(--muted);}
.tick-tag{display:inline-block;font-family:'DM Mono',monospace;font-size:8px;font-weight:700;padding:1px 5px;border-radius:2px;margin-right:8px;letter-spacing:.06em;}
.t1-tag{background:rgba(239,68,68,.15);color:var(--red);border:1px solid rgba(239,68,68,.3);}
.t2-tag{background:rgba(249,115,22,.15);color:var(--orange);border:1px solid rgba(249,115,22,.3);}
.kpi-row{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;padding:14px 24px;background:var(--s1);border-bottom:1px solid var(--brd);}
.kpi{background:var(--s2);border:1px solid var(--brd);border-radius:3px;padding:14px 12px;text-align:center;}
.kv{font-family:'Syne',sans-serif;font-size:32px;font-weight:800;line-height:1;}
.kl{font-family:'DM Mono',monospace;font-size:9px;font-weight:700;color:var(--dim);text-transform:uppercase;letter-spacing:.1em;margin-top:6px;}
.nav-bar{display:flex;flex-wrap:wrap;gap:0;border-bottom:1px solid var(--brd);background:var(--s1);margin:18px 24px 0;border-radius:3px 3px 0 0;}
.tab{padding:11px 18px;font-family:'DM Mono',monospace;font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;cursor:pointer;color:var(--dim);border-bottom:2px solid transparent;white-space:nowrap;background:transparent;border-left:none;border-right:none;border-top:none;min-height:44px;display:flex;align-items:center;}
.tab.active{color:var(--green);border-bottom-color:var(--green);}
.tab:hover{color:var(--white);}
.wrap{padding:20px 24px;}
.grid2{display:grid;grid-template-columns:1fr 360px;gap:18px;}
.panel{background:var(--s1);border:1px solid var(--brd);border-radius:3px;padding:16px;}
.sec{font-family:'DM Mono',monospace;font-size:10px;font-weight:700;color:var(--dim);text-transform:uppercase;letter-spacing:.1em;margin-bottom:14px;}
.bar-row{display:flex;align-items:center;gap:10px;margin-bottom:7px;font-size:12px;}
.bar-label{flex:0 0 200px;font-family:'DM Mono',monospace;font-size:11px;}
.bar-track{flex:1;height:5px;background:var(--brd);border-radius:3px;overflow:hidden;}
.bar-fill{height:100%;border-radius:3px;}
.bar-val{flex:0 0 30px;text-align:right;font-family:'DM Mono',monospace;font-size:11px;font-weight:700;}
.src-row{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid var(--brd);font-size:12px;}
.src-row:last-child{border:none;}
.src-name{color:var(--text);}
.src-val{font-family:'DM Mono',monospace;font-weight:700;color:var(--green);}
.pbar{margin:14px 24px;display:flex;flex-wrap:wrap;gap:7px;}
.ptag{display:inline-block;font-family:'DM Mono',monospace;font-size:10px;font-weight:700;padding:8px 12px;border-radius:14px;cursor:pointer;border:1px solid transparent;letter-spacing:.04em;min-height:44px;display:flex;align-items:center;}
.fbar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;padding:12px 24px;background:var(--s1);border-top:1px solid var(--brd);border-bottom:1px solid var(--brd);}
.fbar input,.fbar select{background:var(--s2);border:1px solid var(--brd);color:var(--text);padding:10px 12px;border-radius:3px;font-family:inherit;font-size:12px;min-height:44px;}
.fbar input{flex:1;min-width:200px;}
.rcount{font-family:'DM Mono',monospace;font-size:10px;color:var(--dim);margin-left:auto;}
.tbl-wrap{margin:12px 24px 0;border:1px solid var(--brd);border-radius:3px;overflow-x:auto;overflow-y:visible;background:var(--s1);max-height:none;-webkit-overflow-scrolling:touch;}
table{width:100%;border-collapse:collapse;font-size:12px;}
th{position:sticky;top:0;background:var(--s2);color:var(--dim);font-family:'DM Mono',monospace;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;text-align:left;padding:12px 8px;border-bottom:1px solid var(--brd);cursor:pointer;min-height:44px;}
td{padding:12px 8px;border-bottom:1px solid var(--brd);vertical-align:top;}
tr:hover td{background:rgba(255,255,255,.02);}
.firm{font-weight:600;color:var(--white);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px;}
.prod{color:var(--muted);max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px;}
.r1{border-left:3px solid var(--red);}
.r2{border-left:3px solid var(--orange);}
.loading{text-align:center;color:var(--dim);padding:30px;font-style:italic;}
.pager{padding:16px 24px;display:flex;gap:12px;align-items:center;justify-content:center;flex-wrap:wrap;}
.pb{background:var(--s2);border:1px solid var(--brd);color:var(--text);padding:10px 16px;border-radius:3px;cursor:pointer;font-family:'DM Mono',monospace;font-size:10px;min-height:44px;}
.pb:disabled{opacity:.4;cursor:not-allowed;}
.pi{font-family:'DM Mono',monospace;font-size:10px;color:var(--dim);}
.footer{padding:24px;background:var(--s1);border-top:1px solid var(--brd);font-size:10px;color:var(--dim);font-family:'DM Mono',monospace;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:14px;}
.footer-brand{color:var(--green);font-weight:700;}
.footer em{color:var(--white);font-style:normal;}
.flinks a{color:var(--dim);text-decoration:none;margin-right:14px;padding:8px 0;min-height:44px;display:inline-flex;align-items:center;}
.flinks a:hover{color:var(--green);}
.report-card{background:var(--s2);border:1px solid var(--brd);border-radius:3px;padding:18px;margin-bottom:12px;cursor:pointer;transition:border-color .15s;}
.report-card:hover{border-color:var(--green);}
.report-week{font-family:'DM Mono',monospace;font-size:10px;color:var(--dim);text-transform:uppercase;letter-spacing:.1em;}
.report-title{font-family:'Syne',sans-serif;font-size:18px;font-weight:700;color:var(--white);margin:6px 0 10px;}
.report-stats{display:flex;gap:18px;font-family:'DM Mono',monospace;font-size:11px;color:var(--muted);flex-wrap:wrap;}
.report-stats strong{color:var(--green);font-weight:700;font-size:14px;}
.report-stats .stat-tier1{color:var(--red);}
.report-stats .stat-outbreak{color:var(--orange);}
.report-actions{margin-top:12px;display:flex;gap:10px;}
.report-btn{font-family:'DM Mono',monospace;font-size:9px;font-weight:700;padding:8px 14px;border-radius:2px;letter-spacing:.08em;text-decoration:none;text-transform:uppercase;min-height:44px;display:flex;align-items:center;justify-content:center;}
.report-btn.primary{background:rgba(0,255,136,.1);color:var(--green);border:1px solid rgba(0,255,136,.3);}
.report-btn.secondary{background:var(--s1);color:var(--dim);border:1px solid var(--brd);}
/* Archive: compact list for older weeks */
.arch-wrap{margin-top:22px;padding-top:18px;border-top:1px solid var(--brd);}
.arch-title{font-family:'DM Mono',monospace;font-size:10px;font-weight:700;color:var(--dim);text-transform:uppercase;letter-spacing:.12em;margin-bottom:12px;}
.arch-year{margin-bottom:10px;border:1px solid var(--brd);border-radius:3px;background:var(--s2);}
.arch-year[open]{background:var(--s1);}
.arch-year-head{padding:10px 14px;font-family:'DM Mono',monospace;font-size:11px;font-weight:700;color:var(--text);text-transform:uppercase;letter-spacing:.08em;cursor:pointer;list-style:none;min-height:44px;display:flex;align-items:center;}
.arch-year-head::-webkit-details-marker{display:none;}
.arch-year-head::before{content:'▸';margin-right:10px;color:var(--dim);font-size:10px;transition:transform .15s;}
.arch-year[open] .arch-year-head::before{transform:rotate(90deg);display:inline-block;}
.arch-list{border-top:1px solid var(--brd);}
.arch-row{display:grid;grid-template-columns:56px 1fr 52px 60px 1.5fr 28px;gap:12px;align-items:center;padding:10px 14px;font-family:'DM Mono',monospace;font-size:11px;text-decoration:none;color:var(--muted);border-bottom:1px solid var(--brd);min-height:44px;}
.arch-row:last-child{border-bottom:none;}
.arch-row:hover{background:var(--s2);color:var(--text);}
.arch-wk{color:var(--green);font-weight:700;}
.arch-dates{color:var(--dim);}
.arch-total{color:var(--text);font-weight:700;text-align:right;}
.arch-tier1{color:var(--red);font-weight:700;}
.arch-top{color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:'DM Sans',sans-serif;font-size:12px;}
.arch-go{color:var(--dim);text-align:right;}
@media(max-width:700px){.arch-row{grid-template-columns:48px 1fr 40px 46px 24px;gap:8px;font-size:10px;}.arch-top{display:none;}}
/* Enhanced Mobile Responsiveness */
@media(max-width:920px){
  .grid2{grid-template-columns:1fr;}
  .bar-label{flex:0 0 140px;font-size:10px;}
  .nav-bar{margin:12px 16px 0;}
  .wrap{padding:16px 20px;}
  .fbar{padding:12px 16px;}
  .tbl-wrap{margin:8px 16px 0;}
  .pbar{margin:12px 16px;}
}
@media(max-width:768px){
  .kpi-row{grid-template-columns:repeat(3,1fr);gap:8px;padding:12px 16px;}
  .kv{font-size:24px;}
  .kl{font-size:8px;}
  .hdr{padding:16px 20px;flex-direction:column;gap:12px;}
  .hdr-right{text-align:left;}
  .live-pill{display:none;}
  .brand{font-size:16px;}
  .brand small{font-size:8px;}
  .ticker{padding:6px 16px;}
  .tick-item{margin-right:24px;font-size:10px;}
  .tab{padding:12px 14px;font-size:9px;min-height:44px;display:flex;align-items:center;}
  .fbar{flex-direction:column;align-items:stretch;}
  .fbar input,.fbar select{margin-bottom:8px;min-height:44px;}
  .rcount{margin-left:0;text-align:center;}
}
@media(max-width:640px){
  .kpi-row{grid-template-columns:repeat(2,1fr);}
  .kv{font-size:20px;}
  .grid2{gap:12px;}
  .panel{padding:12px;}
  .bar-label{flex:0 0 100px;font-size:9px;}
  .bar-val{flex:0 0 25px;font-size:9px;}
  .src-row{font-size:11px;}
  /* Mobile-friendly table */
  .tbl-wrap{margin:8px 12px 0;}
  .wrap{padding:12px 16px;}
  .pbar{margin:8px 12px;gap:6px;}
  .ptag{padding:8px 12px;font-size:9px;min-height:44px;display:flex;align-items:center;}
  table thead{display:none;}
  table,tbody,tr{display:block;width:100%;}
  td{display:block;padding:4px 12px;border:none;text-align:left!important;}
  td:nth-child(1){background:var(--s2);font-weight:700;color:var(--white);padding:8px 12px;margin-top:12px;border-radius:3px 3px 0 0;}
  td:nth-child(2){font-size:10px;color:var(--green);padding:6px 12px 4px;}
  td:nth-child(3){font-size:14px!important;font-weight:700!important;color:var(--white)!important;padding:6px 12px 2px!important;max-width:none!important;white-space:normal!important;overflow:visible!important;}
  td:nth-child(4){font-size:12px!important;color:var(--muted)!important;white-space:normal!important;max-width:none!important;overflow:visible!important;padding:2px 12px 8px!important;}
  td:nth-child(5){font-size:11px!important;color:var(--cyan)!important;padding:4px 12px!important;font-family:'DM Mono',monospace!important;}
  td:nth-child(6),td:nth-child(7),td:nth-child(8){display:inline-block;margin-right:8px;padding:2px 12px!important;font-size:10px!important;color:var(--dim)!important;}
  td:nth-child(8){background:var(--s1);border-radius:0 0 3px 3px;width:100%;text-align:center!important;padding:8px 12px!important;}
  .r1,.r2{border-left:none;border-top:3px solid var(--red);}
  .r2{border-top-color:var(--orange);}
}
@media(max-width:480px){
  .hdr{padding:12px 16px;}
  .kpi-row{padding:8px 12px;gap:6px;}
  .kv{font-size:18px;}
  .fbar{padding:8px 12px;}
  .tab{padding:10px 12px;font-size:8px;}
  .nav-bar{margin:8px 12px 0;-webkit-overflow-scrolling:touch;}
  .tbl-wrap{margin:8px 8px 0;-webkit-overflow-scrolling:touch;}
  .wrap{padding:8px 12px;}
  .pbar{margin:8px 8px;}
  .footer{padding:16px 12px;flex-direction:column;text-align:center;}
  .flinks{display:flex;flex-wrap:wrap;justify-content:center;gap:8px;}
  .flinks a{padding:8px 0;min-height:44px;display:inline-flex;align-items:center;}
  .report-title{font-size:16px;}
  .report-stats{gap:12px;}
  .pb{min-height:44px;}
  .report-btn{min-height:44px;display:flex;align-items:center;justify-content:center;}
}
</style></head><body>

<div class="hdr"><div class="brand">AFTS <em>·</em> Food Safety <em>Intelligence</em> System<small>// AI-powered Process Validation Intelligence · under AFTS process authority · 66 sources · 60+ countries</small></div><div class="hdr-right"><span class="live-pill">● LIVE MONITORING</span><div class="hdr-meta" id="meta">Loading…</div></div></div>

<div class="ticker"><div class="ticker-inner" id="ticker">Loading latest alerts…</div></div>

<div class="kpi-row">
<div class="kpi"><div class="kv" id="k-total" style="color:var(--green)">—</div><div class="kl">Total Records</div></div>
<div class="kpi"><div class="kv" id="k-t1" style="color:var(--red)">—</div><div class="kl">Tier-1 Critical</div></div>
<div class="kpi"><div class="kv" id="k-ob" style="color:var(--orange)">—</div><div class="kl">Outbreaks</div></div>
<div class="kpi"><div class="kv" id="k-path" style="color:var(--cyan)">—</div><div class="kl">Pathogens</div></div>
<div class="kpi"><div class="kv" id="k-ctry" style="color:var(--purple)">—</div><div class="kl">Countries</div></div>
<div class="kpi"><div class="kv" id="k-src" style="color:var(--white)">—</div><div class="kl">Sources</div></div>
</div>

<div class="nav-bar">
<button class="tab active" id="tab-recalls" onclick="switchTab('recalls')">● Recall Intelligence</button>
<button class="tab" id="tab-news" onclick="switchTab('news')">🌐 Live News <span id="news-count" style="opacity:.55;font-weight:400"></span></button>
<button class="tab" id="tab-reports" onclick="switchTab('reports')">📊 Weekly <span id="rep-count" style="opacity:.55;font-weight:400"></span></button>
<button class="tab" id="tab-monthly" onclick="switchTab('monthly')">🗓 Monthly <span id="mon-count" style="opacity:.55;font-weight:400"></span></button>
<button class="tab" id="tab-yearly" onclick="return false" aria-disabled="true" title="Yearly reports activate January 2027, after 12 months of data" style="opacity:.35;cursor:not-allowed;color:var(--dim)">📆 Yearly <span style="font-family:'DM Mono',monospace;font-size:8px;font-weight:400;opacity:.65;margin-left:4px">soon</span></button>
<button class="tab" onclick="downloadRecallsXlsx()">↓ XLSX</button>
<button class="tab" onclick="window.open('alerts.html','_blank','noopener,width=980,height=820')" style="color:#fbbf24;border-color:rgba(251,191,36,.35);background:rgba(251,191,36,.08)">🔔 Alerts</button>
</div>

<div id="panel-recalls">
<div class="wrap"><div class="grid2"><div class="panel"><div class="sec">Top Pathogens — All Sources</div><div id="chart-path"></div></div><div class="panel"><div class="sec">By Source</div><div id="chart-src"></div></div></div></div>
<div class="pbar" id="pills"></div>
<div class="fbar"><input type="search" id="q" placeholder="Search firm, product, country, pathogen..."><select id="f-src"><option value="">All Sources</option></select><select id="f-path"><option value="">All Pathogens</option></select><select id="f-tier"><option value="">All Tiers</option><option value="1">Tier 1</option><option value="2">Tier 2</option></select><select id="f-region"><option value="">All Records</option><option value="Americas">Americas</option><option value="Europe">Europe</option><option value="Asia">Asia</option><option value="Oceania">Oceania</option><option value="Africa">Africa</option></select><select id="f-pp"><option value="50">50/page</option><option value="25">25/page</option><option value="100">100/page</option></select><span class="rcount" id="rcount"></span></div>
<div class="tbl-wrap"><table><thead><tr><th data-col="date">Date</th><th data-col="source">Source</th><th data-col="company">Firm / Producer</th><th data-col="product">Product</th><th data-col="pathogen">Pathogen</th><th data-col="tier">Risk</th><th data-col="country">Country</th><th>Link</th></tr></thead><tbody id="tbody"><tr><td colspan="8" class="loading">Loading data/recalls.xlsx …</td></tr></tbody></table></div>
<div class="pager"><button class="pb" id="prev" disabled>‹ Prev</button><span class="pi" id="pinfo">—</span><button class="pb" id="next">Next ›</button></div>
</div>

<div id="panel-news" style="display:none">
<div class="wrap"><div class="panel"><div class="sec">Live Food Safety News — last 7 days · auto-refresh from dedicated publishers</div>
<div class="fbar" style="border:none;padding:10px 0;background:transparent;">
<input type="search" id="news-q" placeholder="Search title, source, pathogen..."><select id="news-path"><option value="">All Pathogens</option></select><select id="news-src"><option value="">All Sources</option></select><span class="rcount" id="news-rcount"></span>
</div>
<div class="tbl-wrap"><table><thead><tr><th>Published</th><th>Pathogen</th><th>Event</th><th>Source</th><th>Title</th><th>Link</th></tr></thead><tbody id="news-tbody"><tr><td colspan="6" class="loading">Loading NEWS sheet…</td></tr></tbody></table></div>
</div></div>
</div>

<div id="panel-reports" style="display:none">
<div class="wrap"><div class="panel"><div class="sec">Weekly Pathogen Recall Reports · sent every Friday 10am</div>
<div id="reports-list"><p class="loading">Loading reports/index.json…</p></div>
</div></div>
</div>

<div id="panel-monthly" style="display:none">
<div class="wrap"><div class="panel"><div class="sec">Monthly Pathogen Surveillance · published 1st of each month</div>
<div id="monthly-list"><p class="loading">Loading monthly reports…</p></div>
</div></div>
</div>

<div class="footer"><div><div class="footer-brand">Advanced Food-Tech Solutions <em>·</em> AFTS</div><div class="flinks"><a href="https://www.advfood.tech" target="_blank">advfood.tech</a><a href="https://www.advfood.tech/food-safety-intelligence" target="_blank">FSIS Page</a><a href="mailto:info@advfood.tech">info@advfood.tech</a><a href="#" onclick="downloadRecallsXlsx();return false;">↓ XLSX</a></div></div><a href="https://www.advfood.tech" target="_blank" style="background:rgba(0,255,136,.08);border:1px solid rgba(0,255,136,.2);color:var(--green);font-family:'DM Mono',monospace;font-size:9px;padding:7px 14px;border-radius:2px;text-decoration:none;letter-spacing:.08em;">advfood.tech →</a></div>

<script>
let ALL=[],filtered=[],page=1,perPage=50,sortCol='date',sortAsc=false,pillFilter='';
let NEWS=[],NEWS_filtered=[];
const PC={'Listeria':'#00ff88','Salmonella':'#06b6d4','E. coli':'#f97316','Cereulide':'#ef4444','Clostridium':'#a78bfa','Aflatoxin':'#fbbf24','Hepatitis':'#f472b6','Norovirus':'#818cf8','Campylobacter':'#34d399','Vibrio':'#fb923c','Cronobacter':'#c084fc','Bacillus':'#ef4444','STEC':'#f97316'};
function pColor(p){if(!p)return'#00ff88';for(const[k,v]of Object.entries(PC))if(p.indexOf(k)>=0)return v;return'#00ff88';}
function esc(s){return(s==null?'':String(s)).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]);}

// ===== FIXED PATHOGEN NORMALIZATION =====
function normalizePathogen(p){
  if(!p)return'';
  // Defensive: anything >80 chars is boilerplate leakage (e.g. a page
  // intro paragraph that slipped past a buggy scraper), not a pathogen.
  // Dropping to '' here removes it from the filter dropdown entirely.
  if(p.length>80)return'';
  const low=p.toLowerCase();
  if(low.includes('listeria'))return'Listeria';
  if(low.includes('salmonella'))return'Salmonella';
  if(low.includes('e. coli')||low.includes('stec')||low.includes('o157')||low.includes('shiga'))return'E. coli / STEC';
  if(low.includes('botulin')||low.includes('clostridium'))return'C. botulinum';
  if(low.includes('norovirus'))return'Norovirus';
  if(low.includes('aflatoxin'))return'Aflatoxin';
  if(low.includes('cereulide')||low.includes('bacillus cereus'))return'Cereulide';
  if(low.includes('hepatit'))return'Hepatitis A';
  if(low.includes('campylobacter'))return'Campylobacter';
  if(low.includes('cyclospora'))return'Cyclospora';
  if(low.includes('vibrio'))return'Vibrio';
  if(low.includes('cronobacter'))return'Cronobacter';
  if(low.includes('histamine')||low.includes('scombro'))return'Histamine';
  if(low.includes('ochratoxin'))return'Ochratoxin A';
  if(low.includes('patulin'))return'Patulin';
  if(low.includes('biotoxin')||low.includes('shellfish')||low.includes('saxitoxin')||low.includes('domoic'))return'Marine biotoxins';
  if(low.includes('rodenticide')||low.includes('rat poison')||low.includes('bromadiolon'))return'Rodenticide';
  if(low.includes('heavy metal')||low.includes('lead contamin')||low.includes('cadmium')||low.includes('arsenic')||low.includes('mercury'))return'Heavy metals';
  if(low.includes('glass fragm')||low.includes('metal fragm')||low.includes('plastic fragm')||low.includes('foreign'))return'Physical contaminants';
  // Fallback for short, plausibly-pathogen-like values only. Strip
  // parentheticals and "spp" suffix but still cap to 60 chars.
  const cleaned=p.split('(')[0].split(' spp')[0].trim();
  return cleaned.length<=60?cleaned:'';
}

function switchTab(t){
  document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
  document.getElementById('panel-recalls').style.display=(t==='recalls'?'block':'none');
  document.getElementById('panel-news').style.display=(t==='news'?'block':'none');
  document.getElementById('panel-reports').style.display=(t==='reports'?'block':'none');
  document.getElementById('panel-monthly').style.display=(t==='monthly'?'block':'none');
  document.getElementById('tab-'+t).classList.add('active');
  if(t==='reports'&&!window._reports_loaded){loadReports();window._reports_loaded=true;}
  if(t==='monthly'&&!window._monthly_loaded){loadMonthlyReports();window._monthly_loaded=true;}
  notifyHeight();
}

function notifyHeight(){
  if(window.parent!==window){
    setTimeout(()=>{window.parent.postMessage({type:'fsis-height',height:document.body.scrollHeight+20},'*');},100);
  }
}

// ===== DOWNLOADS — Recalls only (strip Pending + NEWS) =====
// Rebuild a clean workbook from in-memory ALL so subscribers never receive
// the Pending worktable or the NEWS ingestion sheet in their download.
function _recallsExportRows(){
  return ALL.map(r=>({
    Date:r.date, Source:r.source, Company:r.company, Brand:r.brand,
    Product:r.product, Pathogen:r.pathogen, Reason:r.reason, Class:r.class_,
    Country:r.country, Region:r.region, Tier:r.tier, Outbreak:r.is_outbreak,
    URL:r.url, Notes:r.notes
  }));
}
function downloadRecallsXlsx(){
  if(!ALL.length){alert('Data is still loading — please try again in a moment.');return;}
  const rows=_recallsExportRows();
  const ws=XLSX.utils.json_to_sheet(rows);
  const wb=XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb,ws,'Recalls');
  const dt=new Date().toISOString().slice(0,10);
  XLSX.writeFile(wb,`afts-recalls-${dt}.xlsx`);
}
function downloadRecallsJson(){
  if(!ALL.length){alert('Data is still loading — please try again in a moment.');return;}
  const blob=new Blob([JSON.stringify(_recallsExportRows(),null,2)],{type:'application/json'});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  const dt=new Date().toISOString().slice(0,10);
  a.href=url; a.download=`afts-recalls-${dt}.json`;
  document.body.appendChild(a); a.click();
  setTimeout(()=>{URL.revokeObjectURL(url); a.remove();},100);
}

// ===== LOAD RECALLS + NEWS FROM SAME XLSX =====
fetch('data/recalls.xlsx').then(r=>r.arrayBuffer()).then(buf=>{
  const wb=XLSX.read(buf,{type:'array'});
  const ws=wb.Sheets['Recalls']||wb.Sheets[wb.SheetNames[0]];
  const raw=XLSX.utils.sheet_to_json(ws,{defval:''});
  ALL=raw.map(r=>({date:String(r.Date||''),source:String(r.Source||''),company:String(r.Company||''),brand:String(r.Brand||''),product:String(r.Product||''),pathogen:String(r.Pathogen||''),reason:String(r.Reason||''),class_:String(r.Class||''),country:String(r.Country||''),region:String(r.Region||''),tier:parseInt(r.Tier||3),is_outbreak:parseInt(r.Outbreak||0),url:String(r.URL||''),notes:String(r.Notes||'')}));
  ALL.sort((a,b)=>b.date.localeCompare(a.date));filtered=[...ALL];init();
  // Also load NEWS sheet from same buffer
  loadNewsFromBuffer(wb);
}).catch(e=>{document.getElementById('tbody').innerHTML='<tr><td colspan="8" class="loading">Failed: '+e.message+'</td></tr>';});

function loadNewsFromBuffer(wb){
  const ws=wb.Sheets['NEWS'];
  if(!ws){document.getElementById('news-tbody').innerHTML='<tr><td colspan="6" class="loading">NEWS sheet not present yet. Run Apps Script: NEWS_fetchFoodPathogenNews()</td></tr>';return;}
  const raw=XLSX.utils.sheet_to_json(ws,{defval:''});
  NEWS=raw.map(r=>({published:String(r['Published (UTC)']||r.published||''),pathogen:String(r.Pathogen||''),event:String(r.Event||''),source:String(r.Source||''),title:String(r.Title||''),link:String(r.Link||r.URL||''),retrieved:String(r['Retrieved (UTC)']||'')})).filter(x=>x.title&&x.title.indexOf('No food + pathogen news')<0);
  NEWS.sort((a,b)=>(a.published<b.published)?1:-1);
  NEWS_filtered=[...NEWS];
  document.getElementById('news-count').textContent=NEWS.length?'· '+NEWS.length:'';
  populateNewsFilters();renderNews();
}

function init(){
  renderKPIs();renderCharts();renderPills();populateFilters();renderTicker();render();
  document.getElementById('meta').innerHTML='Updated: '+(ALL[0]?ALL[0].date:'—')+' · '+ALL.length+' records<br>Source: data/recalls.xlsx';
  notifyHeight();
  if(window.parent!==window){new ResizeObserver(()=>{notifyHeight();}).observe(document.body);}
}
function renderKPIs(){
  document.getElementById('k-total').textContent=ALL.length;
  document.getElementById('k-t1').textContent=ALL.filter(r=>r.tier===1).length;
  document.getElementById('k-ob').textContent=ALL.filter(r=>r.is_outbreak===1).length;
  document.getElementById('k-path').textContent=new Set(ALL.map(r=>normalizePathogen(r.pathogen)).filter(Boolean)).size;
  document.getElementById('k-ctry').textContent=new Set(ALL.map(r=>r.country).filter(Boolean)).size;
  document.getElementById('k-src').textContent=new Set(ALL.map(r=>r.source).filter(Boolean)).size;
}
function renderCharts(){
  const pc={};ALL.forEach(r=>{const p=normalizePathogen(r.pathogen);if(p)pc[p]=(pc[p]||0)+1;});
  const ps=Object.entries(pc).sort((a,b)=>b[1]-a[1]).slice(0,10),mx=ps[0]?ps[0][1]:1;
  document.getElementById('chart-path').innerHTML=ps.map(([k,v])=>{const c=pColor(k);return`<div class="bar-row"><span class="bar-label" style="color:${c}">${esc(k)}</span><div class="bar-track"><div class="bar-fill" style="width:${(v/mx*100).toFixed(0)}%;background:${c}"></div></div><span class="bar-val" style="color:${c}">${v}</span></div>`;}).join('');
  const sc={};ALL.forEach(r=>{if(r.source)sc[r.source]=(sc[r.source]||0)+1;});
  document.getElementById('chart-src').innerHTML=Object.entries(sc).sort((a,b)=>b[1]-a[1]).slice(0,12).map(([k,v])=>`<div class="src-row"><span class="src-name">${esc(k)}</span><span class="src-val">${v}</span></div>`).join('');
}
function renderPills(){
  const ns=[['Listeria','Listeria'],['Salmonella','Salmonella'],['E. coli / STEC','E. coli / STEC'],['C. botulinum','C. botulinum'],['Norovirus','Norovirus'],['Aflatoxin','Aflatoxin'],['Cereulide','Cereulide']];
  const cs=['#00ff88','#06b6d4','#f97316','#a78bfa','#818cf8','#fbbf24','#ef4444'];
  let h=ns.map(([disp,fval],i)=>`<span class="ptag" data-f="${fval}" style="background:${cs[i]}11;color:${cs[i]};border-color:${cs[i]}55">${disp}</span>`).join('');
  h+=`<span class="ptag" data-f="" style="background:rgba(255,255,255,.05);color:var(--dim);border-color:var(--brd)">× All</span>`;
  document.getElementById('pills').innerHTML=h;
  document.querySelectorAll('.ptag').forEach(el=>el.addEventListener('click',()=>{pillFilter=el.dataset.f||'';applyFilters();}));
}
function populateFilters(){
  [...new Set(ALL.map(r=>r.source).filter(Boolean))].sort().forEach(s=>{const o=document.createElement('option');o.value=s;o.textContent=s;document.getElementById('f-src').appendChild(o);});
  // Use normalized pathogen names for dropdown (no duplicates)
  const normalized=[...new Set(ALL.map(r=>normalizePathogen(r.pathogen)).filter(Boolean))].sort();
  normalized.forEach(p=>{const o=document.createElement('option');o.value=p;o.textContent=p;document.getElementById('f-path').appendChild(o);});
}
['q','f-src','f-path','f-tier','f-region','f-pp'].forEach(id=>{const el=document.getElementById(id);el.addEventListener('input',()=>{pillFilter='';applyFilters();});el.addEventListener('change',()=>{pillFilter='';applyFilters();});});
function applyFilters(){
  const q=document.getElementById('q').value.toLowerCase(),src=document.getElementById('f-src').value,path=document.getElementById('f-path').value,tier=document.getElementById('f-tier').value,region=document.getElementById('f-region').value;
  perPage=parseInt(document.getElementById('f-pp').value)||50;
  filtered=ALL.filter(r=>{
    if(src&&r.source!==src)return false;
    if(path&&normalizePathogen(r.pathogen)!==path)return false;
    if(tier&&r.tier!==parseInt(tier))return false;if(region&&r.region!==region)return false;
    if(pillFilter){
      const normalized=normalizePathogen(r.pathogen);
      if(normalized.toLowerCase()!==pillFilter.toLowerCase())return false;
    }
    if(q){const h=(r.company+' '+r.brand+' '+r.product+' '+r.country+' '+r.pathogen+' '+r.source).toLowerCase();if(h.indexOf(q)<0)return false;}
    return true;});
  filtered.sort((a,b)=>sortAsc?String(a[sortCol]).localeCompare(String(b[sortCol])):String(b[sortCol]).localeCompare(String(a[sortCol])));
  page=1;render();
}
function render(){
  const start=(page-1)*perPage,slice=filtered.slice(start,start+perPage),tbody=document.getElementById('tbody');
  if(!slice.length){tbody.innerHTML='<tr><td colspan="8" class="loading">No matching records.</td></tr>';}
  else{tbody.innerHTML=slice.map(r=>{
    const tc=r.tier===1?'r1':r.tier===2?'r2':'';
    const tierBg=r.tier===1?'rgba(239,68,68,.15)':'rgba(249,115,22,.15)';
    const tierCol=r.tier===1?'var(--red)':'var(--orange)';
    const tl=r.tier===1?'TIER-1':'TIER-2';
    const ob=r.is_outbreak===1?' <span style="background:rgba(239,68,68,.1);color:var(--red);font-size:7px;padding:1px 5px;border-radius:2px;font-family:DM Mono,monospace;font-weight:700;letter-spacing:.08em;border:1px solid rgba(239,68,68,.25)">OUTBREAK</span>':'';
    return`<tr class="${tc}"><td style="font-family:'DM Mono',monospace;font-size:10px;color:var(--dim);white-space:nowrap">${esc(r.date)}</td><td><span style="font-family:'DM Mono',monospace;font-size:8px;padding:2px 7px;border-radius:2px;font-weight:700;letter-spacing:.05em;background:rgba(0,255,136,.08);color:var(--green);border:1px solid rgba(0,255,136,.25)">${esc(r.source)}</span></td><td class="firm" title="${esc(r.company)}">${esc(r.company||r.brand||'—')}</td><td class="prod" title="${esc(r.product)}">${esc((r.product||'').substring(0,80))}</td><td style="font-family:'DM Mono',monospace;font-size:9px;color:${pColor(r.pathogen)}">${esc(r.pathogen)}${ob}</td><td><span style="font-family:'DM Mono',monospace;font-size:8px;padding:2px 7px;border-radius:2px;font-weight:700;letter-spacing:.08em;background:${tierBg};color:${tierCol};border:1px solid ${tierCol}40">${tl}</span></td><td style="font-size:10px;color:var(--muted)">${esc(r.country)}</td><td>${r.url?`<a href="${esc(r.url)}" target="_blank" rel="noopener" style="color:var(--green);font-size:11px;text-decoration:none">↗</a>`:''}</td></tr>`;
  }).join('');}
  const total=filtered.length,maxP=Math.max(1,Math.ceil(total/perPage));
  document.getElementById('pinfo').textContent=`Page ${page} of ${maxP} · ${total} records`;
  document.getElementById('prev').disabled=page<=1;document.getElementById('next').disabled=page>=maxP;
  document.getElementById('rcount').textContent=total+' records';
  notifyHeight();
}
function renderTicker(){
  const recent=ALL.slice(0,8),items=recent.map(r=>{const tag=r.tier===1?'t1-tag':'t2-tag',label=r.tier===1?'TIER-1':'TIER-2';return`<span class="tick-item"><span class="tick-tag ${tag}">${label}</span>${esc(r.company||r.brand)} · ${esc(r.pathogen)} · ${esc(r.country)}</span>`;}).join('');
  document.getElementById('ticker').innerHTML=items+items;
}
document.querySelectorAll('th[data-col]').forEach(th=>th.addEventListener('click',()=>{const c=th.dataset.col;if(sortCol===c)sortAsc=!sortAsc;else{sortCol=c;sortAsc=true;}applyFilters();}));
document.getElementById('prev').addEventListener('click',()=>{page--;render();});
document.getElementById('next').addEventListener('click',()=>{page++;render();});

// ===== NEWS =====
function populateNewsFilters(){
  const ps=[...new Set(NEWS.map(r=>normalizePathogen(r.pathogen)).filter(Boolean))].sort();
  const ss=[...new Set(NEWS.map(r=>r.source).filter(Boolean))].sort();
  const psel=document.getElementById('news-path'),ssel=document.getElementById('news-src');
  psel.innerHTML='<option value="">All Pathogens</option>'+ps.map(p=>`<option value="${esc(p)}">${esc(p)}</option>`).join('');
  ssel.innerHTML='<option value="">All Sources</option>'+ss.map(s=>`<option value="${esc(s)}">${esc(s)}</option>`).join('');
  ['news-q','news-path','news-src'].forEach(id=>document.getElementById(id).addEventListener('input',applyNewsFilters));
}
function applyNewsFilters(){
  const q=document.getElementById('news-q').value.toLowerCase(),p=document.getElementById('news-path').value,s=document.getElementById('news-src').value;
  NEWS_filtered=NEWS.filter(r=>{
    if(p&&normalizePathogen(r.pathogen)!==p)return false;
    if(s&&r.source!==s)return false;
    if(q){const h=(r.title+' '+r.source+' '+r.pathogen+' '+r.event).toLowerCase();if(h.indexOf(q)<0)return false;}
    return true;});
  renderNews();
}
function renderNews(){
  const tb=document.getElementById('news-tbody');
  if(!NEWS_filtered.length){tb.innerHTML='<tr><td colspan="6" class="loading">No matching news.</td></tr>';document.getElementById('news-rcount').textContent='0 items';return;}
  tb.innerHTML=NEWS_filtered.slice(0,200).map(r=>{
    const dateOnly=(r.published||'').substring(0,10);const pcol=pColor(r.pathogen);
    return`<tr><td style="font-family:'DM Mono',monospace;font-size:10px;color:var(--dim);white-space:nowrap">${esc(dateOnly)}</td><td style="font-family:'DM Mono',monospace;font-size:9px;color:${pcol}">${esc(r.pathogen)}</td><td style="font-size:10px;color:var(--muted)">${esc(r.event)}</td><td><span style="font-family:'DM Mono',monospace;font-size:8px;padding:2px 7px;border-radius:2px;font-weight:700;background:rgba(6,182,212,.1);color:var(--cyan);border:1px solid rgba(6,182,212,.3)">${esc(r.source)}</span></td><td style="font-size:11px;color:var(--white)">${esc(r.title)}</td><td>${r.link?`<a href="${esc(r.link)}" target="_blank" rel="noopener" style="color:var(--green);font-size:11px">↗</a>`:''}</td></tr>`;
  }).join('');
  document.getElementById('news-rcount').textContent=NEWS_filtered.length+' items';
  notifyHeight();
}

// ===== WEEKLY REPORTS =====
function loadReports(){
  // Embedded reports data (bypasses server file access issues)
  const reports = [
    {
        "filename": "2026-W16.html",
        "week_num": 16,
        "year": 2026,
        "week_start": "2026-04-11",
        "week_end": "2026-04-17",
        "generated": "2026-04-17T12:03:40.110087Z",
        "total": 12,
        "tier1": 7,
        "outbreaks": 0,
        "top_pathogen": "Listeria monocytogenes",
        "summary": "Week 16: 12 recalls, 7 Tier-1, 0 outbreak(s). Leading pathogen: Listeria monocytogenes."
    },
    {
        "filename": "2026-W15.html",
        "week_num": 15,
        "year": 2026,
        "week_start": "2026-04-04",
        "week_end": "2026-04-10",
        "generated": "2026-04-17T11:49:40.369524Z",
        "total": 67,
        "tier1": 60,
        "outbreaks": 4,
        "top_pathogen": "Listeria monocytogenes",
        "summary": "Week 15: 67 recalls, 60 Tier-1, 4 outbreak(s). Leading pathogen: Listeria monocytogenes."
    }
];
  
  // 1. Defensive filter: never show future-dated weeks even if they snuck into the array
  const todayISO = new Date().toISOString().slice(0,10);
  const published = reports.filter(r => (r.week_end || '') <= todayISO);
  document.getElementById('rep-count').textContent = published.length ? '· '+published.length : '';
  if(!published.length){
    document.getElementById('reports-list').innerHTML='<p class="loading">No reports yet. First report ships Friday.</p>';
    return;
  }

  // 2. Split: most recent 6 as rich cards, remainder as compact archive entries.
  //    When weeks pile up (52/yr), this keeps the page scannable without hiding content.
  const RICH_LIMIT = 6;
  const richCards  = published.slice(0, RICH_LIMIT);
  const archive    = published.slice(RICH_LIMIT);

  const renderRich = r => {
    const wstart = new Date(r.week_start).toLocaleDateString('en-GB',{day:'numeric',month:'short'});
    const wend   = new Date(r.week_end  ).toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric'});
    return `<div class="report-card" onclick="window.open('${esc(r.filename)}','_blank')">
      <div class="report-week">Week ${r.week_num} · ${wstart} – ${wend}</div>
      <div class="report-title">${r.total} pathogen recalls · ${r.top_pathogen||'—'}</div>
      <div class="report-stats">
        <span><strong>${r.total}</strong> total</span>
        <span class="stat-tier1"><strong style="color:var(--red)">${r.tier1}</strong> Tier-1</span>
        <span class="stat-outbreak"><strong style="color:var(--orange)">${r.outbreaks}</strong> outbreaks</span>
      </div>
      <div class="report-actions">
        <a class="report-btn primary" href="${esc(r.filename)}" target="_blank" onclick="event.stopPropagation()">→ View Report</a>
        <a class="report-btn secondary" href="#" onclick="event.preventDefault();event.stopPropagation();downloadReportPDF('${esc(r.filename)}')">↓ Download PDF</a>
      </div>
    </div>`;
  };

  // Archive: one-line rows grouped by year, collapsible.
  // Only rendered when there's content beyond the rich cards.
  const renderArchive = rows => {
    if(!rows.length) return '';
    // Group by year
    const byYear = {};
    rows.forEach(r => { (byYear[r.year] = byYear[r.year] || []).push(r); });
    const years = Object.keys(byYear).sort((a,b)=>b-a);
    const currentYear = new Date().getFullYear();
    const yearBlocks = years.map(y => {
      const entries = byYear[y].map(r => {
        const wstart = new Date(r.week_start).toLocaleDateString('en-GB',{day:'numeric',month:'short'});
        const wend   = new Date(r.week_end  ).toLocaleDateString('en-GB',{day:'numeric',month:'short'});
        return `<a class="arch-row" href="${esc(r.filename)}" target="_blank">
          <span class="arch-wk">W${String(r.week_num).padStart(2,'0')}</span>
          <span class="arch-dates">${wstart} – ${wend}</span>
          <span class="arch-total">${r.total}</span>
          <span class="arch-tier1">${r.tier1} T1</span>
          <span class="arch-top">${esc(r.top_pathogen||'—')}</span>
          <span class="arch-go">→</span>
        </a>`;
      }).join('');
      const openAttr = (String(y) === String(currentYear)) ? ' open' : '';
      return `<details class="arch-year"${openAttr}>
        <summary class="arch-year-head">${y} · ${byYear[y].length} report${byYear[y].length===1?'':'s'}</summary>
        <div class="arch-list">${entries}</div>
      </details>`;
    }).join('');
    return `<div class="arch-wrap">
      <div class="arch-title">Archive · older briefings</div>
      ${yearBlocks}
    </div>`;
  };

  document.getElementById('reports-list').innerHTML =
    richCards.map(renderRich).join('') + renderArchive(archive);
  notifyHeight();
}

// ===== MONTHLY REPORTS =====
// Fetches data/monthly-index.json on first tab click. The JSON is committed
// by the Python monthly builder workflow (build_monthly_report_afts.py)
// alongside the MM-NN.html report on the 1st of each month, so no manual
// edits to this file are needed once a new report ships.
//
// Expected shape:
//   [
//     { filename, year, month_num, month_name, month_start, month_end,
//       total, tier1, outbreaks, top_pathogen, summary }
//   ]
//   Sorted newest-first by month_end (same order weekly uses).
//
// Fallback: if data/monthly-index.json is missing or unreachable (e.g. on
// first deploy, or Wix cached an old version), the embedded FALLBACK_REPORTS
// below ensure the tab still renders rather than showing an error. Keep the
// embedded array tiny — just enough to avoid a blank state. Authoritative
// data always comes from the JSON.
async function loadMonthlyReports(){
  const FALLBACK_REPORTS = [
    {
      filename: "2026-M03.html",
      year: 2026, month_num: 3, month_name: "March 2026",
      month_start: "2026-03-01", month_end: "2026-03-31",
      total: 66, tier1: 35, outbreaks: 3,
      top_pathogen: "Salmonella spp.",
      summary: "March 2026: 66 recalls, 35 Tier-1, 3 outbreak(s). Leading pathogen: Salmonella."
    }
  ];

  let reports = FALLBACK_REPORTS;
  try {
    // Cache-bust so the published index refresh doesn't get trapped behind
    // an old Service Worker or CDN cache.
    const r = await fetch('data/monthly-index.json?_=' + Date.now(), {cache: 'no-store'});
    if (r.ok) {
      const j = await r.json();
      // Accept either a bare array or a wrapped {reports:[...]} object
      const arr = Array.isArray(j) ? j : (Array.isArray(j.reports) ? j.reports : null);
      if (arr && arr.length) {
        // Sort newest-first by month_end so the rich cards always show the
        // freshest month even if the JSON isn't pre-sorted.
        reports = arr.slice().sort((a, b) => String(b.month_end || '').localeCompare(String(a.month_end || '')));
      }
    }
  } catch (e) {
    // Silent fallback — the UI will still render via FALLBACK_REPORTS.
    console.warn('monthly-index.json fetch failed, using embedded fallback:', e);
  }

  // Never surface a month whose end date is still in the future.
  const todayISO = new Date().toISOString().slice(0, 10);
  const published = reports.filter(r => (r.month_end || '') <= todayISO);
  document.getElementById('mon-count').textContent = published.length ? '· ' + published.length : '';
  if (!published.length) {
    document.getElementById('monthly-list').innerHTML =
      '<p class="loading">No monthly reports yet. First report ships on the 1st of the month.</p>';
    return;
  }

  // Same rich / archive split as weekly: most recent 6 are rich cards, the
  // rest group by year in a collapsible archive.
  const RICH_LIMIT = 6;
  const rich    = published.slice(0, RICH_LIMIT);
  const archive = published.slice(RICH_LIMIT);

  const renderRich = r => {
    const mstart = new Date(r.month_start).toLocaleDateString('en-GB', {day:'numeric', month:'short'});
    const mend   = new Date(r.month_end  ).toLocaleDateString('en-GB', {day:'numeric', month:'short', year:'numeric'});
    return `<div class="report-card" onclick="window.open('${esc(r.filename)}','_blank')">
      <div class="report-week">${esc(r.month_name)} · ${mstart} – ${mend}</div>
      <div class="report-title">${r.total} pathogen recalls · ${esc(r.top_pathogen||'—')}</div>
      <div class="report-stats">
        <span><strong>${r.total}</strong> total</span>
        <span class="stat-tier1"><strong style="color:var(--red)">${r.tier1}</strong> Tier-1</span>
        <span class="stat-outbreak"><strong style="color:var(--orange)">${r.outbreaks}</strong> outbreaks</span>
      </div>
      <div class="report-actions">
        <a class="report-btn primary" href="${esc(r.filename)}" target="_blank" onclick="event.stopPropagation()">→ View Report</a>
        <a class="report-btn secondary" href="#" onclick="event.preventDefault();event.stopPropagation();downloadReportPDF('${esc(r.filename)}')">↓ Download PDF</a>
      </div>
    </div>`;
  };

  const renderArchive = rows => {
    if (!rows.length) return '';
    const byYear = {};
    rows.forEach(r => { (byYear[r.year] = byYear[r.year] || []).push(r); });
    const years = Object.keys(byYear).sort((a, b) => b - a);
    const currentYear = new Date().getFullYear();
    const yearBlocks = years.map(y => {
      const entries = byYear[y].map(r => {
        const mstart = new Date(r.month_start).toLocaleDateString('en-GB', {day:'numeric', month:'short'});
        const mend   = new Date(r.month_end  ).toLocaleDateString('en-GB', {day:'numeric', month:'short'});
        return `<a class="arch-row" href="${esc(r.filename)}" target="_blank">
          <span class="arch-wk">M${String(r.month_num).padStart(2,'0')}</span>
          <span class="arch-dates">${mstart} – ${mend}</span>
          <span class="arch-total">${r.total}</span>
          <span class="arch-tier1">${r.tier1} T1</span>
          <span class="arch-top">${esc(r.top_pathogen||'—')}</span>
          <span class="arch-go">→</span>
        </a>`;
      }).join('');
      const openAttr = (String(y) === String(currentYear)) ? ' open' : '';
      return `<details class="arch-year"${openAttr}>
        <summary class="arch-year-head">${y} · ${byYear[y].length} report${byYear[y].length===1?'':'s'}</summary>
        <div class="arch-list">${entries}</div>
      </details>`;
    }).join('');
    return `<div class="arch-wrap">
      <div class="arch-title">Archive · older monthly briefings</div>
      ${yearBlocks}
    </div>`;
  };

  document.getElementById('monthly-list').innerHTML =
    rich.map(renderRich).join('') + renderArchive(archive);
  notifyHeight();
}

// Download PDF: open the actual published report (single source of truth) and
// trigger the browser's print dialog. No hardcoded data, no styling drift --
// whatever is in the Python-generated report is what gets printed to PDF.
function downloadReportPDF(filename){
  if(!filename){console.error('downloadReportPDF: filename required');return;}
  const w = window.open(filename, '_blank');
  if(!w){alert('Please allow pop-ups to download the PDF report.');return;}
  // Wait for the report to load, then print. onload fires once the HTML is ready.
  w.addEventListener('load', () => {
    // Small delay so Google Fonts (Syne / DM Sans / DM Mono) finish rendering before print
    setTimeout(() => { try{ w.focus(); w.print(); } catch(e){ console.error(e); } }, 800);
  });
  // Fallback for browsers where onload doesn't fire for already-cached pages
  setTimeout(() => {
    try{ if(w.document && w.document.readyState === 'complete'){ w.focus(); w.print(); } }catch(e){}
  }, 2000);
}
</script></body></html>