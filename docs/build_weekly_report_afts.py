"""
AFTS Food Safety Intelligence System - Weekly Report Generator
Template: 2026-W16.html (gold standard)
Output: docs/YYYY-WW.html + data/weekly-summary-latest.json
Reads: docs/data/recalls.xlsx Recalls sheet ONLY.
Schedule: Friday 10:00 Athens time (dual-DST cron + TZ guard).
"""

import json, logging, os, re, requests, sys, argparse, html as html_mod
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from collections import Counter, OrderedDict
from typing import List, Dict, Any, Tuple
from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

SEVERITY = OrderedDict([
    ("clostridium botulinum",1),("botulinum",1),
    ("listeria monocytogenes",2),("listeria",2),
    ("stec",3),("e. coli o157",3),("e. coli",3),
    ("salmonella",4),("cereulide",5),("bacillus cereus",5),
    ("norovirus",6),("hepatitis",7),
])

def _dot_color(pathogen):
    p = (pathogen or "").lower()
    return "#9333ea" if ("botulinum" in p or "clostridium" in p) else "#dc2626"

def _severity_score(pathogen):
    p = (pathogen or "").lower()
    for k, v in SEVERITY.items():
        if k in p: return v
    return 99

COUNTRY_DISPLAY = {"USA":"United States","UK":"United Kingdom"}
AUTHORITY_DISPLAY = {
    "FDA":"FDA","USDA FSIS":"USDA FSIS","CFIA":"CFIA","MAPAQ QC":"MAPAQ QC",
    "RappelConso (FR)":"RappelConso (FR)","BVL (DE)":"BVL (DE)",
    "FSA (UK)":"FSA (UK)","MPI NZ":"MPI NZ","MPI (NZ)":"MPI NZ",
    "Min. Salute (IT)":"Min. Salute (IT)","EFET (GR)":"EFET (GR)",
    "AESAN (ES)":"AESAN (ES)","RASFF (EU)":"RASFF (EU)",
    "FSANZ (AU)":"FSANZ (AU)","BLV (CH)":"BLV (CH)",
    "AGES (AT)":"AGES (AT)","SZPI (CZ)":"SZPI (CZ)",
    "ANVISA (BR)":"ANVISA (BR)","ANMAT (AR)":"ANMAT (AR)",
    "COFEPRIS (MX)":"COFEPRIS (MX)","NCC (ZA)":"NCC (ZA)",
    "SFA (SG)":"SFA (SG)","CFS (HK)":"CFS (HK)","MFDS (KR)":"MFDS (KR)",
}
GEO_AUTHORITY = {
    "France":"RappelConso / DGCCRF","Germany":"BVL","Canada":"CFIA",
    "United States":"FDA / USDA FSIS","USA":"FDA / USDA FSIS",
    "New Zealand":"MPI / FSANZ","Italy":"Ministero della Salute",
    "United Kingdom":"FSA","UK":"FSA","Spain":"AESAN","Greece":"EFET",
    "Australia":"FSANZ","Switzerland":"BLV","Austria":"AGES",
    "Brazil":"ANVISA","South Africa":"NCC / NRCS","Hong Kong":"CFS",
    "Singapore":"SFA","Japan":"MHLW","South Korea":"MFDS",
    "Taiwan":"MOHW / TFDA","Mexico":"COFEPRIS","Argentina":"ANMAT",
    "Czech Republic":"SZPI","Slovakia":"\u0160VPS","Belgium":"AFSCA",
}

def load_recalls(xlsx_path):
    wb = load_workbook(xlsx_path, data_only=True)
    if "Recalls" not in wb.sheetnames:
        log.error("No Recalls sheet"); return []
    ws = wb["Recalls"]; hdr = [c.value for c in ws[1]]
    return [{h:(v if v is not None else "") for h,v in zip(hdr, row)}
            for row in ws.iter_rows(min_row=2, values_only=True)]

def filter_week(recalls, week_end):
    ws = week_end - timedelta(days=6); out = []
    for r in recalls:
        d = r.get("Date","")
        if not d: continue
        try: rd = datetime.strptime(str(d)[:10],"%Y-%m-%d").date()
        except ValueError: continue
        if ws <= rd <= week_end: out.append(r)
    return out

def _safe_int(v, default=0):
    try: return int(v)
    except (ValueError, TypeError): return default

def _country_display(c): return COUNTRY_DISPLAY.get(c, c)

def compute_stats(wr, pr):
    total = len(wr)
    tier1 = sum(1 for r in wr if _safe_int(r.get("Tier")) == 1)
    outbreaks = sum(1 for r in wr if _safe_int(r.get("Outbreak")) == 1)
    pc = Counter(); cc = Counter()
    for r in wr:
        p = (r.get("Pathogen") or "").strip()
        if p: pc[p.split("(")[0].strip()] += 1
        cc[_country_display(r.get("Country","") or "Unknown")] += 1
    pt = len(pr); delta = total - pt
    return {"total":total,"tier1":tier1,"outbreaks":outbreaks,
            "top_pathogen":pc.most_common(1)[0] if pc else ("\u2014",0),
            "pathogen_counts":pc.most_common(20),"country_counts":cc.most_common(20),
            "prev_total":pt,"delta":delta,
            "delta_pct":round(delta/max(pt,1)*100) if pt else 0}

def sort_by_severity(recalls):
    def key(r):
        return (_severity_score(r.get("Pathogen","")),
                -_safe_int(r.get("Outbreak",0)), _safe_int(r.get("Tier",2)),
                -(datetime.strptime(str(r.get("Date","2000-01-01"))[:10],"%Y-%m-%d").toordinal()
                  if str(r.get("Date",""))[:10] else 0))
    return sorted(recalls, key=key)

def esc(s):
    if s is None: return ""
    return html_mod.escape(str(s))

def _fmt_date(d):
    try: return datetime.strptime(str(d)[:10],"%Y-%m-%d").strftime("%-d %b %Y")
    except Exception: return str(d)[:10]

def _fmt_date_short(d):
    try: return datetime.strptime(str(d)[:10],"%Y-%m-%d").strftime("%-d %b")
    except Exception: return str(d)[:10]


def generate_analysis_claude(stats, recalls):
    tp, tc = stats["top_pathogen"]; t = stats["total"]
    pct = round(tc/max(t,1)*100)
    bot = [r for r in recalls if "botulinum" in (r.get("Pathogen") or "").lower()
           or "clostridium" in (r.get("Pathogen") or "").lower()]
    pa = ""
    if bot:
        co = ", ".join(
            "{} ({})".format(r.get("Company",""), _country_display(r.get("Country","")))
            for r in bot[:3])
        pa = "\n4. PROCESS AUTHORITY NOTE paragraph (start with \'This window contains {} incident(s) implicating Clostridium or botulinum toxin, with {} cited.\' Then explain shelf-stable/low-acid/acidified/aseptic/UHT/hot-filled/reduced-O2 products must be reviewed under qualified process authority per FDA 21 CFR 113/114, EU 852/2004, CFIA SFCR, FSANZ Ch.3, Japan FSA. Typical gaps: unfiled scheduled process, deviation without qualified review, seal-integrity lapse, formulation change not re-evaluated. Tier-1/Class-I triggers process-filing audit.)".format(len(bot), co)
    prompt = """You are a food safety intelligence analyst for AFTS. Generate the Intelligence Analysis section.

DATA: Total={}, Tier-1={}, Outbreaks={}, Leading={} ({}, {}%)
Pathogens: {}
Countries: {}

Generate EXACTLY these paragraphs (plain text, no HTML, no headers):
1. Executive overview (total, tier-1, outbreaks, leading pathogen %, what it signals)
2. Pathogen risk (failure modes for {}, relevant regs)
3. Geographic/regulatory assessment (jurisdictions, recommendation)
{}
Professional, concise, no emojis, no bullets. 3-5 sentences each.""".format(
        t, stats["tier1"], stats["outbreaks"], tp, tc, pct,
        dict(stats["pathogen_counts"]), dict(stats["country_counts"]), tp, pa)
    try:
        resp = requests.post("https://api.anthropic.com/v1/messages",
            headers={"Content-Type":"application/json","x-api-key":CLAUDE_API_KEY},
            json={"model":"claude-sonnet-4-20250514","max_tokens":1200,
                  "messages":[{"role":"user","content":prompt}]}, timeout=60)
        if resp.status_code == 200:
            return resp.json()["content"][0]["text"]
        log.error("Claude %d", resp.status_code)
    except Exception as e:
        log.error("Claude error: %s", e)
    return _fallback(stats, bot)

def _fallback(stats, bot):
    tp, tc = stats["top_pathogen"]; t = stats["total"]
    pct = round(tc/max(t,1)*100)
    paras = [
        "This week produced {} pathogen-related recall incidents across the AFTS monitoring network, with {} classified as Tier-1 and {} confirmed outbreak event(s). {} dominated the surveillance window, accounting for {} of {} incidents ({}%). The elevated Tier-1 ratio indicates sustained regulatory pressure and should be read by food manufacturers as a signal of tightening enforcement.".format(t, stats["tier1"], stats["outbreaks"], tp, tc, t, pct),
        "{} at this prevalence points to post-process recontamination in ready-to-eat lines rather than thermal underprocess. The likely failure modes are environmental harbourage, sanitation SOP drift, and post-lethality recontamination.".format(tp),
        "Regulatory activity this week spanned multiple jurisdictions, signalling continued inspection intensity. AFTS recommends that food manufacturers use this briefing as a prompt to re-verify the single highest-leverage control for their commodity this week.",
    ]
    if bot:
        co = ", ".join("{} ({})".format(r.get("Company",""), _country_display(r.get("Country",""))) for r in bot[:3])
        paras.append("This window contains {} incident(s) implicating Clostridium or botulinum toxin, with {} cited for Clostridium botulinum. Any shelf-stable low-acid, acidified, aseptic/UHT, hot-filled, or reduced-oxygen-packaged product in the affected category must be reviewed to confirm the scheduled thermal process under a qualified process authority \u2014 required under FDA 21 CFR 113/114, EU Reg. 852/2004, CFIA SFCR, FSANZ Food Standards Code Ch. 3, and Japan\u2019s Food Sanitation Act. Typical compliance gaps: unfiled or outdated scheduled process, deviation resolved without qualified review, container or seal-integrity lapse, or a formulation change (pH, a_w, salt, preservative) not re-evaluated. A Tier-1 / Class-I classification on a product of this class reliably triggers a process-filing audit and regulatory citations in every major jurisdiction.".format(len(bot), co))
    return "\n\n".join(paras)

def review_with_openai(text):
    if not OPENAI_API_KEY: return text
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization":"Bearer "+OPENAI_API_KEY,"Content-Type":"application/json"},
            json={"model":"gpt-4o-mini","messages":[{"role":"user","content":"Review this food safety analysis. Fix grammar. Keep structure/facts. Return polished version only.\n\n"+text}],"max_tokens":1200},timeout=60)
        if r.status_code==200: return r.json()["choices"][0]["message"]["content"]
    except Exception as e: log.warning("OpenAI: %s",e)
    return text


def _recall_row(rank, r, top_n=5):
    pathogen = r.get("Pathogen","") or "Unknown"
    tier = _safe_int(r.get("Tier",2)); ob = _safe_int(r.get("Outbreak",0))
    company = r.get("Company","") or "\u2014"
    brand = r.get("Brand","") or "\u2014"
    product = r.get("Product","") or "\u2014"
    country = _country_display(r.get("Country","") or "Unknown")
    source = r.get("Source","") or ""; url = r.get("URL","") or ""
    dt = _fmt_date(r.get("Date",""))
    dot = _dot_color(pathogen); ps = pathogen.split("(")[0].strip()
    chip = '<span class="chip-tier1">T1</span>' if tier==1 else '<span class="chip-tier2">T2</span>'
    obc = ' <span class="chip-outbreak">OUTBREAK</span>' if ob else ""
    rc = "rank-num" if rank<=top_n else "rank-num rank-num--multi"
    sd = AUTHORITY_DISPLAY.get(source, source)
    if url and url.strip():
        lk = '<div class="juris-link"><a class="src-link" href="{}" target="_blank" rel="noopener">View source &rarr;</a></div>'.format(esc(url))
    else:
        lk = '<div class="juris-link"><span class="src-na" title="No verified specific-recall URL available">unverified</span></div>'
    bs = esc(brand) if brand and brand!="\u2014" else "\u2014"
    return """    <tr>
      <td class="{rc}" data-label="#">{rank}</td>
      <td class="date-cell" data-label="Date">{dt}</td>
      <td data-label="Pathogen">
        <span class="path-dot" style="background:{dot}"></span>
        <span class="path-name">{ps}</span>
        {chip}{obc}
      </td>
      <td class="co-cell" data-label="Company"><strong>{co}</strong><div class="brand-sub">{bs}</div></td>
      <td class="prod-cell" data-label="Product">{prod}</td>
      <td class="juris-cell" data-label="Jurisdiction">
        <div class="juris-country">{ctry}</div>
        <div class="src-sub">{src}</div>
        {lk}
      </td>
    </tr>""".format(rc=rc, rank=rank, dt=esc(dt), dot=dot, ps=esc(ps),
                    chip=chip, obc=obc, co=esc(company), bs=bs,
                    prod=esc(product), ctry=esc(country), src=esc(sd), lk=lk)


W16_CSS = """
:root {
  --black:#0a0e1a; --orange:#E8601A;
  --ink:#111827; --body:#1f2937; --muted:#6b7280; --dim:#9ca3af;
  --bg:#ffffff; --s1:#f9fafb; --s2:#f3f4f6; --brd:#e5e7eb;
  --red:#dc2626; --amber:#f59e0b; --violet:#9333ea; --green:#059669;
}
* { box-sizing:border-box; }
html, body { margin:0; padding:0; background:var(--bg); }
body {
  font-family:'DM Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  color:var(--body); font-size:14px; line-height:1.65;
  max-width:1180px; margin:0 auto; padding:0 40px 60px;
}
a { color:var(--orange); text-decoration:none; }
a:hover { text-decoration:underline; }

.masthead {
  border-top:6px solid var(--black);
  padding:28px 0 22px;
  display:flex; justify-content:space-between; align-items:flex-start;
  border-bottom:1px solid var(--brd);
  margin-bottom:32px;
}
.brand-block .brand {
  font-family:'Syne', sans-serif; font-weight:800; font-size:24px;
  color:var(--black); letter-spacing:-0.01em; text-transform:uppercase;
  line-height:1.1;
}
.brand-block .brand em { color:var(--orange); font-style:normal; font-weight:800; }
.brand-block .tagline {
  font-family:'DM Mono', monospace; font-size:10px; font-weight:600;
  color:var(--muted); text-transform:uppercase; letter-spacing:0.14em;
  margin-top:8px;
}
.mast-right { text-align:right; }
.report-label {
  display:inline-block; background:var(--black); color:#fff;
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  padding:5px 11px; letter-spacing:0.12em; text-transform:uppercase;
  margin-bottom:10px;
}
.report-meta {
  font-family:'DM Mono', monospace; font-size:11px;
  color:var(--muted); line-height:1.8;
}
.report-meta strong { color:var(--ink); font-weight:700; }

.r-title {
  font-family:'Syne', sans-serif; font-weight:800; font-size:38px;
  color:var(--black); letter-spacing:-0.02em; line-height:1.15;
  margin:2px 0 10px;
}
.r-title .accent { color:var(--orange); }
.r-kicker {
  font-family:'Syne', sans-serif; font-weight:800; font-size:13px;
  color:var(--black); letter-spacing:0.08em; text-transform:uppercase;
  margin:8px 0 6px;
}
.r-kicker-dot { color:var(--orange); font-style:normal; margin:0 2px; }
.r-sub {
  color:var(--muted); font-size:14px; margin-bottom:16px;
}
.r-sub strong { color:var(--ink); font-weight:600; }
.r-authority {
  display:flex; align-items:center; gap:10px; flex-wrap:wrap;
  padding:10px 14px; background:var(--s1); border-left:3px solid var(--orange);
  font-family:'DM Mono', monospace; font-size:11px; color:var(--ink);
  margin-bottom:30px;
}
.auth-label {
  font-size:9px; font-weight:700; color:var(--orange);
  text-transform:uppercase; letter-spacing:0.14em;
  border-right:1px solid var(--brd); padding-right:10px;
}

.kpi-strip {
  display:grid; grid-template-columns:repeat(4, 1fr);
  gap:1px; background:var(--brd); border:1px solid var(--brd);
  margin-bottom:32px;
}
.kpi { background:#fff; padding:22px 20px; }
.kpi-label {
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  color:var(--muted); text-transform:uppercase; letter-spacing:0.1em;
  margin-bottom:8px;
}
.kpi-value {
  font-family:'Syne', sans-serif; font-weight:800; font-size:42px;
  color:var(--black); line-height:1; letter-spacing:-0.02em;
}
.kpi-value.red { color:var(--red); }
.kpi-value.violet { color:var(--violet); }
.kpi-value.orange { color:var(--orange); font-size:20px; line-height:1.2; font-style:italic; }
.kpi-value a { color:inherit; text-decoration:none; border-bottom:2px solid var(--orange); padding-bottom:1px; }
.kpi-value a:hover { opacity:0.8; text-decoration:none; }
.kpi-delta {
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  margin-top:10px; letter-spacing:0.04em;
}
.kpi-top { font-size:11px; color:var(--muted); margin-top:10px; font-style:italic; }

.sec-head {
  display:flex; align-items:baseline; gap:14px;
  margin:40px 0 16px;
}
.sec-num {
  font-family:'DM Mono', monospace; font-size:11px; font-weight:700;
  color:var(--orange); letter-spacing:0.12em;
}
.sec-title {
  font-family:'Syne', sans-serif; font-weight:800; font-size:22px;
  color:var(--black); letter-spacing:-0.01em;
}
.sec-rule { flex:1; height:1px; background:var(--brd); }
.sec-caption { color:var(--muted); font-size:13px; margin:-4px 0 14px; }
.sec-caption em { color:var(--ink); font-style:italic; }

.analysis {
  background:var(--s1); border-left:4px solid var(--orange);
  padding:26px 30px; margin-bottom:10px;
}
.analysis p { margin:0 0 14px; font-size:14.5px; line-height:1.75; }
.analysis p:last-child { margin-bottom:0; }
.analysis p.pa-note {
  margin:18px -30px 0 -30px; padding:18px 30px 2px 30px;
  background:#fff; border-top:1px solid var(--brd);
  font-size:13.5px; line-height:1.7;
}
.analysis p.pa-note .pa-label {
  display:inline; font-family:'DM Mono', monospace; font-weight:700;
  letter-spacing:0.08em; text-transform:uppercase;
  color:var(--red); font-size:10px; margin-right:8px;
}

table.data {
  width:100%; border-collapse:collapse; margin:0 0 10px;
  background:#fff; border:1px solid var(--brd);
  font-size:13px;
}
table.data th {
  background:var(--black); color:#fff;
  font-family:'DM Mono', monospace; font-size:10px; font-weight:700;
  text-transform:uppercase; letter-spacing:0.1em;
  padding:12px 12px; text-align:left; border-bottom:2px solid var(--orange);
}
table.data td {
  padding:14px 12px; border-bottom:1px solid var(--brd);
  vertical-align:top;
}
table.data tr:last-child td { border-bottom:none; }
table.data tr:nth-child(even) td { background:#fafbfc; }
table.data td.num {
  font-family:'DM Mono', monospace; font-weight:600; text-align:right;
  white-space:nowrap;
}
table.data td.empty {
  text-align:center; color:var(--muted); padding:28px; font-style:italic;
}

/* Top 5 column sizing - keeps table within A4 and desktop viewport */
table.top5 { table-layout:fixed; width:100%; }
table.top5 th:nth-child(1), table.top5 td:nth-child(1) { width:5%;  }  /* # */
table.top5 th:nth-child(2), table.top5 td:nth-child(2) { width:9%;  }  /* Date */
table.top5 th:nth-child(3), table.top5 td:nth-child(3) { width:19%; }  /* Pathogen */
table.top5 th:nth-child(4), table.top5 td:nth-child(4) { width:18%; }  /* Company */
table.top5 th:nth-child(5), table.top5 td:nth-child(5) { width:30%; }  /* Product */
table.top5 th:nth-child(6), table.top5 td:nth-child(6) { width:19%; }  /* Jurisdiction+Source */
table.top5 td { word-wrap:break-word; overflow-wrap:break-word; }

.rank-num {
  font-family:'Syne', sans-serif; font-weight:800; font-size:22px;
  color:var(--orange); text-align:center;
  white-space:nowrap; font-variant-numeric:tabular-nums;
  letter-spacing:-0.02em;
}
.rank-num.rank-num--multi { font-size:18px; }
.date-cell {
  font-family:'DM Mono', monospace; font-size:11px; color:var(--muted);
  white-space:nowrap;
}
.path-dot {
  display:inline-block; width:9px; height:9px; border-radius:50%;
  margin-right:7px; vertical-align:middle;
}
.path-name { font-weight:600; color:var(--ink); font-style:italic; }
.co-cell strong { color:var(--black); font-weight:700; display:block; }
.brand-sub { font-size:11px; color:var(--muted); margin-top:2px; font-style:italic; }
.prod-cell { color:var(--body); }
.juris-country { font-weight:600; color:var(--ink); }
.src-sub {
  font-family:'DM Mono', monospace; font-size:10px;
  color:var(--muted); margin-top:3px;
}
.juris-link { margin-top:6px; }
.chip-tier1 {
  display:inline-block; background:var(--red); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:6px; letter-spacing:0.06em;
}
.chip-tier2 {
  display:inline-block; background:var(--amber); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:6px; letter-spacing:0.06em;
}
.chip-outbreak {
  display:inline-block; background:var(--violet); color:#fff;
  font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
  padding:2px 6px; border-radius:2px; margin-left:4px; letter-spacing:0.06em;
}
.src-link {
  font-family:'DM Mono', monospace; font-size:11px; font-weight:700;
  color:var(--orange); letter-spacing:0.02em;
}
.src-na { color:var(--dim); font-family:'DM Mono', monospace; font-size:10px; font-style:italic; }

.dist-grid {
  display:grid; grid-template-columns:1fr 1fr; gap:24px; margin-bottom:10px;
}
.dist-grid h3 {
  font-family:'DM Mono', monospace; font-size:11px; color:var(--muted);
  text-transform:uppercase; letter-spacing:0.1em; margin:0 0 10px;
}
.bar-track {
  width:100%; height:8px; background:var(--s2);
  border-radius:1px; overflow:hidden;
}
.bar-fill { height:100%; }

.cta-box {
  margin:40px 0 30px;
  padding:26px 30px;
  background:var(--black); color:#fff;
  display:flex; justify-content:space-between; align-items:center;
  flex-wrap:wrap; gap:18px;
}
.cta-text { flex:1; min-width:280px; }
.cta-text h3 {
  font-family:'Syne', sans-serif; font-weight:800; font-size:20px;
  margin:0 0 6px; color:#fff; letter-spacing:-0.01em;
}
.cta-text p { margin:0; color:#d1d5db; font-size:13px; }
.cta-btn {
  background:var(--orange); color:#fff; font-family:'DM Mono', monospace;
  font-size:11px; font-weight:700; padding:14px 22px;
  text-transform:uppercase; letter-spacing:0.1em;
  border:none; cursor:pointer; white-space:nowrap;
}
.cta-btn:hover { background:#d35416; text-decoration:none; color:#fff; }

.meth {
  background:var(--s1); border:1px solid var(--brd);
  padding:22px 26px; margin-bottom:24px; font-size:13px;
  color:var(--body);
}
.meth strong { color:var(--black); }
.meth p { margin:0 0 10px; }
.meth p:last-child { margin-bottom:0; }

.footer {
  margin-top:50px; padding-top:26px; border-top:2px solid var(--black);
  display:flex; justify-content:space-between; align-items:flex-start;
  flex-wrap:wrap; gap:20px; font-size:12px;
}
.foot-brand {
  font-family:'Syne', sans-serif; font-weight:800; font-size:15px;
  color:var(--black); text-transform:uppercase; letter-spacing:0.02em;
}
.foot-brand em { color:var(--orange); font-style:normal; }
.foot-meta {
  font-family:'DM Mono', monospace; font-size:10px;
  color:var(--muted); line-height:1.8; margin-top:6px;
}
.foot-legal {
  font-size:11px; color:var(--muted); max-width:440px;
  text-align:right; line-height:1.6;
}

@media print {
  /* Running footer on every printed page: process-authority attribution
     anchors the AFTS differentiator visually throughout the document. */
  @page {
    size: A4;
    margin: 14mm 14mm 18mm 14mm;
    @bottom-left {
      content: "AFTS · Food Safety Validation Intelligence";
      font-family: 'DM Mono', monospace; font-size: 8pt; color: #6b7280;
      letter-spacing: 0.04em;
    }
    @bottom-right {
      content: "Page " counter(page) " / " counter(pages);
      font-family: 'DM Mono', monospace; font-size: 8pt; color: #6b7280;
      letter-spacing: 0.04em;
    }
  }
  body { max-width:none; padding:0; margin:0; font-family:'Times New Roman', Times, serif; font-size:12pt; }
  .cta-box { display:none; }

  /* Lock print-mode layout: even if the browser's print page is narrow,
     these must not collapse into mobile responsive layouts. */
  .masthead { flex-direction:row !important; }
  .mast-right { text-align:right !important; }
  .kpi-strip { grid-template-columns:repeat(4, 1fr) !important; }
  .dist-grid { display:block !important; grid-template-columns:1fr !important; gap:0 !important; }
  .dist-grid > div { width:100% !important; display:block !important; }
  .dist-grid > div:nth-child(2) { margin-top:18px !important; }
  .dist-grid > div { page-break-inside:avoid; break-inside:avoid; }

  /* Page 1 compression: tighten the above-the-fold so the first Intelligence
     Analysis paragraph opens on page 1 rather than orphaning the heading. */
  .masthead { border-top-width:4px; padding:18px 0 12px; margin-bottom:22px; }
  .brand-block .brand { font-size:18px; }
  .brand-block .tagline { font-size:10px; margin-top:5px; letter-spacing:0.12em; }
  .report-label { font-size:9px; padding:4px 10px; margin-bottom:8px; }
  .report-meta { font-size:10px; line-height:1.7; }
  .r-kicker { font-size:12px; margin:6px 0 5px; letter-spacing:0.07em; }
  .r-title { font-size:26px; margin:2px 0 8px; }
  .r-sub { font-size:13px; margin-bottom:12px; line-height:1.55; }
  .r-authority { padding:9px 12px; font-size:11px; margin-bottom:22px; }
  .auth-label { font-size:8px; padding-right:9px; }
  .kpi-strip { margin-bottom:24px; }
  .kpi { padding:16px 14px; }
  .kpi-label { font-size:9px; margin-bottom:6px; }
  .kpi-value { font-size:28px; }
  .kpi-value.orange { font-size:18px; }
  .kpi-delta { font-size:9px; margin-top:7px; }
  .kpi-top { font-size:10px; margin-top:7px; }
  .sec-head { margin:28px 0 12px; page-break-after:avoid; break-after:avoid; }
  .sec-num { font-size:10px; }
  .sec-title { font-size:20px; white-space:nowrap; }
  .analysis { padding:22px 26px; }
  .analysis p { font-size:13px; margin:0 0 12px; line-height:1.7; }
  .analysis p.pa-note { margin:14px -26px 0 -26px; padding:14px 26px 2px 26px; font-size:12px; line-height:1.65; }
  .analysis p.pa-note .pa-label { font-size:9px; }

  table.data th { background:var(--black) !important; color:#fff !important; -webkit-print-color-adjust:exact; print-color-adjust:exact; }
  /* Prevent any table row from splitting across a page break */
  table.data tr { page-break-inside:avoid; break-inside:avoid; }
  .analysis { border-left-width:3px; }
  /* Top-5 print tightening - fit all 6 columns on A4 */
  table.top5 { font-size:9px; page-break-inside:avoid; }
  table.top5 th { padding:6px 5px; font-size:8px; }
  table.top5 td { padding:6px 5px; line-height:1.35; }
  table.top5 tr { page-break-inside:avoid; }
  table.top5 .rank-num { font-size:14px; }
  table.top5 .path-name { font-size:9px; }
  table.top5 .date-cell { font-size:8px; }
  table.top5 .prod-cell { font-size:9px; line-height:1.35; }
  table.top5 .co-cell strong { font-size:9px; }
  table.top5 .juris-country { font-size:9px; }
  table.top5 .brand-sub, table.top5 .src-sub { font-size:8px; margin-top:1px; }
  table.top5 .chip-tier1, table.top5 .chip-tier2, table.top5 .chip-outbreak { font-size:7px; padding:1px 3px; margin-left:3px; }
  table.top5 .src-link { font-size:8px; }
  table.top5 .juris-link { margin-top:3px; }
  /* Tighten the caption above the Top 5 so more room for rows */
  .sec-caption { font-size:10px; margin:-2px 0 8px; }
  /* Force section boundaries on page breaks for clean 4-page distribution:
     P1 = masthead + KPI + § 01 Analysis
     P2 = § 02 Top 5
     P3 = § 03 Distribution
     P4 = § 04 Methodology + Footer */
  section.page-break, div.page-break { page-break-before:always; }
  .sec-head.break-before { page-break-before:always; break-before:page; }

  /* Footer: switch from flex to a clean vertical stack for print.
     WeasyPrint and some browser print engines overlap the two halves
     when flex wraps at narrow widths - block layout avoids it entirely.
     page-break-inside: avoid keeps brand block + disclaimer together on one page. */
  .footer {
    display:block !important;
    margin-top:26px;
    page-break-inside:avoid;
    break-inside:avoid;
  }
  .footer > div { display:block !important; width:auto !important; }
  .footer > div:first-child { margin-bottom:12px; }
  .foot-legal {
    text-align:left !important;
    max-width:none !important;
    padding-top:10px;
    border-top:1px solid var(--brd);
  }
  /* Keep the methodology section with its adjacent section intact */
  .meth { page-break-inside:avoid; break-inside:avoid; }
}

@media screen and (max-width:900px) {
  body { padding:0 20px 40px; }
  .kpi-strip { grid-template-columns:repeat(2,1fr); }
  .dist-grid { grid-template-columns:1fr; }
  .masthead { flex-direction:column; gap:16px; }
  .mast-right { text-align:left; }
  .r-title { font-size:28px; }
}

/* Mobile Top-5: switch from a 6-column table to stacked cards.
   On phones, a horizontal table would either scroll sideways (bad UX) or
   compress columns into unreadable widths. Instead, each row becomes a
   card with labeled fields - all data visible, no horizontal scroll. */
@media screen and (max-width:700px) {
  table.top5, table.top5 thead, table.top5 tbody, table.top5 tr, table.top5 td {
    display:block; width:auto !important;
  }
  /* Kill all fixed column widths - they would make card-mode cells unreadably narrow */
  table.top5 th:nth-child(1), table.top5 td:nth-child(1),
  table.top5 th:nth-child(2), table.top5 td:nth-child(2),
  table.top5 th:nth-child(3), table.top5 td:nth-child(3),
  table.top5 th:nth-child(4), table.top5 td:nth-child(4),
  table.top5 th:nth-child(5), table.top5 td:nth-child(5),
  table.top5 th:nth-child(6), table.top5 td:nth-child(6) {
    width:auto !important;
  }
  table.top5 { border:none; table-layout:auto !important; }
  table.top5 thead { display:none; }
  table.top5 tr {
    border:1px solid var(--brd); border-left:4px solid var(--orange);
    background:#fff; margin-bottom:12px; padding:8px 4px;
    position:relative;
  }
  table.top5 tr:nth-child(even) td { background:transparent; }
  table.top5 td {
    border:none !important; padding:7px 14px 7px 108px !important;
    position:relative; min-height:28px;
    word-wrap:normal; overflow-wrap:normal;
  }
  table.top5 td::before {
    content:attr(data-label);
    position:absolute; left:14px; top:7px; width:88px;
    font-family:'DM Mono', monospace; font-size:9px; font-weight:700;
    color:var(--muted); text-transform:uppercase; letter-spacing:0.08em;
  }
  /* Rank number sits in top-right corner as an orange badge */
  table.top5 .rank-num {
    position:absolute; top:8px; right:14px; padding:0 !important;
    font-size:28px; min-height:0; text-align:right;
  }
  table.top5 .rank-num::before { display:none; }
  table.top5 .date-cell { font-size:11px; }
  table.top5 .path-name { font-size:13px; }
  table.top5 .co-cell strong { font-size:13px; }
  table.top5 .prod-cell { line-height:1.45; font-size:13px; }
  table.top5 .juris-country { font-size:13px; }
  table.top5 .juris-link { margin-top:6px; }
}

@media screen and (max-width:480px) {
  body { padding:0 14px 30px; }
  .kpi-strip { grid-template-columns:1fr 1fr; }
  .kpi { padding:16px 14px; }
  .kpi-value { font-size:28px; }
  .r-title { font-size:24px; }
  .analysis { padding:18px 20px; }
  .analysis p { font-size:13px; }
  .analysis p.pa-note { margin:14px -20px 0 -20px; padding:14px 20px 2px 20px; }
}
"""


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AFTS Pathogen Intelligence Briefing &middot; Week {wnum}, {year}</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
__CSS_PLACEHOLDER__
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
      <strong>ISSUE</strong> &middot; Week {wnum}, {year}<br>
      <strong>PERIOD</strong> &middot; {period}<br>
      <strong>PUBLISHED</strong> &middot; {published}
    </div>
  </div>
</header>

<div class="r-kicker">AFTS <span class="r-kicker-dot">&middot;</span> Food Safety Validation Intelligence</div>
<h1 class="r-title">Pathogen Surveillance <span class="accent">&middot;</span> Week {wnum}</h1>
<p class="r-sub">
  AI-powered analysis of <strong>{total}</strong> regulatory recall actions across
  <strong>{n_jurisdictions}</strong> jurisdictions, aggregated from 66 primary sources
  monitored continuously by the AFTS intelligence platform.
</p>

<div class="kpi-strip">
  <div class="kpi">
    <div class="kpi-label">Total Recalls</div>
    <div class="kpi-value"><a href="#all-recalls">{total}</a></div>
    {delta_html}
  </div>
  <div class="kpi">
    <div class="kpi-label">Tier-1 Critical</div>
    <div class="kpi-value red">{tier1}</div>
    <div class="kpi-delta" style="color:var(--muted)">Immediate public-health risk</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Active Outbreaks</div>
    <div class="kpi-value violet">{outbreaks}</div>
    <div class="kpi-delta" style="color:var(--muted)">Confirmed cluster events</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Leading Pathogen</div>
    <div class="kpi-value orange">{top_pathogen_name}</div>
    <div class="kpi-top">{top_cnt} cases &middot; {top_pct}% of total</div>
  </div>
</div>

<div class="sec-head">
  <span class="sec-num">&sect; 01</span>
  <h2 class="sec-title">Intelligence Analysis</h2>
  <span class="sec-rule"></span>
</div>
<div class="analysis">
{analysis_html}
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
  <thead><tr><th>#</th><th>Date</th><th>Pathogen</th><th>Company / Brand</th><th>Product</th><th>Jurisdiction &amp; Source</th></tr></thead>
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
      <thead><tr><th>Pathogen</th><th class="num">Cases</th><th class="num">%</th><th>Share</th></tr></thead>
      <tbody>
{pathogen_rows}
      </tbody>
    </table>
  </div>
  <div>
    <h3>Geographic &middot; Regulatory</h3>
    <table class="data">
      <thead><tr><th>Country</th><th>Authority</th><th class="num">Cases</th><th class="num">%</th></tr></thead>
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

<div id="all-recalls" class="sec-head">
  <span class="sec-num">&sect; 04</span>
  <h2 class="sec-title">All {total} Recalls &middot; {period}</h2>
  <span class="sec-rule"></span>
</div>
<p class="sec-caption">
  Complete record for the reporting period. Sorted by pathogen severity, outbreak status, and tier classification.
  Each row links to the originating regulatory notice.
</p>
<table class="data top5">
  <thead><tr><th>#</th><th>Date</th><th>Pathogen</th><th>Company / Brand</th><th>Product</th><th>Jurisdiction &amp; Source</th></tr></thead>
  <tbody>
    {all_rows}
  </tbody>
</table>

<div class="sec-head">
  <span class="sec-num">&sect; 05</span>
  <h2 class="sec-title">Methodology &amp; Sources</h2>
  <span class="sec-rule"></span>
</div>
<div class="meth">
  <p><strong>Process authority.</strong> Analytical frameworks, severity rubrics, pathogen classification, and the engineering interpretation of each recall are developed under the process authority of AFTS, drawing on in-house expertise in food process engineering, thermal processing, and regulatory compliance. Every view is grounded in validated process engineering: thermal processing (21 CFR 113/114), pasteurisation (PMO), aseptic and UHT, hold-tube and F-value lethality, and HACCP. This is what the AFTS platform brings that pure data feeds do not &mdash; data under engineering authority.</p>
  <p><strong>Data &amp; AI pipeline.</strong> The system aggregates regulatory recall notices from 66 primary sources across 60+ countries (FDA, USDA FSIS, RASFF, FSA, FSANZ, CFIA, RappelConso, BVL, AESAN, EFET, and national authorities) and processes each record through Gemini (extraction), OpenAI GPT (normalisation), and Claude (Tier-1 validation). Records are de-duplicated and harmonised into the accumulative dataset.</p>
  <p><strong>This briefing.</strong> Statistical analysis filters the accumulative dataset to the reporting week ({period}). AI-generated narrative is produced against AFTS process-authority prompts and edited for publication. Figures and pathogen names are preserved verbatim from source data.</p>
</div>

<footer class="footer">
  <div>
    <div class="foot-brand">Advanced Food-Tech Solutions <em>&middot;</em> AFTS</div>
    <div class="foot-meta">Food Safety Validation Intelligence<br>advfood.tech &middot; info@advfood.tech &middot; Athens, Greece<br>&copy; {year} Advanced Food Tech Solutions</div>
  </div>
  <div class="foot-legal">This briefing is provided for informational purposes only and does not constitute regulatory, legal, or medical advice. Subscribers should verify recall status with the originating regulatory authority before taking action. Next issue: Friday, {next_issue}.</div>
</footer>

</body>
</html>"""


def build_html(week_end, recalls, prev_week):
    stats = compute_stats(recalls, prev_week)
    ws = week_end - timedelta(days=6)
    wnum = week_end.isocalendar()[1]; year = week_end.year
    total = stats["total"]
    sr = sort_by_severity(recalls)

    raw = generate_analysis_claude(stats, recalls)
    final = review_with_openai(raw)

    paras = [p.strip() for p in final.strip().split("\n\n") if p.strip()]
    pa_html = ""; reg = []
    for p in paras:
        pl = p.lower()
        if ("botulinum" in pl or "clostridium" in pl) and ("process authority" in pl or "scheduled thermal" in pl or "21 cfr 113" in pl):
            pa_html = '<p class="pa-note"><span class="pa-label">Process Authority Note:</span> ' + esc(p) + '</p>'
        else:
            reg.append(p)
    analysis = "\n".join("  <p>{}</p>".format(esc(p)) for p in reg)
    if pa_html: analysis += "\n" + pa_html

    t5rows = "\n".join(_recall_row(i+1, r, 5) for i,r in enumerate(sr[:5]))
    allrows = "\n".join(_recall_row(i+1, r, 5) for i,r in enumerate(sr))
    if not t5rows: t5rows = '<tr><td class="empty" colspan="6">No recalls this week</td></tr>'
    if not allrows: allrows = t5rows

    d = stats["delta"]; dp = stats["delta_pct"]
    if d < 0:
        dh = '<div class="kpi-delta" style="color:#059669">&#9660; {} ({}%) vs prior week</div>'.format(d, dp)
    elif d > 0:
        dh = '<div class="kpi-delta" style="color:#dc2626">&#9650; +{} (+{}%) vs prior week</div>'.format(d, dp)
    else:
        dh = '<div class="kpi-delta" style="color:var(--muted)">No change vs prior week</div>'

    tp, tc = stats["top_pathogen"]
    tpct = round(tc/max(total,1)*100) if total else 0

    prows = ""
    for path, cnt in stats["pathogen_counts"]:
        pct = round(cnt/max(total,1)*100); dot = _dot_color(path)
        prows += '        <tr>\n          <td><span class="path-dot" style="background:{}"></span><em class="path-name">{}</em></td>\n          <td class="num">{}</td>\n          <td class="num">{}%</td>\n          <td><div class="bar-track"><div class="bar-fill" style="width:{}%;background:{}"></div></div></td>\n        </tr>\n'.format(dot, esc(path), cnt, pct, pct, dot)
    if not prows: prows = '        <tr><td class="empty" colspan="4">No pathogen data</td></tr>\n'

    crows = ""
    for country, cnt in stats["country_counts"]:
        pct = round(cnt/max(total,1)*100)
        auth = GEO_AUTHORITY.get(country, "National Authority")
        crows += '        <tr>\n          <td>{}</td>\n          <td>{}</td>\n          <td class="num">{}</td>\n          <td class="num">{}%</td>\n        </tr>\n'.format(esc(country), esc(auth), cnt, pct)
    if not crows: crows = '        <tr><td class="empty" colspan="4">No geographic data</td></tr>\n'

    wsd = _fmt_date_short(ws); wed = _fmt_date(week_end)
    period = "{} &ndash; {}".format(wsd, wed)
    pub = datetime.now(timezone.utc).strftime("%-d %b %Y &middot; %H:%M UTC")
    nf = _fmt_date(week_end + timedelta(days=7))

    html = HTML_TEMPLATE.format(
        wnum=wnum, year=year, period=period, published=pub, total=total,
        n_jurisdictions=len(stats["country_counts"]), delta_html=dh,
        tier1=stats["tier1"], outbreaks=stats["outbreaks"],
        top_pathogen_name=esc(tp), top_cnt=tc, top_pct=tpct,
        analysis_html=analysis, top5_rows=t5rows, pathogen_rows=prows,
        country_rows=crows, all_rows=allrows, next_issue=nf,
    )
    html = html.replace("__CSS_PLACEHOLDER__", W16_CSS)
    return html, stats

def update_dashboard_data(week_end, stats, all_recalls=None):
    """Update index.html — always shows the 4 most recent weeks that have data
    in the dataset, regardless of which --week-end was passed to the builder."""
    idx = ROOT / "docs" / "index.html"
    if not idx.exists():
        log.warning("index.html not found — skipping dashboard update"); return

    def _make_entry(we, st):
        wn = we.isocalendar()[1]; yr = we.year; ws = we - timedelta(days=6)
        tp_name = st["top_pathogen"][0] if st["top_pathogen"] else "Mixed"
        return {"filename":"{}-W{:02d}.html".format(yr,wn),"week_num":wn,"year":yr,
                "week_start":ws.strftime("%Y-%m-%d"),"week_end":we.strftime("%Y-%m-%d"),
                "generated":datetime.now(timezone.utc).isoformat(),
                "total":st["total"],"tier1":st["tier1"],"outbreaks":st["outbreaks"],
                "top_pathogen":tp_name,
                "summary":"Week {} saw {} pathogen recalls with {} as primary concern.".format(
                    wn, st["total"], tp_name)}

    entries = []

    if all_recalls:
        # Find the latest recall date in the dataset → snap to its Friday
        latest = date(2020, 1, 1)
        for r in all_recalls:
            d = r.get("Date", "")
            if not d: continue
            try: rd = datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
            except ValueError: continue
            if rd > latest: latest = rd
        # Snap to the Friday of that week (weekday 4 = Friday)
        days_until_fri = (4 - latest.weekday()) % 7
        if days_until_fri == 0 and latest.weekday() != 4:
            days_until_fri = 7
        latest_friday = latest + timedelta(days=days_until_fri)
        # If latest_friday is in the future beyond today+7, cap it
        today = date.today()
        if latest_friday > today + timedelta(days=7):
            latest_friday = today - timedelta(days=(today.weekday() - 4) % 7)

        # Build 4 entries backward from latest_friday
        for offset in range(4):
            we = latest_friday - timedelta(days=7 * offset)
            prev_we = we - timedelta(days=7)
            wr = filter_week(all_recalls, we)
            if not wr: continue
            pr = filter_week(all_recalls, prev_we)
            st = compute_stats(wr, pr)
            entries.append(_make_entry(we, st))
    else:
        # Fallback: just the current week
        entries = [_make_entry(week_end, stats)]

    try:
        c = idx.read_text()
        c = re.sub(r"const reports = \[.*?\];",
                    "const reports = {};".format(json.dumps(entries, indent=4)),
                    c, flags=re.DOTALL)
        idx.write_text(c)
        wk_list = ", ".join("W{}".format(e["week_num"]) for e in entries)
        log.info("Dashboard updated — %d reports: %s", len(entries), wk_list)
    except Exception as e:
        log.error("Dashboard: %s", e)

def write_weekly_summary_json(week_end, recalls, stats, data_dir):
    wnum = week_end.isocalendar()[1]; year = week_end.year; ws = week_end - timedelta(days=6)
    tp = stats.get("top_pathogen")
    leading = {"name":tp[0],"cases":tp[1],"pct":round(tp[1]/max(stats["total"],1)*100)} if tp and len(tp)>=2 else {"name":"Mixed","cases":0,"pct":0}
    sr = sort_by_severity(recalls); threats = []
    for i,r in enumerate(sr[:5],1):
        threats.append({"rank":i,"date":str(r.get("Date",""))[:10],
            "pathogen":str(r.get("Pathogen","")),"pathogen_raw":str(r.get("Pathogen","")),
            "tier":_safe_int(r.get("Tier",2)),"outbreak":bool(_safe_int(r.get("Outbreak",0))),
            "company":str(r.get("Company","")),"brand":str(r.get("Brand","\u2014")),
            "product":str(r.get("Product","")),"country":str(r.get("Country","")),
            "source":str(r.get("Source","")),"url":str(r.get("URL",""))})
    summary = {"filename":"{}-W{:02d}.html".format(year,wnum),
        "report_url":"https://gstoforos.github.io/Food-Safety-Intelligence-System/{}-W{:02d}.html".format(year,wnum),
        "dashboard_url":"https://www.advfood.tech/food-safety-intelligence",
        "week_num":wnum,"year":year,"week_start":ws.isoformat(),"week_end":week_end.isoformat(),
        "week_start_display":ws.strftime("%-d %b"),"week_end_display":week_end.strftime("%-d %b %Y"),
        "generated_utc":datetime.now(timezone.utc).isoformat(),
        "stats":{"total":stats["total"],"tier1":stats["tier1"],"outbreaks":stats["outbreaks"],
                 "delta":stats.get("delta",0),"delta_pct":stats.get("delta_pct",0)},
        "leading_pathogen":leading,"ai_lead_paragraph":"","top_threats":threats,
        "country_count":len(set(str(r.get("Country","")) for r in recalls if r.get("Country")))}
    out = data_dir / "weekly-summary-latest.json"
    out.write_text(json.dumps(summary,indent=2,ensure_ascii=False),encoding="utf-8")
    log.info("Wrote %s",out)

def _extract_total_from_html(path):
    """Read an existing report HTML and extract the total-recalls KPI.
    Returns int or None if the file doesn't exist or can't be parsed."""
    try:
        html = Path(path).read_text(encoding="utf-8")
        m = re.search(r'<a href="#all-recalls">(\d+)</a>', html)
        if m: return int(m.group(1))
        # Fallback: look for "All N Recalls" in §04 heading
        m2 = re.search(r'All (\d+) Recalls', html)
        if m2: return int(m2.group(1))
    except Exception:
        pass
    return None


def refresh_stale_weeks(all_recalls, current_week_end, n_previous=1):
    """Check up to n_previous weeks before current_week_end.
    If the recall count in the dataset differs from what's baked into
    the existing HTML report, rebuild that week's report.
    Returns list of rebuilt week-end dates."""
    rebuilt = []
    for offset in range(1, n_previous + 1):
        prev_end = current_week_end - timedelta(days=7 * offset)
        prev_prev_end = prev_end - timedelta(days=7)
        wnum = prev_end.isocalendar()[1]
        year = prev_end.year
        report_path = ROOT / "docs" / "{}-W{:02d}.html".format(year, wnum)

        dataset_recalls = filter_week(all_recalls, prev_end)
        dataset_total = len(dataset_recalls)

        existing_total = _extract_total_from_html(report_path)

        if existing_total is None:
            log.info("W%02d: no existing report at %s — building fresh", wnum, report_path)
        elif existing_total == dataset_total:
            log.info("W%02d: report matches dataset (%d recalls) — no refresh needed", wnum, dataset_total)
            continue
        else:
            log.info("W%02d: STALE — report has %d recalls, dataset has %d — rebuilding",
                      wnum, existing_total, dataset_total)

        prev_week_recalls = filter_week(all_recalls, prev_prev_end)
        html, stats = build_html(prev_end, dataset_recalls, prev_week_recalls)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(html, encoding="utf-8")
        log.info("W%02d refreshed -> %s (%d bytes, %d recalls)",
                  wnum, report_path, len(html), dataset_total)
        rebuilt.append(prev_end)

    return rebuilt


def main():
    ap = argparse.ArgumentParser(description="Build AFTS weekly report")
    ap.add_argument("--week-end", required=True, help="Friday YYYY-MM-DD")
    ap.add_argument("--xlsx", default=str(ROOT/"docs"/"data"/"recalls.xlsx"))
    ap.add_argument("--output", default=None)
    ap.add_argument("--refresh-previous", type=int, default=1, metavar="N",
                    help="Check N previous weeks for stale data and rebuild if needed (default: 1)")
    ap.add_argument("--no-refresh", action="store_true",
                    help="Skip stale-week refresh (build current week only)")
    args = ap.parse_args()
    week_end = datetime.strptime(args.week_end,"%Y-%m-%d").date()
    log.info("Building report for %s", week_end)
    all_r = load_recalls(Path(args.xlsx))
    log.info("Loaded %d recalls", len(all_r))

    # --- Refresh stale previous weeks ---
    if not args.no_refresh and args.refresh_previous > 0:
        log.info("Checking %d previous week(s) for stale data...", args.refresh_previous)
        rebuilt = refresh_stale_weeks(all_r, week_end, args.refresh_previous)
        if rebuilt:
            log.info("Refreshed %d stale report(s): %s",
                      len(rebuilt), ", ".join(d.strftime("W%V") for d in rebuilt))

    # --- Build current week ---
    wr = filter_week(all_r, week_end)
    pr = filter_week(all_r, week_end - timedelta(days=7))
    log.info("This week: %d  Prev: %d", len(wr), len(pr))
    html, stats = build_html(week_end, wr, pr)
    wnum = week_end.isocalendar()[1]
    out = Path(args.output) if args.output else ROOT/"docs"/"{}-W{:02d}.html".format(week_end.year, wnum)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    log.info("Report -> %s (%d bytes)", out, len(html))
    update_dashboard_data(week_end, stats, all_r)
    write_weekly_summary_json(week_end, wr, stats, Path(args.xlsx).parent)
    return 0

if __name__ == "__main__":
    sys.exit(main())
