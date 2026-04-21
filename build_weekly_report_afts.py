"""
FSIS Global — Monthly Intelligence Report
OpenAI GPT-4o narrative + predictive risk model.
Run: 1st of each month 07:00 UTC.
Cost: ~$1.50-2.50/month (GPT-4o-mini analysis + GPT-4o narrative).
"""
import os, json, logging, statistics
from datetime import datetime, timezone, timedelta
from pathlib import Path
import requests
from database import get_recalls, get_stats_for_period, get_historical_baseline, get_monthly_trend

log = logging.getLogger("report.monthly")
OPENAI_URL = "https://api.openai.com/v1/chat/completions"


def _call_openai(model, messages, max_tokens=1500):
    key = os.environ.get("OPENAI_API_KEY","").strip()
    if not key:
        raise EnvironmentError("OPENAI_API_KEY not set")
    r = requests.post(OPENAI_URL,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={"model": model, "messages": messages, "max_tokens": max_tokens, "temperature": 0.3},
        timeout=90)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


def _esc(s):
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def _pathogen_color(p):
    p = (p or "").lower()
    if "listeria" in p: return "#ff8c5a"
    if "salmonella" in p: return "#7ab8e8"
    if "e. coli" in p or "stec" in p: return "#f0c040"
    if "botulinum" in p: return "#ce93d8"
    if "norovirus" in p: return "#80d4a8"
    if "aflatoxin" in p or "mycotoxin" in p: return "#ffcc80"
    return "#888"


# ══════════════════════════════════════════════════════════════════════════════
#  PREDICTIVE MODEL
# ══════════════════════════════════════════════════════════════════════════════

def build_predictions(current_month: str) -> dict:
    """
    Build risk predictions for next month using 5-year historical baseline.
    Returns a dict with risk scores and elevated pathogens.
    """
    baseline = get_historical_baseline()
    next_month_num = str(int(current_month[5:7]) % 12 + 1).zfill(2)
    next_month_name = datetime.strptime(next_month_num, "%m").strftime("%B")

    predictions = []

    for pathogen, monthly_avg in baseline.items():
        if pathogen in ("Unknown", "", None):
            continue
        # Get historical average for next month
        hist_avg = monthly_avg.get(next_month_num, 0)
        if hist_avg < 0.5:
            continue  # too rare to predict

        # Get current 3-month trend
        trend_months = []
        now = datetime.now()
        for i in range(3):
            m = (now - timedelta(days=30*i)).strftime("%m")
            trend_months.append(monthly_avg.get(m, 0))

        # Calculate trend direction
        if len(trend_months) >= 2:
            trend = trend_months[0] - trend_months[-1]
            trend_dir = "↑" if trend > 0.5 else "↓" if trend < -0.5 else "→"
        else:
            trend_dir = "→"

        # Overall mean and std across all months
        all_vals = list(monthly_avg.values())
        if len(all_vals) < 2:
            continue
        mean = statistics.mean(all_vals)
        try:
            std = statistics.stdev(all_vals)
        except:
            std = 0.1

        # Risk score: how much above average is next month's historical value?
        if std > 0:
            z_score = (hist_avg - mean) / std
        else:
            z_score = 0

        # Normalize to 0-10 scale
        risk_score = min(10, max(0, round(5 + z_score * 2, 1)))

        # Peak month detection
        peak_month = max(monthly_avg, key=monthly_avg.get) if monthly_avg else "01"
        is_peak = (peak_month == next_month_num)

        predictions.append({
            "pathogen":       pathogen,
            "color":          _pathogen_color(pathogen),
            "hist_avg":       round(hist_avg, 1),
            "risk_score":     risk_score,
            "trend":          trend_dir,
            "is_peak":        is_peak,
            "peak_month_name": datetime.strptime(peak_month, "%m").strftime("%B"),
        })

    # Sort by risk score descending
    predictions.sort(key=lambda x: x["risk_score"], reverse=True)

    return {
        "next_month":  next_month_name,
        "predictions": predictions[:12],
        "top_risk":    predictions[0] if predictions else {},
        "elevated":    [p for p in predictions if p["risk_score"] >= 7],
    }


# ══════════════════════════════════════════════════════════════════════════════
#  REPORT GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def generate():
    now        = datetime.now(timezone.utc)
    month_key  = now.strftime("%Y-%m")
    month_name = now.strftime("%B %Y")
    date_to    = now.strftime("%Y-%m-%d")
    date_from  = (now.replace(day=1)).strftime("%Y-%m-%d")
    year       = now.year

    recalls = get_recalls(days=35)  # slight overlap for month boundaries
    st      = get_stats_for_period(date_from, date_to)
    trend   = get_monthly_trend(months=13)
    preds   = build_predictions(month_key)

    log.info(f"Monthly report — {month_name} — {st['total']} recalls")

    # ── OpenAI Step 1: Data analysis (gpt-4o-mini, cheap) ───────────────────
    analysis_data = {}
    try:
        top_recalls = "\n".join(
            f"- {r.get('recall_date','')} | {r.get('source','')} | {r.get('firm','')} | "
            f"{r.get('product','')[:50]} | {r.get('pathogen_ai') or r.get('pathogen','')} | "
            f"{r.get('classification','')} | outbreak:{r.get('is_outbreak',0)}"
            for r in recalls[:25]
        )
        path_summary = "; ".join(f"{p['p']}:{p['c']}" for p in st["by_path"][:8])
        src_summary  = "; ".join(f"{s['source']}:{s['c']}" for s in st["by_src"][:6])

        analysis_raw = _call_openai("gpt-4o-mini", [{
            "role": "user",
            "content": f"""Analyze this food safety data for {month_name} and return ONLY JSON:
Total recalls: {st['total']}
Critical (Class I): {st['critical']}
Outbreaks: {st['outbreaks']}
Pathogens: {path_summary}
Sources: {src_summary}
Top 25 recalls:
{top_recalls}

Return JSON only:
{{"dominant_pathogen":"...","dominant_pct":"...%","highest_risk_region":"...","highest_risk_product_category":"...","notable_events":["...","..."],"emerging_trends":["...","...","..."],"key_concern":"one sentence"}}"""
        }], max_tokens=400)
        analysis_data = json.loads(analysis_raw.strip().strip("```json").strip("```"))
    except Exception as e:
        log.warning(f"OpenAI analysis failed: {e}")
        analysis_data = {}

    # ── OpenAI Step 2: Executive narrative (gpt-4o, quality) ────────────────
    narrative = ""
    try:
        narrative = _call_openai("gpt-4o", [{
            "role": "user",
            "content": f"""You are a food safety intelligence director. Write a monthly brief for {month_name}.

Data: {st['total']} recalls, {st['critical']} Class I, {st['outbreaks']} outbreaks
Top pathogens: {path_summary}
Key concern: {analysis_data.get('key_concern','')}
Notable events: {'; '.join(analysis_data.get('notable_events',[]))}
Risk outlook for next month: {', '.join(p['pathogen'] for p in preds['elevated'][:3])}

Write 4 focused paragraphs (max 300 words total):
1. Month summary — what defined this month
2. Key incidents — 2-3 most significant events and implications
3. Trend analysis — patterns across pathogens, geographies, product categories
4. Next month outlook — what to watch, elevated risks

Plain paragraphs, no bullet points, no HTML, authoritative tone."""
        }], max_tokens=450)
    except Exception as e:
        log.warning(f"OpenAI narrative failed: {e}")
        narrative = f"Monthly data processed: {st['total']} pathogen recalls recorded in {month_name} across all monitored sources."

    # ── Build HTML ────────────────────────────────────────────────────────────
    html = _build_html(month_name, month_key, year, st, recalls,
                       preds, trend, analysis_data, narrative,
                       datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))

    Path("docs/monthly").mkdir(parents=True, exist_ok=True)
    out = Path(f"docs/monthly/{month_key}.html")
    out.write_text(html, encoding="utf-8")
    _update_index(month_name, out.name, st["total"])
    log.info(f"Monthly report → {out}")
    return str(out)


def _build_html(month_name, month_key, year, st, recalls, preds, trend,
                analysis, narrative, now_utc):

    def path_rows():
        return "".join(
            f'<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid #1e1e1e;">'
            f'<span style="font-size:13px;color:{_pathogen_color(p["p"])};">{_esc(p["p"] or "Unknown")}</span>'
            f'<span style="color:#E8601A;font-weight:700;">{p["c"]}</span></div>'
            for p in st["by_path"][:8]
        )

    def pred_rows():
        html = ""
        for p in preds["predictions"][:8]:
            bar_w = int(p["risk_score"] * 10)
            peak_flag = ' <span style="background:#E8601A22;color:#E8601A;font-size:9px;padding:1px 5px;border-radius:2px;font-family:monospace;">PEAK MONTH</span>' if p["is_peak"] else ""
            html += f"""<div style="margin-bottom:12px;">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
  <span style="font-size:13px;color:{p['color']};font-weight:500;">{_esc(p['pathogen'])}{peak_flag}</span>
  <span style="font-family:monospace;font-size:12px;color:#888;">{p['trend']} &nbsp; score: <span style="color:#E8601A;font-weight:700;">{p['risk_score']}/10</span></span>
</div>
<div style="background:#1e1e1e;border-radius:2px;height:4px;overflow:hidden;">
  <div style="background:{p['color']};width:{bar_w}%;height:100%;border-radius:2px;"></div>
</div>
<div style="font-size:10px;color:#555;margin-top:2px;font-family:monospace;">Historical avg {preds['next_month']}: {p['hist_avg']}/month &nbsp;·&nbsp; Peak: {p['peak_month_name']}</div>
</div>"""
        return html

    def recall_rows():
        html = ""
        for r in sorted(recalls[:15], key=lambda x: x.get("recall_date",""), reverse=True):
            pathogen = r.get("pathogen_ai") or r.get("pathogen") or "—"
            col = _pathogen_color(pathogen)
            html += f"""<tr>
<td style="font-family:monospace;font-size:11px;color:#666;">{_esc(r.get('recall_date',''))}</td>
<td style="font-size:11px;">{_esc(r.get('source',''))}</td>
<td style="font-weight:500;font-size:12px;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{_esc(r.get('firm','')[:50])}</td>
<td style="font-size:11px;color:#bbb;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{_esc(r.get('product','')[:55])}</td>
<td style="color:{col};font-weight:700;font-size:11px;font-family:monospace;">{_esc(pathogen)}</td>
<td style="font-size:11px;color:#666;">{_esc(r.get('country',''))}</td>
</tr>"""
        return html

    narrative_html = "".join(
        f'<p style="margin-bottom:14px;line-height:1.75;font-size:15px;color:#ddd;">{_esc(p)}</p>'
        for p in narrative.split("\n\n") if p.strip()
    )

    elevated_tags = "".join(
        f'<span style="background:rgba(232,96,26,.12);border:1px solid rgba(232,96,26,.3);color:{p["color"]};padding:4px 12px;border-radius:3px;font-size:12px;font-family:monospace;font-weight:700;margin:3px;">{_esc(p["pathogen"])} {p["trend"]} {p["risk_score"]}/10</span>'
        for p in preds.get("elevated", [])[:5]
    ) or '<span style="color:#555;font-size:12px;">No elevated risk pathogens identified</span>'

    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AFTS Monthly Intelligence — {month_name}</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:#0e0e0e;color:#f0f0f0;font-family:'DM Sans',sans-serif;line-height:1.6;}}
.hdr{{background:#161616;border-bottom:2px solid #E8601A;padding:24px 40px;}}
.report-type{{font-family:'DM Mono',monospace;font-size:11px;color:#E8601A;letter-spacing:.1em;text-transform:uppercase;margin-bottom:4px;}}
h1{{font-family:'Syne',sans-serif;font-size:30px;font-weight:800;}}
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
.ai-badge{{display:inline-block;background:rgba(91,155,213,.1);border:1px solid rgba(91,155,213,.25);color:#5b9bd5;padding:3px 10px;border-radius:3px;font-size:10px;font-family:monospace;margin-left:8px;}}
table{{width:100%;border-collapse:collapse;font-size:12px;}}
thead tr{{border-bottom:1px solid #2a2a2a;}}
th{{text-align:left;padding:8px;font-size:10px;color:#555;letter-spacing:.08em;}}
td{{padding:7px 8px;border-bottom:1px solid #161616;vertical-align:middle;}}
tr:hover td{{background:rgba(255,255,255,.025);}}
.footer{{text-align:center;padding:28px;font-size:11px;color:#555;border-top:1px solid #1e1e1e;font-family:monospace;}}
.footer a{{color:#E8601A;text-decoration:none;}}
@media(max-width:600px){{.kpi-row,.grid-2{{grid-template-columns:1fr;}}.wrap{{padding:20px;}}}}
</style></head><body>

<div class="hdr">
  <div class="report-type">Monthly Intelligence Report</div>
  <h1>{month_name}</h1>
  <div class="meta">AI analysis: GPT-4o &nbsp;·&nbsp; Data: 50+ global sources &nbsp;·&nbsp; Generated: {now_utc}</div>
</div>

<div class="wrap">
  <div class="kpi-row" style="margin-top:30px;">
    <div class="kpi"><div class="kv">{st['total']}</div><div class="kl">Total Recalls</div></div>
    <div class="kpi r"><div class="kv">{st['critical']}</div><div class="kl">Class I Critical</div></div>
    <div class="kpi a"><div class="kv">{st['outbreaks']}</div><div class="kl">Active Outbreaks</div></div>
    <div class="kpi b"><div class="kv">{len(set(r.get('country','') for r in recalls if r.get('country')))}</div><div class="kl">Countries Affected</div></div>
  </div>

  <div class="panel">
    <h2>Executive Intelligence Brief <span class="ai-badge">GPT-4o</span></h2>
    <div>{narrative_html}</div>
  </div>

  <div class="grid-2">
    <div class="panel">
      <h2>Top Pathogens This Month</h2>
      {path_rows()}
    </div>
    <div class="panel">
      <h2>Next Month Risk Outlook — {preds['next_month']} <span class="ai-badge">Predictive Model</span></h2>
      <div style="margin-bottom:14px;">{elevated_tags}</div>
      {pred_rows()}
    </div>
  </div>

  <div class="panel">
    <h2>Top Recall Events This Month</h2>
    <table><thead><tr>
      <th>Date</th><th>Source</th><th>Firm</th><th>Product</th><th>Pathogen</th><th>Country</th>
    </tr></thead>
    <tbody>{recall_rows()}</tbody></table>
  </div>
</div>

<div class="footer">
  <a href="https://advfood.tech">AFTS · Advanced Food-Tech Solutions</a> &nbsp;·&nbsp;
  <a href="../index.html">Live Dashboard</a> &nbsp;·&nbsp;
  <a href="../weekly/">Weekly Reports</a> &nbsp;·&nbsp;
  <a href="../yearly/">Yearly Reports</a> &nbsp;·&nbsp; {year}
</div>
</body></html>"""


def _update_index(month_name, filename, count):
    p = Path("docs/monthly/index.html")
    link = f'<li style="padding:6px 0;border-bottom:0.5px solid #222;display:flex;justify-content:space-between;"><a href="{filename}" style="color:#E8601A;text-decoration:none;">{month_name}</a><span style="color:#666;font-size:12px;">{count} recalls</span></li>'
    if p.exists():
        c = p.read_text()
        c = c.replace("</ul>", f"{link}\n</ul>", 1)
        p.write_text(c)
    else:
        p.write_text(f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Monthly Reports | AFTS</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@800&family=DM+Sans&display=swap" rel="stylesheet">
<style>body{{background:#0e0e0e;color:#f0f0f0;font-family:'DM Sans',sans-serif;padding:40px;max-width:700px;margin:0 auto;}}
h1{{font-family:'Syne',sans-serif;font-size:22px;color:#E8601A;margin-bottom:24px;}}
ul{{list-style:none;padding:0;}}</style></head>
<body><h1>AFTS · Monthly Intelligence Reports</h1><ul>{link}</ul></body></html>""")
