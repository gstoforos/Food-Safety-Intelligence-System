"""
FSIS Agent – HTML Report Generator
Produces a single self-contained HTML file (no external deps except Google Fonts CDN).
Output: docs/index.html  →  GitHub Pages  →  Wix iframe embed
"""
import os
import json
from datetime import datetime, timezone
from pathlib import Path

from config import (
    REPORT_OUTPUT, REPORT_TITLE, BRAND_NAME, BRAND_URL,
    COLOR_BG, COLOR_SURFACE, COLOR_ORANGE, COLOR_WHITE, COLOR_MUTED,
    COLOR_CRITICAL, COLOR_MODERATE, COLOR_LOW, CLASS_SEVERITY
)
from database import get_recent_recalls, get_stats

# ── Severity badge ────────────────────────────────────────────────────────────

def _badge(classification: str) -> str:
    sev = CLASS_SEVERITY.get(classification, "UNKNOWN")
    colors = {
        "CRITICAL": (COLOR_CRITICAL, "#fff"),
        "MODERATE": (COLOR_MODERATE, "#111"),
        "LOW":      (COLOR_LOW,      "#fff"),
        "UNKNOWN":  ("#555",          "#fff"),
    }
    bg, fg = colors.get(sev, ("#555", "#fff"))
    return (
        f'<span style="background:{bg};color:{fg};padding:2px 8px;'
        f'border-radius:3px;font-size:11px;font-weight:700;'
        f'letter-spacing:.05em;">{sev}</span>'
    )


def _esc(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")


# ── Sparkline (SVG bars – no JS lib) ─────────────────────────────────────────

def _sparkline(monthly: list[dict]) -> str:
    if not monthly:
        return ""
    months = [m["month"] for m in monthly]
    counts = [m["cnt"]   for m in monthly]
    mx     = max(counts) if counts else 1
    w, h   = 600, 120
    bar_w  = w / max(len(counts), 1)
    bars   = ""
    labels = ""

    for i, (mon, cnt) in enumerate(zip(months, counts)):
        bh   = int((cnt / mx) * (h - 30)) + 2
        x    = i * bar_w
        y    = h - bh - 20
        bars += (
            f'<rect x="{x+2}" y="{y}" width="{bar_w-4}" height="{bh}" '
            f'fill="{COLOR_ORANGE}" rx="2" opacity="0.85"/>'
            f'<title>{mon}: {cnt}</title>'
        )
        # label every 3 months
        if i % 3 == 0:
            short = mon[5:]  # MM
            labels += (
                f'<text x="{x + bar_w/2}" y="{h-2}" text-anchor="middle" '
                f'font-size="9" fill="{COLOR_MUTED}">{short}</text>'
            )

    return (
        f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" '
        f'xmlns="http://www.w3.org/2000/svg">{bars}{labels}</svg>'
    )


# ── Donut chart (SVG) ─────────────────────────────────────────────────────────

def _donut(by_class: list[dict]) -> str:
    palette = [COLOR_CRITICAL, COLOR_MODERATE, COLOR_LOW, "#555"]
    r, cx, cy, stroke = 50, 70, 70, 28
    total = sum(d["cnt"] for d in by_class) or 1
    offset = 0
    slices = ""
    legend = ""
    circ   = 2 * 3.14159 * r

    for i, d in enumerate(by_class[:4]):
        frac  = d["cnt"] / total
        dash  = frac * circ
        color = palette[i % len(palette)]
        slices += (
            f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" '
            f'stroke="{color}" stroke-width="{stroke}" '
            f'stroke-dasharray="{dash:.1f} {circ:.1f}" '
            f'stroke-dashoffset="-{offset:.1f}" '
            f'transform="rotate(-90 {cx} {cy})">'
            f'<title>{d["classification"]}: {d["cnt"]}</title></circle>'
        )
        offset += dash
        pct = int(frac * 100)
        legend += (
            f'<div style="display:flex;align-items:center;gap:6px;margin:3px 0;">'
            f'<span style="width:10px;height:10px;border-radius:2px;'
            f'background:{color};flex-shrink:0;"></span>'
            f'<span style="font-size:12px;color:{COLOR_MUTED};">'
            f'{_esc(d["classification"])} – {d["cnt"]} ({pct}%)</span></div>'
        )

    svg = (
        f'<svg viewBox="0 0 140 140" width="140" height="140" '
        f'xmlns="http://www.w3.org/2000/svg">'
        f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{COLOR_SURFACE}"/>'
        f'{slices}'
        f'<text x="{cx}" y="{cy+5}" text-anchor="middle" '
        f'font-size="14" fill="{COLOR_WHITE}" font-weight="700">{total}</text>'
        f'<text x="{cx}" y="{cy+18}" text-anchor="middle" '
        f'font-size="8" fill="{COLOR_MUTED}">RECALLS</text>'
        f'</svg>'
    )
    return f'<div style="display:flex;align-items:center;gap:20px;">{svg}<div>{legend}</div></div>'


# ── Recalls table ─────────────────────────────────────────────────────────────

def _table(recalls: list[dict], limit: int = 20) -> str:
    rows = ""
    for r in recalls[:limit]:
        pathogen = _esc(r.get("pathogen") or "—")
        conf     = r.get("pathogen_confidence", "")
        conf_col = {"HIGH": COLOR_CRITICAL, "MEDIUM": COLOR_MODERATE, "LOW": COLOR_LOW}.get(conf, COLOR_MUTED)
        summary  = _esc(r.get("risk_summary") or r.get("reason", "")[:80])

        rows += f"""
        <tr>
          <td style="color:{COLOR_MUTED};font-size:11px;">{_esc(r.get('recall_date',''))}</td>
          <td style="font-size:12px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;"
              title="{_esc(r.get('product',''))}">{_esc(r.get('product','')[:60])}</td>
          <td style="font-size:12px;">{_esc(r.get('firm',''))[:40]}</td>
          <td style="color:{conf_col};font-weight:700;font-size:12px;">{pathogen}</td>
          <td>{_badge(r.get('classification',''))}</td>
          <td style="font-size:11px;color:{COLOR_MUTED};max-width:200px;overflow:hidden;
              text-overflow:ellipsis;white-space:nowrap;" title="{summary}">{summary}</td>
        </tr>"""

    return f"""
    <table style="width:100%;border-collapse:collapse;font-family:'DM Mono',monospace;">
      <thead>
        <tr style="border-bottom:1px solid #333;">
          <th style="text-align:left;padding:8px 6px;color:{COLOR_MUTED};font-size:11px;font-weight:600;letter-spacing:.08em;">DATE</th>
          <th style="text-align:left;padding:8px 6px;color:{COLOR_MUTED};font-size:11px;font-weight:600;letter-spacing:.08em;">PRODUCT</th>
          <th style="text-align:left;padding:8px 6px;color:{COLOR_MUTED};font-size:11px;font-weight:600;letter-spacing:.08em;">FIRM</th>
          <th style="text-align:left;padding:8px 6px;color:{COLOR_MUTED};font-size:11px;font-weight:600;letter-spacing:.08em;">PATHOGEN</th>
          <th style="text-align:left;padding:8px 6px;color:{COLOR_MUTED};font-size:11px;font-weight:600;letter-spacing:.08em;">CLASS</th>
          <th style="text-align:left;padding:8px 6px;color:{COLOR_MUTED};font-size:11px;font-weight:600;letter-spacing:.08em;">SUMMARY</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>"""


# ── Full HTML ─────────────────────────────────────────────────────────────────

def generate_report(days: int = 30):
    Path("docs").mkdir(exist_ok=True)

    stats   = get_stats(days)
    recalls = get_recent_recalls(days)
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    year    = datetime.now().year

    # Pathogen breakdown text
    path_items = "".join(
        f'<div style="display:flex;justify-content:space-between;align-items:center;'
        f'padding:6px 0;border-bottom:1px solid #222;">'
        f'<span style="font-size:13px;">{_esc(p["pathogen"])}</span>'
        f'<span style="color:{COLOR_ORANGE};font-weight:700;font-size:13px;">{p["cnt"]}</span>'
        f'</div>'
        for p in stats["by_pathogen"]
    )

    # Top stat cards
    total    = stats["total"]
    critical = sum(d["cnt"] for d in stats["by_class"] if "I" in d["classification"] and "II" not in d["classification"])
    enriched = sum(1 for r in recalls if r.get("enriched"))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{REPORT_TITLE}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{
    background: {COLOR_BG};
    color: {COLOR_WHITE};
    font-family: 'DM Sans', sans-serif;
    font-size: 14px;
    line-height: 1.6;
    padding: 0;
  }}
  .header {{
    background: {COLOR_SURFACE};
    border-bottom: 2px solid {COLOR_ORANGE};
    padding: 18px 28px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }}
  .brand {{
    font-family: 'Syne', sans-serif;
    font-size: 18px;
    font-weight: 800;
    letter-spacing: -.01em;
  }}
  .brand span {{ color: {COLOR_ORANGE}; }}
  .updated {{
    font-family: 'DM Mono', monospace;
    font-size: 11px;
    color: {COLOR_MUTED};
  }}
  .container {{ padding: 24px 28px; max-width: 1100px; margin: 0 auto; }}
  .section-title {{
    font-family: 'Syne', sans-serif;
    font-size: 13px;
    font-weight: 700;
    letter-spacing: .12em;
    color: {COLOR_ORANGE};
    text-transform: uppercase;
    margin-bottom: 14px;
    padding-bottom: 6px;
    border-bottom: 1px solid #333;
  }}
  .grid-3 {{ display: grid; grid-template-columns: repeat(3,1fr); gap:14px; margin-bottom:28px; }}
  .stat-card {{
    background: {COLOR_SURFACE};
    border: 1px solid #2a2a2a;
    border-radius: 6px;
    padding: 16px 20px;
  }}
  .stat-card .val {{
    font-family: 'Syne', sans-serif;
    font-size: 36px;
    font-weight: 800;
    color: {COLOR_ORANGE};
    line-height: 1;
  }}
  .stat-card .lbl {{
    font-size: 11px;
    color: {COLOR_MUTED};
    letter-spacing: .08em;
    text-transform: uppercase;
    margin-top: 4px;
  }}
  .panel {{
    background: {COLOR_SURFACE};
    border: 1px solid #2a2a2a;
    border-radius: 6px;
    padding: 20px;
    margin-bottom: 20px;
  }}
  .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
  table tr:hover {{ background: rgba(255,255,255,.03); }}
  td, th {{ padding: 8px 6px; vertical-align: middle; }}
  .footer {{
    text-align: center;
    padding: 20px;
    font-size: 11px;
    color: {COLOR_MUTED};
    border-top: 1px solid #222;
    margin-top: 10px;
  }}
  .footer a {{ color: {COLOR_ORANGE}; text-decoration: none; }}
  @media (max-width: 640px) {{
    .grid-3, .grid-2 {{ grid-template-columns: 1fr; }}
    .header {{ flex-direction: column; gap: 8px; align-items: flex-start; }}
  }}
</style>
</head>
<body>

<!-- HEADER -->
<div class="header">
  <div class="brand">AFTS <span>·</span> Food Safety Intelligence</div>
  <div class="updated">FDA · Last updated: {now_utc}</div>
</div>

<div class="container">

  <!-- KPI CARDS -->
  <div class="grid-3" style="margin-top:24px;">
    <div class="stat-card">
      <div class="val">{total}</div>
      <div class="lbl">Pathogen Recalls – Last {days} Days</div>
    </div>
    <div class="stat-card">
      <div class="val" style="color:{COLOR_CRITICAL};">{critical}</div>
      <div class="lbl">Class I (Life-Threatening)</div>
    </div>
    <div class="stat-card">
      <div class="val">{len(stats['by_pathogen'])}</div>
      <div class="lbl">Distinct Pathogens Detected</div>
    </div>
  </div>

  <!-- TREND + BREAKDOWN -->
  <div class="grid-2">
    <div class="panel">
      <div class="section-title">Monthly Trend (12 months)</div>
      {_sparkline(stats['by_month'])}
    </div>
    <div class="panel">
      <div class="section-title">By Classification</div>
      {_donut(stats['by_class'])}
    </div>
  </div>

  <!-- PATHOGEN BREAKDOWN -->
  <div class="grid-2">
    <div class="panel">
      <div class="section-title">Top Pathogens</div>
      {path_items or '<div style="color:#555;font-size:12px;">No data</div>'}
    </div>
    <div class="panel">
      <div class="section-title">Data Coverage</div>
      <div style="font-size:12px;color:{COLOR_MUTED};line-height:2;">
        <div>Source: <span style="color:{COLOR_WHITE};">FDA openFDA API</span></div>
        <div>Window: <span style="color:{COLOR_WHITE};">Last {days} days</span></div>
        <div>AI-enriched: <span style="color:{COLOR_WHITE};">{enriched} / {len(recalls)} records</span></div>
        <div>Pathogens only: <span style="color:{COLOR_WHITE};">allergens &amp; label errors excluded</span></div>
        <div>Powered by: <span style="color:{COLOR_ORANGE};">Gemini 2.0 Flash</span></div>
      </div>
    </div>
  </div>

  <!-- RECALLS TABLE -->
  <div class="panel">
    <div class="section-title">Recent Pathogen Recalls (latest {min(20,len(recalls))})</div>
    {_table(recalls, 20)}
  </div>

</div>

<!-- FOOTER -->
<div class="footer">
  <a href="{BRAND_URL}" target="_blank">{BRAND_NAME}</a> &nbsp;·&nbsp;
  Data: <a href="https://open.fda.gov/apis/food/enforcement/" target="_blank">openFDA</a>
  &nbsp;·&nbsp; Updated automatically every 24 h &nbsp;·&nbsp; {year}
</div>

</body>
</html>"""

    out = Path(REPORT_OUTPUT)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"Report written → {REPORT_OUTPUT}")
    return str(out.resolve())


if __name__ == "__main__":
    generate_report()
