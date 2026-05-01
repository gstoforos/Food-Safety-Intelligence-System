"""
AFTS FSIS — Monthly MARKETING one-pager builder.

This is the PUBLIC-FACING PDF that lands on hub.html / advfood.tech and is
linked from monthly-index.json's `pdf_url`. It is DIFFERENT from the
subscriber report:

    docs/2026-M04.html   — full subscriber report (paid product, 9 sections)
    docs/2026-M04.pdf    — render of the above (full subscriber edition)

THIS BUILDER produces:

    docs/marketing/2026-M04.pdf  — single-page marketing teaser:
        • Header bar (AFTS branding)
        • Subhead line with month / window / counts / leading pathogen
        • Three big stat tiles (Total / Tier-1 / Outbreaks)
        • § Top 10 Critical Incidents table (rank/date/pathogen/co/product/src)
        • Footer with generated stamp + contact

The marketing one-pager is the LEAD MAGNET — it shows enough to be
useful (the top-10 ranked incidents) but withholds the analytical depth
of the subscriber report. Subscribers get the full PDF via the Apps
Script mailer; the public sees only this one-pager.

Usage:
    python -m pipeline.build_monthly_marketing \\
        --summary docs/data/monthly-summary-latest.json \\
        --out-dir docs/marketing
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("monthly_marketing")

# Brand palette — matches the subscriber report and AFTS website. Kept in
# sync with docs/build_monthly_report_afts.py BRAND dict.
ORANGE = "#E8601A"
INK    = "#1f2937"
BLACK  = "#0a0e1a"
MUTED  = "#6b7280"
RED    = "#dc2626"
AMBER  = "#f59e0b"
GREEN  = "#059669"
BG     = "#ffffff"
S1     = "#f9fafb"
BRD    = "#e5e7eb"

# Pathogen → tier-1 colour. Mirrors weekly/monthly badge colours so the
# one-pager renders the same chips that appear in the subscriber report.
PATHOGEN_COLOR = {
    "clostridium botulinum":  "#b91c1c",
    "c. botulinum":           "#b91c1c",
    "e. coli":                "#ea580c",
    "e. coli stec":           "#ea580c",
    "stec":                   "#ea580c",
    "listeria monocytogenes": "#dc2626",
    "listeria":               "#dc2626",
    "salmonella":             "#f59e0b",
    "salmonella spp.":        "#f59e0b",
    "campylobacter":          "#d97706",
    "histamine":              "#a16207",
    "bacillus cereus":        "#6b7280",
    "rodenticide":            "#1e293b",
}


# ──────────────────────────────────────────────────────────────────────
# Data helpers
# ──────────────────────────────────────────────────────────────────────
def load_summary(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def fmt_int(n) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return "—"


def pathogen_color(name: str) -> str:
    n = (name or "").strip().lower()
    return PATHOGEN_COLOR.get(n, MUTED)


def _h(s: str) -> str:
    """Minimal HTML escape for inline content."""
    if s is None:
        return ""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;").replace("'", "&#x27;"))


def fmt_date(iso: str) -> str:
    """2026-04-30 → 30 Apr 2026 (matching March example exact format)."""
    try:
        return datetime.fromisoformat(iso[:10]).strftime("%-d %b %Y")
    except Exception:
        return iso or ""


# ──────────────────────────────────────────────────────────────────────
# Top-10 row rendering — table layout matching March one-pager
# ──────────────────────────────────────────────────────────────────────
def render_top10_row(rec: Dict[str, Any]) -> str:
    """Render one <tr> for the § Top 10 table.

    Source layout (from monthly summary's top10 array):
        rank, date, pathogen, pathogen_raw, company, brand, product,
        country, source, tier, outbreak, url, url_ok
    """
    rank      = rec.get("rank", "")
    date      = rec.get("date", "")[:10]
    path_canon = rec.get("pathogen") or rec.get("pathogen_raw", "")
    co        = rec.get("company") or "—"
    country   = rec.get("country", "")
    product   = rec.get("product", "")
    source    = rec.get("source", "")
    tier      = rec.get("tier", 3)
    outbreak  = rec.get("outbreak", 0)
    url       = rec.get("url", "")
    url_ok    = rec.get("url_ok", False)

    # Truncate product to fit one-pager width (~60 chars)
    if len(product) > 78:
        product = product[:75] + "…"
    if len(co) > 40:
        co = co[:38] + "…"

    color = pathogen_color(path_canon)

    chips = []
    if tier == 1:
        chips.append(
            f'<span style="display:inline-block;background:{RED};color:#fff;'
            f'font-size:8.5px;font-weight:700;padding:1.5px 5px;'
            f'border-radius:3px;letter-spacing:0.04em;margin-left:4px">T1</span>'
        )
    if outbreak:
        chips.append(
            f'<span style="display:inline-block;background:{ORANGE};color:#fff;'
            f'font-size:8.5px;font-weight:700;padding:1.5px 5px;'
            f'border-radius:3px;letter-spacing:0.04em;margin-left:3px">OUTBREAK</span>'
        )
    chip_html = "".join(chips)

    src_cell = _h(source)
    if url and url_ok:
        src_cell += (
            f'<br><a href="{_h(url)}" target="_blank" '
            f'style="color:{ORANGE};text-decoration:none;'
            f'font-size:9px;font-weight:600">view →</a>'
        )

    return (
        f'<tr style="border-bottom:1px solid {BRD}">'
        f'<td style="padding:7px 6px;font-weight:700;color:{ORANGE};'
        f'font-family:Georgia,serif;font-size:13px;text-align:center">{rank}</td>'
        f'<td style="padding:7px 6px;font-size:9.5px;color:{MUTED};white-space:nowrap">{_h(date)}</td>'
        f'<td style="padding:7px 6px;font-size:10px">'
        f'<span style="display:inline-block;width:7px;height:7px;border-radius:50%;'
        f'background:{color};margin-right:4px"></span>'
        f'<em style="font-style:italic;color:{INK};font-weight:600">{_h(path_canon)}</em>'
        f'{chip_html}</td>'
        f'<td style="padding:7px 6px;font-size:10px">'
        f'<div style="font-weight:700;color:{BLACK}">{_h(co)}</div>'
        f'<div style="color:{MUTED};font-size:9px">{_h(country)}</div></td>'
        f'<td style="padding:7px 6px;font-size:9.5px;color:{INK};line-height:1.4">{_h(product)}</td>'
        f'<td style="padding:7px 6px;font-size:9.5px;color:{MUTED};white-space:nowrap;text-align:right">{src_cell}</td>'
        f'</tr>'
    )


# ──────────────────────────────────────────────────────────────────────
# Page builder
# ──────────────────────────────────────────────────────────────────────
def build_html(summary: Dict[str, Any]) -> str:
    """Build the full single-page marketing one-pager HTML.

    Layout matches the March 2026 reference exactly:
      header bar → subhead line → 3 stat tiles → § Top 10 table → footer
    """
    month_name = summary.get("month_name", "Month")
    year       = summary.get("year", "")
    win_start  = summary.get("window_start") or summary.get("month_start") or ""
    win_end    = summary.get("window_end")   or summary.get("month_end")   or ""

    # Header date strip: "01 APR – 30 APR 2026"
    try:
        ws = datetime.fromisoformat(win_start[:10])
        we = datetime.fromisoformat(win_end[:10])
        date_strip = f"{ws.strftime('%d %b').upper()} – {we.strftime('%d %b %Y').upper()}"
    except Exception:
        date_strip = f"{win_start} – {win_end}"

    # Stats — accept either nested s.stats.total or flat s.total
    stats = summary.get("stats") or {}
    total     = stats.get("total")     if stats else summary.get("total", "—")
    tier1     = stats.get("tier1")     if stats else summary.get("tier1", "—")
    outbreaks = stats.get("outbreaks") if stats else summary.get("outbreaks", "—")

    # Leading pathogen — same nested-or-flat tolerance
    lp = summary.get("leading_pathogen") or {}
    if isinstance(lp, dict):
        lead_name = lp.get("name") or "—"
    else:
        tp = summary.get("top_pathogen")
        lead_name = tp[0] if isinstance(tp, list) and tp else (tp or "—")

    # Top 10 — already pre-shaped by the monthly builder. Take up to 10.
    top10 = summary.get("top10") or []
    top10 = top10[:10]
    section_title = f"§ TOP {len(top10)} CRITICAL INCIDENTS · {month_name.upper()} {year}"
    rows_html = "".join(render_top10_row(r) for r in top10) if top10 else (
        f'<tr><td colspan="6" style="padding:18px;text-align:center;'
        f'color:{MUTED};font-style:italic">No critical incidents recorded for this period.</td></tr>'
    )

    gen_stamp = datetime.now(timezone.utc).astimezone().strftime("%-d %b %Y · %H:%M Athens")

    return f"""<!doctype html>
<html><head><meta charset="utf-8">
<title>AFTS · Monthly Briefing · {_h(month_name)} {year}</title>
<style>
  @page {{ size: A4; margin: 14mm 12mm 12mm 12mm; }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 0;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    color: {INK}; background: {BG};
    font-size: 10.5px; line-height: 1.45;
  }}
  .header-bar {{
    background: {BLACK}; color: #fff;
    padding: 10px 16px;
    margin-bottom: 0;
  }}
  .header-eyebrow {{
    font-family: monospace;
    font-size: 8.5px;
    color: #94a3b8;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    margin-bottom: 3px;
  }}
  .header-title {{
    font-family: Georgia, serif;
    font-weight: 800;
    font-size: 18px;
    letter-spacing: -0.01em;
  }}
  .header-month {{
    font-family: monospace;
    font-size: 10px;
    color: {ORANGE};
    letter-spacing: 0.16em;
    text-transform: uppercase;
    margin-top: 2px;
  }}
  .subhead {{
    background: {S1};
    border-left: 3px solid {ORANGE};
    padding: 10px 14px;
    margin: 0;
    font-size: 10px;
    line-height: 1.55;
    color: {INK};
  }}
  .subhead strong {{ color: {BLACK}; font-weight: 800; }}
  .stats-row {{
    display: flex;
    gap: 10px;
    padding: 14px 0 16px 0;
    margin: 0;
  }}
  .stat-tile {{
    flex: 1;
    border: 1px solid {BRD};
    background: {S1};
    padding: 12px 8px;
    text-align: center;
  }}
  .stat-tile .num {{
    font-family: Georgia, serif;
    font-weight: 800;
    font-size: 28px;
    line-height: 1;
    color: {BLACK};
  }}
  .stat-tile.tier1 .num {{ color: {RED}; }}
  .stat-tile.outbreak .num {{ color: {ORANGE}; }}
  .stat-tile .lbl {{
    font-family: monospace;
    font-size: 8.5px;
    color: {MUTED};
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin-top: 5px;
    font-weight: 700;
  }}
  .section-title {{
    font-family: Georgia, serif;
    font-weight: 800;
    font-size: 13px;
    color: {BLACK};
    margin: 6px 0 8px 0;
    padding-bottom: 5px;
    border-bottom: 2px solid {BLACK};
    letter-spacing: 0.02em;
    text-transform: uppercase;
  }}
  table.top10 {{
    width: 100%;
    border-collapse: collapse;
    border-top: 1px solid {BRD};
  }}
  table.top10 thead th {{
    font-family: monospace;
    font-size: 8.5px;
    color: {MUTED};
    letter-spacing: 0.08em;
    text-transform: uppercase;
    text-align: left;
    padding: 6px 6px;
    border-bottom: 1px solid {BRD};
    font-weight: 700;
  }}
  table.top10 thead th.center {{ text-align: center; }}
  table.top10 thead th.right  {{ text-align: right; }}
  .footer {{
    margin-top: 16px;
    padding-top: 10px;
    border-top: 1px solid {BRD};
    font-size: 9px;
    color: {MUTED};
    line-height: 1.55;
    text-align: center;
  }}
  .footer .brand {{
    font-family: Georgia, serif;
    font-weight: 800;
    color: {BLACK};
    font-size: 9.5px;
    letter-spacing: 0.02em;
    margin-bottom: 3px;
  }}
  .footer .tagline {{ color: {INK}; }}
  .footer .contact {{ font-family: monospace; letter-spacing: 0.06em; }}
</style>
</head>
<body>

<div class="header-bar">
  <div class="header-eyebrow">Advanced Food-Tech Solutions · AFTS</div>
  <div class="header-title">Food Safety Intelligence System</div>
  <div class="header-month">{_h(month_name).upper()} {year}</div>
</div>

<div class="subhead">
  <strong>Monthly Pathogen Surveillance · {_h(month_name)} {year}</strong>
   · {date_strip} · <strong>{fmt_int(total)}</strong> recalls
   · <strong>{fmt_int(tier1)}</strong> Tier-1
   · <strong>{fmt_int(outbreaks)}</strong> outbreaks
   · <strong>Leading: {_h(lead_name)}</strong>
</div>

<div class="stats-row">
  <div class="stat-tile total">
    <div class="num">{fmt_int(total)}</div>
    <div class="lbl">Total Recalls</div>
  </div>
  <div class="stat-tile tier1">
    <div class="num">{fmt_int(tier1)}</div>
    <div class="lbl">Tier-1 Critical</div>
  </div>
  <div class="stat-tile outbreak">
    <div class="num">{fmt_int(outbreaks)}</div>
    <div class="lbl">Outbreaks</div>
  </div>
</div>

<div class="section-title">{_h(section_title)}</div>

<table class="top10">
  <thead>
    <tr>
      <th class="center" style="width:4%">#</th>
      <th style="width:11%">Date</th>
      <th style="width:23%">Pathogen</th>
      <th style="width:22%">Company / Country</th>
      <th style="width:30%">Product</th>
      <th class="right" style="width:10%">Source</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
</table>

<div class="footer">
  <div class="brand">ADVANCED FOOD-TECH SOLUTIONS · AFTS</div>
  <div class="tagline">
    Food Process Engineering · Thermal Processing · Regulatory Compliance
  </div>
  <div style="margin-top:4px">Generated {gen_stamp}</div>
  <div class="contact" style="margin-top:3px">
    advfood.tech · info@advfood.tech · Athens, Greece
  </div>
</div>

</body></html>
"""


# ──────────────────────────────────────────────────────────────────────
# PDF render
# ──────────────────────────────────────────────────────────────────────
def render_pdf(html: str, out_path: Path) -> None:
    """Render the HTML one-pager to PDF using WeasyPrint."""
    from weasyprint import HTML
    out_path.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=html).write_pdf(str(out_path))
    log.info("Wrote marketing PDF: %s (%d bytes)",
             out_path, out_path.stat().st_size)


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────
def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--summary", required=True, type=Path,
                   help="Path to monthly-summary-latest.json")
    p.add_argument("--out-dir", required=True, type=Path,
                   help="Output directory (e.g. docs/marketing)")
    p.add_argument("--out-name", default=None,
                   help="Output filename (default: <month_tag>.pdf)")
    args = p.parse_args()

    summary = load_summary(args.summary)
    month_tag = (
        summary.get("month_tag")
        or summary.get("month")
        or (summary.get("month_end", "")[:7].replace("-", "-M") if summary.get("month_end") else None)
    )
    if not month_tag:
        log.error("Could not derive month_tag from summary")
        return 1

    out_name = args.out_name or f"{month_tag}.pdf"
    out_path = args.out_dir / out_name

    html = build_html(summary)

    # Write HTML alongside (useful for QA before PDF render)
    html_path = out_path.with_suffix(".html")
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(html, encoding="utf-8")
    log.info("Wrote marketing HTML: %s (%d bytes)",
             html_path, html_path.stat().st_size)

    render_pdf(html, out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
