"""
30-day scraper health email report.

Audit 2026-04-29 — George requested a monthly health email. This
module produces:
  • A single HTML report summarising scraper-health.json over the past
    30 days (latest snapshot + breakdown of what's failing)
  • An accompanying plain-text version for email clients that don't render HTML
  • Optionally, sends both via Gmail Apps Script (or saves to docs/data/
    so the existing afts_monthly_mailer.gs Apps Script can pick it up)

The report shows:
  • Headline: total scrapers / OK / failing percentages
  • Per-failure-type table: which scrapers are broken and how (404 / 403 /
    DNS / SSL / timeout)
  • Top RECOMMENDATIONS based on failure type (e.g. "Fix 4 EU scrapers
    with HTTP 404 — regulator URL drift")
  • An ACTION-LIST tab mapping each broken scraper → likely cause +
    one-line fix to investigate

Cadence:
  Monthly (1st of each month, 09:00 Athens) via .github/workflows/
  scraper-health-monthly.yml. Runs the audit then emails the report.

Output paths:
  docs/data/scraper-health-monthly-{YYYY-MM}.html
  docs/data/scraper-health-monthly-{YYYY-MM}.txt
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parent.parent
HEALTH_PATH = ROOT / "docs" / "data" / "scraper-health.json"
OUT_DIR = ROOT / "docs" / "data"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("scraper-health-monthly")

# Failure-type → recommendation
ACTION_BY_STATE = {
    "FAIL_404": "Regulator URL drift — open the agency website, find the new "
                "recalls/alerts page URL, update the scraper's URL constant.",
    "FAIL_403": "Regulator is blocking us — add a normal browser User-Agent "
                "header in scrapers/_base.py, or rotate IP via proxy.",
    "FAIL_DNS": "Site DNS unreachable from GitHub runner — verify the agency "
                "site is up via curl from another network. Often transient; "
                "if persistent, the agency may have decommissioned the site.",
    "FAIL_SSL": "TLS handshake failure (DH_KEY_TOO_SMALL or expired cert) — "
                "the regulator runs an outdated server. Add a custom SSL "
                "context with weak-DH support in scrapers/_base.py, or "
                "access the site via a proxy.",
    "FAIL_TIMEOUT": "Connection or read timeout — increase scraper timeout, "
                    "or the agency rate-limited us. Try a slower polling "
                    "schedule.",
    "FAIL_OTHER": "Unclassified failure — check the scraper's recent log "
                  "lines for the underlying error (could be JSON decode, "
                  "missing dataset, redirect loop, etc.).",
}

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<title>FSIS Scraper Health — {month_label}</title>
<style>
body{{font-family:'Inter',-apple-system,system-ui,sans-serif;
     font-size:14px;line-height:1.55;color:#1a1a1a;
     max-width:760px;margin:0 auto;padding:24px}}
h1{{color:#0a3d2e;font-size:22px;margin:0 0 6px;letter-spacing:-0.01em}}
h2{{color:#0a3d2e;font-size:16px;margin:24px 0 8px;
   border-bottom:1px solid #ddd;padding-bottom:4px}}
.lead{{font-size:13px;color:#555;margin:0 0 18px}}
.kpi{{display:flex;gap:14px;margin:12px 0 16px;flex-wrap:wrap}}
.kpi .b{{flex:1 1 130px;border:1px solid #d4d4d4;border-radius:6px;padding:10px 12px}}
.kpi .b .n{{font-size:24px;font-weight:700;color:#0a3d2e;line-height:1.1}}
.kpi .b .l{{font-size:11px;color:#666;text-transform:uppercase;
            letter-spacing:0.05em;margin-top:2px}}
.kpi .b.ok .n{{color:#0a7d3e}}
.kpi .b.warn .n{{color:#c45a08}}
.kpi .b.bad .n{{color:#a51c2c}}
table{{border-collapse:collapse;width:100%;margin:8px 0;font-size:12.5px}}
th{{background:#f4f7f5;text-align:left;padding:6px 9px;
   border:1px solid #d4d4d4;font-weight:700;color:#0a3d2e}}
td{{padding:5px 9px;border:1px solid #e0e0e0;vertical-align:top}}
.tag{{display:inline-block;padding:1px 7px;border-radius:3px;
     font-size:10.5px;font-weight:600;letter-spacing:0.03em}}
.tag.f404{{background:#fee2e2;color:#a51c2c}}
.tag.f403{{background:#fed7aa;color:#7c2d12}}
.tag.fdns{{background:#fef3c7;color:#92400e}}
.tag.fssl{{background:#fce7f3;color:#9d174d}}
.tag.ftime{{background:#e0e7ff;color:#3730a3}}
.tag.fother{{background:#e5e7eb;color:#374151}}
.action{{background:#f4f7f5;border-left:3px solid #0a3d2e;
        padding:8px 12px;margin:6px 0;font-size:12.5px}}
.foot{{margin-top:32px;padding-top:14px;border-top:1px solid #ddd;
      font-size:11px;color:#888}}
</style>
</head><body>

<h1>FSIS Scraper Health Report</h1>
<p class="lead">{month_label} · 30-day rollup of regulator scraper status</p>

<div class="kpi">
  <div class="b ok"><div class="n">{ok}</div><div class="l">scrapers OK</div></div>
  <div class="b warn"><div class="n">{ok_empty}</div><div class="l">OK but empty window</div></div>
  <div class="b bad"><div class="n">{failed}</div><div class="l">failing</div></div>
  <div class="b"><div class="n">{fail_pct}%</div><div class="l">fail rate</div></div>
</div>

<h2>Failure breakdown</h2>
{breakdown_table}

<h2>Failing scrapers — action list</h2>
{action_table}

{recommendation_section}

<div class="foot">
Generated {generated} · Source: docs/data/scraper-health.json<br>
AFTS Food Safety Intelligence System · advfood.tech
</div>
</body></html>
"""


def _state_tag_class(state: str) -> str:
    return {
        "FAIL_404": "f404", "FAIL_403": "f403",
        "FAIL_DNS": "fdns", "FAIL_SSL": "fssl",
        "FAIL_TIMEOUT": "ftime", "FAIL_OTHER": "fother",
    }.get(state, "fother")


def build_html(report: Dict) -> str:
    totals = report["totals"]
    by_state = report["by_state"]
    per = report["per_scraper"]

    month = datetime.now(timezone.utc).strftime("%B %Y")

    # Breakdown table
    breakdown_rows = []
    for state in ["FAIL_404", "FAIL_403", "FAIL_DNS", "FAIL_SSL",
                  "FAIL_TIMEOUT", "FAIL_OTHER"]:
        count = by_state.get(state, 0)
        if count == 0:
            continue
        pretty = state.replace("FAIL_", "").replace("_", " ").title()
        breakdown_rows.append(
            f'<tr><td><span class="tag {_state_tag_class(state)}">'
            f'{pretty}</span></td><td>{count}</td>'
            f'<td>{ACTION_BY_STATE.get(state, "")}</td></tr>'
        )
    breakdown_table = (
        '<table><tr><th>Type</th><th>Count</th><th>What it means</th></tr>'
        + "".join(breakdown_rows) + '</table>'
        if breakdown_rows else
        "<p>No failures detected. All scrapers are returning data.</p>"
    )

    # Action list
    action_rows = []
    failing = sorted([(name, st) for name, st in per.items()
                      if st.startswith("FAIL_")],
                     key=lambda kv: (kv[1], kv[0]))
    for name, st in failing:
        action_rows.append(
            f'<tr><td>{name}</td>'
            f'<td><span class="tag {_state_tag_class(st)}">'
            f'{st.replace("FAIL_", "")}</span></td></tr>'
        )
    action_table = (
        '<table><tr><th>Scraper</th><th>State</th></tr>'
        + "".join(action_rows) + '</table>'
        if action_rows else
        "<p>No failing scrapers.</p>"
    )

    # Recommendation
    rec_lines = []
    fail_404 = by_state.get("FAIL_404", 0)
    fail_403 = by_state.get("FAIL_403", 0)
    if fail_404 >= 5:
        rec_lines.append(
            f'<div class="action"><strong>Priority 1:</strong> '
            f'{fail_404} scrapers failing with HTTP 404 — regulator URL '
            f'drift. Each is a one-line fix in the scraper file. Estimated '
            f'~{fail_404*15} minutes total.</div>'
        )
    if fail_403 >= 2:
        rec_lines.append(
            f'<div class="action"><strong>Priority 2:</strong> '
            f'{fail_403} scrapers blocked (HTTP 403) — add a browser '
            f'User-Agent header in scrapers/_base.py. One change covers '
            f'all of them.</div>'
        )
    if not rec_lines:
        rec_lines.append(
            '<div class="action"><strong>System healthy</strong> — '
            'no major action items this month.</div>'
        )

    recommendation_section = (
        "<h2>Recommendations</h2>" + "".join(rec_lines)
    )

    return HTML_TEMPLATE.format(
        month_label=month,
        ok=totals.get("ok_returning_data", 0),
        ok_empty=totals.get("ok_empty_window", 0),
        failed=totals.get("failed", 0),
        fail_pct=totals.get("fail_pct", 0),
        breakdown_table=breakdown_table,
        action_table=action_table,
        recommendation_section=recommendation_section,
        generated=report.get("generated_utc", ""),
    )


def build_text(report: Dict) -> str:
    totals = report["totals"]
    by_state = report["by_state"]
    per = report["per_scraper"]
    lines = []
    lines.append("FSIS SCRAPER HEALTH REPORT")
    lines.append(datetime.now(timezone.utc).strftime("%B %Y"))
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"Total scrapers:    {totals.get('scrapers',0)}")
    lines.append(f"  OK (data):       {totals.get('ok_returning_data',0)}")
    lines.append(f"  OK (empty):      {totals.get('ok_empty_window',0)}")
    lines.append(f"  Failing:         {totals.get('failed',0)} "
                 f"({totals.get('fail_pct',0)}%)")
    lines.append("")
    lines.append("FAILURES BY TYPE")
    lines.append("-" * 60)
    for st in ["FAIL_404", "FAIL_403", "FAIL_DNS", "FAIL_SSL",
               "FAIL_TIMEOUT", "FAIL_OTHER"]:
        c = by_state.get(st, 0)
        if c:
            lines.append(f"  {st.replace('FAIL_',''):10}  {c:3}  "
                         f"-- {ACTION_BY_STATE.get(st, '')}")
    lines.append("")
    lines.append("ACTION LIST (failing scrapers)")
    lines.append("-" * 60)
    failing = sorted([(n, s) for n, s in per.items() if s.startswith("FAIL_")],
                     key=lambda kv: (kv[1], kv[0]))
    for name, st in failing:
        lines.append(f"  [{st.replace('FAIL_',''):8}] {name}")
    lines.append("")
    lines.append(f"Generated {report.get('generated_utc','')}")
    lines.append("AFTS · advfood.tech · info@advfood.tech")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--health-json", type=Path, default=HEALTH_PATH,
                    help="Source health JSON")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR,
                    help="Output directory for the HTML+TXT report")
    args = ap.parse_args()

    if not args.health_json.exists():
        log.error("Health JSON not found: %s", args.health_json)
        log.error("Run pipeline.scraper_health_audit first.")
        return 1

    try:
        report = json.loads(args.health_json.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        log.error("Health JSON malformed: %s", e); return 1

    month = datetime.now(timezone.utc).strftime("%Y-%m")
    out_html = args.out_dir / f"scraper-health-monthly-{month}.html"
    out_txt = args.out_dir / f"scraper-health-monthly-{month}.txt"

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_html.write_text(build_html(report), encoding="utf-8")
    out_txt.write_text(build_text(report), encoding="utf-8")
    log.info("Wrote %s and %s", out_html, out_txt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
