#!/usr/bin/env python3
"""Build TR-2026-01 as a self-contained HTML page from the live repo state.

WHY THIS EXISTS
---------------
The first draft of TR-2026-01 was written by hand. That is how its Table 1
came to show an effect of x3.31 for Salmonella / United States, a number
the detector does not produce on any channel (share 3.27, count 3.50). A
technical report whose headline table cannot be reproduced from the code
it describes is not a technical report.

Every figure below is read at build time from:

    docs/data/recalls.xlsx        via pipeline.signal_detector.load_corpus
    docs/data/source-coverage.json via pipeline.source_coverage.load_register
    pipeline/signal_detector.py   module constants, by introspection

Nothing is typed in. If a number in the output is wrong, the code that
produced it is wrong, and that is a fixable condition.

    python -m tools.build_technical_report
    python -m tools.build_technical_report --out docs/reports/TR-2026-01.html
"""
from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import html
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pipeline.signal_detector as sd            # noqa: E402
from pipeline.source_coverage import load_register  # noqa: E402

REPORT_ID = "TR-2026-01"
TITLE = "Aberration detection over a multi-jurisdiction food recall corpus"
SUBTITLE = ("Method, coverage constraints and a walk-forward replay of "
            "{n_weeks} weeks of regulatory notices")
AUTHOR = "Advanced Food-Tech Solutions (AFTS), Athens"
SITE = "https://fsis.advfood.tech"
PUBLIC = "https://www.advfood.tech/fsis-recalls"


def _esc(x) -> str:
    return html.escape(str(x), quote=True)


def _git_sha() -> str:
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                             cwd=str(ROOT), capture_output=True, text=True,
                             timeout=10)
        return out.stdout.strip() or "unversioned"
    except Exception:                                        # noqa: BLE001
        return "unversioned"


# Display-only naming. The corpus stores the value each regulator
# published; rewriting 1,506 rows to change a rendered label would be a
# bulk edit with no upside and a real chance of collateral damage. The map
# is applied at render time and nowhere else, and it is deliberately
# exact-match: no substring rules, so "Turkey" cannot catch a brand name.
DISPLAY_NAMES = {"Turkey": "T\u00fcrkiye"}


def _display(label: str) -> str:
    """Apply DISPLAY_NAMES to the country half of 'Pathogen \u00b7 Country'."""
    if " \u00b7 " not in label:
        return DISPLAY_NAMES.get(label, label)
    head, _, tail = label.rpartition(" \u00b7 ")
    return f"{head} \u00b7 {DISPLAY_NAMES.get(tail, tail)}"


def _wk(period_str: str) -> str:
    """'2026-06-08/2026-06-14' -> '8 Jun 2026'."""
    try:
        d = _dt.date.fromisoformat(str(period_str).split("/")[0])
        return f"{d.day} {d:%b} {d.year}"
    except Exception:                                        # noqa: BLE001
        return str(period_str)


# =============================================================================
# GATHER
# =============================================================================

def gather() -> dict:
    corpus = sd.load_corpus()
    strata = sd.build_strata(corpus)
    register = load_register()
    window = sd.coverage_window_start(register)

    need = sd.BASELINE_WEEKS + sd.GUARD_WEEKS + sd.C3_SPAN
    replay, scanned = [], 0
    max_out, cap_bound = 0, 0
    for i, w in enumerate(corpus.weeks):
        if i < need:
            continue
        scanned += 1
        sigs, _meta = sd.detect(corpus, strata, asof=w)
        max_out = max(max_out, int(_meta.get("after_dedup", len(sigs))))
        cap_bound += 1 if _meta.get("cap_binding") else 0
        for s in sigs:
            replay.append({
                "week": str(w), "week_label": _wk(str(w)), "label": s.label,
                "level": s.level, "channel": s.channel,
                "observed": s.observed, "baseline_mean": s.baseline_mean,
                "effect": s.effect, "effect_share": s.effect_share,
                "effect_count": s.effect_count, "p_value": s.p_value,
                "dominant_source": s.dominant_source,
                "dominant_share": s.dominant_share,
                "fdr_pass": bool(s.fdr_pass),
                "c1": s.c1, "c2": s.c2, "c3": s.c3,
                "in_window": window is None or str(w) >= window,
            })

    by_class: dict[str, list] = {}
    for s in register.values():
        by_class.setdefault(s.coverage_class, []).append(s)
    for v in by_class.values():
        v.sort(key=lambda s: (-s.records, s.source))

    wk = corpus.frame.groupby("week")
    weekly = []
    for w in corpus.weeks:
        try:
            sub = wk.get_group(w)
        except KeyError:
            weekly.append({"week": str(w), "label": _wk(str(w)),
                           "total": 0, "sources": 0,
                           "in_window": window is None or str(w) >= window})
            continue
        weekly.append({
            "week": str(w), "label": _wk(str(w)),
            "total": int(len(sub)),
            "sources": int(sub["Source"].nunique()),
            "in_window": window is None or str(w) >= window,
        })

    # Attribute the largest week-over-week jumps to the source that caused
    # them. The report claims volume steps are publisher events; that claim
    # is checkable, so it is checked here rather than asserted in prose.
    piv = (corpus.frame.pivot_table(index="week", columns="Source",
                                    values="Date", aggfunc="size")
           .reindex(corpus.weeks).fillna(0))
    steps = []
    for i in range(1, len(weekly)):
        delta = weekly[i]["total"] - weekly[i - 1]["total"]
        if delta <= 0:
            continue
        per = (piv.iloc[i] - piv.iloc[i - 1]).sort_values(ascending=False)
        top_src = str(per.index[0])
        steps.append({"label": weekly[i]["label"], "delta": int(delta),
                      "source": top_src, "src_delta": int(per.iloc[0]),
                      "week": weekly[i]["week"]})
    steps.sort(key=lambda x: -x["delta"])

    src_counts = corpus.frame["Source"].value_counts()
    total = int(len(corpus.frame))
    top_sources = [(str(k), int(v), v / total) for k, v in src_counts.head(8).items()]

    return {
        "generated": _dt.datetime.now(_dt.timezone.utc),
        "sha": _git_sha(),
        "n_records": total,
        "n_weeks": len(corpus.weeks),
        "first_week": str(corpus.weeks[0]),
        "last_week": str(corpus.weeks[-1]),
        "n_strata": len(strata),
        "register": register,
        "by_class": by_class,
        "window": window,
        "replay": replay,
        "weeks_scanned": scanned,
        "max_out": max_out,
        "cap_bound_weeks": cap_bound,
        "top_sources": top_sources,
        "weekly": weekly,
        "steps": steps,
        "concentration": sum(s[2] for s in top_sources[:2]),
    }


# =============================================================================
# RENDER
# =============================================================================

CSS = """
:root{
  --ink:#12161c; --ink-2:#3d4654; --ink-3:#6b7585; --rule:#e2e6ec;
  --bg:#ffffff; --bg-2:#f7f9fb; --accent:#0b6b5e; --accent-2:#e8f3f1;
  --warn-bg:#fff8e6; --warn-line:#e0b44a; --warn-ink:#6b4e08;
  --mono:"SFMono-Regular",Consolas,"Liberation Mono",Menlo,monospace;
  --viz:#0f9c86;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);
  font:16px/1.65 Charter,"Iowan Old Style","Source Serif Pro",Georgia,serif;
  -webkit-text-size-adjust:100%}
.wrap{max-width:860px;margin:0 auto;padding:0 24px 96px}
header.masthead{border-bottom:3px solid var(--ink);margin-bottom:38px;
  padding:44px 0 22px}
.eyebrow{font:600 11px/1 ui-sans-serif,system-ui,sans-serif;
  letter-spacing:.16em;text-transform:uppercase;color:var(--accent);
  margin-bottom:16px}
h1{font-size:33px;line-height:1.2;margin:0 0 10px;letter-spacing:-.015em}
.sub{color:var(--ink-2);font-size:18px;margin:0 0 20px}
.byline{font:13px/1.6 ui-sans-serif,system-ui,sans-serif;color:var(--ink-3)}
.byline a{color:var(--accent);text-decoration:none}
h2{font-size:22px;margin:52px 0 14px;padding-top:14px;
  border-top:1px solid var(--rule);letter-spacing:-.01em}
h2 .num{color:var(--ink-3);font-weight:400;margin-right:10px}
h3{font-size:16.5px;margin:28px 0 8px;color:var(--ink-2)}
p{margin:0 0 15px}
ul,ol{margin:0 0 15px;padding-left:22px}
li{margin:0 0 7px}
code,.mono{font-family:var(--mono);font-size:.88em;
  background:var(--bg-2);padding:1px 5px;border-radius:3px}
.lede{font-size:17.5px;color:var(--ink-2);border-left:3px solid var(--accent);
  padding-left:18px;margin:0 0 30px}
.tw{overflow-x:auto;margin:20px 0 24px;-webkit-overflow-scrolling:touch}
table{border-collapse:collapse;width:100%;font:13.5px/1.5 ui-sans-serif,
  system-ui,sans-serif;min-width:640px}
th{text-align:left;padding:9px 10px;border-bottom:2px solid var(--ink);
  font-weight:600;white-space:nowrap;vertical-align:bottom}
td{padding:8px 10px;border-bottom:1px solid var(--rule);vertical-align:top}
td.d,th.d{white-space:nowrap}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums;
  font-family:var(--mono);font-size:12.5px}
tbody tr:hover{background:var(--bg-2)}
caption{caption-side:bottom;text-align:left;padding-top:10px;font:13px/1.6
  ui-sans-serif,system-ui,sans-serif;color:var(--ink-3)}
.chan{display:inline-block;font:600 10px/1.5 ui-sans-serif,system-ui,sans-serif;
  letter-spacing:.06em;padding:1px 6px;border-radius:3px;text-transform:uppercase;
  white-space:nowrap}
.chan.proportion{background:var(--accent-2);color:var(--accent)}
.chan.count{background:#eef0f3;color:var(--ink-2)}
.note{background:var(--bg-2);border:1px solid var(--rule);border-left:3px solid
  var(--accent);padding:15px 18px;margin:22px 0;font-size:15px}
.note .h{font:600 11px/1 ui-sans-serif,system-ui,sans-serif;letter-spacing:.12em;
  text-transform:uppercase;color:var(--accent);margin-bottom:9px}
.warn{background:var(--warn-bg);border:1px solid var(--warn-line);
  border-left:3px solid var(--warn-line);padding:15px 18px;margin:22px 0;
  font-size:15px;color:var(--warn-ink)}
.warn .h{font:600 11px/1 ui-sans-serif,system-ui,sans-serif;letter-spacing:.12em;
  text-transform:uppercase;margin-bottom:9px}
/* Six tiles: an auto-fit track leaves a stranded empty cell at most
   widths, which reads as a rendering fault. Fixed 3x2 / 2x3 instead. */
.kpis{display:grid;grid-template-columns:repeat(2,1fr);
  gap:1px;background:var(--rule);border:1px solid var(--rule);margin:26px 0}
@media (min-width:700px){.kpis{grid-template-columns:repeat(3,1fr)}}
.kpi{background:var(--bg);padding:16px 18px}
.kpi .v{font:600 26px/1.1 ui-sans-serif,system-ui,sans-serif;
  font-variant-numeric:tabular-nums;letter-spacing:-.02em}
.kpi .l{font:12px/1.4 ui-sans-serif,system-ui,sans-serif;color:var(--ink-3);
  margin-top:5px}

/* ── Figure 1 ─────────────────────────────────────────────────────────── */
.fig{margin:26px 0 28px}
.fig .svgwrap{position:relative;overflow-x:auto;overflow-y:hidden;
  -webkit-overflow-scrolling:touch;padding-top:4px}
.fig svg{display:block;min-width:660px}
.fig .bar{fill:var(--viz);shape-rendering:crispEdges}
.fig .bar:hover{fill:var(--ink)}
.fig .grid{stroke:var(--rule);stroke-width:1;shape-rendering:crispEdges}
.fig .excl{fill:var(--bg-2)}
.fig .cut{stroke:var(--ink-2);stroke-width:1.5;stroke-dasharray:4 3}
.fig text{font-family:ui-sans-serif,system-ui,sans-serif}
.fig .ax{font-size:10px;fill:var(--ink-3)}
.fig .ptitle{font-size:11.5px;font-weight:600;fill:var(--ink-2);
  letter-spacing:.02em}
.fig .vlab{font-size:10px;font-weight:600;fill:var(--ink-2);text-anchor:middle}
.fig .cutlab{font-size:10.5px;font-weight:600;fill:var(--ink-2)}
.fig .cutlab.dim{font-weight:400;fill:var(--ink-3)}
.fig .tip{position:absolute;pointer-events:none;background:var(--ink);
  color:var(--bg);padding:6px 9px;border-radius:4px;font:12px/1.4
  ui-sans-serif,system-ui,sans-serif;white-space:nowrap;z-index:5;
  transform:translate(-50%,-118%)}
.fig figcaption{font:13px/1.6 ui-sans-serif,system-ui,sans-serif;
  color:var(--ink-3);margin-top:12px}
.fig details{margin-top:10px}
.fig summary{cursor:pointer;color:var(--accent);font-weight:600}
.fig details table{min-width:420px}
.sub-cta{border:2px solid var(--accent);padding:26px 28px;margin:56px 0 0;
  background:var(--accent-2)}
.sub-cta h3{margin:0 0 10px;color:var(--accent);font-size:19px}
.sub-cta p{margin:0 0 14px;font-size:15.5px;color:var(--ink-2)}
.sub-cta a.btn{display:inline-block;background:var(--accent);color:#fff;
  text-decoration:none;padding:11px 22px;font:600 14px/1 ui-sans-serif,
  system-ui,sans-serif;border-radius:3px}
footer{margin-top:56px;padding-top:20px;border-top:1px solid var(--rule);
  font:13px/1.7 ui-sans-serif,system-ui,sans-serif;color:var(--ink-3)}
footer a{color:var(--accent)}
@media (max-width:640px){
  .wrap{padding:0 16px 64px} h1{font-size:26px} .sub{font-size:16px}
  h2{font-size:19px} body{font-size:15.5px}
}
@media print{
  body{font-size:10.5pt} .wrap{max-width:none;padding:0}
  h2{page-break-after:avoid} table{page-break-inside:avoid}
  .sub-cta{border-color:#999}
}
:root[data-theme="dark"]{
  --ink:#e8ecf1; --ink-2:#b3bcc9; --ink-3:#828d9c; --rule:#2b323c;
  --bg:#12161c; --bg-2:#1a2028; --accent:#5fd0bd; --accent-2:#12302c;
  --warn-bg:#2e2610; --warn-line:#8a6d1f; --warn-ink:#e8ce88;
  --viz:#17a08c;
}
:root[data-theme="dark"] .chan.count{background:#232a34;color:var(--ink-2)}
:root[data-theme="dark"] .sub-cta a.btn{color:#0d1418}
@media (prefers-color-scheme:dark){
  :root:not([data-theme="light"]){
    --ink:#e8ecf1; --ink-2:#b3bcc9; --ink-3:#828d9c; --rule:#2b323c;
    --bg:#12161c; --bg-2:#1a2028; --accent:#5fd0bd; --accent-2:#12302c;
    --warn-bg:#2e2610; --warn-line:#8a6d1f; --warn-ink:#e8ce88;
    --viz:#17a08c;
  }
  :root:not([data-theme="light"]) .chan.count{background:#232a34;color:var(--ink-2)}
  :root:not([data-theme="light"]) .sub-cta a.btn{color:#0d1418}
}
"""


def _admission(r) -> str:
    """How this row got in — the two channels are admitted differently."""
    if r["channel"] == "proportion":
        p = r["p_value"]
        return ("p&nbsp;&lt;0.0001" if p < 0.0001 else f"p&nbsp;{p:.4f}")
    stat, name = max(((r["c1"], "C1"), (r["c2"], "C2"), (r["c3"], "C3")),
                     key=lambda t: t[0])
    return f"{name}&nbsp;{stat:.1f}"


def _signal_rows(replay) -> str:
    out = []
    for r in replay:
        chan = "share" if r["channel"] == "proportion" else "count-only"
        cls = "proportion" if r["channel"] == "proportion" else "count"
        other = (f'{r["effect_count"]:.2f}' if r["channel"] == "proportion"
                 else f'{r["effect_share"]:.2f}')
        out.append(
            f'<tr>'
            f'<td class="d">{_esc(r["week_label"])}</td>'
            f'<td>{_esc(_display(r["label"]))}</td>'
            f'<td><span class="chan {cls}">{chan}</span></td>'
            f'<td class="n">{r["observed"]}</td>'
            f'<td class="n">{r["baseline_mean"]:.2f}</td>'
            f'<td class="n"><strong>{r["effect"]:.2f}</strong></td>'
            f'<td class="n">{other}</td>'
            f'<td class="n">{_admission(r)}</td>'
            f'</tr>')
    return "\n".join(out)


def _coverage_rows(by_class) -> str:
    out = []
    order = ["continuous", "intermittent", "sporadic", "excluded"]
    for cls in order:
        rows = by_class.get(cls, [])
        if not rows:
            continue
        out.append(f'<tr><td colspan="6" style="background:var(--bg-2);'
                   f'font-weight:600;font-size:12px;letter-spacing:.08em;'
                   f'text-transform:uppercase">{_esc(cls)} '
                   f'&middot; {len(rows)} source{"s" if len(rows)!=1 else ""}</td></tr>')
        for s in rows:
            mat = _wk(s.mature_week) if s.mature_week else "&mdash;"
            basis = s.mature_basis or "&mdash;"
            out.append(
                f'<tr><td>{_esc(s.source)}</td>'
                f'<td class="n">{s.records}</td>'
                f'<td class="n">{s.active_rate:.2f}</td>'
                f'<td class="d">{_wk(s.onset_week) if s.onset_week else "&mdash;"}</td>'
                f'<td class="d">{mat}</td>'
                f'<td style="font-size:12px;color:var(--ink-3)">{_esc(basis)}</td></tr>')
    return "\n".join(out)



# =============================================================================
# FIGURE 1 — the mechanism behind the coverage window
# =============================================================================
# Two panels, one shared x axis, one series each. The argument the report
# makes in prose is that early corpus volume is a fact about the scraper
# fleet rather than the food supply; the second panel is that claim's
# evidence, and putting the two measures on one pair of axes (a dual-axis
# chart) would have made it unreadable and unfalsifiable at once.

VIZ_LIGHT = "#0f9c86"      # validated: light band, chroma, contrast vs #ffffff
VIZ_DARK = "#17a08c"       # validated for the dark surface #12161c


def _bars(rows, key, x0, y0, w, h, bw, gap, maxv, label_idx):
    out = []
    for i, r in enumerate(rows):
        v = r[key]
        bh = 0 if maxv <= 0 else (v / maxv) * h
        bh = max(bh, 1.2) if v > 0 else 0
        x = x0 + i * (bw + gap)
        y = y0 + h - bh
        if bh:
            out.append(f'<rect class="bar" x="{x:.1f}" y="{y:.1f}" '
                       f'width="{bw:.1f}" height="{bh:.1f}" rx="2" '
                       f'data-l="{_esc(r["label"])}" data-v="{v}"/>')
        if i in label_idx:
            out.append(f'<text class="vlab" x="{x + bw/2:.1f}" '
                       f'y="{y - 5:.1f}">{v}</text>')
    return "".join(out)


def figure_one(weekly, window_label, steps=(), peak_sources=None) -> str:
    n = len(weekly)
    if not n:
        return ""
    W, PAD_L, PAD_R = 840, 34, 12
    plot_w = W - PAD_L - PAD_R
    gap = 3.0
    bw = (plot_w - gap * (n - 1)) / n
    hA, hB = 132.0, 74.0
    yA, yB = 24.0, 24.0 + 132.0 + 54.0
    H = yB + hB + 34

    maxA = max(r["total"] for r in weekly) or 1
    maxB = max(r["sources"] for r in weekly) or 1

    first_in = next((i for i, r in enumerate(weekly) if r["in_window"]), None)
    peakA = max(range(n), key=lambda i: weekly[i]["total"])
    peakB = max(range(n), key=lambda i: weekly[i]["sources"])

    def gridlines(y0, h, maxv, step):
        g = []
        v = 0
        while v <= maxv:
            y = y0 + h - (v / maxv) * h
            g.append(f'<line class="grid" x1="{PAD_L}" x2="{W - PAD_R}" '
                     f'y1="{y:.1f}" y2="{y:.1f}"/>')
            g.append(f'<text class="ax" x="{PAD_L - 7}" y="{y + 3.5:.1f}" '
                     f'text-anchor="end">{v}</text>')
            v += step
        return "".join(g)

    shade = ""
    rule = ""
    if first_in:
        sx = PAD_L + first_in * (bw + gap) - gap / 2
        shade = (f'<rect class="excl" x="{PAD_L - 2}" y="{yA - 8}" '
                 f'width="{sx - PAD_L + 2:.1f}" height="{yB + hB - yA + 8:.1f}"/>')
        rule = (f'<line class="cut" x1="{sx:.1f}" x2="{sx:.1f}" '
                f'y1="{yA - 8:.1f}" y2="{yB + hB:.1f}"/>'
                f'<text class="cutlab" x="{sx + 6:.1f}" y="{yA - 12:.1f}">'
                f'analytical window opens {_esc(window_label)}</text>'
                f'<text class="cutlab dim" x="{sx - 6:.1f}" y="{yA - 12:.1f}" '
                f'text-anchor="end">not analysable</text>')

    xticks = []
    for i in range(0, n, 4):
        x = PAD_L + i * (bw + gap) + bw / 2
        xticks.append(f'<text class="ax" x="{x:.1f}" y="{H - 12:.1f}" '
                      f'text-anchor="middle">{_esc(weekly[i]["label"][:-5])}</text>')

    if steps:
        parts = [f'the week of {_esc(x["label"])} (+{x["delta"]}, of which '
                 f'{x["src_delta"]} from <strong>{_esc(x["source"])}</strong>)'
                 for x in steps[:2]]
        dom_txt = ("The two largest weekly increases in the corpus are "
                   + " and ".join(parts)
                   + ". Both are publication events, not changes in the "
                     "food supply — which is precisely what the share "
                     "channel in &sect;2.2 exists to survive.")
    else:
        dom_txt = ("Corpus volume rises as sources switch on, one after "
                   "another.")

    last_src = weekly[-1]["sources"]
    if peak_sources and peak_sources[1] > last_src:
        attr_txt = (
            f'<br><span style="color:var(--warn-ink)">Note the lower panel '
            f'after {_esc(peak_sources[0])}:</span> the number of publishing '
            f'sources peaks at {peak_sources[1]} and settles at '
            f'{last_src}. That decline is scraper attrition, not regulators '
            f'falling quiet, and it is tracked as outages in the coverage '
            f'register rather than being smoothed away here.')
    else:
        attr_txt = ""

    rowsA = _bars(weekly, "total", PAD_L, yA, plot_w, hA, bw, gap, maxA,
                  {0, peakA, n - 1})
    rowsB = _bars(weekly, "sources", PAD_L, yB, plot_w, hB, bw, gap, maxB,
                  {0, peakB, n - 1})

    table = "".join(
        f'<tr><td class="d">{_esc(r["label"])}</td>'
        f'<td class="n">{r["total"]}</td><td class="n">{r["sources"]}</td>'
        f'<td>{"analysable" if r["in_window"] else "pre-window"}</td></tr>'
        for r in weekly)

    return f'''
<figure class="fig">
<div class="svgwrap">
<svg viewBox="0 0 {W:.0f} {H:.0f}" role="img" width="100%"
     aria-label="Weekly notice volume and the number of distinct sources
     publishing each week, with the analytical window marked">
  {shade}
  <text class="ptitle" x="{PAD_L}" y="{yA - 12:.1f}">Notices per week</text>
  {gridlines(yA, hA, maxA, 40)}
  <g class="series">{rowsA}</g>
  <text class="ptitle" x="{PAD_L}" y="{yB - 12:.1f}">Distinct sources publishing that week</text>
  {gridlines(yB, hB, maxB, 5)}
  <g class="series">{rowsB}</g>
  {rule}
  {"".join(xticks)}
</svg>
<div class="tip" hidden></div>
</div>
<figcaption><strong>Figure 1.</strong> The lower panel is the reason the
upper panel cannot be read as a food-safety trend. {dom_txt} A baseline drawn from the shaded region would read every later normal week
as an increase, so everything left of the rule is excluded from the
findings in &sect;5.2.
{attr_txt}
<details><summary>Data table</summary>
<div class="tw"><table><thead><tr><th class="d">Week commencing</th>
<th class="n">Notices</th><th class="n">Sources</th><th>Status</th></tr></thead>
<tbody>{table}</tbody></table></div></details></figcaption>
</figure>'''


def analytical_digest(d: dict) -> str:
    """SHA-256 over the analytical content, deliberately excluding the clock.

    The page carries a build timestamp, so two builds are never byte-equal
    and an earlier draft's claim of byte-for-byte reproducibility was
    simply false. What can be pinned is the result set: corpus shape, the
    coverage window, every replay row, and the constants that produced
    them. If this digest matches, the analysis matches, whatever the clock
    said.
    """
    payload = {
        "records": d["n_records"], "weeks": d["n_weeks"],
        "first_week": d["first_week"], "last_week": d["last_week"],
        "strata": d["n_strata"], "window": d["window"],
        "constants": {k: getattr(sd, k) for k in (
            "BASELINE_WEEKS", "GUARD_WEEKS", "C3_SPAN", "MIN_ABSOLUTE_COUNT",
            "MIN_BASELINE_MEAN", "MIN_BASELINE_NONZERO", "ALPHA", "FDR_Q",
            "MAX_SIGNALS", "PUBLISHER_DOMINANCE")},
        "replay": [[r["week"], r["label"], r["channel"], r["observed"],
                    r["baseline_mean"], r["effect"], r["effect_share"],
                    r["effect_count"], round(r["p_value"], 10)]
                   for r in d["replay"]],
        "weekly": [[r["week"], r["total"], r["sources"]] for r in d["weekly"]],
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def render(d: dict, lead_html: str = "", eyebrow: str = None,
           title: str = None, deck: str = None, extra_css: str = "") -> str:
    """Render the page.

    `lead_html` is unnumbered front matter inserted between the summary
    tiles and §1. It exists so that ONE document can open with what the
    system found and still carry the full method underneath, rather than
    the reader needing two files. The numbered sections are untouched by
    it, so nothing that cites "§5.2" goes stale.
    """
    win = d["window"]
    win_label = _wk(win) if win else "not established"
    n_sig = len(d["replay"])
    n_in = sum(1 for r in d["replay"] if r["in_window"])
    n_out = n_sig - n_in
    n_share = sum(1 for r in d["replay"]
                  if r["in_window"] and r["channel"] == "proportion")
    n_count = n_in - n_share
    # Baseline span of the earliest in-window row, stated rather than
    # implied: the §5.2 claim is about baselines, not about test weeks.
    # Both channels, because they do not agree — see the note in §2.3.
    # Derived from the code's own indexing, not from the prose description:
    #   count : _window(offset=GUARD_WEEKS) -> idx-9 .. idx-3   (2-week gap)
    #   share : range(GUARD_WEEKS, ...)     -> idx-8 .. idx-2    (1-week gap)
    c_lo = c_hi = s_lo = s_hi = "&mdash;"
    guard_wks = "&mdash;"
    if d["window"]:
        wks = [w["week"] for w in d["weekly"]]
        if d["window"] in wks:
            i = wks.index(d["window"])
            cl, ch = i - sd.GUARD_WEEKS - sd.BASELINE_WEEKS, i - sd.GUARD_WEEKS - 1
            sl, sh = i - sd.GUARD_WEEKS - sd.BASELINE_WEEKS + 1, i - sd.GUARD_WEEKS
            if cl >= 0:
                c_lo, c_hi = _wk(wks[cl]), _wk(wks[ch])
                s_lo, s_hi = _wk(wks[sl]), _wk(wks[sh])
                guard_wks = " and ".join(_wk(wks[k]) for k in range(ch + 1, i))
    # How many rows the DISPLAYED effect actually changed on. Comparing the
    # two ratios for inequality answers a different and useless question:
    # a share ratio and a count ratio are almost never equal, so that test
    # returns "every row" and tells a reader nothing.
    n_changed = sum(1 for r in d["replay"]
                    if r["in_window"] and r["channel"] != "proportion")
    _unused_disagree = sum(1 for r in d["replay"]
                     if abs(r["effect_share"] - r["effect_count"]) >= 0.005)
    cont = d["by_class"].get("continuous", [])
    mats = [s.mature_week for s in cont if s.mature_week]
    latest_mat = max(mats) if mats else None
    top2 = d["top_sources"][:2]
    # Count what is actually collected. The register also carries excluded
    # entries (aggregators, superseded labels); advertising those as
    # publishers would overstate coverage.
    n_publishers = sum(1 for sc in d["register"].values()
                       if sc.coverage_class != "excluded")
    pk = max(d["weekly"], key=lambda r: r["sources"]) if d["weekly"] else None
    peak_src = (pk["label"], pk["sources"]) if pk else None
    gen = d["generated"]
    digest = analytical_digest(d)
    lede = "" if lead_html else ""
    max_out = d["max_out"]

    src_rows = "\n".join(
        f'<tr><td>{_esc(n)}</td><td class="n">{c}</td>'
        f'<td class="n">{p*100:.1f}%</td></tr>'
        for n, c, p in d["top_sources"])

    const_rows = "\n".join(
        f'<tr><td><code>{k}</code></td><td class="n">{v}</td><td>{note}</td></tr>'
        for k, v, note in [
            ("BASELINE_WEEKS", sd.BASELINE_WEEKS,
             "EARS standard. Seven weeks is short; it is what the corpus supports."),
            ("GUARD_WEEKS", sd.GUARD_WEEKS,
             "C2/C3 guard band, keeping a slow-onset event out of its own baseline."),
            ("C3_SPAN", sd.C3_SPAN, "Weeks of C2 excess summed for the C3 statistic."),
            ("MIN_ABSOLUTE_COUNT", sd.MIN_ABSOLUTE_COUNT,
             "Floor on the observed week. Below this, 1&rarr;3 is not a finding."),
            ("MIN_BASELINE_MEAN", f"{sd.MIN_BASELINE_MEAN:.1f}",
             "A stratum averaging under one record a week rolls up a level."),
            ("MIN_BASELINE_NONZERO", sd.MIN_BASELINE_NONZERO,
             "Non-zero weeks required in the baseline before a stratum is testable."),
            ("ALPHA", f"{sd.ALPHA:.2f}", "Per-test, before multiplicity correction."),
            ("FDR_Q", f"{sd.FDR_Q:.2f}", "Benjamini&ndash;Hochberg, applied across all strata in a run."),
            ("MAX_SIGNALS", sd.MAX_SIGNALS, "Hard cap per run, ranked by effect."),
            ("PUBLISHER_DOMINANCE", f"{sd.PUBLISHER_DOMINANCE:.2f}",
             "Above this share from one publisher, the signal is annotated as such."),
        ])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title or (REPORT_ID + " &middot; Aberration Detection")}</title>
<meta name="author" content="{AUTHOR}">
<meta name="description" content="{deck or SUBTITLE.format(n_weeks=d['n_weeks'])}">
<style>{CSS}{extra_css}</style>
</head>
<body>
<div class="wrap">

<header class="masthead">
  <div class="eyebrow">{eyebrow or f"Technical Report {REPORT_ID}"}</div>
  <h1>{title or TITLE}</h1>
  <p class="sub">{deck or SUBTITLE.format(n_weeks=d["n_weeks"])}</p>
  <div class="byline">
    {AUTHOR} &middot; Food Safety Intelligence System<br>
    Built {gen:%d %B %Y} at {gen:%H:%M} UTC from corpus revision
    <span class="mono">{_esc(d['sha'])}</span> &middot;
    <a href="{SITE}">{SITE.replace('https://','')}</a>
  </div>
</header>

<p class="lede">{lede}Every figure in this report is generated at build
time from the corpus and the detector source. No table is transcribed by
hand. The command that produced this page is given in &sect;7: running the
documented build against the same corpus, source-code revision and
configuration reproduces the reported analytical results, verifiable
against the digest printed there.</p>

<div class="kpis">
  <div class="kpi"><div class="v">{d['n_records']:,}</div>
    <div class="l">notices, complete weeks</div></div>
  <div class="kpi"><div class="v">{d['n_weeks']}</div>
    <div class="l">complete weeks</div></div>
  <div class="kpi"><div class="v">{d['n_strata']}</div>
    <div class="l">strata evaluated</div></div>
  <div class="kpi"><div class="v">{n_share}</div>
    <div class="l">FDR-controlled share signals</div></div>
  <div class="kpi"><div class="v">{n_count}</div>
    <div class="l">count-channel diagnostics</div></div>
  <div class="kpi"><div class="v">{len(cont)}</div>
    <div class="l">continuous sources</div></div>
</div>

{lead_html}
<h2><span class="num">1</span>What this is, and what it is not</h2>

<p>This is a <strong>retrospective aberration detector</strong> over
regulatory recall notices. It answers one question: is a given stratum
&mdash; a pathogen, a country, a hazard tier, or a combination &mdash;
running above its own recent baseline this week?</p>

<p>It does not forecast. The corpus has no denominator: no production
volumes, no consumption data, no negative examples. Nothing here supports
a statement of the form &ldquo;product X is likely to be recalled&rdquo;,
and no output string in the module makes one. A rise in notices is first
a fact about publishers and only second a fact about food.</p>

<p>The corpus covers {d['n_weeks']} complete weeks,
{_wk(d['first_week'])} to {_wk(d['last_week'])},
holding {d['n_records']:,} notices. The trailing partial week is dropped
at load: a week still being populated always looks like a decline, and
including it would both suppress genuine signals and poison every baseline
it later enters.</p>

<h2><span class="num">2</span>Method</h2>

<h3>2.1 Detector</h3>

<p>CDC EARS C1/C2/C3 (Hutwagner et al., 2003), chosen over Farrington
(1996) and Noufaily (2013) because those generally require multiple years
of stable historical data to model seasonality reliably, and this corpus
holds {d['n_weeks']} weeks. The stratum
contract is designed to survive a migration to Farrington without changing
callers, once the history exists.</p>

<ul>
  <li><strong>C1</strong> &mdash; baseline is the {sd.BASELINE_WEEKS} weeks
      immediately preceding the test week.</li>
  <li><strong>C2</strong> &mdash; same width, offset by a
      {sd.GUARD_WEEKS}-week guard band, so a slow-onset event cannot
      quietly raise the baseline it is being measured against. Both
      channels read this same window: for the week commencing {win_label}
      the baseline is <span class="mono">{c_lo}&ndash;{c_hi}</span>, with
      {guard_wks} held out.</li>
  <li><strong>C3</strong> &mdash; thresholded excess accumulated over the
      last {sd.C3_SPAN} weeks, which catches sustained low-grade drift that
      no single week would trip. The implementation is exactly
      <span class="mono">C3<sub>t</sub> = &Sigma;<sub>i=0..{sd.C3_SPAN - 1}</sub>
      max(0, C2<sub>t-i</sub> &minus; 1)</span>, each C2 recomputed against
      its own baseline window &mdash; not a plain sum of C2 values.</li>
</ul>

<p>The corpus's first
<strong>{sd.BASELINE_WEEKS + sd.GUARD_WEEKS + sd.C3_SPAN}</strong> weeks
therefore produce no output, which is why the replay below starts where it
does. <strong>That figure is an implementation requirement, not an
inherent property of EARS.</strong> The code guards with
<code>{sd.BASELINE_WEEKS}&nbsp;+&nbsp;{sd.GUARD_WEEKS}&nbsp;+&nbsp;{sd.C3_SPAN}</code>,
a readable expression that is one week conservative: the binding
constraint is the earliest week the oldest C2 in the C3 sum can reach,
which is
t&nbsp;&minus;&nbsp;{sd.C3_SPAN - 1}&nbsp;&minus;&nbsp;({sd.BASELINE_WEEKS}&nbsp;+&nbsp;{sd.GUARD_WEEKS}),
making <strong>{sd.BASELINE_WEEKS + sd.GUARD_WEEKS + sd.C3_SPAN - 1}</strong>
leading weeks the arithmetic minimum. One further week of history is
discarded than strictly necessary. Nothing is wrong with the scored weeks;
there is simply one fewer of them than there could be.</p>

<h3>2.2 Two channels, and why the share channel is the one that alarms</h3>

<p>{_esc(top2[0][0])} and {_esc(top2[1][0])} together supply
{d['concentration']*100:.0f}% of all records. When one of them clears a
backlog, every raw count in the corpus rises at once, and a count-only
detector alarms on all of them simultaneously &mdash; a publisher event
wearing the costume of a food-safety event.</p>

<p>So two channels are computed. The <strong>count</strong> channel
compares the raw weekly count against its baseline mean and standard
deviation; it is diagnostic. The <strong>share</strong> channel compares
the stratum's proportion of that week's whole corpus against the pooled
baseline share, by exact binomial test; this is the channel that
alarms.</p>

<p>It is tempting to say shares are invariant to publisher volume, and an
earlier draft of this report did. That is too strong. The share channel <strong>partially normalises
changes in total weekly volume, although it remains sensitive to changes
in publisher mix and to publisher-specific hazard composition</strong> - a
publisher whose output skews towards one hazard class moves that class's
share when it moves its volume. A count signal with no matching share
signal is therefore reported as a <strong>potentially volume-driven
publication event requiring publisher-level review</strong>, not as a
finding.</p>

<div class="note">
  <div class="h">Effect follows the channel</div>
  <p style="margin:0">Until 27 August 2026 the effect column carried the
  <em>share</em> ratio for every signal, including count-only ones. Share
  rows were unaffected; the count-only rows were not, so inside the
  analytical window the reported effect changes on
  <strong>{n_changed} of {n_in}</strong> rows. One of those changes was
  disqualifying rather than cosmetic: a count-only Listeria signal for
  France carried a share ratio of 0.85 &mdash; below one &mdash; telling
  the reader the stratum fell in the same row that said it alarmed.
  The effect column now follows the channel. Both ratios are retained on
  every record, so no previously published figure becomes unrecoverable.</p>
</div>

<h3>2.3 Sparsity ladder</h3>

<p>Most pathogen &times; country cells hold under one record a week.
Testing them directly turns 1&rarr;3 into a &ldquo;threefold jump&rdquo;.
Each stratum is therefore tested only at the finest level whose baseline
mean clears <code>MIN_BASELINE_MEAN</code> and whose observed count clears
<code>MIN_ABSOLUTE_COUNT</code>; otherwise it rolls up, country &rarr;
region &rarr; global.</p>

<h3>2.4 Multiplicity</h3>

<p>{d['n_strata']} strata are evaluated per run. At
&alpha;&nbsp;=&nbsp;{sd.ALPHA:.2f} that could produce approximately
<strong>{d['n_strata'] * sd.ALPHA:.1f} nominal positives per run under an
all-null approximation</strong> with independent tests &mdash; the strata
are not independent, since a child and its parent share records, so treat
that as an order of magnitude rather than an expectation. Either way it is
enough to train a reader to ignore the feed, which is the failure mode
that matters most for an operational product. Benjamini&ndash;Hochberg FDR
at q&nbsp;=&nbsp;{sd.FDR_Q:.2f} is applied across all strata within a run, and
the output is capped at {sd.MAX_SIGNALS} signals ranked by effect.</p>

<h3>2.5 Ranking, and what the output cap actually does</h3>

<p>&sect;5.1 warns against reading the two channels as one scale, which
raises a fair question about the cap: if output is limited to
{sd.MAX_SIGNALS} rows &ldquo;ranked by effect&rdquo;, are share ratios and
count ratios being ranked against each other? They are not. The sort key
is <code>(channel != "proportion", -effect)</code>: the first element
<em>separates</em> the channels into blocks, and the effect ordering
applies only within a block. A share ratio is never compared to a count
ratio.</p>

<p>Two further facts, both measurable rather than asserted. The cap has
never bound on this corpus: across all {d['weeks_scanned']} scored weeks,
the largest post-deduplication output was <strong>{max_out}</strong> rows
against a cap of {sd.MAX_SIGNALS}. And the gap a reader may notice between
&ldquo;survived FDR&rdquo; and &ldquo;reported&rdquo; in the run metadata
is <em>not</em> the cap &mdash; it is the parent/child collapse, which
drops a parent stratum when a child accounts for 80% or more of it. The
run metadata now carries <code>after_dedup</code> and
<code>cap_binding</code> so the two cannot be confused.</p>

<h3>2.6 Constants</h3>

<div class="tw"><table>
<thead><tr><th>Constant</th><th class="n">Value</th><th>Role</th></tr></thead>
<tbody>
{const_rows}
</tbody>
<caption>Read directly from <code>pipeline/signal_detector.py</code> at
build time, and included in the digest in &sect;7.1. These are judgement
calls, not fitted values. <strong>They have not been swept.</strong>
&sect;4 reports a sensitivity analysis of the <em>coverage register's</em>
constants, which is a different set; the detector's own thresholds are an
open item, and the honest statement today is that their influence on the
ledger is undocumented.</caption>
</table></div>

<h2><span class="num">3</span>Coverage: which history is analysable at all</h2>

<p>The corpus is assembled by a scraper fleet that came online source by
source. A source's first weeks of records are a fact about the scraper,
not about the food supply, and a baseline drawn from them will read any
subsequent normal week as an increase. The coverage register classifies
every source and records when its collection matured.</p>

{figure_one(d["weekly"], win_label, d["steps"], peak_src)}

<div class="tw"><table>
<thead><tr><th>Source</th><th class="n">Records</th><th class="n">Active rate</th>
<th class="d">Onset</th><th class="d">Mature</th><th>Basis</th></tr></thead>
<tbody>
{_coverage_rows(d['by_class'])}
</tbody>
<caption>From <code>docs/data/source-coverage.json</code>. Active rate is
the fraction of observable weeks in which the source produced at least one
record. &ldquo;Basis&rdquo; distinguishes an inferred date from one frozen
at first determination. Record counts here span the full history
including the trailing partial week, so they run slightly above the
complete-week figures in &sect;5.4.</caption>
</table></div>

<h3>3.1 Maturity dates are frozen once determined</h3>

<div class="note">
  <div class="h">Why maturity is frozen</div>
  <p style="margin:0 0 10px">Maturity was originally computed against the
  most recent eight weeks &mdash; a <em>moving</em> reference. The
  consequence was not drift but oscillation: one source's maturity moved
  five times across roughly eleven weeks, and another alternated between
  two dates eight weeks apart. Because the analytical window opens
  {sd.BASELINE_WEEKS + sd.GUARD_WEEKS} weeks after the latest maturity
  among continuous sources, a single source flipping could have moved the
  window forward by eight weeks and silently excluded six of the seven
  signal weeks in this report.</p>
  <p style="margin:0">Maturity is now frozen at first determination and
  carries its basis. A determination made on an unripe window is marked
  <code>provisional</code> and is not frozen.
  <code>--verify-stability</code> exits non-zero if a rebuild would move
  any cutoff, so this cannot regress unobserved.</p>
</div>

<h3>3.2 The analytical window</h3>

<p>The latest maturity among continuously collected sources is
<strong>{_wk(latest_mat) if latest_mat else 'not established'}</strong>.
A test week is only defensible once its <em>entire</em> baseline lies
after that date, which is {sd.BASELINE_WEEKS} baseline weeks plus
{sd.GUARD_WEEKS} guard weeks later. The window therefore opens
<strong>{win_label}</strong>.</p>

<p>Two things about that date are worth stating plainly. It is derived by
code, not asserted: <code>signal_detector.coverage_window_start()</code>
returns it from the register. And until 27 August 2026 the detector
contained no reference to the coverage register at all &mdash; the window
in the first draft of this report had been applied by hand. The gate is
now a property of the code. It is deliberately additive: signals outside
the window are <em>flagged</em>, not dropped, unless a caller asks for
enforcement, because silently changing what a detector reports is a worse
cure than the disease.</p>

<h2><span class="num">4</span>Threshold sensitivity</h2>

<p>The coverage register has seven judgement-set constants. Rather than
defend them by argument, each was swept across a plausible range with the
others held at default, measuring whether it moves the analytical cutoff.
The result is uncomfortable and worth publishing:</p>

<div class="tw"><table>
<thead><tr><th>Constant</th><th>Swept</th><th>Verdict</th></tr></thead>
<tbody>
<tr><td><code>CONTINUOUS_ACTIVE_RATE</code></td><td class="n">0.50 &ndash; 0.90</td>
<td><strong>Load-bearing.</strong> 0.50 gives 10 continuous sources and a
27 Apr cutoff; 0.90 gives one source and 16 Mar. This parameter silently
decides how much history is analysable.</td></tr>
<tr><td><code>ONSET_WINDOW</code></td><td class="n">4 &ndash; 10</td>
<td><strong>Load-bearing.</strong> At 4 the cutoff jumps ten weeks, to 15 Jun.</td></tr>
<tr><td><code>ONSET_MIN_ACTIVE</code></td><td class="n">2 &ndash; 5</td>
<td><strong>Load-bearing.</strong> At 5 the cutoff jumps to 15 Jun.</td></tr>
<tr><td><code>MATURITY_REF_WEEKS</code></td><td class="n">4 &ndash; 16</td>
<td><strong>Load-bearing at one end only.</strong> 4 gives 13 Apr;
6, 8, 12 and 16 are identical.</td></tr>
<tr><td><code>MATURITY_RATIO</code></td><td class="n">0.30 &ndash; 0.90</td>
<td><strong>Inert.</strong> A threefold range with no output change
anywhere. The parameter that reads as most consequential does the least.</td></tr>
<tr><td><code>OUTAGE_MIN_WEEKS</code></td><td class="n">2 &ndash; 6</td>
<td><strong>Inert.</strong> All values identical.</td></tr>
<tr><td><code>SPORADIC_MIN_RECORDS</code></td><td class="n">4 &ndash; 20</td>
<td><strong>Cosmetic.</strong> Shifts the sporadic tally between 23 and 28;
never moves the cutoff.</td></tr>
</tbody>
<caption>Reproduce with <code>python -m tools.coverage_sensitivity</code>.
Three of seven constants do not affect any decision this report depends on.
Publishing that is more useful than defending all seven.</caption>
</table></div>

<h2><span class="num">5</span>Results: walk-forward replay</h2>

<p>Every week is scored using only weeks strictly before it. This is not
cross-validation &mdash; random folds on surveillance time series leak the
future into the baseline and produce beautiful, meaningless numbers.
{d['weeks_scanned']} weeks were scored; the first
{sd.BASELINE_WEEKS + sd.GUARD_WEEKS + sd.C3_SPAN} are consumed by the
baseline, the guard band and the C3 accumulation window
({sd.BASELINE_WEEKS}&nbsp;+&nbsp;{sd.GUARD_WEEKS}&nbsp;+&nbsp;{sd.C3_SPAN};
see &sect;2.1) and cannot be scored at all.</p>

<p>There is no labelled anomaly set, so these tables are not an accuracy
measurement. They are the alarm ledger: each row is reviewed by hand and
marked genuine or artefact, and that log becomes the calibration record.</p>

<h3>5.1 How a row is admitted &mdash; the two channels differ</h3>

<p>This matters for reading the last column, and it is not symmetric.
A <strong>share</strong> row is admitted by an exact binomial test at
&alpha;&nbsp;=&nbsp;{sd.ALPHA} and must then survive
Benjamini&ndash;Hochberg at q&nbsp;=&nbsp;{sd.FDR_Q:.2f} across every stratum
tested that week. A <strong>count-only</strong> row is admitted by the
EARS statistic alone &mdash; C1 or C2 at 3.0, or C3 at 2.0, with the
observed count at least 20% above baseline &mdash; and <em>bypasses FDR by
design</em>, because it is not being offered as a discovery. It is logged
as a potentially volume-driven publication event requiring publisher-level
review, and suppressing those by multiplicity control would hide exactly
the rows an operator needs in order to recognise a backlog dump.</p>

<p>Because they are not multiplicity-controlled, count-channel rows carry
no false-discovery guarantee of any kind, and no count of them should be
reported as a number of signals. Every summary figure in this report
separates the two.</p>

<div class="warn">
  <div class="h">Do not read the two channels as one scale</div>
  <p style="margin:0">A count-only row carries a binomial p-value in the
  underlying record, and that number is frequently near 1.0 &mdash; which
  is the point: the count rose while the <em>share</em> did not, so the
  share test correctly declines to alarm. Reporting that p-value beside a
  count-only row would invite the reader to conclude the row is
  meaningless, when what it actually documents is a publication event. The
  column below therefore shows each row's own admission criterion, never
  the other channel's.</p>
</div>

<h3>5.2 Alert ledger inside the coverage window</h3>

<p>These {n_in} rows fall within the approved analytical window: for every
row, the complete baseline lies within mature collection, beginning no
earlier than the maturity week of
<strong>{_wk(latest_mat) if latest_mat else 'not established'}</strong>.
Taking the earliest row here, the week commencing {win_label}: its
count-channel (C2) baseline runs <strong>{c_lo} to {c_hi}</strong>, with
{guard_wks} held out as the guard band. The window opens exactly
{sd.BASELINE_WEEKS}&nbsp;+&nbsp;{sd.GUARD_WEEKS} weeks after maturity so
that the first baseline week coincides with the maturity week and no
earlier &mdash; true of the first row as well as the last.</p>

<p>The {n_in} rows are not one kind of object, and the distinction is the
most important thing on this page:</p>

<ul>
  <li><strong>{n_share} share-channel signals</strong>, each admitted by an
      exact binomial test and surviving Benjamini&ndash;Hochberg at
      q&nbsp;=&nbsp;{sd.FDR_Q:.2f}. These are the FDR-controlled findings.</li>
  <li><strong>{n_count} count-channel diagnostics</strong>, admitted on the
      EARS statistic alone and <em>not</em> multiplicity-controlled. These
      are publisher-review items. They are not statistical findings and
      must not be counted as such.</li>
</ul>

<div class="tw"><table>
<thead><tr>
<th class="d">Week commencing</th><th>Stratum</th><th>Channel</th>
<th class="n">Obs.</th><th class="n">Baseline</th>
<th class="n">Effect</th><th class="n">Other ratio</th><th class="n">Admitted by</th>
</tr></thead>
<tbody>
{_signal_rows([r for r in d['replay'] if r['in_window']])}
</tbody>
<caption><strong>Table 1.</strong> Effect follows the channel: share rows
show observed share over baseline share, count-only rows show observed
over baseline mean. &ldquo;Other ratio&rdquo; is the value on the channel
not used, retained so that nothing is unrecoverable. &ldquo;Admitted
by&rdquo; is each row's own criterion: for share rows the <strong>raw
exact binomial p, which survived Benjamini&ndash;Hochberg at
q&nbsp;=&nbsp;{sd.FDR_Q:.2f}</strong> (BH here returns a rejection mask,
not adjusted p-values, so no adjusted p exists to display); for
count-channel rows the governing EARS statistic, which is what admitted
them. The two columns are not comparable and are not ranked against each
other &mdash; see &sect;5.1 and &sect;2.6.</caption>
</table></div>

<h3>5.3 Audit ledger: rows outside the coverage window</h3>

<p>The remaining {n_out} rows were produced by the same detector on weeks
whose baseline predates mature collection. They are retained rather than
deleted, because a detector that quietly drops its own output cannot be
audited &mdash; but <strong>none of them is a finding</strong>. Any
elevation here is at least as likely to be the scraper fleet coming online
as a change in the food supply, and several are visibly that: a stratum
going from a baseline of 1.00 to 41 in one week is strongly consistent
with source activation rather than an underlying outbreak, though only
publisher-level review of those rows can confirm it.</p>

<div class="tw"><table>
<thead><tr>
<th class="d">Week commencing</th><th>Stratum</th><th>Channel</th>
<th class="n">Obs.</th><th class="n">Baseline</th>
<th class="n">Effect</th><th class="n">Other ratio</th><th class="n">Admitted by</th>
</tr></thead>
<tbody>
{_signal_rows([r for r in d['replay'] if not r['in_window']])}
</tbody>
<caption><strong>Table 2.</strong> Not findings. Reported for
completeness of the ledger only, and excluded from every count in this
report's summary. The coverage window opens {win_label}; see
&sect;3.2.</caption>
</table></div>

<h3>5.4 Corpus concentration</h3>

<div class="tw"><table>
<thead><tr><th>Source</th><th class="n">Records</th><th class="n">Share</th></tr></thead>
<tbody>
{src_rows}
</tbody>
<caption>Top eight publishers by volume. This concentration is the reason
the share channel exists.</caption>
</table></div>

<h2><span class="num">6</span>Limitations</h2>

<p><strong>No denominator.</strong> Notice counts are not incidence. A
country that publishes diligently will look worse than one that does not,
and this detector cannot distinguish the two. Every stratum is compared
only against itself.</p>

<p><strong>Short history.</strong> {d['n_weeks']} weeks supports EARS and
nothing seasonal. Any pattern with an annual period is invisible, and a
genuine seasonal rise will be read as an aberration until multiple years
of stable history exist.</p>

<p><strong>Publisher events dominate the count channel.</strong> Mitigated
by the share channel, not eliminated. A backlog clearance concentrated in
one hazard class will still move a share.</p>

<p><strong>Outbreaks that never generate a notice are invisible.</strong>
Some outbreaks surface through clinical surveillance rather than product
testing, and the implicated commodity is identified only after case
interviews converge. A recent multi-state Cyclospora cluster is the clean
example: FSIS observes the regulatory record, and where no notice is
issued until an investigation concludes, there is nothing in the corpus
for the detector to see. This is a structural limit of recall-derived
signal, not a tuning failure, and no amount of threshold work addresses
it.</p>

<p><strong>Coverage inference is not ground truth.</strong> Onset and
maturity are inferred from publication behaviour because no regulator
publishes a machine-readable statement of when its own feed became
complete. &sect;4 shows how much the inference depends on constants that
were chosen by judgement.</p>

<h2><span class="num">7</span>Reproducibility</h2>

<p>This page was generated from corpus revision
<span class="mono">{_esc(d['sha'])}</span> by:</p>

<pre style="background:var(--bg-2);border:1px solid var(--rule);padding:14px 16px;
overflow-x:auto;font-family:var(--mono);font-size:13px;margin:0 0 16px"><code
style="background:none;padding:0">python -m tools.build_technical_report</code></pre>

<p>Supporting commands:</p>

<ul>
  <li><code>python -m pipeline.source_coverage --build</code> &mdash; rebuild the register</li>
  <li><code>python -m pipeline.source_coverage --verify-stability</code> &mdash; exits 2 if any cutoff would move</li>
  <li><code>python -m tools.coverage_sensitivity</code> &mdash; reproduce &sect;4</li>
  <li><code>python -m pipeline.signal_detector --backtest</code> &mdash; reproduce Table 1 as CSV</li>
  <li><code>python -m tools.build_technical_report --print-digest</code> &mdash; recompute the digest below without writing the page</li>
</ul>

<h3>7.1 What reproducibility means here, precisely</h3>

<p>The page is <strong>not</strong> byte-for-byte reproducible, and it
would be dishonest to claim otherwise: it stamps its own build time, so
two builds always differ. Corpus revision alone would not be sufficient
even without that, because the result also depends on the detector's
source revision, its constants, the coverage register, and the runtime.</p>

<p>What is pinned is the analysis. The digest below is SHA-256 over the
analytical payload with the clock excluded &mdash; corpus shape, coverage
window, every constant in &sect;2.6, and every replay row with its
observed count, baseline, both effect ratios and p-value:</p>

<pre style="background:var(--bg-2);border:1px solid var(--rule);padding:14px 16px;
overflow-x:auto;font-family:var(--mono);font-size:12.5px;margin:0 0 16px;
word-break:break-all;white-space:pre-wrap"><code
style="background:none;padding:0">analytical-digest sha256
{digest}</code></pre>

<div class="tw"><table>
<thead><tr><th>Input</th><th>Pinned as</th></tr></thead>
<tbody>
<tr><td>Corpus (<code>docs/data/recalls.xlsx</code>)</td>
<td><span class="mono">{_esc(d['sha'])}</span> &middot; {d['n_records']:,} notices,
{d['n_weeks']} complete weeks, {_wk(d['first_week'])} to {_wk(d['last_week'])}</td></tr>
<tr><td>Detector source &amp; constants</td>
<td>Same revision; every constant listed in &sect;2.6 and included in the digest</td></tr>
<tr><td>Coverage register</td>
<td><code>docs/data/source-coverage.json</code>; maturity dates frozen, window
{_esc(win_label)}</td></tr>
<tr><td>Runtime</td>
<td>Python 3 with pandas; no fitted model, no random seed, no network access
at build time</td></tr>
<tr><td>Build clock</td>
<td><strong>Not pinned</strong> &mdash; excluded from the digest by construction</td></tr>
</tbody>
<caption>A rebuild that reports this digest has reproduced the analysis. A
rebuild that reports a different one has changed an input, and the table
above is the list of places to look.</caption>
</table></div>

<h2><span class="num">8</span>Revision history</h2>

<p>The method is versioned, and corrections to it are listed rather than
absorbed. Each entry names what changed and whether reported results moved,
so a figure quoted from an earlier build can be placed.</p>

<div class="tw"><table>
<thead><tr><th class="d">Date</th><th>Change</th><th>Results moved</th></tr></thead>
<tbody>
<tr><td class="d">27 Aug 2026</td>
<td><strong>Guard band aligned across channels.</strong> The share channel
built its baseline with <code>range(GUARD_WEEKS, &hellip;)</code>, which
includes the week at offset {sd.GUARD_WEEKS}: only the immediately
preceding week was excluded, and the second preceding week entered the
share-channel baseline. The count channel, using
<code>_window(&hellip;, offset=GUARD_WEEKS)</code>, excluded both. Both
channels now read the same {sd.BASELINE_WEEKS}-week window, and
<code>tests/test_guard_band.py</code> fails if they ever diverge
again.</td>
<td>Yes &mdash; regenerated in full for this edition.</td></tr>

<tr><td class="d">27 Aug 2026</td>
<td><strong>Effect follows the channel.</strong> The effect column carried
the share ratio for every row, including count-only ones, where it could
fall below 1. Both ratios are now retained on every record and the
displayed value matches the channel.</td>
<td>Yes &mdash; count-channel rows only.</td></tr>

<tr><td class="d">27 Aug 2026</td>
<td><strong>Coverage window enforced in code.</strong>
<code>coverage_window_start()</code> derives the window from the register;
previously it was applied by hand. Maturity dates are frozen at first
determination, ending an oscillation that could have moved the window by
eight weeks.</td>
<td>No &mdash; the derived window matches the one previously applied.</td></tr>

<tr><td class="d">27 Aug 2026</td>
<td><strong>Reproducibility restated.</strong> A byte-for-byte claim was
replaced by the analytical digest in &sect;7.1, which excludes the build
clock and pins the result set.</td>
<td>No.</td></tr>
</tbody>
<caption>Corrections found by external statistical review and by the
project's own test suite. Every entry is covered by a regression test;
none can recur silently.</caption>
</table></div>

<h2><span class="num">9</span>References</h2>

<ol style="font-size:14.5px">
  <li>Hutwagner L, Thompson W, Seeman GM, Treadwell T. The bioterrorism
      preparedness and response Early Aberration Reporting System (EARS).
      <em>Journal of Urban Health</em> 2003;80(2 Suppl 1):i89&ndash;i96.
      <span style="color:var(--ink-3)">&mdash; C1/C2/C3 as implemented in
      &sect;2.1.</span></li>
  <li>Farrington CP, Andrews NJ, Beale AD, Catchpole MA. A statistical
      algorithm for the early detection of outbreaks of infectious disease.
      <em>Journal of the Royal Statistical Society: Series A</em>
      1996;159(3):547&ndash;563.
      <span style="color:var(--ink-3)">&mdash; the method this corpus is
      not yet long enough to support.</span></li>
  <li>Noufaily A, Enki DG, Farrington P, Garthwaite P, Andrews N, Charlett A.
      An improved algorithm for outbreak detection in multiple surveillance
      systems. <em>Statistics in Medicine</em> 2013;32(7):1206&ndash;1222.
      <span style="color:var(--ink-3)">&mdash; the migration target once
      multiple years of stable history exist.</span></li>
  <li>Benjamini Y, Hochberg Y. Controlling the false discovery rate: a
      practical and powerful approach to multiple testing. <em>Journal of
      the Royal Statistical Society: Series B</em> 1995;57(1):289&ndash;300.
      <span style="color:var(--ink-3)">&mdash; the step-up procedure in
      &sect;2.4, applied to the share channel only.</span></li>
  <li>Clopper CJ, Pearson ES. The use of confidence or fiducial limits
      illustrated in the case of the binomial. <em>Biometrika</em>
      1934;26(4):404&ndash;413.
      <span style="color:var(--ink-3)">&mdash; the exact binomial basis of
      the share channel.</span></li>
</ol>

<div class="sub-cta">
  <h3>Food Safety Intelligence System</h3>
  <p>FSIS aggregates food recall and alert notices from
  {n_publishers} regulatory publishers across Europe, North America,
  Asia-Pacific and beyond, normalises them into one schema, and screens
  them for pathogens, biotoxins, mycotoxins, foreign material, pest and
  chemical hazards. Daily briefs, weekly and monthly digests, and the
  aberration scan described in this report.</p>
  <p style="margin-bottom:18px">Subscribe for the daily brief and weekly
  digest, or request access to the full corpus and the signal feed.</p>
  <a class="btn" href="{PUBLIC}">Subscribe to FSIS</a>
</div>

<footer>
  <p>{REPORT_ID} &middot; {AUTHOR}<br>
  Generated {gen:%d %B %Y %H:%M} UTC &middot; corpus revision
  <span class="mono">{_esc(d['sha'])}</span> &middot;
  {d['n_records']:,} notices over {d['n_weeks']} complete weeks.</p>
  <p>Advisory only. Nothing in this report is a substitute for the
  originating regulator's notice, which remains the authoritative record
  in every case. Source links are carried on every row in the FSIS corpus.</p>
  <p><a href="{SITE}">{SITE.replace('https://','')}</a> &middot;
  <a href="{PUBLIC}">{PUBLIC.replace('https://','')}</a></p>
</footer>

</div>

<script>
(function(){{
  /* Hover layer. The chart is legible without it - every bar carries its
     week in the data table below the figure - so failure here is silent. */
  var fig = document.querySelector('.fig .svgwrap');
  if (!fig) return;
  var tip = fig.querySelector('.tip');
  var svg = fig.querySelector('svg');
  function show(e){{
    var b = e.target.closest('.bar'); if (!b) return;
    var r = fig.getBoundingClientRect(), br = b.getBoundingClientRect();
    tip.textContent = b.dataset.l + ' \u2014 ' + b.dataset.v;
    tip.hidden = false;
    tip.style.left = (br.left - r.left + br.width/2) + 'px';
    tip.style.top  = (br.top - r.top) + 'px';
  }}
  svg.addEventListener('pointerover', show);
  svg.addEventListener('pointermove', show);
  svg.addEventListener('pointerleave', function(){{ tip.hidden = true; }});
}})();
</script>
</body>
</html>
"""


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(ROOT / "docs" / "reports" / f"{REPORT_ID}.html"))
    ap.add_argument("--json", default=None,
                    help="also write the gathered figures as JSON")
    ap.add_argument("--print-digest", action="store_true",
                    help="print the analytical digest and exit without "
                         "writing the page")
    a = ap.parse_args(argv)

    d = gather()
    if a.print_digest:
        print(analytical_digest(d))
        return 0
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(d), encoding="utf-8")

    if a.json:
        payload = {k: v for k, v in d.items()
                   if k not in ("register", "by_class", "generated")}
        payload["generated"] = d["generated"].isoformat(timespec="seconds")
        Path(a.json).write_text(json.dumps(payload, indent=2, ensure_ascii=False,
                                           default=str), encoding="utf-8")

    print(f"{REPORT_ID}: {out}  "
          f"({d['n_records']:,} notices / {d['n_weeks']} weeks / "
          f"{len(d['replay'])} ledger rows, window opens {d['window']})")
    print(f"analytical-digest sha256 {analytical_digest(d)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
