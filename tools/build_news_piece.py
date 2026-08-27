#!/usr/bin/env python3
"""Build the FSIS News piece that fronts TR-2026-01.

WHY A GENERATOR AND NOT A DOCUMENT
----------------------------------
The News piece and the technical annex will be read by the same people,
one after the other, and the fastest way to lose a technical reader is to
have the marketing page say 13 signals where the report says 12. Both
pages are therefore built from the SAME `gather()` call in
tools.build_technical_report. If a figure moves in the corpus it moves in
both, or neither.

Nothing here may state a capability the annex does not support. The claim
is detect-and-attribute; there is no outcome data linking any signal to a
confirmed event, and no sentence on this page implies otherwise.

    python -m tools.build_news_piece
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tools.build_technical_report import (  # noqa: E402
    CSS as REPORT_CSS, PUBLIC, SITE, _display, _esc, _wk, gather, sd,
)

TITLE = "How FSIS tells a food-safety signal from a publisher dump"
ANNEX_HREF = "TR-2026-01.html"
ANNEX_LABEL = "TR-2026-01"

EXTRA_CSS = """
.hero{padding:52px 0 30px;border-bottom:3px solid var(--ink);margin-bottom:36px}
.hero .eyebrow{font:600 11px/1 ui-sans-serif,system-ui,sans-serif;
  letter-spacing:.16em;text-transform:uppercase;color:var(--accent);
  margin-bottom:16px}
.hero h1{font-size:36px;line-height:1.15;margin:0 0 16px;letter-spacing:-.02em}
.hero .deck{font-size:19px;line-height:1.55;color:var(--ink-2);margin:0}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));
  gap:16px;margin:26px 0 30px}
.card{border:1px solid var(--rule);border-top:3px solid var(--accent);
  padding:18px 18px 20px;background:var(--bg)}
.card .big{font:600 30px/1 ui-sans-serif,system-ui,sans-serif;
  font-variant-numeric:tabular-nums;letter-spacing:-.02em;color:var(--accent)}
.card .who{font:600 14px/1.35 ui-sans-serif,system-ui,sans-serif;
  margin:10px 0 6px}
.card .det{font:13px/1.55 ui-sans-serif,system-ui,sans-serif;color:var(--ink-3)}
.pull{font-size:20px;line-height:1.5;color:var(--ink);border-left:3px solid
  var(--accent);padding:4px 0 4px 20px;margin:30px 0;font-style:italic}
.honest{border:1px solid var(--rule);background:var(--bg-2);padding:20px 22px;
  margin:30px 0}
.honest h3{margin:0 0 12px;font-size:15px;color:var(--ink-2)}
.honest ul{margin:0;padding-left:20px;font-size:15px}
.annex{display:block;border:1px solid var(--rule);border-left:3px solid
  var(--ink-3);padding:16px 20px;margin:30px 0;text-decoration:none;
  color:inherit}
.annex:hover{border-left-color:var(--accent);background:var(--bg-2)}
.annex .k{font:600 10.5px/1 ui-sans-serif,system-ui,sans-serif;
  letter-spacing:.14em;text-transform:uppercase;color:var(--ink-3);
  margin-bottom:8px}
.annex .t{font-weight:600;font-size:16px;margin-bottom:4px}
.annex .s{font:13.5px/1.55 ui-sans-serif,system-ui,sans-serif;color:var(--ink-3)}
@media (max-width:640px){.hero h1{font-size:27px}.hero .deck{font-size:17px}
  .pull{font-size:17px}}
"""


def _p(p: float) -> str:
    """Never print p = 0.00000 - it is false and it reads as a rounding bug."""
    return ("p&nbsp;&lt;&nbsp;0.00001" if p < 0.00001
            else f"p&nbsp;=&nbsp;{p:.5f}")


def _facts(d: dict) -> dict:
    inw = [r for r in d["replay"] if r["in_window"]]
    share = [r for r in inw if r["channel"] == "proportion"]
    count = [r for r in inw if r["channel"] != "proportion"]
    conc = [r for r in share if r["dominant_share"] >= sd.PUBLISHER_DOMINANCE]

    in_weeks = [w for w in d["weekly"] if w["in_window"]]
    per_week = sum(w["total"] for w in in_weeks) / max(len(in_weeks), 1)
    publishers = sum(1 for s in d["register"].values()
                     if s.coverage_class != "excluded")
    step = d["steps"][0] if d["steps"] else None

    # Three headline catches. The rule is stated on the page and applied
    # here: strongest statistical evidence first, at most one row per
    # stratum. Without the dedup the same French Listeria stratum takes two
    # of the three slots, which would read as padding rather than breadth.
    picks, seen = [], set()
    for r in sorted(share, key=lambda r: (r["p_value"], -r["effect"])):
        if r["label"] in seen:
            continue
        seen.add(r["label"])
        picks.append(r)
        if len(picks) == 3:
            break
    cards = "".join(
        f'<div class="card"><div class="big">&times;{r["effect"]:.2f}</div>'
        f'<div class="who">{_esc(_display(r["label"]))}</div>'
        f'<div class="det">Week of {_esc(r["week_label"])} &middot; '
        f'{r["observed"]} notices against a baseline of '
        f'{r["baseline_mean"]:.1f}. Share of corpus output, exact binomial '
        f'{_p(r["p_value"])}.<br>'
        f'<strong>Attributed:</strong> {int(round(r["dominant_share"]*100))}% '
        f'{_esc(r["dominant_source"] or "mixed sources")}.</div></div>'
        for r in picks)

    fr = [r for r in share if "France" in r["label"] and "Listeria" in r["label"]]
    fr_txt = ""
    if len(fr) >= 2:
        wk_list = ", ".join(_wk(r["week"]) for r in fr)
        fr_txt = (
            f'<p>The clearest example is Listeria monocytogenes in France, '
            f'which cleared the threshold in {len(fr)} separate weeks '
            f'({wk_list}) &mdash; {fr[0]["observed"]} notices against a '
            f'baseline of {fr[0]["baseline_mean"]:.1f} in the first of them. '
            f'A sustained elevation in one country, identified in the '
            f'walk-forward replay for the week in which the elevation '
            f'began, inside a stream averaging {per_week:.0f} notices a '
            f'week that nobody reads end to end.</p>')

    step_txt = ""
    if step:
        step_txt = (
            f'<p>Here is the trap. The single largest weekly jump in this '
            f'corpus is the week of {_esc(step["label"])}: '
            f'<strong>+{step["delta"]} notices</strong>, of which '
            f'{step["src_delta"]} came from {_esc(step["source"])} alone. '
            f'Any tool that watches raw counts alarms on that, and on every '
            f'hazard class inside it, simultaneously. The pattern is '
            f'strongly consistent with a publisher-volume event rather '
            f'than evidence of a comparable change across the food '
            f'supply.</p>')

    return {"share": share, "count": count, "conc": conc,
            "per_week": per_week, "publishers": publishers,
            "cards": cards, "fr_txt": fr_txt, "step_txt": step_txt}


def deck(d: dict) -> str:
    """One-sentence promise, built from the numbers it promises."""
    f = _facts(d)
    return (f"Recall volume goes up for two very different reasons. One of "
            f"them matters. Across {d['weeks_scanned']} scored weeks of "
            f"walk-forward replay FSIS found {len(f['share'])} "
            f"statistically controlled elevations \u2014 and named the "
            f"publisher behind every one.")


def front_matter(d: dict) -> str:
    """The capability lead, with no document shell and no hero.

    Kept separate so the same prose can open the single published report
    without duplicating a masthead, and so its figures come from the same
    `gather()` the method sections use.
    """
    f = _facts(d)
    share, count, conc = f["share"], f["count"], f["conc"]
    per_week, publishers = f["per_week"], f["publishers"]
    cards, fr_txt, step_txt = f["cards"], f["fr_txt"], f["step_txt"]
    return f"""
<p>FSIS reads recall and alert notices from {publishers} regulatory
publishers, normalises them into one schema, and screens them for
pathogens, biotoxins, mycotoxins, foreign material, pest and chemical
hazards. Across the analysable window that is {d['n_records']:,} notices
and about {per_week:.0f} a week.</p>

<p>Nobody reads {per_week:.0f} notices a week across {publishers}
publishers and spots that one pathogen in one country has quietly doubled
against its own recent baseline. That is the job the detector does.</p>

<h2>What it caught</h2>

<p>Three of the {len(share)}, chosen by a rule rather than by which reads
best: strongest statistical evidence first, one per stratum.</p>

<div class="cards">{cards}</div>

{fr_txt}

<h2>Why counting is not enough</h2>

{step_txt}

<p>FSIS scores each stratum on its <strong>share</strong> of the week's
total output rather than its raw count. When a publisher doubles
everything it files, every share stays roughly where it was and the
detector stays quiet. When one pathogen in one country rises against a
flat total, the share moves and the detector does not stay quiet.</p>

<p class="pull">The question is never &ldquo;are there more notices this
week?&rdquo; It is &ldquo;is this hazard taking a larger share of the
monitored regulatory corpus than it did a month ago?&rdquo;</p>

<h2>Detect, then attribute</h2>

<p>A share signal is not automatically a food-safety event either, and
this is where most tools stop and FSIS does not.
{len(conc)} of the {len(share)} elevations above came predominantly from a
single publisher &mdash; and every one of those carries that fact in the
record, with the publisher named and its percentage stated. The
{len(count)} further rows that moved on count but <em>not</em> on share are
kept in a separate ledger and labelled as what they are: publisher-review
items, not findings.</p>

<p>You get the signal and the reason to doubt it in the same row. An
analyst can act on one and dismiss the other in seconds, which is the
difference between a feed that gets read every morning and one that gets
muted in week three.</p>

<div class="honest">
  <h3>What we do not claim</h3>
  <ul>
    <li><strong>This is not prediction.</strong> The corpus has no
        denominator &mdash; no production volumes, no negative examples.
        Nothing here forecasts which product will be recalled next.</li>
    <li><strong>Notice counts are not incidence.</strong> A diligent
        regulator looks worse than a quiet one. Every stratum is compared
        only against itself.</li>
    <li><strong>No outcome linkage.</strong> We do not claim any of these
        signals predicted a confirmed outbreak. They are elevations in the
        regulatory record, detected and attributed.</li>
    <li><strong>Some events are invisible to us.</strong> An outbreak
        identified through clinical surveillance may generate no notice
        until the investigation closes. Nothing in the corpus can
        anticipate that.</li>
  </ul>
</div>
"""


def main(argv=None) -> int:
    """The News piece is no longer a separate page.

    It was briefly published as its own file with the technical report as
    an annex. That is two URLs to keep in step and two places for a number
    to drift, so the two were merged: the capability lead is now the front
    matter of the single published report. This module keeps the lead as a
    function; `tools.build_publication` renders the document.
    """
    print("build_news_piece is a component, not a page. "
          "Run: python -m tools.build_publication")
    return 1


if __name__ == "__main__":
    sys.exit(main())
