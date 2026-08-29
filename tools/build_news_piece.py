#!/usr/bin/env python3
"""Build the AFTS-FSIS News piece that fronts TR-2026-01.

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

# TITLE names the FINDING, not the machinery (operator instruction,
# 2026-08-27: "retitle to the data"). The previous title — "How FSIS tells a
# food-safety signal from a publisher dump" — described the tool and buried
# the result, and led with the ambiguous bare "FSIS".
# "global food recall record" overclaims: 48 publishers is extensive and is
# not the complete global regulatory record. Corrected 2026-08-28.
TITLE = "Anomalies in a multi-jurisdiction food recall corpus"
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


_NUM_WORDS = {6: "six", 8: "eight", 35: "thirty-five", 43: "forty-three"}


def _num_word(n: int) -> str:
    """Spell small counts that open a sentence; leave the rest as digits."""
    return _NUM_WORDS.get(n, str(n))



def _facts(d: dict) -> dict:
    inw = [r for r in d["replay"] if r["in_window"]]
    share = [r for r in inw if r["channel"] == "proportion"]
    count = [r for r in inw if r["channel"] != "proportion"]
    conc = [r for r in share if r["dominant_share"] >= sd.PUBLISHER_DOMINANCE]

    # TWO WINDOWS, NEVER CONFLATED (review 2026-08-28).
    #
    # The page previously read "Across the analysable window that is N
    # notices and about M a week", where N was the WHOLE 34-week corpus and
    # M was the mean over the 11 analytical weeks only. Two different
    # denominators in one sentence: the corpus total divided by the window's
    # week count is a number describing nothing.
    #
    # They are now separate figures with separate names, and both are
    # computed here so the sentence cannot drift from them again.
    in_weeks = [w for w in d["weekly"] if w["in_window"]]
    window_weeks = len(in_weeks)
    window_notices = sum(w["total"] for w in in_weeks)
    per_week = window_notices / max(window_weeks, 1)

    # PUBLISHER COUNT. This counted only sources with a coverage class other
    # than "excluded" and reported ~35, which understates the collection:
    # the register carries every source the corpus has ever seen, and a
    # source excluded from ANALYSIS is still a source being read. Both
    # numbers are now available and the prose says which is which.
    # Three different things, and the lead had been collapsing them:
    #   * SOURCE LABELS in the register (43) — not all of them publishers;
    #   * labels with a coverage class (35) — the official and regulatory
    #     ones, the only sense in which "publisher" is accurate;
    #   * EXCLUDED labels (8) — aggregators that re-publish another
    #     authority's notice (six CFS Hong Kong feeds) plus two
    #     non-regulatory sources, Food Safety News and BeaconBio.
    # And 84 is a count of JURISDICTIONS the notices cover, not of places
    # publishers sit in. "43 publishers in 84 countries" was wrong twice.
    labels = len(d["register"])
    publishers_classified = sum(1 for s in d["register"].values()
                                if s.coverage_class != "excluded")
    labels_excluded = labels - publishers_classified
    continuous = sum(1 for s in d["register"].values()
                     if s.coverage_class == "continuous")
    publishers = publishers_classified  # kept for callers
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

        return {"share": share, "count": count, "conc": conc, "per_week": per_week,
            "publishers": publishers, "labels": labels,
            "publishers_classified": publishers_classified,
            "labels_excluded": labels_excluded, "continuous": continuous,
            "window_weeks": window_weeks, "window_notices": window_notices,
            "countries": d.get("n_countries"),
            "cards": cards, "fr_txt": fr_txt, "step_txt": step_txt}


def deck(d: dict) -> str:
    """One-sentence promise, built from the numbers it promises."""
    f = _facts(d)
    # SCORED WEEKS ARE NOT REPORTED WEEKS (review 2026-08-28).
    # The replay scores every week it can reach — 22 — but only the weeks
    # inside the mature-coverage window are reportable, and the outputs of
    # the other scored weeks are explicitly excluded as findings. Quoting
    # the scored count next to the finding count implies the findings were
    # drawn from all of them.
    return (f"Recall volume goes up for two very different reasons. One of "
            f"them matters. Within the approved {f['window_weeks']}-week "
            f"analytical window, AFTS-FSIS identified {len(f['share'])} "
            f"FDR-controlled share elevations and attributed the dominant "
            f"publisher for each.")


def front_matter(d: dict) -> str:
    """The capability lead, with no document shell and no hero.

    Kept separate so the same prose can open the single published report
    without duplicating a masthead, and so its figures come from the same
    `gather()` the method sections use.
    """
    f = _facts(d)
    share, count, conc = f["share"], f["count"], f["conc"]
    per_week = f["per_week"]
    labels, p_cls = f["labels"], f["publishers_classified"]
    excl, cont = f["labels_excluded"], f["continuous"]
    win_wk, win_n = f["window_weeks"], f["window_notices"]
    cards, fr_txt, step_txt = f["cards"], f["fr_txt"], f["step_txt"]
    jur = f["countries"]
    return f"""
<p>AFTS-FSIS contains notices spanning {jur} jurisdictions and {labels}
source labels. {_num_word(p_cls).capitalize()} labels represent official or
regulatory publishers assigned a coverage class; {_num_word(excl)}
aggregator or non-regulatory labels are excluded. {_num_word(cont).capitalize()}
continuously collected publishers determine the analytical coverage window.
Every notice is normalised into one schema and screened for pathogens,
biotoxins, mycotoxins, foreign material, pest and chemical hazards.</p>

<p>The complete {d['n_weeks']}-week corpus contains
{d['n_records']:,} notices. The approved {win_wk}-week analytical window
contains {win_n:,} — about {per_week:.0f} a week. Those are two different
denominators, and the rest of this page keeps them apart.</p>

<p>Nobody reads approximately {per_week:.0f} notices a week across the
monitored feeds and spots that one pathogen in one country has quietly
doubled against its own recent baseline. That is the job the detector
does.</p>

<h2>What it caught</h2>

<p>Three of the {len(share)}, chosen by a rule rather than by which reads
best: strongest statistical evidence first, one per stratum.</p>

<div class="cards">{cards}</div>

{fr_txt}

<h2>Why counting is not enough</h2>

{step_txt}

<p>AFTS-FSIS scores each stratum on its <strong>share</strong> of the week's
total output rather than its raw count. When a publisher doubles
everything it files, every share stays roughly where it was and the
detector stays quiet. When one pathogen in one country rises against a
flat total, the share moves and the detector does not stay quiet.</p>

<p class="pull">The question is never &ldquo;are there more notices this
week?&rdquo; It is &ldquo;is this hazard taking a larger share of the
monitored regulatory corpus than it did a month ago?&rdquo;</p>

<h2>Detect, then attribute</h2>

<p>A share signal is not automatically a food-safety event either, and
this is where most tools stop and AFTS-FSIS does not.
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


def build(d: dict) -> str:
    """The News piece as one string: deck followed by front matter.

    Kept because tests/test_news_piece.py asserts against the whole piece,
    and because the two halves have to stay consistent with each other — a
    figure quoted in the deck and contradicted in the front matter is exactly
    the drift the merge into a single published page was meant to end.

    This is a composition of the two functions above, not a second renderer:
    tools.build_publication remains the thing that produces the page. Nothing
    here should ever grow logic of its own.
    """
    return deck(d) + "\n\n" + front_matter(d)


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
