"""The News piece must not outrun the annex.

A marketing page and its technical annex read by the same person minutes
apart is exactly where an overclaim gets caught. These tests hold the page
to the annex's own numbers and to the claim boundary we set: detect and
attribute, no outcome linkage, no prediction.
"""
from __future__ import annotations

import re

import pytest

from tools import build_news_piece as news
from tools import build_technical_report as btr


@pytest.fixture(scope="module")
def built():
    d = btr.gather()
    return d, news.build(d)


def test_counts_match_the_annex(built):
    d, page = built
    inw = [r for r in d["replay"] if r["in_window"]]
    share = sum(1 for r in inw if r["channel"] == "proportion")
    count = len(inw) - share
    assert f"{share} statistically controlled elevations" in page
    assert f"Three of the {share}" in page
    assert f"The\n{count} further rows" in page or f"{count} further rows" in page
    assert f"{d['n_records']:,} notices" in page


def test_makes_no_prediction_or_outcome_claim(built):
    """Scoped to the selling copy.

    The disclaimer block necessarily contains these words in negated form
    ("we do not claim any of these signals predicted..."). Checking the
    whole page would fail on the very sentences that make the page honest,
    so the claim block is excised and then asserted separately.
    """
    _d, page = built
    body, _, rest = page.partition('<div class="honest">')
    honest, _, tail = rest.partition("</div>")
    selling = (body + tail).lower()
    for banned in ("predicted", "prevented", "early warning", "forecast",
                   "will be recalled", "outbreak detected"):
        assert banned not in selling, f"unsupported claim in the copy: {banned}"
    low = honest.lower()
    assert "this is not prediction" in low
    assert "no outcome linkage" in low
    assert "notice counts are not incidence" in low


def test_every_headline_card_is_a_real_share_signal(built):
    d, page = built
    share = {r["label"]: r for r in d["replay"]
             if r["in_window"] and r["channel"] == "proportion"}
    cards = re.findall(r'<div class="who">(.*?)</div>', page)
    assert len(cards) == 3
    display = {btr._display(k): v for k, v in share.items()}
    for c in cards:
        assert c in display, f"card names a stratum that is not a share signal: {c}"
    assert len(set(cards)) == 3, "the same stratum fills more than one card"


def test_card_effects_are_the_detectors_own(built):
    d, page = built
    # A stratum can signal in several weeks with different effects, so
    # match on the (stratum, effect) PAIR. Keying a dict by label alone
    # silently keeps the last week and compares the wrong number.
    real = {(btr._display(r["label"]), f'{r["effect"]:.2f}')
            for r in d["replay"]
            if r["in_window"] and r["channel"] == "proportion"}
    block = page.split('<div class="cards">', 1)[1]
    pairs = re.findall(r'<div class="big">&times;([\d.]+)</div>'
                       r'\s*<div class="who">(.*?)</div>', block)
    assert len(pairs) == 3
    for eff, label in pairs:
        assert (label, eff) in real, (
            f"card shows x{eff} for {label}; the detector produced no such "
            f"signal")


def test_no_p_value_is_printed_as_zero(built):
    _d, page = built
    assert "0.00000." not in page and "&nbsp;0.00000" not in page


def test_attribution_is_stated_on_every_card(built):
    _d, page = built
    assert page.count("<strong>Attributed:</strong>") == 3


def test_the_publisher_concentration_figure_is_computed(built):
    d, page = built
    share = [r for r in d["replay"]
             if r["in_window"] and r["channel"] == "proportion"]
    conc = sum(1 for r in share if r["dominant_share"] >= news.sd.PUBLISHER_DOMINANCE)
    assert f"{conc} of the {len(share)} elevations" in page, (
        "the page must state how many signals are single-publisher dominated, "
        "using the detector's own threshold")


def test_links_to_the_annex(built):
    _d, page = built
    assert news.ANNEX_HREF in page
    assert news.ANNEX_LABEL in page


def test_valid_document_shell(built):
    _d, page = built
    assert page.startswith("<!DOCTYPE html>")
    for tag in ('<html lang="en">', "<head>", '<meta charset="UTF-8">',
                "</head>", "<body>", "</body>", "</html>"):
        assert tag in page
    assert "src=\"http" not in page
