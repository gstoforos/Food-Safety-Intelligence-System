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
from tools import build_publication as pub
from tools import build_technical_report as btr


@pytest.fixture(scope="module")
def built():
    d = btr.gather()
    return d, pub.build(d)


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
    # Scope: the capability lead only. The numbered method sections state
    # the same limits in plain language ("It does not forecast"), and
    # scanning those would fail on the sentences that make the page honest.
    lead = page.split('<span class="num">1</span>', 1)[0]
    body, _, rest = lead.partition('<div class="honest">')
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


def test_is_one_document_with_the_method_inside_it(built):
    """No annex link: the method must be in the same file, not a second URL."""
    _d, page = built
    assert 'class="annex"' not in page
    assert "TR-2026-01.html" not in page, (
        "the published report links out to an annex that no longer exists "
        "as a separate page")
    for section in ("Method", "Coverage", "Threshold sensitivity",
                    "Alert ledger inside the coverage window",
                    "Reproducibility", "References"):
        assert section in page, f"method section missing from the one file: {section}"


def test_the_lead_precedes_the_numbered_sections(built):
    _d, page = built
    assert page.index("What it caught") < page.index('<span class="num">1</span>')
    assert page.index("What we do not claim") < page.index('<span class="num">1</span>')


def test_valid_document_shell(built):
    _d, page = built
    assert page.startswith("<!DOCTYPE html>")
    for tag in ('<html lang="en">', "<head>", '<meta charset="UTF-8">',
                "</head>", "<body>", "</body>", "</html>"):
        assert tag in page
    assert "src=\"http" not in page


# ---------------------------------------------------------------------------
# Baseline indexing — the review of 27 August 2026
# ---------------------------------------------------------------------------

def test_stated_baseline_span_matches_the_code(built):
    """§5.2 names a concrete baseline span. Derive it from the module.

    An earlier draft quoted the SHARE channel's window while calling it the
    C2 baseline. The two channels disagree by a week (see §2.3), so the
    stated span must be pinned to the channel it claims to describe.
    """
    import pipeline.signal_detector as sd
    d, page = built
    corpus = sd.load_corpus()
    i = [k for k, w in enumerate(corpus.weeks) if str(w) == d["window"]][0]
    lo = i - sd.GUARD_WEEKS - sd.BASELINE_WEEKS
    hi = i - sd.GUARD_WEEKS - 1
    span = sd._window(
        {w: 1 for w in corpus.weeks}, corpus.weeks, i,
        sd.BASELINE_WEEKS, sd.GUARD_WEEKS)
    assert len(span) == sd.BASELINE_WEEKS
    assert f"{btr._wk(str(corpus.weeks[lo]))} to " \
           f"{btr._wk(str(corpus.weeks[hi]))}" in page
    # and every week held out as guard is named
    for k in range(hi + 1, i):
        assert btr._wk(str(corpus.weeks[k])) in page


def test_the_two_channels_now_read_the_same_baseline(built):
    """Derive both windows from the module; never assume the offset.

    An earlier version of this test hardcoded `share_lo = count_lo + 1`,
    which was the defect it was written to describe. When the defect was
    corrected the test failed for the wrong reason: it was asserting the
    bug. Both windows are now derived.
    """
    import pipeline.signal_detector as sd
    _d, page = built
    idx = 30
    hi = idx - sd.GUARD_WEEKS
    count = list(range(hi - sd.BASELINE_WEEKS, hi))
    share = sorted(idx - b for b in range(sd.GUARD_WEEKS + 1,
                                          sd.GUARD_WEEKS + 1 + sd.BASELINE_WEEKS))
    assert share == count, "the guard-band defect has been reintroduced"
    assert "Open defect" not in page, (
        "the channels agree; the open-defect disclosure must not ship")
    assert "Guard band aligned across channels" in page, (
        "a correction that moved published results must appear in the "
        "revision history")


def test_revision_history_records_whether_results_moved(built):
    _d, page = built
    assert "Revision history" in page
    hist = page.split("Revision history", 1)[1].split("</table>", 1)[0]
    assert hist.count("<tr>") >= 4
    assert "Results moved" in hist
    for entry in ("Guard band aligned across channels",
                  "Effect follows the channel",
                  "Coverage window enforced in code",
                  "Reproducibility restated"):
        assert entry in hist, f"missing revision-history entry: {entry}"


def test_leading_week_count_is_labelled_as_implementation_not_method(built):
    import pipeline.signal_detector as sd
    _d, page = built
    need = sd.BASELINE_WEEKS + sd.GUARD_WEEKS + sd.C3_SPAN
    assert f"<strong>{need}</strong>" in page
    assert f"<strong>{need - 1}</strong>" in page, (
        "the arithmetic minimum must be stated alongside the implemented one")
    assert "implementation requirement, not an" in page


def test_lead_does_not_imply_prospective_detection(built):
    _d, page = built
    lead = page.split('<span class="num">1</span>', 1)[0].lower()
    for banned in ("flagged in the week it began", "in real time",
                   "as it happened", "alerted"):
        assert banned not in lead, f"implies prospective detection: {banned}"
    assert "walk-forward replay" in lead


def test_lead_does_not_overstate_scope_or_certainty(built):
    _d, page = built
    lead = page.split('<span class="num">1</span>', 1)[0]
    assert "world's regulatory attention" not in lead
    assert "monitored regulatory corpus" in lead
    assert "Nothing happened to the food supply" not in lead
    assert "strongly consistent with a publisher-volume event" in lead


def test_effect_change_count_is_meaningful_not_tautological(built):
    """Guard against the "61 of 61" bug.

    Share and count ratios are almost never numerically equal, so counting
    rows where they differ returns every row and reports nothing. The
    figure that matters is how many DISPLAYED effects changed, which is
    the count-only rows.
    """
    d, page = built
    inw = [r for r in d["replay"] if r["in_window"]]
    changed = sum(1 for r in inw if r["channel"] != "proportion")
    assert 0 < changed < len(inw), "the statistic has gone degenerate"
    assert f"<strong>{changed} of {len(inw)}</strong> rows" in page
    assert f"{len(d['replay'])} of {len(d['replay'])}" not in page


# ── Branding: AFTS-FSIS, never bare "FSIS" ───────────────────────────────
# Operator rule, 2026-08-28. This regressed once already: an earlier build
# on main carried the pre-rule title and eleven bare self-references, and
# nothing caught it because nothing was checking. These tests are the check.

def _visible(page: str) -> str:
    """Rendered text only — comments and attributes are not what a reader sees."""
    body = re.sub(r"<!--.*?-->", " ", page, flags=re.S)
    body = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", body, flags=re.S | re.I)
    body = re.sub(r"<[^>]+>", " ", body)
    return re.sub(r"\s+", " ", body)


def test_the_page_never_says_bare_fsis(built):
    """USDA's Food Safety and Inspection Service is itself a Source in the
    corpus this report analyses, and is named in the results. A reader who
    meets "FSIS found 11 signals" cannot tell whose system is meant."""
    _d, page = built
    text = _visible(page)
    bare = [m.start() for m in re.finditer(r"FSIS", text)
            if not text[max(0, m.start() - 5):m.start()].endswith("AFTS-")
            and not text[max(0, m.start() - 5):m.start()].endswith("USDA ")]
    context = [text[max(0, i - 60):i + 40] for i in bare[:5]]
    assert not bare, f"{len(bare)} bare 'FSIS' in rendered text: {context}"


def test_the_page_brands_itself_at_least_once(built):
    _d, page = built
    assert "AFTS-FSIS" in _visible(page)


def test_the_title_names_the_finding_not_the_machinery(built):
    """Operator instruction 2026-08-27: retitle to the data. The previous
    title described the tool — "How FSIS tells a food-safety signal from a
    publisher dump" — and led with the ambiguous bare name."""
    _d, page = built
    title = re.search(r"<title>(.*?)</title>", page, re.S).group(1).strip()
    assert title == "Anomalies in the global food recall record", title
    assert not title.startswith("How "), "title describes the mechanism, not the result"


def test_the_two_builders_agree_on_the_title():
    """build_news_piece and build_publication each hold a TITLE. They are
    the front and the back of one document; two titles is two documents."""
    assert news.TITLE == pub.TITLE
