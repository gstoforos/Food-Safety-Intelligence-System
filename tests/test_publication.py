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
    # Wording changed 2026-08-28: "scored weeks" was replaced by the
    # analytical-window count, because the replay scores 22 weeks and only
    # the 11 inside the mature-coverage window are reportable.
    assert f"{share} FDR-controlled share elevations" in page
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
    assert title == "Anomalies in a multi-jurisdiction food recall corpus", title
    assert not title.startswith("How "), "title describes the mechanism, not the result"
    assert "global" not in title.lower(), (
        "48 publishers is extensive and is not the complete global regulatory "
        "record — the title must not claim it is")


def test_the_two_builders_agree_on_the_title():
    """build_news_piece and build_publication each hold a TITLE. They are
    the front and the back of one document; two titles is two documents."""
    assert news.TITLE == pub.TITLE


def test_the_corpus_and_the_window_are_never_one_number(built):
    """The lead said "Across the analysable window that is N notices and about
    M a week", where N was the whole 34-week corpus and M was the mean over
    the 11 analytical weeks. Two denominators in one sentence: the corpus
    total divided by the window's week count describes nothing.

    Both figures must appear, each attached to its own week count.
    """
    d, page = built
    inw = [w for w in d["weekly"] if w["in_window"]]
    win_n, win_wk = sum(w["total"] for w in inw), len(inw)
    text = re.sub(r"<[^>]+>", " ", page)
    text = re.sub(r"\s+", " ", text)
    assert f"complete {d['n_weeks']}-week corpus contains {d['n_records']:,} notices" in text, text[:400]
    assert f"approved {win_wk}-week analytical window contains {win_n:,}" in text
    assert "analysable window that is" not in text, (
        "the conflated sentence is back")


def test_the_deck_does_not_quote_the_scored_week_count(built):
    """22 weeks are scored; 11 are reportable. Quoting the scored count beside
    the finding count implies the findings came from all of them."""
    d, page = built
    inw = [w for w in d["weekly"] if w["in_window"]]
    text = re.sub(r"<[^>]+>", " ", page)
    assert f"approved {len(inw)}-week analytical window" in re.sub(r"\s+", " ", text)
    assert f"Across {d['weeks_scanned']} scored weeks" not in text


# ── Carried over from tests/test_news_piece.py, retired 2026-08-28 ───────
# That file held nine tests, seven of which duplicated this one. Every
# wording change therefore broke two files, and on 2026-08-28 a title change
# did exactly that. These two were the only ones with no counterpart here,
# so they moved rather than being lost with the file.

@pytest.fixture(scope="module")
def lead_fragment():
    d = btr.gather()
    return d, news.build(d)


def test_the_lead_is_a_fragment_not_a_page(lead_fragment):
    """build() returns a component, not a document.

    The shell is build_publication's job. If this ever starts with a DOCTYPE
    again, the two-URL split has been reintroduced.
    """
    _d, page = lead_fragment
    assert not page.lstrip().startswith("<!DOCTYPE"), (
        "the News piece must remain a fragment — the document shell belongs "
        "to tools.build_publication")
    for tag in ("<html", "<head>", "</html>"):
        assert tag not in page, f"{tag} means a second page shell crept back"


def test_no_remote_assets(lead_fragment):
    """No externally hosted scripts or images — the published report must
    render from the repo alone."""
    _d, page = lead_fragment
    assert 'src="http' not in page


# ── Review round 3, 2026-08-28 ──────────────────────────────────────────

def test_no_printed_p_value_can_be_read_as_failing_its_threshold(built):
    """The 29 June Listeria/Europe signal has p = 0.009988. At four decimals
    that prints 0.0100, which a reader compares against alpha = 0.01 and
    concludes did not clear it — while the row sits in the admitted table.
    Rounding is a display choice and must never invert the finding it shows.
    """
    d, page = built
    text = re.sub(r"&nbsp;", " ", re.sub(r"<[^>]+>", " ", page))
    for r in d["replay"]:
        if not r["in_window"] or r["channel"] != "proportion":
            continue
        p = r["p_value"]
        for alpha in (0.05, 0.01, 0.001):
            if p < alpha:
                bad = f"p {alpha:.4f}"
                assert bad not in text, (
                    f"p={p!r} is below {alpha} but prints as {bad}, which reads "
                    f"as failing")


def test_the_lead_separates_labels_publishers_and_jurisdictions(built):
    """Three different counts that the lead had been collapsing into one
    phrase: source LABELS in the register, the subset that are official or
    regulatory PUBLISHERS, and the JURISDICTIONS the notices cover. "43
    publishers in 84 countries" was wrong twice — eight of the labels are
    aggregators or non-regulatory, and 84 counts jurisdictions covered, not
    places publishers sit."""
    d, page = built
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", page))
    reg = d["register"]
    labels = len(reg)
    classified = sum(1 for s in reg.values() if s.coverage_class != "excluded")
    assert f"{d['n_countries']} jurisdictions and {labels} source labels" in text
    assert "aggregator or non-regulatory labels are excluded" in text
    assert f"{labels} publishers" not in text, "the collapsed phrase is back"
    assert f"{labels} official and regulatory publishers" not in text
    assert classified + (labels - classified) == labels


def test_figure_two_does_not_call_diagnostics_findings(built):
    """Ten of the twenty-one rows are diagnostics, not statistical findings.
    A caption that calls all of them findings overstates the result by
    ninety per cent."""
    _d, page = built
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", page))
    assert "Every alert-ledger row inside the analytical window" in text
    assert "Every finding inside the analytical window" not in text
    assert "carried as diagnostics, not findings" in text
    assert "is usually the publisher, not the food supply" not in text, (
        "the causal claim is back; publisher activity may explain a "
        "count-only rise, it is not established that it does")
