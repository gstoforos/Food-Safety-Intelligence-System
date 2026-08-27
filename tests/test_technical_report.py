"""TR-2026-01 must be reproducible from the code it describes.

The first draft of the report was written by hand, and its Table 1
reported an effect of 3.31 for Salmonella / United States — a number the
detector produces on no channel (share 3.27, count 3.50). These tests
exist so that class of divergence cannot recur silently: every figure in
the rendered page has to come from the detector.
"""
from __future__ import annotations

import re

import pytest

from tools import build_technical_report as btr


@pytest.fixture(scope="module")
def built():
    d = btr.gather()
    return d, btr.render(d)


def test_report_builds_and_is_self_contained(built):
    _d, page = built
    assert "<title>" in page
    # A strict CSP page and an offline audit artefact both require this.
    assert "http-equiv" not in page.lower() or "csp" not in page.lower()
    for bad in ("src=\"http", "href=\"http://cdn", "@import url(http"):
        assert bad not in page, f"external asset reference: {bad}"


def test_every_table_one_effect_comes_from_the_detector(built):
    d, page = built
    body = page.split('<caption><strong>Table 1.', 1)[0]
    table = body.rsplit('<tbody>', 1)[-1]
    rendered = set(re.findall(r'<strong>(\d+\.\d\d)</strong>', table))
    produced = {f"{r['effect']:.2f}" for r in d["replay"]}
    orphans = rendered - produced
    assert not orphans, (
        f"Table 1 shows effect values the detector never produced: "
        f"{sorted(orphans)}. Every figure must be generated, not typed.")


def test_no_in_window_signal_reports_an_effect_below_one(built):
    d, _page = built
    bad = [r for r in d["replay"] if r["in_window"] and r["effect"] < 1.0]
    assert not bad, (
        "A signals table cannot report an effect below 1 — it tells the "
        f"reader the stratum fell in the row that says it alarmed: {bad}")


def test_count_rows_use_the_count_ratio(built):
    d, _page = built
    # `effect` is stored at 2dp and the two ratios at 3dp, so compare at
    # the coarser precision rather than with a hand-picked epsilon.
    for r in d["replay"]:
        src = "effect_count" if r["channel"] != "proportion" else "effect_share"
        assert r["effect"] == pytest.approx(round(r[src], 2), abs=0.011), (
            f'{r["label"]} {r["week"]} ({r["channel"]}): effect '
            f'{r["effect"]} does not follow {src} {r[src]}')


def test_both_ratios_survive_into_the_report(built):
    d, _page = built
    for r in d["replay"]:
        assert r["effect_share"] > 0 or r["observed"] == 0
        assert r["effect_count"] > 0 or r["observed"] == 0


def test_headline_counts_match_the_corpus(built):
    d, page = built
    assert f"{d['n_records']:,}" in page
    assert str(d["n_weeks"]) in page
    assert str(d["n_strata"]) in page


def test_coverage_window_is_derived_not_asserted(built):
    d, page = built
    assert d["window"], "no coverage window derived from the register"
    # The window must be the one the detector computes, not a literal.
    assert d["window"] == btr.sd.coverage_window_start()
    assert btr._wk(d["window"]) in page


def test_out_of_window_signals_are_kept_in_a_separate_ledger(built):
    d, page = built
    out = [r for r in d["replay"] if not r["in_window"]]
    if not out:
        pytest.skip("every scored week is inside the coverage window")
    assert "Table 2." in page, (
        "signals outside the coverage window must be shown in the audit "
        "ledger, not silently removed")
    ledger = page.split("<h3>5.3", 1)[1]
    for r in out[:5]:
        assert r["label"] in ledger


def test_findings_table_holds_only_in_window_rows(built):
    d, page = built
    # Scope to the table BODY: §5.2's prose legitimately names the
    # baseline span, which is made of pre-window weeks.
    findings = (page.split("<h3>5.2", 1)[1]
                    .split("<tbody>", 1)[1]
                    .split("</tbody>", 1)[0])
    for r in d["replay"]:
        if r["in_window"]:
            continue
        # An out-of-window week label may coincide with an in-window one
        # only if some in-window row shares it; check the pairing instead.
        if any(x["in_window"] and x["week"] == r["week"] for x in d["replay"]):
            continue
        assert r["week_label"] not in findings, (
            f"{r['week_label']} predates mature coverage and cannot appear "
            f"among the findings")


def test_count_only_rows_never_display_a_p_value(built):
    """The count channel bypasses FDR and its binomial p is often ~1.0.

    Printing that p beside a count-only row invites the reader to dismiss
    a genuine publication artefact as noise. Each row shows its own
    admission criterion instead.
    """
    d, page = built
    body = page.split("<h3>5.2", 1)[1]
    rows = re.findall(r"<tr>(.*?)</tr>", body, re.S)
    for row in rows:
        if "count-only" in row:
            assert "p&nbsp;" not in row, (
                "a count-only row is showing a share-channel p-value: " + row)


def test_the_report_does_not_claim_universal_fdr_passage(built):
    d, page = built
    if any(r["channel"] != "proportion" for r in d["replay"]):
        assert "all rows shown passed" not in page.lower(), (
            "count-only rows bypass Benjamini-Hochberg by design; the "
            "report must not claim otherwise")


# ---------------------------------------------------------------------------
# Figure 1
# ---------------------------------------------------------------------------

def test_figure_weekly_series_matches_the_corpus(built):
    d, _page = built
    corpus = btr.sd.load_corpus()
    assert len(d["weekly"]) == len(corpus.weeks)
    assert sum(r["total"] for r in d["weekly"]) == d["n_records"]
    for r, w in zip(d["weekly"], corpus.weeks):
        assert r["week"] == str(w)
        assert r["total"] == int(corpus.totals.get(w, 0))


def test_figure_step_attribution_is_computed_not_asserted(built):
    """The caption names a source for each volume step. Verify the arithmetic."""
    d, page = built
    if not d["steps"]:
        pytest.skip("no positive week-over-week steps")
    corpus = btr.sd.load_corpus()
    weeks = {str(w): w for w in corpus.weeks}
    for st in d["steps"][:2]:
        w = weeks[st["week"]]
        i = corpus.weeks.index(w)
        prev = corpus.weeks[i - 1]
        f = corpus.frame
        now = int((f[(f["week"] == w) & (f["Source"] == st["source"])]).shape[0])
        was = int((f[(f["week"] == prev) & (f["Source"] == st["source"])]).shape[0])
        assert now - was == st["src_delta"], st
        assert st["src_delta"] <= st["delta"], st
        assert st["label"] in page


def test_figure_bars_carry_their_own_values(built):
    d, page = built
    fig = page.split('<figure class="fig"', 1)[1].split("</figure>", 1)[0]
    vals = re.findall(r'data-v="(\d+)"', fig)
    produced = [r["total"] for r in d["weekly"] if r["total"] > 0]
    produced += [r["sources"] for r in d["weekly"] if r["sources"] > 0]
    assert sorted(int(v) for v in vals) == sorted(produced)


def test_figure_has_a_table_view(built):
    _d, page = built
    fig = page.split('<figure class="fig"', 1)[1].split("</figure>", 1)[0]
    assert "<details>" in fig and "<table>" in fig, (
        "a chart must ship an accessible table view of its own data")


# ---------------------------------------------------------------------------
# Corrections applied 27 August 2026 after external review
# ---------------------------------------------------------------------------

def test_analytical_digest_excludes_the_clock(built):
    """Two builds of the same corpus must agree on the digest."""
    d, _page = built
    first = btr.analytical_digest(d)
    second = btr.analytical_digest(btr.gather())
    assert first == second, (
        "the digest moved between two builds of the same corpus — it is "
        "picking up the clock or some other unpinned input")
    assert len(first) == 64


def test_report_does_not_claim_byte_for_byte_reproducibility(built):
    _d, page = built
    low = page.lower()
    assert "byte for byte" not in low or "not</strong> byte-for-byte" in low
    assert "reproduces the reported analytical results" in low


def test_share_and_count_are_counted_separately(built):
    d, page = built
    n_share = sum(1 for r in d["replay"]
                  if r["in_window"] and r["channel"] == "proportion")
    n_count = sum(1 for r in d["replay"]
                  if r["in_window"] and r["channel"] != "proportion")
    kpis = page.split('<div class="kpis">', 1)[1].split("</div>\n</div>", 1)[0]
    assert "FDR-controlled share signals" in kpis
    assert "count-channel diagnostics" in kpis
    assert f'<div class="v">{n_share}</div>' in kpis
    assert f'<div class="v">{n_count}</div>' in kpis
    # The combined figure must not be presented as a signal count.
    assert f'<div class="v">{n_share + n_count}</div>\n    <div class="l">signals' not in page


def test_the_output_cap_claim_matches_the_detector(built):
    """§2.5 states the cap has never bound. Verify it, do not trust it."""
    d, page = built
    assert d["cap_bound_weeks"] == 0, (
        f"the cap bound on {d['cap_bound_weeks']} weeks; §2.5 says it never "
        f"has and must be corrected")
    assert d["max_out"] <= btr.sd.MAX_SIGNALS
    assert f"<strong>{d['max_out']}</strong> rows" in page


def test_ranking_never_compares_a_share_ratio_to_a_count_ratio(built):
    """The claim in §2.5 is about the sort key. Exercise it directly."""
    import pipeline.signal_detector as sd
    corpus = sd.load_corpus()
    strata = sd.build_strata(corpus)
    need = sd.BASELINE_WEEKS + sd.GUARD_WEEKS + sd.C3_SPAN
    for w in corpus.weeks[need:]:
        sigs, _m = sd.detect(corpus, strata, asof=w)
        chans = [s.channel for s in sigs]
        # every share row precedes every count-only row: one block boundary
        assert chans == sorted(chans, key=lambda c: c != "proportion"), w
        for a, b in zip(sigs, sigs[1:]):
            if a.channel == b.channel:
                assert a.effect >= b.effect, (w, a.label, b.label)


def test_turkey_is_displayed_as_turkiye(built):
    d, page = built
    if not any("Turkey" in r["label"] for r in d["replay"]):
        pytest.skip("no Turkish row in the ledger")
    assert "T\u00fcrkiye" in page
    body = page.split("<h3>5.2", 1)[1]
    assert "\u00b7 Turkey<" not in body, (
        "display alias did not reach the ledger tables")
