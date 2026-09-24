# -*- coding: utf-8 -*-
"""url_resurrect must not accept a URL just because it loads.

THE CASE THIS COMES FROM (2026-09-24)
=====================================
A Pending row for "CFS orders recall of US raw oysters after excessive
E. coli" carried

    cfs.gov.hk/english/whatsnew/whatsnew_fa/2026_627.html

which is a completely different alert — "CFS finds trace amount of
formaldehyde in prepackaged rice vermicelli sample".

url_resurrect did the right thing: it proposed the real oysters notice,
cfs.gov.hk/english/press/20260921_12610.html, at confidence 1.00, and
rewrote the row. The daily accuracy brief blamed it for the bad URL; the
audit trail says the opposite, and there is exactly one [resurrected]
stamp in the register — on the row now holding the CORRECT url.

Two things around it were wrong, and neither was checked anywhere:

  1. verify_url() only asks whether a URL LOADS. A plausible-but-wrong
     regulator URL returns 200 and was accepted exactly like the right
     one. That blind spot is how the bad URL got onto the row upstream.

  2. merge_master._dedup_key is URL-PRIMARY, so rewriting a row's URL
     changes its identity. The resurrected row could no longer dedupe
     against its own pre-resurrection copy, and the register ended up
     holding the same recall twice — two Pending rows, same source_id,
     same product, same date, one per URL.

And the Gemini confidence score was computed, logged, and never used.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

ur = pytest.importorskip("pipeline.url_resurrect")


# ── Gate 1: confidence ──────────────────────────────────────────────────

def test_a_confidence_floor_exists_and_is_not_a_coin_flip():
    assert hasattr(ur, "CONF_MIN"), (
        "the Gemini confidence score is computed and logged; it must also "
        "gate acceptance")
    assert 0.5 < ur.CONF_MIN <= 1.0, (
        f"CONF_MIN={ur.CONF_MIN} — a floor at or below 0.5 accepts a "
        "coin-flip proposal")


def test_the_confidence_floor_is_wired_into_the_accept_path():
    src = (ROOT / "pipeline" / "url_resurrect.py").read_text(encoding="utf-8")
    code = "\n".join(l for l in src.splitlines()
                     if not l.lstrip().startswith("#"))
    assert "confidence < CONF_MIN" in code, (
        "CONF_MIN is defined but nothing compares against it")


# ── Gate 2: the page must be about this recall ──────────────────────────

def test_row_tokens_ignores_words_that_prove_nothing():
    toks = ur._row_tokens({
        "Company": "R.J. King Fisheries Ltd.",
        "Brand": "Moncton Fish Market",
        "Product": "frozen cooked lobster meat",
    })
    assert "lobster" in toks and "moncton" in toks
    for junk in ("recall", "product", "brand", "food", "ltd"):
        assert junk not in toks, f"{junk!r} would match almost any page"


def test_a_row_with_nothing_distinctive_does_not_block():
    """No tokens means no evidence either way — must not be a mismatch."""
    verdict, _ = ur.verify_url_is_about_this_recall(
        "https://example.invalid/x", {"Company": "", "Brand": "", "Product": ""})
    assert verdict == "no-body"


def test_an_unreadable_page_never_blocks(monkeypatch):
    """403 and JS-only pages must not be treated as wrong.

    fda.gov, fsis.usda.gov, fda.gov.ph and gov.il all refuse datacentre
    traffic. Blocking on unreadable would discard every correct proposal
    for precisely the regulators this module exists to rescue.
    """
    class Boom:
        def get(self, *a, **k): raise OSError("refused")
    monkeypatch.setitem(sys.modules, "requests", Boom())
    verdict, why = ur.verify_url_is_about_this_recall(
        "https://fsis.usda.gov/x", {"Product": "frozen cooked lobster meat"})
    assert verdict == "no-body", f"got {verdict} ({why}) — this must not block"


def test_a_page_about_something_else_is_a_mismatch(monkeypatch):
    """The oysters/formaldehyde case, in miniature."""
    class Resp:
        status_code = 200
        text = ("<html><h1>CFS finds trace amount of formaldehyde in "
                "prepackaged rice vermicelli sample</h1></html>")
    class Fake:
        def get(self, *a, **k): return Resp()
    monkeypatch.setitem(sys.modules, "requests", Fake())
    verdict, why = ur.verify_url_is_about_this_recall(
        "https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/2026_627.html",
        {"Company": "American Pearl", "Product": "raw oysters"})
    assert verdict == "mismatch", f"got {verdict} ({why})"


def test_the_right_page_matches(monkeypatch):
    class Resp:
        status_code = 200
        text = ("<html><p>The Centre for Food Safety today ordered a recall "
                "of American Pearl raw oysters after excessive E. coli.</p></html>")
    class Fake:
        def get(self, *a, **k): return Resp()
    monkeypatch.setitem(sys.modules, "requests", Fake())
    verdict, why = ur.verify_url_is_about_this_recall(
        "https://www.cfs.gov.hk/english/press/20260921_12610.html",
        {"Company": "American Pearl", "Product": "raw oysters"})
    assert verdict == "match", f"got {verdict} ({why})"


def test_the_content_gate_is_wired_in_and_only_blocks_on_mismatch():
    src = (ROOT / "pipeline" / "url_resurrect.py").read_text(encoding="utf-8")
    code = "\n".join(l for l in src.splitlines()
                     if not l.lstrip().startswith("#"))
    assert "verify_url_is_about_this_recall" in code
    assert 'verdict == "mismatch"' in code, (
        "the content check must block on mismatch specifically — blocking on "
        "no-body would throw away every proposal for a 403 regulator")


# ── Gate 3: the twin a URL rewrite creates ──────────────────────────────

OLD = "https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/2026_627.html"
NEW = "https://www.cfs.gov.hk/english/press/20260921_12610.html"


def _pair():
    stale = {"Date": "2026-09-21", "Product": "CFS orders recall of US raw oysters",
             "URL": OLD, "Status": "pending_gap_v2",
             "Notes": "source_id=GN-ce4cfbdeff98 [via Google News]"}
    fixed = {"Date": "2026-09-21", "Product": "CFS orders recall of US raw oysters",
             "URL": NEW, "Status": "pending",
             "Notes": "source_id=GN-ce4cfbdeff98 [via Google News]"}
    return stale, fixed


def test_the_stale_twin_is_found_by_source_id():
    stale, fixed = _pair()
    twin = ur.collapse_resurrection_twin([stale, fixed], fixed, OLD)
    assert twin is stale


def test_a_different_recall_is_never_mistaken_for_the_twin():
    stale, fixed = _pair()
    other = {"Date": "2026-09-21", "Product": "something else entirely",
             "URL": OLD, "Status": "pending",
             "Notes": "source_id=GN-DIFFERENT"}
    twin = ur.collapse_resurrection_twin([other, fixed], fixed, OLD)
    assert twin is None, "matched a row with a different source_id"


def test_a_row_already_on_the_new_url_is_not_dropped():
    """Only the copy still on the OLD url is stale. Dropping a row that
    already agrees would lose data."""
    _, fixed = _pair()
    agreeing = dict(fixed); agreeing["Status"] = "pending_gap_v2"
    twin = ur.collapse_resurrection_twin([agreeing, fixed], fixed, OLD)
    assert twin is None


def test_the_row_being_fixed_is_never_its_own_twin():
    _, fixed = _pair()
    assert ur.collapse_resurrection_twin([fixed], fixed, OLD) is None


def test_twin_collapse_is_wired_into_the_write_path():
    src = (ROOT / "pipeline" / "url_resurrect.py").read_text(encoding="utf-8")
    code = "\n".join(l for l in src.splitlines()
                     if not l.lstrip().startswith("#"))
    assert "collapse_resurrection_twin(" in code
    assert "pending.remove(twin)" in code, (
        "the twin is identified but never removed, so the duplicate stays")


def test_dedup_is_still_url_primary_so_this_guard_is_still_needed():
    """If _dedup_key ever stops being URL-primary this guard becomes
    redundant — but until then, removing it reintroduces the duplicate."""
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    assert "_normalize_url_for_dedup(raw_url)" in src
