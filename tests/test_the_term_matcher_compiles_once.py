"""The term matcher compiles each pattern once — pinned 2026-09-30.

``pipeline/product_axes`` defines 1318 terms across five axes (CATEGORY 478,
PROCESS 436, CONSUMPTION 260, PACKAGING 120, PRESERVATION 24). ``_find`` built
the word-boundary pattern string fresh for every term on every call and handed
it to ``re.search``, whose module-level cache holds 512 entries — so each row
evicted the patterns the next row needed and most were recompiled from scratch.

MEASURED 2026-09-30 on the 1825-row register:

    python -m pipeline.enrich_schema --dry-run    119.9 s   before
                                                   10.2 s   after

and the printed report is byte-identical. Nothing about the MATCHING changed:
same pattern text, same flags, same term order, same first-hit-wins result.
Only the compile is hoisted into a cache.

This is why test_enrich_schema was the longest test in the suite, and why the
first attempt at the 2026-09-30 baseline hung there under a 90-second per-test
timeout with the stack sitting in ``re._parser`` parsing a character set. The
whole suite went from 287 s to 159 s.
"""
from __future__ import annotations

import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]



def test_the_term_matcher_does_not_recompile_every_pattern():
    """Code-side pin, so the 12x regression cannot creep back silently."""
    src = (ROOT / "pipeline" / "product_axes.py").read_text(encoding="utf-8")
    assert "_PAT_CACHE" in src and "def _term_pattern" in src, (
        "product_axes lost its compiled-pattern cache. With 1318 terms and a "
        "512-entry re cache, _find recompiles most patterns on every row: "
        "enrich_schema over the register went from 10.2 s back to 119.9 s.")
    assert "re.search(pat, text)" not in src, (
        "_find is building a pattern string and calling re.search with it "
        "again. Use _term_pattern(term).search(text).")


def test_the_cache_does_not_change_what_matches():
    """Same term, same text, same verdict — boundaries and plurals included."""
    from pipeline import product_axes as PA
    # boundary cases the docstring of _find names explicitly
    assert PA._find("crustaces en sauce", ("cru",)) is None
    assert PA._find("strawberry jam", ("raw",)) is None
    assert PA._find("crevettes roses", ("crevette",)) == "crevette"
    assert PA._find("lait cru de vache", ("cru",)) == "cru"
    # and the cache must be keyed per-term, not shared
    assert PA._find("saumon fume", ("cru", "fume")) == "fume"


def test_the_schema_sweep_finishes_in_a_sane_time():
    """A whole-register axis pass, timed. Generous, so it pins the ORDER of
    magnitude rather than flapping on a loaded runner."""
    pd = pytest.importorskip("pandas")
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():                                   # pragma: no cover
        pytest.skip("no workbook")
    from pipeline import product_axes as PA
    rows = pd.read_excel(xlsx, "Recalls").head(300).to_dict("records")
    t0 = time.time()
    for r in rows:
        PA.food_category(r)
        PA.process_type(r)
    dt = time.time() - t0
    assert dt < 20, (
        f"300 rows took {dt:.1f}s through two axes. Before the pattern cache "
        f"the full 1825-row sweep took 119.9 s; after it, 10.2 s. A number "
        f"this high means the cache is gone or bypassed.")
