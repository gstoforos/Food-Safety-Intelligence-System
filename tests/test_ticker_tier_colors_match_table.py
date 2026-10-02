"""Ticker Tier-3 badge uses the table's Tier-3 color (operator 2026-10-02:
"in the headlines tier 3 color does not match the table").

The ticker used a binary t1/t2 test, so every Tier-3 headline wore the
Tier-2 orange tag while the register showed it cyan."""
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parents[1] / "docs"


@pytest.mark.parametrize("page", ["index.html", "index-promo.html"])
def test_ticker_has_a_tier3_tag_in_cyan(page):
    html = (DOCS / page).read_text(encoding="utf-8")
    assert "const tag   = r.tier === 1 ? 't1-tag' : (r.tier === 2 ? 't2-tag' : 't3-tag');" in html
    assert ".t3-tag{background:rgba(96,165,250,.13);color:var(--cyan);" in html
    assert "const tag   = r.tier === 1 ? 't1-tag' : 't2-tag';" not in html
