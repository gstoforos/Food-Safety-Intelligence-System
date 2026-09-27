"""A published Product must name something.

WHY THIS EXISTS (2026-09-27)
===========================
An operator looking at the live dashboard spotted a row whose Product column
read, in full, "//".

    RappelConso fiche 23602 · Super U de Truchtersheim · France
    Listeria monocytogenes · Tier 1 · published 2026-09-25

It cleared every existing Product check. It is not blank, so the _blank()
guard passed it. It is not a page headline, so looks_like_a_headline passed
it. It does not open with a causal connector, so looks_like_a_fragment passed
it. Nothing asked whether it contained a single letter or digit.

A Tier-1 Listeria row that cannot say what to avoid is the one thing a recall
notice exists to do, so the gate now asks.

Deliberately narrow — one alphanumeric character in ANY script. The register
carries Greek (EFET), Cyrillic, Japanese and Chinese product names, and none
of them may be caught by a rule aimed at punctuation.
"""
from __future__ import annotations

import pytest

from pipeline._publish_gate import publish_blockers

BASE = dict(
    Date="2026-09-25", Source="RappelConso (FR)",
    Company="Super U de Truchtersheim", Brand="Super U",
    Pathogen="Listeria monocytogenes",
    Reason="Presence of Listeria monocytogenes",
    Class="Voluntary", Country="France", Tier=1,
    URL="https://rappel.conso.gouv.fr/fiche-rappel/23602/interne",
)

MARKER = "no letter or digit"


def _blocked(product):
    return [p for p in publish_blockers({**BASE, "Product": product})
            if MARKER in p]


class TestPunctuationIsNotAProduct:

    def test_the_live_row(self):
        """The exact value found on the dashboard."""
        assert _blocked("//"), "'//' must not be publishable as a Product"

    @pytest.mark.parametrize("product", [
        "///", "—", "- / -", "...", "·", "«»", "()", "   /   ",
    ])
    def test_other_punctuation_only_values(self, product):
        assert _blocked(product)


class TestRealProductsAreUntouched:

    @pytest.mark.parametrize("product", [
        "bouchee a la reine 300g x2",
        "crevettes entieres cuites refrigerees elevage 40/60 300g",
        "40/60 300g",                       # digits and slashes only
        "Supercol Food and Liquid Thickener - 325g, 900g and 4.5kg",
        "8",                                # a lone digit still names something
    ])
    def test_latin(self, product):
        assert not _blocked(product)

    @pytest.mark.parametrize("product,script", [
        ("ΤΥΡΙ ΦΕΤΑ ΠΟΠ 400γρ", "Greek — EFET publishes in Greek"),
        ("крем 200г", "Cyrillic"),
        ("ハム 300g", "Japanese"),
        ("黑芝麻醬", "Chinese"),
        ("KIMCHI 300 g Glas", "German"),
        ("szynka wieprzowa surowa dojrzewająca", "Polish"),
    ])
    def test_non_latin_scripts_pass(self, product, script):
        """The register carries all of these. A rule aimed at punctuation
        must not become a rule against alphabets."""
        assert not _blocked(product), script

    def test_an_empty_product_is_left_to_the_blank_guard(self):
        """This rule only speaks about non-empty values; blankness is
        already rule 1's business and has its own message."""
        assert not _blocked("")
        assert not _blocked("   ")


class TestTheRuleIsNarrow:

    def test_it_does_not_fire_on_any_published_row_but_the_known_one(self):
        """Run the rule across the live register. Exactly one row should
        trip it; if a second appears, the rule is too broad or the pipeline
        has regressed."""
        import openpyxl
        from pathlib import Path
        p = Path(__file__).resolve().parent.parent / "docs" / "data" / "recalls.xlsx"
        wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
        ws = wb["Recalls"]
        rows = list(ws.iter_rows(values_only=True))
        hdr = [str(c or "") for c in rows[0]]
        i = hdr.index("Product")
        hits = [str(r[i]) for r in rows[1:]
                if str(r[i] or "").strip()
                and not any(ch.isalnum() for ch in str(r[i]).strip())]
        assert len(hits) <= 1, (
            f"{len(hits)} published rows have a Product with no letter or "
            f"digit: {hits[:5]}")
