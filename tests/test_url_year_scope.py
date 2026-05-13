"""
tests/test_url_year_scope.py
=============================
Sanity tests for the URL-year extractor, pathogen-scope filter, and
news-mirror blocklist — three small but high-value gatekeepers that
several pipeline steps depend on:

    pipeline._url_year             — extracts year from URL, returns None
                                     when ambiguous. Used by claude_check
                                     and url_gate to flag year-mismatches.
    pipeline._pathogen_scope       — is this pathogen in FSIS scope?
    pipeline._news_mirror_blocklist— is this URL a news re-print of a
                                     real regulator notice?

These are pure functions with no I/O. Tests run in milliseconds.

If you break url_year() or is_in_scope(), the bug surfaces silently
downstream — a recall slips through gating OR a valid recall gets
wrongly rejected. These tests catch that at commit time.
"""
from __future__ import annotations

from datetime import date

import pytest

from pipeline._url_year import url_year, is_year_mismatch
from pipeline._pathogen_scope import is_in_scope
from pipeline._news_mirror_blocklist import is_news_mirror


# ───────────────────────────────────────────────────────────────────────
# url_year() — extracting year from URL
# ───────────────────────────────────────────────────────────────────────
class TestUrlYearExtraction:
    """Year extraction from various URL shapes the scrapers encounter."""

    def test_empty_url_returns_none(self):
        assert url_year("") is None
        assert url_year(None) is None

    def test_rappelconso_new_format(self):
        # New RappelConso URL: /fiche-rappel/YYYY-MM-NNNN/
        assert url_year(
            "https://rappel.conso.gouv.fr/fiche-rappel/2026-04-0149/interne"
        ) == 2026

    def test_rappelconso_old_fiche_id_is_not_a_year(self):
        # Old RappelConso URL: /fiche-rappel/22019/Interne. The 22019 is a
        # sequential fiche ID, NOT a year. If url_year wrongly returned
        # 2202 or 2019, gating would think a 2026 row is from 2019 and
        # reject it. Lock this behavior.
        assert url_year(
            "https://rappel.conso.gouv.fr/fiche-rappel/22019/Interne"
        ) is None

    def test_produktwarnung_path_yyyy_mm_dd(self):
        # produktwarnung.eu and most news mirrors use /YYYY/MM/DD/slug
        assert url_year(
            "https://www.produktwarnung.eu/2026/04/24/rueckruf-aflatoxin"
        ) == 2026

    def test_fda_recall_number_extracts_year(self):
        # FDA recall numbers embed the year: F-NNNN-YYYY
        assert url_year(
            "https://www.fda.gov/safety/recalls?search_api_fulltext=F-1234-2024"
        ) == 2024

    def test_fsis_usda_recall_slug(self):
        # FSIS uses slugs like /recalls/some-product-2026 — extractor must
        # only match when domain is fsis.usda.gov, not on every site.
        assert url_year(
            "https://www.fsis.usda.gov/recalls/ground-beef-recall-2026"
        ) == 2026

    def test_fsis_usda_landing_no_year(self):
        # The FSIS landing page itself has no year in the URL — caller
        # gets None, must NOT treat that as mismatch.
        assert url_year(
            "https://www.fsis.usda.gov/recalls"
        ) is None

    def test_pdf_filename_digits_ignored(self):
        # PDF filenames often contain dates in weird formats (DDMMYY) that
        # naive year extraction would misread. Skip them entirely.
        assert url_year(
            "https://www.blv.admin.ch/dam/.../rr-calmar.pdf.download.pdf/"
            "26_04_08%20...142026.pdf"
        ) is None

    def test_fsanz_landing_no_year(self):
        # FSANZ recall landings don't encode the year — caller must not
        # try to match.
        assert url_year(
            "https://www.foodstandards.gov.au/food-recalls/recall-alert/aldi"
        ) is None

    def test_generic_year_path_recognised(self):
        # Generic /YYYY/ in path catches most blog-style URLs.
        assert url_year(
            "https://example.com/news/2026/something-recall.html"
        ) == 2026

    @pytest.mark.parametrize("year", [2020, 2025, 2026, 2027])
    def test_year_extraction_round_trip(self, year):
        # Year extraction should preserve exact year, not get rounded.
        u = f"https://www.produktwarnung.eu/{year}/04/24/foo"
        assert url_year(u) == year


# ───────────────────────────────────────────────────────────────────────
# is_year_mismatch() — gating decision rule
# ───────────────────────────────────────────────────────────────────────
class TestYearMismatchRule:
    """
    The mismatch rule (see pipeline/_url_year.py):
      - URL year < 2026 → ALWAYS mismatch (FSIS scope is 2026+)
      - |URL year − Date year| > 1 → mismatch
      - Otherwise → not a mismatch
      - If URL year is None → not a mismatch (caller can't tell)
    """

    def test_no_date_returns_none(self):
        assert is_year_mismatch(None, "https://example.com/2026/foo") is None

    def test_url_year_pre_2026_always_fails(self):
        # 2025 URL with a 2026 date — pre-2026 scope filter trips
        result = is_year_mismatch(
            date(2026, 1, 5), "https://example.com/2025/foo")
        assert result is not None
        assert "pre-2026" in result.lower() or "2025" in result

    def test_url_year_2024_with_2026_date_fails(self):
        result = is_year_mismatch(
            date(2026, 6, 1), "https://example.com/2024/foo")
        assert result is not None

    def test_within_tolerance_2027_url_2026_date_ok(self):
        # |2027 − 2026| = 1 → within tolerance, not a mismatch.
        assert is_year_mismatch(
            date(2026, 12, 31), "https://example.com/2027/foo"
        ) is None

    def test_extractor_returns_none_no_mismatch(self):
        # When the URL has no extractable year, the rule must return None
        # (NOT assume mismatch). Caller policy: ambiguity ≠ rejection.
        assert is_year_mismatch(
            date(2026, 4, 14),
            "https://www.foodstandards.gov.au/food-recalls/recall-alert/aldi"
        ) is None


# ───────────────────────────────────────────────────────────────────────
# is_in_scope() — pathogen scope filter
# ───────────────────────────────────────────────────────────────────────
class TestPathogenScope:
    """
    FSIS in-scope pathogens are the ones that cause acute food-safety
    recalls of broad commercial interest. Out of scope: marine biotoxins,
    histamine/scombrotoxin (covered elsewhere), allergens.
    """

    @pytest.mark.parametrize("pathogen", [
        "Listeria monocytogenes",
        "Salmonella",
        "Campylobacter",
        "Escherichia coli O157:H7",
        "Clostridium botulinum",
        "Cronobacter sakazakii",
    ])
    def test_in_scope_pathogens(self, pathogen):
        assert is_in_scope(pathogen), \
            f"Expected {pathogen!r} to be in FSIS scope"

    @pytest.mark.parametrize("pathogen", [
        "Marine biotoxin",
        "Histamine / scombrotoxin",
        "",          # empty
        None,        # missing
        "—",         # placeholder dash
    ])
    def test_out_of_scope_pathogens(self, pathogen):
        assert not is_in_scope(pathogen), \
            f"Expected {pathogen!r} to be OUT of FSIS scope"


# ───────────────────────────────────────────────────────────────────────
# is_news_mirror() — news outlet blocklist
# ───────────────────────────────────────────────────────────────────────
class TestNewsMirrorBlocklist:
    """
    News mirrors (sedaily, beaconbio, ilfattoalimentare, produktwarnung)
    reprint regulator notices but are not authoritative sources. The
    blocklist keeps the gating pipeline from accepting them as origin.
    """

    @pytest.mark.parametrize("url", [
        "https://en.sedaily.com/NewsView/2REI1NCM2Q",
        "https://www.ilfattoalimentare.it/example.html",
        "https://www.beaconbio.org/news/example",
        "https://www.foodsafetynews.com/2026/05/listeria-outbreak/",
        "https://www.reuters.com/world/recall-news",
    ])
    def test_known_news_mirrors_blocked(self, url):
        assert is_news_mirror(url), f"Expected {url!r} to be flagged as news mirror"

    @pytest.mark.parametrize("url", [
        "https://rappel.conso.gouv.fr/fiche-rappel/22157/Interne",
        "https://www.fsis.usda.gov/recalls/ground-beef-2026",
        "https://www.foodstandards.gov.au/food-recalls/recall-alert/aldi",
    ])
    def test_clean_regulator_urls_pass(self, url):
        assert not is_news_mirror(url), \
            f"Expected {url!r} NOT to be flagged as news mirror"
