"""Sanity tests for canonical URL-year extractor."""
from datetime import date
from pipeline._url_year import url_year, is_year_mismatch
from pipeline._pathogen_scope import is_in_scope
from pipeline._news_mirror_blocklist import is_news_mirror


def test_rappelconso_old_fiche_returns_none():
    assert url_year("https://rappel.conso.gouv.fr/fiche-rappel/22019/Interne") is None


def test_rappelconso_new_format_extracts_year():
    assert url_year("https://rappel.conso.gouv.fr/fiche-rappel/2026-04-0149/interne") == 2026


def test_pdf_filename_returns_none():
    assert url_year(
        "https://www.blv.admin.ch/dam/.../rr-calmar.pdf.download.pdf/26_04_08%20...142026.pdf"
    ) is None


def test_produktwarnung_yyyy_mm_dd_path():
    assert url_year("https://www.produktwarnung.eu/2026/04/24/rueckruf-aflatoxin") == 2026


def test_fda_recall_number_pattern():
    assert url_year("https://www.fda.gov/safety/recalls?search_api_fulltext=F-1234-2024") == 2024


def test_fsanz_landing_no_year():
    assert url_year("https://www.foodstandards.gov.au/food-recalls/recall-alert/aldi") is None


def test_year_mismatch_pre_2026():
    assert is_year_mismatch(date(2026, 4, 14), "https://example.com/safety/2024/foo") is not None


def test_year_mismatch_tolerance_1y():
    # Per locked rules: URL year < 2026 always fails (FSIS scope is 2026+)
    # so a URL with "/2025/" against a 2026-01-05 date should fail.
    assert is_year_mismatch(date(2026, 1, 5), "https://example.com/2025/foo") is not None


def test_year_mismatch_2y_gap():
    assert is_year_mismatch(date(2026, 6, 1), "https://example.com/2024/foo") is not None


def test_pathogen_in_scope_listeria():
    assert is_in_scope("Listeria monocytogenes")


def test_pathogen_in_scope_campylobacter():
    assert is_in_scope("Campylobacter")


def test_pathogen_out_of_scope_marine_biotoxin():
    assert not is_in_scope("Marine biotoxin")


def test_pathogen_out_of_scope_histamine():
    assert not is_in_scope("Histamine / scombrotoxin")


def test_pathogen_empty():
    assert not is_in_scope("")
    assert not is_in_scope(None)
    assert not is_in_scope("—")


def test_news_mirror_sedaily():
    assert is_news_mirror("https://en.sedaily.com/NewsView/2REI1NCM2Q")


def test_news_mirror_clean_regulator():
    assert not is_news_mirror("https://rappel.conso.gouv.fr/fiche-rappel/22157/Interne")


if __name__ == "__main__":
    fail = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  ✓ {name}")
            except AssertionError:
                print(f"  ✗ {name}")
                fail += 1
    print(f"\n{'All tests pass.' if not fail else f'{fail} FAILURES'}")
