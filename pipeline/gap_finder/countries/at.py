"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Austria (AGES — Austrian Agency for Health and Food Safety)

REVISION (Batch 2.1 hotfix): The first Austria deployment caught 0 real
recalls because Google News query "Lebensmittelwarnung Österreich" matched
noise (Putin warnings, heat warnings, scam warnings). Fixed by:
  1. Adding produktwarnung.eu as PRIMARY source — it publishes Austrian
     recalls in its "Rückrufe in Nachbarstaaten" section (in German).
  2. Replacing broad Google News terms with precise compound German phrases.
  3. Removing noisy news domains (tt.com returned 100 sport/political results).
"""

from .base import CountryConfig, RssSource, register


AUSTRIA = CountryConfig(
    code="at",
    name_en="Austria",
    name_local="Österreich",

    authority_short="AGES",
    authority_full="Agentur für Gesundheit und Ernährungssicherheit",
    authority_domain="ages.at",
    authority_item_url_regex=r"produktwarnungen|produktrueckrufe|rueckruf",
    # AGES recall list (verified 2026-06): lists food product warnings/recalls
    # with per-item Details pages. Tier-2 fetches this and matches by keyword.
    authority_index_url="https://www.ages.at/mensch/produktwarnungen-produktrueckrufe",

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        # PRIMARY — German-language aggregator with daily AT/CH/LU/BE/NL coverage
        RssSource("produktwarnung.eu", [
            "https://www.produktwarnung.eu/feed",
            "https://www.produktwarnung.eu/rubrik/rueckrufe-in-nachbarstaaten/feed",
            "https://www.produktwarnung.eu/rubrik/produktrueckrufe-und-verbraucherwarnungen/feed",
        ]),
        # SECONDARY — Austrian major dailies (low signal, occasional big recall)
        RssSource("orf.at", [
            "https://rss.orf.at/news.xml",
        ]),
        RssSource("derstandard.at", [
            "https://www.derstandard.at/rss",
        ]),
        RssSource("kleinezeitung.at", [
            "https://www.kleinezeitung.at/rss",
        ]),
    ],
    google_news_domains=[
        "produktwarnung.eu",   # primary
        "orf.at", "derstandard.at", "krone.at", "kleinezeitung.at",
        # NOTE: removed tt.com, news.at, heute.at, oe24.at, sn.at — they
        # returned 100+ noise results per query in Batch 2 run.
    ],
    # TIGHTENED — precise compound German phrases instead of broad "Warnung"
    google_news_keywords=[
        '"Lebensmittel zurückgerufen"',
        '"Produktrückruf Lebensmittel"',
        '"AGES Warnung Lebensmittel"',
        '"Listerien" Österreich Rückruf',
        '"Salmonellen" Österreich Rückruf',
    ],

    bulk_index_queries=[
        "site:ages.at Rückruf 2026",
        "site:ages.at Rückruf 2025",
        "site:ages.at Lebensmittelwarnung",
        "site:ages.at Warnung Listerien OR Salmonellen",
        "site:ages.at Aflatoxin OR Allergen",
    ],

    language_name="German",
    language_code="de",
    brand_handling_note=(
        "Austrian brands are typically Latin script. Preserve casing and "
        "umlauts (e.g. 'Spar Österreich', 'Hofer', 'Billa', 'Penny', "
        "'Berglandmilch')."
    ),

    # TIGHTENED — require recall-action verb, not just "Warnung"
    recall_signal_terms=[
        "rückruf", "rueckruf", "rückgerufen", "rueckgerufen",
        "rücknahme", "ruecknahme", "zurückgerufen", "zurueckgerufen",
        "verbraucherwarnung", "lebensmittelwarnung",
        "produktrückruf", "produktrueckruf",
        "ages.at", "ages warnung",
        "nicht verzehren", "nicht konsumieren",
        # REMOVED: "warnung vor", "warnt vor" — too generic, matched Putin/heat/scam
    ],

    timezone="Europe/Vienna",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(AUSTRIA)
