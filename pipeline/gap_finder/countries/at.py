"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Austria (AGES — Austrian Agency for Health and Food Safety)

Austrian food-recall regime:
  - AGES at ages.at publishes alerts ("Warnungen")
  - Recall vocabulary is identical to Germany (same language).
  - Lower volume than Germany (~50-80 recalls/year).
"""

from .base import CountryConfig, RssSource, register


AUSTRIA = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="at",
    name_en="Austria",
    name_local="Österreich",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="AGES",
    authority_full="Agentur für Gesundheit und Ernährungssicherheit",
    authority_domain="ages.at",
    authority_item_url_regex=r"(warnung|rueckruf|news|aktuelles|themen)",

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        RssSource("orf.at", [
            "https://rss.orf.at/news.xml",
        ]),
        RssSource("derstandard.at", [
            "https://www.derstandard.at/rss",
            "https://www.derstandard.at/rss/inland",
        ]),
        RssSource("kurier.at", [
            "https://kurier.at/xml/rss",
        ]),
        RssSource("krone.at", [
            "https://www.krone.at/feed/rss/nachrichten",
        ]),
        RssSource("kleinezeitung.at", [
            "https://www.kleinezeitung.at/rss",
        ]),
        RssSource("news.at", [
            "https://www.news.at/feeds/news.xml",
        ]),
    ],
    google_news_domains=[
        "orf.at", "derstandard.at", "kurier.at", "krone.at",
        "kleinezeitung.at", "news.at", "tt.com", "sn.at",
        "oe24.at", "heute.at",
    ],
    google_news_keywords=[
        "Lebensmittel Rückruf AGES",
        "Lebensmittelwarnung Österreich",
        "Rückruf Salmonellen",
        "Rückruf Listerien",
        "AGES Warnung",
    ],

    bulk_index_queries=[
        "site:ages.at Rückruf 2026",
        "site:ages.at Rückruf 2025",
        "site:ages.at Lebensmittelwarnung",
        "site:ages.at Warnung Listerien OR Salmonellen",
        "site:ages.at Aflatoxin OR Allergen",
    ],

    # ── LLM extraction ──────────────────────────────────────────────────────
    language_name="German",
    language_code="de",
    brand_handling_note=(
        "Austrian brands are typically Latin script. Preserve casing and "
        "umlauts (e.g. 'Spar Österreich', 'Hofer', 'Billa', 'Penny', "
        "'Berglandmilch')."
    ),

    recall_signal_terms=[
        "rückruf", "rueckruf", "rückgerufen", "rueckgerufen",
        "rücknahme", "ruecknahme", "zurückgerufen",
        "verbraucherwarnung", "lebensmittelwarnung",
        "warnung vor", "warnt vor",
        "ages", "ages.at",
        "nicht verzehren", "nicht konsumieren",
        "allergen warnung", "allergene warnung",
    ],

    timezone="Europe/Vienna",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(AUSTRIA)
