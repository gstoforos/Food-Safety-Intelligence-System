"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Germany (BVL / lebensmittelwarnung.de)

German food-recall regime:
  - lebensmittelwarnung.de is the official BVL portal where all 16 federal
    states (Bundesländer) plus the BVL publish food recalls.
  - Germany has THE highest recall publication volume in Europe — ~270/year
    via BVL, plus hundreds more via direct manufacturer notices.
  - Local terminology:
      "Rückruf"       = recall (the central German recall word)
      "Rücknahme"     = withdrawal
      "Warnung"       = warning
      "Verbraucher"   = consumer
      "Lebensmittel"  = food / foodstuff
      "Allergen"      = allergen (also "Allergene")
"""

from .base import CountryConfig, RssSource, register


GERMANY = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="de",
    name_en="Germany",
    name_local="Deutschland",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="BVL",
    authority_full="Bundesamt für Verbraucherschutz und Lebensmittelsicherheit",
    authority_domain="lebensmittelwarnung.de",
    authority_item_url_regex=r"(warnung|meldung|rueckruf|lebensmittel)",

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        # AGGREGATORS — highest recall density
        RssSource("produktwarnung.eu", [
            "https://www.produktwarnung.eu/feed",
            "https://www.produktwarnung.eu/rubrik/produktrueckrufe-und-verbraucherwarnungen/feed",
        ]),
        RssSource("news.de", [
            "https://www.news.de/wirtschaft/rss",
        ]),
        # MAJOR DAILIES
        RssSource("spiegel.de", [
            "https://www.spiegel.de/schlagzeilen/index.rss",
            "https://www.spiegel.de/wirtschaft/index.rss",
        ]),
        RssSource("faz.net", [
            "https://www.faz.net/rss/aktuell/",
        ]),
        RssSource("sueddeutsche.de", [
            "https://rss.sueddeutsche.de/rss/Topthemen",
        ]),
        RssSource("welt.de", [
            "https://www.welt.de/feeds/topnews.rss",
        ]),
        RssSource("focus.de", [
            "https://rss.focus.de/fol/XML/rss_folnews.xml",
        ]),
        RssSource("zeit.de", [
            "https://newsfeed.zeit.de/index",
        ]),
    ],
    google_news_domains=[
        "produktwarnung.eu", "news.de",
        "spiegel.de", "faz.net", "sueddeutsche.de", "welt.de",
        "focus.de", "zeit.de", "tagesspiegel.de", "n-tv.de",
        "tagesschau.de", "bild.de",
    ],
    google_news_keywords=[
        "Lebensmittel Rückruf",
        "Rückruf Salmonellen",
        "Rückruf Listerien",
        "Lebensmittelwarnung Aflatoxin",
        "Verbraucherwarnung Lebensmittel",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:lebensmittelwarnung.de Rückruf 2026",
        "site:lebensmittelwarnung.de Rückruf 2025",
        "site:lebensmittelwarnung.de Listerien OR Salmonellen",
        "site:lebensmittelwarnung.de Allergen Warnung",
        "site:lebensmittelwarnung.de Lebensmittel Meldung",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="German",
    language_code="de",
    brand_handling_note=(
        "German brands are typically written in Latin script. "
        "Preserve original casing including umlauts (e.g. 'Müller', 'Edeka', "
        "'Rewe', 'Lidl', 'Aldi Süd', 'Kaufland', 'Rossmann')."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "rückruf", "rueckruf", "rückgerufen", "rueckgerufen",
        "rücknahme", "ruecknahme", "zurückgerufen", "zurueckgerufen",
        "verbraucherwarnung", "lebensmittelwarnung",
        "warnung vor", "warnt vor",
        "bvl", "lebensmittelwarnung.de",
        "nicht verzehren", "nicht konsumieren",
        "allergen warnung", "allergene warnung",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    timezone="Europe/Berlin",
    run_local_hour=21,
    # Berlin: CEST=UTC+2 (summer), CET=UTC+1 (winter)
    cron_utc_offsets=(19, 20),
)


register(GERMANY)
