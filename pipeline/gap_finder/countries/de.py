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
    # REWRITTEN 2026-09-24 against the 18 real BVL URLs in the register.
    #
    # It read r"(warnung|meldung|rueckruf|lebensmittel)" — a word match on
    # the whole URL. The HOST is lebensmittelwarnung.de, so "lebensmittel"
    # and "warnung" are both in the hostname and EVERY page on the site
    # passed the gate, the bare site root included. authority_url_finder
    # matches the full URL, so that is where it bit.
    #
    # This is the same defect Switzerland had, found the same way: BLV's
    # regex matched "rappel" in /fr/mises-en-garde-et-rappels-aliments and
    # a landing page reached Recalls with Company = Brand = the page title
    # and Product = "aliments". Germany has been dark since 2026-06-14 so it
    # has not had the chance to repeat it — but the push fix landed today and
    # East EU committed for the first time, so Germany will start publishing
    # again and the hole would open with it.
    #
    # The five shapes BVL actually uses, all present in the register
    # (14 + 1 + 1 + 1 + 1 = the 18 German rows the register holds today):
    #   /___lebensmittelwarnung.de/Meldungen/<YYYY>/<MM>_<Month>/<slug>/...  (14)
    #   /bvl-lmw-de/detail/lebensmittel/<id>                                 (1)
    #   /bvl-blv-fcm-de/detail/lebensmittel/<id>                             (1)
    #   /bvl-blv/de/lebensmittel/rueckruf_<slug>                             (1)
    #   /bvl-blv-report/BVL_BLV_Report_<YYYY>_<MM>_<DD>_<product>_<hazard>.pdf (1)
    # Each carries an IDENTIFIER — a dated Meldungen path, a numeric detail
    # id, or a rueckruf_ slug. The listing pages and the site root carry
    # none, which is what now separates them.
    #
    # Host-optional prefix because the regex is matched against the full URL
    # in authority_url_finder and against path-only in search_verifier.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:"
        r"___lebensmittelwarnung\.de/Meldungen/\d{4}/\d{2}_[A-Za-z]+/[^/]+"
        r"|bvl-[a-z-]+/detail/lebensmittel/\d+"
        r"|bvl-blv/de/lebensmittel/rueckruf_[^/]+"
        # A joint BVL/BLV report PDF, one per recall — the register holds
        # "..._2026_07_24_Freshona_Bio_Beerenmischung_Noroviren_erweitert.pdf",
        # which names a date, a product and a hazard. A PDF is accepted here
        # for the same reason Hong Kong's Food Incident Posts are: the
        # authority-URL guarantee is about WHO published it and whether it
        # addresses one incident, not about the file format.
        r"|bvl-blv-report/BVL_BLV_Report_\d{4}_\d{2}_\d{2}_[^/]+"
        r")"
    ),


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
