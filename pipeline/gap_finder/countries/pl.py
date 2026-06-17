"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Poland (GIS — Główny Inspektorat Sanitarny)

REVISION (Batch 2.2): First Poland deployment caught 10 candidates but
all titles were vague ("Groźne bakterie w produkcie rybnym", "Skażona
herbatka") — Polish general media doesn't name pathogens in headlines.

Fixed by adding Polish FOOD-INDUSTRY portals as PRIMARY sources:
  - portalspozywczy.pl — food-industry portal, publishes recalls with
    specific pathogen names ("Salmonella w produkcie ziołowym",
    "Listeria monocytogenes w metce cebulowej")
  - natemat.pl — consumer-facing news with named pathogens
  - motywatordietetyczny.pl/gis — dedicated GIS alert aggregator page
  - Regional press groups (pomorska, gloswielkopolski) — monthly recall lists
"""

from .base import CountryConfig, RssSource, register


POLAND = CountryConfig(
    code="pl",
    name_en="Poland",
    name_local="Polska",

    authority_short="GIS",
    authority_full="Główny Inspektorat Sanitarny",
    authority_domain="gov.pl",
    # GIS publishes each recall as its own page on the gov.pl portal, e.g.
    #   /web/gis/ostrzezenie-publiczne-dotyczace-zywnosci-...
    #   /web/gis/aktualizacja-ostrzezenia-publicznego-dotyczacego-zywnosci-...
    # This regex is the Stage-3 GATE: the Pending URL must be a gov.pl GIS page.
    authority_item_url_regex=r"/web/gis/(ostrzezenie|aktualizacja-ostrzezenia)",

    # ── Aggregator front-end: oalert.pl ─────────────────────────────────────
    # gov.pl's own index/recall pages return mostly navigation chrome to a
    # scraper (tiny bodies) and rate-limit rapid sequential fetches (403). The
    # oalert.pl aggregator mirrors GIS/GIF/UOKiK/RASFF as clean, English,
    # per-item pages, and EACH item links to the official gov.pl GIS report
    # ("Source: GIS — view the official alert"). We therefore MATCH on oalert.pl
    # (rich rows: product + source + severity + date + shop + hazard), then
    # FOLLOW THROUGH to the gov.pl link so the stored Pending URL stays the
    # official authority page (authority-pure) and the gate above still applies.
    #
    # English list:  https://oalert.pl/wycofane-produkty?lang=en
    # Item pages:    https://oalert.pl/wycofane-produkty/<slug>/en
    index_domain="oalert.pl",
    index_item_url_regex=r"/wycofane-produkty/[a-z0-9][a-z0-9\-]+",
    authority_index_url="https://oalert.pl/wycofane-produkty?lang=en",

    rss_sources=[
        # PRIMARY — Polish food-industry portal (names pathogens in titles)
        RssSource("portalspozywczy.pl", [
            "https://www.portalspozywczy.pl/rss/wiadomosci",
            "https://www.portalspozywczy.pl/rss/wszystko",
            "https://www.portalspozywczy.pl/rss/technologie/wiadomosci",
        ]),
        # PRIMARY — Polish recall aggregators with full pathogen-named titles
        RssSource("natemat.pl", [
            "https://natemat.pl/rss",
        ]),
        # SECONDARY — Polonia Press / regional papers (monthly recall lists)
        RssSource("pomorska.pl", [
            "https://pomorska.pl/rss",
        ]),
        # GENERAL — major dailies (low signal, occasional big story)
        RssSource("rmf24.pl", [
            "https://www.rmf24.pl/fakty/feed",
        ]),
        RssSource("interia.pl", [
            "https://fakty.interia.pl/feed",
        ]),
        RssSource("gazeta.pl", [
            "https://wiadomosci.gazeta.pl/pub/rss/wiadomosci.xml",
        ]),
    ],
    google_news_domains=[
        # PRIMARY food/recall portals
        "portalspozywczy.pl",
        "natemat.pl",
        "motywatordietetyczny.pl",
        # Regional press
        "pomorska.pl", "gloswielkopolski.pl",
        # General
        "tvn24.pl", "wp.pl", "onet.pl", "gazeta.pl", "rmf24.pl",
        "interia.pl", "polsatnews.pl",
    ],
    # TIGHTENED — directly target pathogen-named headlines
    google_news_keywords=[
        '"Salmonella" GIS wycofanie',
        '"Listeria" GIS ostrzeżenie',
        '"Listeria monocytogenes" wycofanie',
        '"aflatoksyny" wycofanie produktu',
        '"tlenek etylenu" GIS',
        '"E. coli" wycofanie żywności',
        "GIS ostrzeżenie publiczne żywność",
    ],

    bulk_index_queries=[
        "site:gis.gov.pl ostrzeżenie 2026",
        "site:gis.gov.pl Salmonella 2026",
        "site:gis.gov.pl Listeria 2026",
        "site:gis.gov.pl aflatoksyny",
        "site:gov.pl/web/gis ostrzeżenie żywność",
    ],

    language_name="Polish",
    language_code="pl",
    brand_handling_note=(
        "Polish brands often use diacritics. Preserve original characters "
        "(ą, ć, ę, ł, ń, ó, ś, ź, ż). Examples: 'Biedronka', 'Żabka', "
        "'Lidl Polska', 'Auchan Polska', 'Kaufland Polska'."
    ),

    recall_signal_terms=[
        "wycofanie", "wycofany", "wycofania", "wycofuje", "wycofano",
        "ostrzeżenie", "ostrzeżenia", "ostrzega",
        "alert żywnościowy", "alert sanitarny",
        "gis",
        "nie spożywać", "nie spozywac",
        "alergen niezadeklarowany", "alergen nie zadeklarowany",
        # Names-as-signals: when a Polish title contains these, it's almost
        # always a recall context (boost catch rate for pathogen-named items)
        "salmonella", "salmonelli", "salmonellą",
        "listeria", "listerii", "listerią", "listeria monocytogenes",
        "aflatoksyny", "aflatoksyna",
        "tlenek etylenu",
    ],

    timezone="Europe/Warsaw",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(POLAND)
