"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Poland (GIS — Główny Inspektorat Sanitarny / Chief
                Sanitary Inspectorate)

Polish food-recall regime:
  - GIS at gis.gov.pl publishes "Ostrzeżenia" (warnings/alerts).
  - Also see gov.pl/web/gis for the official portal.
  - Major retailers (Biedronka, Lidl PL, Auchan PL) publish their own.
  - Volume: ~100-150 recalls/year.
  - Vocabulary:
      "wycofanie"  = withdrawal/recall
      "wycofuje"   = recalls (verb)
      "ostrzeżenie" = warning
      "alert żywnościowy" = food alert
      "nie spożywać" = do not consume
      "alergen" = allergen
"""

from .base import CountryConfig, RssSource, register


POLAND = CountryConfig(
    code="pl",
    name_en="Poland",
    name_local="Polska",

    authority_short="GIS",
    authority_full="Główny Inspektorat Sanitarny",
    authority_domain="gis.gov.pl",
    authority_item_url_regex=r"(ostrzezenie|wycofanie|aktualnosci|news|zywnosc)",

    rss_sources=[
        RssSource("tvn24.pl", [
            "https://tvn24.pl/najnowsze.xml",
            "https://tvn24.pl/biznes/najnowsze.xml",
        ]),
        RssSource("wp.pl", [
            "https://wiadomosci.wp.pl/rss.xml",
        ]),
        RssSource("onet.pl", [
            "https://wiadomosci.onet.pl/rss",
        ]),
        RssSource("gazeta.pl", [
            "https://wiadomosci.gazeta.pl/pub/rss/wiadomosci.xml",
        ]),
        RssSource("rmf24.pl", [
            "https://www.rmf24.pl/fakty/feed",
        ]),
        RssSource("interia.pl", [
            "https://fakty.interia.pl/feed",
        ]),
    ],
    google_news_domains=[
        "tvn24.pl", "wp.pl", "onet.pl", "gazeta.pl", "rmf24.pl",
        "interia.pl", "rp.pl", "wyborcza.pl", "money.pl", "polsatnews.pl",
    ],
    google_news_keywords=[
        "GIS wycofanie żywności",
        "ostrzeżenie żywnościowe Salmonella",
        "wycofanie produktu Listeria",
        "alert żywnościowy aflatoksyna",
        "wycofanie alergen niezadeklarowany",
    ],

    bulk_index_queries=[
        "site:gis.gov.pl ostrzeżenie 2026",
        "site:gis.gov.pl wycofanie 2026",
        "site:gis.gov.pl Salmonella OR Listeria",
        "site:gis.gov.pl alergen żywność",
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
    ],

    timezone="Europe/Warsaw",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(POLAND)
