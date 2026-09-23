"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Hungary (NÉBIH — Nemzeti Élelmiszerlánc-biztonsági Hivatal /
                National Food Chain Safety Office)

Hungarian food-recall regime:
  - NÉBIH at nebih.gov.hu publishes alerts ("riasztások", "visszahívások").
  - Hungarian is non-Indo-European (Uralic family) — completely separate
    vocabulary from other EU languages.
  - Volume: ~40-80 recalls/year.
  - Vocabulary:
      "visszahívás"     = recall (the central Hungarian recall word)
      "visszahívja"     = recalls (verb)
      "kivonás"         = withdrawal
      "élelmiszer riasztás" = food alert
      "ne fogyasszák"   = do not consume
      "allergén"        = allergen
      "nem jelölt"      = not declared
"""

from .base import CountryConfig, RssSource, register


HUNGARY = CountryConfig(
    code="hu",
    name_en="Hungary",
    name_local="Magyarország",

    authority_short="NÉBIH",
    authority_full="Nemzeti Élelmiszerlánc-biztonsági Hivatal",
    authority_domain="nebih.gov.hu",
    # NÉBIH recalls live on portal.nebih.gov.hu (host ends with .nebih.gov.hu,
    # so the authority_domain gate matches). Each press release is its own HTML
    # page: portal.nebih.gov.hu/-/<slug> (e.g. /-/gyorsfagyasztott-zoldsegeket-
    # hiv-vissza-a-nebih). The index is the searchable termékvisszahívás page.
    # HOST-OPTIONAL PREFIX — see tests/test_country_config_conformance.py
    # ::test_the_regex_matches_both_forms_the_pipeline_uses. This regex is
    # applied to TWO different strings: the full URL (authority_url_finder,
    # extractor) and just "path?query" with the netloc stripped
    # (search_verifier, when it filters the bulk index). A regex naming the
    # host therefore matches at the first site and silently fails at the
    # second, which drops every bulk-index hit as a portal page. The
    # "^(?:https?://[^/]+)?" prefix — the idiom gh.py and za.py already
    # used — matches both.
    authority_item_url_regex=r"^(?:https?://[^/]+)?/-/",
    authority_index_url="https://portal.nebih.gov.hu/termekvisszahivas",

    rss_sources=[
        RssSource("index.hu", [
            "https://index.hu/24ora/rss/",
            "https://index.hu/gazdasag/rss/",
        ]),
        RssSource("hvg.hu", [
            "https://hvg.hu/rss",
        ]),
        RssSource("telex.hu", [
            "https://telex.hu/rss",
        ]),
        RssSource("24.hu", [
            "https://24.hu/feed/",
        ]),
        RssSource("blikk.hu", [
            "https://www.blikk.hu/rss",
        ]),
        RssSource("magyarnemzet.hu", [
            "https://magyarnemzet.hu/feed",
        ]),
        RssSource("portfolio.hu", [
            "https://www.portfolio.hu/rss/all.xml",
        ]),
    ],
    google_news_domains=[
        "index.hu", "hvg.hu", "telex.hu", "24.hu", "blikk.hu",
        "magyarnemzet.hu", "portfolio.hu", "origo.hu", "444.hu", "nlc.hu",
    ],
    google_news_keywords=[
        "NÉBIH visszahívás élelmiszer",
        "élelmiszer riasztás Salmonella",
        "visszahívás Listeria",
        "élelmiszer kivonás",
        "allergén nem jelölt",
    ],

    bulk_index_queries=[
        "site:nebih.gov.hu visszahívás 2026",
        "site:nebih.gov.hu riasztás 2026",
        "site:nebih.gov.hu Salmonella OR Listeria",
        "site:nebih.gov.hu allergén",
        "site:portal.nebih.gov.hu élelmiszer",
    ],

    language_name="Hungarian",
    language_code="hu",
    brand_handling_note=(
        "Hungarian brands often use accented characters (á, é, í, ó, ö, ő, "
        "ú, ü, ű). Preserve original casing. Examples: 'Tesco Magyarország', "
        "'Lidl Magyarország', 'Auchan Magyarország', 'Spar Magyarország', "
        "'CBA', 'Pénny Market'."
    ),

    recall_signal_terms=[
        "visszahívás", "visszahivas", "visszahívja", "visszahívták",
        "kivonás", "kivonas", "kivonja",
        "élelmiszer riasztás", "elelmiszer riasztas",
        "riasztás", "riasztas",
        "nébih", "nebih",
        "ne fogyasszák", "ne fogyasszak",
        "allergén nem jelölt", "allergen nem jelolt",
        "nem deklarált", "nem deklaralt",
    ],

    timezone="Europe/Budapest",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(HUNGARY)
