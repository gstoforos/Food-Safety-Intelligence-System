"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Finland (Ruokavirasto — Finnish Food Authority)

Finnish food-recall regime:
  - Ruokavirasto (formerly Evira) at ruokavirasto.fi publishes alerts
    ("takaisinvedot" / recalls).
  - Finnish is Finno-Ugric (Uralic family) — completely separate vocabulary
    from Indo-European languages. Compound words common.
  - Volume: ~30-50 recalls/year (small market).
  - Vocabulary:
      "takaisinveto"    = recall (noun)
      "vetää takaisin"  = recalls (verb)
      "varoitus"        = warning
      "elintarvike"     = food
      "älä syö"         = do not eat
      "allergeeni"      = allergen

Note: workflow runs at 21:00 Stockholm = 22:00 Helsinki. Acceptable for
daily aggregation; Finnish news sources are stable through evening.
"""

from .base import CountryConfig, RssSource, register


FINLAND = CountryConfig(
    code="fi",
    name_en="Finland",
    name_local="Suomi",

    authority_short="Ruokavirasto",
    authority_full="Ruokavirasto (Finnish Food Authority)",
    authority_domain="ruokavirasto.fi",
    authority_item_url_regex=r"(takaisinveto|varoitus|tiedote|uutiset)",

    rss_sources=[
        # Major Finnish news outlets
        RssSource("yle.fi", [
            "https://feeds.yle.fi/uutiset/v1/majorHeadlines/YLE_UUTISET.rss",
            "https://feeds.yle.fi/uutiset/v1/recent.rss?publisherIds=YLE_UUTISET",
        ]),
        RssSource("hs.fi", [
            "https://www.hs.fi/rss/tuoreimmat.xml",
        ]),
        RssSource("iltalehti.fi", [
            "https://www.iltalehti.fi/rss/uutiset.xml",
        ]),
        RssSource("is.fi", [
            "https://www.is.fi/rss/tuoreimmat.xml",
        ]),
        RssSource("mtvuutiset.fi", [
            "https://www.mtvuutiset.fi/api/feed/rss/uutiset_uusimmat",
        ]),
        RssSource("maaseuduntulevaisuus.fi", [
            "https://www.maaseuduntulevaisuus.fi/rss",
        ]),
    ],
    google_news_domains=[
        "ruokavirasto.fi",
        "yle.fi", "hs.fi", "iltalehti.fi", "is.fi", "mtvuutiset.fi",
        "kauppalehti.fi", "talouselama.fi", "maaseuduntulevaisuus.fi",
    ],
    google_news_keywords=[
        "Ruokavirasto takaisinveto",
        "elintarvike takaisinveto",
        "takaisinveto salmonella",
        "takaisinveto listeria",
        "elintarvikevaroitus",
        '"Lidl" takaisinveto Suomi',
        '"K-ruoka" takaisinveto',
        '"S-ryhmä" takaisinveto',
    ],

    bulk_index_queries=[
        "site:ruokavirasto.fi takaisinveto 2026",
        "site:ruokavirasto.fi Salmonella OR Listeria",
        "site:ruokavirasto.fi varoitus elintarvike",
    ],

    language_name="Finnish",
    language_code="fi",
    brand_handling_note=(
        "Finnish brands often use diacritics (ä, ö). Preserve original "
        "casing. Examples: 'K-ruoka', 'S-ryhmä', 'Prisma', 'Lidl Suomi', "
        "'Alepa', 'K-Citymarket', 'Valio', 'HKScan', 'Atria', 'Fazer'."
    ),

    recall_signal_terms=[
        "takaisinveto", "takaisinvedot", "takaisinvedetään",
        "vetää takaisin", "vetävät takaisin", "vedetty takaisin",
        "elintarvikevaroitus", "ruokavaroitus",
        "varoitus elintarvike",
        "älä syö", "ala syo", "ei saa syödä",
        "ruokavirasto",
        "salmonella", "listeria", "listeria monocytogenes",
        "ei ilmoitettu", "ilmoittamaton",
    ],

    timezone="Europe/Helsinki",
    run_local_hour=21,
    # Helsinki: EEST=UTC+3 (summer), EET=UTC+2 (winter)
    # But workflow runs at 21:00 Stockholm = 22:00 Helsinki
    # We still gate by Stockholm in the YAML, so these offsets are advisory
    cron_utc_offsets=(18, 19),
)


register(FINLAND)
