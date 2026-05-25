"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Netherlands (NVWA — Nederlandse Voedsel- en Warenautoriteit)

Dutch food-recall regime:
  - NVWA at nvwa.nl publishes alerts ("waarschuwingen").
  - Major retailers (Albert Heijn, Jumbo, Lidl NL) publish their own recalls.
  - Volume: ~100-150 recalls/year.
  - Vocabulary:
      "terugroep"  = recall
      "voedselveiligheid" = food safety
      "waarschuwing" = warning
      "niet consumeren" = do not consume
"""

from .base import CountryConfig, RssSource, register


NETHERLANDS = CountryConfig(
    code="nl",
    name_en="Netherlands",
    name_local="Nederland",

    authority_short="NVWA",
    authority_full="Nederlandse Voedsel- en Warenautoriteit",
    authority_domain="nvwa.nl",
    authority_item_url_regex=r"(nieuws|waarschuwing|terugroep|onderwerpen)",

    rss_sources=[
        RssSource("nu.nl", [
            "https://www.nu.nl/rss/Algemeen",
            "https://www.nu.nl/rss/Economie",
        ]),
        RssSource("nos.nl", [
            "https://feeds.nos.nl/nosnieuwsalgemeen",
            "https://feeds.nos.nl/nosnieuwsbinnenland",
        ]),
        RssSource("rtlnieuws.nl", [
            "https://www.rtlnieuws.nl/service/rss/nederland/index.xml",
        ]),
        RssSource("telegraaf.nl", [
            "https://www.telegraaf.nl/rss",
        ]),
        RssSource("ad.nl", [
            "https://www.ad.nl/home/rss.xml",
        ]),
        RssSource("nrc.nl", [
            "https://www.nrc.nl/rss/",
        ]),
        RssSource("volkskrant.nl", [
            "https://www.volkskrant.nl/rss/full.xml",
        ]),
    ],
    google_news_domains=[
        "nu.nl", "nos.nl", "rtlnieuws.nl", "telegraaf.nl", "ad.nl",
        "nrc.nl", "volkskrant.nl", "trouw.nl", "parool.nl",
    ],
    google_news_keywords=[
        "NVWA terugroep voedsel",
        "voedselveiligheid Salmonella",
        "terugroep Listeria",
        "voedselwaarschuwing allergeen",
        "Albert Heijn terugroep",
    ],

    bulk_index_queries=[
        "site:nvwa.nl terugroep 2026",
        "site:nvwa.nl waarschuwing 2026",
        "site:nvwa.nl Salmonella OR Listeria",
        "site:nvwa.nl allergeen niet vermeld",
        "site:nvwa.nl voedselveiligheid",
    ],

    language_name="Dutch",
    language_code="nl",
    brand_handling_note=(
        "Dutch brands are typically Latin script. Preserve original casing "
        "(e.g. 'Albert Heijn', 'Jumbo', 'Lidl Nederland', 'PLUS', 'Aldi')."
    ),

    recall_signal_terms=[
        "terugroep", "terugroeping", "teruggeroepen", "teruggehaald",
        "voedselveiligheid", "voedselwaarschuwing",
        "waarschuwing voedsel",
        "niet consumeren", "niet verbruiken", "niet eten",
        "nvwa",
        "allergeen niet vermeld", "niet aangegeven",
    ],

    timezone="Europe/Amsterdam",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(NETHERLANDS)
