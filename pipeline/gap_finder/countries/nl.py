"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Netherlands (NVWA — Nederlandse Voedsel- en Warenautoriteit)

REVISION (Batch 2.1 hotfix): The first Netherlands deployment caught 0
candidates total — RSS feeds returned empty, Google News queries returned
empty. Diagnosis:
  - rtlnieuws.nl and volkskrant.nl RSS URLs 404'd
  - General Dutch news feeds (nu.nl/nos.nl) don't surface recall articles
    in their main feeds — recall coverage lives in dedicated sections
  - Google News queries "NVWA terugroep voedsel" too restrictive

Fixed by:
  1. Adding produktwarnung.eu Nachbarstaaten feed (covers Dutch recalls
     in German — our classifier handles it).
  2. Replacing broken RSS URLs with working ones.
  3. Adding foodlog.nl — Dutch food-news aggregator with recall coverage.
  4. Simpler Google News queries (single-word recall verbs).
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
        # PRIMARY — produktwarnung.eu also covers Dutch recalls (in German)
        RssSource("produktwarnung.eu", [
            "https://www.produktwarnung.eu/feed",
            "https://www.produktwarnung.eu/rubrik/rueckrufe-in-nachbarstaaten/feed",
        ]),
        # PRIMARY — Dutch food-news aggregator (recall-focused)
        RssSource("foodlog.nl", [
            "https://www.foodlog.nl/feed/",
        ]),
        # SECONDARY — major Dutch dailies (verified URLs)
        RssSource("nu.nl", [
            "https://www.nu.nl/rss/Algemeen",
        ]),
        RssSource("nos.nl", [
            "https://feeds.nos.nl/nosnieuwsalgemeen",
        ]),
        RssSource("telegraaf.nl", [
            "https://www.telegraaf.nl/rss",
        ]),
        RssSource("ad.nl", [
            "https://www.ad.nl/home/rss.xml",
        ]),
    ],
    google_news_domains=[
        "produktwarnung.eu",    # primary
        "foodlog.nl",           # primary
        "nu.nl", "nos.nl", "rtlnieuws.nl", "telegraaf.nl", "ad.nl",
        "nrc.nl", "volkskrant.nl",
    ],
    # SIMPLIFIED — single-word recall verbs catch more
    google_news_keywords=[
        "terugroep voedsel",
        "voedselveiligheid Listeria",
        "voedselveiligheid Salmonella",
        '"Albert Heijn" terugroep',
        '"Jumbo" terugroepactie',
        "NVWA waarschuwing",
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
        "terugroepactie",
        "voedselveiligheid", "voedselwaarschuwing",
        "waarschuwing voedsel",
        "niet consumeren", "niet verbruiken", "niet eten",
        "nvwa",
        "allergeen niet vermeld", "niet aangegeven",
        # German aggregator coverage of Dutch recalls
        "rückruf", "rueckruf", "produktrückruf",
    ],

    timezone="Europe/Amsterdam",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(NETHERLANDS)
