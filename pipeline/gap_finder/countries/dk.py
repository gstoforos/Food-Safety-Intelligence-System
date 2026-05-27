"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Denmark (Fødevarestyrelsen)

REVISION (Batch 3.1 hotfix): First Denmark deployment returned 0 candidates
TOTAL. Diagnosis:
  - foedevarestyrelsen.dk has no public RSS feed (verified)
  - Quoted Google News queries too restrictive — all returned 0
  - Wrong/missing aggregator sources

Fixed by:
  1. Adding netavisengrindsted.dk/tag/tilbagekaldte-foedevarer (Danish
     news aggregator with dedicated recall tag page)
  2. Adding via.ritzau.dk pressemeddelelse (Danish press release wire,
     equivalent of NTB for Norway)
  3. Simpler unquoted Google News queries
  4. Adding meandmet.dk and tvsyd.dk (regional + health sites covering recalls)
"""

from .base import CountryConfig, RssSource, register


DENMARK = CountryConfig(
    code="dk",
    name_en="Denmark",
    name_local="Danmark",

    authority_short="Fødevarestyrelsen",
    authority_full="Fødevarestyrelsen (Danish Veterinary and Food Administration)",
    authority_domain="foedevarestyrelsen.dk",
    authority_item_url_regex=r"(tilbagekald|advarsel|nyheder|press)",

    rss_sources=[
        # PRIMARY — Danish news aggregator with dedicated recall tag
        RssSource("netavisengrindsted.dk", [
            "https://www.netavisengrindsted.dk/feed/",
            "https://www.netavisengrindsted.dk/tag/tilbagekaldte-foedevarer/feed/",
        ]),
        # PRIMARY — Danish press release wire
        RssSource("via.ritzau.dk", [
            "https://via.ritzau.dk/rss/",
        ]),
        # SECONDARY — major Danish news
        RssSource("dr.dk", [
            "https://www.dr.dk/nyheder/service/feeds/allenyheder",
        ]),
        RssSource("bt.dk", [
            "https://www.bt.dk/bt/seneste/rss",
        ]),
        RssSource("jyllands-posten.dk", [
            "https://jyllands-posten.dk/?service=rssfeed",
        ]),
    ],
    google_news_domains=[
        "foedevarestyrelsen.dk",
        "netavisengrindsted.dk",        # primary
        "via.ritzau.dk",                # primary
        "foedevarewatch.dk", "fodevarewatch.dk",
        "dr.dk", "ekstrabladet.dk", "bt.dk", "jyllands-posten.dk",
        "berlingske.dk", "politiken.dk", "tv2.dk", "tvsyd.dk", "borsen.dk",
        "meandmet.dk",
    ],
    # SIMPLIFIED — no quotes; let Google News do partial matching
    google_news_keywords=[
        "tilbagekalder fødevare Salmonella",
        "tilbagekaldelse Listeria Danmark",
        "Fødevarestyrelsen advarsel",
        "Coop tilbagekalder",
        "Lidl Danmark tilbagekalder",
        "Salling Group tilbagekalder",
        "listeria tilbagekaldelse",
        "salmonella tilbagekaldelse",
    ],

    bulk_index_queries=[
        "site:foedevarestyrelsen.dk tilbagekaldelse 2026",
        "site:foedevarestyrelsen.dk Salmonella OR Listeria",
        "site:foedevarestyrelsen.dk advarsel fødevare",
        "site:via.ritzau.dk tilbagekalder fødevare",
        "site:netavisengrindsted.dk tilbagekaldte foedevarer",
    ],

    language_name="Danish",
    language_code="da",
    brand_handling_note=(
        "Danish brands often use diacritics (æ, ø, å). Preserve original "
        "casing. Examples: 'Coop Danmark', 'Salling Group', 'Netto', "
        "'Fakta', 'Føtex', 'Bilka', 'Lidl Danmark', 'Aldi Danmark', "
        "'Danish Crown', 'Scanfish Danmark', 'Hilton Foods'."
    ),

    recall_signal_terms=[
        "tilbagekalder", "tilbagekaldelse", "tilbagekaldt", "tilbagekald",
        "tilbagekalde", "tilbagekaldte",
        "trækker tilbage", "trækkes tilbage", "tilbagetrækning",
        "trækker fra hylderne",
        "fødevareadvarsel", "fødevareadvarsler",
        "spis ikke", "spis ikke produktet",
        "fødevarestyrelsen",
        "advarsel fødevare",
        # Pathogen-name signals
        "salmonella", "listeria", "listeria monocytogenes",
        "ikke deklareret", "ikke angivet",
    ],

    timezone="Europe/Copenhagen",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(DENMARK)
