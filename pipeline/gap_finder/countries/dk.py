"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Denmark (Fødevarestyrelsen — Danish Veterinary and Food
                Administration)

Danish food-recall regime:
  - Fødevarestyrelsen at foedevarestyrelsen.dk publishes alerts and
    recalls ("tilbagekaldelser").
  - FødevareWatch (foedevarewatch.dk) is the Danish food-industry
    publication with strong recall coverage.
  - Volume: ~40-80 recalls/year.
  - Vocabulary:
      "tilbagekalder"  = recalls (verb)
      "tilbagekaldelse" = recall (noun)
      "trækker tilbage" = withdraws
      "spis ikke"      = do not eat
      "advarsel"       = warning
      "fødevarestyrelsen" = food authority
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
        # PRIMARY — Fødevarestyrelsen official RSS
        RssSource("foedevarestyrelsen.dk", [
            "https://www.foedevarestyrelsen.dk/nyheder.rss",
            "https://www.foedevarestyrelsen.dk/rss",
        ]),
        # PRIMARY — FødevareWatch (Danish food-industry publication)
        RssSource("foedevarewatch.dk", [
            "https://foedevarewatch.dk/rss",
            "https://foedevarewatch.dk/feed",
        ]),
        # SECONDARY — major Danish news
        RssSource("dr.dk", [
            "https://www.dr.dk/nyheder/service/feeds/allenyheder",
        ]),
        RssSource("ekstrabladet.dk", [
            "https://ekstrabladet.dk/rss/nyheder",
        ]),
        RssSource("bt.dk", [
            "https://www.bt.dk/bt/seneste/rss",
        ]),
        RssSource("jyllands-posten.dk", [
            "https://jyllands-posten.dk/?service=rssfeed",
        ]),
        RssSource("berlingske.dk", [
            "https://www.berlingske.dk/content/news.rss",
        ]),
    ],
    google_news_domains=[
        "foedevarestyrelsen.dk",        # primary
        "foedevarewatch.dk",            # primary
        "dr.dk", "ekstrabladet.dk", "bt.dk", "jyllands-posten.dk",
        "berlingske.dk", "politiken.dk", "tv2.dk", "borsen.dk",
    ],
    google_news_keywords=[
        '"Fødevarestyrelsen" tilbagekalder',
        '"tilbagekalder" Salmonella',
        '"tilbagekalder" Listeria',
        '"tilbagekaldelse" fødevare',
        '"Coop" tilbagekalder',
        '"Salling" tilbagekalder',
        '"Netto" tilbagekalder',
    ],

    bulk_index_queries=[
        "site:foedevarestyrelsen.dk tilbagekaldelse 2026",
        "site:foedevarestyrelsen.dk Salmonella OR Listeria",
        "site:foedevarestyrelsen.dk advarsel fødevare",
        "site:foedevarestyrelsen.dk allergen ikke deklareret",
    ],

    language_name="Danish",
    language_code="da",
    brand_handling_note=(
        "Danish brands often use diacritics (æ, ø, å). Preserve original "
        "casing. Examples: 'Coop Danmark', 'Salling Group', 'Netto', "
        "'Fakta', 'Føtex', 'Bilka', 'Lidl Danmark', 'Aldi Danmark'."
    ),

    recall_signal_terms=[
        "tilbagekalder", "tilbagekaldelse", "tilbagekaldt", "tilbagekald",
        "trækker tilbage", "trækkes tilbage", "tilbagetrækning",
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
