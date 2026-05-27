"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Sweden (Livsmedelsverket — National Food Agency)

Swedish food-recall regime:
  - Livsmedelsverket at livsmedelsverket.se/om-oss/press/aterkallanden
    publishes the official recall list.
  - Råd & Rön (radron.se/aktuella-aterkallelser) is the dedicated
    consumer-facing recall aggregator — primary source for our pipeline.
  - Volume: ~50-100 recalls/year. Notable May 2026: Listeria outbreak
    in cold-smoked salmon (3 deaths nationally).
  - Vocabulary:
      "återkallar"     = recalls (verb)
      "återkallelse"   = recall (noun)
      "återkallanden"  = recalls (plural noun)
      "dragits tillbaka" = withdrawn
      "varning"        = warning
      "ät inte"        = do not eat
"""

from .base import CountryConfig, RssSource, register


SWEDEN = CountryConfig(
    code="se",
    name_en="Sweden",
    name_local="Sverige",

    authority_short="Livsmedelsverket",
    authority_full="Livsmedelsverket (National Food Agency)",
    authority_domain="livsmedelsverket.se",
    authority_item_url_regex=r"(aterkall|press|nyheter|varning)",

    rss_sources=[
        # PRIMARY — Råd & Rön consumer aggregator (dedicated recall page)
        RssSource("radron.se", [
            "https://www.radron.se/rss",
            "https://www.radron.se/feed/",
        ]),
        # PRIMARY — Livsmedelsverket official press feed
        RssSource("livsmedelsverket.se", [
            "https://www.livsmedelsverket.se/om-oss/press/aterkallanden/rss",
            "https://www.livsmedelsverket.se/rss",
        ]),
        # SECONDARY — major Swedish dailies
        RssSource("aftonbladet.se", [
            "https://rss.aftonbladet.se/rss2/small/pages/sections/senastenytt/",
        ]),
        RssSource("expressen.se", [
            "https://feeds.expressen.se/nyheter/",
        ]),
        RssSource("svt.se", [
            "https://www.svt.se/rss.xml",
        ]),
        RssSource("dn.se", [
            "https://www.dn.se/rss/",
        ]),
        RssSource("svd.se", [
            "https://www.svd.se/?service=rss",
        ]),
    ],
    google_news_domains=[
        "radron.se",                  # primary
        "livsmedelsverket.se",        # primary
        "aftonbladet.se", "expressen.se", "svt.se", "dn.se", "svd.se",
        "di.se", "gp.se", "sydsvenskan.se",
    ],
    google_news_keywords=[
        '"Livsmedelsverket" återkallar',
        '"återkallar" Salmonella',
        '"återkallar" Listeria',
        '"återkallelse" livsmedel',
        '"ICA" återkallar',
        '"Coop" återkallar',
        '"Lidl" återkallar Sverige',
    ],

    bulk_index_queries=[
        "site:livsmedelsverket.se återkallande 2026",
        "site:livsmedelsverket.se Salmonella OR Listeria",
        "site:radron.se livsmedel återkallelse",
        "site:livsmedelsverket.se allergen ej deklarerad",
    ],

    language_name="Swedish",
    language_code="sv",
    brand_handling_note=(
        "Swedish brands often use diacritics (å, ä, ö). Preserve original "
        "casing. Examples: 'ICA', 'Coop Sverige', 'Lidl Sverige', 'Axfood', "
        "'Hemköp', 'Willys', 'Arvid Nordquist'."
    ),

    recall_signal_terms=[
        "återkallar", "återkallat", "återkallelse", "återkallanden",
        "aterkallar", "aterkallat", "aterkallelse", "aterkallanden",
        "dragits tillbaka", "drar tillbaka", "tillbakadragande",
        "livsmedelsvarning", "matvarning",
        "ej äta", "ät inte", "at inte",
        "livsmedelsverket",
        # Pathogen-name signals (Swedish news names them in titles)
        "salmonella", "listeria", "listeria monocytogenes",
        "ej deklarerad", "ej deklarerade", "odeklarerad",
    ],

    timezone="Europe/Stockholm",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(SWEDEN)
