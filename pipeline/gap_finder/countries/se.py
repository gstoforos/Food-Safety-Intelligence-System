"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Sweden (Livsmedelsverket — National Food Agency)

REVISION (Batch 3.1 hotfix): First Sweden deployment caught 1 Pending
(Lidl Dumplings Salmonella) but all RSS feeds 404'd. Sweden's recall
volume is real (~50-100/year) but our sources were wrong.

Fixed by:
  1. Removing 404'd RSS URLs (radron.se/rss, livsmedelsverket.se/rss)
  2. Adding livsmedelsforetagen.se (Swedish food industry association)
  3. Adding atl.nu (Swedish agricultural news with food coverage)
  4. Keeping Google News which actually worked (12 candidates → 1 real)
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
        # NOTE: radron.se and livsmedelsverket.se have no public RSS.
        # Removed. Rely on Google News for those domains instead.
        # SECONDARY — major Swedish dailies (verified working endpoints)
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
        # FOOD INDUSTRY
        RssSource("atl.nu", [
            "https://www.atl.nu/rss",
        ]),
    ],
    google_news_domains=[
        "radron.se",                    # consumer recall aggregator
        "livsmedelsverket.se",          # official authority
        "aftonbladet.se", "expressen.se", "svt.se", "dn.se", "svd.se",
        "di.se", "gp.se", "sydsvenskan.se",
        "atl.nu", "livsmedelsforetagen.se",
    ],
    google_news_keywords=[
        '"Livsmedelsverket" återkallar',
        "återkallar Salmonella",
        "återkallar Listeria",
        "återkallelse livsmedel",
        '"ICA" återkallar',
        '"Coop" återkallar',
        '"Lidl" återkallar',
        '"Axfood" återkallar',
        "livsmedelsverket varnar",
    ],

    bulk_index_queries=[
        "site:livsmedelsverket.se återkallande 2026",
        "site:livsmedelsverket.se Salmonella OR Listeria",
        "site:radron.se livsmedel återkallelse",
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
        "salmonella", "listeria", "listeria monocytogenes",
        "ej deklarerad", "ej deklarerade", "odeklarerad",
    ],

    timezone="Europe/Stockholm",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(SWEDEN)
