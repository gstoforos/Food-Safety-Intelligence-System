"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Belgium (FAVV/AFSCA — Federal Agency for the Safety of
                the Food Chain)

Belgian food-recall regime:
  - BILINGUAL: Flemish/Dutch (NL) + French (FR). Brussels region uses both.
  - FAVV (Dutch) / AFSCA (French) at favv-afsca.be publishes alerts.
  - Recall vocabulary spans 2 languages — we union them.
  - Volume: ~80-120 recalls/year.
"""

from .base import CountryConfig, RssSource, register


BELGIUM = CountryConfig(
    code="be",
    name_en="Belgium",
    name_local="België / Belgique",

    authority_short="FAVV-AFSCA",
    authority_full="Federaal Agentschap voor de Veiligheid van de Voedselketen",
    authority_domain="favv-afsca.be",
    authority_item_url_regex=r"(news|terugroep|rappel|alerte|persbericht|warning)",

    # ── News sources (bilingual NL + FR) ────────────────────────────────────
    rss_sources=[
        # Dutch-speaking (Flemish)
        RssSource("hln.be", [
            "https://www.hln.be/home/rss.xml",
        ]),
        RssSource("demorgen.be", [
            "https://www.demorgen.be/rss.xml",
        ]),
        RssSource("vrt.be", [
            "https://www.vrt.be/vrtnws/nl.rss.articles.xml",
        ]),
        RssSource("nieuwsblad.be", [
            "https://www.nieuwsblad.be/rss/section/55178e67-15a8-4ddd-a3d8-bfe5708f8932",
        ]),
        RssSource("standaard.be", [
            "https://www.standaard.be/rss",
        ]),
        # French-speaking (Wallonia + Brussels)
        RssSource("lesoir.be", [
            "https://www.lesoir.be/rss",
        ]),
        RssSource("rtbf.be", [
            "https://www.rtbf.be/info/rss",
        ]),
        RssSource("lalibre.be", [
            "https://www.lalibre.be/arc/outboundfeeds/rss/?outputType=xml",
        ]),
        RssSource("dhnet.be", [
            "https://www.dhnet.be/arc/outboundfeeds/rss/?outputType=xml",
        ]),
    ],
    google_news_domains=[
        "hln.be", "demorgen.be", "vrt.be", "nieuwsblad.be", "standaard.be",
        "lesoir.be", "rtbf.be", "lalibre.be", "dhnet.be", "lavenir.net",
    ],
    google_news_keywords=[
        # Dutch
        "FAVV terugroep voedsel",
        "voedselveiligheid Salmonella",
        "terugroeping voedsel Listeria",
        # French
        "AFSCA rappel alimentaire",
        "rappel produit Salmonella",
        "alerte alimentaire Listeria",
    ],

    bulk_index_queries=[
        "site:favv-afsca.be terugroep 2026",
        "site:favv-afsca.be rappel 2026",
        "site:favv-afsca.be Salmonella OR Listeria",
        "site:favv-afsca.be allergeen OR allergène",
        "site:favv-afsca.be persbericht communiqué",
    ],

    language_name="Dutch or French (Belgian bilingual context)",
    language_code="nl",
    brand_handling_note=(
        "Belgian brands span 2 languages. Preserve original casing "
        "(e.g. 'Delhaize', 'Colruyt', 'Carrefour Belgium', 'Lidl', 'Aldi')."
    ),

    # UNION of NL + FR recall vocabularies
    recall_signal_terms=[
        # Dutch
        "terugroep", "terugroeping", "teruggeroepen",
        "voedselveiligheid", "voedselwaarschuwing",
        "niet consumeren", "niet verbruiken",
        "favv",
        # French
        "rappel", "rappelé", "rappelée", "rappelés", "rappelées",
        "retrait", "retiré", "alerte alimentaire",
        "ne pas consommer", "ne consommez pas",
        "afsca",
    ],

    timezone="Europe/Brussels",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(BELGIUM)
