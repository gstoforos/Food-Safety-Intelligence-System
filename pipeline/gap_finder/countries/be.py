"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Belgium (FAVV/AFSCA — Federal Agency for the Safety of
                the Food Chain)

REVISION (Batch 2.1 hotfix): First Belgium deployment caught Nutrilon
babymelk recall but all variants got rule-rejected as 'unknown' because
the headlines say "mogelijk giftige stof" (vague: possibly toxic substance)
rather than naming the actual pathogen (cereulide / Bacillus cereus).

Fixed by:
  1. Adding produktwarnung.eu Nachbarstaaten — Belgian recalls in German
     with specific pathogen names that our classifier can match.
  2. Adding retaildetail.eu/retaildetail.be — Belgian retail news with
     food-recall coverage.
  3. Adding foodlog.nl (covers Flemish-speaking Belgian recalls too).
"""

from .base import CountryConfig, RssSource, register


BELGIUM = CountryConfig(
    code="be",
    name_en="Belgium",
    name_local="België / Belgique",

    authority_short="FAVV-AFSCA",
    authority_full="Federaal Agentschap voor de Veiligheid van de Voedselketen",
    authority_domain="favv-afsca.be",
    authority_item_url_regex=r"/(produits|producten)/(rappel|terugroep)",
    # FAVV-AFSCA recall index (verified 2026-06): lists every recall; each has
    # its own page /fr/produits/rappel-de-<company>. Tier-2 fetches this,
    # parses the rappel-de-* links, matches the recall by company keyword.
    authority_index_url="https://favv-afsca.be/fr/produits",

    rss_sources=[
        # PRIMARY — German aggregator covers Belgian recalls
        RssSource("produktwarnung.eu", [
            "https://www.produktwarnung.eu/feed",
            "https://www.produktwarnung.eu/rubrik/rueckrufe-in-nachbarstaaten/feed",
        ]),
        # PRIMARY — Belgian retail news (food-recall coverage)
        RssSource("retaildetail.eu", [
            "https://www.retaildetail.eu/feed/",
        ]),
        # Dutch-speaking (Flemish) — verified working
        RssSource("hln.be", [
            "https://www.hln.be/home/rss.xml",
        ]),
        RssSource("demorgen.be", [
            "https://www.demorgen.be/rss.xml",
        ]),
        RssSource("vrt.be", [
            "https://www.vrt.be/vrtnws/nl.rss.articles.xml",
        ]),
        RssSource("standaard.be", [
            "https://www.standaard.be/rss",
        ]),
        # French-speaking — verified working endpoints only
        RssSource("lalibre.be", [
            "https://www.lalibre.be/arc/outboundfeeds/rss/?outputType=xml",
        ]),
        RssSource("dhnet.be", [
            "https://www.dhnet.be/arc/outboundfeeds/rss/?outputType=xml",
        ]),
    ],
    google_news_domains=[
        "produktwarnung.eu",    # primary
        "retaildetail.eu",      # primary
        "hln.be", "demorgen.be", "vrt.be", "nieuwsblad.be", "standaard.be",
        "lesoir.be", "rtbf.be", "lalibre.be", "dhnet.be",
    ],
    # TIGHTENED — name retailers + use compound recall phrases
    google_news_keywords=[
        '"FAVV" terugroep',
        '"AFSCA" rappel',
        '"Delhaize" terugroep OR rappel',
        '"Colruyt" terugroep OR rappel',
        '"Carrefour" terugroep OR rappel België',
        '"Listeria" terugroep België',
        '"Salmonella" rappel Belgique',
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

    recall_signal_terms=[
        # Dutch
        "terugroep", "terugroeping", "teruggeroepen", "terugroepactie",
        "voedselveiligheid", "voedselwaarschuwing",
        "niet consumeren", "niet verbruiken",
        "favv",
        # French
        "rappel", "rappelé", "rappelée", "rappelés", "rappelées",
        "retrait", "retiré", "alerte alimentaire",
        "ne pas consommer", "ne consommez pas",
        "afsca",
        # German (produktwarnung.eu coverage)
        "rückruf", "rueckruf", "produktrückruf",
    ],

    timezone="Europe/Brussels",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(BELGIUM)
