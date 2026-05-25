"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Luxembourg (Sécurité Alimentaire / Lebensmittelsicherheit)

Luxembourg food-recall regime:
  - Small market (~660K population). Recall regime mostly aligns with
    French + German systems via cross-border distribution.
  - Authority: securite-alimentaire.public.lu (Direction de la Sécurité
    alimentaire) — bilingual FR/DE.
  - News sources mostly French + some German. Luxembourgish (lb) rarely
    used in print.
  - Low volume: ~10-20 recalls/year expected.
"""

from .base import CountryConfig, RssSource, register


LUXEMBOURG = CountryConfig(
    code="lu",
    name_en="Luxembourg",
    name_local="Luxembourg",

    authority_short="SECLU",
    authority_full="Direction de la Sécurité alimentaire",
    authority_domain="securite-alimentaire.public.lu",
    authority_item_url_regex=r"(rappel|warnung|alerte|news|actualite|aktualiteit)",

    rss_sources=[
        # French-language Luxembourg news
        RssSource("rtl.lu", [
            "https://www.rtl.lu/feed",
        ]),
        RssSource("wort.lu", [
            "https://www.wort.lu/de/rss.xml",
        ]),
        RssSource("paperjam.lu", [
            "https://paperjam.lu/feed",
        ]),
        RssSource("lessentiel.lu", [
            "https://www.lessentiel.lu/rss/",
        ]),
        RssSource("tageblatt.lu", [
            "https://www.tageblatt.lu/feed/",
        ]),
    ],
    google_news_domains=[
        "rtl.lu", "wort.lu", "paperjam.lu", "lessentiel.lu",
        "tageblatt.lu", "delano.lu", "chronicle.lu",
    ],
    google_news_keywords=[
        # French
        "rappel produit alimentaire Luxembourg",
        "alerte alimentaire Luxembourg",
        # German
        "Lebensmittel Rückruf Luxemburg",
        "Lebensmittelwarnung Luxemburg",
    ],

    bulk_index_queries=[
        "site:securite-alimentaire.public.lu rappel 2026",
        "site:securite-alimentaire.public.lu Rückruf 2026",
        "site:securite-alimentaire.public.lu alerte",
        "site:securite-alimentaire.public.lu Listeria OR Salmonella",
        "site:securite-alimentaire.public.lu allergène allergen",
    ],

    language_name="French or German (Luxembourg bilingual)",
    language_code="fr",
    brand_handling_note=(
        "Luxembourg uses brands shared with neighbours. Preserve casing "
        "(e.g. 'Cactus', 'Auchan Luxembourg', 'Delhaize Luxembourg', "
        "'Lidl Luxembourg')."
    ),

    # UNION of FR + DE recall vocabularies
    recall_signal_terms=[
        # French
        "rappel", "rappelé", "rappelée", "rappelés", "rappelées",
        "retrait", "retiré", "alerte alimentaire",
        "ne pas consommer", "ne consommez pas",
        # German
        "rückruf", "rueckruf", "rückgerufen", "rueckgerufen",
        "warnung vor", "lebensmittelwarnung",
        "nicht verzehren", "nicht konsumieren",
        # Common
        "sécurité alimentaire", "lebensmittelsicherheit",
    ],

    timezone="Europe/Luxembourg",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(LUXEMBOURG)
