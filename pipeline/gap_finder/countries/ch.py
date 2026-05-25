"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Switzerland (BLV — Bundesamt für Lebensmittelsicherheit
                und Veterinärwesen / Office fédéral de la sécurité
                alimentaire et des affaires vétérinaires)

Swiss food-recall regime:
  - TRILINGUAL: German (Deutschschweiz), French (Romandie), Italian (Ticino).
  - BLV at blv.admin.ch publishes alerts in all 3 languages.
  - Recall vocabulary spans 3 languages — we union them all.
  - Volume: ~50-100 recalls/year.
"""

from .base import CountryConfig, RssSource, register


SWITZERLAND = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="ch",
    name_en="Switzerland",
    name_local="Schweiz / Suisse / Svizzera",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="BLV",
    authority_full="Bundesamt für Lebensmittelsicherheit und Veterinärwesen",
    authority_domain="blv.admin.ch",
    authority_item_url_regex=r"(warnung|rappel|richiamo|news|aktuell)",

    # ── News sources (trilingual: DE + FR + IT) ─────────────────────────────
    rss_sources=[
        # German-speaking
        RssSource("nzz.ch", [
            "https://www.nzz.ch/recent.rss",
        ]),
        RssSource("tagesanzeiger.ch", [
            "https://www.tagesanzeiger.ch/rss.html",
        ]),
        RssSource("blick.ch", [
            "https://www.blick.ch/news/rss.xml",
        ]),
        RssSource("srf.ch", [
            "https://www.srf.ch/news/bnf/rss/1646",
        ]),
        RssSource("watson.ch", [
            "https://www.watson.ch/feed",
        ]),
        # French-speaking
        RssSource("rts.ch", [
            "https://www.rts.ch/info/?format=rss",
        ]),
        RssSource("letemps.ch", [
            "https://www.letemps.ch/feed",
        ]),
        RssSource("24heures.ch", [
            "https://www.24heures.ch/rss.html",
        ]),
        # Italian-speaking
        RssSource("rsi.ch", [
            "https://www.rsi.ch/news/rss",
        ]),
        RssSource("tio.ch", [
            "https://www.tio.ch/rss",
        ]),
    ],
    google_news_domains=[
        # DE
        "nzz.ch", "tagesanzeiger.ch", "blick.ch", "srf.ch", "watson.ch",
        # FR
        "rts.ch", "letemps.ch", "24heures.ch", "tdg.ch",
        # IT
        "rsi.ch", "tio.ch", "cdt.ch", "laregione.ch",
    ],
    google_news_keywords=[
        # German
        "BLV Lebensmittel Rückruf Schweiz",
        "Rückruf Salmonellen Schweiz",
        # French
        "rappel alimentaire Suisse BLV",
        "OSAV rappel produit",
        # Italian
        "richiamo alimentare Svizzera USAV",
    ],

    bulk_index_queries=[
        "site:blv.admin.ch Rückruf 2026",
        "site:blv.admin.ch rappel 2026",
        "site:blv.admin.ch richiamo 2026",
        "site:blv.admin.ch Warnung Listerien OR Salmonellen",
        "site:blv.admin.ch warnung rappel",
    ],

    # ── LLM extraction ──────────────────────────────────────────────────────
    language_name="German, French, or Italian (Swiss multilingual context)",
    language_code="de",  # default; LLM handles all 3 in practice
    brand_handling_note=(
        "Swiss brands span 3 languages. Preserve original casing in whatever "
        "language the source uses (e.g. 'Migros', 'Coop Schweiz', 'Denner', "
        "'Manor', 'Aligro', 'Volg')."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    # UNION of DE + FR + IT recall vocabularies
    recall_signal_terms=[
        # German
        "rückruf", "rueckruf", "rückgerufen", "rueckgerufen",
        "rücknahme", "warnung vor", "lebensmittelwarnung",
        "nicht verzehren",
        # French
        "rappel", "rappelé", "rappelée", "rappelés",
        "retrait", "retiré", "alerte alimentaire",
        "ne pas consommer", "ne consommez pas",
        # Italian
        "richiamo", "richiamato", "richiamati", "richiamata",
        "ritiro", "ritirato", "allerta alimentare",
        "non consumare", "non consumate",
        # Common to multiple
        "blv", "osav", "usav",
    ],

    timezone="Europe/Zurich",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(SWITZERLAND)
