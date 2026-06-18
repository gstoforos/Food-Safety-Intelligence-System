"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Bosnia and Herzegovina (FSA BiH / Agencija za sigurnost hrane
                Bosne i Hercegovine — Food Safety Agency of BiH).

VERIFIED 2026-06 (Greece concept): FSA BiH publishes each recall as its own
per-item HTML page on fsa.gov.ba with a descriptive slug, e.g.:
    https://fsa.gov.ba/hr/obavijest-o-povlacenju-i-opozivu-odredjenih-serija-bebelac-1-400-g-nutricia-2/
    (Bebelac infant formula, cereulide)
Index (paginated):
    https://fsa.gov.ba/bs/kategorija/vijesti/vijesti-po-temama/povlacenje-i-opoziv-hrane/
Each page carries product, hazard (cereulide, Listeria, foreign body, pesticide),
producer/operator, lot, with EFSA/RASFF references. Latin script
(Bosnian/Croatian; site has /bs and /hr language paths). FSA BiH is the Codex
contact point.

Clean Tier-2 authority_index_url country, like EFET (Greece).
"""

from .base import CountryConfig, RssSource, register


BOSNIA = CountryConfig(
    code="ba",
    name_en="Bosnia and Herzegovina",
    name_local="Bosna i Hercegovina",

    authority_short="FSA BiH",
    authority_full="Agencija za sigurnost hrane Bosne i Hercegovine",
    authority_domain="fsa.gov.ba",
    # Per-recall slug pages: /obavijest-o-povlacenju...  and  /...-opoziv-...
    # Match the recall/withdrawal verbs on the authority domain.
    authority_item_url_regex=r"(povlacenj|povlacenju|opoziv|obavijest-o-povlac)",
    # Recall index (verified live 2026-06), paginated.
    authority_index_url="https://fsa.gov.ba/bs/kategorija/vijesti/vijesti-po-temama/povlacenje-i-opoziv-hrane/",

    # ── News sources (SIGNAL layer) ─────────────────────────────────────────
    rss_sources=[
        # Bosnian major news (verified working endpoints).
        RssSource("klix.ba", [
            "https://www.klix.ba/rss/naslovnica",
        ]),
        RssSource("avaz.ba", [
            "https://avaz.ba/rss",
        ]),
        RssSource("nezavisne.com", [
            "https://www.nezavisne.com/rss/",
        ]),
    ],
    google_news_domains=[
        "fsa.gov.ba",        # authority
        "klix.ba", "avaz.ba", "nezavisne.com", "n1info.ba",
        "faktor.ba", "radiosarajevo.ba", "oslobodjenje.ba",
        "vijesti.ba", "bljesak.info", "source.ba",
    ],
    # Precise Bosnian/Croatian compound phrases — recall-action + hazard.
    google_news_keywords=[
        '"povlačenje i opoziv" hrane',
        '"opoziv" proizvod FSA OR agencija sigurnost hrane',
        '"Listeria" povlačenje BiH',
        '"Salmonela" proizvod opoziv',
        'agencija sigurnost hrane povlačenje tržišta',
    ],

    bulk_index_queries=[
        "site:fsa.gov.ba povlačenje opoziv 2026",
        "site:fsa.gov.ba obavijest o povlačenju",
        "site:fsa.gov.ba opoziv Listeria OR Salmonela",
    ],

    language_name="Bosnian",
    language_code="bs",
    brand_handling_note=(
        "Bosnian/Croatian brand and company names are Latin script (may include "
        "č, ć, š, ž, đ). Preserve casing and diacritics. Company legal forms "
        "'d.o.o.' / 'd.d.' should be kept as written."
    ),

    recall_signal_terms=[
        # Bosnian/Croatian recall / withdrawal vocabulary
        "povlačenje", "povlači", "povučen", "povučeni",
        "opoziv", "opoziva", "opozvan",
        "s tržišta", "iz prodaje",
        "obavijest", "obavještavamo potrošače",
        "ne konzumirati", "nesukladan",
        "agencija za sigurnost hrane", "fsa",
        # Pathogen/hazard cues
        "listeria", "listerija", "salmonela", "salmonella",
        "cereulid", "aflatoksin", "pesticid", "strano tijelo",
    ],

    timezone="Europe/Sarajevo",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(BOSNIA)
