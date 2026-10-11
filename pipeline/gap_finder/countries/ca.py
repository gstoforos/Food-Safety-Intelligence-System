"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Canada — Québec (MAPAQ, Ministère de l'Agriculture, des
Pêcheries et de l'Alimentation du Québec).

WHY THIS EXISTS (operator 2026-10-11: "yes" to writing the MAPAQ collector)
==========================================================================
tools/monitored_sources.py has advertised "MAPAQ QC" on the dashboard's
"Sources monitored" tile while nothing in scrapers/ or pipeline/ collected
it (tests/test_every_monitored_source_has_a_collector.py pinned it as the
one known hole). scrapers/mapaq.py is the Qatar MoPH scraper under a
permuted filename. The newest MAPAQ row in the register was 2026-04-13, and
the four that exist came through the Tavily path, which is not in the daily
fleet. On 2026-10-10 two in-scope Québec notices were missed: a Clostridium
botulinum risk in Naloune brand marinade (Manas épicerie africaine inc.) and
E. coli in HUMMUS HALAL (Marché Sultan).

CFIA (federal Canada) is collected separately (scrapers/ for
recalls-rappels.canada.ca). This config is MAPAQ only: the Québec ministry
publishes its own "mise en garde" / "avis de ne pas consommer" notices for
products made and sold in Québec that never reach the CFIA list.

THE PAGE SHAPE — verified against the two MAPAQ rows already in Recalls
---------------------------------------------------------------------
    https://www.quebec.ca/nouvelles/actualites/details/
        mise-en-garde-a-la-population-innocuite-non-assuree-depices-
        haitiennes-vendues-par-lentreprise-marche-rio-69749
    https://www.quebec.ca/nouvelles/actualites/details/
        mise-en-garde-a-la-population-presence-possible-de-salmonelle-dans-
        les-barres-de-chocolat-aux-pistaches-...-chocostyle-440-69727

A per-notice slug ending in a numeric id. quebec.ca is the WHOLE Québec
government (an umbrella like gov.br), so the regex is scoped to the MAPAQ
notice wordings — "mise en garde à la population", "avis de ne pas
consommer", "absence d'informations nécessaires", "mise à jour" — and the
host is listed in _url_guard.UMBRELLA_HOSTS, never in _AUTHORITY_HOSTS.
quebec.ca renders as a JavaScript shell to automated clients, so there is
no authority index to scan (authority_index_url empty); discovery is the
Greek route — news and search, resolved back to the quebec.ca notice.

news_authority_mode stays FALSE: a real per-notice page exists, so a news
URL must never stand in for it. (MAPAQ releases are also distributed on
newswire.ca; those are discovery signals only.)
"""

from .base import CountryConfig, register


CANADA_QC = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="ca",
    name_en="Canada",
    name_local="Québec",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="MAPAQ",
    authority_full=("Ministère de l'Agriculture, des Pêcheries et de "
                    "l'Alimentation du Québec"),
    authority_domain="quebec.ca",
    authority_domains_extra=["mapaq.gouv.qc.ca"],
    # Path-scoped, host-optional (base.py: the regex is matched against the
    # full URL AND against path?query with the netloc stripped).
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:fr/)?nouvelles/actualites/details/"
        r"(?:mise-en-garde-a-la-population|avis-de-ne-pas-consommer|"
        r"absence-dinformations-necessaires|mise-a-jour)[a-z0-9-]*-\d+/?$"
    ),
    authority_index_url="",

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[],
    google_news_domains=[
        "quebec.ca", "newswire.ca", "lapresse.ca", "ledevoir.com",
        "journaldemontreal.com", "journaldequebec.com", "ici.radio-canada.ca",
        "tvanouvelles.ca", "noovo.info", "narcity.com",
    ],
    google_news_keywords=[
        '"mise en garde à la population" MAPAQ',
        '"avis de ne pas consommer" MAPAQ',
        'MAPAQ "innocuité non assurée"',
        'MAPAQ rappel aliment Salmonelle OR Listeria OR "E. coli" OR botulisme',
    ],
    bulk_index_queries=[
        'site:quebec.ca "mise en garde à la population" 2026',
        'site:quebec.ca "avis de ne pas consommer" 2026',
        'site:quebec.ca "innocuité non assurée"',
        'site:quebec.ca "absence d\'informations nécessaires à la consommation sécuritaire"',
        'site:quebec.ca nouvelles MAPAQ présence possible E. coli OR Listeria OR salmonelle',
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="French (Québec)",
    language_code="fr",
    brand_handling_note=(
        "MAPAQ notices name the business that prepared or sold the product "
        "(\"vendu par l'entreprise X\") and, where there is one, the brand "
        "(\"de marque Y\"). Company is the business named after "
        "\"l'entreprise\"; Brand is the \"marque\" when given. Keep both as "
        "written. Translate the product description into English, keeping the "
        "French name in parentheses."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "mapaq", "mise en garde", "avis de ne pas consommer",
        "innocuité non assurée", "ne pas consommer", "rappel",
        "présence possible", "présence de", "salmonelle", "listeria",
        "e. coli", "botulisme", "botulinum", "absence d'informations",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    timezone="America/Toronto",
    run_local_hour=21,
    # EDT UTC-4 / EST UTC-5 -> 01:00 / 02:00 UTC.
    cron_utc_offsets=(1, 2),
)


register(CANADA_QC)
