"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Brazil (ANVISA — Agência Nacional de Vigilância Sanitária)

WHY THIS EXISTS (audit 2026-09-23)
==================================
scrapers/latam/anvisa_br.py placed its last row on 2026-06-27, in the same
fortnight ANMAT (AR, 06-27), COFEPRIS (MX, 06-26), SFA (SG, 06-25),
FDA (PH, 06-14) and NCC (ZA, 06-14) all went quiet. Six regulators on
three continents inside two weeks is a pattern, and the common factor is
that all six are fetched directly from a datacentre IP.

Brazil matters more than its row count suggests: it is 37 rows in the
register and almost all of them arrive as RASFF notifications about goods
exported to Europe — Brazil seen through a European lens. ANVISA's own
notices are the primary source.

Verified 2026-09-23 — ANVISA publishes a permanent page per action:

    /anvisa/pt-br/assuntos/noticias-anvisa/2026/anvisa-determina-
        recolhimento-de-produtos-alimenticios-por-irregularidades
    /anvisa/pt-br/assuntos/noticias-anvisa/2025/anvisa-determina-
        recolhimento-de-alimento-que-nao-informava-presenca-de-alergenicos

Always a year segment. news_authority_mode stays FALSE.
"""

from .base import CountryConfig, RssSource, register


BRAZIL = CountryConfig(
    code="br",
    name_en="Brazil",
    name_local="Brasil",

    authority_short="ANVISA",
    authority_full="Agência Nacional de Vigilância Sanitária",
    authority_domain="gov.br",
    # Scoped to the anvisa path: gov.br is the whole federal government, so
    # a bare host match would accept any ministry's page as authoritative.
    # HOST-OPTIONAL PREFIX — see tests/test_country_config_conformance.py
    # ::test_the_regex_matches_both_forms_the_pipeline_uses. This regex is
    # applied to TWO different strings: the full URL (authority_url_finder,
    # extractor) and just "path?query" with the netloc stripped
    # (search_verifier, when it filters the bulk index). A regex naming the
    # host therefore matches at the first site and silently fails at the
    # second, which drops every bulk-index hit as a portal page. The
    # "^(?:https?://[^/]+)?" prefix — the idiom gh.py and za.py already
    # used — matches both.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/anvisa/pt-br/assuntos/noticias-anvisa/"
        r"\d{4}/[a-z0-9\-]+"
    ),
    authority_index_url="https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa/2026",

    rss_sources=[
        RssSource("agenciabrasil.ebc.com.br", [
            "https://agenciabrasil.ebc.com.br/rss/geral/feed.xml",
            "https://agenciabrasil.ebc.com.br/rss/saude/feed.xml",
        ]),
        RssSource("g1.globo.com", [
            "https://g1.globo.com/rss/g1/",
            "https://g1.globo.com/rss/g1/ciencia-e-saude/",
        ]),
        RssSource("folha.uol.com.br", [
            "https://feeds.folha.uol.com.br/cotidiano/rss091.xml",
        ]),
    ],
    google_news_domains=[
        "g1.globo.com", "folha.uol.com.br", "estadao.com.br",
        "agenciabrasil.ebc.com.br", "cnnbrasil.com.br", "uol.com.br",
        "metropoles.com", "oglobo.globo.com",
    ],
    google_news_keywords=[
        "Anvisa recolhimento alimento",
        "Anvisa proíbe lote alimento",
        "recall de alimento Brasil",
    ],

    bulk_index_queries=[
        "site:gov.br/anvisa recolhimento de alimento 2026",
        "site:gov.br/anvisa recolhimento 2025",
        "site:gov.br/anvisa determina recolhimento",
        "site:gov.br/anvisa proíbe lote alimento",
        "site:gov.br/anvisa alerta alimento contaminado",
    ],

    language_name="Portuguese",
    language_code="pt",
    brand_handling_note=(
        "Keep Brazilian brand names exactly as published, with accents "
        "(e.g. 'Nestlé', 'Piracanjuba'). Do not translate product names."
    ),

    recall_signal_terms=[
        "recolhimento", "recolher", "recall",
        "anvisa", "suspende", "suspensao", "suspensão",
        "proibe", "proíbe", "interdita", "interdicao", "interdição",
        "alerta sanitario", "alerta sanitário", "impropio", "impróprio",
    ],

    # BRT = UTC-3 year round; Brazil abolished DST in 2019.
    # 21:00 local = 00:00 UTC the following day.
    timezone="America/Sao_Paulo",
    run_local_hour=21,
    cron_utc_offsets=(0, 0),
)

register(BRAZIL)
