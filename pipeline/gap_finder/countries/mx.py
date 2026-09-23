"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Mexico (COFEPRIS)

WHY THIS EXISTS (audit 2026-09-23)
==================================
scrapers/latam/cofepris_mx.py last placed a row on 2026-06-26. Its three
configured entry points all failed on a direct fetch when tested.

COFEPRIS is the awkward one of this batch. Its "Alertas sanitarias de
alimentos" page is a DOCUMENT LIBRARY — a listing of PDFs — not a set of
per-alert HTML pages. What it does publish per item, verified 2026-09-23,
are articles and press releases:

    /cofepris/articulos/cofepris-emite-alertas-sanitarias-contra-cinco-
        productos-engano
    /cofepris/prensa/<slug>

Those are the linkable per-recall pages, so they are what the regex
accepts. news_authority_mode stays FALSE — a real authority page exists,
even though the alert PDFs sit behind a listing.

NOTE for whoever tunes this next: if the news route keeps finding recalls
that only ever appear as a PDF row in the document library and never as an
articulo/prensa page, that is the signal to revisit — either by accepting
the PDF url, or by setting news_authority_mode=True. Do not guess now;
let the unmatched.jsonl accumulate and read it.
"""

from .base import CountryConfig, RssSource, register


MEXICO = CountryConfig(
    code="mx",
    name_en="Mexico",
    name_local="México",

    authority_short="COFEPRIS",
    authority_full="Comisión Federal para la Protección contra Riesgos Sanitarios",
    authority_domain="gob.mx",
    # Scoped to the cofepris path — gob.mx is the whole federal government.
    # HOST-OPTIONAL PREFIX — see tests/test_country_config_conformance.py
    # ::test_the_regex_matches_both_forms_the_pipeline_uses. This regex is
    # applied to TWO different strings: the full URL (authority_url_finder,
    # extractor) and just "path?query" with the netloc stripped
    # (search_verifier, when it filters the bulk index). A regex naming the
    # host therefore matches at the first site and silently fails at the
    # second, which drops every bulk-index hit as a portal page. The
    # "^(?:https?://[^/]+)?" prefix — the idiom gh.py and za.py already
    # used — matches both.
    # Two shapes. The second was added 2026-09-23 on the register's
    # evidence: BOTH published Mexican rows use it, and the articulos/prensa
    # pattern alone rejected both.
    #     /cofepris/(articulos|prensa)/<slug>
    #     /cms/uploads/attachment/file/<id>/Alerta_Sanitaria__<name>.pdf
    # The file-library path is the caveat this config shipped with — "if
    # unmatched.jsonl fills with PDF-only recalls, revisit" — answered
    # sooner than expected, by the register rather than by unmatched.jsonl.
    #
    # HONEST TRADE-OFF. /cms/uploads/ is the WHOLE Mexican federal
    # government's upload library, not COFEPRIS's, so this one shape cannot
    # be scoped by agency path the way the rest of this file is. It is
    # scoped by FILENAME instead — the document must be named as a sanitary
    # alert. That is weaker, and it is recorded here rather than hidden:
    # a non-COFEPRIS ministry publishing a file called "Alerta_Sanitaria"
    # would pass this gate. The extractor still has to classify it, and
    # Mexico is one of the countries to re-check first if odd rows appear.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:"
        r"cofepris/(?:articulos|prensa)/[a-z0-9\-]+"
        r"|cms/uploads/attachment/file/\d+/[^/]*[Aa]lerta[^/]*\.pdf)"
    ),
    authority_index_url="https://www.gob.mx/cofepris/archivo/articulos",

    rss_sources=[
        RssSource("eluniversal.com.mx", [
            "https://www.eluniversal.com.mx/rss.xml",
        ]),
        RssSource("jornada.com.mx", [
            "https://www.jornada.com.mx/rss/edicion.xml",
        ]),
        RssSource("animalpolitico.com", ["https://animalpolitico.com/feed"]),
    ],
    google_news_domains=[
        "eluniversal.com.mx", "milenio.com", "excelsior.com.mx",
        "jornada.com.mx", "animalpolitico.com", "infobae.com",
        "elfinanciero.com.mx", "proceso.com.mx",
    ],
    google_news_keywords=[
        "Cofepris alerta sanitaria alimento",
        "Cofepris retiro producto",
        "alerta sanitaria alimento México",
    ],

    bulk_index_queries=[
        "site:gob.mx/cofepris alerta sanitaria alimento 2026",
        "site:gob.mx/cofepris alerta sanitaria 2025",
        "site:gob.mx/cofepris retiro del mercado alimento",
        "site:gob.mx/cofepris producto contaminado",
        "site:gob.mx/cofepris no consumir producto",
    ],

    language_name="Spanish",
    language_code="es",
    brand_handling_note=(
        "Keep Mexican brand names exactly as published, with accents "
        "(e.g. 'Bimbo', 'La Costeña'). Do not translate product names."
    ),

    recall_signal_terms=[
        "alerta sanitaria", "retiro", "retira", "retirar",
        "cofepris", "no consumir", "no consuma",
        "contaminado", "contaminada", "riesgo sanitario",
        "recall", "suspension", "suspensión",
    ],

    # CST = UTC-6 year round; Mexico abolished DST in 2022.
    timezone="America/Mexico_City",
    run_local_hour=21,
    cron_utc_offsets=(3, 3),
)

register(MEXICO)
