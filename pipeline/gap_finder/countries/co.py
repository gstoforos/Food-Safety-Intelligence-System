"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Colombia (INVIMA)

WHY THIS EXISTS (audit 2026-09-23)
==================================
Colombia has been dark. The Greek route applies: local media first,
product/date/hazard confirmed from the article, then the article resolved
back to INVIMA's own page, and that page's URL is what is published.

VERIFIED 2026-09-23 — a real per-alert page:

    invima.gov.co/biblioteca/preview/102418
        "ALERTA SANITARIA DIRECCIÓN DE ALIMENTOS Y BEBIDAS"

INVIMA's layout needs care, because three different things look alike:

  * app.invima.gov.co/alertas/alertas-alimentos-bebidas   — the food and
    drink alert LISTING (an Angular app; rows, not pages)
  * app.invima.gov.co/alertas/alertas-sanitarias-general  — the all-sectors
    listing
  * invima.gov.co/biblioteca/preview/<id>                 — the individual
    alert document, one permanent numbered page each

The listings are Angular routes that render client-side, which is a second
reason not to accept them: fetched, they yield an app shell, and a row
extracted from an app shell is how a recall acquires a navigation label
for a company name.

CORRECTED the same day, by the register rather than by more searching.
INVIMA also publishes alerts as PRESS-ROOM articles, and the one Colombian
row already in Recalls is one:

    invima.gov.co/blog/sala-de-prensa-13/alimento-para-propositos-medicos-
        especiales-contaminado-...

The biblioteca-only pattern rejected it. Both shapes are now accepted.

SCOPE NOTE. INVIMA regulates medicines, devices, cosmetics and food from
one alert series, and /biblioteca/preview/<id> is the whole document
library, so the URL alone does not establish that a document is a food
alert. The extractor filters on content, as it always has, and
recall_signal_terms below keeps the news prefilter on food and drink.
"""

from .base import CountryConfig, RssSource, register


COLOMBIA = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="co",
    name_en="Colombia",
    name_local="Colombia",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="INVIMA",
    authority_full=(
        "Instituto Nacional de Vigilancia de Medicamentos y Alimentos"
    ),
    authority_domain="invima.gov.co",
    # Two shapes; see the docstring for the Angular listings this excludes.
    # The second was added 2026-09-23 on the register's
    # evidence: the ONE published Colombian row is a press-room article,
    #     /blog/sala-de-prensa-13/alimento-para-propositos-medicos-
    #         especiales-contaminado-...
    # not a library document, and the biblioteca pattern alone rejected it.
    # Requiring a slug AFTER the section keeps the section landing page
    # (/blog/sala-de-prensa-13) out.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:"
        r"biblioteca/preview/\d+"
        r"|blog/[a-z0-9\-]+/[a-z0-9][a-z0-9\-]{9,})"
    ),
    authority_index_url="https://app.invima.gov.co/alertas/alertas-alimentos-bebidas",
    authority_domains_extra=["app.invima.gov.co"],

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        RssSource("eltiempo.com", ["https://www.eltiempo.com/rss/colombia.xml"]),
        RssSource("elespectador.com", [
            "https://www.elespectador.com/arc/outboundfeeds/rss/?outputType=xml",
        ]),
        RssSource("rcnradio.com", ["https://www.rcnradio.com/feed"]),
    ],
    google_news_domains=[
        "eltiempo.com", "elespectador.com", "semana.com", "rcnradio.com",
        "caracol.com.co", "noticiascaracol.com", "wradio.com.co",
        "infobae.com", "portafolio.co", "elcolombiano.com",
        "elheraldo.co", "bluradio.com",
    ],
    google_news_keywords=[
        "alerta sanitaria Invima alimentos",
        "Invima retiro producto alimenticio",
        "Invima alerta alimento contaminado",
        "Colombia food recall",
        "Invima no consumir producto",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:invima.gov.co alerta sanitaria alimentos bebidas 2026",
        "site:invima.gov.co biblioteca preview alerta alimentos",
        "site:invima.gov.co alerta sanitaria retiro producto",
        "site:invima.gov.co alimentos y bebidas alerta",
        "site:invima.gov.co producto fraudulento alimento",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Spanish",
    language_code="es",
    brand_handling_note=(
        "Colombian company names are in Spanish and normally carry S.A.S., "
        "S.A. or Ltda.; keep the suffix exactly as published. INVIMA alerts "
        "distinguish a producto FRAUDULENTO (counterfeit or unregistered — "
        "there is no legitimate company to name, so leave company blank) "
        "from a retiro by a registered holder, where the titular del "
        "registro sanitario IS the company. The registro sanitario number "
        "(RSA…, RSIA…) belongs in the product/identifier field."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "alerta sanitaria", "retiro", "retirar", "invima",
        "no consumir", "no consuma", "contaminado", "contaminación",
        "fraudulento", "producto fraudulento", "sin registro sanitario",
        "alimento", "alimentos", "bebida", "bebidas",
        "recall", "retiro del mercado",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # COT = UTC-5, no DST since 1993, so both offsets are identical.
    # 21:00 Bogotá is 02:00 UTC the following day.
    timezone="America/Bogota",
    run_local_hour=21,
    cron_utc_offsets=(2, 2),
)

register(COLOMBIA)
