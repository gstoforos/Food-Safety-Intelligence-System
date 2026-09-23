"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Chile (ACHIPIA / MINSAL)

WHY THIS EXISTS (audit 2026-09-23)
==================================
Chile has been dark. The Greek route applies: local media first,
product/date/hazard confirmed from the article, then the article resolved
back to the authority's own page, and that URL is what is published.

WHICH AUTHORITY. Chile divides this work, and the division matters:
  * MINSAL (Ministerio de Salud) and the regional SEREMIs DECREE a food
    alert — they hold the legal power.
  * ISP (Instituto de Salud Pública) does the laboratory confirmation.
  * ACHIPIA (Agencia Chilena para la Inocuidad y Calidad Alimentaria) is
    the coordinating agency, and it is ACHIPIA that PUBLISHES each alert as
    a permanent, linkable page.
Since the register needs a URL that still resolves next year, ACHIPIA is
the authority domain here, even though MINSAL is the body that decreed the
alert. The notice text names MINSAL or the SEREMI, and that is what should
appear as the issuing authority in the record.

VERIFIED 2026-09-23 — ACHIPIA runs WordPress with date-based permalinks,
so every alert is /YYYY/MM/DD/<slug>/:

    /2024/10/25/ministerio-de-salud-comunica-presencia-de-listeria-en-alimentos/
    /2018/04/20/minsal-decreta-alerta-alimentaria-en-formula-lactea/

That shape is unusually good for this purpose: the publication date is IN
the URL, so the date can be cross-checked against the date extracted from
the article rather than taken on trust.

What the date requirement excludes is the rest of the site — the PDF report
library at /wp-content/uploads/<year>/<month>/<file>.pdf in particular,
which shares the year and month segments but has no day segment and ends
.pdf. The RIAL annual reports live there and are not alerts.

saludresponde.minsal.cl carries the national alert page and is accepted as
a context domain, but its alert page is a single rolling URL rather than
one page per alert, so it is never an item URL.
"""

from .base import CountryConfig, RssSource, register


CHILE = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="cl",
    name_en="Chile",
    name_local="Chile",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="ACHIPIA",
    authority_full=(
        "Agencia Chilena para la Inocuidad y Calidad Alimentaria "
        "(publishing alerts decreed by MINSAL / SEREMI de Salud)"
    ),
    authority_domain="achipia.gob.cl",
    # WordPress date permalinks. The day segment is what separates an alert
    # from the /wp-content/uploads/<year>/<month>/ report library.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/\d{4}/\d{2}/\d{2}/[a-z0-9][a-z0-9\-]+/?$"
    ),
    authority_index_url="https://www.achipia.gob.cl/",
    authority_domains_extra=["saludresponde.minsal.cl", "minsal.cl", "ispch.cl"],

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        RssSource("emol.com", ["https://www.emol.com/rss/rss.asp?canal=nacional"]),
        RssSource("biobiochile.cl", ["https://www.biobiochile.cl/rss/rss.xml"]),
        RssSource("cooperativa.cl", ["https://www.cooperativa.cl/noticias/site/tax/port/all/rss_2_0.xml"]),
    ],
    google_news_domains=[
        "emol.com", "latercera.com", "cooperativa.cl", "biobiochile.cl",
        "t13.cl", "24horas.cl", "elmostrador.cl", "adnradio.cl",
        "meganoticias.cl", "df.cl", "chvnoticias.cl", "lacuarta.com",
    ],
    google_news_keywords=[
        "alerta alimentaria Chile",
        "MINSAL retiro producto alimento",
        "SEREMI alerta alimentaria",
        "Chile food recall",
        "retiro producto listeria Chile",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:achipia.gob.cl alerta alimentaria 2026",
        "site:achipia.gob.cl retiro producto alimento",
        "site:achipia.gob.cl MINSAL alerta alimentaria",
        "site:achipia.gob.cl listeria salmonella alimento",
        "site:achipia.gob.cl comunica presencia alimentos",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Spanish",
    language_code="es",
    brand_handling_note=(
        "Chilean company names are in Spanish and normally carry S.A., SpA "
        "or Ltda.; keep the suffix exactly as published. Note that the "
        "ISSUING authority named in the text will usually be MINSAL or a "
        "regional SEREMI de Salud even though the page is on ACHIPIA — "
        "record the body named in the notice, not the publisher. An alerta "
        "alimentaria in Chile is the national instrument; a retiro is the "
        "company's own withdrawal. Both are recalls for this register, but "
        "an alerta is the stronger action and the distinction is worth "
        "preserving in the description."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "alerta alimentaria", "alerta sanitaria", "retiro", "retirar",
        "no consumir", "minsal", "seremi", "achipia", "ispch",
        "listeria", "salmonella", "contaminado", "contaminación",
        "brote", "intoxicación", "recall",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # Chile observes DST in the SOUTHERN hemisphere, so the two offsets are
    # the reverse of Europe's: CLST = UTC-3 in the southern summer (Jan),
    # CLT = UTC-4 in the southern winter (Jul). 21:00 Santiago is therefore
    # 00:00 UTC in summer and 01:00 UTC in winter — both the following day.
    timezone="America/Santiago",
    run_local_hour=21,
    cron_utc_offsets=(0, 1),
)

register(CHILE)
