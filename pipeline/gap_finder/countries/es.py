"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Spain (AESAN — Spanish Food Safety & Nutrition Agency)

Spanish food-recall regime:
  - AESAN (Agencia Española de Seguridad Alimentaria y Nutrición) publishes
    recall and alert notices at:
      https://www.aesan.gob.es/AECOSAN/web/seguridad_alimentaria/alertas_alimentarias/
  - Each alert has a year-numbered ID (e.g. 2026_03)
  - Spanish recall vocabulary:
      "retirada"          = withdrawal/recall
      "alerta alimentaria"= food alert
      "aviso"             = notice
      "no consumir"       = do not consume
      "alérgeno"          = allergen
"""

from .base import CountryConfig, RssSource, register


SPAIN = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="es",
    name_en="Spain",
    name_local="España",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="AESAN",
    authority_full="Agencia Española de Seguridad Alimentaria y Nutrición",
    authority_domain="aesan.gob.es",
    # AESAN alert URLs: /alertas_alimentarias/2026_NN.htm
    authority_item_url_regex=r"(alertas_alimentarias|aecosan)",

    # ── News sources ────────────────────────────────────────────────────────
    # Focus on consumer-rights orgs (high recall density) + major dailies.
    rss_sources=[
        # CONSUMER-FOCUSED (highest recall signal density)
        RssSource("65ymas.com", [
            "https://www.65ymas.com/rss/seguridad-alimentaria",
            "https://www.65ymas.com/rss",
        ]),
        RssSource("consumer.es", [
            "https://www.consumer.es/feed",
            "https://www.consumer.es/seguridad-alimentaria/feed",
        ]),
        RssSource("ocu.org", [
            "https://www.ocu.org/alimentacion/rss",
        ]),
        # MAJOR DAILIES (broader coverage, occasional big-recall stories)
        RssSource("elpais.com", [
            "https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/portada",
        ]),
        RssSource("elmundo.es", [
            "https://e00-elmundo.uecdn.es/elmundo/rss/portada.xml",
        ]),
        RssSource("20minutos.es", [
            "https://www.20minutos.es/rss/",
        ]),
        RssSource("lavanguardia.com", [
            "https://www.lavanguardia.com/mvc/feed/rss/home",
        ]),
    ],
    google_news_domains=[
        "65ymas.com", "consumer.es", "ocu.org",
        "elpais.com", "elmundo.es", "abc.es", "lavanguardia.com",
        "20minutos.es", "elperiodico.com", "elconfidencial.com",
        "heraldo.es", "larazon.es",
    ],
    google_news_keywords=[
        "alerta alimentaria AESAN",
        "retirada producto alimentario",
        "alerta sanitaria Salmonella",
        "alerta alimentaria Listeria",
        "no consumir alérgeno no declarado",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:aesan.gob.es alerta alimentaria 2026",
        "site:aesan.gob.es alerta alimentaria 2025",
        "site:aesan.gob.es alertas alimentarias retirada",
        "site:aesan.gob.es notificación RASFF",
        "site:aesan.gob.es alerta Salmonella OR Listeria",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Spanish",
    language_code="es",
    brand_handling_note=(
        "Spanish brands are typically written in Latin script. "
        "Preserve original casing (e.g. 'Mercadona', 'Carrefour España', "
        "'Hacendado', 'Lactalis')."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    # Strict: require actual recall vocabulary. Drop generic 'alerta' alone
    # because it matches earthquake/weather/political alerts.
    recall_signal_terms=[
        "retirada", "retirado", "retirados", "retirada", "retira", "retiran",
        "alerta alimentaria", "alerta sanitaria",
        "aesan", "agencia española seguridad alimentaria",
        "no consumir", "no consuma",
        "aviso seguridad",
        "alérgeno no declarado", "alergeno no declarado",
        "ministerio sanidad",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    timezone="Europe/Madrid",
    run_local_hour=21,
    # Madrid: CEST=UTC+2 (summer), CET=UTC+1 (winter)
    # 21:00 Madrid → 19:00 UTC (CEST) / 20:00 UTC (CET)
    cron_utc_offsets=(19, 20),
)


register(SPAIN)
