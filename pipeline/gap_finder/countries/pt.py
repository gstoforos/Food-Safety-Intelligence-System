"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Portugal (ASAE — Economic and Food Safety Authority)

Portuguese food-recall regime:
  - ASAE (Autoridade de Segurança Alimentar e Económica) publishes
    alerts and recalls at:
      https://www.asae.gov.pt/
  - Recalls are lower volume than Italy/Spain (smaller market, smaller
    consumer-news ecosystem). Expect 1-3 Pending rows per week.
  - Portuguese recall vocabulary:
      "recolha"          = recall (the action of collecting back)
      "retirada"         = withdrawal
      "alerta alimentar" = food alert
      "não consumir"     = do not consume
      "alergénio"        = allergen
"""

from .base import CountryConfig, RssSource, register


PORTUGAL = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="pt",
    name_en="Portugal",
    name_local="Portugal",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="ASAE",
    authority_full="Autoridade de Segurança Alimentar e Económica",
    authority_domain="asae.gov.pt",
    authority_item_url_regex=r"(alerta|noticia|comunicado|alimentar|destaque)",

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        # MAJOR DAILIES
        RssSource("publico.pt", [
            "https://www.publico.pt/rss",
            "https://feeds.feedburner.com/PublicoRSS",
        ]),
        RssSource("jn.pt", [
            "https://www.jn.pt/rss",
            "https://www.jn.pt/rss/nacional.xml",
        ]),
        RssSource("dn.pt", [
            "https://www.dn.pt/rss",
        ]),
        RssSource("expresso.pt", [
            "https://expresso.pt/rss/ultimas",
            "https://feeds.feedburner.com/expresso-geral",
        ]),
        RssSource("cmjornal.pt", [
            "https://www.cmjornal.pt/rss",
        ]),
        RssSource("noticiasaominuto.com", [
            "https://www.noticiasaominuto.com/rss/economia",
            "https://www.noticiasaominuto.com/rss/pais",
        ]),
        RssSource("sicnoticias.pt", [
            "https://sicnoticias.pt/rss",
        ]),
    ],
    google_news_domains=[
        "publico.pt", "jn.pt", "dn.pt", "expresso.pt",
        "cmjornal.pt", "noticiasaominuto.com", "sicnoticias.pt",
        "observador.pt", "tvi.iol.pt", "rtp.pt",
    ],
    google_news_keywords=[
        "ASAE recolha alimentar",
        "alerta alimentar Salmonella",
        "alerta alimentar Listeria",
        "retirada produto alimentar",
        "alergénio não declarado",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:asae.gov.pt alerta alimentar 2026",
        "site:asae.gov.pt alerta alimentar 2025",
        "site:asae.gov.pt recolha alimentar",
        "site:asae.gov.pt notificação RASFF",
        "site:asae.gov.pt alerta Salmonella OR Listeria",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Portuguese",
    language_code="pt",
    brand_handling_note=(
        "Portuguese brands are typically written in Latin script. "
        "Preserve original casing (e.g. 'Continente', 'Pingo Doce', "
        "'Lidl Portugal', 'Auchan Portugal')."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "recolha", "recolhido", "recolhidos", "recolha alimentar",
        "retirada", "retirado", "retirados",
        "alerta alimentar", "alerta sanitário",
        "asae",
        "não consumir", "não consuma",
        "aviso de segurança",
        "alergénio não declarado", "alergenio nao declarado",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    timezone="Europe/Lisbon",
    run_local_hour=21,
    # Lisbon: WEST=UTC+1 (summer), WET=UTC+0 (winter)
    # 21:00 Lisbon → 20:00 UTC (WEST) / 21:00 UTC (WET)
    cron_utc_offsets=(20, 21),
)


register(PORTUGAL)
