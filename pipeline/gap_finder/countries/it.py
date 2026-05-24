"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Italy (Ministero della Salute — Italian Ministry of Health)

Italian food-recall regime:
  - Ministero della Salute publishes recall notices at:
      https://www.salute.gov.it/portale/news/p3_2_1_3_1.jsp?lingua=italiano&...
  - Each recall has a numeric internal ID and a PDF + HTML notice
  - Local terminology:
      "richiamo"        = recall
      "ritiro"          = withdrawal
      "avviso"          = notice
      "allerta"         = alert
      "non conformità"  = non-conformity
"""

from .base import CountryConfig, RssSource, register


ITALY = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="it",
    name_en="Italy",
    name_local="Italia",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="Salute",
    authority_full="Ministero della Salute",
    authority_domain="salute.gov.it",
    # Salute publishes recall items at predictable URLs containing 'avvisiSicurezza'
    # and/or specific JSP id parameters. The recall-page filter is the JSP root.
    authority_item_url_regex=r"(avvisiSicurezza|richiami|p3_2_1_3_1)",

    # ── News sources ────────────────────────────────────────────────────────
    # Italian RSS landscape is healthier than Greek — major dailies still publish feeds.
    rss_sources=[
        RssSource("ansa.it", [
            "https://www.ansa.it/sito/ansait_rss.xml",
            "https://www.ansa.it/canale_cronaca/notizie/cronaca_rss.xml",
        ]),
        RssSource("repubblica.it", [
            "https://www.repubblica.it/rss/cronaca/rss2.0.xml",
            "https://www.repubblica.it/rss/homepage/rss2.0.xml",
        ]),
        RssSource("corriere.it", [
            "https://xml2.corriereobjects.it/rss/homepage.xml",
            "https://xml2.corriereobjects.it/rss/cronache.xml",
        ]),
        RssSource("ilfattoquotidiano.it", [
            "https://www.ilfattoquotidiano.it/feed/",
        ]),
        RssSource("ilsole24ore.com", [
            "https://www.ilsole24ore.com/rss/italia.xml",
        ]),
        RssSource("ilmessaggero.it", [
            "https://www.ilmessaggero.it/rss.xml",
            "https://www.ilmessaggero.it/rss/cronaca.xml",
        ]),
        RssSource("ilfattoalimentare.it", [
            "https://ilfattoalimentare.it/feed",
        ]),
    ],
    google_news_domains=[
        "ansa.it", "repubblica.it", "corriere.it",
        "ilfattoquotidiano.it", "ilsole24ore.com", "ilmessaggero.it",
        "ilfattoalimentare.it", "fanpage.it",
    ],
    google_news_keywords=[
        "richiamo alimentare Ministero Salute",
        "ritiro alimentare allergene",
        "richiamo prodotto Salmonella",
        "allerta alimentare",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:salute.gov.it richiamo alimentare 2026",
        "site:salute.gov.it richiamo alimentare 2025",
        "site:salute.gov.it avvisi sicurezza alimentare",
        "site:salute.gov.it allerta richiamo",
        "site:salute.gov.it richiamo Salmonella OR Listeria",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Italian",
    language_code="it",
    brand_handling_note=(
        "Italian brands are typically written in Latin script. "
        "Preserve original casing (e.g. 'Barilla', 'Coop Italia', 'Esselunga')."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "richiamo", "richiamato", "richiamati",
        "ritiro", "ritirato", "ritirati",
        "allerta", "allarme",
        "ministero salute", "salute.gov",
        "avviso sicurezza",
        "non conformita", "non conformità",
        "consumare", "non consumare",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    timezone="Europe/Rome",
    run_local_hour=21,
    # Rome: CEST=UTC+2 (summer), CET=UTC+1 (winter)
    # 21:00 Rome → 19:00 UTC (CEST) / 20:00 UTC (CET)
    cron_utc_offsets=(19, 20),
)


register(ITALY)
