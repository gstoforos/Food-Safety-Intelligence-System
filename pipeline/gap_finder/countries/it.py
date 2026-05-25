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
    # Permissive URL filter — drops only obvious non-recall paths.
    # The rule classifier (rules.py) is the real defense: portal/category pages
    # have generic titles ("Avvisi e richiami...") with no specific pathogen/
    # product terms, so they classify as 'unknown' and get rejected automatically.
    authority_item_url_regex=r"(avvisiSicurezza|richiami|p3_2_1_3_1)",

    # ── News sources ────────────────────────────────────────────────────────
    # CRITICAL: use RECALL-SPECIFIC category feeds, not general homepages.
    # General feeds (e.g. /feed/) are 95% non-recall content — our prefilter
    # drops most of it. WordPress sites expose per-category feeds at
    # /argomenti/<category>/feed which give us pure recall content.
    rss_sources=[
        # PRIMARY — il Fatto Alimentare publishes ~14 recall articles/month
        # in this category. This single feed gives us nearly all real Italian
        # recalls with rich descriptions (brand, product, hazard, lot).
        RssSource("ilfattoalimentare.it", [
            "https://ilfattoalimentare.it/argomenti/richiami-e-ritiri/feed",
            "https://ilfattoalimentare.it/argomenti/richiami-e-ritiri/feed/",
            "https://ilfattoalimentare.it/tag/richiamo/feed",
            "https://ilfattoalimentare.it/tag/listeria-monocytogenes/feed",
            "https://ilfattoalimentare.it/tag/salmonella/feed",
            # Fallback to general feed (we already had this — keep as net)
            "https://ilfattoalimentare.it/feed",
        ]),
        # SECONDARY — quifinanza.it has dedicated /ritiri-alimentari section,
        # ~3-5 articles/week with rich snippets
        RssSource("quifinanza.it", [
            "https://quifinanza.it/info-utili/ritiri-alimentari/feed/",
            "https://quifinanza.it/feed/",
        ]),
        # TERTIARY — major dailies (low signal but catches RASFF-level events)
        RssSource("ansa.it", [
            "https://www.ansa.it/sito/ansait_rss.xml",
        ]),
        RssSource("repubblica.it", [
            "https://www.repubblica.it/rss/cronaca/rss2.0.xml",
        ]),
        RssSource("corriere.it", [
            "https://xml2.corriereobjects.it/rss/homepage.xml",
        ]),
        RssSource("ilfattoquotidiano.it", [
            "https://www.ilfattoquotidiano.it/feed/",
        ]),
        RssSource("ilsole24ore.com", [
            "https://www.ilsole24ore.com/rss/italia.xml",
        ]),
    ],
    google_news_domains=[
        "ilfattoalimentare.it",  # primary — backup for direct RSS gaps
        "quifinanza.it",         # primary — recall-focused
        "ansa.it", "repubblica.it", "corriere.it",
        "ilfattoquotidiano.it", "ilmessaggero.it",
        # NOTE: fanpage.it removed — produces ~250 generic-news false positives
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
    # Be strict: require actual recall/withdrawal vocabulary, NOT generic
    # alarm words. "allarme" alone matched climate news, MSF press releases,
    # health-study warnings — all noise. Real recall articles use "richiamo"
    # or "ritiro" or mention the Ministry directly.
    recall_signal_terms=[
        "richiamo", "richiamato", "richiamati", "richiamata",
        "ritiro", "ritirato", "ritirati", "ritirata",
        "allerta alimentare", "allerta sanitaria",
        "ministero salute", "ministero della salute",
        "avviso sicurezza",
        "non consumare", "non consumate",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    timezone="Europe/Rome",
    run_local_hour=21,
    # Rome: CEST=UTC+2 (summer), CET=UTC+1 (winter)
    # 21:00 Rome → 19:00 UTC (CEST) / 20:00 UTC (CET)
    cron_utc_offsets=(19, 20),
)


register(ITALY)
