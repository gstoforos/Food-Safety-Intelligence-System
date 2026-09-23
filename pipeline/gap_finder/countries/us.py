"""
AFTS Food Safety Intelligence — Gap Finder
Country config: United States (USDA FSIS)

WHY THIS EXISTS (audit 2026-09-23)
==================================
USDA FSIS — the largest meat-recall jurisdiction this register covers —
had gone fifteen days without a single row while a working FDA scraper
made North America look covered. fsis.usda.gov returns a 403 to a direct
fetch from a datacentre IP, same as fda.gov, fda.gov.ph and gov.il, so the
Greek route applies: local media first (Food Safety News, CIDRAP and the
wire services all mirror FSIS recalls the same day), product/date/hazard
confirmed from the article, then resolved back to FSIS's own page, and
that URL is what gets published.

VERIFIED 2026-09-23 — real notices, all under /recalls-alerts/<slug>:

    /recalls-alerts/star-meat-delivery-inc--recalls-raw-pork-beef-and-goat-
        products-produced-without
    /recalls-alerts/prime-line-distributors-inc--and-ferrarini-usa-inc--
        recall-imported-ready-eat-pork
    /recalls-alerts/015-2026-city-foods-inc-recalls-ready-eat-pastrami-and-
        corned-beef-products
    /recalls-alerts/fsis-issues-public-health-alert-beef-kofta-products-
        served-kebab-shop

Pages that must NOT match, all on the same host:
    /recalls-alerts?search=015-2026            (the search/listing form —
    /recalls-alerts?search=019-020-2026         differs from an item by one
    /recalls-alerts?search=PHA-08082026-01      character: "?" not "/")
    /recalls                                    (the bare index)
    /food-safety/foodborne-illness-and-disease/outbreaks/
        outbreak-investigations-response        (standing guidance, not a
                                                  per-recall notice)
Requiring the "/recalls-alerts/" path SEGMENT (not just the prefix) is what
excludes the query-string search form and the bare index.
"""

from .base import CountryConfig, RssSource, register


USA = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="us",
    name_en="United States",
    name_local="United States",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="FSIS",
    authority_full="USDA Food Safety and Inspection Service",
    authority_domain="fsis.usda.gov",
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/recalls-alerts/[a-z0-9][a-z0-9\-]+"
    ),
    authority_index_url="https://www.fsis.usda.gov/recalls-alerts",
    # FDA covers non-meat/poultry human food separately (its own config's
    # job); this is meat, poultry and processed-egg products only.

    # ── News sources ────────────────────────────────────────────────────────
    # Food Safety News and CIDRAP both mirror FSIS recalls same-day; the
    # wire services and network affiliates follow within hours.
    rss_sources=[
        RssSource("foodsafetynews.com",
                  ["https://www.foodsafetynews.com/feed/"]),
        RssSource("cidrap.umn.edu",
                  ["https://www.cidrap.umn.edu/rss.xml"]),
    ],
    google_news_domains=[
        "foodsafetynews.com", "cidrap.umn.edu", "foodpoisoningbulletin.com",
        "apnews.com", "cbsnews.com", "nbcnews.com", "abcnews.go.com",
        "usatoday.com", "foxnews.com",
    ],
    google_news_keywords=[
        "USDA FSIS recall",
        "FSIS public health alert",
        "meat recall Listeria",
        "meat recall Salmonella E. coli",
        "USDA recalls beef pork poultry",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:fsis.usda.gov recalls-alerts 2026",
        "site:fsis.usda.gov public health alert",
        "site:fsis.usda.gov recall listeria",
        "site:fsis.usda.gov recall salmonella",
        "site:fsis.usda.gov recall e. coli",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="English",
    language_code="en",
    brand_handling_note=(
        "FSIS notices are published in English only; use the company and "
        "brand spelling exactly as FSIS writes it, including the "
        "establishment name where the notice gives one distinct from the "
        "consumer-facing brand."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "recall", "recalls", "recalled", "public health alert",
        "do not eat", "do not consume", "contaminated", "adulterated",
        "unsafe", "listeria", "salmonella", "e. coli", "fsis", "usda",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # FSIS is a federal Washington DC agency — Eastern time, with DST.
    timezone="America/New_York",
    run_local_hour=21,
    cron_utc_offsets=(1, 2),  # EDT (UTC-4): 21+4=1 next day; EST (UTC-5): 2
)

register(USA)
