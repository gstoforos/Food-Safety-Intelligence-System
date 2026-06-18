"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Ghana (FDA Ghana — Food and Drugs Authority).

VERIFIED 2026-06 (Greece concept): FDA Ghana publishes recalls/alerts under a
WordPress newsroom on fdaghana.gov.gh, English, with a formal Rapid Alert &
Recall System (Class I/II/III, numerical designations). Recall index:
    https://fdaghana.gov.gh/newsroom/product-recalls-and-alerts/
Product alerts index:
    https://fdaghana.gov.gh/newsroom/product-alerts/
Each recall/alert is its own newsroom post. English — trivial extraction.

NOTE: FDA Ghana regulates food AND drugs/cosmetics/herbal; the classifier
filters non-food/non-hazard items. The exact per-post permalink (root /<slug>/
vs /newsroom/<slug>/) is confirmed on the first live dry-run — the item regex
below accepts both forms, and Tier-0 (candidate URL is itself an authority page)
catches Google-News-surfaced posts directly regardless.

Clean Tier-2 authority_index_url country, like EFET (Greece).
"""

from .base import CountryConfig, RssSource, register


GHANA = CountryConfig(
    code="gh",
    name_en="Ghana",
    name_local="Ghana",

    authority_short="FDA Ghana",
    authority_full="Food and Drugs Authority (Ghana)",
    authority_domain="fdaghana.gov.gh",
    # Per-recall posts are descriptive WordPress slugs, either root-level or
    # under /newsroom/. Accept both; exclude the section/taxonomy index pages
    # (product-recalls-and-alerts, product-alerts, news, press-release, etc.)
    # and short nav slugs. Anchored so multi-segment nav paths don't match.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:newsroom/)?"
        r"(?!product-recalls-and-alerts/?$|product-alerts/?$|press-release-public-notice/?$"
        r"|features/?$|this-week-fda/?$|gallery/?$|news/|category/|tag/|wp-)"
        r"[a-z][a-z0-9-]{14,}/?$"
    ),
    # Recall & alerts newsroom index (verified live 2026-06).
    authority_index_url="https://fdaghana.gov.gh/newsroom/product-recalls-and-alerts/",

    # ── News sources (SIGNAL layer) ─────────────────────────────────────────
    rss_sources=[
        RssSource("graphic.com.gh", [
            "https://www.graphic.com.gh/feed.html",
        ]),
        RssSource("myjoyonline.com", [
            "https://www.myjoyonline.com/feed/",
        ]),
        RssSource("citinewsroom.com", [
            "https://citinewsroom.com/feed/",
        ]),
    ],
    google_news_domains=[
        "fdaghana.gov.gh",   # authority
        "graphic.com.gh", "myjoyonline.com", "citinewsroom.com",
        "ghanaweb.com", "3news.com", "pulse.com.gh", "adomonline.com",
        "thebftonline.com", "gbcghanaonline.com",
    ],
    # English compound phrases — recall-action + hazard + FDA Ghana.
    google_news_keywords=[
        '"FDA" Ghana recall food OR product',
        '"Food and Drugs Authority" recall OR alert Ghana',
        '"Listeria" OR Salmonella recall Ghana',
        'aflatoxin OR contamination food alert Ghana FDA',
        'FDA Ghana unsafe product "do not consume"',
    ],

    bulk_index_queries=[
        "site:fdaghana.gov.gh product recall Listeria OR Salmonella OR aflatoxin",
        "site:fdaghana.gov.gh newsroom recall 2026",
        "site:fdaghana.gov.gh alert contamination food",
    ],

    language_name="English",
    language_code="en",
    brand_handling_note=(
        "FDA Ghana alerts are English/Latin script. Preserve casing. Capture "
        "the manufacturer/importer and the foreign-regulator source when the "
        "alert mirrors an overseas recall. Company forms 'Ltd' / 'Plc' kept "
        "as written."
    ),

    recall_signal_terms=[
        "recall", "recalled", "product recall", "safety alert", "alert",
        "withdraw", "withdrawn", "do not consume", "unsafe",
        "fda", "food and drugs authority", "rapid alert",
        # Pathogen/hazard cues
        "salmonella", "listeria", "aflatoxin", "contamination",
        "cereulide", "e. coli", "undeclared allergen", "foreign matter",
    ],

    timezone="Africa/Accra",
    run_local_hour=21,
    # GMT = UTC+0 year-round (no DST). 21:00 local = 21:00 UTC.
    cron_utc_offsets=(21, 21),
)


register(GHANA)
