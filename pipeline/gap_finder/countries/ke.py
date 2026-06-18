"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Kenya (KEBS — Kenya Bureau of Standards, with Ministry of Health).

⚠ NEWS-AUTHORITY MODE (news_authority_mode=True).
VERIFIED 2026-06: KEBS (kebs.org) issues recalls as individual letters to
manufacturers + public statements; kebs.org/public-notice/ is not a per-recall
slugged index. Real recalls (e.g. early-2026 aflatoxin peanut-butter seize-and-
destroy of Nutie / Kismat / Pannaj / Muleka; the 10-brand edible-oil withdrawal)
reach the public ONLY through quality news — NOT as linkable per-recall pages on
KEBS's own site.

Because there is no authority URL to point at, this country runs in
news-authority mode: the gate accepts the NEWS article URL (from the curated
whitelist below) as the record URL, provided the record classifies at tier 1/2.
Cross-outlet duplicates collapse via dedupe_by_recall_identity downstream.

Scoped exception to the authority-pure rule for a portal-less country; does NOT
affect GR/ZA/NG/GH or any EU country.
"""

from .base import CountryConfig, RssSource, register


KENYA = CountryConfig(
    code="ke",
    name_en="Kenya",
    name_local="Kenya",

    authority_short="KEBS",
    authority_full="Kenya Bureau of Standards",
    authority_domain="kebs.org",
    authority_item_url_regex=r"^\b$",   # never matches — KEBS has no per-recall item pages
    authority_index_url="",

    news_authority_mode=True,

    # ── News sources (SIGNAL + record URL in news-authority mode) ───────────
    # English-language Kenyan outlets. These are the ONLY accepted record-URL
    # hosts (whitelist enforced by the gate).
    rss_sources=[
        RssSource("nation.africa", [
            "https://nation.africa/kenya/rss",
        ]),
        RssSource("standardmedia.co.ke", [
            "https://www.standardmedia.co.ke/rss/headlines.php",
        ]),
    ],
    google_news_domains=[
        "kebs.org",            # authority (statements occasionally linkable)
        "nation.africa", "standardmedia.co.ke", "citizen.digital",
        "the-star.co.ke", "businessdailyafrica.com", "kenyans.co.ke",
        "tuko.co.ke", "capitalfm.co.ke", "people.co.ke", "kbc.co.ke",
    ],
    google_news_keywords=[
        '"KEBS" recall food OR product Kenya',
        '"Kenya Bureau of Standards" recall OR withdraw OR seize',
        'aflatoxin recall Kenya peanut butter OR maize',
        '"Ministry of Health" Kenya food recall contaminated',
        'Kenya food recall "remove from shelves" OR "stop consuming"',
    ],

    bulk_index_queries=[
        "Kenya KEBS recall food aflatoxin OR Salmonella OR Listeria 2026",
        "Kenya Bureau of Standards product recall contaminated food",
    ],

    language_name="English",
    language_code="en",
    brand_handling_note=(
        "Kenyan recalls are reported in English. Preserve brand/manufacturer "
        "casing (e.g. Nutie, Kismat, Pannaj, Muleka peanut butter; Fresh Fri, "
        "Rina, Postman edible oils). Aflatoxin in peanut/maize/groundnut is the "
        "dominant hazard — capture ppb levels and batch numbers when present."
    ),

    recall_signal_terms=[
        "recall", "recalled", "withdraw", "withdrawal", "seize", "seizure",
        "destroy", "remove from shelves", "stop consuming", "ban",
        "kebs", "kenya bureau of standards", "ministry of health",
        "aflatoxin", "salmonella", "listeria", "contamination", "toxin",
        "non-compliant", "substandard",
    ],

    timezone="Africa/Nairobi",
    run_local_hour=21,
    # EAT = UTC+3 year-round (no DST). 21:00 local = 18:00 UTC.
    cron_utc_offsets=(18, 18),
)


register(KENYA)
