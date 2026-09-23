"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Philippines (FDA Philippines)

WHY THIS EXISTS (audit 2026-09-23)
==================================
scrapers/asia/fda_ph.py stopped placing rows on 2026-06-14 — one of six
collectors that fell silent inside a single fortnight. fda.gov.ph is also
on the measured 403 list: it refuses datacentre traffic outright, the same
as fda.gov, fsis.usda.gov and gov.il. The scraper is not broken; it is
blocked.

That is precisely the case the Greek route was built for. Find the advisory
in local media, confirm product, date and hazard from the article, resolve
the article back to the FDA advisory page, publish that URL.

VERIFIED 2026-09-23 — a real advisory:

    /fda-advisory-no-2026-1226-public-health-warning-against-the-purchase-
        and-consumption-of-the-unregistered-food-product-...-da-la-pian/

The numbering is FDA Advisory No. <year>-<serial>, and the slug carries it
verbatim, which makes the pattern unusually clean: any page whose slug
begins "fda-advisory-no-<year>-<serial>" is a numbered advisory, and
nothing else on the site is.

Two pages that must NOT match, both live on the same site:
    /market-surveillance/                       (the listing)
    /draft-for-comments-guidelines-on-the-recall-of-authorized-health-
        products-regulated-by-the-food-and-drug-administration/
The second is the trap — its slug contains "recall" and it is a policy
consultation document, not a recall. Requiring the advisory NUMBER rather
than the word "recall" is what excludes it.

SCOPE NOTE. FDA Philippines regulates food, drugs, cosmetics and devices
from one advisory series, so the advisory number alone does not tell us the
advisory is about food. That filtering is the extractor's job — it always
was — and recall_signal_terms below keeps the news prefilter on food.
"""

from .base import CountryConfig, RssSource, register


PHILIPPINES = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="ph",
    name_en="Philippines",
    name_local="Pilipinas",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="FDA PH",
    authority_full="Food and Drug Administration Philippines",
    authority_domain="fda.gov.ph",
    # Numbered advisories only. See the docstring for the policy-consultation
    # page this deliberately excludes.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/fda-advisory-no-\d{4}-\d+[a-z0-9\-]*/?$"
    ),
    authority_index_url="https://www.fda.gov.ph/market-surveillance/",

    # ── News sources ────────────────────────────────────────────────────────
    # Philippine outlets report FDA advisories in English within a day and
    # routinely link the advisory page, which is what Tier-2 needs.
    rss_sources=[
        RssSource("inquirer.net", ["https://newsinfo.inquirer.net/feed"]),
        RssSource("gmanetwork.com", [
            "https://data.gmanetwork.com/gno/rss/news/feed.xml",
        ]),
        RssSource("philstar.com", ["https://www.philstar.com/rss/headlines"]),
        RssSource("pna.gov.ph", ["https://www.pna.gov.ph/rss"]),
    ],
    google_news_domains=[
        "inquirer.net", "gmanetwork.com", "philstar.com", "rappler.com",
        "mb.com.ph", "manilatimes.net", "pna.gov.ph", "abs-cbn.com",
        "news5.com.ph", "businessworld.com.ph", "sunstar.com.ph",
    ],
    google_news_keywords=[
        "FDA advisory food recall Philippines",
        "FDA Philippines public health warning food",
        "food recall Philippines",
        "FDA warns unregistered food product",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:fda.gov.ph fda advisory 2026 food",
        "site:fda.gov.ph public health warning food product",
        "site:fda.gov.ph fda advisory recall food",
        "site:fda.gov.ph unregistered food product warning",
        "site:fda.gov.ph advisory 2026 consumption",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="English",
    language_code="en",
    brand_handling_note=(
        "Philippine FDA advisories are written in English and name brands in "
        "Latin script; keep them exactly as published. Imported products are "
        "frequently labelled only in Chinese, Korean or Thai — the advisory "
        "then writes the brand as '(IN FOREIGN LANGUAGE) <transliteration>'. "
        "Keep that rendering as published rather than guessing the original "
        "script. Many advisories concern UNREGISTERED products, where the "
        "distributor is unknown; leave company blank rather than inferring it "
        "from the brand."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    # FDA PH issues advisories across food, drugs, cosmetics and devices from
    # one series, so the food terms here carry the weight.
    recall_signal_terms=[
        "recall", "recalled", "recalls",
        "fda advisory", "public health warning", "health warning",
        "unregistered food", "unregistered product",
        "do not purchase", "do not consume", "stop consuming",
        "food product", "contaminated", "adulterated",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # PHT = UTC+8, no DST, so both offsets are identical.
    timezone="Asia/Manila",
    run_local_hour=21,
    cron_utc_offsets=(13, 13),
)

register(PHILIPPINES)
