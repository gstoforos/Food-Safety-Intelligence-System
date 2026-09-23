"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Singapore (SFA — Singapore Food Agency)

WHY THIS EXISTS (audit 2026-09-23)
==================================
scrapers/asia/sfa_sg.py has run every day since it was written and placed
its last row in the register on 2026-06-25. It fetches
sfa.gov.sg/news-publications/newsroom directly, and a direct fetch is the
one thing a datacentre IP is least likely to be allowed to do.

That is the same wall EFET put up in Greece, and Greece is already solved:
find the recall in LOCAL MEDIA, confirm the product, date and hazard from
the article, then resolve the article back to the regulator's own page and
publish THAT url. The authority-URL gate stays absolute; only the route to
it changes.

SFA suits the Greek route well, because it does publish a permanent page
per recall — verified 2026-09-23:

    /news-publications/newsroom/recall-of-coolibah-herbs-gourmet-salad-mix-
        due-to-exceeding-levels-of-bacillus-cereus
    /news-publications/newsroom/2025/recall-of-various-tasti-brand-products-
        due-to-possible-presence-of-metal-pieces
    /news-publications/newsroom/recall-of-two-additional-infant-formula-
        products-due-to-presence-of-cereulide-toxin

Two shapes — with and without a year segment — and the slug always carries
"recall". news_authority_mode stays FALSE: a real per-recall page exists,
so no news URL may ever enter Recalls.
"""

from .base import CountryConfig, RssSource, register


SINGAPORE = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="sg",
    name_en="Singapore",
    name_local="Singapore",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="SFA",
    authority_full="Singapore Food Agency",
    authority_domain="sfa.gov.sg",
    # Both observed shapes, and the slug must contain "recall" — the
    # newsroom also carries licensing notices and annual reports, and a
    # bare /newsroom/<slug> pattern would take those too.
    authority_item_url_regex=r"news-publications/newsroom/(?:\d{4}/)?[a-z0-9\-]*recall",
    authority_index_url="https://www.sfa.gov.sg/news-publications/newsroom",

    # ── News sources ────────────────────────────────────────────────────────
    # Singapore recalls are reported quickly and in English by the national
    # outlets. CNA and The Straits Times carry the SFA link in the body,
    # which is what the Tier-2 resolver needs.
    rss_sources=[
        RssSource("channelnewsasia.com", [
            "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml",
            "https://www.channelnewsasia.com/rssfeeds/8395986",
        ]),
        RssSource("straitstimes.com", [
            "https://www.straitstimes.com/news/singapore/rss.xml",
        ]),
        RssSource("mothership.sg", ["https://mothership.sg/feed/"]),
        RssSource("asiaone.com", ["https://www.asiaone.com/rss.xml"]),
    ],
    google_news_domains=[
        "channelnewsasia.com", "straitstimes.com", "mothership.sg",
        "asiaone.com", "todayonline.com", "businesstimes.com.sg",
        "shin.min.sg", "stomp.straitstimes.com",
    ],
    google_news_keywords=[
        "SFA recall",
        "Singapore Food Agency recall",
        "food recall Singapore",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:sfa.gov.sg recall 2026",
        "site:sfa.gov.sg recall 2025",
        "site:sfa.gov.sg newsroom recall of",
        "site:sfa.gov.sg recall due to presence",
        "site:sfa.gov.sg recall Listeria Salmonella",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="English",
    language_code="en",
    brand_handling_note=(
        "Singapore recalls name brands in Latin script; keep them exactly as "
        "published. Chinese-character brand names appear occasionally — keep "
        "those in the original and do not transliterate."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "recall", "recalled", "recalls", "recalling",
        "sfa", "singapore food agency",
        "withdraw", "withdrawn", "withdrawal",
        "food alert", "do not consume", "stop consuming",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # SGT = UTC+8, no DST, so both offsets are identical.
    timezone="Asia/Singapore",
    run_local_hour=21,
    cron_utc_offsets=(13, 13),
)

register(SINGAPORE)
