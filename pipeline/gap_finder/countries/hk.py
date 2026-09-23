"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Hong Kong (CFS — Centre for Food Safety)

WHY THIS EXISTS (audit 2026-09-23)
==================================
scrapers/asia/cfs_hk.py is the ONE scraper out of 33 covering Asia, Latin
America, the Middle East and Africa that placed a row in the last 45 days
(2026-09-09). It is the least dead of the set, and it still produces a
trickle: 8 CFS rows in a 1,764-row register.

The Greek route is added alongside it rather than instead of it. The two
find the same recalls by different paths and dedupe on URL, so a week
where the direct fetch is blocked is a week the news route still covers.

CFS gives the cleanest per-recall URL of any authority in this batch —
verified 2026-09-23:

    https://www.cfs.gov.hk/english/press/20260416_12332.html

A date and a serial. No slug to get wrong, no year-segment variant.
news_authority_mode stays FALSE.
"""

from .base import CountryConfig, RssSource, register


HONG_KONG = CountryConfig(
    code="hk",
    name_en="Hong Kong",
    name_local="香港",

    authority_short="CFS",
    authority_full="Centre for Food Safety (食物安全中心)",
    authority_domain="cfs.gov.hk",
    # /english/press/YYYYMMDD_NNNNN.html — also /tc_chi/ and /sc_chi/ for
    # the Chinese editions of the same notice.
    # HOST-OPTIONAL PREFIX — see tests/test_country_config_conformance.py
    # ::test_the_regex_matches_both_forms_the_pipeline_uses. This regex is
    # applied to TWO different strings: the full URL (authority_url_finder,
    # extractor) and just "path?query" with the netloc stripped
    # (search_verifier, when it filters the bulk index). A regex naming the
    # host therefore matches at the first site and silently fails at the
    # second, which drops every bulk-index hit as a portal page. The
    # "^(?:https?://[^/]+)?" prefix — the idiom gh.py and za.py already
    # used — matches both.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:english|tc_chi|sc_chi)/press/\d{8}_\d+\.html"
    ),
    authority_index_url="https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/whatsnew_fa.html",

    rss_sources=[
        RssSource("news.rthk.hk", [
            "https://rthk9.rthk.hk/rthk/news/rss/e_expressnews_elocal.xml",
        ]),
        RssSource("hongkongfp.com", ["https://hongkongfp.com/feed/"]),
        RssSource("scmp.com", [
            "https://www.scmp.com/rss/2/feed",
            "https://www.scmp.com/rss/91/feed",
        ]),
    ],
    google_news_domains=[
        "scmp.com", "news.rthk.hk", "hongkongfp.com", "thestandard.com.hk",
        "hk01.com", "mingpao.com", "info.gov.hk", "dimsumdaily.hk",
    ],
    google_news_keywords=[
        "CFS food alert Hong Kong",
        "Centre for Food Safety recall",
        "食物安全中心 回收",
    ],

    bulk_index_queries=[
        "site:cfs.gov.hk press food alert 2026",
        "site:cfs.gov.hk press recall 2026",
        "site:cfs.gov.hk urges public not to consume",
        "site:cfs.gov.hk food alert Listeria Salmonella",
        "site:cfs.gov.hk press 2025 recall",
    ],

    language_name="English",
    language_code="en",
    brand_handling_note=(
        "CFS publishes in English, Traditional and Simplified Chinese. Keep "
        "the brand exactly as the English notice writes it; where only a "
        "Chinese name is given, keep the Chinese characters unchanged."
    ),

    recall_signal_terms=[
        "recall", "recalled", "food alert", "cfs",
        "centre for food safety", "urges the public",
        "should not consume", "stop using", "withdraw",
        "回收", "食物安全中心", "食安中心",
    ],

    # HKT = UTC+8, no DST.
    timezone="Asia/Hong_Kong",
    run_local_hour=21,
    cron_utc_offsets=(13, 13),
)

register(HONG_KONG)
