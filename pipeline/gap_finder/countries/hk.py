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
    # (search_verifier). A regex naming the host matches at the first and
    # silently fails at the second.
    #
    # THREE BOARDS, NOT ONE — corrected 2026-09-23, same day as written.
    # The first version of this took /press/<YYYYMMDD>_<n>.html only, and
    # rejected every Food Alert.
    #
    # THE WITNESS I FIRST CITED WAS CONTAMINATED — retracted 2026-09-24.
    # I cited a Pending row carrying
    #     /english/whatsnew/whatsnew_fa/2026_627.html
    # as "CFS orders recall of US raw oysters after excessive E. coli".
    # That row's URL was WRONG. 2026_627 is "CFS finds trace amount of
    # formaldehyde in prepackaged rice vermicelli sample" — a different
    # alert entirely. The oysters notice is /english/press/20260921_12610
    # .html, and url_resurrect corrected the row to it the next morning.
    #
    # The CHANGE still stands, on evidence that does not depend on that
    # row. Four rows already PUBLISHED in Recalls sit on this board:
    #     whatsnew_fa/2026_628.html   French brie, three batches
    #     whatsnew_fa/2026_614.html   bottled apple juice drink
    #     whatsnew_fa/2026_611.html   powdered infant formula
    #     whatsnew_fa/2026_610.html   infant and young children formula
    # plus fourteen more on the Food Incident Post board below. A
    # press-only pattern refuses all eighteen.
    #
    # The lesson is narrower than the fix: a single register row is not
    # evidence, because a row's URL can itself be wrong. Four published
    # rows on one board are.
    #
    # CFS publishes recalls across three boards, and press releases are the
    # LEAST relevant of the three for this register:
    #     /press/<YYYYMMDD>_<n>.html                 press releases
    #     /whatsnew/whatsnew_fa/<YYYY>_<n>.html      Food / Allergy Alerts
    #     /whatsnew/whatsnew_sfpa/<YYYY>_<n>.html    Suspected Food Poisoning
    # Taking press only would have left Hong Kong running every day,
    # finding alerts, and rejecting all of them at the authority gate —
    # the exact silent-empty failure this file's tests exist to prevent.
    #
    # The index/item rule on the whatsnew boards is that an ITEM is
    # <year>_<serial>.html while the LISTING repeats the board name
    # (whatsnew_fa/whatsnew_fa.html). Requiring the year_serial shape is
    # what keeps the listing out.
    # FOUR boards, not three — corrected again 2026-09-23 against the live
    # register, which holds 14 published Recalls rows on a shape the
    # three-board pattern still rejected:
    #     /english/rc/subject/files/<YYYYMMDD>_<n>.pdf
    # These are CFS Food Incident Posts (indexed at rc/subject/fi_list.html)
    # and they are published as PDFs rather than HTML pages. A PDF is
    # accepted here because it is a permanent, per-incident document on the
    # authority's own host — the authority-URL guarantee is about WHO
    # published it and WHETHER it addresses one incident, not about the
    # file format.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:english|tc_chi|sc_chi)/"
        r"(?:press/\d{8}_\d+\.html"
        r"|whatsnew/whatsnew_(?:fa|sfpa)/\d{4}_\d+\.html"
        r"|rc/subject/files/\d{8}_\d+\.pdf)"
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
