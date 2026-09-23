"""
AFTS Food Safety Intelligence — Gap Finder
Country config: United Arab Emirates (MOCCAE)

WHY THIS EXISTS (audit 2026-09-23)
==================================
The UAE has been dark. The Greek route applies: local media first,
product/date/hazard confirmed from the article, then the article resolved
back to the ministry's own page, and that URL is what is published.

WHICH AUTHORITY — and this one is genuinely split three ways:
  * MOCCAE (Ministry of Climate Change and Environment) issues FEDERAL
    food recalls and is the body that orders a product off the national
    market.
  * Dubai Municipality and ADAFSA (Abu Dhabi Agriculture and Food Safety
    Authority) run their own emirate-level food safety regimes and publish
    their own actions.
  * The Ministry of Economy runs a general consumer product recall page
    covering non-food goods.
MOCCAE is the authority here because it is the federal one and the one
whose actions apply nationally. The emirate authorities are accepted as
context domains so that an article linking them is not disqualified, but a
federal record needs the federal URL.

VERIFIED 2026-09-23 — a real recall:

    /en/media-center/news/8/4/2022/ministry-of-climate-change-and-
        environment-recalls-kinder-surprise-uovo-maxi-chocolate-from-uae-mark

The date is in the path as day/month/year — note the ORDER, which is
day-first, unlike the year-first paths used by Brazil and Chile. Getting
that backwards would build a regex that matches nothing for eleven months
of the year and then quietly starts working.

Two pages that must NOT match, both on the same host:
    /en/knowledge/food-safety
    /en/knowledge-and-statistics/food-safety.aspx
Both are standing guidance pages about food safety, and both would be read
as recalls by a pattern that keyed on the words rather than the structure.
Requiring the media-center news path plus a date is what excludes them.
"""

from .base import CountryConfig, RssSource, register


UAE = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="ae",
    name_en="United Arab Emirates",
    name_local="الإمارات العربية المتحدة",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="MOCCAE",
    authority_full=(
        "وزارة التغير المناخي والبيئة "
        "(Ministry of Climate Change and Environment)"
    ),
    authority_domain="moccae.gov.ae",
    # day/month/year — day first. See the docstring.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:en|ar)/media-center/news/"
        r"\d{1,2}/\d{1,2}/\d{4}/[a-z0-9][a-z0-9\-]+"
    ),
    authority_index_url="https://www.moccae.gov.ae/en/media-center/news.aspx",
    # Emirate-level regulators and the consumer-goods recall register.
    # Linkable context; never the record URL for a federal action.
    authority_domains_extra=["adafsa.gov.ae", "dm.gov.ae", "moec.gov.ae"],

    # ── News sources ────────────────────────────────────────────────────────
    # WAM is the state news agency and carries ministry announcements first;
    # the English dailies then report them with the ministry link.
    rss_sources=[
        RssSource("wam.ae", ["https://www.wam.ae/en/feed/rss"]),
        RssSource("gulfnews.com", ["https://gulfnews.com/rss?generatorName=uae"]),
        RssSource("khaleejtimes.com", ["https://www.khaleejtimes.com/rss"]),
    ],
    google_news_domains=[
        "wam.ae", "gulfnews.com", "thenationalnews.com", "khaleejtimes.com",
        "arabianbusiness.com", "emaratalyoum.com", "albayan.ae",
        "alkhaleej.ae", "dubaieye1038.com", "timeoutdubai.com",
        "zawya.com",
    ],
    google_news_keywords=[
        "UAE food recall MOCCAE",
        "UAE recalls food product",
        "استدعاء منتج غذائي الإمارات",
        "سحب منتج غذائي من الأسواق",
        "UAE food safety alert recall",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:moccae.gov.ae recall food product 2026",
        "site:moccae.gov.ae media center news recalls",
        "site:moccae.gov.ae withdraws food market",
        "site:moccae.gov.ae استدعاء منتج غذائي",
        "site:moccae.gov.ae food recall contamination",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Arabic",
    language_code="ar",
    brand_handling_note=(
        "MOCCAE publishes the same notice in English and Arabic. Where the "
        "English version exists, prefer its spelling of company and brand — "
        "UAE recalls overwhelmingly concern IMPORTED products whose brands "
        "have an established Latin-script name (Kinder, Aptamil, Nestlé), "
        "and the Arabic page transliterates them. Do NOT keep a "
        "transliteration when the original brand is identifiable. Note that "
        "the company named is usually the local IMPORTER or distributor, "
        "distinct from the international manufacturer named in the product; "
        "record both where the notice gives both."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "recall", "recalls", "recalled", "withdraw", "withdrawn",
        "food safety", "contaminated", "do not consume", "unsafe",
        "استدعاء", "سحب", "منتج غذائي", "تحذير", "تلوث",
        "moccae", "ministry of climate change",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # GST = UTC+4, no DST, so both offsets are identical.
    timezone="Asia/Dubai",
    run_local_hour=21,
    cron_utc_offsets=(17, 17),
)

register(UAE)
