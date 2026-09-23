"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Saudi Arabia (SFDA — Saudi Food and Drug Authority)

WHY THIS EXISTS (audit 2026-09-23)
==================================
Saudi Arabia has been dark. The Greek route applies: local media first,
product/date/hazard confirmed from the article, then the article resolved
back to SFDA's own page, and that URL is what is published.

VERIFIED 2026-09-23 — real item URLs, numeric ids on the news board:

    sfda.gov.sa/en/news/2683516
        "Voluntary Recall: Safeguarding Consumers and Ensuring Food Safety
         and Quality"
    sfda.gov.sa/en/news/17638      (the same story on the older site)

The drugs board is a SEPARATE path and is deliberately excluded:

    sfda.gov.sa/en/drugscircularsandwithdrawal/89167
    sfda.gov.sa/en/drugs-circulars-withdrawal        (its listing)

SFDA regulates food, drugs, devices and cosmetics, and only the food
actions belong in this register. Scoping the regex to /news/ rather than
taking any numbered SFDA page is what keeps pharmaceutical recalls — which
have their own board and their own numbering — out of a food register.

Arabic and English are both first-class here: SFDA publishes each notice at
/ar/news/<id> and /en/news/<id>, so the regex takes either. The Saudi Press
Agency (spa.gov.sa) carries the same announcements and is in the news
domains below; it is a news outlet for this purpose, not the authority, and
an SPA URL is never accepted as the record URL.
"""

from .base import CountryConfig, RssSource, register


SAUDI_ARABIA = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="sa",
    name_en="Saudi Arabia",
    name_local="المملكة العربية السعودية",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="SFDA",
    authority_full="الهيئة العامة للغذاء والدواء (Saudi Food and Drug Authority)",
    authority_domain="sfda.gov.sa",
    # /news/ only — the drugs board has its own path and stays out. Both
    # language prefixes are live.
    authority_item_url_regex=r"^(?:https?://[^/]+)?/(?:en|ar)/news/\d+",
    authority_index_url="https://www.sfda.gov.sa/en/news",

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        RssSource("spa.gov.sa", ["https://www.spa.gov.sa/rss.xml"]),
        RssSource("arabnews.com", ["https://www.arabnews.com/rss.xml"]),
        RssSource("okaz.com.sa", ["https://www.okaz.com.sa/rss.xml"]),
    ],
    google_news_domains=[
        "spa.gov.sa", "arabnews.com", "saudigazette.com.sa",
        "alarabiya.net", "okaz.com.sa", "aleqt.com", "sabq.org",
        "alriyadh.com", "alyaum.com", "alwatan.com.sa", "argaam.com",
    ],
    google_news_keywords=[
        "استدعاء منتج غذائي",
        "الغذاء والدواء سحب منتج",
        "استدعاء طوعي غذاء",
        "SFDA food recall",
        "Saudi food recall",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:sfda.gov.sa استدعاء منتج غذائي 2026",
        "site:sfda.gov.sa voluntary recall food",
        "site:sfda.gov.sa news recall food product",
        "site:sfda.gov.sa سحب منتج غذائي",
        "site:sfda.gov.sa food recall 2026",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Arabic",
    language_code="ar",
    brand_handling_note=(
        "SFDA publishes in Arabic and English. Where an English notice "
        "exists, prefer its spelling of the company and brand. Where only "
        "Arabic exists, keep the Arabic exactly as published and do NOT "
        "transliterate — a transliterated Arabic brand will not match the "
        "same brand seen again later. Saudi company names commonly carry "
        "شركة (sharikah, 'company') as a prefix; keep it. Most SFDA food "
        "actions are استدعاء طوعي — a voluntary recall initiated by the "
        "company — so the company named is the recalling party, not a "
        "target of enforcement."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "استدعاء", "سحب", "استدعاء طوعي", "تحذير",
        "الغذاء والدواء", "منتج غذائي", "غذائية", "تلوث",
        "recall", "recalled", "withdrawal", "sfda",
        "food safety", "do not consume",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # AST = UTC+3, no DST, so both offsets are identical.
    timezone="Asia/Riyadh",
    run_local_hour=21,
    cron_utc_offsets=(18, 18),
)

register(SAUDI_ARABIA)
