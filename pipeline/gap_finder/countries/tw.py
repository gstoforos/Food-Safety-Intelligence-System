"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Taiwan (TFDA — 衛生福利部食品藥物管理署)

WHY THIS EXISTS (audit 2026-09-23)
==================================
Taiwan has never placed a row. The Greek route applies: local media first,
product/date/hazard confirmed from the article, then the article resolved
back to the regulator's own page, and that page's URL is what is published.

VERIFIED 2026-09-23 — real item URLs from the 本署新聞 board:

    /tc/newsContent.aspx?cid=4&id=t634701
    /tc/newsContent.aspx?cid=4&id=t634692
    /tc/newsContent.aspx?cid=4&id=31722

against the listing, whose URL differs by seven characters:

    /tc/news.aspx?cid=4

news.aspx vs newsContent.aspx is the entire distinction between the index
and an item here, so the regex requires newsContent and an id. Note the id
takes both shapes — bare digits on older items, a "t" prefix on newer ones —
and both are live, so the pattern accepts either. cid is the board (4 =
agency news) and is left unpinned; consumer-facing recall notices appear
under more than one board.

The consumer product-recall portal consumer.fda.gov.tw/GMP/Product.aspx
lists recalls in a table with no per-recall page, so it is accepted as a
context domain but never as an item URL.

Taiwan publishes in Traditional Chinese. Keep that in mind reading the
brand note below: the Simplified/Traditional distinction is not cosmetic
here, and a Simplified rendering of a Taiwanese brand is wrong.
"""

from .base import CountryConfig, RssSource, register


TAIWAN = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="tw",
    name_en="Taiwan",
    name_local="臺灣",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="TFDA",
    authority_full="衛生福利部食品藥物管理署 (Taiwan Food and Drug Administration)",
    authority_domain="fda.gov.tw",
    # Host-optional prefix; the id lives in the query string, which IS part
    # of the string matched. See base.py.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:tc|eng|ENG|TC)/newsContent\.aspx\?"
        r"(?:.*&)?id=[a-zA-Z]?\d+"
    ),
    authority_index_url="https://www.fda.gov.tw/tc/news.aspx?cid=4",
    authority_domains_extra=["consumer.fda.gov.tw"],

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        RssSource("cna.com.tw", [
            "https://feeds.feedburner.com/rsscna/social",
            "https://feeds.feedburner.com/rsscna/lifehealth",
        ]),
        RssSource("ltn.com.tw", ["https://news.ltn.com.tw/rss/life.xml"]),
        RssSource("udn.com", ["https://udn.com/rssfeed/news/2/6638?ch=news"]),
    ],
    google_news_domains=[
        "cna.com.tw", "udn.com", "ltn.com.tw", "chinatimes.com",
        "setn.com", "ettoday.net", "tvbs.com.tw", "nownews.com",
        "taipeitimes.com", "focustaiwan.tw", "storm.mg", "cts.com.tw",
    ],
    google_news_keywords=[
        "食品 回收 下架",
        "食藥署 邊境查驗 不合格",
        "食品 預防性下架",
        "Taiwan food recall",
        "食品 違規 退運銷毀",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:fda.gov.tw 食品 回收 下架 2026",
        "site:fda.gov.tw 邊境查驗不合格 食品",
        "site:fda.gov.tw newsContent 食品 回收",
        "site:fda.gov.tw 預防性下架",
        "site:fda.gov.tw 食品 違規 退運",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Traditional Chinese",
    language_code="zh-TW",
    brand_handling_note=(
        "Taiwan publishes in TRADITIONAL Chinese. Keep company and brand "
        "names in Traditional characters exactly as published — do NOT "
        "convert to Simplified and do NOT romanise. Company names commonly "
        "end in 股份有限公司 or 有限公司; keep the suffix. Imported products "
        "carry the importer's name as well as the foreign brand — the "
        "RECALLING party is the importer named in the notice, and the foreign "
        "brand belongs in the product field. Border-rejection notices "
        "(邊境查驗不合格) name the importer and the source country; they are "
        "rejections at import, not market recalls, and should be recorded as "
        "such rather than as consumer recalls."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "回收", "下架", "預防性下架", "召回", "退運", "銷毀",
        "食藥署", "邊境查驗", "不合格", "違規",
        "停止販售", "停售", "食安",
        "recall", "recalled",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # CST = UTC+8, no DST since 1979, so both offsets are identical.
    timezone="Asia/Taipei",
    run_local_hour=21,
    cron_utc_offsets=(13, 13),
)

register(TAIWAN)
