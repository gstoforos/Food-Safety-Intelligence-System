"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Japan (CAA — Consumer Affairs Agency, リコール情報サイト)

WHY THIS EXISTS (audit 2026-09-23)
==================================
Japan has been dark. The Greek route applies unchanged: find the recall in
local media, confirm product, date and hazard from the article, resolve the
article back to the regulator's own page, publish that page's URL.

WHICH AUTHORITY. Japan splits food safety across three bodies, and picking
the wrong one is the easiest mistake here:
  * MHLW (厚生労働省) writes the Food Sanitation Act and handles
    poisoning outbreaks — but does not maintain a per-recall register.
  * FSC (食品安全委員会) does risk ASSESSMENT, not recalls.
  * CAA (消費者庁) runs リコール情報サイト, recall.caa.go.jp, the national
    recall register, and food-safety.caa.go.jp/recall for the food-labelling
    recall notifications that became mandatory in 2021.
CAA is therefore the authority for this purpose, and recall.caa.go.jp is
the host that carries one permanent page per recall.

VERIFIED 2026-09-23 — real item URLs:

    /result/detail.php?rcl=00000034744&screenkbn=06
    /result/detail.php?rcl=00000034735&screenkbn=06
    /result/detail.php?rcl=00000034743&screenkbn=01

against the listing, which is the sibling:

    /result/index.php?screenkbn=01&category=1

The recall id is in the QUERY STRING, so the regex requires detail.php plus
a numeric rcl= — matching index.php would admit the listing itself, which
is how a row acquires a page title for a company name. screenkbn is the
product category (06 food, 01 all) and is deliberately NOT pinned: the same
recall is reachable under more than one value.
"""

from .base import CountryConfig, RssSource, register


JAPAN = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="jp",
    name_en="Japan",
    name_local="日本",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="CAA",
    authority_full="消費者庁 (Consumer Affairs Agency) リコール情報サイト",
    authority_domain="caa.go.jp",
    # Host-optional prefix — matched against the full URL in one place and
    # against "path?query" in another. See base.py.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/result/detail\.php\?(?:.*&)?rcl=\d+"
    ),
    authority_index_url="https://www.recall.caa.go.jp/result/index.php?screenkbn=01&category=1",
    # food-safety.caa.go.jp/recall carries the Food Labelling Act recall
    # notifications on a different host under the same agency.
    authority_domains_extra=["recall.caa.go.jp", "food-safety.caa.go.jp"],

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        RssSource("nhk.or.jp", [
            "https://www.nhk.or.jp/rss/news/cat0.xml",
            "https://www.nhk.or.jp/rss/news/cat1.xml",
        ]),
        RssSource("asahi.com", ["https://www.asahi.com/rss/asahi/newsheadlines.rdf"]),
        RssSource("mainichi.jp", ["https://mainichi.jp/rss/etc/flash.rss"]),
    ],
    google_news_domains=[
        "nhk.or.jp", "asahi.com", "yomiuri.co.jp", "mainichi.jp",
        "nikkei.com", "sankei.com", "jiji.com", "kyodo.co.jp",
        "tokyo-np.co.jp", "fnn.jp", "news.tv-asahi.co.jp", "tbs.co.jp",
    ],
    google_news_keywords=[
        "食品 自主回収",
        "リコール 食品",
        "回収 アレルゲン 表示欠落",
        "食品 回収 消費者庁",
        "Japan food recall",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:recall.caa.go.jp 食品 回収 2026",
        "site:recall.caa.go.jp 自主回収 detail",
        "site:recall.caa.go.jp アレルゲン 表示欠落",
        "site:caa.go.jp 食品 回収 2026",
        "site:recall.caa.go.jp 異物混入 回収",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Japanese",
    language_code="ja",
    brand_handling_note=(
        "Japanese company names are written in kanji/kana and normally carry "
        "株式会社 (before or after the name) or 有限会社. Keep the name and the "
        "suffix exactly as published — do NOT romanise and do NOT drop the "
        "suffix. Where the notice gives a katakana rendering of a foreign "
        "brand (コカ・コーラ), keep the katakana; do not back-translate. "
        "Japanese recalls are overwhelmingly 自主回収 — voluntary recalls by "
        "the company — so the company named IS the recalling party."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "回収", "自主回収", "リコール", "販売中止", "返金",
        "アレルゲン", "表示欠落", "異物混入", "賞味期限",
        "食中毒", "消費者庁",
        "recall", "recalled",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # JST = UTC+9, no DST since 1951, so both offsets are identical.
    timezone="Asia/Tokyo",
    run_local_hour=21,
    cron_utc_offsets=(12, 12),
)

register(JAPAN)
