"""
AFTS Food Safety Intelligence — Gap Finder
Country config: China (SAMR — 国家市场监督管理总局,
                State Administration for Market Regulation)

OPERATOR RULING 2026-09-30: "China only confirmed, published officially".
=========================================================================
China enters the register ONLY through notices SAMR itself publishes on
samr.gov.cn. News coverage is a discovery signal and nothing more (the
authority-URL gate already makes that absolute); provincial AMR bulletins
and news-only reports do not qualify.

WHAT SAMR PUBLISHES. The frequent notices are national sampling results —
"市场监管总局办公厅关于N批次食品抽检不合格情况的通报" — each a LIST of 30-50
unqualified batches, verified 2026-09-30:

    https://www.samr.gov.cn/zw/zfxxgk/fdzdgknr/spcjs/art/2026/art_d2739a902a3249d9a8b9300b5e3230d0.html
        40 batches, 2026-05-22

REPRESENTATION (the decision the Turkey note in RESEARCH-NOTES.md asked for):
one row per product in the list whose unqualified item is inside the AFTS
printed scope — a named pathogen (沙门氏菌, 金黄色葡萄球菌, 单核细胞增生李斯特氏菌,
蜡样芽孢杆菌), a mycotoxin (黄曲霉毒素), visible mould (霉变), a pesticide or
veterinary-drug residue, a heavy metal, an undeclared drug. Dated and linked to
the SAMR notice. NEVER an indicator count on its own: 菌落总数 (total plate
count), 大肠菌群 (coliforms) and 霉菌数 (mould COUNT) are hygiene indicators,
not hazards, and the classifier (pipeline/gap_finder/rules.py) deliberately
does not match them. Additive over-use with no hazard class is out, as it is
everywhere else.

URL SHAPE. Every SAMR article is /…/art/<year>/art_<32 hex>.html under a
department path; department listings never end in art_<hex>.html, so that
tail is the whole item/listing distinction.
"""

from .base import CountryConfig, RssSource, register


CHINA = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="cn",
    name_en="China",
    name_local="中国",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="SAMR",
    authority_full="国家市场监督管理总局 (State Administration for Market Regulation)",
    authority_domain="samr.gov.cn",
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:[a-z0-9_]+/)*art/\d{4}/art_[0-9a-f]{32}\.html"
    ),
    authority_index_url="https://www.samr.gov.cn/zw/zfxxgk/fdzdgknr/spcjs/",

    # ── News sources (discovery only — never a record URL) ─────────────────
    rss_sources=[
        RssSource("chinanews.com.cn", ["https://www.chinanews.com.cn/rss/society.xml"]),
        RssSource("people.com.cn", ["http://www.people.com.cn/rss/society.xml"]),
    ],
    google_news_domains=[
        "xinhuanet.com", "news.cn", "people.com.cn", "chinanews.com.cn",
        "thepaper.cn", "cctv.com", "bjnews.com.cn", "cfsn.cn",
        "chinadaily.com.cn", "foodmate.net",
    ],
    google_news_keywords=[
        "市场监管总局 批次食品抽检不合格",
        "食品安全监督抽检 沙门氏菌 不合格",
        "市场监管总局 食品 召回",
        "China SAMR unqualified food samples",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:samr.gov.cn 批次食品抽检不合格情况的通报 2026",
        "site:samr.gov.cn 抽检不合格 沙门氏菌",
        "site:samr.gov.cn 抽检不合格 金黄色葡萄球菌",
        "site:samr.gov.cn 抽检不合格 黄曲霉毒素",
        "site:samr.gov.cn 食品 召回 2026",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Chinese (Simplified)",
    language_code="zh",
    brand_handling_note=(
        "SAMR sampling notices list many products. Produce ONE record per "
        "product whose 不合格项目 (unqualified item) is a named pathogen, a "
        "mycotoxin, visible mould (霉变), a pesticide or veterinary-drug "
        "residue, a heavy metal or an undeclared drug — and NO record for a "
        "product whose only failure is 菌落总数, 大肠菌群, 霉菌数 or a food "
        "additive. Company = 标称生产企业 (the nominal producer), NOT the "
        "被抽样单位 (the sampled seller); Brand = 商标. Keep names exactly as "
        "published in Simplified Chinese; do not romanise or translate them. "
        "Date = the notice's publication date on samr.gov.cn."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "召回", "不合格", "抽检", "通报", "下架", "停止销售",
        "沙门氏菌", "李斯特", "金黄色葡萄球菌", "黄曲霉", "霉变",
        "recall", "samr",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # CST = UTC+8, no DST.
    timezone="Asia/Shanghai",
    run_local_hour=21,
    cron_utc_offsets=(13, 13),
)

register(CHINA)
