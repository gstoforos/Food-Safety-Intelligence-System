"""
AFTS Food Safety Intelligence — Gap Finder
Country config: South Korea (MFDS — Ministry of Food and Drug Safety)

WHY THIS EXISTS (audit 2026-09-23)
==================================
Korea is one of the 33 collectors covering Asia, Latin America, the Middle
East and Africa, of which exactly one placed a row in the register in the
last 45 days. The MFDS portal is a Korean government eGovFrame site; a
direct fetch from a datacentre IP is the least likely request to be served.

So Korea is found the Greek way: local media first, product/date/hazard
confirmed from the article, then the article resolved back to the
regulator's own page — and that regulator page is what gets published.

VERIFIED 2026-09-23. MFDS board items are eGovFrame list.do/view.do pairs,
with the item identity in the QUERY STRING rather than the path:

    /eng/brd/m_61/view.do?seq=192&srchFr=&srchTo=&srchWord=...&page=1
    /eng/brd/m_61/view.do?seq=191&srchFr=&srchTo=&srchWord=...&page=1

while the listing is the sibling /eng/brd/m_61/list.do. The Korean-language
boards use the same shape without the /eng/ segment, and the board number
differs per board (m_61 English news, m_99 Korean press releases, and
others), so the regex takes m_<digits> rather than one board.

Requiring "view.do" plus a numeric seq= is what separates a notice from its
own listing page — the distinction that matters, since an index page
admitted as a recall is how a row ends up with the page title as its
company name.

Korea also runs a dedicated food-safety portal, foodsafetykorea.go.kr
(식품안전나라), whose 회수·판매중지 section is the authoritative recall
register. Its detail pages are session-driven POST results with no stable
linkable URL, so it is NOT accepted as an item URL here; it is listed in
authority_domains_extra only so that a link to it inside an article does
not disqualify the article.
"""

from .base import CountryConfig, RssSource, register


SOUTH_KOREA = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="kr",
    name_en="South Korea",
    name_local="대한민국",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="MFDS",
    authority_full="식품의약품안전처 (Ministry of Food and Drug Safety)",
    authority_domain="mfds.go.kr",
    # Host-optional prefix: this regex is matched against the full URL in
    # authority_url_finder and against "path?query" (netloc stripped) in
    # search_verifier. See base.py.
    #
    # The query string IS part of the string matched, which is what makes a
    # query-identified board item expressible at all.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:eng/)?brd/m_\d+/view\.do\?(?:.*&)?seq=\d+"
    ),
    authority_index_url="https://www.mfds.go.kr/eng/brd/m_61/list.do",
    # See docstring: linkable for context, never accepted as an item URL.
    authority_domains_extra=["foodsafetykorea.go.kr"],

    # ── News sources ────────────────────────────────────────────────────────
    # Korean recalls are carried first by the wires (Yonhap, Newsis) and the
    # dailies, which quote the MFDS announcement and usually link it.
    rss_sources=[
        RssSource("yna.co.kr", [
            "https://www.yna.co.kr/rss/news.xml",
            "https://www.yna.co.kr/rss/society.xml",
        ]),
        RssSource("koreaherald.com", [
            "https://www.koreaherald.com/rss/newsAll.xml",
        ]),
        RssSource("hani.co.kr", [
            "https://www.hani.co.kr/rss/society/",
        ]),
    ],
    google_news_domains=[
        "yna.co.kr", "koreaherald.com", "koreatimes.co.kr",
        "chosun.com", "donga.com", "hani.co.kr", "joongang.co.kr",
        "khan.co.kr", "newsis.com", "kbs.co.kr", "ytn.co.kr",
        "mk.co.kr", "hankyung.com",
    ],
    google_news_keywords=[
        "식품 회수",
        "식약처 회수",
        "판매중지 식품",
        "MFDS food recall",
        "Korea food recall",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:mfds.go.kr 회수 식품 2026",
        "site:mfds.go.kr 회수 판매중지",
        "site:mfds.go.kr 부적합 식품 회수",
        "site:mfds.go.kr recall food 2026",
        "site:mfds.go.kr 식약처 회수 조치",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Korean",
    language_code="ko",
    brand_handling_note=(
        "Korean brand and company names are written in Hangul. Keep them in "
        "Hangul exactly as published — do NOT romanise. Where the notice "
        "itself gives an English name alongside the Hangul (common for "
        "exporters and multinationals), use the published English name and "
        "keep the Hangul in the product field. Company names routinely end "
        "in 주식회사 or (주); keep that suffix."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "회수", "판매중지", "회수조치", "부적합", "식약처",
        "잠정 유통판매 금지", "섭취 중단", "폐기",
        "recall", "recalled", "mfds", "food safety korea",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # KST = UTC+9, no DST since 1988, so both offsets are identical.
    timezone="Asia/Seoul",
    run_local_hour=21,
    cron_utc_offsets=(12, 12),
)

register(SOUTH_KOREA)
