"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Vietnam (VFA — Cục An toàn thực phẩm, Bộ Y tế)

WHY THIS EXISTS (audit 2026-09-23)
==================================
Vietnam has been dark. The Greek route applies: local media first,
product/date/hazard confirmed from the article, then the article resolved
back to VFA's own page, and that URL is what is published.

VERIFIED 2026-09-23 — a real recall notice:

    /tin-tuc/thu-hoi-san-pham-khong-bao-dam-an-toan-thuc-pham-theo-quyet-
        dinh-so-79qd-attp-ngay-1532025.html
        "Recall of products not meeting food safety requirements under
         Decision 79/QĐ-ATTP of 15/3/2025"

and a real enforcement listing:

    /xu-ly-vi-pham-attp/cong-khai-danh-sach-co-so-vi-pham-hanh-chinh-ve-an-
        toan-thuc-pham-cap-nhat-tu-0142025-den-0172025.html

THE TRAP ON THIS SITE. Section landing pages have exactly the same shape as
items — a slug ending .html directly under a section:

    /thanh-kiem-tra/xu-ly-vi-pham-ve-an-toan-thuc-pham.html

is the "Handling of food safety violations" SECTION, not a notice, and its
slug reads like one. There is no structural way to tell it apart, so the
regex takes the two sections that carry notices (tin-tuc, the news board,
and xu-ly-vi-pham-attp, the published violations board) and excludes
thanh-kiem-tra, which is the inspection section whose landing page is the
trap above. That costs nothing: notices posted there are cross-posted to
tin-tuc.

Vietnamese recalls are issued as a numbered Decision — Quyết định số
<n>/QĐ-ATTP — and the number and its date are in the title and usually in
the slug. That number is the durable identifier for the action and belongs
in the record.

The provincial Chi cục sites (langson.vfa.gov.vn and its siblings) run on
subdomains of the same host with a completely different Drupal URL shape
(/node/<id>). They are accepted as context domains but not as item URLs:
a provincial node id is not stable across their periodic site rebuilds.
"""

from .base import CountryConfig, RssSource, register


VIETNAM = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="vn",
    name_en="Vietnam",
    name_local="Việt Nam",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="VFA",
    authority_full="Cục An toàn thực phẩm (Vietnam Food Administration), Bộ Y tế",
    authority_domain="vfa.gov.vn",
    # Two notice-carrying sections only. See the docstring for the
    # thanh-kiem-tra landing page this deliberately excludes.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:tin-tuc|xu-ly-vi-pham-attp)/"
        r"[a-z0-9][a-z0-9\-]+\.html$"
    ),
    authority_index_url="https://vfa.gov.vn/tin-tuc",

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        RssSource("vnexpress.net", [
            "https://vnexpress.net/rss/suc-khoe.rss",
            "https://vnexpress.net/rss/thoi-su.rss",
        ]),
        RssSource("tuoitre.vn", ["https://tuoitre.vn/rss/suc-khoe.rss"]),
        RssSource("thanhnien.vn", ["https://thanhnien.vn/rss/suc-khoe.rss"]),
    ],
    google_news_domains=[
        "vnexpress.net", "tuoitre.vn", "thanhnien.vn", "vietnamnet.vn",
        "dantri.com.vn", "nhandan.vn", "laodong.vn", "tienphong.vn",
        "suckhoedoisong.vn", "vtv.vn", "baochinhphu.vn", "zingnews.vn",
    ],
    google_news_keywords=[
        "thu hồi sản phẩm thực phẩm",
        "thu hồi thực phẩm không an toàn",
        "cảnh báo an toàn thực phẩm",
        "Vietnam food recall",
        "Cục An toàn thực phẩm thu hồi",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:vfa.gov.vn thu hồi sản phẩm 2026",
        "site:vfa.gov.vn thu hồi thực phẩm không bảo đảm an toàn",
        "site:vfa.gov.vn quyết định thu hồi QĐ-ATTP",
        "site:vfa.gov.vn cảnh báo sản phẩm thực phẩm",
        "site:vfa.gov.vn xử lý vi phạm an toàn thực phẩm 2026",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Vietnamese",
    language_code="vi",
    brand_handling_note=(
        "Vietnamese is written with diacritics and they are part of the "
        "name — keep company and product names fully accented exactly as "
        "published, never stripped to ASCII. Company names normally carry "
        "Công ty TNHH (limited liability) or Công ty Cổ phần (joint stock); "
        "keep the form. Recalls are issued as a numbered decision, Quyết "
        "định số <n>/QĐ-ATTP dated <d/m/yyyy> — capture that number and its "
        "date, since it is the durable identifier for the action and the "
        "product list often sits in an annex rather than the notice body."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "thu hồi", "cảnh báo", "vi phạm", "không bảo đảm an toàn",
        "an toàn thực phẩm", "ngộ độc", "chất cấm", "tiêu hủy",
        "đình chỉ lưu thông", "quyết định thu hồi",
        "recall", "food safety",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # ICT = UTC+7, no DST, so both offsets are identical.
    timezone="Asia/Ho_Chi_Minh",
    run_local_hour=21,
    cron_utc_offsets=(14, 14),
)

register(VIETNAM)
