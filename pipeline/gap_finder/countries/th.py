"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Thailand (Thai FDA — สำนักงานคณะกรรมการอาหารและยา, Food Division)

WHY THIS EXISTS (2026-09-30)
============================
Operator request: Thailand sends the site a visitor a day and the register
has never held a Thai-authority row. Found the Greek way: local media first,
product/date/hazard confirmed from the article, then resolved back to the
Thai FDA's own page, and that page is what gets published.

URL SHAPE — VERIFIED 2026-09-30 against the Food Division's consumer-alert
board (food.fda.moph.go.th/consumer-alertnews/category/verification-results-2569),
not guessed. Each alert is ONE product, published as a PDF served through
media.php, named <BE-year>_<month>_<x>.pdf (69 = B.E. 2569 = 2026 CE):

    item  https://food.fda.moph.go.th/media.php?id=876713488913932288&name=69_03_Salmon.pdf
          smoked salmon, Listeria monocytogenes (March 2026)
    item  https://food.fda.moph.go.th/media.php?id=927456901359345664&name=69_08_01.pdf
          E. coli detected (August 2026)
    item  https://food.fda.moph.go.th/media.php?id=943030778982440960&name=69_09_01.pdf
          supplement with undeclared sildenafil (September 2026)

media.php serves EVERY file on the site — laws, forms, guides — so the regex
does not accept media.php alone: it requires the verification-result file
name, two digits, underscore, two digits, underscore. The board page itself
(/consumer-alertnews/category/...) is the index and never matches.

An earlier draft of this file accepted /press-release/<slug>. Those pages are
regulations and announcements, not alerts; that pattern was wrong and was
caught before it shipped. (RESEARCH-NOTES.md recorded a third shape,
/news/<percent-encoded Thai>, with no verified recall behind it.)

The Food Division (food.fda.moph.go.th) is a subdomain of fda.moph.go.th,
so the primary authority_domain covers it by suffix.
"""

from .base import CountryConfig, RssSource, register


THAILAND = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="th",
    name_en="Thailand",
    name_local="ประเทศไทย",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="Thai FDA",
    authority_full="สำนักงานคณะกรรมการอาหารและยา (Food and Drug Administration, Thailand)",
    authority_domain="fda.moph.go.th",
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/media\.php\?(?:.*&)?name=\d{2}_\d{2}_[^&/]*\.pdf"
    ),
    authority_index_url=(
        "https://food.fda.moph.go.th/consumer-alertnews/category/"
        "verification-results-2569"
    ),

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        RssSource("bangkokpost.com", ["https://www.bangkokpost.com/rss/data/topstories.xml"]),
        RssSource("thairath.co.th", ["https://www.thairath.co.th/rss/news"]),
        RssSource("matichon.co.th", ["https://www.matichon.co.th/feed"]),
    ],
    google_news_domains=[
        "bangkokpost.com", "nationthailand.com", "thairath.co.th",
        "matichon.co.th", "khaosod.co.th", "thaipbs.or.th", "pptvhd36.com",
        "mgronline.com", "dailynews.co.th", "posttoday.com", "sanook.com",
        "hfocus.org",
    ],
    google_news_keywords=[
        "อย. เรียกคืน อาหาร",
        "อย. เตือน อาหาร ปนเปื้อน",
        "ตรวจพบเชื้อ อาหาร อย.",
        "Thai FDA food recall",
        "Thailand food recall contamination",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:food.fda.moph.go.th media.php 69_",
        "site:food.fda.moph.go.th ผลการตรวจพิสูจน์อาหาร 2569",
        "site:food.fda.moph.go.th consumer-alertnews",
        "site:fda.moph.go.th ตรวจพบเชื้อ อาหาร",
        "site:fda.moph.go.th ซิลเดนาฟิล อาหาร",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Thai",
    language_code="th",
    brand_handling_note=(
        "Thai company and brand names are written in Thai script and often "
        "carry บริษัท ... จำกัด (company limited) or ห้างหุ้นส่วน (partnership). "
        "Keep the name exactly as published — do NOT romanise. Thai FDA "
        "identifies registered food by a 13-digit อย. number (เลขสารบบอาหาร); "
        "that number belongs in the product/identifier field. Thai dates are "
        "often Buddhist Era (พ.ศ. 2569 = 2026 CE): convert to CE."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "เรียกคืน", "เรียกเก็บ", "ระงับการจำหน่าย", "ปนเปื้อน",
        "ตรวจพบเชื้อ", "เตือนภัย", "อย. เตือน", "ห้ามจำหน่าย",
        "อาหารเป็นพิษ", "เชื้อรา",
        "recall", "recalled", "thai fda",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # ICT = UTC+7, no DST.
    timezone="Asia/Bangkok",
    run_local_hour=21,
    cron_utc_offsets=(14, 14),
)

register(THAILAND)
