"""
AFTS Food Safety Intelligence — Gap Finder
Country config: India (FSSAI — Food Safety and Standards Authority of India).

Module is india.py, not in.py: "in" is a Python keyword (same as Iceland's
"is" -> iceland.py). The registry walks the directory, so nothing else needs
to know.

WHY INDIA (operator 2026-10-05: "build India same concept as Greece")
---------------------------------------------------------------------
The daily global search of 2026-10-05 found exactly one in-window action
outside the EU and allergen-only noise: FSSAI ordering the recall of Everest
cumin powder batch E080668761 (azoxystrobin and thiamethoxam above the
limit), 2026-10-04. It was absent from all five sheets, and no row in the
register has EVER come from an Indian authority — the 12 India rows are all
RASFF border rejections of Indian exports.

⚠ NEWS-AUTHORITY MODE (news_authority_mode=True) — VERIFIED 2026-10-05.
FSSAI publishes no per-recall page for the public:
  * recall records moved into FoSCoS (foscos.fssai.gov.in/food-recall), a
    JavaScript application with no per-record URL;
  * fssai.gov.in/advisories.php and /cms/food-recall.php render as
    JavaScript shells to an automated client;
  * the 2026 recall directions (Wonderland raisins 2026-07-30, Nilgiri Oil /
    Aquagri, Everest cumin 2026-10-04) reached the public only as press notes
    carried by IANS/PTI and the national press — no linkable FSSAI document
    was found for any of them.
FSSAI DID publish one earlier direction as a PDF
(fssai.gov.in/upload/advisories/2024/06/<hash>Recall directions dt. 18-6-24.pdf),
so authority_item_url_regex accepts that shape: when an FSSAI document
exists the gate prefers it. When none exists, the record URL is the
article from the curated national-press whitelist below, exactly as for Kenya
and Egypt — and the record must still classify at tier 1 or 2.
"""

from .base import CountryConfig, RssSource, register


INDIA = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="in",
    name_en="India",
    name_local="भारत",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="FSSAI",
    authority_full="Food Safety and Standards Authority of India",
    authority_domain="fssai.gov.in",
    # FSSAI recall direction PDFs (/upload/advisories/YYYY/MM/<file>.pdf).
    # Path-scoped, host-optional — see base.py for why a regex must not name
    # its host. (Press Information Bureau releases were considered and left
    # out: pib.gov.in is a whole-government umbrella, the same reason gov.br
    # and gob.mx are kept out of the URL guard's escalation.)
    authority_item_url_regex=r"/upload/advisories/\d{4}/\d{2}/[^/?#]*recall[^/?#]*\.pdf",
    authority_index_url="",
    authority_domains_extra=["foscos.fssai.gov.in"],

    news_authority_mode=True,

    # ── News sources (SIGNAL + record URL in news-authority mode) ───────────
    # National English-language outlets. These are the ONLY accepted
    # record-URL hosts when no FSSAI/PIB document exists.
    rss_sources=[
        RssSource("thehindu.com", [
            "https://www.thehindu.com/news/national/feeder/default.rss",
        ]),
        RssSource("indianexpress.com", [
            "https://indianexpress.com/section/india/feed/",
        ]),
        RssSource("hindustantimes.com", [
            "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
        ]),
        RssSource("timesofindia.indiatimes.com", [
            "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms",
        ]),
        RssSource("livemint.com", [
            "https://www.livemint.com/rss/news",
        ]),
    ],
    google_news_domains=[
        "fssai.gov.in",                        # authority (when linkable)
        "thehindu.com", "indianexpress.com", "hindustantimes.com",
        "timesofindia.indiatimes.com", "livemint.com",
        "economictimes.indiatimes.com", "business-standard.com",
        "businesstoday.in", "deccanchronicle.com", "thefederal.com",
        "ndtv.com", "moneycontrol.com",
    ],
    google_news_keywords=[
        '"FSSAI" recall',
        '"FSSAI" directs recall OR "orders recall"',
        '"Food Safety and Standards Authority" recall batch unsafe',
        'FSSAI "unsafe" batch recall pesticide OR aflatoxin OR Salmonella',
        'India food recall "declared unsafe" FSSAI',
    ],

    bulk_index_queries=[
        "FSSAI recall 2026 unsafe batch",
        "FSSAI directs recall product 2026",
        "site:fssai.gov.in recall directions",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="English",
    language_code="en",
    brand_handling_note=(
        "Indian recalls are reported in English. Keep the brand and the "
        "Food Business Operator name as written (e.g. Everest, Wonderland, "
        "Nilgiri Oil and Allied Industries). Batch numbers, the referral-lab "
        "finding and the named contaminant (pesticide residue, aflatoxin, "
        "microbial count) are the facts that matter — capture them exactly."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "fssai", "food safety and standards authority",
        "recall", "recalled", "directs", "withdraw", "withdrawal",
        "unsafe", "sub-standard", "substandard", "batch",
        "pesticide", "aflatoxin", "salmonella", "listeria", "e. coli",
        "contaminat", "adulterat",
        "वापस", "असुरक्षित",                    # Hindi: recall / unsafe
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    timezone="Asia/Kolkata",
    run_local_hour=21,
    # IST = UTC+5:30 year-round (no DST). The fleet schedules by whole UTC
    # hours; the offset rule (21 - 5) puts it at 16:00 UTC = 21:30 IST.
    cron_utc_offsets=(16, 16),
)


register(INDIA)
