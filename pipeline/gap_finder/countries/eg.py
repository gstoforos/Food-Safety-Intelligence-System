"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Egypt (NFSA — National Food Safety Authority).

⚠ NEWS-AUTHORITY MODE (news_authority_mode=True).
VERIFIED 2026-06: NFSA (nfsa.gov.eg) is an Arabic SharePoint-style information
portal (about / laws / white-list / departments) with NO per-recall HTML pages
to link to. Real NFSA recalls (e.g. the Jan-2026 Nestlé NAN Comfort 1 / OptiPro 1
cereulide/Bacillus-cereus infant-formula recall) reach the public ONLY through
quality English news + government portals — they are NOT published as linkable
per-recall pages on NFSA's own site.

Because there is no authority URL to point at, this country runs in
news-authority mode: the gate accepts the NEWS article URL (from the curated
whitelist below) as the record URL, provided the record classifies at tier 1/2.
Cross-outlet duplicates collapse via dedupe_by_recall_identity downstream.

This is a deliberate, scoped exception to the authority-pure rule, used only for
portal-less countries. It does NOT affect GR/ZA/NG/GH or any EU country.
"""

from .base import CountryConfig, RssSource, register


EGYPT = CountryConfig(
    code="eg",
    name_en="Egypt",
    name_local="مصر",

    authority_short="NFSA",
    authority_full="National Food Safety Authority (Egypt)",
    # No per-recall portal — kept for labelling/Source only. The gate will not
    # find authority URLs here; news_authority_mode supplies the record URL.
    authority_domain="nfsa.gov.eg",
    authority_item_url_regex=r"^\b$",   # never matches — NFSA has no item pages
    authority_index_url="",

    news_authority_mode=True,

    # ── News sources (SIGNAL + record URL in news-authority mode) ───────────
    # English-language Egyptian outlets + government news portals. These are the
    # ONLY accepted record-URL hosts (whitelist enforced by the gate).
    rss_sources=[
        RssSource("english.ahram.org.eg", [
            "https://english.ahram.org.eg/rss/Portal/0.aspx",
        ]),
    ],
    google_news_domains=[
        "english.ahram.org.eg", "ahram.org.eg",
        "cairo.gov.eg",
        "egyptindependent.com", "dailynewsegypt.com",
        "egypttoday.com", "see.news", "sis.gov.eg",
    ],
    google_news_keywords=[
        '"NFSA" recall Egypt food OR formula',
        '"National Food Safety Authority" recall OR withdrawal',
        '"Listeria" OR Salmonella OR cereulide Egypt recall',
        'Egypt food recall contamination NFSA',
        'Egypt "precautionary recall" product',
    ],

    bulk_index_queries=[
        "Egypt NFSA recall food 2026 Listeria OR Salmonella OR cereulide",
        "Egypt National Food Safety Authority product recall contamination",
    ],

    language_name="English",
    language_code="en",
    brand_handling_note=(
        "Egyptian recalls are reported in English by Ahram Online and government "
        "portals. Preserve brand/manufacturer casing (e.g. Nestlé Egypt, NAN "
        "Comfort 1). Many are multinational recalls (Nestlé, Danone) reaching "
        "Egypt; capture the NFSA decision context and batch numbers when present."
    ),

    recall_signal_terms=[
        "recall", "recalled", "precautionary recall", "withdraw", "withdrawal",
        "nfsa", "national food safety authority", "pulled from shelves",
        "salmonella", "listeria", "cereulide", "bacillus cereus",
        "aflatoxin", "contamination", "undeclared allergen", "toxin",
    ],

    timezone="Africa/Cairo",
    run_local_hour=21,
    # EET = UTC+2 year-round (Egypt has no DST since 2014; reinstated briefly but
    # currently no observed summer shift for our purposes). 21:00 local = 19:00 UTC.
    cron_utc_offsets=(19, 19),
)


register(EGYPT)
