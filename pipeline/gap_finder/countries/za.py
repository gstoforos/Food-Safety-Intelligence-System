"""
AFTS Food Safety Intelligence — Gap Finder
Country config: South Africa (NCC — National Consumer Commission, the body that
                publishes product recalls under the Consumer Protection Act 68/2008).

VERIFIED 2026-06 (Greece concept): the NCC publishes each recall as its own
per-item HTML page on thencc.org.za (WordPress), e.g.:
    https://thencc.org.za/media-statement-deli-hummus-range-product-safety-recall/
        (Deli Hummus, Listeria monocytogenes)
    https://thencc.org.za/national-consumer-commission-refers-hummus-supplier-to-the-tribunal-over-listeria-contamination/
Index (recall category, paginated):
    https://thencc.org.za/category/product-recalls/
Each page carries product, hazard (Listeria, aflatoxin, Salmonella, foreign
body), supplier/retailer, batch/sell-by. English — trivial extraction.

NOTE: South Africa has no single food-safety authority publishing per-recall
pages; the NCC is the de-facto national recall publisher (food + non-food). The
classifier filters non-food/non-hazard items (vehicles, e-bikes, hair relaxer).
A secondary host thencc.gov.za is accepted in case some notices live there.

Clean Tier-2 authority_index_url country, like EFET (Greece). Direct GN hits on
thencc.org.za recall posts are caught by Tier-0 (candidate URL is itself authority).
"""

from .base import CountryConfig, RssSource, register


SOUTH_AFRICA = CountryConfig(
    code="za",
    name_en="South Africa",
    name_local="South Africa",

    authority_short="NCC",
    authority_full="National Consumer Commission",
    authority_domain="thencc.org.za",
    # Per-recall WordPress posts are SINGLE root-level descriptive slugs, e.g.
    # /media-statement-deli-hummus-range-product-safety-recall/ . Anchor to the
    # path root (one segment only) so multi-segment taxonomy/nav paths like
    # /category/product-recalls/ or /tag/... never match. Works for both
    # absolute URLs and root-relative hrefs.
    authority_item_url_regex=r"^(?:https?://[^/]+)?/(?!category/|tag/|author/|page/|wp-)[a-z][a-z0-9-]{14,}/?$",
    # Recall category index (verified live 2026-06).
    authority_index_url="https://thencc.org.za/category/product-recalls/",
    # Some NCC notices may appear on the .gov.za host.
    authority_domains_extra=["thencc.gov.za"],

    # ── News sources (SIGNAL layer) ─────────────────────────────────────────
    rss_sources=[
        RssSource("news24.com", [
            "https://feeds.news24.com/articles/news24/SouthAfrica/rss",
        ]),
        RssSource("iol.co.za", [
            "https://www.iol.co.za/cmlink/1.640",
        ]),
        RssSource("businesstech.co.za", [
            "https://businesstech.co.za/news/feed/",
        ]),
    ],
    google_news_domains=[
        "thencc.org.za", "thencc.gov.za",   # authority
        "news24.com", "iol.co.za", "timeslive.co.za", "dailymaverick.co.za",
        "businesstech.co.za", "thesouthafrican.com", "ewn.co.za",
        "sabcnews.com", "citizen.co.za",
    ],
    # English compound phrases — recall-action + hazard + the NCC name.
    google_news_keywords=[
        '"product recall" NCC food',
        '"National Consumer Commission" recall',
        '"Listeria" recall South Africa',
        '"Salmonella" OR aflatoxin food recall South Africa',
        'food recall "return to the point of purchase"',
    ],

    bulk_index_queries=[
        "site:thencc.org.za product recall Listeria OR Salmonella OR aflatoxin",
        "site:thencc.org.za media statement product safety recall",
        "site:thencc.org.za recall 2026",
    ],

    language_name="English",
    language_code="en",
    brand_handling_note=(
        "South African brand and company names are English/Latin script. "
        "Preserve casing. Retailer/manufacturer split matters (e.g. Shoprite "
        "Checkers retails, BM Foods manufactures) — capture both when present. "
        "Company forms '(Pty) Ltd' should be kept as written."
    ),

    recall_signal_terms=[
        "recall", "recalled", "product recall", "safety recall",
        "withdrawn", "withdraw", "return to the point of purchase",
        "stop consuming", "stop using",
        "national consumer commission", "ncc",
        # Pathogen/hazard cues
        "listeria", "monocytogenes", "salmonella", "aflatoxin",
        "microbial contamination", "foreign body", "e. coli", "cereulide",
    ],

    timezone="Africa/Johannesburg",
    run_local_hour=21,
    # SAST = UTC+2 year-round (no DST). 21:00 local = 19:00 UTC.
    cron_utc_offsets=(19, 19),
)


register(SOUTH_AFRICA)
