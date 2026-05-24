"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Greece (ΕΦΕΤ — Hellenic Food Authority)
"""

from .base import CountryConfig, RssSource, register


GREECE = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="gr",
    name_en="Greece",
    name_local="Ελλάδα",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="EFET",
    authority_full="Ενιαίος Φορέας Ελέγχου Τροφίμων",
    authority_domain="efet.gr",
    # EFET (Joomla): /index.php/el/enimerosi/deltia-typou/anakleiseis-cat/item/<NUM>-<slug>
    authority_item_url_regex=r"anakleiseis-cat/item/\d+",

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        RssSource("kathimerini.gr", [
            "https://www.kathimerini.gr/feed/",
            "https://www.kathimerini.gr/society/feed/",
        ]),
        RssSource("protothema.gr", [
            "https://www.protothema.gr/rss/",
            "https://www.protothema.gr/ellada/rss/",
        ]),
        RssSource("in.gr", ["https://www.in.gr/feed/"]),
        RssSource("troktiko.gr", ["https://www.troktiko.gr/feed/"]),
        RssSource("news247.gr", [
            "https://www.news247.gr/rss/all.xml",
            "https://www.news247.gr/rss/",
        ]),
        RssSource("protagon.gr", ["https://www.protagon.gr/feed/"]),
        RssSource("skai.gr", [
            "https://www.skai.gr/feeds/news",
            "https://www.skai.gr/feed",
        ]),
    ],
    google_news_domains=[
        "kathimerini.gr", "protothema.gr", "in.gr", "troktiko.gr",
        "news247.gr", "protagon.gr", "skai.gr",
    ],
    google_news_keywords=[
        "ΕΦΕΤ ανάκληση",
        "ανάκληση τροφίμου",
        "ανακαλεί προϊόν",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:efet.gr ΕΦΕΤ ανάκληση 2026",
        "site:efet.gr ΕΦΕΤ ανάκληση 2025",
        "site:efet.gr Δελτίο Τύπου Ανάκληση",
        "site:efet.gr ανακαλεί τρόφιμο",
        "site:efet.gr ανάκληση προϊόντος",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Greek",
    language_code="el",
    brand_handling_note=(
        "Keep Greek-only brands in Greek (e.g. ΒΥΤΙΝΑΣ). "
        "Use Latin form if it appears in the source (e.g. 'Strudito', 'Nestlé')."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "ανακληση", "ανάκληση", "ανακαλει", "ανακαλεί",
        "ανακαλειται", "ανακαλούνται",
        "εφετ", "ε.φ.ε.τ",
        "αποσυρση", "απόσυρση",
        "δελτιο τυπου", "δελτίο τύπου",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    timezone="Europe/Athens",
    run_local_hour=21,
    # Athens: EEST=UTC+3 (summer), EET=UTC+2 (winter)
    # 21:00 Athens → 18:00 UTC (EEST) / 19:00 UTC (EET)
    cron_utc_offsets=(18, 19),
)


register(GREECE)
