"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Indonesia (BPOM — Badan Pengawas Obat dan Makanan)

WHY THIS EXISTS (audit 2026-09-23)
==================================
Indonesia has been dark. The Greek route applies unchanged: find the recall
in local media, confirm product, date and hazard from the article, resolve
the article back to BPOM's own page, publish that URL.

VERIFIED 2026-09-23 — real item URLs, two boards, both per-item slugs:

    /siaran-pers/penarikan-produk-mi-instan-indonesia
    /penjelasan-publik/penarikan-4-produk-pangan-oleh-sfa

against their listings, which are the bare board paths:

    /siaran-pers
    /penjelasan-publik

so the regex requires a slug AFTER the board segment. That is the whole
index/item distinction on this site, and it is why the trailing [a-z0-9-]+
is not optional.

The two boards do different jobs and both matter. siaran-pers is BPOM's own
press release — an Indonesian recall. penjelasan-publik is BPOM explaining
a foreign recall's bearing on the Indonesian market; the second URL above
is BPOM on an SFA (Singapore) action. Those are real records, but they are
records about ANOTHER jurisdiction's recall, and the extractor should place
them under the country that recalled. Keeping both boards in scope is
deliberate — dropping penjelasan-publik would lose genuine Indonesian
market actions that happen to have started abroad.
"""

from .base import CountryConfig, RssSource, register


INDONESIA = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="id",
    name_en="Indonesia",
    name_local="Indonesia",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="BPOM",
    authority_full="Badan Pengawas Obat dan Makanan (National Agency of Drug and Food Control)",
    authority_domain="pom.go.id",
    # A slug must follow the board segment — the bare board path is the index.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/(?:siaran-pers|penjelasan-publik)/[a-z0-9][a-z0-9\-]+"
    ),
    authority_index_url="https://www.pom.go.id/siaran-pers",

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources=[
        RssSource("antaranews.com", ["https://www.antaranews.com/rss/terkini.xml"]),
        RssSource("kompas.com", ["https://www.kompas.com/rss"]),
        RssSource("tempo.co", ["https://rss.tempo.co/nasional"]),
    ],
    google_news_domains=[
        "kompas.com", "detik.com", "tempo.co", "cnnindonesia.com",
        "antaranews.com", "liputan6.com", "tribunnews.com", "kumparan.com",
        "thejakartapost.com", "republika.co.id", "suara.com", "bisnis.com",
    ],
    google_news_keywords=[
        "penarikan produk pangan BPOM",
        "BPOM tarik produk makanan",
        "produk pangan ditarik",
        "Indonesia food recall",
        "BPOM cemaran pangan",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:pom.go.id siaran pers penarikan produk 2026",
        "site:pom.go.id penarikan pangan",
        "site:pom.go.id penjelasan publik produk pangan",
        "site:pom.go.id cemaran pangan olahan",
        "site:pom.go.id BPOM tarik produk",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="Indonesian",
    language_code="id",
    brand_handling_note=(
        "Indonesian company names are in Latin script and normally carry a "
        "PT prefix (PT Indofood Sukses Makmur) or a Tbk suffix for listed "
        "companies. Keep the prefix and suffix exactly as published. Brand "
        "names are often a single word in capitals; keep the published "
        "casing. BPOM identifies registered products by an izin edar number "
        "beginning BPOM RI MD (domestic) or BPOM RI ML (imported) — that "
        "number belongs in the product/identifier field, not the company "
        "field, and its MD/ML letter says whether the product was made in "
        "Indonesia or imported."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "penarikan", "ditarik", "tarik produk", "recall",
        "bpom", "cemaran", "tidak memenuhi syarat", "tms",
        "izin edar", "tanpa izin edar", "berbahaya",
        "keracunan", "peringatan", "jangan dikonsumsi",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # WIB = UTC+7, no DST, so both offsets are identical.
    timezone="Asia/Jakarta",
    run_local_hour=21,
    cron_utc_offsets=(14, 14),
)

register(INDONESIA)
