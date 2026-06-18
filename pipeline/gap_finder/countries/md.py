"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Moldova (ANSA — Agenția Națională pentru Siguranța Alimentelor /
                National Food Safety Agency).

VERIFIED 2026-06 (Greece concept): ANSA publishes each recall as its own per-item
HTML page on ansa.gov.md, under /media/rechemare-retragere/ and
/media/comunicate-de-presa/, with a descriptive slug ending in .html (repeats get
a -N suffix), e.g.:
    https://www.ansa.gov.md/media/rechemare-retragere/atentie-rechemare-de-produs.html-2
    (eggs, banned veterinary substance metronidazol)
Index:
    https://www.ansa.gov.md/media/rechemare-retragere
Each page carries product, hazard (Salmonella, pyrrolizidine alkaloids, aflatoxin,
banned residues), producer/operator, lot. Romanian language.

Clean Tier-2 authority_index_url country, like EFET (Greece).
"""

from .base import CountryConfig, RssSource, register


MOLDOVA = CountryConfig(
    code="md",
    name_en="Moldova",
    name_local="Republica Moldova",

    authority_short="ANSA",
    authority_full="Agenția Națională pentru Siguranța Alimentelor",
    authority_domain="ansa.gov.md",
    # Per-recall slug pages live under recall/press paths and end in .html
    # (sometimes with a -N repeat suffix). Match the recall/withdrawal verbs.
    authority_item_url_regex=r"(rechemare|retragere)[a-z0-9\-/]*",
    # Recall / withdrawal index (verified live 2026-06).
    authority_index_url="https://www.ansa.gov.md/media/rechemare-retragere",

    # ── News sources (SIGNAL layer) ─────────────────────────────────────────
    rss_sources=[
        # Moldovan major news (verified working endpoints).
        RssSource("agora.md", [
            "https://agora.md/rss",
        ]),
        RssSource("point.md", [
            "https://point.md/ro/rss/",
        ]),
        RssSource("zdg.md", [
            "https://www.zdg.md/feed/",
        ]),
    ],
    google_news_domains=[
        "ansa.gov.md",       # authority
        "agora.md", "point.md", "zdg.md", "stiri.md",
        "newsmaker.md", "tv8.md", "diez.md", "unimedia.info",
        "moldova.org", "realitatea.md",
    ],
    # Precise Romanian compound phrases — recall-action + hazard.
    google_news_keywords=[
        '"rechemare de produs" ANSA',
        '"retragere" produs alimentar Moldova',
        '"Listeria" produs rechemare',
        '"Salmonella" aliment ANSA',
        'ANSA produs neconform piață',
    ],

    bulk_index_queries=[
        "site:ansa.gov.md rechemare 2026",
        "site:ansa.gov.md rechemare retragere produs",
        "site:ansa.gov.md Listeria OR Salmonella",
    ],

    language_name="Romanian",
    language_code="ro",
    brand_handling_note=(
        "Moldovan brand and company names are Latin script (Romanian; may "
        "include ă, â, î, ș, ț). Preserve casing and diacritics. Company legal "
        "forms 'SRL' / 'ÎI' / 'SA' should be kept as written. Some product "
        "names may appear in Cyrillic for older/imported goods."
    ),

    recall_signal_terms=[
        # Romanian (Moldova) recall / withdrawal vocabulary
        "rechemare", "rechemarea", "recheamă",
        "retragere", "retras", "retrage", "se retrage",
        "produs neconform", "produs alimentar",
        "nu consumați", "returnați",
        "ansa", "agenția națională pentru siguranța alimentelor",
        # Pathogen/hazard cues
        "listeria", "salmonella", "salmonela",
        "aflatoxină", "alcaloizi", "pesticid", "etilen oxid",
    ],

    timezone="Europe/Chisinau",
    run_local_hour=21,
    # Moldova is EET/EEST (UTC+2/+3), same as Greece/Estonia.
    cron_utc_offsets=(18, 19),
)


register(MOLDOVA)
