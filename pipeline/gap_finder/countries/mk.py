"""
AFTS Food Safety Intelligence — Gap Finder
Country config: North Macedonia (FVA / АХВ — Агенција за храна и ветеринарство /
                Food and Veterinary Agency).

VERIFIED 2026-06 (Greece concept): FVA publishes each recall as its own per-item
HTML page on fva.gov.mk with a numeric-id + slug, e.g.:
    https://fva.gov.mk/mk/240214-povlekuvanje-nebezbeden-proizvod-bio-kakao-prav-200-gr
Index (paginated):
    https://fva.gov.mk/mk/izvestuvanje-otpovikuvanje-nebezbedni-proizvodi
Each page carries product, hazard (aflatoxin, ethylene oxide, cadmium, etc.),
operator/company. FVA is the RASFF/EFSA/Codex contact point. English site at /en.

Clean Tier-2 authority_index_url country, like EFET (Greece).
"""

from .base import CountryConfig, RssSource, register


NORTH_MACEDONIA = CountryConfig(
    code="mk",
    name_en="North Macedonia",
    name_local="Северна Македонија",

    authority_short="FVA",
    authority_full="Агенција за храна и ветеринарство (Food and Veterinary Agency)",
    authority_domain="fva.gov.mk",
    # Per-recall pages: /mk/<numeric-id>-povlekuvanje|otpovikuvanje-<slug>.
    # Match the recall verbs on the authority domain (covers повлекување /
    # отповикување in transliterated slug form).
    authority_item_url_regex=r"(povlekuvanje|otpovikuvanje|povlekuvane|nebezbeden-proizvod)",
    # Consumer recall index (verified live 2026-06), paginated ?page=N.
    authority_index_url="https://fva.gov.mk/mk/izvestuvanje-otpovikuvanje-nebezbedni-proizvodi",

    # ── News sources (SIGNAL layer) ─────────────────────────────────────────
    rss_sources=[
        # Macedonian major news (verified working endpoints).
        RssSource("time.mk", [
            "https://www.time.mk/rss",
        ]),
        RssSource("mkd.mk", [
            "https://www.mkd.mk/feed",
        ]),
        RssSource("sdk.mk", [
            "https://sdk.mk/index.php/feed/",
        ]),
    ],
    google_news_domains=[
        "fva.gov.mk",        # authority
        "time.mk", "mkd.mk", "sdk.mk", "slobodenpecat.mk",
        "a1on.mk", "telma.com.mk", "sitel.com.mk", "kanal5.com.mk",
        "meta.mk", "novamakedonija.com.mk",
    ],
    # Precise Macedonian compound phrases — recall-action + hazard.
    google_news_keywords=[
        '"отповикување" производ АХВ',
        '"повлекување" небезбеден производ',
        '"листерија" храна отповик',
        '"салмонела" производ повлекување',
        'АХВ небезбеден производ пазар',
    ],

    bulk_index_queries=[
        "site:fva.gov.mk отповикување 2026",
        "site:fva.gov.mk повлекување небезбеден производ",
        "site:fva.gov.mk otpovikuvanje OR povlekuvanje",
    ],

    language_name="Macedonian",
    language_code="mk",
    brand_handling_note=(
        "Macedonian brand and company names are Cyrillic; product names may be "
        "Latin (imported brands). Preserve script and casing as written. "
        "Company legal forms 'ДОО' / 'ДООЕЛ' should be kept as written."
    ),

    recall_signal_terms=[
        # Macedonian recall / withdrawal vocabulary (Cyrillic)
        "отповикување", "отповикува", "отповик",
        "повлекување", "повлекува", "се повлекува",
        "небезбеден производ", "небезбедни производи",
        "од пазарот", "не консумирајте",
        "ахв", "агенција за храна и ветеринарство",
        # Pathogen/hazard cues
        "листерија", "салмонела", "афлатоксин",
        "етилен оксид", "кадмиум", "пестицид",
        # Latin transliterations sometimes used in slugs/news
        "listeria", "salmonela", "aflatoksin",
    ],

    timezone="Europe/Skopje",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(NORTH_MACEDONIA)
