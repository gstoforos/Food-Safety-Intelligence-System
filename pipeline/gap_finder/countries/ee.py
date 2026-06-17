"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Estonia (PTA — Põllumajandus- ja Toiduamet /
                Estonian Agriculture and Food Board).

VERIFIED 2026-06 (Greece concept): PTA publishes each food recall as its own
per-item HTML page on pta.agri.ee with a descriptive slug ending in
"-tagasikutsumine" (recall), e.g.:
    https://pta.agri.ee/kiirnuudlite-tagasikutsumine   (instant noodles, Salmonella)
Each page carries product, hazard (e.g. Salmonella, Listeria monocytogenes),
batch/lot, producer, and consumer guidance. An English site exists at
pta.agri.ee/en. News-type recall items also appear under /uudised/<slug>.

Clean Tier-2 authority_index_url country, like EFET (Greece): the news layer
is the SIGNAL; the resolver matches to the official pta.agri.ee page; all
stored data comes from the authority page.
"""

from .base import CountryConfig, RssSource, register


ESTONIA = CountryConfig(
    code="ee",
    name_en="Estonia",
    name_local="Eesti",

    authority_short="PTA",
    authority_full="Põllumajandus- ja Toiduamet",
    authority_domain="pta.agri.ee",
    # Per-recall slug pages end in "-tagasikutsumine"; recall news also lives
    # under /uudised/<slug>. Match either form on the authority domain.
    authority_item_url_regex=r"(tagasikutsumine|/uudised/)",
    # Recall/news index (verified live 2026-06). PTA lists recalls as news
    # items; the resolver matches by product keyword and fetches the item page.
    authority_index_url="https://pta.agri.ee/uudised",

    # ── News sources (SIGNAL layer) ─────────────────────────────────────────
    rss_sources=[
        # PRIMARY — Estonian major news (verified working endpoints).
        RssSource("err.ee", [
            "https://www.err.ee/rss",
        ]),
        RssSource("delfi.ee", [
            "https://www.delfi.ee/rss",
        ]),
        RssSource("postimees.ee", [
            "https://www.postimees.ee/rss",
        ]),
    ],
    google_news_domains=[
        "pta.agri.ee",       # authority
        "err.ee", "delfi.ee", "postimees.ee", "ohtuleht.ee",
        "epl.delfi.ee", "aripaev.ee",
    ],
    # Precise Estonian compound phrases — recall-action + hazard.
    google_news_keywords=[
        '"tagasikutsumine" toit',
        '"tagasi kutsutud" toode PTA',
        '"Listeria" tagasikutsumine',
        '"salmonella" toit tagasikutsumine',
        'PTA ohtlik toode turult',
    ],

    bulk_index_queries=[
        "site:pta.agri.ee tagasikutsumine 2026",
        "site:pta.agri.ee tagasikutsumine Listeria OR salmonella",
        "site:pta.agri.ee uudised tagasikutsumine",
    ],

    language_name="Estonian",
    language_code="et",
    brand_handling_note=(
        "Estonian brands and company names are Latin script (may include "
        "õ, ä, ö, ü). Preserve casing and diacritics. Estonian company legal "
        "forms 'OÜ' / 'AS' should be kept as written."
    ),

    recall_signal_terms=[
        # Estonian recall / withdrawal vocabulary
        "tagasikutsumine", "tagasi kutsutud", "tagasi kutsuma",
        "turult eemaldamine", "turult kõrvaldamine",
        "ohtlik toode", "ohtlik toit",
        "ei tarbi", "mitte tarbida",
        "pta", "põllumajandus- ja toiduamet",
        # Pathogen/hazard cues
        "listeria", "salmonella", "salmonelloos",
        "pestitsiid", "aflatoksiin",
    ],

    timezone="Europe/Tallinn",
    run_local_hour=21,
    # Estonia is EET/EEST (UTC+2/+3) — one hour ahead of CET. 21:00 Tallinn
    # = 18:00/19:00 UTC.
    cron_utc_offsets=(18, 19),
)


register(ESTONIA)
