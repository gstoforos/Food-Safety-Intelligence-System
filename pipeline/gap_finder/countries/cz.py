"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Czechia (SZPI — Czech Agriculture and Food Inspection Authority,
                Státní zemědělská a potravinářská inspekce)

VERIFIED 2026-06 (Greece concept): SZPI publishes every food recall as its own
per-item HTML page on the official "Potraviny na pranýři" (Food Pillory) portal:
    https://www.potravinynapranyri.cz/Detail.aspx?id=<number>
Each Detail page carries product, place of inspection (retailer), producer,
batch, usability date, reference number, and the unsatisfactory parameter
(e.g. "Salmonella", "Listeria monocytogenes"). An English rendering exists at
the same URL with lang=en. The food index that lists current per-recall items:
    https://www.potravinynapranyri.cz/Search.aspx?lang=cs&...&listtype=tiles

This is a clean Tier-2 authority_index_url country, exactly like EFET (Greece)
and FAVV (Belgium): the news layer is the SIGNAL a recall happened, the resolver
matches it to the official Detail.aspx page, and ALL stored data comes from the
authority page. No aggregator needed.

GATE NOTE: only /Detail.aspx (food) is a recall. The portal also has:
    /WDetail.aspx  → closed premises (hygiene closures, NOT food recalls)
    /EDetail.aspx  → risky websites (NOT food)
so authority_item_url_regex matches Detail.aspx?id= ONLY (not WDetail/EDetail).
"""

from .base import CountryConfig, RssSource, register


CZECHIA = CountryConfig(
    code="cz",
    name_en="Czechia",
    name_local="Česko",

    authority_short="SZPI",
    authority_full="Státní zemědělská a potravinářská inspekce",
    # The official recall portal is on its own domain (run by SZPI). The Stage-3
    # gate requires the Pending URL host to be this domain.
    authority_domain="potravinynapranyri.cz",
    # Per-recall FOOD pages only: /Detail.aspx?id=<number>.
    # The (?<![A-Za-z]) lookbehind prevents matching WDetail.aspx / EDetail.aspx
    # (premises closures / risky websites — not food recalls).
    authority_item_url_regex=r"(?<![A-Za-z])Detail\.aspx\?id=\d+",
    # Food recall index (verified live 2026-06): lists current non-compliant
    # food lots as tiles, each linking to its Detail.aspx page.
    authority_index_url="https://www.potravinynapranyri.cz/Search.aspx?lang=cs&design=default&archive=actual&listtype=tiles",

    # ── News sources (the SIGNAL layer) ─────────────────────────────────────
    rss_sources=[
        # PRIMARY — SZPI's own Potraviny na pranýři RSS (direct recall feed).
        RssSource("potravinynapranyri.cz", [
            "https://www.potravinynapranyri.cz/Rss.aspx",
        ]),
        # SECONDARY — Czech major news (verified working endpoints).
        RssSource("idnes.cz", [
            "https://servis.idnes.cz/rss.aspx?c=zpravodaj",
        ]),
        RssSource("irozhlas.cz", [
            "https://www.irozhlas.cz/rss/irozhlas",
        ]),
        RssSource("ceskatelevize.cz", [
            "https://ct24.ceskatelevize.cz/rss",
        ]),
    ],
    google_news_domains=[
        "potravinynapranyri.cz",  # primary (authority)
        "szpi.gov.cz",            # authority press releases
        "novinky.cz", "idnes.cz", "irozhlas.cz", "ceskatelevize.cz",
        "seznamzpravy.cz", "denik.cz", "tn.cz", "lidovky.cz",
    ],
    # Precise compound Czech phrases — recall-action + hazard, avoiding the
    # generic "varování" (warning) noise.
    google_news_keywords=[
        '"stažení z prodeje" potravina',
        '"SZPI" stažení potravina',
        '"salmonela" stažení potraviny',
        '"listerie" stažení potraviny OR Listeria monocytogenes',
        '"nebezpečná potravina" SZPI',
        'potravina stažena pesticidy OR aflatoxiny',
    ],

    bulk_index_queries=[
        "site:potravinynapranyri.cz Detail salmonela 2026",
        "site:potravinynapranyri.cz Detail listeria 2026",
        "site:potravinynapranyri.cz Detail 2026",
        "site:szpi.gov.cz stažení potravina 2026",
        "site:szpi.gov.cz Listeria OR Salmonella",
    ],

    language_name="Czech",
    language_code="cs",
    brand_handling_note=(
        "Czech brands and company names are Latin script with diacritics. "
        "Preserve casing and diacritics (e.g. 'Kaufland Česká republika', "
        "'Albert Česká republika', 'BILLA', 'Globus', 'Penny Market'). "
        "Company legal forms 's.r.o.' / 'a.s.' should be kept as written."
    ),

    recall_signal_terms=[
        # Czech recall / withdrawal vocabulary
        "stažení", "stažena", "staženo", "stahuje", "stáhla", "stáhl",
        "z prodeje", "z trhu",
        "nebezpečná potravina", "nebezpečné potraviny",
        "varování", "upozornění",
        "závadná potravina", "závadné potraviny",
        "potraviny na pranýři", "szpi",
        "nevhodná ke spotřebě", "nekonzumujte",
        # Pathogen/hazard cues (also help news filter)
        "salmonela", "salmonella", "listerie", "listeria",
        "pesticid", "aflatoxin",
    ],

    timezone="Europe/Prague",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(CZECHIA)
