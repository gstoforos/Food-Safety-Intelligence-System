"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Czechia (SZPI / CAFIA — Státní zemědělská a potravinářská
                inspekce / Czech Agriculture and Food Inspection Authority).

VERIFIED 2026-06 (Greece concept): SZPI publishes each recall/consumer-warning
as its own per-item HTML page on the REGULATOR site szpi.gov.cz:
    Czech:   https://www.szpi.gov.cz/clanek/<slug>.aspx
    English: https://www.szpi.gov.cz/en/article/<slug>.aspx   (clean, English)
e.g. the pathogen recalls (the FSIS target):
    /en/article/consumer-warning-salmonella-in-imported-chicken-cutlets.aspx
    /en/article/consumer-warning-salmonella-found-in-imported-minced-pork-product.aspx
    /en/article/consumer-warning-enoki-mushrooms-contaminated-with-listeria-monocytogenes.aspx
The English news index is STATIC HTML (every article link in the markup, 761
items), unlike the pillory's JS Search.aspx:
    https://www.szpi.gov.cz/en/news.aspx
Append ?design=text to any page for a clean text render.

SECONDARY authority domain: potravinynapranyri.cz ("food pillory") — SZPI's
public non-compliance database, per-recall pages /Detail.aspx?id=N. Google News
often surfaces these Detail pages directly for a given recall, so they are kept
as an accepted authority domain (authority_domains_extra) for maximum coverage.
WDetail.aspx (premises closures) / EDetail.aspx (risky websites) are NOT food
recalls and are excluded by the item regex.

Both domains are accepted via the gate / Tier-0 (candidate URL is itself an
authority page) / Tier-1 (HTML scan). The news layer is only the SIGNAL.
"""

from .base import CountryConfig, RssSource, register


CZECHIA = CountryConfig(
    code="cz",
    name_en="Czechia",
    name_local="Česko",

    authority_short="SZPI",
    authority_full="Státní zemědělská a potravinářská inspekce (Czech Agriculture and Food Inspection Authority)",
    authority_domain="szpi.gov.cz",
    # Accept all three per-recall URL forms across both authority domains:
    #   szpi.gov.cz/clanek/<slug>.aspx          (Czech regulator article)
    #   szpi.gov.cz/en/article/<slug>.aspx      (English regulator article)
    #   potravinynapranyri.cz/Detail.aspx?id=N  (pillory per-recall; NOT WDetail/EDetail)
    authority_item_url_regex=(
        r"(/clanek/[a-z0-9][a-z0-9\-]+\.aspx"
        r"|/en/article/[a-z0-9][a-z0-9\-]+\.aspx"
        r"|(?<![A-Za-z])Detail\.aspx\?id=\d+)"
    ),
    # Static English news index — lists every article as a parseable link.
    authority_index_url="https://www.szpi.gov.cz/en/news.aspx",
    # Pillory Detail pages also count as authority (Google News surfaces them).
    authority_domains_extra=["potravinynapranyri.cz"],

    # ── News sources (SIGNAL layer) ─────────────────────────────────────────
    rss_sources=[
        # SZPI's own feed (often empty, but cheap to try).
        RssSource("szpi.gov.cz", [
            "https://www.szpi.gov.cz/Rss.aspx",
        ]),
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
        "szpi.gov.cz",            # PRIMARY authority — GN surfaces /clanek + /en/article
        "potravinynapranyri.cz",  # secondary authority (pillory Detail pages)
        "novinky.cz", "idnes.cz", "irozhlas.cz", "ceskatelevize.cz",
        "seznamzpravy.cz", "denik.cz", "tn.cz", "lidovky.cz",
    ],
    # Precise Czech compound phrases — recall-action + hazard.
    google_news_keywords=[
        '"stažení z prodeje" potravina',
        '"SZPI" stažení potravina',
        '"salmonela" stažení potraviny',
        '"listerie" stažení potraviny OR Listeria monocytogenes',
        '"nebezpečná potravina" SZPI',
        'potravina stažena pesticidy OR aflatoxiny',
    ],

    bulk_index_queries=[
        "site:szpi.gov.cz consumer warning salmonella OR listeria",
        "site:szpi.gov.cz spotřebitelská výstraha salmonela OR listerie",
        "site:szpi.gov.cz/en/article consumer warning 2026",
        "site:szpi.gov.cz/clanek stažení 2026",
        "site:potravinynapranyri.cz Detail.aspx salmonela OR listerie",
    ],

    language_name="Czech",
    language_code="cs",
    brand_handling_note=(
        "Czech brand and company names are Latin script with diacritics "
        "(č, ř, š, ž, ý, á, í, é, ů). Preserve casing and diacritics. Czech "
        "company legal forms 's.r.o.' / 'a.s.' should be kept as written. "
        "SZPI English articles already give product/importer/batch in English."
    ),

    recall_signal_terms=[
        # Czech recall / withdrawal vocabulary
        "stažení", "stažena", "stahuje", "stáhla", "staženy",
        "z prodeje", "z trhu", "z oběhu",
        "spotřebitelská výstraha", "varování", "nebezpečná potravina",
        "nevyhovující", "nekonzumovat",
        "szpi", "potravinářská inspekce",
        # English (SZPI /en/ articles)
        "consumer warning", "recall", "withdrawn",
        # Pathogen/hazard cues
        "salmonela", "salmonella", "listerie", "listeria", "monocytogenes",
        "pesticid", "aflatoxin", "escherichia",
    ],

    timezone="Europe/Prague",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(CZECHIA)
