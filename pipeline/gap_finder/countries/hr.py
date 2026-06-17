"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Croatia (HAPIH — Hrvatska agencija za poljoprivredu i hranu /
                Croatian Agency for Agriculture and Food; recalls issued by the
                Ministarstvo poljoprivrede / Državni inspektorat RH).

VERIFIED 2026-06 (Greece concept): HAPIH publishes every food recall as its own
per-item HTML page on hapih.hr. The consumer-notice index:
    https://www.hapih.hr/kategorija/obavijesti-za-potrosace/
Each recall is a slug page, e.g.:
    /opoziv-proizvoda-grof-lovski-salama-s-mesom-jelena-250-g/   (Listeria)
    /opoziv-ile-de-france-petit-brie-125g-.../                   (Staph. aureus)
    /povlacenje-i-opoziv-proizvoda-<slug>/                       (older form)
Each page carries product, hazard (e.g. "Listeria monocytogenes"), LOT, dates,
and the issuing authority (Državni inspektorat / Ministarstvo poljoprivrede).

Clean Tier-2 authority_index_url country, exactly like EFET (Greece): the news
layer is the SIGNAL; the resolver matches it to the official hapih.hr page; all
stored data comes from the authority page.
"""

from .base import CountryConfig, RssSource, register


CROATIA = CountryConfig(
    code="hr",
    name_en="Croatia",
    name_local="Hrvatska",

    authority_short="HAPIH",
    authority_full="Hrvatska agencija za poljoprivredu i hranu",
    authority_domain="hapih.hr",
    # Per-recall slug pages: /opoziv-... and /povlacenje-...-opoziv-...
    # (covers 'opoziv-proizvoda-', bare 'opoziv-', and 'povlacenje-i-opoziv-').
    authority_item_url_regex=r"/(opoziv|povlacenje)[a-z0-9-]+",
    # Consumer-notice index (verified live 2026-06): lists every recall, each
    # linking to its own slug page. Paginated (/page/N/).
    authority_index_url="https://www.hapih.hr/kategorija/obavijesti-za-potrosace/",

    # ── News sources (SIGNAL layer) ─────────────────────────────────────────
    rss_sources=[
        # PRIMARY — Croatian major news (verified working endpoints).
        RssSource("index.hr", [
            "https://www.index.hr/rss/vijesti",
        ]),
        RssSource("24sata.hr", [
            "https://www.24sata.hr/feeds/aktualno.xml",
        ]),
        RssSource("vecernji.hr", [
            "https://www.vecernji.hr/feeds/latest",
        ]),
        RssSource("tportal.hr", [
            "https://www.tportal.hr/rss",
        ]),
    ],
    google_news_domains=[
        "hapih.hr",          # authority
        "index.hr", "24sata.hr", "vecernji.hr", "jutarnji.hr",
        "tportal.hr", "novilist.hr", "dnevnik.hr", "rtl.hr", "hrt.hr",
    ],
    # Precise Croatian compound phrases — recall-action + hazard, avoiding the
    # generic "upozorenje" noise.
    google_news_keywords=[
        '"opoziv proizvoda" hrana',
        '"povlačenje" hrane HAPIH OR "Ministarstvo poljoprivrede"',
        '"Listeria" opoziv Hrvatska',
        '"Salmonella" opoziv hrana',
        '"opoziv" trgovina povučen proizvod',
    ],

    bulk_index_queries=[
        "site:hapih.hr opoziv 2026",
        "site:hapih.hr opoziv Listeria OR Salmonella",
        "site:hapih.hr povlacenje opoziv 2026",
        "site:hapih.hr opoziv proizvoda",
    ],

    language_name="Croatian",
    language_code="hr",
    brand_handling_note=(
        "Croatian brands and company names are Latin script with diacritics "
        "(č, ć, š, ž, đ). Preserve casing and diacritics. Croatian company "
        "legal forms 'd.o.o.' / 'j.d.o.o.' / 'd.d.' should be kept as written."
    ),

    recall_signal_terms=[
        # Croatian recall / withdrawal vocabulary
        "opoziv", "opoziva", "opozvan", "opozvani",
        "povlačenje", "povlači", "povučen", "povučeni",
        "s tržišta", "iz prodaje",
        "obavijest za potrošače", "nesukladan proizvod",
        "hapih", "ministarstvo poljoprivrede", "državni inspektorat",
        "ne konzumirati",
        # Pathogen/hazard cues
        "listeria", "listerija", "salmonela", "salmonella",
        "pesticid", "aflatoksin", "enterotoksin",
    ],

    timezone="Europe/Zagreb",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(CROATIA)
