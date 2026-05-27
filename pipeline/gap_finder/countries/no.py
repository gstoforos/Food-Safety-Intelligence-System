"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Norway (Mattilsynet — Norwegian Food Safety Authority)

REVISION (Batch 3.1 hotfix): First Norway run found 59 candidates → 1 Pending
(Valmsnes Gårdsysteri Listeria). Working, but RSS feeds 404'd. The big
"Coop sandwich-is metallbiter" multi-product recall correctly rejected as
foreign matter (out of Rule B scope).

Fixed by:
  1. Removing 404'd RSS URLs (mattilsynet.no/tilbakekallinger/rss,
     ntb.no/rss with publisher filter)
  2. Adding intrafish.no and ilaks.no (Norwegian fish industry — high
     signal for seafood recalls which are common in Norway)
  3. Keeping the working Google News strategy (yielded 107 candidates)
"""

from .base import CountryConfig, RssSource, register


NORWAY = CountryConfig(
    code="no",
    name_en="Norway",
    name_local="Norge",

    authority_short="Mattilsynet",
    authority_full="Mattilsynet (Norwegian Food Safety Authority)",
    authority_domain="mattilsynet.no",
    authority_item_url_regex=r"(tilbakekall|tilbaketrekk|nyheter|press)",

    rss_sources=[
        # Mattilsynet RSS exists at /rss but returns empty for recalls.
        # Removed bad /tilbakekallinger/rss URL.
        RssSource("mattilsynet.no", [
            "https://www.mattilsynet.no/rss",
        ]),
        # Major Norwegian news (verified working from production run)
        RssSource("vg.no", [
            "https://www.vg.no/rss/feed/",
        ]),
        RssSource("nrk.no", [
            "https://www.nrk.no/toppsaker.rss",
            "https://www.nrk.no/norge/toppsaker.rss",
        ]),
        RssSource("dagbladet.no", [
            "https://www.dagbladet.no/?lab_viewport=rss",
        ]),
        RssSource("aftenposten.no", [
            "https://www.aftenposten.no/rss",
        ]),
        RssSource("nettavisen.no", [
            "https://www.nettavisen.no/service/rich-rss",
        ]),
        # Fish industry — high signal for Norwegian seafood recalls
        RssSource("intrafish.no", [
            "https://www.intrafish.no/rss/",
        ]),
    ],
    google_news_domains=[
        "mattilsynet.no",
        "ntb.no", "kommunikasjon.ntb.no",
        "vg.no", "nrk.no", "dagbladet.no", "aftenposten.no",
        "nettavisen.no", "bt.no", "tv2.no", "abcnyheter.no",
        "intrafish.no", "ilaks.no",
    ],
    google_news_keywords=[
        '"Mattilsynet" tilbakekaller',
        "tilbakekaller Salmonella",
        "tilbakekaller Listeria",
        "tilbaketrekking mat",
        '"Coop" tilbakekaller',
        '"REMA 1000" tilbakekaller',
        '"Kiwi" tilbakekaller',
        '"Meny" tilbakekaller',
    ],

    bulk_index_queries=[
        "site:mattilsynet.no tilbakekalling 2026",
        "site:mattilsynet.no Salmonella OR Listeria",
        "site:mattilsynet.no advarsel mat",
    ],

    language_name="Norwegian (Bokmål)",
    language_code="no",
    brand_handling_note=(
        "Norwegian brands often use diacritics (æ, ø, å). Preserve original "
        "casing. Examples: 'Coop Norge', 'REMA 1000', 'Kiwi', 'Meny', "
        "'Joker', 'Spar Norge', 'Bunnpris', 'Norfersk', 'Sjømathuset'."
    ),

    recall_signal_terms=[
        "tilbakekaller", "tilbakekalling", "tilbakekalt", "tilbakekall",
        "tilbaketrekking", "trekker tilbake", "trekkes tilbake",
        "matvareadvarsel", "matvarsel",
        "ikke spise", "ikke konsumere",
        "mattilsynet",
        "advarsel mat",
        "salmonella", "listeria", "listeria monocytogenes",
        "ikke deklarert", "ikke merket",
    ],

    timezone="Europe/Oslo",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(NORWAY)
