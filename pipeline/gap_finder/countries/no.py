"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Norway (Mattilsynet — Norwegian Food Safety Authority)

Norwegian food-recall regime:
  - Mattilsynet at mattilsynet.no/tilbakekallinger publishes recalls with
    full pathogen names in titles (verified ground truth — high quality).
  - NTB pressemelding (kommunikasjon.ntb.no) distributes Mattilsynet press
    releases via standard newswire.
  - Volume: ~80-120 recalls/year. Norwegian outlets like Sjømathuset,
    Coop, Norfersk, UNIL frequently appear.
  - Vocabulary:
      "tilbakekaller"  = recalls (verb)
      "tilbakekall"    = recall (noun, also "tilbakekalling")
      "tilbaketrekking" = withdrawal
      "ikke spise"     = do not eat
      "advarsel"       = warning
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
        # PRIMARY — Mattilsynet official RSS
        RssSource("mattilsynet.no", [
            "https://www.mattilsynet.no/rss",
            "https://www.mattilsynet.no/tilbakekallinger/rss",
        ]),
        # PRIMARY — NTB press release wire (distributes Mattilsynet announcements)
        RssSource("ntb.no", [
            "https://kommunikasjon.ntb.no/rss/?publisherId=10773547",
        ]),
        # SECONDARY — major Norwegian dailies
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
    ],
    google_news_domains=[
        "mattilsynet.no",       # primary
        "ntb.no",               # primary
        "vg.no", "nrk.no", "dagbladet.no", "aftenposten.no",
        "nettavisen.no", "bt.no", "tv2.no", "abcnyheter.no",
    ],
    google_news_keywords=[
        '"Mattilsynet" tilbakekaller',
        '"tilbakekaller" Salmonella',
        '"tilbakekaller" Listeria',
        '"tilbaketrekking" mat',
        '"Coop" tilbakekaller',
        '"REMA 1000" tilbakekaller',
        '"Kiwi" tilbakekaller',
    ],

    bulk_index_queries=[
        "site:mattilsynet.no tilbakekalling 2026",
        "site:mattilsynet.no Salmonella OR Listeria",
        "site:mattilsynet.no advarsel mat",
        "site:mattilsynet.no allergen ikke deklarert",
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
        # Pathogen-name signals
        "salmonella", "listeria", "listeria monocytogenes",
        "ikke deklarert", "ikke merket",
    ],

    timezone="Europe/Oslo",
    run_local_hour=21,
    cron_utc_offsets=(19, 20),
)


register(NORWAY)
