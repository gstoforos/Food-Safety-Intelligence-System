"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Iceland (MAST — Matvælastofnun / Icelandic Food and
                Veterinary Authority)

Icelandic food-recall regime:
  - MAST at mast.is publishes alerts ("innkallanir").
  - Icelandic is West Germanic with unique grammar and characters
    (þ, ð, æ). Very small market.
  - Volume: ~5-15 recalls/year (population ~380K).
  - Vocabulary:
      "innkalla"      = recall (verb)
      "innköllun"     = recall (noun)
      "afturkalla"    = withdraw
      "matvæli"       = food
      "ekki neyta"    = do not consume
      "ofnæmisvaki"   = allergen

Note: Iceland is on GMT year-round (no DST). Workflow runs at 21:00
Stockholm = 19:00 Reykjavík (or 20:00 during CEST). Acceptable for
daily aggregation.
"""

from .base import CountryConfig, RssSource, register


ICELAND = CountryConfig(
    code="is",
    name_en="Iceland",
    name_local="Ísland",

    authority_short="MAST",
    authority_full="Matvælastofnun (Icelandic Food and Veterinary Authority)",
    authority_domain="mast.is",
    authority_item_url_regex=r"(innkall|frettir|tilkynn|matvaeli)",

    rss_sources=[
        # Major Icelandic news outlets
        RssSource("ruv.is", [
            "https://www.ruv.is/rss/frettir",
        ]),
        RssSource("mbl.is", [
            "https://www.mbl.is/feeds/innlent/",
            "https://www.mbl.is/feeds/nyjast/",
        ]),
        RssSource("visir.is", [
            "https://www.visir.is/rss/frettir",
        ]),
        RssSource("dv.is", [
            "https://www.dv.is/feed/",
        ]),
        RssSource("vb.is", [
            "https://www.vb.is/rss",
        ]),
    ],
    google_news_domains=[
        "mast.is",
        "ruv.is", "mbl.is", "visir.is", "dv.is", "vb.is",
        "frettabladid.is", "vidskiptabladid.is",
    ],
    google_news_keywords=[
        "MAST innköllun matvæli",
        "innkalla matvæli salmonella",
        "innkalla listeria",
        "matvælainnköllun",
        "matvælavarúðarorð",
        '"Bónus" innkalla',
        '"Krónan" innkalla',
        '"Hagkaup" innkalla',
    ],

    bulk_index_queries=[
        "site:mast.is innköllun 2026",
        "site:mast.is Salmonella OR Listeria",
        "site:mast.is matvæli varúðarorð",
    ],

    language_name="Icelandic",
    language_code="is",
    brand_handling_note=(
        "Icelandic uses unique characters (þ, ð, æ, ó, ý). Preserve all "
        "original characters. Examples: 'Bónus', 'Krónan', 'Hagkaup', "
        "'Nettó', 'Iceland Foods', 'Mjólkursamsalan', 'Sláturfélag Suðurlands'."
    ),

    recall_signal_terms=[
        "innkalla", "innkallar", "innköllun", "innkallað", "innkalla matvæli",
        "afturkalla", "afturköllun",
        "matvælavarúðarorð", "matvælavarning",
        "ekki neyta", "ekki borða",
        "mast", "matvælastofnun",
        "salmonella", "listeria", "listeria monocytogenes",
        "ekki tilkynnt", "ekki gefið upp",
    ],

    timezone="Atlantic/Reykjavik",
    run_local_hour=21,
    # Reykjavik is GMT year-round (no DST since 1968), so 21:00 local IS
    # 21:00 UTC, in both halves of the year.
    #
    # CORRECTED 2026-09-23. This read (19, 20) — Central-European numbers,
    # copied from a Nordic sibling. The old comment reasoned in Stockholm
    # time and then contradicted itself inside one sentence ("19:00 ... CET
    # winter or 19:00 ... CEST summer"), while the value said two different
    # hours. Nothing failed: a wrong offset is silent, it just runs the
    # country at the wrong hour forever. Iceland had been running at 19:00
    # and 20:00 local. Asserted now in
    # tests/test_country_config_conformance.py against the real tz database.
    cron_utc_offsets=(21, 21),
)


register(ICELAND)
