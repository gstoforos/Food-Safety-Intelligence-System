"""
AFTS Food Safety Intelligence — Gap Finder
Country config: Nigeria (NAFDAC — National Agency for Food and Drug
                Administration and Control).

VERIFIED 2026-06 (Greece concept): NAFDAC publishes each recall/safety alert as
its own per-item HTML page on nafdac.gov.ng (WordPress), numbered
"Public Alert No. NN/YYYY", e.g.:
    https://nafdac.gov.ng/public-alert-no-08-2026-danone-nutricia-recalls-several-batches-of-aptamil-and-cow-and-gate-first-infant-milk-and-follow-on-milk-formula-products-due-to-potential-contamination-with-cereulide-toxin/
    https://nafdac.gov.ng/public-alert-no-04-2026-nestle-uk-recallsspecific-batches-of-its-smainfant-formula-and-follow-on-formulaover-toxin-contamination-concerns/
Index (recalls & alerts category):
    https://nafdac.gov.ng/category/recalls-and-alerts/
Each page carries product, hazard (cereulide/Bacillus cereus, undeclared
allergens, Salmonella, etc.), manufacturer, batch, alert number. English.

NOTE: NAFDAC regulates food AND drugs; alerts include counterfeit medicines,
test kits, etc. The classifier filters non-food/non-hazard items. Many NAFDAC
alerts mirror foreign recalls (RappelConso, EU RASFF) reaching Nigeria.

Clean Tier-2 authority_index_url country, like EFET (Greece). Direct GN hits on
nafdac.gov.ng alert posts are caught by Tier-0.
"""

from .base import CountryConfig, RssSource, register


NIGERIA = CountryConfig(
    code="ng",
    name_en="Nigeria",
    name_local="Nigeria",

    authority_short="NAFDAC",
    authority_full="National Agency for Food and Drug Administration and Control",
    authority_domain="nafdac.gov.ng",
    # Per-recall pages are numbered public-alert slugs:
    #   /public-alert-no-08-2026-...-cereulide-toxin/
    authority_item_url_regex=r"/public-alert-no-[a-z0-9][a-z0-9-]+/?",
    # Recalls & alerts index (verified live 2026-06).
    authority_index_url="https://nafdac.gov.ng/category/recalls-and-alerts/",

    # ── News sources (SIGNAL layer) ─────────────────────────────────────────
    rss_sources=[
        RssSource("punchng.com", [
            "https://punchng.com/feed/",
        ]),
        RssSource("vanguardngr.com", [
            "https://www.vanguardngr.com/feed/",
        ]),
        RssSource("guardian.ng", [
            "https://guardian.ng/feed/",
        ]),
    ],
    google_news_domains=[
        "nafdac.gov.ng",     # authority
        "punchng.com", "vanguardngr.com", "guardian.ng", "premiumtimesng.com",
        "legit.ng", "thisdaylive.com", "leadership.ng", "dailytrust.com",
        "thecable.ng", "allafrica.com",
    ],
    # English compound phrases — recall-action + hazard + NAFDAC.
    google_news_keywords=[
        '"NAFDAC" public alert recall',
        '"NAFDAC" unsafe product OR contamination',
        '"NAFDAC" recall food OR formula',
        '"Salmonella" OR Listeria OR cereulide NAFDAC',
        'NAFDAC alert do not consume',
    ],

    bulk_index_queries=[
        "site:nafdac.gov.ng public alert recall 2026",
        "site:nafdac.gov.ng public-alert-no contamination",
        "site:nafdac.gov.ng recall Salmonella OR Listeria OR cereulide OR allergen",
    ],

    language_name="English",
    language_code="en",
    brand_handling_note=(
        "Nigerian NAFDAC alerts are English/Latin script. Preserve casing. "
        "Capture the manufacturer and the foreign-regulator source when the "
        "alert mirrors an overseas recall (e.g. RappelConso, EU RASFF, FDA). "
        "Company forms 'Plc' / 'Ltd' should be kept as written."
    ),

    recall_signal_terms=[
        "recall", "recalled", "public alert", "alert notice",
        "unsafe", "do not consume", "withdraw", "mop up", "mop-up",
        "nafdac", "safety alert",
        # Pathogen/hazard cues
        "salmonella", "listeria", "cereulide", "bacillus cereus",
        "aflatoxin", "undeclared allergen", "contamination", "e. coli",
    ],

    timezone="Africa/Lagos",
    run_local_hour=21,
    # WAT = UTC+1 year-round (no DST). 21:00 local = 20:00 UTC.
    cron_utc_offsets=(20, 20),
)


register(NIGERIA)
