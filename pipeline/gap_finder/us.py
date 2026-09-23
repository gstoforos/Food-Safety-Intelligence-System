"""
AFTS Food Safety Intelligence — Gap Finder
Country config: United States (USDA FSIS — meat, poultry and egg products)

WHY THIS EXISTS (2026-09-23, found by a question rather than an audit)
=====================================================================
The audit that produced the other fourteen configs looked at Asia, Latin
America, the Middle East and Africa. North America looked healthy, because
the FDA scraper is healthy — it placed a row the same day.

It is not healthy. Asked whether a specific recall had been captured:

    /recalls-alerts/star-meat-delivery-inc--recalls-raw-pork-beef-and-goat-
        products-produced-without

    Star Meat Delivery Inc., 167,639 lb of raw beef, pork and goat,
    distributed NATIONWIDE, produced without federal inspection and
    bearing FALSE USDA inspection marks. FSIS Class I — "reasonable
    probability that use of the product will cause serious, adverse health
    consequences or death". Recalled 2026-09-23.

It is not in the register. The last FSIS row is Prime Line Distributors /
Ferrarini USA, added 2026-09-08 — fifteen days earlier.

The cause is the one already measured: fsis.usda.gov returns 403 to
datacentre traffic. It refused the audit sandbox exactly as it refuses the
scraper. scrapers/north_america/usda_fsis.py is not broken; it is blocked,
in the single largest meat-recall jurisdiction the register covers.

So the United States gets the Greek route like everywhere else: find the
recall in national media, confirm product, poundage, date and hazard from
the article, resolve the article back to the FSIS release, publish THAT
url. American coverage of a Class I meat recall is immediate, national,
and reliably links the FSIS page — this is one of the easier countries to
do this way, not one of the harder ones.

FSIS ONLY — FDA IS DELIBERATELY OUT OF SCOPE
--------------------------------------------
The United States splits food safety between two federal agencies:
  * FSIS (USDA) — meat, poultry, processed egg products. Blocked. Silent.
  * FDA — everything else. Working: last row 2026-09-23, 121 URLs in the
    register.
Only fsis.usda.gov is an authority domain here. Adding fda.gov would put a
gap finder on top of a scraper that already works, and every row it found
would arrive as a duplicate for dedupe to clean up. If the FDA scraper
ever goes quiet, widen this then — and say so here.

VERIFIED against the 24 real fsis.usda.gov URLs in the register:

    ITEM     /recalls-alerts/<slug>
             /recalls-alerts/015-2026-city-foods-inc-recalls-ready-eat-...
             /recalls-alerts/cs-beef-packers-llc-recalls-ground-beef-...
             /recalls-alerts/fsis-issues-public-health-alert-beef-kofta-...

    LISTING  /recalls-alerts?search=015-2026
             /recalls-alerts?search=019-020-2026
             /recalls-alerts?search=PHA-08082026-01

The listing differs from an item by one character — a "?" where an item
has "/" — and all three listing forms are already sitting in the Rejected
sheet, which is the gate doing its job. Requiring a slug AFTER the slash
is what keeps them out.

Also excluded: /food-safety/foodborne-illness-and-disease/outbreaks/...,
which is outbreak investigation, not a recall.

PUBLIC HEALTH ALERTS ARE IN SCOPE. FSIS issues a PHA instead of a recall
when the product is no longer available for recall or the producer cannot
be identified — the Kebab Shop kofta and the headcheese alerts in the
register are both PHAs. They are real food-safety actions on the same
board and the same URL shape, and the register already carries them.
"""

from .base import CountryConfig, RssSource, register


UNITED_STATES = CountryConfig(
    # ── Identity ────────────────────────────────────────────────────────────
    code="us",
    name_en="United States",
    name_local="United States",

    # ── Authority ───────────────────────────────────────────────────────────
    authority_short="FSIS",
    authority_full="USDA Food Safety and Inspection Service",
    authority_domain="fsis.usda.gov",
    # A slug must follow /recalls-alerts/. The listing is the same path with
    # a QUERY instead — see the docstring; all three of its forms are
    # already in Rejected.
    authority_item_url_regex=(
        r"^(?:https?://[^/]+)?/recalls-alerts/[a-z0-9][a-z0-9\-]+"
    ),
    authority_index_url="https://www.fsis.usda.gov/recalls",

    # ── News sources ────────────────────────────────────────────────────────
    # Food Safety News is the specialist outlet and covers every FSIS
    # release, usually within hours and always with the link. The national
    # outlets carry the Class I recalls.
    rss_sources=[
        RssSource("foodsafetynews.com", ["https://www.foodsafetynews.com/feed/"]),
        RssSource("apnews.com", ["https://apnews.com/hub/ap-top-news.rss"]),
        RssSource("npr.org", ["https://feeds.npr.org/1001/rss.xml"]),
    ],
    google_news_domains=[
        "foodsafetynews.com", "apnews.com", "reuters.com", "cnn.com",
        "nbcnews.com", "cbsnews.com", "abcnews.go.com", "usatoday.com",
        "npr.org", "newsweek.com", "people.com", "foodsafetymagazine.com",
        "thepacker.com", "meatpoultry.com", "foodnavigator-usa.com",
    ],
    google_news_keywords=[
        "USDA FSIS recall meat",
        "FSIS public health alert",
        "beef recall Listeria Salmonella E. coli",
        "chicken recall USDA",
        "meat recalled without inspection",
    ],

    # ── DDG bulk index queries ──────────────────────────────────────────────
    bulk_index_queries=[
        "site:fsis.usda.gov recalls-alerts 2026 recalls",
        "site:fsis.usda.gov recalls-alerts public health alert 2026",
        "site:fsis.usda.gov recalls-alerts Listeria OR Salmonella",
        "site:fsis.usda.gov recalls-alerts undeclared allergen",
        "site:fsis.usda.gov recalls-alerts produced without inspection",
    ],

    # ── LLM extraction context ──────────────────────────────────────────────
    language_name="English",
    language_code="en",
    brand_handling_note=(
        "FSIS releases are in English and name the recalling FIRM and its "
        "city and state — keep the company exactly as published, including "
        "the '(City, ST)' where the release gives it, and any 'dba' trading "
        "name. Three identifiers matter and are distinct: the RECALL NUMBER "
        "(e.g. 019-020-2026, or PHA-08082026-01 for a public health alert), "
        "the ESTABLISHMENT NUMBER printed inside the USDA mark of "
        "inspection (e.g. 'EST. 12345' or 'P-12345'), and the product's own "
        "lot or pack date. Capture the recall number as the identifier. "
        "Note that FSIS states poundage explicitly ('approximately 167,639 "
        "pounds') and assigns a Class — I, II or III — where Class I is the "
        "most serious; both belong in the record. A recall of product "
        "'produced without the benefit of inspection' means the product "
        "never lawfully entered commerce at all, which is a different and "
        "usually broader hazard than a specific contaminant."
    ),

    # ── Title prefilter ─────────────────────────────────────────────────────
    recall_signal_terms=[
        "recall", "recalled", "recalls", "recalling",
        "public health alert", "fsis", "usda",
        "listeria", "salmonella", "e. coli", "e.coli",
        "undeclared", "misbranding", "allergen",
        "without the benefit of inspection", "uninspected",
        "do not eat", "do not consume", "throw it away",
        "class i", "meat", "poultry",
    ],

    # ── Scheduling ──────────────────────────────────────────────────────────
    # FSIS is federal and headquartered in Washington DC, so the register
    # follows Eastern time. EDT = UTC-4 in summer, EST = UTC-5 in winter,
    # so 21:00 Eastern is 01:00 UTC (summer) or 02:00 UTC (winter) the
    # FOLLOWING day.
    timezone="America/New_York",
    run_local_hour=21,
    cron_utc_offsets=(1, 2),
)

register(UNITED_STATES)
