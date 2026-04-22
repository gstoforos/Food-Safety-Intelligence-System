"""USDA FSIS recall RSS feed — more reliable than the JSON API."""
from __future__ import annotations
from scrapers._rss_base import BaseRegulatorRSS


class USDAFSISRss(BaseRegulatorRSS):
    AGENCY = "USDA FSIS"
    COUNTRY = "USA"
    FEED_URL = "https://www.fsis.usda.gov/fsis-recalls/rss.xml"
    LANGUAGE = "en"
    # FSIS feed is food-only by design, and includes non-pathogen recalls
    # (allergens, foreign objects) — RSS base class filters those out.
    REQUIRE_FOOD_CONTEXT = False
