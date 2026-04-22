"""FSANZ (Food Standards Australia New Zealand) recalls RSS feed."""
from __future__ import annotations
from scrapers._rss_base import BaseRegulatorRSS


class FSANZRss(BaseRegulatorRSS):
    AGENCY = "FSANZ (AU)"
    COUNTRY = "Australia"
    FEED_URL = "https://www.foodstandards.gov.au/RSS/Recalls/Pages/default.aspx"
    LANGUAGE = "en"
    REQUIRE_FOOD_CONTEXT = False
