"""FSAI (Food Safety Authority of Ireland) food alerts RSS feed."""
from __future__ import annotations
from scrapers._rss_base import BaseRegulatorRSS


class FSAIRss(BaseRegulatorRSS):
    AGENCY = "FSAI (IE)"
    COUNTRY = "Ireland"
    FEED_URL = "https://www.fsai.ie/rss.aspx"
    LANGUAGE = "en"
    REQUIRE_FOOD_CONTEXT = False  # feed is food-only
