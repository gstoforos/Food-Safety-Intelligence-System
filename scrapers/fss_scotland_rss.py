"""FSA Scotland (Food Standards Scotland) news & alerts RSS feed.

Separate from FSA (UK) — Scotland has its own regulator covering Scottish
food safety incidents which sometimes don't cross into the UK-wide FSA feed.
"""
from __future__ import annotations
from scrapers._rss_base import BaseRegulatorRSS


class FSScotlandRss(BaseRegulatorRSS):
    AGENCY = "FSS (Scotland)"
    COUNTRY = "United Kingdom"
    FEED_URL = "https://www.foodstandards.gov.scot/news-and-alerts/feed"
    LANGUAGE = "en"
    REQUIRE_FOOD_CONTEXT = False  # feed is food-only by design
