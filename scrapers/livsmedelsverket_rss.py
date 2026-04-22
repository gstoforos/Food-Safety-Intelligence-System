"""Livsmedelsverket (Sweden) news & alerts RSS feed."""
from __future__ import annotations
from scrapers._rss_base import BaseRegulatorRSS


class LivsmedelsverketRss(BaseRegulatorRSS):
    AGENCY = "Livsmedelsverket (SE)"
    COUNTRY = "Sweden"
    FEED_URL = "https://www.livsmedelsverket.se/nyhetsarkiv/?RssFeed=true"
    LANGUAGE = "sv"
    # Swedish pathogen names that might not be in the base keyword list
    EXTRA_PATHOGEN_KEYWORDS = (
        "återkallar", "återkallas",       # "recalls"
        "livsmedelsburen",                 # "foodborne"
        "bakterie", "toxin",
    )
