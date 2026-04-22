"""Fødevarestyrelsen (Denmark) news RSS feed."""
from __future__ import annotations
from scrapers._rss_base import BaseRegulatorRSS


class FodevarestyrelsenRss(BaseRegulatorRSS):
    AGENCY = "Fødevarestyrelsen (DK)"
    COUNTRY = "Denmark"
    FEED_URL = "https://www.foedevarestyrelsen.dk/nyheder/rss.aspx"
    LANGUAGE = "da"
    EXTRA_PATHOGEN_KEYWORDS = (
        "tilbagekaldelse", "tilbagetrækning",  # "recall", "withdrawal"
        "fødevarebårne", "madforgiftning",     # "foodborne", "food poisoning"
    )
