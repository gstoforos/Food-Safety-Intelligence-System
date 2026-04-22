"""EFSA (European Food Safety Authority) news RSS — EU-wide scientific arm.

EFSA doesn't issue recalls directly but publishes urgent scientific opinions
on outbreaks and contamination events, often 24-48h ahead of member-state
recalls. Useful early-warning signal.
"""
from __future__ import annotations
from scrapers._rss_base import BaseRegulatorRSS


class EFSARss(BaseRegulatorRSS):
    AGENCY = "EFSA (EU)"
    COUNTRY = "European Union"
    FEED_URL = "https://www.efsa.europa.eu/en/rss.xml"
    LANGUAGE = "en"
    REQUIRE_FOOD_CONTEXT = True  # EFSA covers non-food topics too
