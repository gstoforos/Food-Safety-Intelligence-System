"""
RASFF Window — EU Rapid Alert System for Food and Feed.
Uses Gemini to parse the search results page (RASFF Window has no public JSON API).
"""
from __future__ import annotations
from typing import List
import logging
from scrapers._base import GenericGeminiScraper

log = logging.getLogger(__name__)


class RASFFScraper(GenericGeminiScraper):
    AGENCY = "RASFF (EU)"
    COUNTRY = ""  # Multi-country; pathogen origin extracted per-row by Gemini
    BASE_URL = "https://webgate.ec.europa.eu/rasff-window"
    INDEX_URLS = [
        # Public search interface — Gemini extracts notifications
        "https://webgate.ec.europa.eu/rasff-window/screen/search",
    ]
    LANGUAGE = "en"
    EXTRACTION_HINTS = """
For RASFF notifications, format the Company field as:
"Origin: <country of origin> | Distributed: <country list>"

Set Brand to "—" if not specified.
Set Country to the ORIGIN country (where contamination occurred), not the distributing country.
Include the RASFF reference number in Notes (e.g. "RASFF 2026.1234").
URL should be the deep link to the specific notification, not the search page.
"""
