"""
RASFF Window — EU Rapid Alert System for Food and Feed.

RASFF is the backbone of EU food safety notifications. The Window portal
shows real-time notifications from all EU/EEA member states. We scrape
multiple views to maximise coverage:
  1. The main search results page (default sorted by date)
  2. The notifications list filtered to the last 30 days
  3. The consumers portal (public-facing summary)

Uses Gemini to parse the search results page (RASFF Window has no public
JSON API — the internal API requires authentication).
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
        # Primary: search interface sorted by date (newest first)
        "https://webgate.ec.europa.eu/rasff-window/screen/search",
        # Consumers portal: simplified public-facing notification list
        "https://webgate.ec.europa.eu/rasff-window/screen/consumers",
    ]
    LANGUAGE = "en"
    EXTRACTION_HINTS = """
For RASFF notifications, extract these carefully:

DATE: Use the notification date (not the last-updated date).
COMPANY: Format as "Origin: <country of origin> | Distributed: <country list>"
         If only one country is listed, use it for both.
BRAND: Set to "—" if not specified (RASFF rarely includes brand names).
PRODUCT: Include the product category and specific description.
PATHOGEN: Extract the specific hazard (pathogen name, toxin, contaminant).
         For microbiological hazards, include species if given.
COUNTRY: Set to the ORIGIN country (where contamination occurred), not
         the notifying or distributing country.
CLASS: Map RASFF notification type:
       - "alert" -> "Alert"
       - "border rejection" -> "Border Rejection"
       - "information" -> "Information Notification"
       - "follow-up" -> skip (these are updates to existing notifications)
URL: Build the deep link: https://webgate.ec.europa.eu/rasff-window/screen/notification/<reference>
     where <reference> is the RASFF ref number (e.g. 2026.1234).
NOTES: Include the full RASFF reference number (e.g. "RASFF 2026.1234").

IMPORTANT: Skip follow-up notifications — only extract original notifications.
Only keep entries where the hazard is a pathogen, toxin, or biological contaminant.
Skip allergen-only, pesticide-only, additive, or labelling notifications.
"""
