"""BVL (Germany) lebensmittelwarnung.de RSS feed.

RSS alternative to the existing HTML/Gemini BVL scraper. Runs in parallel —
merge_master.py dedups by URL so no duplicates are created. Provides belt-and-
suspenders coverage if the HTML layout changes and Gemini starts failing.
"""
from __future__ import annotations
from scrapers._rss_base import BaseRegulatorRSS


class BVLRss(BaseRegulatorRSS):
    AGENCY = "BVL (DE) RSS"
    COUNTRY = "Germany"
    FEED_URL = "https://www.lebensmittelwarnung.de/bvl-lmw-de/rss/warnungen_aktuell"
    LANGUAGE = "de"
    EXTRACT_HINTS = ""
    EXTRA_PATHOGEN_KEYWORDS = (
        "rückruf", "rückrufe",             # "recall(s)"
        "keime", "bakterien",              # "germs", "bacteria"
        "verunreinigung", "kontamination", # "contamination"
        "lebensmittelvergiftung",          # "food poisoning"
    )
    REQUIRE_FOOD_CONTEXT = False  # feed is food-only
