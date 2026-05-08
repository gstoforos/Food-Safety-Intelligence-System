"""FSSAI (IN) food safety scraper — uses Gemini for HTML extraction.

URL retargeting (Batch 3, 2026-05-08):
The previous URL `/cms/recall.php` is a procedural/educational page about
food recall regulations — not a feed of actual recalls. It returned 0 rows.

Two replacement URLs:

1. `/all-whatnew.php` — FSSAI's "What's New" listing covers seizures,
   regulatory actions, advisories, and safety updates. Recent items like
   "FSSAI West Region Seizes Misbranded Alkaline Water for Use of
   Non-Permitted Ingredients" (May 2026) appear here with dates.

2. `fira.fssai.gov.in/` — Food Import Rejection Alert portal, launched
   September 2024. Dedicated portal for food consignments rejected at
   Indian borders due to safety concerns. This is the closest India has
   to a structured recall feed.
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class FSSAIScraper(GenericGeminiScraper):
    AGENCY = "FSSAI (IN)"
    COUNTRY = "India"
    INDEX_URLS = [
        'https://fssai.gov.in/all-whatnew.php',
        'https://fira.fssai.gov.in/',
    ]
    LANGUAGE = "en"
    EXTRACTION_HINTS = (
        "Focus on entries about seizures ('Seizes Misbranded'), "
        "import rejections, recalls, withdrawals, or safety advisories "
        "involving specific products or brands. Skip items about "
        "training, internships, gazette notifications of officers, "
        "tender notices, and general regulatory amendments — those are "
        "not recalls. Pathogen-related entries may mention 'pathogen', "
        "'Salmonella', 'Listeria', 'E. coli', or 'microbial contamination'."
    )
