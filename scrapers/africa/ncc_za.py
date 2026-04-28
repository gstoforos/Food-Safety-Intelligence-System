"""NCC (ZA) food safety scraper — uses Gemini for HTML extraction.

NCC's website does not have a single dedicated `/category/product-recalls/`
listing. The actual recall announcements live in two places:

  1. Media statements:        /media-statements-ncc/
  2. Product recall archive:  /product-recalls/   (paginated)
  3. Per-recall pages:        /product-recall-<slug>/  or
                              /product-safety-recall-<slug>/

The previous index URL `/category/product-recalls/` returned a category
page that often missed individual recalls. With the two index URLs below,
Gemini extraction picks up both vehicle and food/consumer-product recalls,
filtered downstream by pathogen/category logic.
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class NCCScraper(GenericGeminiScraper):
    AGENCY = "NCC (ZA)"
    COUNTRY = "South Africa"
    INDEX_URLS = [
        'https://thencc.org.za/media-statements-ncc/',
        'https://thencc.org.za/product-recalls/',
    ]
    LANGUAGE = "en"
