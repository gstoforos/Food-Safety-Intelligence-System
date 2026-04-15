"""MHLW (JP) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class MHLWScraper(GenericGeminiScraper):
    AGENCY = "MHLW (JP)"
    COUNTRY = "Japan"
    INDEX_URLS = ['https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/kenkou_iryou/shokuhin/syokuchu/index.html']
    LANGUAGE = "ja"
