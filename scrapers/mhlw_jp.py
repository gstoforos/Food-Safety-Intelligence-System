"""MHLW (JP) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class MHLWScraper(GenericLLMScraper):
    AGENCY = "MHLW (JP)"
    COUNTRY = "Japan"
    INDEX_URLS = ['https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/kenkou_iryou/shokuhin/syokuchu/index.html']
    LANGUAGE = "ja"
