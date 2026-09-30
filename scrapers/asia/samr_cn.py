"""SAMR (CN) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class SAMRScraper(GenericLLMScraper):
    AGENCY = "SAMR (CN)"
    COUNTRY = "China"
    INDEX_URLS = ['https://www.samr.gov.cn/zw/zfxxgk/fdzdgknr/spcjs/']
    LANGUAGE = "zh"
