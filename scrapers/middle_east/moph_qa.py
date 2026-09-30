"""MoPH (QA) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class MoPHQAScraper(GenericLLMScraper):
    AGENCY = "MoPH (QA)"
    COUNTRY = "Qatar"
    INDEX_URLS = ['https://www.moph.gov.qa/english/mediacenter/Announcements/Pages/default.aspx']
    LANGUAGE = "en"
