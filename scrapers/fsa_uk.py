"""Livsmedelsverket (SE) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class LivsmedelsverketScraper(GenericLLMScraper):
    AGENCY = "Livsmedelsverket (SE)"
    COUNTRY = "Sweden"
    INDEX_URLS = ['https://www.livsmedelsverket.se/livsmedel-och-innehall/aterkallade-varor']
    LANGUAGE = "sv"
