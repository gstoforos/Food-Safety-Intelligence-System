"""MFDS (KR) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class MFDSScraper(GenericLLMScraper):
    AGENCY = "MFDS (KR)"
    COUNTRY = "South Korea"
    INDEX_URLS = ['https://www.mfds.go.kr/brd/m_99/list.do']
    LANGUAGE = "ko"
