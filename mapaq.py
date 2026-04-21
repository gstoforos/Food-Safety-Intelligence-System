"""MFDS (KR) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class MFDSScraper(GenericGeminiScraper):
    AGENCY = "MFDS (KR)"
    COUNTRY = "South Korea"
    INDEX_URLS = ['https://www.mfds.go.kr/brd/m_99/list.do']
    LANGUAGE = "ko"
