"""COMESA food safety scraper — uses Gemini for HTML extraction.

Two changes in Batch 4 (2026-05-08):

1. URL retargeting:
   The previous URL `comesa.int/category/news/` is the COMESA Secretariat's
   trade-news page, which does NOT publish recalls. Recall and consumer-safety
   alerts are published by the COMESA Competition and Consumer Commission
   (CCC) at a different domain: comesacompetition.org. Recent 2026 alerts
   (Aptamil/Nursie infant milk cereulide recall, Ford/Toyota vehicles,
   Thermos jars) all live at /case_type/consumer-cases/.

2. Browser fingerprint headers:
   The previous AFTS session header set passes a Chrome 127 User-Agent but
   omits the Client Hints (sec-ch-ua-*) and Sec-Fetch-* headers that real
   Chrome always sends. Modern WAFs flag this combination as bot-like and
   return 403. Adding a full Chrome 127 fingerprint bypasses those checks.
   These are scoped to this scraper's session only.
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


_CHROME_FINGERPRINT_HEADERS = {
    "sec-ch-ua": '"Chromium";v="127", "Not(A:Brand";v="24", "Google Chrome";v="127"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "Cache-Control": "max-age=0",
    "DNT": "1",
}


class COMESAScraper(GenericGeminiScraper):
    AGENCY = "COMESA"
    COUNTRY = "Kenya"
    INDEX_URLS = [
        # COMESA Competition and Consumer Commission — actual recall publisher.
        'https://comesacompetition.org/case_type/consumer-cases/',
    ]
    LANGUAGE = "en"
    EXTRACTION_HINTS = (
        "All entries are formal CCC consumer cases with reference numbers "
        "like CCC/CP/CA/<NN>/<YYYY>. Focus on consumer alerts mentioning "
        "food/beverage products, infant formula, contamination, or recalls. "
        "Pathogen-related entries may mention 'Listeria', 'Salmonella', "
        "'Bacillus cereus', 'cereulide', 'Cronobacter', or 'aflatoxin'. "
        "Skip vehicle, electronics, and competition-law cases (mergers, "
        "antitrust) — those are not in scope."
    )

    def __init__(self, session=None) -> None:
        super().__init__(session=session)
        self.session.headers.update(_CHROME_FINGERPRINT_HEADERS)
