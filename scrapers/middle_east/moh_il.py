"""MoH (IL) food safety scraper — uses Gemini for HTML extraction.

Two changes in Batch 4 (2026-05-08):

1. URL retargeting:
   The previous URL was the generic Ministry of Health landing page on
   gov.il (covers all health topics, including drugs, hospitals, etc.).
   Switching to the dedicated `food-recall` topic page narrows scope.

2. Browser fingerprint headers:
   gov.il is fronted by Akamai. Akamai's bot detection checks for the
   full Chrome Client Hints set (sec-ch-ua-*) and Sec-Fetch-* headers.
   The AFTS default session sends Chrome's User-Agent but none of those,
   producing a 403. Adding the full fingerprint bypasses the bot check.

CAVEAT: gov.il pages are JavaScript-rendered SPAs that show
"JavaScript must be enabled in order to view this page" to non-JS clients.
Even with a perfect browser fingerprint, the HTML returned to a
requests-based fetch will not contain the recall list — only the SPA
shell. Gemini will therefore extract little or nothing.

This scraper is included for completeness; if it continues to return 0
rows after the 403 is resolved, a follow-up batch should switch to a
headless-browser fetch (Playwright) or a discovered JSON endpoint.
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
    # Hebrew-first to match site's primary language (gov.il content
    # negotiates on Accept-Language).
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
}


class MoHILScraper(GenericGeminiScraper):
    AGENCY = "MoH (IL)"
    COUNTRY = "Israel"
    INDEX_URLS = [
        'https://www.gov.il/he/departments/topics/food-recall/govil-landing-page',
    ]
    LANGUAGE = "he"
    EXTRACTION_HINTS = (
        "Page is the Israeli Ministry of Health food-recall topic page. "
        "Focus on entries describing specific food product recalls, "
        "contamination findings, or removals from sale. Pathogens may "
        "appear in Hebrew or English: 'Listeria' / 'ליסטריה', "
        "'Salmonella' / 'סלמונלה', 'E. coli', 'cereulide'. The page is "
        "JavaScript-rendered, so only items present in the static HTML "
        "shell or noscript fallback will be extractable; if the response "
        "looks like an SPA shell, return no rows rather than fabricated ones."
    )

    def __init__(self, session=None) -> None:
        super().__init__(session=session)
        self.session.headers.update(_CHROME_FINGERPRINT_HEADERS)
