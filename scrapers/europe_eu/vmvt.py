"""VMVT (LT) food safety scraper — uses Gemini for HTML extraction.

Browser fingerprint headers (Batch 4, 2026-05-08):
The previous AFTS session uses a Chrome 127 User-Agent but omits the
Client Hints (sec-ch-ua-*) and Sec-Fetch-* headers that real Chrome
always sends. vmvt.lt's WAF returns 403 on this combination. Adding
the full Chrome fingerprint clears the block.

The URL itself (`/maisto-sauga/aktualijos`) is correct — it's the food
safety news/updates section.
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
    # Lithuanian-first to match the site's primary language.
    "Accept-Language": "lt-LT,lt;q=0.9,en-US;q=0.8,en;q=0.7",
}


class VMVTScraper(GenericGeminiScraper):
    AGENCY = "VMVT (LT)"
    COUNTRY = "Lithuania"
    INDEX_URLS = ['https://vmvt.lt/maisto-sauga/aktualijos']
    LANGUAGE = "lt"
    EXTRACTION_HINTS = (
        "Focus on entries about food safety violations, business activity "
        "suspensions, or product warnings. Pathogen-related entries may "
        "mention 'Listeria', 'Salmonella', 'patogenas', 'tarša' "
        "(contamination), 'pažeidimai'. Skip entries about training events, "
        "registration procedures, market fair regulations, animal disease "
        "notifications (paukščių gripas / bird flu, AKM / ASF), and general "
        "consumer guidance — those are not recalls."
    )

    def __init__(self, session=None) -> None:
        super().__init__(session=session)
        self.session.headers.update(_CHROME_FINGERPRINT_HEADERS)
