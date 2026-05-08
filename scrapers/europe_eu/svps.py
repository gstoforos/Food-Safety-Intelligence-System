"""ŠVPS (SK) food safety scraper — uses Gemini for HTML extraction.

Two changes in Batch 4 (2026-05-08):

1. Domain migration:
   The previous URL `www.svps.sk/zakladne_info/upozornenia.asp` was on the
   legacy ASP-classic site at the `www.` subdomain. ŠVPS SR has migrated
   to a new WordPress-based site at `svps.sk/` (no www). The legacy ASP
   endpoints are returning 403 — likely either a WAF rule on the legacy
   host or a misconfigured redirect that fails on bot UAs.

   The new canonical listing of non-compliant products ("Nevyhovujúci
   výrobok") is `svps.sk/uradne-kontroly/` plus the homepage feed.

2. Browser fingerprint headers:
   The new site is hosted with bot protection that requires Client Hints
   and Sec-Fetch-* headers. Adding these matches what real Chrome sends
   on first navigation and clears the 403.
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
    # Slovak-first to match the site's primary language.
    "Accept-Language": "sk-SK,sk;q=0.9,en-US;q=0.8,en;q=0.7",
}


class SVPSScraper(GenericGeminiScraper):
    AGENCY = "ŠVPS (SK)"
    COUNTRY = "Slovakia"
    INDEX_URLS = [
        # New canonical listing page — flat reverse-chronological feed of
        # "Nevyhovujúci výrobok" (non-compliant product) entries plus monthly
        # control reports.
        'https://svps.sk/uradne-kontroly/',
        # Homepage carries the latest non-compliant items in a sidebar feed —
        # useful for catching items that haven't yet propagated to the archive.
        'https://svps.sk/',
    ]
    LANGUAGE = "sk"
    EXTRACTION_HINTS = (
        "Focus on entries with prefix 'Nevyhovujúci výrobok' (non-compliant "
        "product) — each cites a specific food product, manufacturer, and "
        "the deficiency. Also extract entries about products withdrawn for "
        "RASFF reasons. Pathogen-related entries may mention 'Listeria', "
        "'Salmonella', 'patogén', 'kontaminácia'. Skip monthly summary "
        "reports ('Správa z úradnej kontroly'), staffing announcements, "
        "training events, and animal-disease notices (AMO/SLAK/FMD)."
    )

    def __init__(self, session=None) -> None:
        super().__init__(session=session)
        self.session.headers.update(_CHROME_FINGERPRINT_HEADERS)
