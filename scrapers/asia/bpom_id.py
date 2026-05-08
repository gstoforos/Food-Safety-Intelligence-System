"""BPOM (ID) food safety scraper — uses Gemini for HTML extraction.

TLS workaround (added 2026-05-07):
pom.go.id serves an incomplete HTTPS certificate chain — the GitHub Actions
runner's CA bundle can't follow it to a trusted root, producing
"CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate".

Since this scraper only reads public news pages and never sends credentials
or sensitive data, we disable certificate verification for this scraper's
session only. Each scraper instance gets its own session
(pipeline.run_all instantiates with `cls()`), so this does not leak.

The InsecureRequestWarning is suppressed at module-import time to keep the
orchestrator log clean.
"""
from __future__ import annotations

import urllib3
from urllib3.exceptions import InsecureRequestWarning

from scrapers._base import GenericGeminiScraper

# Suppress the verify=False warning that would otherwise fire on every
# request through this scraper's session. Module-level so it runs once.
urllib3.disable_warnings(InsecureRequestWarning)


class BPOMScraper(GenericGeminiScraper):
    AGENCY = "BPOM (ID)"
    COUNTRY = "Indonesia"
    INDEX_URLS = ['https://www.pom.go.id/siaran-pers']
    LANGUAGE = "id"

    def __init__(self, session=None) -> None:
        super().__init__(session=session)
        # Disable SSL verification for THIS scraper's session only.
        # See module docstring for rationale.
        self.session.verify = False
