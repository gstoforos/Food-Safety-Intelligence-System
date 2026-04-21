"""
EFET (GR) — Ενιαίος Φορέας Ελέγχου Τροφίμων (Hellenic Food Authority).

EFET publishes food safety recalls and warnings in Greek. The main
listings page shows recent ανακλήσεις (recalls) with product details.
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class EFETScraper(GenericGeminiScraper):
    AGENCY = "EFET (GR)"
    COUNTRY = "Greece"
    BASE_URL = "https://www.efet.gr"
    INDEX_URLS = [
        "https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-cat",
        "https://www.efet.gr/index.php/el/enimerosi/deltia-typou",
    ]
    LANGUAGE = "el"
    EXTRACTION_HINTS = """
EFET pages are in Greek. Key terms:
- ανάκληση = recall, απόσυρση = withdrawal
- Listeria = Λιστέρια, Salmonella = Σαλμονέλα
- Country is always "Greece" unless the product origin is specified otherwise.
- URL should be the individual article link, not the category listing page.
- Product descriptions may include lot numbers (παρτίδα) — include them.
"""

