"""NCC (ZA) food safety scraper — uses Gemini for HTML extraction.

URL retargeting (Batch 3, 2026-05-08):
The previous two URLs were:
  - /media-statements-ncc/   (mixed-content media hub, 0 rows)
  - /product-recalls/        (top-level archive page, 0 rows)

Both consistently returned 0 from Gemini. The actual recall listing —
where individual recalls appear with dates, product names, batch
numbers, and details — is the WordPress category page at
`/category/product-recalls/`.

Cross-checked 2026-05-08: the page contains entries like
  - "Top Score Instant Porridge brand" (precautionary recall)
  - "Dark and Lovely Moisture Plus Kits" (NCC consumer alert)
  - "Deli Hummus range manufactured by BM Foods... Listeria Monocytogenes"
all in the same listing format that Gemini extracts cleanly.

The previous docstring in this file claimed `/category/product-recalls/`
"often missed individual recalls" — that no longer matches reality, and
the older non-category URLs return 0. Reverting to the category URL.
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class NCCScraper(GenericGeminiScraper):
    AGENCY = "NCC (ZA)"
    COUNTRY = "South Africa"
    INDEX_URLS = [
        'https://thencc.org.za/category/product-recalls/',
    ]
    LANGUAGE = "en"
    EXTRACTION_HINTS = (
        "Extract product recalls with their dates, product names, batch "
        "numbers, and reason. NCC recalls cover food, drugs, cosmetics, "
        "vehicles, and electronics — only food-related entries are in "
        "scope. Pathogen-related entries may mention 'Listeria', "
        "'Salmonella', 'Bacillus cereus', 'cereulide', 'aflatoxin', "
        "or 'foodborne bacteria'. Vehicle recalls (cars, motorcycles, "
        "trucks) are not in scope and should be skipped."
    )
