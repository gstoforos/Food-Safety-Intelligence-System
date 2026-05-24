"""
AFTS Food Safety Intelligence — Gap Finder (multi-country)

Discovers food recall news that hasn't yet appeared in our primary FSIS pipeline,
verifies it against the country's official food authority via search-engine
snippets, classifies hazard tier, and extracts structured fields.

Supports: Greece (gr), Italy (it). Designed to add ES, PT, CH, BE, DE, AT, NL
by writing one ~80-line country config per addition.

Entry point: `python -m pipeline.gap_finder.main --country <ISO2>`
"""

from . import countries, news_scraper, search_verifier, extractor, llama_client, rules, main  # noqa: F401
