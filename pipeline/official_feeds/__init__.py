"""
AFTS Food Safety Intelligence — Official-Feed Collector

A SEPARATE pipeline from the EU gap_finder. Where the gap finder scrapes
news + Google News + LLM-extracts fields (necessary for fragmented EU
national authorities with no English feed), this collector pulls STRUCTURED
recall data directly from official authority APIs/RSS in English-speaking
countries.

Key differences from gap_finder:
  - No news scraping, no Google News
  - No LLM extraction — authorities already separate company/product/hazard
  - Exact dedup by authority notation ID (not fuzzy Jaccard)
  - Tier classification reuses pipeline.gap_finder.rules.classify() on the
    clean hazard text the authority provides

Same output target: docs/data/recalls.xlsx Pending sheet, same schema.
"""
