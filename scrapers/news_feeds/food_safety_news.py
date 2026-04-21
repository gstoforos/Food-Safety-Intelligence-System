"""
Food Safety News — RSS feed scraper.

https://www.foodsafetynews.com covers foodborne-illness outbreaks, recalls,
regulatory enforcement, and litigation in the food industry. Published by
Marler Clark, it is one of the most comprehensive US-focused food-safety
news outlets.

Feed: https://www.foodsafetynews.com/feed/
Update frequency: daily
Coverage: Primarily USA with regular international coverage
"""
from __future__ import annotations
from scrapers.news_feeds._news_base import BaseNewsScraper


class FoodSafetyNewsScraper(BaseNewsScraper):
    SOURCE_NAME = "Food Safety News"
    FEED_URLS = [
        "https://www.foodsafetynews.com/feed/",
    ]
    # Food Safety News is food-specific by editorial mission, so we don't
    # need the strict pathogen filter — any item that mentions a pathogen
    # OR matches the broad food-safety context keywords is kept.
    PATHOGEN_STRICT = False
