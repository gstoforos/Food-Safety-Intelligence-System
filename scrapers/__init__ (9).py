"""
Food Safety News — RSS feed scraper.

https://www.foodsafetynews.com is the most widely cited dedicated food safety
news outlet, run by Marler Clark (the food safety law firm). Its editorial
team covers recalls, outbreaks, regulatory enforcement, and food science
globally. The RSS feed provides full-text or summary content for each post.

Feed: https://www.foodsafetynews.com/feed/
Update frequency: multiple times daily
Coverage: Global (USA-heavy, but covers EU, Asia, Oceania outbreaks)
"""
from __future__ import annotations
from scrapers.news_feeds._news_base import BaseNewsScraper


class FoodSafetyNewsScraper(BaseNewsScraper):
    SOURCE_NAME = "Food Safety News"
    FEED_URLS = [
        "https://www.foodsafetynews.com/feed/",
    ]
    # FSN articles are almost always food-safety-relevant, but we still
    # filter to pathogen/recall/outbreak content to avoid opinion pieces
    # and regulatory-process articles that aren't actionable intelligence.
    PATHOGEN_STRICT = False
