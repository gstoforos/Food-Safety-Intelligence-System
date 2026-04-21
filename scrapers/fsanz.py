"""
Food Poison Journal — RSS feed scraper.

https://foodpoisonjournal.com covers foodborne illness litigation, outbreak
investigations, and recall developments. Run by Marler Clark, it provides a
legal/victim-centric lens on food safety events that complements the editorial
coverage from Food Safety News.

Feed: https://foodpoisonjournal.com/feed/
Update frequency: several times per week
Coverage: Primarily USA, some international outbreaks
"""
from __future__ import annotations
from scrapers.news_feeds._news_base import BaseNewsScraper


class FoodPoisonJournalScraper(BaseNewsScraper):
    SOURCE_NAME = "Food Poison Journal"
    FEED_URLS = [
        "https://foodpoisonjournal.com/feed/",
    ]
    PATHOGEN_STRICT = False
