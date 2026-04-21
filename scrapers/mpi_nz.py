"""
Outbreak News Today — RSS feed scraper.

https://outbreaknewstoday.com covers global infectious disease and foodborne
illness outbreaks. Run by Robert Herriman, it aggregates outbreak reports from
official sources worldwide (CDC, ECDC, WHO, national health authorities) and
provides timely coverage of emerging foodborne events.

Feed: https://outbreaknewstoday.com/feed/
Update frequency: daily
Coverage: Global — strong on international outbreaks underreported by US media
"""
from __future__ import annotations
from scrapers.news_feeds._news_base import BaseNewsScraper


class OutbreakNewsTodayScraper(BaseNewsScraper):
    SOURCE_NAME = "Outbreak News Today"
    FEED_URLS = [
        "https://outbreaknewstoday.com/feed/",
    ]
    # This site covers all infectious diseases (Dengue, Measles, etc.)
    # so we use PATHOGEN_STRICT to keep only food-related pathogens.
    PATHOGEN_STRICT = True
