"""News-mirror and aggregator domains — hard reject.

Locked 2026-04-30. URLs containing any of these substrings get rejected
even if Source label claims a regulator. These are not official sources.
"""

NEWS_MIRROR_DOMAINS = (
    "sedaily.com",
    "reuters.com",
    "apnews.com",
    "bbc.com",
    "cnn.com",
    "foodsafetynews.com",
    "food-safety.com",
    "foodpoisonjournal.com",
    "outbreaknewstoday.com",
    "cidrap.umn.edu",
    "cbc.ca",
    "theguardian.com",
    "nytimes.com",
    "washingtonpost.com",
    "beaconbio.org",
    "ilfattoalimentare.it",
)


def is_news_mirror(url: str) -> bool:
    """True if URL host is on the news-mirror blocklist."""
    if not url:
        return False
    u = str(url).lower()
    return any(d in u for d in NEWS_MIRROR_DOMAINS)
