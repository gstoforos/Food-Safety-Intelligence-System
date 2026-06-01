"""
Google News supplement for the official-feed collector.

Authority websites/RSS have proven flaky (HTML scrapers break, RSS 404s,
bot-blocks). This module adds a Google News pass as insurance: it queries
the authority name + recall/pathogen terms, and — per AFTS spec — names the
last N calendar dates explicitly (today, today-1, today-2) so Google surfaces
the most recent recalls rather than historical ones. A `when:7d` operator is
also appended as a recency belt-and-suspenders.

Returns normalized Record objects (same shape as official API records), so
downstream classify/dedup/xlsx code is uniform.
"""

from __future__ import annotations

import hashlib
import urllib.parse
from datetime import datetime, timedelta, timezone

from .base import Record
from .fetch import get_rss

GN_RSS = "https://news.google.com/rss/search"

# A Google News headline must contain one of these recall signals to be kept.
# Without this gate, news *about* pathogens ("Salmonella cases reach 17-year
# high") gets misclassified as a recall. Authority API records skip this gate
# (they are definitionally recalls).
#
# CRITICAL: FSIS/FDA frequently issue "Public Health Alert" / "Safety Alert"
# / "Health Advisory" instead of "recall" when the product is no longer
# commercially available. Those phrasings MUST pass the gate — the Kebab Shop
# STEC outbreak (May 24, 2026) was missed because earlier versions of this
# list lacked "public health alert".
_RECALL_SIGNALS = (
    "recall", "recalled", "recalls", "recalling",
    "withdraw", "withdrawn", "withdrawal",
    "do not eat", "do not consume", "don't eat",
    "pulled from", "pulls", "pull from", "taken off",
    "removed from sale", "off the shelves", "urgent warning",
    # Authority-issued alerts (FSIS/FDA/CFIA phrasing)
    "safety alert", "alert issued", "alert issues",
    "public health alert", "health alert",
    "fsis alert", "fda alert", "usda alert", "cfia alert",
    "issues alert", "issues advisory", "issues warning",
    "food alert", "food advisory", "health advisory",
    "outbreak alert", "outbreak investigation",
    # Authority verbs ("FSIS warns of...", "FDA warns consumers...")
    "warns of", "warns consumers", "warns shoppers", "warns the public",
    "warning about", "warning issued", "warning over", "warning for",
    "warning to consumers", "warns against",
)


def _has_recall_signal(title: str) -> bool:
    t = (title or "").lower()
    return any(sig in t for sig in _RECALL_SIGNALS)


def _date_phrases(days_back: int) -> list[str]:
    """['May 28', 'May 27', 'May 26'] for days_back=3 (no leading zero)."""
    today = datetime.now(timezone.utc).date()
    out = []
    for d in range(days_back):
        dt = today - timedelta(days=d)
        # %-d is platform-dependent; build without leading zero manually
        out.append(f"{dt.strftime('%B')} {dt.day}")
    return out


def build_queries(authority: str, pathogen_terms: list[str],
                  days_back: int = 3) -> list[str]:
    """
    Build the Google News query set:
      - date-named queries: <authority> food recall|safety alert|public health
        alert <Month Day> for each of the last `days_back` days (George's
        explicit-date approach)
      - evergreen pathogen queries: <authority> recall|alert|public health
        alert|outbreak <pathogen> with recency handled by when:7d. The alert
        and PHA variants are required to catch FSIS Public Health Alerts and
        FDA Safety Alerts that do not use the word "recall" (e.g. the May 24
        2026 Kebab Shop kofta STEC PHA, which was missed in earlier runs).
    """
    queries: list[str] = []

    # Date-named recall queries (today, -1, -2, ...)
    for phrase in _date_phrases(days_back):
        queries.append(f'{authority} food recall {phrase}')
        queries.append(f'{authority} food safety alert {phrase}')
        queries.append(f'{authority} public health alert {phrase}')

    # Evergreen pathogen/hazard queries (recency via when:7d appended later)
    for term in pathogen_terms:
        queries.append(f'{authority} recall {term}')
        queries.append(f'{authority} alert {term}')
        queries.append(f'{authority} public health alert {term}')
        queries.append(f'{authority} outbreak {term}')

    return queries


def fetch_gnews(authority: str, country_code: str, country_name: str,
                authority_short: str, pathogen_terms: list[str],
                hl: str = "en-US", gl: str = "US", ceid: str = "US:en",
                days_back: int = 3, per_query_cap: int = 10,
                country_keywords: tuple = (),
                country_domains: tuple = (),
                block_title_keywords: tuple = (),
                use_description: bool = False) -> list[Record]:
    """
    Fetch Google News articles matching the source's recall queries.

    country_keywords / country_domains: optional country-scope filter.
        Many recall headlines are cross-border (a US FDA recall article
        surfaces in an Australian Google News locale search). To keep the
        AU/NZ collectors from labelling US recalls as AU/NZ recalls, pass
        a tuple of geographic title keywords (e.g. ('Australia',
        'Aussie', 'Sydney', ...)) and URL domain suffixes (e.g.
        ('.com.au', '.gov.au', ...)). An article is kept only if its
        title contains one of the keywords OR its URL contains one of
        the domain suffixes. Empty tuples disable the filter (legacy
        US/UK/Canada/Ireland behaviour unchanged).

    block_title_keywords: optional title denylist. An article is dropped
        if its title contains any of these keywords, even if it passed
        the country-scope filter. Used to suppress cross-border bleed
        where an AU/NZ news outlet (on a .com.au / .co.nz domain) is
        merely syndicating a US recall story — the title typically
        mentions a US-only retailer (Walmart, Kroger, Sam's Club) or
        agency (FDA, USDA, FSIS).
    """
    records: list[Record] = []
    seen_links = set()
    kw_lower = tuple(k.lower() for k in country_keywords)
    dom_lower = tuple(d.lower() for d in country_domains)
    block_lower = tuple(b.lower() for b in block_title_keywords)
    scope_active = bool(kw_lower or dom_lower)
    block_active = bool(block_lower)
    skipped_scope = 0
    skipped_block = 0
    sample_scope_drops: list = []   # diagnostic: first few URLs dropped at scope
    sample_kept: list = []          # diagnostic: first few KEPT articles

    for q in build_queries(authority, pathogen_terms, days_back):
        full_q = f"{q} when:7d"
        url = (f"{GN_RSS}?q={urllib.parse.quote(full_q)}"
               f"&hl={hl}&gl={gl}&ceid={ceid}")
        items = get_rss(url)
        kept = 0
        for it in items:
            link = it.get("link", "")
            title = it.get("title", "")
            if not title or link in seen_links:
                continue
            if not _has_recall_signal(title):
                continue          # news about a pathogen, not a recall — skip

            t_l = title.lower()
            u_l = (link or "").lower()

            # Country-scope filter (drops cross-border bleed at the
            # geography level — article isn't even about AU/NZ)
            if scope_active:
                in_kw = any(k in t_l for k in kw_lower)
                in_dom = any(d in u_l for d in dom_lower)
                if not (in_kw or in_dom):
                    skipped_scope += 1
                    if len(sample_scope_drops) < 5:
                        sample_scope_drops.append((title[:60], link[:120]))
                    continue

            # Title denylist (drops AU/NZ news outlets that are merely
            # syndicating a foreign recall — the title names a US
            # retailer or agency that doesn't operate in AU/NZ)
            if block_active and any(b in t_l for b in block_lower):
                skipped_block += 1
                continue

            seen_links.add(link)
            sid = "GN-" + hashlib.sha1(
                (link or title).encode("utf-8", "ignore")).hexdigest()[:12]
            description = it.get("description", "") or ""

            # Hazard text: title-only by default (legacy behaviour for
            # UK/IE/US/CA/AU/NZ — unchanged). Opt-in title+description
            # for sources whose headlines often omit the pathogen name
            # (e.g. Asia GNews via redirector URLs + brief headlines).
            if use_description:
                import re as _re
                desc_text = _re.sub(r"<[^>]+>", " ", description)
                desc_text = _re.sub(r"\s+", " ", desc_text).strip()
                hazard_text = (title + " || " + desc_text).strip()
            else:
                hazard_text = title

            rec = Record(
                source_id=sid,
                country_code=country_code,
                country_name=country_name,
                authority=f"{authority_short} (via Google News)",
                title=title,
                company="",
                product="",
                hazard=hazard_text,
                alert_type="recall",
                published=it.get("published"),
                url=link,
                raw={"gnews_query": q, "description": description},
            )
            records.append(rec)
            if use_description and len(sample_kept) < 3:
                sample_kept.append((title[:70], description[:120]))
            kept += 1
            if kept >= per_query_cap:
                break
    total_queries = len(build_queries(authority, pathogen_terms, days_back))
    notes = []
    if scope_active:
        notes.append(f"dropped {skipped_scope} out-of-scope")
    if block_active:
        notes.append(f"dropped {skipped_block} title-blocked")
    # Print sample of dropped URLs so we can see what Google actually returns
    if sample_scope_drops:
        print(f"  [GNews-debug] First {len(sample_scope_drops)} URLs dropped at scope:")
        for t, u in sample_scope_drops:
            print(f"     • {t!r}")
            print(f"       URL: {u}")
    if sample_kept:
        print(f"  [GNews-debug] First {len(sample_kept)} KEPT articles:")
        for t, d in sample_kept:
            print(f"     ✓ {t!r}")
            print(f"       desc: {d!r}")
    note = (", " + ", ".join(notes)) if notes else ""
    print(f"  [GNews] {authority}: {len(records)} candidate articles "
          f"across {total_queries} queries{note}")
    return records
