"""USDA FSIS Recalls + Public Health Alerts API.

WHY THIS REWRITE EXISTS (audit 2026-05-10)
==========================================
The 2026-05-09 production run logged this and nothing else for FSIS:

    15:00:39,132 [START] USDA FSIS/USA
    15:00:39,218 [DONE]  USDA FSIS/USA -> 0 recalls

86ms end-to-end is the signature of `fetch()` returning None on a fast
DNS/TCP/Akamai rejection — a successful API call would take 0.3–2s.
The previous implementation had three return paths that swallowed
errors silently (`if not r: return []`, bare `except: return []`, no
status-code check), so the orchestrator could not distinguish:

  (a) Akamai 403 / bot block (most likely given the 86ms timing)
  (b) JSON parse error from a redesigned response
  (c) Field-name mismatch causing every row to fall through filters
  (d) Genuine zero — no pathogen FSIS recalls in 7 days
      (FSIS volume is roughly 1–5 pathogen recalls per MONTH; recent
      activity is allergen/misbranding-dominated)

This rewrite makes every failure path loud so the next production run
deterministically tells us which of (a)–(d) was the cause.

Plus four real bugs the previous code carried regardless of API health:

  1. Pathogen field bug. Line 59 (old):
         Pathogen=rec.get("field_summary", "")[:200]
     stored 200 chars of HTML summary as the canonical Pathogen value.
     _new_recall calls normalize_pathogen() on it, which expects names
     like "Salmonella" or "Listeria monocytogenes" — not
     "<p dir='ltr'><strong>WASHINGTON, ...". So even when matches
     happened, Pathogen ended up garbage and Tier assignment failed.

  2. Wrong date field name. Code looked up field_recall_date which
     does not appear in the FSIS API docs (Aug 2025) example. Falling
     back to field_last_modified_date — also not in the docs. Result:
     date_str empty for every row, every row skipped. We now try a
     ranked list of plausible names and log the first one that yielded
     a parseable date so the schema is observable.

  3. Pathogen scan only on field_summary. Title and product_items not
     scanned — if FSIS phrases the pathogen in the headline but not the
     summary body, we miss it. Now scans
     title + summary + product_items combined.

  4. Outbreak detection used "illness" or "outbreak" substring. The
     FSIS API exposes field_related_to_outbreak as a "True"/"False"
     string — authoritative source. Use that first; fall back to the
     same 7-token list as fda_press.py for parity.

DESIGN DECISIONS (mirrors fda_listing.py for codebase consistency)
==================================================================
1. Akamai bypass headers — same set as fda_press.py / fda_listing.py,
   with API-style sec-fetch (empty/cors/same-origin) since this is an
   XHR-style JSON API call, not a page navigation.

2. Status code check after fetch — log on any non-200. The previous
   code only checked `if not r`, which is True only when fetch()
   returned None entirely; a 403 with a body would be treated as
   success and JSON-parse-fail silently.

3. Defensive multi-field date parsing. We try four plausible field
   names in priority order. The first one that parses to a valid date
   wins, and we log which field worked once per scrape so the actual
   schema is observable.

4. Strip HTML before pathogen scan. field_summary contains real HTML
   markup (verified in API docs). Substring match on raw HTML works
   for most pathogens but breaks on, e.g., split text like
   "Listeria <em>mono</em>cytogenes". Strip tags first.

5. Skip archived and Spanish records.
   - field_archive_recall == "True" → terminated, kept by FSIS for
     historical reference; we only want active recalls.
   - langcode == "Spanish" → field_summary is in Spanish; English
     pathogen vocab won't reliably match. Same pathogens are also
     emitted with langcode="English" (FSIS issues bilingual records),
     so dropping Spanish doesn't lose data.

6. URL — taken from field_recall_url / url / field_url /
   field_en_press_release / field_press_release, never synthesised from
   the title slug. Site-relative values are resolved against the FSIS
   origin (see AUDIT 2026-07-29 below); if every field is missing the
   row is dropped and logged. (Project rule: real URLs only — no slug
   guessing.)

7. Both Recalls and Public Health Alerts kept in scope. PHAs are
   sometimes pathogen-driven (e.g. raw beef PHAs for STEC). Filtered
   downstream by Pathogen + Outbreak fields, not by recall type.

AUDIT 2026-07-29 — three stacked bugs, all fixed here
=====================================================
The 2026-05-10 rewrite above made the failure paths loud, and the loud
output was then misread as "quiet week". FSIS contributed 0 rows from
2026-06-25 to 2026-07-29. Run log:

    USDA FSIS: 0 pathogen recalls in 7-day window (2013 records scanned,
    skipped: archived=1832 spanish=5 no_date=0 old=174 no_pathogen=0
    no_url=0 bad_url=2)

2013 - 1832 - 5 - 174 - 2 = 0. Filters run in the order
archived -> spanish -> date -> old -> url -> pathogen, so those 2
bad_url drops were the ONLY records to survive the 7-day window, and
no_pathogen=0 proves nothing ever reached the pathogen scan.

  A. URL GATE. _ACCEPTABLE_URL_PREFIXES demands an absolute
     https://www.fsis.usda.gov/... link. The API emits a site-relative
     path, so the gate rejected 100% of in-window records.
     -> _canonicalise_fsis_url() resolves relative / bare-http / no-www /
        protocol-relative forms before the gate, without ever rewriting a
        foreign host into an FSIS one. Any surviving rejection now logs
        the offending value at WARNING, and a scrape where zero records
        reach the pathogen scan logs at ERROR.

  B. PATHOGEN VOCABULARY. PATHOGEN_KEYWORDS used for_languages("en"),
     which is pathogens ∪ recall_signals. Every FSIS title contains
     "Recalls", so the gate matched everything and stamped
     Pathogen="recall" on misbranding / retraction / import-violation
     rows — the exact regression _pathogen_vocab.py was split to prevent.
     Invisible because bug A rejected the rows first; fixing A alone
     would have flooded Pending with junk.
     -> switched to pathogens("en") (149 hazard terms, still includes
        "metal fragment" / "glass fragment").

  C. PATHOGEN IDENTITY. _matched_pathogen_keyword returns the FIRST
     vocabulary hit and the vocabulary lists generic before serotype, so
     a real "E. coli O157:H7" recall stored as
     "Escherichia coli (generic)" — downgrading a STEC Class I.
     -> the matched keyword now decides SCOPE only; identity comes from
        the authoritative field_recall_reason when it normalises.

  D. TEST COVERAGE (the reason A-C survived). The test module lived at
     scrapers/test_usda_fsis_scraper.py while pytest.ini sets
     `testpaths = tests`, so CI never collected it; run by hand, 14 of
     15 tests failed because _MockResponse had no .status_code. Moved to
     tests/, mock completed, and every fixture URL shape above is now
     pinned by a regression test.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
import logging
import re

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall, normalize_pathogen
from scrapers._pathogen_vocab import pathogens as _pathogen_vocab_en

log = logging.getLogger(__name__)


# Outbreak token list — same as fda_press.py / fda_listing.py for
# cross-source parity. Used as fallback when field_related_to_outbreak
# is missing or non-canonical.
_OUTBREAK_TOKENS = (
    "outbreak", "illnesses linked", "linked to illness",
    "linked to investigation", "associated with illness",
    "cases of illness", "reported illnesses",
)


def _detect_outbreak(text_lower: str) -> int:
    return 1 if any(t in text_lower for t in _OUTBREAK_TOKENS) else 0


def _matched_pathogen_keyword(
    text_lower: str, keywords: Tuple[str, ...]
) -> Optional[str]:
    for kw in keywords:
        if kw in text_lower:
            return kw
    return None


def _strip_html(s: str) -> str:
    """Strip HTML tags + collapse whitespace. FSIS field_summary is HTML."""
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"&nbsp;", " ", s)
    s = re.sub(r"&amp;", "&", s)
    s = re.sub(r"&lt;", "<", s)
    s = re.sub(r"&gt;", ">", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_date_any(s: str) -> Optional[datetime]:
    """Parse FSIS date strings — they ship in several formats across fields."""
    if not s:
        return None
    s = s.strip()
    # Common formats observed in FSIS records (date+time and date-only).
    # field_closed_date is "YYYY-MM-DD"; field_recall_date often
    # "MM/DD/YYYY"; some records carry ISO-8601 with T separator.
    for fmt in (
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(s[: len(fmt)] if "T" in fmt or " " in fmt else s, fmt)
        except ValueError:
            continue
    # Last resort — fromisoformat handles ISO variants Python understands
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


_FSIS_ORIGIN = "https://www.fsis.usda.gov"

# Hosts that are genuinely FSIS and may be rewritten to the canonical
# origin. Anything else is left untouched so the prefix gate still
# rejects it — this helper normalises, it never launders a foreign URL
# into an fsis.usda.gov one.
_FSIS_HOSTS = ("www.fsis.usda.gov", "fsis.usda.gov")


def _canonicalise_fsis_url(url: str) -> str:
    """Resolve an FSIS API link to a canonical absolute FSIS URL.

    The Recall API does not emit absolute links. Observed / documented
    shapes, all of which must end up as ``https://www.fsis.usda.gov/...``:

        /recalls-alerts/maple-leaf-foods-inc--recalls-not-ready-eat-...
        recalls-alerts/acme-deli                (no leading slash)
        http://www.fsis.usda.gov/recalls-alerts/x   (bare http)
        https://fsis.usda.gov/recalls-alerts/x      (no www)

    Anything already canonical is returned unchanged, and any URL on a
    host that is not FSIS is returned unchanged so the caller's prefix
    gate still rejects it.
    """
    u = (url or "").strip()
    if not u:
        return ""

    # Protocol-relative → https
    if u.startswith("//"):
        u = "https:" + u

    if u.startswith(("http://", "https://")):
        scheme, _, rest = u.partition("://")
        host, slash, path = rest.partition("/")
        if host.lower() not in _FSIS_HOSTS:
            return u          # foreign host — leave it for the gate to reject
        return _FSIS_ORIGIN + (slash + path if slash else "")

    # Site-relative path. Only accept path-looking values; a bare word or
    # a mailto:/tel: style scheme must NOT be turned into an FSIS URL.
    if ":" in u.split("/", 1)[0]:
        return u              # some other scheme — leave it to be rejected
    return _FSIS_ORIGIN + ("" if u.startswith("/") else "/") + u


# Date field names tried in priority order. The first one that yields
# a parseable date wins. Logged per-scrape so the schema is observable.
_DATE_FIELD_CANDIDATES = (
    "field_recall_date",
    "field_recall_publication_date",
    "field_publication_date",
    "field_last_modified_date",
    "field_closed_date",   # close date — used only as a last resort,
                           # since it represents termination, not issue
)


class USDAFSISScraper(BaseScraper):
    AGENCY = "USDA FSIS"
    COUNTRY = "USA"

    BASE_URL = "https://www.fsis.usda.gov/fsis/api/recall/v/1"

    # AUDIT 2026-07-29 — was `for_languages("en")`, which is
    # pathogens ∪ recall_signals. recall_signals is the 9-word set
    # ("recall", "recalled", "recalls", "recalling", "withdrawal",
    # "withdrawn", "withdraws", "alert", "warning") and _pathogen_vocab.py
    # says so in its own header:
    #
    #     "Generic recall-event verbs and warning words. Useful for 'is this
    #      a recall page' detection but should NOT be used as a pathogen
    #      filter."
    #
    # …because every FSIS record contains the word "Recalls" in its title.
    # The pathogen gate therefore matched 100% of records and stamped
    # Pathogen="recall" on misbranding, retraction, undeclared-allergen and
    # import-violation rows — the exact failure the vocab split was created
    # to end. It stayed invisible only because the URL gate above was
    # rejecting every in-window record first; fixing that bug alone would
    # have flooded Pending with junk.
    #
    # pathogens("en") is the 149-term hazard vocabulary and still includes
    # the non-microbial hazards this scraper must catch ("metal fragment",
    # "glass fragment", …).
    PATHOGEN_KEYWORDS = _pathogen_vocab_en("en")

    # FSIS recall page URL prefix. Anything outside this path is rejected
    # (defensive — the API should only emit canonical recall URLs but we
    # have seen federal APIs return links to landing pages on edge cases).
    _ACCEPTABLE_URL_PREFIXES = (
        "https://www.fsis.usda.gov/recalls-alerts/",
        "https://www.fsis.usda.gov/recalls/",
        "https://www.fsis.usda.gov/news-events/",
    )

    # Akamai bot-detection bypass — copy of the set in fda_press.py /
    # fda_listing.py, with sec-fetch tuned for an API XHR (not a page
    # navigation). Without these, fsis.usda.gov returns HTTP 403 from
    # GitHub Actions runner IPs.
    _AKAMAI_BYPASS_HEADERS = {
        "sec-ch-ua": (
            '"Not)A;Brand";v="99", "Google Chrome";v="127", '
            '"Chromium";v="127"'
        ),
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "Cache-Control": "max-age=0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.fsis.usda.gov/recalls",
    }

    def scrape(self, since_days: int = 30) -> List[Recall]:
        cutoff = datetime.utcnow() - timedelta(days=since_days)
        r = fetch(
            self.session, self.BASE_URL,
            headers=self._AKAMAI_BYPASS_HEADERS,
        )
        if r is None:
            log.warning(
                "USDA FSIS: fetch() returned None for %s "
                "(DNS, TCP, or transport-level failure)",
                self.BASE_URL,
            )
            return []
        if r.status_code != 200:
            log.warning(
                "USDA FSIS: HTTP %d from %s "
                "(Akamai bot-block likely; first 200 chars: %r)",
                r.status_code, self.BASE_URL, r.text[:200],
            )
            return []
        try:
            data = r.json()
        except ValueError as exc:
            log.warning(
                "USDA FSIS: JSON parse failed for %s: %s "
                "(response start: %r)",
                self.BASE_URL, exc, r.text[:200],
            )
            return []

        # FSIS has emitted both `[...]` and `{"data":[...]}` shapes
        # over the years. Be defensive.
        if isinstance(data, dict) and "data" in data:
            records = data["data"]
        elif isinstance(data, list):
            records = data
        else:
            log.warning(
                "USDA FSIS: unexpected top-level JSON shape: %s",
                type(data).__name__,
            )
            return []

        # Diagnostic counters — every drop-reason gets reported in the
        # final summary line so silent zeros become impossible.
        n_total = len(records)
        n_skipped_archived = 0
        n_skipped_spanish = 0
        n_skipped_no_date = 0
        n_skipped_old = 0
        n_skipped_no_pathogen = 0
        n_skipped_bad_url = 0
        n_skipped_no_url = 0

        # Actual offending values behind bad_url / no_url, so a schema
        # change is visible in the run log instead of being a silent zero.
        bad_url_samples: List[str] = []

        # Track which date-field actually held parseable dates this run
        date_field_hits: dict = {}

        out: List[Recall] = []
        seen_urls: set = set()

        for rec in records:
            try:
                # Skip archived (terminated old recalls retained for history)
                if (rec.get("field_archive_recall") or "").lower() == "true":
                    n_skipped_archived += 1
                    continue

                # Skip Spanish-language records (English vocab won't match;
                # FSIS issues English duplicates of all Spanish records,
                # so we lose nothing).
                if (rec.get("langcode") or "").lower() == "spanish":
                    n_skipped_spanish += 1
                    continue

                # Date — try candidate field names in priority order.
                d: Optional[datetime] = None
                used_field: str = ""
                for field_name in _DATE_FIELD_CANDIDATES:
                    raw = rec.get(field_name) or ""
                    parsed = _parse_date_any(raw)
                    if parsed is not None:
                        d = parsed
                        used_field = field_name
                        break
                if d is None:
                    n_skipped_no_date += 1
                    continue
                date_field_hits[used_field] = date_field_hits.get(used_field, 0) + 1

                if d < cutoff:
                    n_skipped_old += 1
                    continue

                # URL — never synthesised from a title slug. Try canonical
                # fields, then fall back to the documented press-release
                # fields.
                #
                # AUDIT 2026-07-29 — this gate was silently destroying 100%
                # of in-window FSIS recalls. The production run logged:
                #     2013 records scanned, skipped: archived=1832 spanish=5
                #     no_date=0 old=174 no_pathogen=0 no_url=0 bad_url=2
                # 2013 - 1832 - 5 - 174 - 2 = 0, and the checks run in the
                # order archived -> spanish -> date -> old -> url, so those 2
                # bad_url drops were the ONLY two records to survive the
                # 7-day window. Both died here. no_pathogen=0 is the proof:
                # nothing ever reached the pathogen scan.
                #
                # Cause: the value the API emits is not an absolute
                # https://www.fsis.usda.gov/... link, so startswith() against
                # _ACCEPTABLE_URL_PREFIXES could never be true.
                #   * `field_recall_url` does not appear in the published FSIS
                #     Recall API schema at all (checked against the official
                #     API documentation PDF, 2026-07-29). The documented link
                #     fields are field_en_press_release / field_press_release.
                #   * pipeline/regulator_apis.py:399, written earlier against
                #     live data, already carries the tell:
                #         if not url.startswith("http"):
                #             url = "https://www.fsis.usda.gov" + url
                #     i.e. the API hands back a site-relative path.
                # The unit-test fixture hard-codes absolute URLs, which is
                # why the suite stayed green while production kept zeroing.
                #
                # Fix: canonicalise before the prefix test, and make any
                # surviving rejection LOUD — log the offending value so a
                # future schema change is diagnosable from one run log
                # instead of another month of silent zeros.
                url = (
                    rec.get("field_recall_url")
                    or rec.get("url")
                    or rec.get("field_url")
                    or rec.get("field_en_press_release")
                    or rec.get("field_press_release")
                    or ""
                ).strip()
                if not url:
                    n_skipped_no_url += 1
                    continue
                url = _canonicalise_fsis_url(url)
                if not any(url.startswith(p) for p in self._ACCEPTABLE_URL_PREFIXES):
                    n_skipped_bad_url += 1
                    if len(bad_url_samples) < 5:
                        bad_url_samples.append(url[:160])
                    continue
                if url in seen_urls:
                    continue

                # Pathogen scan over title + cleaned summary + products.
                title = rec.get("field_title") or rec.get("title") or ""
                summary_html = rec.get("field_summary") or ""
                summary = _strip_html(summary_html)
                products = rec.get("field_product_items") or ""
                haystack = (title + " " + summary + " " + products).lower()

                matched_kw = _matched_pathogen_keyword(
                    haystack, self.PATHOGEN_KEYWORDS,
                )
                if not matched_kw:
                    n_skipped_no_pathogen += 1
                    continue

                # AUDIT 2026-07-29 — the matched keyword decides SCOPE (is
                # this in-scope at all?), but it must not decide IDENTITY.
                # _matched_pathogen_keyword returns the first vocabulary hit,
                # and the vocabulary lists the generic term before the
                # serotype, so a genuine "E. coli O157:H7" recall matched
                # "e. coli" and was stored as "Escherichia coli (generic)" —
                # downgrading a STEC Class I to a generic-coli tier.
                #
                # FSIS publishes the authoritative cause in
                # field_recall_reason ("E. coli O157:H7", "Listeria
                # monocytogenes", …), which normalize_pathogen resolves
                # correctly. Prefer it, exactly as the outbreak flag already
                # prefers field_related_to_outbreak; fall back to the matched
                # keyword only when the API field yields nothing canonical.
                reason_field = _strip_html(rec.get("field_recall_reason") or "")
                pathogen_value = matched_kw
                if reason_field and normalize_pathogen(reason_field):
                    pathogen_value = reason_field

                # Outbreak — prefer authoritative API field, fall back to text.
                api_outbreak = (rec.get("field_related_to_outbreak") or "").lower()
                if api_outbreak == "true":
                    outbreak = 1
                elif api_outbreak == "false":
                    outbreak = 0
                else:
                    outbreak = _detect_outbreak(haystack)

                # Class — keep API value if present (FSIS uses
                # "Class I" / "Class II" / "Class III"; some PHAs have
                # blank classification).
                cls = (
                    rec.get("field_recall_classification")
                    or rec.get("field_recall_type")
                    or "Recall"
                )

                # Company / establishment.
                company = (
                    rec.get("field_establishment")
                    or rec.get("field_recall_company")
                    or ""
                )

                # Notes — preserve recall number + states for downstream context.
                recall_no = rec.get("field_recall_number") or ""
                states = rec.get("field_states") or ""
                notes_parts = ["FSIS API"]
                if recall_no:
                    notes_parts.append(f"#{recall_no}")
                if states:
                    notes_parts.append(states)
                notes = "; ".join(notes_parts)

                out.append(self._new_recall(
                    Date=d.strftime("%Y-%m-%d"),
                    Company=company[:150],
                    Brand="—",
                    Product=(products or title)[:300],
                    Pathogen=pathogen_value,     # normalised by _new_recall
                    Reason=(summary or title)[:400],
                    Class=cls,
                    URL=url,
                    Outbreak=outbreak,
                    Notes=notes,
                ))
                seen_urls.add(url)
            except Exception as e:
                log.warning("USDA FSIS row parse failed: %s", e)

        # Loud summary — covers all four (a)–(d) ambiguity scenarios.
        log.info(
            "USDA FSIS: %d pathogen recalls in %d-day window (%d records scanned, "
            "skipped: archived=%d spanish=%d no_date=%d old=%d "
            "no_pathogen=%d no_url=%d bad_url=%d). Date-field usage: %s",
            len(out), since_days, n_total,
            n_skipped_archived, n_skipped_spanish, n_skipped_no_date,
            n_skipped_old, n_skipped_no_pathogen, n_skipped_no_url,
            n_skipped_bad_url,
            date_field_hits or "n/a",
        )

        # A bad_url drop is never routine: the API is supposed to emit
        # canonical recall links and _canonicalise_fsis_url() already
        # absorbs every relative / http / no-www variant. If anything still
        # falls through, the schema moved — say so at WARNING with the
        # evidence attached.
        if bad_url_samples:
            log.warning(
                "USDA FSIS: %d record(s) rejected by the URL gate AFTER "
                "canonicalisation. This should be 0 — the API schema may have "
                "changed. Offending values: %s",
                n_skipped_bad_url, bad_url_samples,
            )

        # Zero in-window survivors means every fresh record was filtered
        # before the pathogen scan ever ran. That is exactly how the
        # 2026-05..07 FSIS blackout looked, so make it impossible to miss.
        n_reached_pathogen_scan = (
            n_total - n_skipped_archived - n_skipped_spanish
            - n_skipped_no_date - n_skipped_old
            - n_skipped_no_url - n_skipped_bad_url
        )
        if n_reached_pathogen_scan <= 0 and n_total > 0:
            log.error(
                "USDA FSIS: 0 of %d records reached the pathogen scan — every "
                "in-window record was dropped by an upstream filter "
                "(no_url=%d bad_url=%d old=%d archived=%d). This is a filter "
                "fault, not a quiet week.",
                n_total, n_skipped_no_url, n_skipped_bad_url,
                n_skipped_old, n_skipped_archived,
            )
        return out
