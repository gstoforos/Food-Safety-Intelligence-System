"""
Gap-finder promotion guards (audit 2026-06-25, hardened 2026-06-25b).

The AI gap-finder cascade (Gemini -> Claude -> OpenAI) asks an LLM for recent
recalls. LLMs ignore the "last N days" instruction, sometimes return news URLs,
and occasionally emit raw page text (lot/date strings, transcription artifacts)
as the Product. These deterministic guards exist to catch that BEFORE a row
reaches Pending.

SCOPE — CAREFUL: these run ONLY on gap-finder rows. They are gated behind the
`gap_finder=True` flag threaded from the three finders through
append_to_pending() -> validate_pending_row(). Scraper rows and manual injects
(gap_finder=False, the default) are never touched by anything here.

SELF-CONTAINED BY DESIGN (audit 2026-06-25b): an earlier draft imported
`pipeline.regulatory_domains`, a module that does not exist in the repo. That
import would raise ImportError, which validate_pending_row() swallows
(check_gap_finder_row = None), SILENTLY DISABLING every guard below — the exact
news/stale/garbage rows would leak again with no error and no log. To make that
failure mode impossible, this module now depends only on the Python standard
library. The regulator allowlist and news blocklist live here, in one place,
and are the single source of truth for the gap-finder authority gate.

Four guards, each tuned to avoid false rejects of legitimate recalls:

  1. AUTHORITY — ALLOWLIST-based. A gap-finder URL must resolve to a known
     regulator host (REGULATOR_HOSTS) OR a government-shaped host (.gov/.gouv/
     .gob/europa.eu/...) which is KEPT and logged so the host can be promoted
     into REGULATOR_HOSTS. Everything else is rejected. This is deliberately an
     allowlist, not a news blocklist: the rows that leaked on 2026-06-25
     (aol.com, freshplaza.com, livenowfox.com, cbs17.com) were NOT in any news
     blocklist, so only a positive regulator match reliably stops them. A small
     explicit NEWS_HOSTS set is also checked first so the reject reason is
     precise ("news_url") for the common offenders.

  2. RECENCY — reject only a row that HAS a parseable Date that is older than
     MAX_AGE_DAYS or in the future. A row with no/!unparseable date PASSES
     unchanged (same as today's behaviour) — we never newly reject for a missing
     date.

  3. GENERIC URL — reject a regulator HOMEPAGE / landing / category-index URL
     that carries no specific alert id or slug (e.g. aesan.gob.es .../home/
     aecosan_inicio.htm, .../SCIRI.htm, a bare .../recalls or .../food-alerts).
     A gov host alone is not enough; the URL must point at a specific notice.

  4. PRODUCT / COMPANY — reject a Product that is really a raw lot/date code or
     carries a transcription artifact (RappelConso "10-12-13-17/06¤to"), and
     de-duplicate a doubled Company token, including the 2-token case-variant
     repeat ("STOEFFLER Stoeffler" -> "Stoeffler", "MOLET EARL MOLET" ->
     "MOLET EARL").
"""

from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

# ── Reject gap-finder recalls whose publication date is older than this many ──
# days. The Gemini prompt asks for "last 5 days"; 30 is a generous backstop that
# kills the months-old leaks (e.g. a 2026-02-03 recall surfacing in a June
# alert) and mis-dated rows (Ambriola stamped 2026-01-12 when the real recall is
# June) while still tolerating a legitimately late-surfaced recall. Tunable.
MAX_AGE_DAYS = 30

# ── Known regulator hosts (ALLOWLIST). A gap-finder URL must match one of these
# (or the gov-shape fallback below). This is the authoritative list for the
# gap-finder authority gate. Add new regulators here as they appear.
# Matching is host-suffix aware: "rappel.conso.gouv.fr" matches, and any
# subdomain of a listed host matches too.
REGULATOR_HOSTS = frozenset({
    # EU / RASFF
    "webgate.ec.europa.eu",
    # France
    "rappel.conso.gouv.fr",
    # USA
    "fda.gov", "cdc.gov", "fsis.usda.gov", "usda.gov",
    # Canada
    "inspection.canada.ca", "recalls-rappels.canada.ca", "healthycanadians.gc.ca",
    # Ireland
    "fsai.ie",
    # UK
    "food.gov.uk", "data.food.gov.uk",
    # Spain
    "aesan.gob.es",
    # Germany
    "lebensmittelwarnung.de", "bvl.bund.de",
    # Belgium
    "favv-afsca.be", "favv.be", "afsca.be",
    # Italy
    "salute.gov.it",
    # Greece
    "efet.gr",
    # Austria
    "ages.at", "lebensmittelaufsicht.at",
    # Switzerland
    "blv.admin.ch",
    # Netherlands
    "nvwa.nl",
    # Portugal
    "asae.gov.pt",
    # Poland
    "gov.pl", "gis.gov.pl",
    # Nordics
    "livsmedelsverket.se", "mattilsynet.no", "foedevarestyrelsen.dk", "ruokavirasto.fi",
    # Australia / NZ
    "foodstandards.gov.au", "mpi.govt.nz",
    # Asia
    "cfs.gov.hk", "sfa.gov.sg", "fda.gov.ph", "mfds.go.kr", "fda.gov.tw",
    # LATAM
    "anvisa.gov.br", "gob.mx", "argentina.gob.ar",
})

# ── Explicit news / aggregator hosts. Checked FIRST so the reject reason is the
# precise "news_url" for the common offenders. NOT exhaustive — the allowlist
# above is what actually guarantees rejection of unknown news hosts. These are
# the hosts that have actually leaked or are likely to.
NEWS_HOSTS = frozenset({
    "aol.com", "freshplaza.com", "livenowfox.com", "cbs17.com",
    "foxnews.com", "fox13seattle.com", "fox5dc.com",
    "agriland.ie", "efoodalert.com", "65ymas.com",
    "foodsafetynews.com", "foodpoisonjournal.com", "foodpoisoningbulletin.com",
    "outbreaknewstoday.com", "food-safety.com", "barfblog.com",
    "foodbusinessnews.net", "foodnavigator.com", "foodnavigator-usa.com",
    "just-food.com", "foodmanufacture.co.uk", "foodprocessing.com",
    "fooddive.com", "reuters.com", "apnews.com", "bbc.com", "bbc.co.uk",
    "bloomberg.com", "theguardian.com", "nytimes.com", "washingtonpost.com",
    "medicalxpress.com", "sciencedaily.com", "yahoo.com", "msn.com",
    "news.google.com", "newsweek.com", "people.com", "eatingwell.com",
    "marthastewart.com", "consumerreports.org", "thecooldown.com",
    "news247.gr", "kathimerini.gr", "ilfattoalimentare.it", "sedaily.com",
    "beaconbio.com", "produktwarnung.eu", "qz.com", "ibtimes.sg",
    "legalreader.com", "thehealthy.com", "southernliving.com",
})

# ── Government-domain shapes we trust even when a specific host is not (yet) in
# REGULATOR_HOSTS. Prevents false-rejecting a real regulator that simply hasn't
# been added to the allowlist. Matches are KEPT and logged.
_GOV_HINTS = (
    ".gov", ".gov.", ".gouv.", ".gob.", ".go.", ".govt.", ".gc.ca",
    "europa.eu", "admin.ch",
)

# ── Generic / landing / category-index URL shapes that carry no specific alert.
# A regulator HOMEPAGE or section index is not a recall. These patterns match
# the path of a gov host and cause a reject.
_GENERIC_URL_RES = (
    re.compile(r"/aecosan_inicio\.htm/?$", re.I),
    re.compile(r"/web/home/?$", re.I),
    re.compile(r"/sciri\.htm/?$", re.I),
    re.compile(r"/subseccion/[^/]*\.htm/?$", re.I),   # AESAN section index
    re.compile(r"/recalls/?$", re.I),
    re.compile(r"/food-alerts/?$", re.I),
    re.compile(r"/news-and-alerts/?$", re.I),
    re.compile(r"/index\.html?/?$", re.I),
    re.compile(r"^https?://[^/]+/?$", re.I),          # bare host, no path
)

# "10-12-13-17/06" style lot/date strings, and transcription artifacts.
_LOTDATE_RE = re.compile(r"\d{1,2}[-/]\d{1,2}(?:[-/]\d{1,2})+")
_ARTIFACT_RE = re.compile(r"[\u00A4\u00D7\uFFFD]")  # ¤  ×  <replacement char>


def _host(url: str) -> str:
    """Lower-cased host with a leading 'www.' stripped. Stdlib only."""
    try:
        h = urlparse(str(url or "")).netloc.lower()
    except Exception:
        return ""
    if h.startswith("www."):
        h = h[4:]
    return h


def _host_matches(host: str, hosts: frozenset) -> bool:
    """True if host equals, or is a subdomain of, any host in `hosts`."""
    if not host:
        return False
    if host in hosts:
        return True
    for h in hosts:
        if host.endswith("." + h):
            return True
    return False


def is_news_url(url: str) -> bool:
    return _host_matches(_host(url), NEWS_HOSTS)


def is_regulator_url(url: str) -> bool:
    return _host_matches(_host(url), REGULATOR_HOSTS)


def _looks_gov(host: str) -> bool:
    h = (host or "").lower()
    return any(hint in h for hint in _GOV_HINTS)


def is_generic_url(url: str) -> bool:
    """True if the URL is a regulator homepage / landing / category index with
    no specific alert id or slug."""
    u = str(url or "").strip()
    if not u:
        return False
    return any(rx.search(u) for rx in _GENERIC_URL_RES)


def _parse_iso(date_str):
    s = str(date_str or "")[:10]
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        return None


def authority_ok(url: str):
    """Return (ok, reason, keep_logged).

    ok=True  -> URL is acceptable for a gap-finder row.
    keep_logged=True -> kept on the gov-shape fallback; caller should log so the
                        host can be added to REGULATOR_HOSTS.
    """
    if is_news_url(url):
        return False, "news_url (not authority)", False
    if is_regulator_url(url):
        return True, "", False
    if _looks_gov(_host(url)):
        return True, "", True
    return False, "non_regulator_url", False


def product_is_garbage(product: str) -> bool:
    p = str(product or "").strip()
    if not p:
        return False  # empty Product is handled by other gates, not here
    if _ARTIFACT_RE.search(p):
        return True
    compact = p.replace(" ", "")
    if compact and _LOTDATE_RE.search(p):
        digit_sep = sum(c.isdigit() or c in "-/" for c in compact)
        if digit_sep / len(compact) > 0.5:
            return True
    return False


def dedupe_company(company: str) -> str:
    """Collapse a doubled Company token, case-insensitively.

      'MOLET EARL MOLET'   -> 'MOLET EARL'   (3+ tokens, first == last)
      'STOEFFLER Stoeffler'-> 'Stoeffler'    (2 tokens, same word, keep the
                                              better-cased one)
      'Carrefour France Carrefour' -> 'Carrefour France'

    Leaves everything that is not a literal repeat untouched.
    """
    toks = str(company or "").split()
    if len(toks) < 2:
        return company

    # 2-token case-variant repeat: "STOEFFLER Stoeffler" -> keep the token that
    # is not all-caps (the human-readable casing), else the first.
    if len(toks) == 2 and toks[0].lower() == toks[1].lower():
        a, b = toks
        if a.isupper() and not b.isupper():
            return b
        if b.isupper() and not a.isupper():
            return a
        return a

    # 3+ tokens, first token repeated at the end: drop the trailing repeat.
    if len(toks) >= 3 and toks[0].lower() == toks[-1].lower():
        return " ".join(toks[:-1])

    return company


def check_gap_finder_row(row: dict, *, max_age_days: int = MAX_AGE_DAYS,
                         today=None):
    """Apply authority + recency + generic-URL + product guards to one
    gap-finder row.

    Mutates `row` in place (Company de-dupe only). Returns (ok, reason, log_note).
    `ok=False` means reject the row before it enters Pending.
    """
    today = today or datetime.now(timezone.utc).date()
    url = str(row.get("URL", "") or "").strip()

    # 1. AUTHORITY (allowlist; news rejected first for a precise reason)
    ok, reason, keep_logged = authority_ok(url)
    if not ok:
        return False, reason, ""
    log_note = (f"regulator domain not in allowlist (kept): {_host(url)}"
                if keep_logged else "")

    # 2. RECENCY — only act on a parseable date; missing/!unparseable passes.
    d = _parse_iso(row.get("Date"))
    if d is not None:
        if d > today + timedelta(days=1):
            return False, f"future_date: {d}", log_note
        if d < today - timedelta(days=max_age_days):
            return False, f"too_old (>{max_age_days}d): {d}", log_note

    # 3. GENERIC URL — regulator homepage / landing / category index, no alert.
    if is_generic_url(url):
        return False, "generic_url (homepage/landing, no specific alert)", log_note

    # 4. PRODUCT / COMPANY hygiene
    if product_is_garbage(row.get("Product")):
        return False, "garbage_product (lot/date code or artifact)", log_note
    row["Company"] = dedupe_company(row.get("Company", ""))

    return True, "", log_note
