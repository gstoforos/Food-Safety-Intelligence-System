"""pipeline/_url_filters.py — single source of truth for URL classification.

WHY THIS MODULE EXISTS (audit 2026-05-06)
==========================================
Before this module, FSIS had FOUR places that maintained their own
"is this URL a generic listing/search/category page" filter:

  1. pipeline/merge_master.py       _GENERIC_URL_PATTERNS    (regex list)
  2. pipeline/gap_finder_tavily.py  _is_generic_url + _KNOWN_REGULATOR_LANDINGS
  3. pipeline/gap_finder_exa.py     imports from gap_finder_tavily (OK)
  4. pipeline/gap_finder_gemini.py  bad_substrings tuple in _post_filter_recalls

The four lists drifted apart. The 2026-05-05 audit found that
merge_master had been updated with 11 new patterns (search/site,
recherche?, page/N, etc.) but gap_finder_tavily and gap_finder_gemini
had not. Net effect: search-result-shell URLs (CFIA /search/site,
FSANZ /circulars/notification-circular-) sailed past the gap-finders'
own filters, got written to Pending, consumed reviewer slots, then
got rejected at merge_master's validate_pending_row gate days later.

This module hoists every filter to a single canonical implementation.
All callers import from here. Drift impossible.

PUBLIC API
==========
    is_generic_url(url) -> bool
        True if URL is a known generic listing / search / disease /
        transparency / pagination / circular page. False otherwise.

    KNOWN_REGULATOR_LANDINGS    frozenset[str]
        Whitelisted-domain bare landing pages that pass HOST_TO_SOURCE
        but are never specific recalls. Compared canonical (no trailing
        slash, no query, no fragment, lowercased).

    GENERIC_URL_PATTERNS        tuple[str]
        The regex patterns merge_master.validate_pending_row uses. Kept
        public so merge_master can iterate them; everyone else should
        use is_generic_url() instead.

CONTRACT FOR FUTURE EDITS
=========================
- Every new pattern added here is automatically picked up by every
  caller. No need to update gap_finder_tavily.py, gap_finder_gemini.py,
  daily_recall_search.py, scrapers/north_america/cfia.py, or
  scrapers/north_america/fda_press.py separately.
- Adding regulator-specific landing pages: append to
  KNOWN_REGULATOR_LANDINGS as a tuple of canonical URLs (lowercased,
  no trailing slash).
- Adding pattern matches: append to GENERIC_URL_PATTERNS as a regex
  string (will be compiled lazily). Use \\b for word boundaries.
- All matching is case-insensitive (the URL is lowered before matching).
"""
from __future__ import annotations
import re
from typing import Optional
from urllib.parse import urlsplit


# ─────────────────────────────────────────────────────────────────────────
# Whitelisted regulator landing pages — these return HTTP 200, live on a
# whitelisted host, but are NEVER specific recall pages. Compared against
# the URL stripped of query string, fragment, and trailing slash, lowered.
# ─────────────────────────────────────────────────────────────────────────
KNOWN_REGULATOR_LANDINGS = frozenset({
    # FSANZ (Australia)
    "https://www.foodstandards.gov.au/food-recalls",
    "https://www.foodstandards.gov.au/food-recalls/recalls",
    "https://www.foodstandards.gov.au/food-recalls/recall-alert",  # Tavily Row 10 leak (audit 2026-05-06)
    "https://www.foodstandards.gov.au/consumer/safety/recalls",
    # FDA (USA)
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
    "https://www.fda.gov/safety/recalls",
    "https://www.fda.gov/food/recalls-outbreaks-emergencies",
    # USDA FSIS
    "https://www.fsis.usda.gov/recalls",
    "https://www.fsis.usda.gov/recalls-alerts",
    # CFIA (Canada) + search shells
    "https://recalls-rappels.canada.ca/en",
    "https://recalls-rappels.canada.ca/fr",
    "https://recalls-rappels.canada.ca/en/search/site",
    "https://recalls-rappels.canada.ca/fr/search/site",
    "https://recalls-rappels.canada.ca/en/recherche",
    "https://recalls-rappels.canada.ca/fr/recherche",
    "https://inspection.canada.ca/food-recall-warnings-and-allergy-alerts",
    # FSA (UK)
    "https://www.food.gov.uk/news-alerts",
    "https://www.food.gov.uk/about-us/recalls-and-alerts",
    # FSAI (Ireland)
    "https://www.fsai.ie/news-and-alerts/food-alerts",
    "https://www.fsai.ie/news-alerts/food",                  # Tavily Row 7 leak (audit 2026-05-06)
    "https://www.fsai.ie/news-and-alerts",
    "https://www.fsai.ie/news-and-alerts/latest-news",
    "https://www.fsai.ie/consumer-advice",                   # advice section root
    # RappelConso (FR)
    "https://rappel.conso.gouv.fr",
    "https://rappel.conso.gouv.fr/recherche",
    # AESAN (Spain), AGES (Austria), AFSCA (Belgium), NVWA (Netherlands)
    "https://www.aesan.gob.es/aecosan/web/seguridad_alimentaria/subseccion/alertas_alimentarias.htm",
    "https://www.ages.at/konsument/lebensmittelwarnungen",
    "https://www.afsca.be/professionnels/publications/communications/rappels",
    "https://www.nvwa.nl/onderwerpen/voedselveiligheid/veiligheidswaarschuwingen",
    # MPI New Zealand
    "https://www.mpi.govt.nz/food-safety-home/food-recalls",
    # CFS Hong Kong — multiple listing variants (Tavily/Exa Row 4 leak, audit 2026-05-08)
    "https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/whatsnew_fa.html",
    "https://www.cfs.gov.hk/english/rc/subject/fi_list.html",
    "https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa",
    "https://www.cfs.gov.hk/english/whatsnew/whatsnew_act",
    "https://www.cfs.gov.hk/english/programme/programme_rafs/programme_rafs.html",
    # SFA Singapore — risk-at-a-glance is educational, not recall (audit 2026-05-08)
    "https://www.sfa.gov.sg/food-safety-tips/food-risk-concerns/risk-at-a-glance",
    "https://www.sfa.gov.sg/food-safety-tips",
    "https://www.sfa.gov.sg/food-information/recall",
    # RASFF Window (EU) — SPA shells
    "https://webgate.ec.europa.eu/rasff-window",
    "https://webgate.ec.europa.eu/rasff-window/screen",
    "https://webgate.ec.europa.eu/rasff-window/screen/search",
    "https://webgate.ec.europa.eu/rasff-window/screen/consumers",
    "https://webgate.ec.europa.eu/rasff-window/screen/list",
})


# ─────────────────────────────────────────────────────────────────────────
# Generic-listing patterns — regex strings. Compiled lazily on first use.
# Mirrors merge_master._GENERIC_URL_PATTERNS exactly. Keep in sync.
# ─────────────────────────────────────────────────────────────────────────
GENERIC_URL_PATTERNS = (
    r"vertexaisearch\.cloud\.google",        # Gemini grounding redirect
    r"fsai\.ie/news-alerts/food\?page=",     # FSAI paginated listing
    r"rasff-window/screen/list\?",           # RASFF list page
    r"quebec\.ca/.*/listeriosis",            # Quebec disease info
    r"quebec\.ca/.*/animal-disease",         # Quebec animal disease info
    r"quebec\.ca/.*/food-recalls$",          # Quebec generic recalls page
    r"regulatory-transparency-and-openness", # CFIA transparency pages
    r"food-safety-investigations/$",         # CFIA investigation index
    r"/categorie/[\d/]+/?$",                 # RappelConso category index
    r"/rubrik/[^/]+/?$",                     # produktwarnung.eu rubrik
    r"/news-and-alerts/food-alerts/?$",      # FSAI alerts root
    r"/safety/recalls-market-withdrawals-safety-alerts/?$",  # FDA root
    r"/animal-veterinary/news-events/outbreaks-and-advisories/?$",  # FDA pet root
    # CFIA recalls landing page (any locale path or bare host)
    r"recalls-rappels\.canada\.ca/(?:fr|en)/?$",
    r"recalls-rappels\.canada\.ca/?$",
    # FDA share-link wrapper duplicates
    r"/safety/recalls-market-withdrawals-safety-alerts/voluntary-recall\?permalink=",
    # 2026-05-05 audit additions
    r"/search/site",                                # CFIA "advanced search"
    r"/search\?",                                   # generic search query
    r"/recherche\?",                                # French generic search
    r"/recherche/",                                 # French search path
    r"/buscador",                                   # Spanish search
    r"/suche\?",                                    # German search
    r"/page/\d+/?(?:$|\?)",                         # /page/50/, /page/2/?...
    r"\bpage=\d+",                                  # ?page=50
    r"/notification-circulars?/?$",
    r"/notification-circulars?/index",
    r"/circulars?/notification-circular-",          # FSANZ circular ID
    r"/bulletins?/?$",
    r"/news-circulars?/?$",
    # 2026-05-08 audit additions (Tavily/Exa garbage suppression)
    r"sfa\.gov\.sg/food-safety-tips/food-risk-concerns/",     # SFA educational
    r"sfa\.gov\.sg/food-information/?$",                       # SFA listing root
    r"cfs\.gov\.hk/.*/whatsnew_fa/?$",                         # CFS HK what's new index
    r"cfs\.gov\.hk/.*/fi_list\.html",                          # CFS HK food incidents list
    r"cfs\.gov\.hk/.*/programme_rafs",                         # CFS HK RAFS programme root
    r"/risk-at-a-glance",                                      # any-regulator risk-explainer pages
)

# Lazy-compiled cache
_compiled_patterns: Optional[tuple] = None


def _compiled():
    global _compiled_patterns
    if _compiled_patterns is None:
        _compiled_patterns = tuple(
            re.compile(p, re.IGNORECASE) for p in GENERIC_URL_PATTERNS
        )
    return _compiled_patterns


# ─────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────
def is_generic_url(url: str) -> bool:
    """True if URL is a known generic listing / search / category /
    pagination / disease-info / transparency page — i.e. NOT a specific
    recall. Single source of truth used by every gap-finder and the
    merge_master validation gate.

    Matching:
      1. Whitelisted-landing exact match (canonical-form lowercase).
      2. RappelConso fiche-ID sanity (year-as-fid / non-numeric / sentinel).
      3. Regex match against GENERIC_URL_PATTERNS.
      4. Bare-domain (one-segment) heuristic.
    """
    if not url:
        return True
    u = url.lower()

    # 0. Sanity: must have an http(s) scheme. Anything else is garbage.
    if not u.startswith(("http://", "https://")):
        return True

    # 1. Canonical-form whitelist match
    try:
        sp = urlsplit(url)
        canonical = (
            f"{sp.scheme.lower()}://{sp.netloc.lower()}{sp.path.rstrip('/')}"
        )
    except Exception:
        canonical = u.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    if canonical in KNOWN_REGULATOR_LANDINGS:
        return True

    # 2. RappelConso fiche-ID sanity
    if "rappel.conso.gouv.fr" in u or "rappelconso.gouv.fr" in u:
        m_fid = re.search(r"/fiche-rappel/([^/]+)", u)
        if not m_fid:
            return True
        fid = m_fid.group(1).strip()
        if not fid.isdigit():
            return True
        n = int(fid)
        if n < 1000 or (2000 <= n <= 2100):
            return True

    # 3. Pattern match — compiled cache
    for pat in _compiled():
        if pat.search(u):
            return True

    # 4. Bare-domain fallback
    try:
        path = (urlsplit(url).path or "").strip("/")
        if not path:
            return True
    except Exception:
        pass

    return False


__all__ = [
    "KNOWN_REGULATOR_LANDINGS",
    "GENERIC_URL_PATTERNS",
    "is_generic_url",
]
