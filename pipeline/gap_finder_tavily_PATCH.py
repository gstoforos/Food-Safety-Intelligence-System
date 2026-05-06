# =============================================================================
# PATCH FOR pipeline/gap_finder_tavily.py
# =============================================================================
# Three surgical replacements. Apply with str_replace, or paste into the file
# at the indicated line numbers in the current main branch.
#
# Why: as of 2026-05-05 merge_master._GENERIC_URL_PATTERNS gained 11 new
# regex patterns (the 2026-05-05 audit additions block). gap_finder_tavily's
# _is_generic_url() — which is supposed to mirror that list per its
# docstring — was never updated. CFIA /search/site pages, paginated listings,
# and notification-circular indexes therefore continue to slip past the
# gap-finder's own filter and only get caught by merge_master's
# validate_pending_row, AFTER they've already been written through
# pending_gap → pending_gap_v1 (wasting one Gemini reviewer slot per row).
#
# After this patch, identical patterns are applied at both layers and the
# CFIA /search/site row in current Pending will be the last of its kind.
# =============================================================================


# ── REPLACEMENT 1 ────────────────────────────────────────────────────────────
# Location: pipeline/gap_finder_tavily.py, replace the
# _KNOWN_REGULATOR_LANDINGS block (currently lines 472–516, ending at the
# closing `})`).
# Adds CFIA + RappelConso search-shell variants, plus the FSAI alerts root
# explicitly (was already in merge_master but not here).

_KNOWN_REGULATOR_LANDINGS = frozenset({
    # FSANZ (Australia) — Apr 2026 leak
    "https://www.foodstandards.gov.au/food-recalls",
    "https://www.foodstandards.gov.au/food-recalls/recalls",
    "https://www.foodstandards.gov.au/consumer/safety/recalls",
    # FDA (USA) — recalls landing
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
    "https://www.fda.gov/safety/recalls",
    "https://www.fda.gov/food/recalls-outbreaks-emergencies",
    # USDA FSIS — recalls landing
    "https://www.fsis.usda.gov/recalls",
    "https://www.fsis.usda.gov/recalls-alerts",
    # CFIA (Canada) — recall hub + search shells (audit 2026-05-05).
    # The bare /en and /fr landings were already covered; the /search/site
    # advanced-search shell and its locale variants were not. Specific
    # alert pages live at /<lang>/alert-recall/<slug> and pass through.
    "https://recalls-rappels.canada.ca/en",
    "https://recalls-rappels.canada.ca/fr",
    "https://recalls-rappels.canada.ca/en/search/site",
    "https://recalls-rappels.canada.ca/fr/search/site",
    "https://recalls-rappels.canada.ca/en/recherche",
    "https://recalls-rappels.canada.ca/fr/recherche",
    "https://inspection.canada.ca/food-recall-warnings-and-allergy-alerts",
    # FSA (UK) — alerts hub
    "https://www.food.gov.uk/news-alerts",
    "https://www.food.gov.uk/about-us/recalls-and-alerts",
    # FSAI (Ireland) — alerts root (audit 2026-05-05)
    "https://www.fsai.ie/news-and-alerts/food-alerts",
    # RappelConso (FR) — fiche check below catches /fiche-rappel/* but the
    # bare landing page itself needs to be rejected explicitly.
    "https://rappel.conso.gouv.fr",
    "https://rappel.conso.gouv.fr/recherche",
    # AESAN (Spain), AGES (Austria), AFSCA (Belgium), NVWA (Netherlands)
    "https://www.aesan.gob.es/aecosan/web/seguridad_alimentaria/subseccion/alertas_alimentarias.htm",
    "https://www.ages.at/konsument/lebensmittelwarnungen",
    "https://www.afsca.be/professionnels/publications/communications/rappels",
    "https://www.nvwa.nl/onderwerpen/voedselveiligheid/veiligheidswaarschuwingen",
    # MPI New Zealand
    "https://www.mpi.govt.nz/food-safety-home/food-recalls",
    # ── RASFF Window (EU) — SPA shells (audit 2026-04-29) ──
    # Specific notification URLs at /screen/notification/<id> are real
    # pages and must pass; only the search/consumer/landing shells are
    # rejected here. The RASFF row schema (validate_pending_row + run_all
    # _missing_required) further requires the URL to be a /notification/
    # detail page, not anything under /screen/search or /screen/consumers.
    "https://webgate.ec.europa.eu/rasff-window",
    "https://webgate.ec.europa.eu/rasff-window/screen",
    "https://webgate.ec.europa.eu/rasff-window/screen/search",
    "https://webgate.ec.europa.eu/rasff-window/screen/consumers",
    "https://webgate.ec.europa.eu/rasff-window/screen/list",
})


# ── REPLACEMENT 2 ────────────────────────────────────────────────────────────
# Location: pipeline/gap_finder_tavily.py, replace the body of _is_generic_url()
# starting at the line `def _is_generic_url(url: str) -> bool:` (currently
# line 516) through to the matching `return False` of that function.
#
# Changes:
#   • Re-uses the _KNOWN_REGULATOR_LANDINGS short-circuit (unchanged).
#   • bad_substrings expanded to mirror merge_master._GENERIC_URL_PATTERNS:
#       /search/site, /search?, /recherche?, /recherche/, /buscador, /suche?
#       /page/N, page=N
#       /notification-circular(s)/, /circulars/notification-circular-
#       /bulletins/, /news-circulars/
#   • RappelConso fiche-ID sanity unchanged.
#   • One-segment-path heuristic unchanged (the merge_master regex
#     `recalls-rappels\.canada\.ca/(?:fr|en)/?$` is now also redundantly
#     covered by the explicit landings above — that's fine, costs nothing).

def _is_generic_url(url: str) -> bool:
    """True if URL is a generic listing/category/disease/transparency page —
    not a specific recall fiche. Mirrors the patterns in
    merge_master.validate_pending_row() — KEEP IN SYNC.

    Drift between this function and merge_master._GENERIC_URL_PATTERNS lets
    listing pages reach Pending (where they consume one Gemini reviewer
    slot before merge_master finally rejects them on validate_pending_row).
    Caught the CFIA /search/site leak on 2026-05-05.
    """
    u = url.lower()

    # ── Hard-coded regulator landing/listing pages ──────────────────────
    # These are real pages that return HTTP 200 (so the URL gate accepts
    # them) and whose host is a whitelisted regulator domain (so the
    # `_lookup_source` check accepts them) — but they are NEVER specific
    # recall pages. Compare against the URL stripped of trailing slash,
    # query string, and fragment.
    try:
        from urllib.parse import urlsplit as _us
        sp = _us(url)
        canonical = f"{sp.scheme.lower()}://{sp.netloc.lower()}{sp.path.rstrip('/')}"
    except Exception:
        canonical = u.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    if canonical in _KNOWN_REGULATOR_LANDINGS:
        return True

    # ── Generic-listing substring filter ──────────────────────────────
    # Mirrors merge_master._GENERIC_URL_PATTERNS (substring form — these
    # patterns don't need anchoring). Audit 2026-05-05: original list was
    # missing /search/site, /search?, /recherche, /buscador, /suche?,
    # /page/, page=, and the notification-circular family.
    bad_substrings = (
        # Original (pre-2026-05-05)
        "/list?", "/a-z/", "animal-disease",
        "regulatory-transparency", "/categorie/", "/rubrik/", "/tag/",
        "vertexaisearch",
        # Search-result shells (CFIA, FSAI, RappelConso, AESAN, BVL)
        "/search/site",
        "/search?",
        "/recherche?",
        "/recherche/",
        "/buscador",
        "/suche?",
        # Pagination
        "/page/",
        "page=",
        # Notification circulars / regulatory bulletins
        "/notification-circular",   # covers singular + plural + index
        "/circulars/notification-circular-",
        "/bulletins/",
        "/news-circulars/",
    )
    if any(p in u for p in bad_substrings):
        return True

    # ── RappelConso fiche-ID sanity ─────────────────────────────────────
    # Real fiche IDs are 5-digit integers (currently in the 22000s).
    # Reject:
    #   - Year-as-fid: /fiche-rappel/2026/Interne
    #   - Slug-as-fid: /fiche-rappel/2026-04-0305/Interne
    #   - Sentinel/year values: 2000-2100
    #   - Implausibly small: < 1000
    if "rappel.conso.gouv.fr" in u or "rappelconso.gouv.fr" in u:
        import re as _re
        m_fid = _re.search(r"/fiche-rappel/([^/]+)", u)
        if not m_fid:
            return True  # No fid → not a specific fiche page
        fid = m_fid.group(1).strip()
        if not fid.isdigit():
            return True  # Slug-as-fid hallucination
        n = int(fid)
        if n < 1000 or (2000 <= n <= 2100):
            return True  # Sentinel/year value

    # Bare domain or one-segment paths (homepages, root listings)
    try:
        from urllib.parse import urlparse as _up
        path = (_up(url).path or "").strip("/")
        if not path:
            return True  # bare domain
    except Exception:
        pass
    return False
