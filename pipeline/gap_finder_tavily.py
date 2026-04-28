"""
Tavily gap-finder — standalone. No other AI (no Gemini, no Claude, no OpenAI).

Uses Tavily's AI-optimized search API to find pathogen recalls the other
gap-finders missed, then extracts structured Recall fields deterministically
from Tavily's own `title`, `content`, `url`, `published_date` fields —
pure Python regex + keyword matching, zero LLM calls.

Tavily free tier: 1,000 searches/month. ~9 queries/run × 1 run/day = ~270/mo
= ~27% of free quota.

Architecture:
  1. Run 9 targeted Tavily searches (generic pathogen/foreign-body queries
     + 6 regulator-specific site: queries)
  2. Filter to whitelisted regulator domains (REGULATOR_DOMAINS)
  3. Extract fields deterministically from title/content/url/published_date:
        - Pathogen  : keyword scan against known pathogen vocabulary
        - Country   : map from URL host → country/source (HOST_TO_SOURCE)
        - Date      : from Tavily's published_date (topic=news)
        - Company   : heuristic — text before "recalls"/"issues"/"withdraws"
        - Product   : heuristic — text after the recall verb, capped at 200 chars
        - Outbreak  : keyword match ("outbreak", "illness", "hospitalized", etc.)
        - Tier      : assign_tier(pathogen, outbreak)
  4. Write to Pending — rows go through the normal Claude URL gate later.

Rows with no detectable pathogen are dropped. Rows from non-whitelisted hosts
are dropped. Everything else appends to Pending with `Source="Tavily-gap"`.

Cost: $0 (Tavily free tier + no LLM calls).
Runs once per day at 22:00 Athens (per FsisScheduler.txt).

Invoke:
    python -m pipeline.gap_finder_tavily
    python -m pipeline.gap_finder_tavily  # GAP_SINCE_DAYS=14 env override
"""
from __future__ import annotations
import os
import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._models import (  # noqa: E402
    Recall, normalize_pathogen, normalize_country, infer_region, assign_tier,
)
from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    append_to_pending, sort_rows, save_xlsx_with_pending,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("gap-finder-tavily")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "5"))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "").strip()

TAVILY_ENDPOINT = "https://api.tavily.com/search"

# ---------------------------------------------------------------------------
# Regulator domain → (Source label, Country) map.
# Only URLs from these hosts are accepted. The map also provides Country/Source
# for the extracted Recall row without needing an AI call.
# ---------------------------------------------------------------------------
HOST_TO_SOURCE: Dict[str, Tuple[str, str]] = {
    # North America
    "fda.gov":                      ("FDA",        "United States"),
    "accessdata.fda.gov":           ("FDA",        "United States"),
    "fsis.usda.gov":                ("USDA FSIS",  "United States"),
    "cdc.gov":                      ("CDC",        "United States"),
    "inspection.canada.ca":         ("CFIA",       "Canada"),
    "recalls-rappels.canada.ca":    ("CFIA",       "Canada"),
    "quebec.ca":                    ("MAPAQ",      "Canada"),
    # EU & UK
    "food.gov.uk":                  ("FSA",        "United Kingdom"),
    "foodstandards.gov.scot":       ("FSS",        "United Kingdom"),
    "fsai.ie":                      ("FSAI",       "Ireland"),
    "rappelconso.gouv.fr":          ("RappelConso","France"),
    "rappel.conso.gouv.fr":         ("RappelConso","France"),
    "agriculture.gouv.fr":          ("DGAL",       "France"),
    "aesan.gob.es":                 ("AESAN",      "Spain"),
    "lebensmittelwarnung.de":       ("BVL",        "Germany"),
    "bvl.bund.de":                  ("BVL",        "Germany"),
    "bfr.bund.de":                  ("BfR",        "Germany"),
    "ages.at":                      ("AGES",       "Austria"),
    "salute.gov.it":                ("Min. Salute","Italy"),
    "nvwa.nl":                      ("NVWA",       "Netherlands"),
    "favv-afsca.be":                ("FAVV",       "Belgium"),
    "afsca.be":                     ("FAVV",       "Belgium"),
    "foedevarestyrelsen.dk":        ("Fødevarestyrelsen","Denmark"),
    "livsmedelsverket.se":          ("Livsmedelsverket","Sweden"),
    "mattilsynet.no":                ("Mattilsynet","Norway"),
    "ruokavirasto.fi":              ("Ruokavirasto","Finland"),
    "gis.gov.pl":                   ("GIS",        "Poland"),
    "nebih.gov.hu":                 ("NEBIH",      "Hungary"),
    "ansvsa.ro":                    ("ANSVSA",     "Romania"),
    "bfsa.bg":                      ("BFSA",       "Bulgaria"),
    "szpi.gov.cz":                  ("SZPI",       "Czech Republic"),
    "svps.sk":                      ("ŠVPS",       "Slovakia"),
    "mast.is":                      ("MAST",       "Iceland"),
    "webgate.ec.europa.eu":         ("RASFF",      "EU"),
    "food.ec.europa.eu":            ("DG SANTE",   "EU"),
    "efsa.europa.eu":               ("EFSA",       "EU"),
    "ecdc.europa.eu":               ("ECDC",       "EU"),
    "blv.admin.ch":                 ("BLV",        "Switzerland"),
    "admin.ch":                     ("BLV",        "Switzerland"),
    # Asia-Pacific
    "foodstandards.gov.au":         ("FSANZ",      "Australia"),
    "mpi.govt.nz":                  ("MPI NZ",     "New Zealand"),
    "cfs.gov.hk":                   ("CFS",        "Hong Kong"),
    "mfds.go.kr":                   ("MFDS",       "South Korea"),
    "mhlw.go.jp":                   ("MHLW",       "Japan"),
    "samr.gov.cn":                  ("SAMR",       "China"),
    "sfa.gov.sg":                   ("SFA",        "Singapore"),
    "fda.gov.ph":                   ("FDA PH",     "Philippines"),
    "fssai.gov.in":                 ("FSSAI",      "India"),
    "tfda.gov.tw":                  ("TFDA",       "Taiwan"),
    "bpom.go.id":                   ("BPOM",       "Indonesia"),
    "moh.gov.my":                   ("KKM",        "Malaysia"),
    "fda.moph.go.th":               ("ThaiFDA",    "Thailand"),
    # LatAm
    "argentina.gob.ar":             ("ANMAT",      "Argentina"),
    "gov.br":                       ("National Authority","Brazil"),
    "anvisa.gov.br":                ("ANVISA",     "Brazil"),
    "gob.mx":                       ("COFEPRIS",   "Mexico"),
    "arcsa.gob.ec":                 ("ARCSA",      "Ecuador"),
    "digesa.gob.pe":                ("DIGESA",     "Peru"),
    "invima.gov.co":                ("INVIMA",     "Colombia"),
    "ispch.cl":                     ("ISP",        "Chile"),
    "msp.gub.uy":                   ("MSP",        "Uruguay"),
    # Middle East / Africa
    "moccae.gov.ae":                ("MOCCAE",     "United Arab Emirates"),
    "health.gov.il":                ("MOH",        "Israel"),
    "moph.gov.qa":                  ("MOPH",       "Qatar"),
    "sfda.gov.sa":                  ("SFDA",       "Saudi Arabia"),
    "tarimorman.gov.tr":            ("TGTHB",      "Turkey"),
    "nafdac.gov.ng":                ("NAFDAC",     "Nigeria"),
    "kebs.org":                     ("KEBS",       "Kenya"),
    "nfsa.gov.eg":                  ("NFSA",       "Egypt"),
    "onssa.gov.ma":                 ("ONSSA",      "Morocco"),
    "ncc.org.za":                   ("NCC",        "South Africa"),
    # Food-safety news outlets (still filtered by URL gate before promotion)
    "foodsafetynews.com":           ("Food Safety News","Unknown"),
    "food-safety.com":              ("Food Safety Magazine","Unknown"),
    "outbreaknewstoday.com":        ("Outbreak News Today","Unknown"),
}


def _lookup_source(url: str) -> Optional[Tuple[str, str]]:
    """Map URL host to (source_label, country). None if host not whitelisted."""
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return None
    if host.startswith("www."):
        host = host[4:]
    if host in HOST_TO_SOURCE:
        return HOST_TO_SOURCE[host]
    # Subdomain match — e.g. accessdata.fda.gov endswith .fda.gov
    for dom, meta in HOST_TO_SOURCE.items():
        if host.endswith("." + dom):
            return meta
    return None


# ---------------------------------------------------------------------------
# Pathogen / hazard vocabulary.
# Order matters — first match wins. Longer/specific names before generic ones.
# ---------------------------------------------------------------------------
PATHOGEN_PATTERNS: List[Tuple[str, str]] = [
    # (regex, canonical_name)
    (r"\bclostridium\s+botulinum\b|\bbotulism\b|\bbotulinum\b",    "Clostridium botulinum"),
    (r"\blisteria\s+monocytogenes\b|\blisteria\b",                 "Listeria monocytogenes"),
    (r"\be\.?\s*coli\s+O\d+(?::H\d+)?\b|\bSTEC\b|\bshiga[-\s]?toxin[-\s]?producing\b",
                                                                    "E. coli STEC"),
    (r"\be\.?\s*coli\b|\bescherichia\s+coli\b",                     "E. coli"),
    (r"\bsalmonella\b",                                             "Salmonella"),
    (r"\bcampylobacter\b",                                          "Campylobacter"),
    (r"\bnorovirus\b|\bnorwalk\b",                                  "Norovirus"),
    (r"\bhepatitis\s*a\b|\bHAV\b",                                  "Hepatitis A"),
    (r"\bcronobacter\b|\benterobacter\s+sakazakii\b",               "Cronobacter"),
    (r"\bcereulide\b",                                              "Cereulide"),
    (r"\bbacillus\s+cereus\b",                                      "Bacillus cereus"),
    (r"\bstaphylococcus\b|\bstaph\s+enterotoxin\b",                 "Staphylococcus aureus"),
    (r"\bshigella\b",                                               "Shigella"),
    (r"\bvibrio\b",                                                 "Vibrio"),
    (r"\byersinia\b",                                               "Yersinia"),
    # Mycotoxins / chemical hazards (April 2026+ scope)
    (r"\baflatoxin",                                                "Aflatoxin"),
    (r"\bochratoxin|\bocratoxin|\bocratossin",                      "Ochratoxin"),
    (r"\bpatulin\b",                                                "Patulin"),
    (r"\balternaria(?:\s+toxin|\s+spp|\b)|\balternariol|\btenuazonic",
                                                                    "Alternaria toxins"),
    (r"\bfumonisin",                                                "Fumonisin"),
    (r"\bzearalenon",                                               "Zearalenone"),
    (r"\bdeoxynivalenol\b|\bnivalenol\b|\bDON\s+toxin",             "Deoxynivalenol (DON)"),
    (r"\b(?:ht|t)[\s\-]?2[\s\-]?toxin\b",                           "T-2 / HT-2 toxin"),
    (r"\bcitrinin\b",                                               "Citrinin"),
    (r"\bergot\b|\bclaviceps\b|\bmutterkorn\b|\balcaloid\w*\s+(?:de|of|d['e]?)\s*ergot",
                                                                    "Ergot alkaloids"),
    (r"\bmycotoxin\b|\bmykotoxin\b|\bmicotoxin|\bmicotossin",       "Mycotoxin"),
    (r"\bhistamine\b|\bscombroid\b",                                "Histamine"),
    (r"\bethylene\s+oxide\b|\bETO\b",                               "Ethylene oxide"),
    # Physical hazards
    (r"\bglass\s+fragment|\bglass\s+pieces|\bbroken\s+glass\b",     "Glass fragments"),
    (r"\bmetal\s+fragment|\bmetal\s+pieces|\bmetal\s+shavings\b",   "Metal fragments"),
    (r"\bplastic\s+fragment|\bplastic\s+pieces\b",                  "Plastic fragments"),
    (r"\bforeign\s+(material|body|object)\b",                       "Foreign material"),
    # Rodenticide / pest
    (r"\brodenticide\b|\brat\s+poison\b|\bbromadiolone\b",          "Rodenticide"),
    (r"\brodent\s+(contamination|droppings|urine|activity)\b|\bmouse\s+droppings\b",
                                                                    "Rodent contamination"),
    # Heavy metals
    (r"\blead\s+contamination\b|\bhigh\s+lead\b|\blead\s+level",    "Lead"),
    (r"\bcadmium\b",                                                "Cadmium"),
    (r"\barsenic\b",                                                "Arsenic"),
    (r"\bmercury\b",                                                "Mercury"),
    # Mould
    (r"\b(mould|mold)\b(?!.*warning)",                              "Mould"),
]


# ---------------------------------------------------------------------------
# Implicit-hazard fallback patterns.
# Tavily snippets are short — often page <title> + 1-2 sentences. FDA recalls
# routinely use boilerplate like "Due to Potential Foodborne Illness" without
# the snippet ever containing the literal pathogen name, even though the
# recall page itself names Salmonella / Listeria / etc.
#
# These patterns act as a SECONDARY tier: if PATHOGEN_PATTERNS doesn't match,
# we try these. They produce a coarse-grained category that's enough to keep
# the candidate alive — Gemini URL-gate downstream will re-extract the actual
# pathogen from the source page itself. The point is to avoid dropping the
# row at the snippet stage.
#
# Order matters — first match wins. Specific allergens before generic ones.
# Triggered by the audit 2026-04-28: a Ghirardelli Salmonella recall on FDA
# was dropped because the title said only "Potential Foodborne Illness".
IMPLICIT_HAZARD_PATTERNS: List[Tuple[str, str]] = [
    # Specific allergens that have a Layer D PATHOGEN_ALIASES key
    (r"\bundeclared\s+peanut\b|\bmay\s+contain\s+peanut\b",         "Peanut"),
    (r"\bundeclared\s+sulphite|\bundeclared\s+sulfite\b",           "Sulfite"),
    # Generic undeclared allergen — fold into single canonical
    (r"\bundeclared\s+(milk|dairy|lactose|soy|wheat|gluten|egg|"
     r"fish|shellfish|crustacean|sesame|tree[\s\-]?nut|almond|"
     r"cashew|hazelnut|walnut|pecan|pistachio|mustard|celery)\b",   "Undeclared allergen"),
    (r"\bmay\s+contain.*\b(milk|dairy|soy|wheat|gluten|egg|"
     r"fish|shellfish|sesame|nut)\b",                               "Undeclared allergen"),
    (r"\bundeclared\s+allergen\b|\bundeclared\s+ingredient\b",      "Undeclared allergen"),
    # FDA / regulator boilerplate without explicit pathogen named
    (r"\bpotential\s+foodborne\s+illness\b|\bfoodborne\s+illness\b","Foodborne pathogen"),
    (r"\bpotential\s+health\s+(hazard|risk)\b",                     "Pathogen contamination"),
    (r"\bdue\s+to\s+possible\s+contamination\b",                    "Pathogen contamination"),
    (r"\brecalled?\b.*\bcontamination\b",                           "Pathogen contamination"),
    # Physical-hazard softer triggers
    (r"\bmay\s+contain.*\bforeign\b",                               "Foreign material"),
]


def _detect_pathogen(text: str) -> str:
    """Scan text for pathogen/hazard keywords. Returns canonical name or ''."""
    t = text.lower()
    for pat, name in PATHOGEN_PATTERNS:
        if re.search(pat, t, flags=re.IGNORECASE):
            return name
    # Fallback — implicit hazard signals (FDA boilerplate, undeclared
    # allergens, "may contain X"). Triggered when primary patterns miss.
    # Coarse-grained categories are enough; Gemini URL-gate re-extracts the
    # actual pathogen from the source page downstream.
    for pat, name in IMPLICIT_HAZARD_PATTERNS:
        if re.search(pat, t, flags=re.IGNORECASE):
            return name
    return ""


# ---------------------------------------------------------------------------
# Outbreak detection
# ---------------------------------------------------------------------------
_OUTBREAK_RX = re.compile(
    r"\b(outbreak|illness(?:es)?|hospitali[sz]ed|hospitali[sz]ation|"
    r"died|deaths?|fatal(?:ity|ities)?|sick(?:ened)?|cases?\s+of|"
    r"linked\s+to\s+illness|caused\s+illness|epidemiological)\b",
    flags=re.IGNORECASE,
)


def _detect_outbreak(text: str) -> int:
    return 1 if _OUTBREAK_RX.search(text) else 0


# ---------------------------------------------------------------------------
# Company / Product heuristic extraction from title.
# Title pattern examples:
#   "Acme Foods Inc. recalls Sausage product due to Listeria"
#   "FDA Alert: Alpha Sprouts recalls all products after Salmonella"
#   "Brand X issues voluntary recall of Chicken Breast over E. coli"
# ---------------------------------------------------------------------------
_RECALL_VERBS = (
    r"recalls?|recalled|recalling|issues?\s+recall|withdraws?|withdrawn|"
    r"pulls?|pulled|alerts?|alerted|warns?|warned"
)
_TITLE_SPLIT_RX = re.compile(
    r"^(?P<company>.{3,120}?)\s+(?:" + _RECALL_VERBS + r")\s+(?P<product>.+?)"
    r"(?:\s+(?:due\s+to|over|after|because\s+of|linked\s+to|for)\s+|$)",
    flags=re.IGNORECASE,
)

# News outlets often prefix with agency name — strip common prefixes before parsing
_TITLE_PREFIXES = re.compile(
    r"^\s*(?:FDA|USDA|CFIA|FSA|FSANZ|FSAI|RASFF)\s+(?:Alert|Notice|Update|Recall)[:\s-]+",
    flags=re.IGNORECASE,
)


def _extract_company_product(title: str, content: str) -> Tuple[str, str]:
    """Best-effort company + product extraction. Blank strings if nothing found.
    The URL gate (Claude Haiku) will reject rows missing required fields, so
    it's OK to return partial data here."""
    t = _TITLE_PREFIXES.sub("", title or "").strip()
    m = _TITLE_SPLIT_RX.match(t)
    if m:
        company = m.group("company").strip(" -:,;")
        product = m.group("product").strip(" -:,;")[:200]
        return company, product
    # Fallback: no verb match. Use first 80 chars of title as company, rest as product.
    if len(t) > 10:
        # Split at first colon / dash if any
        parts = re.split(r"\s+[-:–—]\s+", t, maxsplit=1)
        if len(parts) == 2:
            return parts[0].strip()[:120], parts[1].strip()[:200]
        return t[:120], ""
    return "", ""


# ---------------------------------------------------------------------------
# Date handling
# ---------------------------------------------------------------------------
_DATE_RX = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


def _parse_date(tavily_item: Dict[str, Any]) -> str:
    """Extract publication date from Tavily's item.

    Returns YYYY-MM-DD on success, or '' on failure. CRITICAL: never
    falls back to today's date — that's the exact bug that put the SFA
    Nature One Dairy recall in the dashboard at 2026-04-25 instead of
    its actual publication date 2026-03-15. Rows without a parseable
    date MUST be dropped by the caller (_item_to_recall).
    """
    pd = (tavily_item.get("published_date") or "").strip()
    if pd:
        m = _DATE_RX.search(pd)
        if m:
            return m.group(0)
    # Try content/title for an ISO date
    for fld in ("content", "title"):
        v = (tavily_item.get(fld) or "")
        m = _DATE_RX.search(v)
        if m:
            return m.group(0)
    # No fallback to today — return empty so caller drops the row
    return ""


# ---------------------------------------------------------------------------
# Tavily search
# ---------------------------------------------------------------------------
def _tavily_search(query: str, max_results: int = 10, days: int = 7) -> List[Dict[str, Any]]:
    """Single Tavily search. Returns raw result dicts or [] on failure."""
    if not TAVILY_API_KEY:
        log.error("TAVILY_API_KEY not set — skipping search")
        return []
    body = {
        "api_key":      TAVILY_API_KEY,
        "query":        query,
        "search_depth": "advanced",     # better recall coverage than "basic"
        "include_answer": False,        # we parse raw results ourselves
        "max_results":  max_results,
        "days":         days,           # restrict to recent N days
        "topic":        "news",         # recall announcements are news
    }
    try:
        r = requests.post(TAVILY_ENDPOINT, json=body, timeout=30)
        if r.status_code != 200:
            log.warning("Tavily %d: %s", r.status_code, r.text[:200])
            return []
        data = r.json()
        return data.get("results", []) or []
    except Exception as e:
        log.warning("Tavily call failed: %s", e)
        return []


def _run_tavily_queries(since_days: int) -> List[Dict[str, Any]]:
    """Run the canonical gap-finder queries and dedup by URL.

    Primary region: NorthAmerica. Tavily is the deterministic backstop
    for the FDA / USDA-FSIS / CFIA pipeline (high-volume English pages
    with predictable structure that Tavily's whitelisted-domain extractor
    parses reliably without an LLM call). NorthAmerica site:queries run
    FIRST — if Tavily's free tier hits its rate limit mid-run, we still
    have full NA coverage. Other regions fill the remaining quota.
    """
    queries = [
        # ── PRIMARY REGION: NorthAmerica (run first, deepest coverage) ──
        # FDA: cover the actual recall path AND press-release wording.
        # Broader than `site:fda.gov/safety/recalls food recall pathogen`
        # which dropped Ghirardelli (audit 2026-04-28) because its snippet
        # said only "Potential Foodborne Illness" without naming Salmonella.
        'site:fda.gov/safety/recalls-market-withdrawals-safety-alerts recall',
        'site:fda.gov "voluntarily recalls" OR "issues recall"',
        'site:fsis.usda.gov/recalls-alerts food recall',
        'site:recalls-rappels.canada.ca food recall',
        'site:inspection.canada.ca food recall',
        'site:quebec.ca recall food MAPAQ',
        # ── Generic global pathogen / hazard sweeps ─────────────────────
        'food recall salmonella OR listeria OR "e. coli" OR botulism OR campylobacter',
        'food recall mould OR mold OR "foreign material" OR glass OR "ethylene oxide"',
        'food recall rodent OR "rat poison" OR rodenticide OR "rodent contamination"',
        'food recall undeclared allergen OR "may contain"',
        # ── Other-region site:queries (lighter pass) ────────────────────
        'RASFF notification food alert withdrawal',
        'site:food.gov.uk food alert recall',
        'site:fsai.ie food recall alert',
        'site:rappelconso.gouv.fr rappel alimentaire',
        'site:foodstandards.gov.au food recall',
    ]
    all_results: Dict[str, Dict[str, Any]] = {}
    for q in queries:
        log.info("Tavily search: %s", q)
        results = _tavily_search(q, max_results=10, days=since_days)
        log.info("  -> %d raw results", len(results))
        for r in results:
            url = (r.get("url") or "").strip()
            if not url:
                continue
            if _lookup_source(url) is None:  # not a whitelisted regulator
                continue
            if url not in all_results:
                all_results[url] = r
    log.info("Regulator-whitelisted results: %d unique URLs", len(all_results))
    return list(all_results.values())


# ---------------------------------------------------------------------------
# Tavily item → Recall (pure Python, no AI)
# ---------------------------------------------------------------------------
def _is_generic_url(url: str) -> bool:
    """True if URL is a generic listing/category/disease/transparency page —
    not a specific recall fiche. Mirrors the patterns in
    merge_master.validate_pending_row()."""
    u = url.lower()
    bad_substrings = (
        "page=", "/list?", "/a-z/", "animal-disease",
        "regulatory-transparency", "/categorie/", "/rubrik/", "/tag/",
        "vertexaisearch",  # defensive (Tavily wouldn't return these but be safe)
    )
    if any(p in u for p in bad_substrings):
        return True
    # Bare domain or one-segment paths (homepages, root listings)
    try:
        from urllib.parse import urlparse as _up
        path = (_up(url).path or "").strip("/")
        if not path:
            return True  # bare domain
    except Exception:
        pass
    return False


def _item_to_recall(item: Dict[str, Any]) -> Optional[Recall]:
    """Convert a single Tavily result into a Recall object.
    Returns None if the item has no detectable pathogen/hazard (and is thus
    not a food-safety recall worth pending-promoting)."""
    url = (item.get("url") or "").strip()
    if not url or not url.lower().startswith(("http://", "https://")):
        return None
    # Drop generic listing/category/disease pages — defensive backstop.
    # The merge_master validate_pending_row gate is authoritative, this
    # just keeps Tavily's logs cleaner.
    if _is_generic_url(url):
        return None

    src = _lookup_source(url)
    if not src:
        return None
    source_label, country_guess = src

    title   = (item.get("title") or "").strip()
    content = (item.get("content") or "").strip()
    blob = (title + "  " + content)

    pathogen_raw = _detect_pathogen(blob)
    if not pathogen_raw:
        return None  # no recognised food-safety hazard → drop
    pathogen = normalize_pathogen(pathogen_raw)

    outbreak = _detect_outbreak(blob)
    country  = normalize_country(country_guess) or country_guess

    company, product = _extract_company_product(title, content)
    if not product:
        # Use first sentence of content as fallback product/description
        product = (content.split(". ", 1)[0])[:200]

    date_str = _parse_date(item)
    # HARD DROP: no date → no row. Same for pre-2026 dates. The merge_master
    # validate_pending_row gate would catch these too, but dropping here
    # keeps the Pending insert log clean.
    if not date_str:
        return None
    if date_str < "2026-01-01":
        return None

    rec = Recall(
        Date=date_str,
        Source=source_label or "Tavily-gap",
        Company=company,
        Brand=company if company else "—",
        Product=product,
        Pathogen=pathogen,
        Reason=(pathogen_raw + (" — outbreak" if outbreak else "")),
        Class="Recall",
        Country=country,
        Region=infer_region(country) if country else "",
        Tier=assign_tier(pathogen, outbreak),
        Outbreak=outbreak,
        URL=url,
        Notes=(content[:300] + "  [via Tavily gap-finder, deterministic extract]"),
    )
    try:
        rec = rec.normalize()
    except Exception as e:
        log.warning("normalize failed for %s: %s", url, e)
        return None

    if not rec.Pathogen or rec.Pathogen in ("—", ""):
        return None
    return rec


def results_to_recalls(items: List[Dict[str, Any]]) -> List[Recall]:
    out: List[Recall] = []
    dropped_no_hazard = 0
    for item in items:
        rec = _item_to_recall(item)
        if rec is None:
            dropped_no_hazard += 1
            continue
        out.append(rec)
    log.info("Tavily: %d items → %d valid recalls (dropped %d with no detectable hazard)",
             len(items), len(out), dropped_no_hazard)
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("Tavily gap-finder run (standalone, no other AI): %s", scraped_at)

    if not TAVILY_API_KEY:
        log.error("TAVILY_API_KEY not set — cannot run")
        return 1

    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    approved = load_existing(XLSX_PATH)
    pending  = load_pending(XLSX_PATH)
    log.info("Loaded %d approved + %d pending rows", len(approved), len(pending))

    # 1. Run Tavily searches, filter to whitelisted regulator domains
    items = _run_tavily_queries(SINCE_DAYS)
    if not items:
        log.info("Tavily: no regulator-whitelisted results this run.")
        return 0

    # 2. Extract structured fields deterministically from title/content/url
    recalls = results_to_recalls(items)
    if not recalls:
        log.info("Tavily gap-finder: no rows with detectable pathogens/hazards.")
        return 0

    # 3. Dedup-append to Pending (URL gate will validate before Recalls promotion)
    new_pending = append_to_pending(
        existing_pending=pending,
        approved=approved,
        new_recalls=recalls,
        scraped_at=scraped_at,
    )
    added = len(new_pending) - len(pending)
    log.info("Tavily gap-finder: added %d new rows to Pending", added)

    save_xlsx_with_pending(
        xlsx_path=XLSX_PATH,
        approved_rows=sort_rows(approved),
        pending_rows=sort_rows(new_pending),
    )

    if added > 0 and not SKIP_COMMIT:
        msg = f"Tavily gap-finder (standalone): +{added} rows to Pending ({scraped_at})"
        git_commit_and_push(ROOT, [str(XLSX_PATH)], msg)
        log.info("Committed and pushed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
