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

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "7"))
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
# Audit 2026-05-06: improved for FDA-style "Brand X Issues Voluntary Recall of Y".
# Pre-fix output: Company="Brand X Issues Voluntary", Product="of Y" (broken).
# Now we accept optional filler ("Issues/Announces/Initiates/Conducts" +
# "Voluntary/Precautionary/Nationwide/Class I/Class II") between the brand
# and the recall verb so "Brand X" gets captured cleanly.
_RECALL_VERBS_WITH_FILLER = (
    r"(?:issues?|announces?|initiates?|conducts?|expands?)\s+"
    r"(?:a\s+)?(?:voluntary|precautionary|nationwide|"
    r"class\s+i+|class\s+1|class\s+2|class\s+3)\s+recall(?:\s+of)?|"
    + _RECALL_VERBS
)
_TITLE_SPLIT_RX = re.compile(
    r"^(?P<company>.{3,120}?)\s+(?:" + _RECALL_VERBS_WITH_FILLER + r")\s+(?P<product>.+?)"
    r"(?:\s+(?:due\s+to|over|after|because\s+of|linked\s+to|for)\s+|$)",
    flags=re.IGNORECASE,
)

# News outlets often prefix with agency name — strip common prefixes before parsing.
# Audit 2026-05-06: also strip the "Recall of [specific/a] [batch(es)] of"
# pattern that FSAI, CFIA, and RappelConso use as the canonical headline
# format. Production data showed FSAI rows rejected by merge_master because
# Company came in as "Recall of specific batches of Rosabella Moringa
# capsules" — the title-split regex couldn't find a recall verb after that
# leading clause, so the whole title fell through to fallback as the company.
_TITLE_PREFIXES = re.compile(
    r"^\s*(?:"
    # Agency-name prefixes from news outlets
    r"(?:FDA|USDA|CFIA|FSA|FSANZ|FSAI|RASFF|HPRA|HSE)\s+(?:Alert|Notice|Update|Recall|Press\s+Release)[:\s\-]+"
    r"|"
    # FSAI / RappelConso / CFIA canonical openers — both EN and FR
    r"Recall\s+of\s+(?:specific\s+)?(?:a\s+)?(?:batches?\s+of\s+|batch\s+of\s+)?"
    r"|"
    r"Rappel\s+(?:de\s+|du\s+|d['']\s*)?(?:certaines?\s+)?(?:lots?\s+(?:de\s+|d['']\s*)?)?"
    r"|"
    # Generic "Notice/Alert: " prefixes
    r"(?:Public\s+Health\s+)?(?:Alert|Notice|Advisory|Warning)\s*[:\-]\s+"
    r")",
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
# Date handling — multi-format (audit 2026-05-06)
# ---------------------------------------------------------------------------
# Pre-fix: only matched ISO YYYY-MM-DD via single regex, then dropped any
# row with no date. Production data shows this systematically misses
# fresh FDA + CFIA recalls because:
#   • Tavily's `published_date` field is best-effort metadata extraction
#     and is often empty for fresh pages.
#   • Recall page text typically formats dates as "May 5, 2026" or
#     "5 May 2026" or "Recall date: 2026-05-05" (mixed locales),
#     none of which the previous \d{4}-\d{2}-\d{2} regex matched.
#
# Post-fix: try (1) Tavily's published_date as ISO, (2) every plausible
# date format embedded in title+content (EN "May 5, 2026", "5 May 2026",
# "May 5 2026", FR "5 mai 2026", DE "5. Mai 2026", IT "5 maggio 2026",
# ES "5 de mayo de 2026", US "5/5/2026" / "05/05/2026", ISO with time).
# When NONE parse, return today's date with a sentinel marker so
# _item_to_recall can write the row to Pending with a Notes flag for
# claude-check to fix during page-content review.

_DATE_RX = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

# Numeric date: 5/5/2026, 05/05/2026, 5-5-2026 (US-style M/D/Y by default)
_NUMERIC_DATE_RX = re.compile(
    r"\b(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})\b"
)

# Written month names — multilingual. Order: English, French, German,
# Italian, Spanish, Portuguese. Lowercased before matching.
_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
    "januar": 1, "februar": 2, "märz": 3, "marz": 3, "mai": 5, "juni": 6,
    "juli": 7, "oktober": 10, "dezember": 12,
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5,
    "giugno": 6, "luglio": 7, "agosto": 8, "settembre": 9, "ottobre": 10,
    "dicembre": 12,
    "enero": 1, "febrero": 2, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4, "maio": 5,
    "junho": 6, "julho": 7, "setembro": 9, "outubro": 10, "novembro": 11,
    "dezembro": 12,
}
# Build a regex matching any month name as a whole-word token
_MONTH_PATTERN = "|".join(sorted(_MONTH_NAMES.keys(), key=len, reverse=True))

# "May 5, 2026" / "May 5 2026" / "Oct. 19, 2018" / "5 May 2026" / "5 de mayo de 2026"
# Audit 2026-05-06: optional period after the month name is critical —
# US/USDA boilerplate uses "Oct. 19, 2018" / "Apr. 30, 2026" formats
# which previously failed the regex entirely and fell back to today.
_WRITTEN_DATE_RX_MD = re.compile(
    rf"\b({_MONTH_PATTERN})\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?[,.\s]+(\d{{4}})\b",
    re.IGNORECASE,
)
_WRITTEN_DATE_RX_DM = re.compile(
    rf"\b(\d{{1,2}})(?:\.|st|nd|rd|th)?\s+(?:de\s+)?({_MONTH_PATTERN})\.?\s+"
    rf"(?:de\s+)?(\d{{4}})\b",
    re.IGNORECASE,
)


def _parse_date(tavily_item: Dict[str, Any]) -> Tuple[str, bool]:
    """Extract publication date from a Tavily/Exa search result.

    Returns ``(YYYY-MM-DD, fallback_flag)`` where ``fallback_flag=True``
    means we couldn't find a real date and used today as a placeholder.
    Caller should stamp ``[date-unknown]`` in Notes so claude-check
    fixes the Date field on its next pass.

    Audit 2026-05-06: previous version returned ``""`` (empty) on no-date
    and ``_item_to_recall`` dropped the row silently. Production data
    shows this systematically blocks fresh FDA+CFIA recalls because
    their pages don't carry standard ``article:published_time`` Open
    Graph metadata and their title/content uses written-out dates.
    The fix is to keep the row, mark it for downstream enrichment.
    """
    # 1. Tavily/Exa-provided published_date field (most reliable when present)
    pd = (tavily_item.get("published_date") or "").strip()
    if pd:
        m = _DATE_RX.search(pd)
        if m:
            return m.group(0), False

    # 2. Search title + content for any of the supported formats
    blob = " ".join(
        str(tavily_item.get(fld) or "")
        for fld in ("title", "content", "snippet", "description")
    )

    # 2a. ISO YYYY-MM-DD
    m = _DATE_RX.search(blob)
    if m:
        return m.group(0), False

    # 2b. Written-month "Month Day, Year"  → 2026-05-05
    m = _WRITTEN_DATE_RX_MD.search(blob)
    if m:
        try:
            mon_name = m.group(1).lower()
            day = int(m.group(2))
            year = int(m.group(3))
            mon = _MONTH_NAMES.get(mon_name)
            # Audit 2026-05-06: widened year range from 2020-2100 to 2010-2030.
            # Previously a real "March 29, 2018" date was rejected by the
            # narrow 2020-2100 window and fell through to today's date —
            # making 8-year-old recalls look fresh. Now we parse them so
            # _item_to_recall can drop them via the staleness check below.
            if mon and 1 <= day <= 31 and 2010 <= year <= 2030:
                return f"{year:04d}-{mon:02d}-{day:02d}", False
        except (ValueError, KeyError):
            pass

    # 2c. Written-month "Day Month Year"  → 2026-05-05
    m = _WRITTEN_DATE_RX_DM.search(blob)
    if m:
        try:
            day = int(m.group(1))
            mon_name = m.group(2).lower()
            year = int(m.group(3))
            mon = _MONTH_NAMES.get(mon_name)
            if mon and 1 <= day <= 31 and 2010 <= year <= 2030:
                return f"{year:04d}-{mon:02d}-{day:02d}", False
        except (ValueError, KeyError):
            pass

    # 2d. Numeric M/D/Y or D/M/Y (ambiguous — assume M/D/Y for US-leaning
    # corpus, but accept both interpretations if both look valid)
    m = _NUMERIC_DATE_RX.search(blob)
    if m:
        a, b, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2010 <= year <= 2030:
            # Try M/D first (US format dominates our corpus)
            if 1 <= a <= 12 and 1 <= b <= 31:
                return f"{year:04d}-{a:02d}-{b:02d}", False
            # Fall back to D/M
            if 1 <= b <= 12 and 1 <= a <= 31:
                return f"{year:04d}-{b:02d}-{a:02d}", False

    # 3. Last resort — return EMPTY date with fallback flag (audit 2026-05-07).
    #    Previous version returned today's date, which caused 45% wrong-Date
    #    rate observed in 2026-05-07 morning claude-check (5 of 11 rows had
    #    today's date stamped on recalls 2-3 months old). claude-check now
    #    fills the real date from the regulator page during content review;
    #    the row carries Date="" until then, with a Notes flag.
    return "", True


# ---------------------------------------------------------------------------
# Tavily search
# ---------------------------------------------------------------------------
def _tavily_search(query: str, max_results: int = 10, days: int = 7,
                   topic: str = "news") -> List[Dict[str, Any]]:
    """Single Tavily search. Returns raw result dicts or [] on failure.

    Audit 2026-05-06 — added ``topic`` parameter. Pre-fix every query
    used ``topic="news"`` which restricts Tavily's index to news-domain
    pages. Government regulator pages on fda.gov, fsis.usda.gov,
    recalls-rappels.canada.ca etc. are not always classified as "news"
    in Tavily's taxonomy — they're regulator notices. The news topic
    over-filtered FDA + CFIA recalls. Passing ``topic="general"`` for
    site-restricted regulator queries widens the search to the full
    Tavily index. Generic-language queries keep ``topic="news"`` because
    those rely on Tavily's news ranking to surface fresh recalls.
    """
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
        "topic":        topic,          # "general" for site:regulator, "news" for free queries
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
    # Per-query topic (audit 2026-05-06): site:regulator queries use
    # "general" because regulator pages aren't always classified as news
    # in Tavily's taxonomy. Free-text queries keep "news" because those
    # rely on Tavily's news ranking to surface recent recalls.
    queries: List[Tuple[str, str]] = [
        # ── PRIMARY REGION: NorthAmerica (run first, deepest coverage) ──
        # FDA: cover the actual recall path AND press-release wording.
        # Broader than `site:fda.gov/safety/recalls food recall pathogen`
        # which dropped Ghirardelli (audit 2026-04-28) because its snippet
        # said only "Potential Foodborne Illness" without naming Salmonella.
        ('site:fda.gov/safety/recalls-market-withdrawals-safety-alerts recall',  "general"),
        ('site:fda.gov "voluntarily recalls" OR "issues recall"',                "general"),
        ('site:fsis.usda.gov/recalls-alerts food recall',                        "general"),
        ('site:recalls-rappels.canada.ca food recall',                           "general"),
        ('site:inspection.canada.ca food recall',                                "general"),
        ('site:quebec.ca recall food MAPAQ',                                     "general"),
        # ── Generic global pathogen / hazard sweeps (news ranking helps) ─
        ('food recall salmonella OR listeria OR "e. coli" OR botulism OR campylobacter',  "news"),
        ('food recall mould OR mold OR "foreign material" OR glass OR "ethylene oxide"',  "news"),
        ('food recall rodent OR "rat poison" OR rodenticide OR "rodent contamination"',   "news"),
        ('food recall undeclared allergen OR "may contain"',                              "news"),
        # ── Other-region site:queries (lighter pass) ────────────────────
        ('RASFF notification food alert withdrawal',                  "news"),
        ('site:food.gov.uk food alert recall',                        "general"),
        ('site:fsai.ie food recall alert',                            "general"),
        # Audit 2026-05-06: query domain typo. Production recall URLs
        # are at `rappel.conso.gouv.fr/fiche-rappel/<id>/Interne` (note
        # the dot — `rappel` is a SUBDOMAIN of `conso.gouv.fr`).
        # The previous query used `rappelconso.gouv.fr` (no dot, no
        # subdomain), which doesn't index recall content — every Tavily
        # run returned 0 results for France.
        ('site:rappel.conso.gouv.fr OR site:rappelconso.gouv.fr rappel alimentaire',  "general"),
        # ── Coverage expansion (audit 2026-05-06) ────────────────────────
        # Pre-fix: only 9 of 75 whitelisted regulators were queried. Major
        # EU and Asia-Pac members had no dedicated query, relying on news-
        # topic searches that rarely surface the regulator URL directly.
        # The added queries below target the highest-volume non-RASFF EU
        # member-state regulators plus the major Asia-Pac food authorities
        # whose dedicated scrapers are weakest.
        # ── EU (non-FR) ──
        ('site:lebensmittelwarnung.de Lebensmittel Rückruf',          "general"),
        ('site:salute.gov.it richiamo alimenti',                      "general"),
        ('site:aesan.gob.es alerta alimentaria retirada',             "general"),
        ('site:ages.at Lebensmittel Warnung',                         "general"),
        ('site:nvwa.nl voedsel terugroep',                            "general"),
        ('site:afsca.be OR site:favv-afsca.be voedsel terugroep rappel', "general"),
        ('site:livsmedelsverket.se livsmedel återkallelse',           "general"),
        # ── Asia-Pacific ──
        ('site:mpi.govt.nz food recall',                              "general"),
        ('site:mfds.go.kr OR site:mfds.go.kr/eng food recall 회수',    "general"),
        ('site:fssai.gov.in food recall',                             "general"),
        ('site:cfs.gov.hk food alert recall',                         "general"),
        # ── EU bodies ──
        ('site:webgate.ec.europa.eu/rasff-window notification',       "general"),
        ('site:foodstandards.gov.au food recall',                     "general"),
        # ── Audit 2026-05-06 batch 2: missing-markets coverage ────────────
        # Production data showed these regulators are 21-109 days stale OR
        # have zero direct scraper captures. The dedicated 10-line Gemini-
        # wrapper scrapers (sfa_sg.py, tfda_tw.py, samr_cn.py, mhlw_jp.py,
        # blv_ch.py) silently fail like FSAI did. Direct site:queries via
        # Tavily are the most reliable backfill until those scrapers can
        # be hardened with deterministic HTML parsing.
        ('site:mhlw.go.jp shokuhin recall',                           "general"),
        ('site:caa.go.jp food recall safety',                         "general"),
        ('site:sfa.gov.sg food recall',                               "general"),
        ('site:fda.gov.tw recall food OR 食品 OR 回收',                 "general"),
        ('site:blv.admin.ch Lebensmittel Rückruf OR Rappel',          "general"),
        ('site:samr.gov.cn 食品 召回 OR 不合格',                          "general"),
        ('site:efet.gr ανάκληση τροφίμων',                            "general"),
    ]
    all_results: Dict[str, Dict[str, Any]] = {}
    for q, topic in queries:
        log.info("Tavily search [topic=%s]: %s", topic, q)
        results = _tavily_search(q, max_results=10, days=since_days, topic=topic)
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
# ── URL filtering (audit 2026-05-06) ──────────────────────────────────────
# Hoisted to pipeline/_url_filters.py — single source of truth shared with
# merge_master, gap_finder_exa, daily_recall_search. Pre-fix, four
# separate copies of this list drifted apart and search-result-shell URLs
# (CFIA /search/site, FSANZ /circulars/notification-circular-) sailed past
# the gap-finders' own filters until merge_master's validate_pending_row
# rejected them downstream — wasting reviewer slots in the meantime.
# Importing the shared module guarantees identical filtering everywhere.
from pipeline._url_filters import (
    is_generic_url as _is_generic_url,                    # noqa: F401
    KNOWN_REGULATOR_LANDINGS as _KNOWN_REGULATOR_LANDINGS,  # noqa: F401
)
# Audit 2026-05-08: news-mirror blocklist applied at GAP-FINDER stage,
# not just merge_master gate. Previously a Tavily/Exa row from
# foodsafetynews.com / cidrap.umn.edu / etc. would consume a reviewer
# slot before being rejected downstream. Now we drop them at extract.
from pipeline._news_mirror_blocklist import is_news_mirror as _is_news_mirror

# Audit 2026-05-08: known-garbage Company values produced by
# _extract_company_product when the page is a regulator landing /
# press-release index / educational article. Claude consistently flags
# these as "Company is generic landing-page text" → FAIL. Reject at
# Tavily filter so they never reach the reviewer queue.
_GARBAGE_COMPANY_TOKENS = frozenset({
    # Generic page-title artefacts
    "food", "food incident post", "press release", "news",
    "what's new", "whats new", "recall", "recalls", "alert", "alerts",
    "food alert", "food alerts", "food recall", "food recalls",
    "food safety", "food safety news", "food poisoning",
    "advisory", "advisories", "notice", "notices",
    # Regulator names (extracted as Company when the page is a landing)
    "fda", "usda", "fsis", "cfia", "fsa", "fsai", "fsanz", "mpi",
    "aesan", "ages", "bvl", "afsca", "favv", "nvwa", "rasff",
    "efet", "cfs", "sfa", "mfds", "mhlw", "fssai", "anvisa",
    "centre for food safety", "food standards agency",
    "food standards australia new zealand",
    "agencia española de seguridad alimentaria",
    "agencia española de seguridad alimentaria y nutrición",
    "ministry of food and drug safety",
    "european commission",
})


def _is_garbage_company(company: str) -> bool:
    """True if extracted Company is a known landing-page / regulator-name
    artefact rather than an actual recalling firm. Conservative: only
    matches whole-string equality (case-insensitive) so legitimate company
    names that happen to contain "Food" (e.g. "Wholesome Food Ltd") are
    NOT rejected.
    """
    if not company:
        return False
    c = company.strip().lower().rstrip(".:;,")
    return c in _GARBAGE_COMPANY_TOKENS


def _hazard_in_title_or_lede(title: str, content: str) -> bool:
    """True if a pathogen/hazard keyword appears in the title or first 300
    chars of content. Pre-fix, _detect_pathogen scanned the entire blob,
    so unrelated articles that mentioned "Salmonella" in a footer or
    related-reading section were promoted to recall candidates. The
    actual recalls always mention the hazard up-front.
    """
    head = (title or "") + "  " + ((content or "")[:300])
    return bool(_detect_pathogen(head))




# ─────────────────────────────────────────────────────────────────────────
# Reliable-direct-scraper hosts (audit 2026-05-21)
# ─────────────────────────────────────────────────────────────────────────
# These regulators have direct API/feed scrapers in scrapers/north_america/
# that catch fresh recalls within hours of publication on the regulator's
# own infrastructure. The gap-finder's ROLE is to catch what the direct
# scrapers MISS — typically slow-publishing or syndication-lagged content.
#
# Production data 2026-05-21: Tavily surfaced four FSIS recalls (JBS USA
# 2021, Yu Shang 2024, FSIS PHA pasta 2025, FSIS PHA ham salad 2025) all
# 1-5 years old, with no parseable date in the snippet. Reviewer drained
# four Pending slots tracking these down before rejecting all of them as
# historical.
#
# The pattern: when a reliable-direct-scraper-host URL surfaces in the
# gap-finder AND the snippet has no extractable date, the result is
# almost always archived content. If it were fresh, the direct scraper
# would have caught it on its own 30-minute / hourly run.
#
# Conservative drop policy: only kicks in when BOTH conditions hold:
#   1. URL host is in this set (regulator has reliable direct coverage)
#   2. Tavily/Exa returned no published_date AND no date-in-snippet
#      (date_is_fallback=True from _parse_date)
#
# Rationale for conservatism: if the direct scraper is broken (Akamai
# block, schema change, feed retired), we want gap-finder to still
# surface fresh recalls. The fallback-date path is the only one this
# guard touches. Rows with parseable dates go through the existing
# 90-day staleness gate unchanged.
RELIABLE_DIRECT_SCRAPER_HOSTS = frozenset({
    "www.fsis.usda.gov",
    "fsis.usda.gov",
    "www.fda.gov",
    "fda.gov",
    "recalls-rappels.canada.ca",
    "inspection.canada.ca",
})


def _is_reliable_direct_scraper_host(url: str) -> bool:
    """True if URL host has a direct AFTS scraper that would catch fresh recalls."""
    if not url:
        return False
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return host in RELIABLE_DIRECT_SCRAPER_HOSTS


def _item_to_recall(item: Dict[str, Any],
                    finder_name: str = "Tavily") -> Optional[Recall]:
    """Convert a single search-engine result into a Recall object.
    Returns None if the item has no detectable pathogen/hazard.

    Audit 2026-04-29: added finder_name parameter so daily_recall_search_exa
    can label its rows correctly. Previously every row got '[via Tavily
    gap-finder]' regardless of which engine actually produced it, which
    broke leak forensics — the 7 leaked rows on 2026-04-29 looked like
    they came from Gemini (because they passed through gap_finder_gemini
    too) when in fact the search hits originated from Exa.

    Audit 2026-05-08: tightened filtering after Exa run produced 50%
    garbage that hit merge_master gate-rejects. New defenses:
      - news-mirror blocklist applied here (not only at gate)
      - garbage-Company detector (drops "Food" / "Press Release" / etc.)
      - hazard mention required in title or first 300 chars (not anywhere)
    """
    url = (item.get("url") or "").strip()
    if not url or not url.lower().startswith(("http://", "https://")):
        return None
    # Drop generic listing/category/disease pages — defensive backstop.
    # The merge_master validate_pending_row gate is authoritative, this
    # just keeps Tavily's logs cleaner.
    if _is_generic_url(url):
        return None
    # Audit 2026-05-08: news-mirror domains rejected here, not just at
    # the merge_master gate. Saves a reviewer slot per leaked row.
    if _is_news_mirror(url):
        return None

    src = _lookup_source(url)
    if not src:
        return None
    source_label, country_guess = src

    title   = (item.get("title") or "").strip()
    content = (item.get("content") or "").strip()
    blob = (title + "  " + content)

    # Audit 2026-05-08: hazard must appear in title OR first 300 chars of
    # content. Recall pages always lead with the pathogen; articles that
    # only mention it in a "related reading" footer are not recalls.
    if not _hazard_in_title_or_lede(title, content):
        return None

    pathogen_raw = _detect_pathogen(blob)
    if not pathogen_raw:
        return None  # no recognised food-safety hazard → drop
    pathogen = normalize_pathogen(pathogen_raw)

    outbreak = _detect_outbreak(blob)
    country  = normalize_country(country_guess) or country_guess

    company, product = _extract_company_product(title, content)
    # Audit 2026-05-08: drop rows where Company comes back as a known
    # landing-page / regulator-name artefact. Claude rejects these
    # uniformly anyway; we save the API call.
    if _is_garbage_company(company):
        return None
    if not product:
        # Use first sentence of content as fallback product/description
        product = (content.split(". ", 1)[0])[:200]

    date_str, date_is_fallback = _parse_date(item)
    # HARD DROP: pre-2026 dates remain rejected (sentinel/year-mismatch leak).
    # Audit 2026-05-07: skip the pre-2026 check on fallback rows where
    # date_str="" — empty string would compare True < "2026-01-01" and
    # silently drop the row before claude-check ever sees it. Fallback rows
    # land in Pending with Date="", and claude-check fills it during the
    # next content review pass.
    if not date_is_fallback and date_str < "2026-01-01":
        return None

    # ── Reliable-direct-scraper-host fallback-date drop (audit 2026-05-21) ──
    # If Tavily can't establish a fresh date AND the URL belongs to a
    # regulator with reliable direct coverage (FSIS, FDA, CFIA), the result
    # is almost certainly archive content the direct scraper already saw
    # and dropped (or that pre-dates AFTS coverage). Drop to avoid letting
    # year-old recalls land in Pending with Date="" where they sit until
    # a reviewer manually rejects them. See module-level constant block
    # for the full rationale.
    if date_is_fallback and _is_reliable_direct_scraper_host(url):
        log.info(
            "%s: dropping fallback-date row from reliable-direct-scraper host: "
            "%s | %s",
            finder_name, urlparse(url).netloc, (title or "(no title)")[:80],
        )
        return None

    # ── Date sanity gate (audit 2026-05-06) ──────────────────────────────
    # Pre-fix production data showed 4 of 11 Tavily-sourced Pending rows
    # had wildly wrong dates: two 2018 USDA recalls stamped 2026-05-06,
    # one FSA-PRIN-05-2026 stamped 2026-06-11 (36 days FUTURE), one
    # 103-day-old Danone Ireland recall surfaced as "fresh".
    #
    # Drop rows where the date is implausible:
    #   • Older than today - 90 days  → archived recall, regular scrapers
    #     handled it long ago, no value re-discovering it now.
    #   • Newer than today + 30 days  → hallucinated future date (real
    #     recalls aren't published 30+ days in advance).
    # We allow the fallback ("today") to pass since the Notes tag already
    # flags it for claude-check Date enrichment.
    if not date_is_fallback:
        try:
            row_d = datetime.strptime(date_str, "%Y-%m-%d").date()
            today = datetime.utcnow().date()
            age_days = (today - row_d).days
            if age_days > 90:
                # Stale — drop. Don't pollute Pending with old recalls
                # the scrapers already covered when they were fresh.
                return None
            if age_days < -30:
                # Date is in the FUTURE by more than 30 days. Either
                # Tavily extracted a "next review" / "valid until" date
                # from the regulator page, or the extractor confused
                # year fields. Drop.
                return None
        except ValueError:
            # Couldn't parse our own output — shouldn't happen, but
            # don't crash the gap-finder if it does.
            pass

    notes_suffix = (
        f"  [via {finder_name} gap-finder, deterministic extract]"
        + (" [date-unknown: claude-check please verify Date]"
           if date_is_fallback else "")
    )

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
        Notes=(content[:300] + notes_suffix),
    )
    try:
        rec = rec.normalize()
    except Exception as e:
        log.warning("normalize failed for %s: %s", url, e)
        return None

    if not rec.Pathogen or rec.Pathogen in ("—", ""):
        return None
    return rec


def results_to_recalls(items: List[Dict[str, Any]],
                       finder_name: str = "Tavily") -> List[Recall]:
    out: List[Recall] = []
    dropped_no_hazard = 0
    for item in items:
        rec = _item_to_recall(item, finder_name=finder_name)
        if rec is None:
            dropped_no_hazard += 1
            continue
        out.append(rec)
    log.info("%s: %d items → %d valid recalls (dropped %d with no detectable hazard)",
             finder_name, len(items), len(out), dropped_no_hazard)
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
    # ── Gap-finder gating (audit 2026-04-29): tag fresh rows pending_gap
    # so merge_master holds them until 2× Gemini + Claude verify. Match
    # by ScrapedAt timestamp — tail-slice misses retried-rejected rows.
    from pipeline.merge_master import STATUS_PENDING_GAP
    tagged = 0
    for r in new_pending:
        if r.get("ScrapedAt") == scraped_at:
            r["Status"] = STATUS_PENDING_GAP
            tagged += 1
    log.info("Tavily gap-finder: added %d new rows to Pending "
             "(%d tagged Status=pending_gap)", added, tagged)

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
