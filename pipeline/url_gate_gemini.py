"""
Gemini final URL check — daily gatekeeper before the weekly report.

Replaces url_gate_claude.py. Claude was pattern-matching from training data and
hallucinating fiche IDs (e.g. RappelConso 17399, 17400, 17401 — all made up).
Gemini 2.5 Flash with native Google Search grounding fires real searches, so
every URL it returns is one that actually appeared in a search result.

Runs at 07:30 Athens, after:
  - 17:00 daily scrape (previous evening)
  - Various gap-finders earlier in the day
And before:
  - 08:00 Friday weekly report (~30 minutes later)

Scope: ALL rows currently in Pending (including previously-rejected ones — if
their URL is now live, they get a fresh evaluation).

GATE LOGIC PER PENDING ROW
==========================

  1. URL validator pre-check (cheap, no AI):
     • Structural reject — drop URLs containing /categorie/, /rubrik/, /tag/,
       /search?, or domain-only paths
     • HTTP probe — DELETE the row if URL is 404 / 410 / 5xx
     • Bot-blocked URLs on known gov domains (403) are kept

  2. PRE-PASS via regulator_apis.py — for any row whose Country has an open-data
     API/RSS, query the official source first. If the API returns a canonical
     URL that matches this row's date+brand+hazard, replace the URL immediately
     with no AI call needed.

  3. Gemini second-opinion with Google Search grounding on the survivors:
     The strict workflow prompt below encodes the 4 ChatGPT-discovered rules:
       • France RappelConso: append /Interne; verify the fiche-rappel ID via
         Google (scraper hallucinates sequential IDs 17399, 17400 etc.)
       • Greece EFET: numeric IDs and Greek slugs frequently hallucinated;
         must Google-verify against product name
       • USA FDA / USDA FSIS / Ireland FSAI: DO NOT TOUCH truncated URLs.
         These agency CMSes enforce strict slug length, so URLs that look
         "chopped" mid-word (e.g. ending in -because, -due, -gr) are the
         OFFICIAL working links.
       • All others: structural verify but don't aggressive-rewrite

  4. Promote to Recalls only rows that pass all 3 gates. Failures stay in
     Pending with the failure reason in Notes.

After all promotions: save xlsx, mirror json, commit, push.
"""
from __future__ import annotations
import os
import sys
import json
import re
import logging
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    promote_approved, sort_rows,
    save_xlsx_with_pending, mirror_json_from_xlsx,
    STATUS_REJECTED, STATUS_PENDING,
    STATUS_PENDING_GAP, STATUS_PENDING_GAP_V1, STATUS_PENDING_GAP_V2,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402
from review.url_validator import check_url, should_blank_url  # noqa: E402
from pipeline._url_year import is_year_mismatch  # noqa: E402
from pipeline._pathogen_scope import is_in_scope as is_tier1_pathogen  # noqa: E402
from pipeline._news_mirror_blocklist import is_news_mirror  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("gemini-url-gate")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
JSON_PATH = DATA_DIR / "recalls.json"

SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")

REQUIRED_FIELDS = ("Date", "Company", "Product", "Pathogen", "URL")
GEMINI_BATCH_SIZE = 10  # Gemini grounded calls are slower; smaller batches
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# Domains where /Interne suffix MUST be appended to fiche-rappel URLs
RAPPELCONSO_DOMAIN = "rappel.conso.gouv.fr"

# Domains where the agency CMS enforces strict slug length — DO NOT touch
# URLs that look truncated mid-word (FDA, FSIS, FSAI). These are the
# official working links per ChatGPT's verified-against-live-server diagnosis.
TRUNCATED_BUT_VALID_DOMAINS = (
    "fda.gov",
    "fsis.usda.gov",
    "fsai.ie",
)


# ─────────────────────────────────────────────────────────────────────────
# Step 1: Structural + HTTP URL validation
# ─────────────────────────────────────────────────────────────────────────
# News-outlet hosts. URLs on these domains are rejected at the gate before
# any Gemini call — saves tokens and provides defence-in-depth alongside
# the merge_master.py validate_pending_row() check (audit 2026-04-28).
_NEWS_HOSTS_GATE = frozenset({
    "foodsafetynews.com", "foodpoisonjournal.com", "foodpoisoningbulletin.com",
    "outbreaknewstoday.com", "cidrap.umn.edu", "food-safety.com", "barfblog.com",
    "foodbusinessnews.net", "foodnavigator.com", "foodnavigator-usa.com",
    "just-food.com", "foodmanufacture.co.uk", "foodprocessing.com",
    "foodengineeringmag.com", "fooddive.com",
    "reuters.com", "apnews.com", "bbc.com", "bbc.co.uk", "bloomberg.com",
    "theguardian.com", "nytimes.com", "washingtonpost.com",
    "medicalxpress.com", "sciencedaily.com",
    "yahoo.com", "msn.com", "news.google.com",
    "cbc.ca",  # CBC News — Canadian; covers recalls but not the regulator
})


def _is_news_host(url: str) -> bool:
    """True if URL host (or any parent domain) is a news outlet — these
    belong in the NEWS sheet, not Recalls."""
    if not url:
        return False
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return False
    if host.startswith("www."):
        host = host[4:]
    if host in _NEWS_HOSTS_GATE:
        return True
    for h in _NEWS_HOSTS_GATE:
        if host.endswith("." + h):
            return True
    return False


# Strings that should never appear in a real Company / Brand / Product —
# they're HTML/JS artifacts left over from a sloppy scrape.
_JS_HTML_ARTIFACTS = (
    "{socials", "window.", "querySelector", "&nbsp;",
    "<title>", "</title>", "[data-progress-bar]", "(function",
    "document.cookie", "addEventListener",
)


def _has_js_html_artifact(s: str) -> bool:
    if not s:
        return False
    return any(a in s for a in _JS_HTML_ARTIFACTS)


def _is_bare_domain(s: str) -> bool:
    """True if s is just a domain like 'canada.ca' or 'fda.gov' (no path)."""
    if not s or len(s) > 30 or " " in s.strip():
        return False
    s = s.strip().lower()
    # Single token containing a dot and a TLD-looking suffix
    if s.count(".") in (1, 2) and s.replace(".", "").replace("-", "").isalnum():
        suffix = s.rsplit(".", 1)[-1]
        if suffix in {"ca", "gov", "com", "org", "eu", "uk", "de", "fr",
                      "es", "it", "ch", "nl", "be", "ie", "au", "nz",
                      "jp", "kr", "cn", "tw", "hk", "sg", "in"}:
            return True
    return False


def _is_structurally_bad(url: str) -> Optional[str]:
    """Return a rejection reason if URL is structurally bad, else None."""
    if not url or not url.startswith("http"):
        return "not a URL"
    # News-outlet URLs belong in the NEWS sheet, not Recalls. Cheap pre-Gemini check.
    if _is_news_host(url):
        return "news outlet URL — belongs in NEWS sheet, not Recalls"
    bad_patterns = [
        "/categorie/", "/rubrik/", "/tag/", "/category/",
        "/search?", "/recherche?",
    ]
    for pat in bad_patterns:
        if pat in url:
            return f"listing/category URL ({pat})"
    p = urllib.parse.urlparse(url)
    segs = [s for s in (p.path or "").split("/") if s]
    if len(segs) == 0:
        return "domain only, no path"

    # ── RappelConso fiche-ID sanity (audit 2026-04-29) ──────────────────
    # Real URL shape: https://rappel.conso.gouv.fr/fiche-rappel/<int>/Interne
    # where <int> is a 4-or-5-digit fiche number assigned chronologically
    # (currently in the 22000s). The site also uses a separate reference
    # slug (e.g. "2026-04-0305") but the fiche-rappel/<...>/Interne path
    # requires the integer ID, NOT the slug.
    #
    # Two real-world failure modes the audit caught:
    #   1. Gemini hallucinated /fiche-rappel/2026/Interne (year as fid)
    #   2. Gemini used /fiche-rappel/2026-04-0305/Interne (reference slug
    #      stuffed into the integer-ID slot)
    # Both passed the previous \d+ regex fallback in api_prepass and were
    # silently re-stamped with "API fixed" audit notes.
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if host in ("rappel.conso.gouv.fr", "rappelconso.gouv.fr"):
        m_fid = re.search(r"/fiche-rappel/([^/]+)", url)
        if not m_fid:
            return "rappelconso URL missing fiche-rappel/<id>"
        fid = m_fid.group(1).strip()
        if not fid.isdigit():
            return (f"rappelconso fiche ID is not numeric ({fid!r}); "
                    "URL likely hallucinated")
        n = int(fid)
        if n < 1000:
            return f"rappelconso fiche ID too small ({n}); not a real fiche"
        if 2000 <= n <= 2100:
            return f"rappelconso fiche ID is a year value ({n}); URL hallucinated"

    return None


def _is_truncation_protected(url: str) -> bool:
    """True if URL is on FDA/FSIS/FSAI — the truncated-but-valid agencies."""
    if not url:
        return False
    p = urllib.parse.urlparse(url)
    host = (p.netloc or "").lower()
    return any(host.endswith(d) for d in TRUNCATED_BUT_VALID_DOMAINS)


def validate_urls(pending: List[Dict[str, Any]]) -> Tuple[List[int], List[int]]:
    """
    Returns (indices_to_delete, indices_still_alive).
    Dead = structurally bad OR HTTP says 404/410/5xx OR URL missing.
    Truncation-protected URLs are never structurally rejected.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    delete_idx: List[int] = []
    alive_idx: List[int] = []

    def _check(i: int) -> Tuple[int, Dict[str, Any], Optional[str]]:
        url = (pending[i].get("URL") or "").strip()
        # Truncation-protected: skip structural check entirely (FDA URLs ending
        # mid-word like ...recall-because-possible-health-risk are the real,
        # working links published by the agency CMS).
        if not _is_truncation_protected(url):
            struct_bad = _is_structurally_bad(url)
            if struct_bad:
                return i, {"reason": "structural", "detail": struct_bad}, struct_bad
        # Live HTTP probe
        return i, check_url(url), None

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = [ex.submit(_check, i) for i in range(len(pending))]
        for f in as_completed(futs):
            i, check, struct = f.result()
            pending[i]["_url_check"] = check
            url = (pending[i].get("URL") or "").strip()
            if struct:
                delete_idx.append(i)
            elif not url or should_blank_url(check):
                delete_idx.append(i)
            else:
                alive_idx.append(i)

    log.info("URL check: %d alive, %d to delete (dead/structural)",
             len(alive_idx), len(delete_idx))
    return sorted(delete_idx), sorted(alive_idx)


# ─────────────────────────────────────────────────────────────────────────
# Step 2: Pre-pass via official regulator APIs (no AI call)
# ─────────────────────────────────────────────────────────────────────────
def api_prepass(rows: List[Dict[str, Any]]) -> Dict[int, str]:
    """
    For each row whose Country has a known API, attempt to find the canonical
    URL via regulator_apis.py. Returns {row_index: corrected_url} for rows
    where the API succeeded.
    """
    fixes: Dict[int, str] = {}
    try:
        from pipeline.regulator_apis import repair_french_row, ALL_FETCHERS
    except ImportError as e:
        log.warning("regulator_apis not available — pre-pass skipped: %s", e)
        return fixes

    for i, row in enumerate(rows):
        country = str(row.get("Country") or "").strip()
        url = (row.get("URL") or "").strip()

        # France-specific: cheapest, most reliable
        if country == "France":
            new_url = repair_french_row(row)
            if new_url and new_url != url:
                fixes[i] = new_url
                continue

            # If France URL is missing /Interne suffix, append it (Rule 1)
            if (RAPPELCONSO_DOMAIN in url
                    and "/fiche-rappel/" in url
                    and not url.rstrip("/").endswith("/Interne")):
                # Strip any trailing slash and any prefix junk like /2026/
                m = re.search(r"/fiche-rappel/(\d+)", url)
                if m:
                    fid_str = m.group(1)
                    n = int(fid_str)
                    # Bounds check (audit 2026-04-29): \d+ alone matched
                    # year values like "2026" and silently legitimized
                    # hallucinated URLs. Real fiche IDs are 4-5 digits,
                    # currently in the 20000s. Reject anything that looks
                    # like a year (2000–2100) or that's implausibly small
                    # (<1000) — let those rows die at the gate instead of
                    # being re-stamped with a false "API fixed" audit note.
                    if n < 1000 or (2000 <= n <= 2100):
                        log.warning(
                            "Skipping bogus fiche-ID %s for row %d (%s) — "
                            "looks like a year or sentinel value, not a "
                            "real fiche number",
                            fid_str, i, (row.get("Brand") or "")[:40],
                        )
                        continue
                    fixes[i] = (
                        f"https://{RAPPELCONSO_DOMAIN}/fiche-rappel/"
                        f"{fid_str}/Interne"
                    )
                    continue

    log.info("API pre-pass corrected %d rows", len(fixes))
    return fixes


# ─────────────────────────────────────────────────────────────────────────
# Step 3: Gemini gate — strict prompt with Google Search grounding
# ─────────────────────────────────────────────────────────────────────────
GEMINI_GATE_SYSTEM = (
    "You are the final URL verifier for a public food-safety dashboard. "
    "You have Google Search. For every row, run real searches and verify "
    "that each URL points to a SPECIFIC recall detail page on the agency's "
    "official domain. NEVER guess fiche/recall IDs — they are chronological "
    "across all categories, not topical. After verification, you must also "
    "EXTRACT four fields directly from the recall page body (NOT from "
    "<title>/<h1>/breadcrumb/nav) and report them back. Return ONLY strict JSON."
)


GEMINI_GATE_PROMPT = """Audit each food-recall row below. For each one decide:
  1. Is the URL on the agency's OFFICIAL domain?
  2. Does the page actually describe THIS recall (date + brand + hazard match)?
  3. Is the URL a SPECIFIC recall page (not a category listing)?
  4. Can the four key fields be cleanly extracted from the page body?

═══ MANDATORY VERIFICATION + EXTRACTION WORKFLOW ═══

For EVERY row, do these steps:

STEP 1 — Compose Google query: "<agency> <brand-or-company> <date> <pathogen>"
         Example: "RappelConso Belle Henriette 2026-04-23 Listeria"
         Example: "FDA Saputo cottage cheese recall 2026-04"
STEP 2 — Run web_search with that query.
STEP 3 — Find the result on the AGENCY'S OFFICIAL DOMAIN whose snippet
         mentions the same date, brand, and hazard.
STEP 4 — Verify all four:
         ✓ date_match (the row's Date matches the recall's PUBLISHED
           DATE on the agency page, ±1 day. The published date is the
           date the agency posted the recall notice — NOT the article's
           CMS modification date or a copyright year in the footer.)
         ✓ brand_match (page mentions the brand or company)
         ✓ hazard_match (page mentions the pathogen)
         ✓ is_detail_page (URL is NOT /categorie/, /rubrik/, /tag/,
           /search?, or a bare domain. URL must HTTP-resolve — return
           pass=false with reason "url_dead" if the agency page no
           longer loads or returns 404/410/5xx)
STEP 5 — EXTRACT from the recall page BODY (not <title>, <h1>,
         breadcrumb, navigation, or footer):
         - company_name        : recalling firm — VERBATIM in source language
         - brand_name          : product brand — VERBATIM in source language
         - product_name        : product name — VERBATIM in source language
         - pathogen_or_hazard  : CANONICAL ENGLISH label (translate from
           source language: "Listerien" → "Listeria monocytogenes",
           "salmonelle" → "Salmonella", "ochratoxine" → "Ochratoxin",
           "Fremdkörper" → "Foreign body", "enthält Fleisch" →
           "Undeclared meat")
         If any field is not present in the page body, return "" for it.
         Never copy from page chrome (title/breadcrumb/nav).

STEP 6 — DETERMINE THE OUTBREAK FLAG (audit 2026-04-29 — STRICT RULE).

         An OUTBREAK means real human illness — at least one person has
         actually GOTTEN SICK from this hazard. NOT a precautionary recall,
         NOT a tampering/criminal-act seizure where products were pulled
         BEFORE consumption, NOT a routine surveillance test that found a
         pathogen in a sealed product nobody ate.

         Set "outbreak_verified": 1 ONLY if the recall notice OR a
         linked official public-health page states ONE OR MORE of:
            • specific number of confirmed/probable illnesses or cases
              ("166 illnesses", "two cases of listeriosis", "26 hospitalised")
            • the words "outbreak" / "épidémie" / "Ausbruch" / "brote" /
              "epidemia" / "surto" / "επιδημία" used to describe THIS
              specific hazard (not a generic precaution paragraph)
            • epidemiological investigation triggered by reported illness
              ("triggered by EODY epidemiological investigation",
              "linked to ongoing CFIA investigation into N illnesses")
            • death(s) attributed to the hazard
            • the recall is part of a NAMED ongoing outbreak with
              published case counts (e.g., "linked to the global pistachio
              Salmonella outbreak — 189 illnesses across 6 provinces")

         Set "outbreak_verified": 0 if any of these are true:
            • notice explicitly says "no reported illnesses",
              "no illnesses associated with this product",
              "aucun cas signalé", "keine Erkrankungen gemeldet"
            • routine sampling caught the contamination (lab test only,
              product not yet consumed, no illness link)
            • criminal tampering / extortion: jars seized before sale or
              by a single complainant; no consumption-linked illnesses
              (HiPP rat-poison Austria 2026-04-18 is the canonical
              example: jars seized, no one ate them, NOT outbreak)
            • the "outbreak" word appears only in the brand name, header,
              or boilerplate ("see our outbreak resources page")

         Default to 0 when in doubt. False positives on outbreak inflate
         risk and erode trust. The pathogen field already tells us a
         dangerous bacterium was detected; outbreak_verified separates
         "we caught it" from "people are sick."

         Also set "outbreak_evidence": short verbatim quote from the page
         that proves your decision (max 200 chars). "" if outbreak_verified=0.

STEP 7 — DETERMINE THE TIER (added 2026-05-04 — see docs/fsis_reviewer_prompts.md).

         FSIS uses the HYBRID framework: trust the regulator's own class
         when present, otherwise apply the FDA Class I/II/III framework
         based on outcome severity (NOT pathogen alone).

         Step 7a — Try the regulator's own classification first.
         If the row's "Class" field matches one of these, use the mapped Tier:

           → TIER 1: FDA "Class I", USDA FSIS "Class I", USDA FSIS
             "Public Health Alert", CFIA "Class 1", MAPAQ "Class 1",
             RappelConso "Mandatory recall (prefectural order)", EFET
             "Mandatory recall", MFDS "Public health alert", MFDS
             "Administrative action", BeaconBio "Outbreak", or any class
             containing "Mandatory" / "Class I" / "Class 1" / "Imperative" /
             "Impératif" / "Public Health Alert"

           → TIER 2: FDA "Class II", CFIA "Class 2", RappelConso
             "Voluntary", RappelConso "Recall", RASFF "Alert", FSA "Alert"
             / "Recall", FSAI "Recall", AESAN "Alert", AGES "Recall",
             BLV/BVL "Recall" / "Alert", FSANZ "Recall", MPI "Recall" /
             "Alert", Min. Salute "Alert", ANVISA "Alert", MSP "Advisory",
             CFS "Food Alert", CFIA "Recall", NCC "Recall", or any class
             containing just "Recall" / "Alert" / "Advisory" / "Conseillé"
             without severity qualifier

           → TIER 3: FDA "Class III", FDA "Advisory", CFIA "Class 3",
             CFIA "Notification", RASFF "Information" / "Border rejection"

         Step 7b — If no regulator class match, apply FDA framework:

           → TIER 1 (FDA Class I — death or serious adverse health):
             • Botulinum toxin, marine biotoxins, cereulide → ANY food
             • Listeria, STEC, Hep A, Norovirus → ANY food
             • Salmonella → READY-TO-EAT food (peanut butter, deli meat,
               raw nuts, ice cream, prepared salads, soft cheese, dry pet
               food, sprouts, frozen berries)
             • Heavy metals at toxic levels, rodenticide if consumed
             • Foreign body in baby food / vulnerable-population food
             • Confirmed outbreak (any pathogen, any food) — bumped via
               outbreak rule below

           → TIER 2 (FDA Class II — temporary or reversible):
             • Salmonella → COOKING-REQUIRED food (raw poultry, raw beef,
               raw pork, raw eggs in shell, ground meats, fresh poultry)
             • Campylobacter, Yersinia, Vibrio, Cyclospora, Cronobacter,
               generic (non-STEC) E. coli, vegetative B. cereus, Brucella,
               Shigella, Staph enterotoxin, histamine
             • Mycotoxins above action level (aflatoxin, ochratoxin, etc.)
             • Foreign body in adult food

           → TIER 3 (FDA Class III — unlikely to cause harm):
             • Labeling violations, net-weight discrepancies
             • Quality issues with no safety component
             • Pathogen in environmental swab only (not product)
             • Unknown pathogen / empty pathogen field (default)

         Step 7c — Apply Outbreak bump rule:
             final_tier = max(1, base_tier - 1)   if outbreak_verified == 1
             final_tier = base_tier               if outbreak_verified == 0

         HARD RULES — never override:
           H1  Cereulide is ALWAYS Tier 1
           H2  Botulinum is ALWAYS Tier 1
           H3  STEC / E. coli O157 / VTEC / EHEC is ALWAYS Tier 1
           H4  Listeria monocytogenes in ANY food is ALWAYS Tier 1
               (overrides any regulator downgrade — regulator misclassification)
           H5  Marine biotoxins are ALWAYS Tier 1
           H6  Salmonella in RTE food is Tier 1 (FDA Class I)
           H7  Salmonella in cooking-required food is Tier 2 (Class II)
           H8  Generic "E. coli" without STEC indicator is Tier 3
           H9  Bacillus cereus alone is Tier 3 (cereulide is the Tier 1 path)
           H13 Foreign body in baby food / vulnerable population → Tier 1
           H14 Foreign body in adult food, no injuries → Tier 3
           H15 When regulator marks Class I / Class 1 / Mandatory /
               Public Health Alert, that IS Tier 1
           H16 When regulator class disagrees with FDA framework but H1-H5
               don't apply, regulator wins

         Set "tier_verified" to the computed final_tier (1, 2, or 3).
         Set "tier_method" to one of:
             "regulator_class" — matched the regulator class table
             "fda_framework"   — applied FDA framework (no regulator class)
             "hybrid"          — regulator class present but bumped via outbreak
         Set "tier_mismatch": true if the row's existing Tier ≠ tier_verified.

═══ HARD REJECT REASONS ═══

Set pass=false with one of these reasons (in priority order):

[REJECT — news_not_recall]
  URL host is a news outlet (foodsafetynews.com, food-safety.com,
  foodpoisonjournal.com, outbreaknewstoday.com, cidrap.umn.edu, cbc.ca,
  reuters.com, apnews.com, bbc.com, etc.). News sites cover recalls but
  are not regulator announcements. Belongs in the NEWS sheet, not Recalls.

[REJECT — extraction_failed]
  Any one of:
  • company_name == brand_name byte-for-byte AND > 5 words long
    (real companies and brands are rarely identical multi-word strings;
    this signals the scraper grabbed the page <title> for both fields)
  • product_name is just a bare domain ("canada.ca", "fda.gov", "fsis.usda.gov")
  • Any of the four extracted fields contains JS/HTML artifacts:
    "{{socials", "window.", "querySelector", "&nbsp;", "<title>",
    "</title>", "[data-progress-bar]", "(function", "document.cookie",
    "addEventListener"

[REJECT — date_pre_2026]
  Row's Date is before 2026-01-01. FSIS scope is 2026 onward only.

[REJECT — url_year_mismatch]
  URL clearly encodes a year AND that year is < 2026 OR differs from the
  row's Date year by more than 1 year. URL year patterns we recognise:
    • RappelConso new: /fiche-rappel/YYYY-MM-NNNN/
    • produktwarnung.eu: /YYYY/MM/DD/
    • FDA: F-NNNN-YYYY
    • FSIS: /recalls/...-YYYY
    • Generic /YYYY/ with non-digit boundary
  CRITICAL: numeric-only fiche IDs like /fiche-rappel/22019/Interne are
  sequential IDs, NOT years. Do not extract years from those, or from
  PDF filenames, or from random embedded digits.

[REJECT — news_mirror_domain]
  URL is on a news-mirror or aggregator domain (sedaily.com, reuters.com,
  beaconbio.org, ilfattoalimentare.it, foodsafetynews.com, etc.) — even
  if the row's Source label claims a regulator. These are not official
  sources.

[REJECT — pathogen_out_of_scope]
  Pathogen field is empty OR not in FSIS Tier-1 scope. Tier-1 scope:
    • Listeria monocytogenes
    • Salmonella (any serovar)
    • Shiga toxin-producing E. coli / STEC / O157 / O104 / O121 / O26 / etc.
    • Clostridium botulinum / botulism
    • Bacillus cereus / cereulide
    • Aflatoxin / Ochratoxin / mycotoxins (any)
    • Cronobacter sakazakii
    • Hepatitis A / Norovirus
    • Staphylococcus aureus enterotoxin
    • Campylobacter
  Anything else (Histamine, Marine biotoxin, Foreign body, allergens,
  heavy metals, sulfites, rodenticides, PFAS, etc.) → reject as out of scope.

[REJECT — extraction_garbage]
  Page extraction returned a homepage or landing page, not a recall.
  Indicators:
    • Company == Brand AND value is a generic word ("Home", "Index",
      "Page", "Recalls", "Alerts", "Welcome", "Main")
    • Product matches the agency name (Source="NAFDAC", Product="NAFDAC")
    • URL ends in /home/, /index/, /main/, /welcome/
  Reject these — they are scraper artifacts, not real recalls.

[COMPANY/BRAND — recalling firm rule]
  CRITICAL: Company is the RECALLING FIRM (manufacturer, packer, "Source
  of the record" / "Marque de salubrité" / brand chip on the page). It
  is NOT the distributor or retailer.

  Distributor lists in Notes (Carrefour, Monoprix, Intermarché, Grand
  Frais, Leclerc, Auchan, etc.) are RESELLERS, NOT the recalling firm.

  Example RappelConso fiche (salmon verrines case):
    Page shows:
      Source of the record: OCEAN DELIGHTS
      Brand chip: Ocean Delights
      Product: 4 verrines aux 2 saumons - 160g
      Distributors (in Notes): Carrefour, Intermarché, Leclerc...
    EXTRACT:
      company_name = "OCEAN DELIGHTS"
      brand_name   = "Ocean Delights"
      product_name = "4 verrines aux 2 saumons - 160g"
    DO NOT extract:
      company_name = "Carrefour"     ← WRONG: that's a distributor

  RASFF (EU) SPECIAL CASE — Source contains "RASFF":
    company_name = "<origin country 3-letter ISO code>" (e.g. "GRC", "DEU")
    brand_name   = "<destination countries, 3-letter ISO, comma-separated>"
                   (e.g. "FRA, DEU, ITA")
    If origin unknown, set company_name="UNK".

  If the page truly has no recalling firm extractable: leave company_name
  and brand_name empty. The row stays in Pending with
  Status=needs_extraction for retry on next run.

═══ AGENCY-SPECIFIC RULES ═══

[RULE 1 — France RappelConso]
  Domain: rappel.conso.gouv.fr
  • URL MUST end with /Interne (e.g. /fiche-rappel/22114/Interne)
  • Scraper frequently hallucinates sequential IDs (17394, 17399, 17400, 17401).
    Do NOT trust the existing ID — verify it via Google Search every time.
  • Reject any URL containing /categorie/.
  • Use the open-data API at data.economie.gouv.fr/.../rappelconso0 if you can
    construct the query — `lien_vers_la_fiche_rappel` is the canonical URL.

[RULE 2 — Greece EFET]
  Domain: efet.gr
  • Numeric item IDs and Greek-to-English slugs are frequently hallucinated.
  • Always Google-verify by product name. Google query example:
    "EFET ανάκληση <product-greek-or-translated> <date>"
  • The correct URL pattern is:
    https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-cat/item/NNNN-...

[RULE 3 — USA FDA / USDA FSIS / Ireland FSAI — DO NOT TOUCH]
  Domains: fda.gov, fsis.usda.gov, fsai.ie
  • These agency content-management systems enforce strict URL-slug length.
  • URLs that LOOK truncated mid-word (ending in "-because", "-due",
    "-possible-health-risk", "-gr") are the OFFICIAL, working links
    published by the agency itself.
  • If a URL is on these domains, set verification.is_detail_page=true and
    do NOT propose a "fix" unless the URL is structurally a category page
    (/recalls/ alone, /alerts/ alone, /search?).
  • Pass these through with confidence=0.95 and strategy="agency-cms-truncated".

[RULE 4 — All other agencies]
  • Verify URL is on the agency's official domain
  • Verify URL is a specific recall page, not an index/listing
  • If the URL fails verification, propose a corrected URL ONLY if your
    Google search returned a verified match
  • If no verified match, set pass=false with reason "needs manual review"
  • NEVER guess

═══ INPUT (ROWS) ═══

{rows_json}

═══ OUTPUT (STRICT JSON) ═══

{{
  "decisions": [
    {{
      "row_index": <int>,
      "pass": true|false,
      "url_corrected": "<canonical URL or null if no fix>",
      "verification": {{
        "date_match": true|false,
        "brand_match": true|false,
        "hazard_match": true|false,
        "is_detail_page": true|false
      }},
      "extracted": {{
        "company_name": "<from page body or empty>",
        "brand_name": "<from page body or empty>",
        "product_name": "<from page body or empty>",
        "pathogen_or_hazard": "<from page body or empty>"
      }},
      "outbreak_verified": 0|1,
      "outbreak_evidence": "<short verbatim quote proving the verdict, '' if 0>",
      "google_query_used": "<the query you actually ran>",
      "confidence": 0.0-1.0,
      "strategy": "agency-cms-truncated | api-lookup | search-verified | failed",
      "reason": "<short — one of: news_not_recall | extraction_failed | date_pre_2026 | url_year_mismatch | news_mirror_domain | pathogen_out_of_scope | extraction_garbage | empty_company_or_brand>"
    }}
  ]
}}

Include EVERY input row in decisions. If you cannot verify a row, set
pass=false and url_corrected=null. NEVER fabricate URLs.
"""


def _missing_required(row: Dict[str, Any]) -> List[str]:
    missing = []
    for f in REQUIRED_FIELDS:
        v = row.get(f)
        if v is None or (isinstance(v, str) and not v.strip()) or v == "—":
            missing.append(f)
    return missing


def _collect_gemini_keys() -> List[str]:
    """Key collection with FREE-tier preference (audit 2026-04-29).

    The URL gate runs once daily with ~9 batches × ~5K tokens. That's
    ~30 calls/day total — well under the gemini-2.5-flash free-tier daily
    cap of 250 req/day. We use a SECOND free-tier key (GEMINI_API_KEY_-
    FREE_2) so the gate doesn't compete with the gap-finder cascade for
    the primary free-tier key's daily quota. Falls back to paid keys if
    the free key is rate-limited.

    Order:
      1. GEMINI_API_KEY_FREE_2  (gate's dedicated free-tier key)
      2. GEMINI_API_KEY_FREE_3..5  (extra free-tier keys, if any)
      3. GEMINI_API_KEY  (paid, fallback)
      4. GEMINI_API_KEY_1..10  (additional paid keys, fallback)

    To switch to free-tier-only operation:
      • Create a 2nd free-tier API key (different Google account from the
        primary GEMINI_API_KEY_FREE used by the gap-finder cascade)
      • Set GEMINI_API_KEY_FREE_2 in GitHub Secrets
      • This module picks it up automatically. Cost on this line drops
        from ~€3/mo to €0.
    """
    keys: List[str] = []

    # ── Free-tier keys first (URL gate's dedicated quota) ────────────────
    free_2 = os.getenv("GEMINI_API_KEY_FREE_2")
    if free_2 and free_2.strip():
        keys.append(free_2.strip())
    for i in range(3, 6):  # FREE_3..FREE_5
        v = os.getenv(f"GEMINI_API_KEY_FREE_{i}")
        if v and v.strip() not in keys:
            keys.append(v.strip())

    # ── Paid keys as fallback ───────────────────────────────────────────
    legacy = os.getenv("GEMINI_API_KEY")
    if legacy and legacy.strip() not in keys:
        keys.append(legacy.strip())
    for i in range(1, 11):
        v = os.getenv(f"GEMINI_API_KEY_{i}")
        if v and v.strip() not in keys:
            keys.append(v.strip())
    return keys


def _strip_fences(txt: str) -> str:
    """Robust JSON extraction — survives prose preamble and code fences.

    Gemini 2.5 Flash with Google Search grounding has a strong tendency to
    emit narrative preamble before the JSON ("Step 1: Process row 0...").
    Pre-2026-05-08 this killed every batch with a JSONDecodeError on the
    first character. The new strategy:

      1. Strip any ``` fences.
      2. Strip any <think>...</think> blocks (DeepSeek R1 style — harmless
         on Gemini, useful when this helper is reused elsewhere).
      3. Find the first '{' and walk forward respecting string literals
         to the matching '}'. Return that substring. Anything before
         the '{' (prose) and after the '}' (trailing notes) is dropped.
      4. If no balanced object is found, return the original string and
         let json.loads raise — the caller logs the problem.
    """
    if not txt:
        return txt
    t = txt.strip()
    # ``` fences
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*\n", "", t)
        t = re.sub(r"\n```\s*$", "", t)
    # <think> blocks (DeepSeek R1)
    t = re.sub(r"<think>.*?</think>", "", t, flags=re.DOTALL)
    t = t.strip()

    # Walk to find the first balanced JSON object.
    start = t.find("{")
    if start < 0:
        return t  # no object — caller will error appropriately
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(t)):
        c = t[i]
        if esc:
            esc = False
            continue
        if c == "\\":
            esc = True
            continue
        if c == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return t[start:i + 1]
    # Unbalanced — return tail starting from first '{' for best-effort parse
    return t[start:]


def _call_gemini_grounded(prompt: str, system: str,
                           max_tokens: int = 8000) -> Optional[str]:
    """Single Gemini call with Google Search grounding enabled.
    Returns text response or None on failure.

    Uses the new google-genai SDK (replaces deprecated google.generativeai).
    Install: pip install google-genai
    """
    try:
        from google import genai  # type: ignore
        from google.genai import types  # type: ignore
    except ImportError:
        log.warning("google-genai not installed; Gemini gate disabled. "
                    "Install with: pip install google-genai")
        return None

    keys = _collect_gemini_keys()
    if not keys:
        log.warning("No GEMINI_API_KEY(_1..10) set — Gemini gate disabled")
        return None

    last_error: Optional[Exception] = None
    for api_key in keys:
        try:
            client = genai.Client(api_key=api_key)

            config = types.GenerateContentConfig(
                system_instruction=system if system else None,
                tools=[types.Tool(google_search=types.GoogleSearch())],
                max_output_tokens=max_tokens,
                temperature=0.1,
            )

            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=config,
            )

            text = (getattr(resp, "text", None) or "").strip()
            if text:
                # Log search activity for cost monitoring
                try:
                    if hasattr(resp, "candidates") and resp.candidates:
                        gm = getattr(resp.candidates[0],
                                     "grounding_metadata", None)
                        if gm:
                            queries = getattr(gm, "web_search_queries", []) or []
                            if queries:
                                log.info("Gemini ran %d Google searches",
                                         len(queries))
                except Exception:
                    pass
                return text
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            log.debug("Gemini attempt failed (%s): %s",
                      type(exc).__name__, str(exc)[:150])
            continue

    log.warning("Gemini gate: all keys failed. Last error: %s", last_error)
    return None


def gemini_gate(rows: List[Dict[str, Any]]) -> Dict[int, Tuple[bool, str, Optional[str]]]:
    """
    Ask Gemini (with grounding) to pass/fail each row and propose URL fixes.
    Returns {input_index: (pass, reason, url_corrected_or_None)}.
    Falls back to deterministic required-field check if Gemini disabled.
    """
    decisions: Dict[int, Tuple[bool, str, Optional[str]]] = {}

    # Deterministic baseline — always compute
    det_fail: Dict[int, str] = {}
    for i, r in enumerate(rows):
        miss = _missing_required(r)
        if miss:
            det_fail[i] = f"Missing required: {', '.join(miss)}"

    if not _collect_gemini_keys():
        log.info("Gemini disabled — using deterministic check only")
        for i in range(len(rows)):
            if i in det_fail:
                decisions[i] = (False, det_fail[i], None)
            else:
                decisions[i] = (True, "ok (gemini disabled)", None)
        return decisions

    # Batch-call Gemini
    for start in range(0, len(rows), GEMINI_BATCH_SIZE):
        chunk = rows[start:start + GEMINI_BATCH_SIZE]
        batch_view = [{
            "row_index": j,
            "Date":     r.get("Date", ""),
            "Source":   r.get("Source", ""),
            "Company":  r.get("Company", ""),
            "Brand":    r.get("Brand", ""),
            "Product":  r.get("Product", ""),
            "Pathogen": r.get("Pathogen", ""),
            "Reason":   r.get("Reason", ""),
            "Country":  r.get("Country", ""),
            "URL":      r.get("URL", ""),
        } for j, r in enumerate(chunk)]

        prompt = GEMINI_GATE_PROMPT.format(
            rows_json=json.dumps(batch_view, ensure_ascii=False, indent=2),
        )
        txt = _call_gemini_grounded(prompt, system=GEMINI_GATE_SYSTEM)

        got_response = False
        if txt:
            try:
                data = json.loads(_strip_fences(txt))
                for d in data.get("decisions", []):
                    j = d.get("row_index")
                    if j is None or j < 0 or j >= len(chunk):
                        continue
                    real_idx = start + j
                    passed = bool(d.get("pass"))
                    url_fix = d.get("url_corrected") or None
                    # Local verification gate — never trust low confidence
                    confidence = float(d.get("confidence", 0.0) or 0.0)
                    v = d.get("verification", {})
                    if passed and v:
                        all_ok = all([
                            v.get("date_match"), v.get("brand_match"),
                            v.get("hazard_match"), v.get("is_detail_page"),
                        ])
                        if not all_ok or confidence < 0.6:
                            passed = False
                            d["reason"] = (str(d.get("reason", ""))
                                           + f" [local gate rejected: conf={confidence:.2f}]")
                    # Local extraction-failed sanity check on the extracted block.
                    # Defence in depth — never trust Gemini's self-rejection alone.
                    # Triggers match Layer B HARD REJECT REASONS in the prompt.
                    if passed:
                        ex = d.get("extracted", {}) or {}
                        co = str(ex.get("company_name", "") or "")
                        br = str(ex.get("brand_name", "") or "")
                        pr = str(ex.get("product_name", "") or "")
                        ha = str(ex.get("pathogen_or_hazard", "") or "")
                        ext_fail_reason = None
                        if (co and co == br
                                and len(co.split()) > 5):
                            ext_fail_reason = ("extraction_failed: "
                                               "company == brand identically (>5 words)")
                        elif _is_bare_domain(pr):
                            ext_fail_reason = ("extraction_failed: "
                                               f"product is bare domain '{pr}'")
                        elif any(_has_js_html_artifact(s) for s in (co, br, pr, ha)):
                            ext_fail_reason = ("extraction_failed: "
                                               "JS/HTML artifact in extracted field")
                        if ext_fail_reason:
                            passed = False
                            d["reason"] = (str(d.get("reason", ""))
                                           + f" [local gate: {ext_fail_reason}]")
                    # Reject structurally-bad url_corrected
                    if url_fix and not _is_truncation_protected(url_fix):
                        if _is_structurally_bad(url_fix):
                            url_fix = None
                            passed = False

                    # ── Pull the new outbreak verdict (audit 2026-04-29) ──
                    # Gemini returns 0/1 for outbreak_verified plus a short
                    # evidence quote. We carry both into the decision tuple
                    # so the gate-apply step can update Outbreak in the row.
                    # Only treat as a definitive verdict when Gemini gave us
                    # a non-empty evidence quote. Empty evidence → leave the
                    # existing scraper-set Outbreak value alone.
                    ob_raw = d.get("outbreak_verified")
                    ob_evidence = str(d.get("outbreak_evidence") or "").strip()
                    outbreak_verdict: Optional[int]
                    if ob_raw is None or ob_evidence == "":
                        outbreak_verdict = None
                    else:
                        try:
                            outbreak_verdict = 1 if int(ob_raw) == 1 else 0
                        except (TypeError, ValueError):
                            outbreak_verdict = None

                    decisions[real_idx] = (passed,
                                            str(d.get("reason") or "")[:300],
                                            url_fix,
                                            outbreak_verdict,
                                            ob_evidence[:200],
                                            # Audit 2026-05-08: carry Gemini's
                                            # page-extracted values to the
                                            # apply step. Gemini reads the URL
                                            # and pulls these from page text;
                                            # the apply step fills any empty
                                            # / garbage fields on the row so
                                            # claude-check sees clean data.
                                            d.get("extracted") or {})
                got_response = True
            except json.JSONDecodeError as e:
                log.warning("Gemini JSON parse failed: %s | %s", e, txt[:200])

        # Fill any uncovered rows with deterministic fallback.
        #
        # Audit 2026-05-07 — flipped default from PASS to FAIL on rows
        # where Gemini did not return a verdict (JSON parse error, network
        # silence, or row skipped in returned JSON). This is the original
        # audit bug #1 root cause: a JSONDecodeError left got_response=False
        # and the fallback silent-passed every row in the chunk, which let
        # the gap-gating state machine advance pending_gap_v0 → v1 → v2
        # without any actual Gemini verification. A bad row needs only two
        # parse failures to land in claude-check as if it had been double-
        # verified by Gemini.
        #
        # New semantics: no Gemini verdict = FAIL. The row goes into
        # rejected_flags and merge_master will mark it Status=rejected
        # via the standard path. "Once rejected, stays rejected" (per the
        # 2026-05-07 architectural fix) is the correct behavior here —
        # if Gemini was unreachable for a row the operator can manually
        # clear the Status field once Gemini is back.
        for j in range(len(chunk)):
            real_idx = start + j
            if real_idx in decisions:
                continue
            # Fallback tuples — when Gemini didn't give us a verdict on
            # outbreak we leave None so the apply step doesn't change
            # whatever the scraper set.
            if real_idx in det_fail:
                decisions[real_idx] = (False,
                                       det_fail[real_idx] + " (gemini unreached)",
                                       None, None, "", {})
            elif not got_response:
                decisions[real_idx] = (False,
                                       "FAIL: gemini parse error (safety default — audit 2026-05-07)",
                                       None, None, "", {})
            else:
                decisions[real_idx] = (False,
                                       "FAIL: gemini silent on this row (safety default — audit 2026-05-07)",
                                       None, None, "", {})

    passes = sum(1 for v in decisions.values() if v[0])
    fixed = sum(1 for v in decisions.values() if v[2] is not None)
    outbreak_set = sum(1 for v in decisions.values() if v[3] is not None)
    log.info("Gemini gate: %d pass, %d fail, %d URL fixes, "
             "%d outbreak verdicts across %d rows",
             passes, len(decisions) - passes, fixed, outbreak_set, len(rows))
    return decisions


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────
def main() -> int:
    t0 = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info("Gemini URL gate (final gatekeeper) started: %s",
             t0.strftime("%Y-%m-%dT%H:%M:%SZ"))

    approved = load_existing(XLSX_PATH) if XLSX_PATH.exists() else []
    pending = load_pending(XLSX_PATH) if XLSX_PATH.exists() else []
    log.info("State: %d approved + %d pending", len(approved), len(pending))

    if not pending:
        log.info("Nothing in Pending — nothing to do. Exiting cleanly.")
        return 0

    # ── Step 1: Structural + HTTP URL check, delete dead URLs ──────────
    delete_idx, alive_idx = validate_urls(pending)
    if delete_idx:
        log.info("Deleting %d rows with dead/structural URLs", len(delete_idx))
    alive_rows = [pending[i] for i in alive_idx]

    if not alive_rows:
        log.info("No surviving rows to gate")
        final_pending_out: List[Dict[str, Any]] = []
        new_approved: List[Dict[str, Any]] = []
    else:
        # ─── AUDIT 2026-05-07 — REMOVED rejection-reset bypass ──────────────
        #
        # Previous code (lines deleted below) reset every Status=rejected
        # row back to Status=pending at the start of each gate run, with
        # the comment "Reset rejected status — fresh evaluation". This was
        # a SILENT BYPASS of claude-check rejections.
        #
        # Concrete failure case (2026-05-06):
        #   Tavily injected 2 fabricated "2026-05-06" rows for old USDA
        #   FSIS recalls (Envolve Foods Oct 2018, Target Corp Mar 2018).
        #   claude-check fetched the pages, returned date_corrected=2018,
        #   triggered the date_pre_2026 hard rule → FAIL → Status=rejected.
        #   Next url_gate_gemini run flipped Status=rejected → pending.
        #   Gemini was disabled in env, so deterministic-only check ran;
        #   URL was alive, no re-rejection. Next merge_master promoted
        #   both fabricated rows to Recalls with Date=2026-05-06.
        #
        # The Phase A rejection counter was also bypassed: it tracks
        # RejectedBy across runs to delete after 2 different-reviewer
        # rejects. Resetting Status=rejected zeros that counter silently.
        #
        # Fix: leave Status=rejected alone. Once rejected, stays rejected.
        # If a row was rejected in error, the operator can manually clear
        # the Status field in the xlsx Pending sheet, or delete the row.
        # Automated "fresh evaluation" is removed.
        # ────────────────────────────────────────────────────────────────────

        # ── Step 2: API pre-pass (cheapest, deterministic, no AI) ──────
        api_fixes = api_prepass(alive_rows)
        for j, new_url in api_fixes.items():
            old_url = alive_rows[j].get("URL", "")
            alive_rows[j]["URL"] = new_url
            notes = (alive_rows[j].get("Notes") or "").strip()
            audit = (f"[url-gate {datetime.now(timezone.utc).strftime('%Y-%m-%d')}: "
                     f"API fixed {old_url[:40]}... -> {new_url[:40]}...]")
            alive_rows[j]["Notes"] = (notes + " " + audit).strip()[:500]

        # ── Step 3: Gemini gate with Google Search grounding ───────────
        decisions = gemini_gate(alive_rows)

        # Apply Gemini-proposed URL fixes + outbreak verdicts + page-extracted enrichment
        outbreak_changes = 0
        enrichment_changes = 0
        for j, decision in decisions.items():
            # Decision tuple is now 6-element: (passed, reason, url_fix,
            # outbreak_verdict, outbreak_evidence, extracted). Older tuples
            # were 5- or 3-element — handle all for safety during rollout.
            if len(decision) == 6:
                passed, reason, url_fix, outbreak_verdict, ob_evidence, extracted = decision
            elif len(decision) == 5:
                passed, reason, url_fix, outbreak_verdict, ob_evidence = decision
                extracted = {}
            else:
                passed, reason, url_fix = decision[:3]
                outbreak_verdict, ob_evidence, extracted = None, "", {}

            # URL fix
            if url_fix and url_fix != alive_rows[j].get("URL"):
                old_url = alive_rows[j].get("URL", "")
                alive_rows[j]["URL"] = url_fix
                notes = (alive_rows[j].get("Notes") or "").strip()
                audit = (f"[url-gate {datetime.now(timezone.utc).strftime('%Y-%m-%d')}: "
                         f"Gemini fixed {old_url[:40]}... -> {url_fix[:40]}...]")
                alive_rows[j]["Notes"] = (notes + " " + audit).strip()[:500]

            # ── Page-extracted enrichment (audit 2026-05-08) ───────────────
            # Gemini reads the URL with Google Search grounding and pulls
            # Company / Brand / Product / Pathogen from page content. When
            # the row's existing value is empty or known-garbage, write
            # Gemini's extracted value in. This is the fix for the V1-broken
            # RappelConso rows where Company is blank but the page clearly
            # names the recalling firm — Gemini sees it, we now apply it
            # before claude-check runs, claude-check passes the row, and it
            # promotes correctly.
            #
            # We only fill, never overwrite a non-empty non-garbage value:
            # if the scraper got it right, we trust it. Gemini corrections
            # to known-wrong values happen via the verdict=fail / corrected
            # path in claude-check, not here.
            if extracted and isinstance(extracted, dict):
                _PLACEHOLDERS = {
                    "", "—", "-", "unbranded", "sans marque", "n/a", "na",
                    "none", "various", "multiple", "various brands",
                    "marque inconnue", "no brand",
                }
                _GARBAGE = {
                    "home", "index", "page", "recalls", "alerts", "alert",
                    "recall", "welcome", "main", "press release", "food",
                    "food incident post", "aesan", "fsai",
                }
                # ── Source-authority guard (audit 2026-05-15) ──────────
                # Gemini sometimes returns the regulator/portal name as
                # the Company when it can't find the recalling firm on
                # the page header (e.g. for RappelConso fid 22229 it
                # returned company_name="RappelConso" — the agency name
                # baked into the page chrome, not the firm). The agency
                # name is NEVER a valid Company/Brand value, so we
                # reject any new_val that matches the row's Source or
                # one of the known source-authority names below.
                _SOURCE_AUTHORITIES = {
                    # Open-data / regulator portal names
                    "rappelconso", "rappel conso", "rappel.conso",
                    "fda", "u.s. food and drug administration",
                    "food and drug administration",
                    "usda", "usda fsis", "fsis", "food safety and inspection service",
                    "rasff", "rasff window", "rasff portal",
                    "fsai", "food safety authority of ireland",
                    "fsa", "fsa uk", "food standards agency",
                    "fss", "food standards scotland",
                    "fsanz", "food standards australia new zealand",
                    "cfia", "canadian food inspection agency",
                    "aesan", "agencia española de seguridad alimentaria",
                    "efet", "efsa", "ages", "afsca", "favv",
                    "bvl", "bfr", "mfds", "mhlw",
                    "anses", "dgccrf", "dgal",
                    "salute", "ministero della salute",
                    "nvwa", "voedsel waren autoriteit",
                    "mattilsynet", "livsmedelsverket", "ruokavirasto",
                    "fodevarestyrelsen", "moph", "moccae",
                    "signalconso", "signal conso",
                    "european commission", "commission européenne",
                }

                def _matches_source(v: str, row: dict) -> bool:
                    """True iff v matches the row's Source value or any
                    known regulator/portal name. Strips country codes
                    like '(FR)' or '/ DGCCRF' so 'RappelConso' matches
                    Source='RappelConso (FR)'."""
                    if not v:
                        return False
                    needle = (v or "").strip().lower()
                    if not needle:
                        return False
                    if needle in _SOURCE_AUTHORITIES:
                        return True
                    src = str(row.get("Source") or "").lower()
                    # strip "(FR)" / "(EU)" / " / DGCCRF" style decorations
                    src_clean = re.sub(r"\s*[\(/].*$", "", src).strip()
                    if src_clean and (needle == src_clean or src_clean in needle):
                        return True
                    return False

                def _is_empty_or_garbage(v: str) -> bool:
                    s = (v or "").strip().lower()
                    return s in _PLACEHOLDERS or s in _GARBAGE or len(s) < 2

                # Pairs: (row_field, extracted_key)
                _ENRICH = (
                    ("Company",  "company_name"),
                    ("Brand",    "brand_name"),
                    ("Product",  "product_name"),
                    ("Pathogen", "pathogen_or_hazard"),
                )
                for row_field, ex_key in _ENRICH:
                    new_val = str(extracted.get(ex_key, "") or "").strip()
                    if not new_val or len(new_val) < 2:
                        continue
                    # Source-authority guard: only for Company / Brand —
                    # Product and Pathogen are not at risk of confusion
                    # with regulator names.
                    if row_field in ("Company", "Brand"):
                        if _matches_source(new_val, alive_rows[j]):
                            log.warning(
                                "gemini-enrich rejected agency-as-%s "
                                "for URL=%s: Gemini returned %r which "
                                "matches Source=%r — leaving field empty "
                                "for downstream review",
                                row_field,
                                str(alive_rows[j].get("URL", ""))[:60],
                                new_val,
                                alive_rows[j].get("Source"),
                            )
                            continue
                    cur_val = str(alive_rows[j].get(row_field, "") or "")
                    if _is_empty_or_garbage(cur_val) and not _is_empty_or_garbage(new_val):
                        alive_rows[j][row_field] = new_val
                        enrichment_changes += 1
                        notes = (alive_rows[j].get("Notes") or "").strip()
                        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                        audit = (f"[gemini-enrich {today}: {row_field} "
                                 f"{cur_val!r}→{new_val[:60]!r}]")
                        alive_rows[j]["Notes"] = (notes + " " + audit).strip()[:500]

            # ── Outbreak verdict (audit 2026-04-29) ─────────────────────
            # Apply ONLY if Gemini gave us a non-None verdict AND the
            # value differs from what's in the row. Stamp an audit note
            # with the verbatim evidence quote so future debugging can
            # trace why the flag flipped.
            if outbreak_verdict is not None:
                current = alive_rows[j].get("Outbreak")
                # Coerce current to int (Outbreak may be 0/1, "0"/"1", or None)
                try:
                    current_int = int(current) if current is not None else None
                except (TypeError, ValueError):
                    current_int = None
                if current_int != outbreak_verdict:
                    alive_rows[j]["Outbreak"] = outbreak_verdict
                    outbreak_changes += 1
                    notes = (alive_rows[j].get("Notes") or "").strip()
                    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    audit = (f"[outbreak {today}: {current_int}→{outbreak_verdict} — "
                             f"{ob_evidence[:120]}]")
                    alive_rows[j]["Notes"] = (notes + " " + audit).strip()[:500]
        if outbreak_changes:
            log.info("Outbreak field updated on %d rows by Gemini verdict",
                     outbreak_changes)
        if enrichment_changes:
            log.info("Page-extracted enrichment applied to %d field(s) across rows",
                     enrichment_changes)

        # Translate to rejected_flags for promote_approved
        rejected_flags: Dict[int, str] = {}
        for j in range(len(alive_rows)):
            decision = decisions.get(j, (True, "ok", None, None, "", {}))
            passed = decision[0]
            reason = decision[1] if len(decision) > 1 else "ok"
            if not passed:
                rejected_flags[j] = f"Gemini gate: {reason}"
                continue

            # Post-decision sanity checks (locked 2026-04-30) — catch
            # rows the model approved but that violate hard rules.
            row = alive_rows[j]
            url = row.get("URL", "") or ""
            pathogen = row.get("Pathogen", "") or ""

            # 1. News-mirror domain — hard reject
            if is_news_mirror(url):
                rejected_flags[j] = "news_mirror_domain"
                continue

            # 2. URL year vs Date year mismatch
            try:
                row_date = (datetime.fromisoformat(str(row.get("Date",""))[:10]).date()
                            if row.get("Date") else None)
            except (TypeError, ValueError):
                row_date = None
            year_issue = is_year_mismatch(row_date, url)
            if year_issue:
                rejected_flags[j] = f"url_year_mismatch: {year_issue}"
                continue

            # 3. Pathogen out of FSIS Tier-1 scope
            if not is_tier1_pathogen(pathogen):
                rejected_flags[j] = f"pathogen_out_of_scope: {pathogen!r}"
                continue

            # 4. Extraction garbage (Company=Brand=generic word)
            company = str(row.get("Company") or "").strip().lower()
            brand = str(row.get("Brand") or "").strip().lower()
            GARBAGE = {"home","index","page","recalls","alerts","alert",
                       "recall","welcome","main"}
            if company and company == brand and company in GARBAGE:
                rejected_flags[j] = f"extraction_garbage: Company=Brand={company!r}"
                continue
            if re.search(r"/(home|index|main|welcome)/?$", url.lower()):
                rejected_flags[j] = "extraction_garbage: URL is landing page"
                continue

        # ── Gap-finder gating state machine advance (audit 2026-04-29) ──
        # pending_gap → v1 (one Gemini pass), pending_gap_v1 → v2 (two
        # passes). pending_gap_v2 is NOT advanced here — claude_check is
        # the one that flips v2 → "pending". Two independent Gemini runs
        # required (different sampling, different network conditions)
        # before Claude is even consulted.
        gap_advances = {"v0_to_v1": 0, "v1_to_v2": 0}
        today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for j in range(len(alive_rows)):
            if j in rejected_flags:
                continue  # let promote_approved mark it rejected
            row = alive_rows[j]
            cur = (row.get("Status") or "").strip()
            new_status = None
            if cur == STATUS_PENDING_GAP:
                new_status = STATUS_PENDING_GAP_V1
                gap_advances["v0_to_v1"] += 1
            elif cur == STATUS_PENDING_GAP_V1:
                new_status = STATUS_PENDING_GAP_V2
                gap_advances["v1_to_v2"] += 1
            if new_status:
                row["Status"] = new_status
                notes = (row.get("Notes") or "").strip()
                tag = f"[gap-gate {today_iso}: {cur} → {new_status}]"
                row["Notes"] = (notes + " " + tag).strip()[:1000]
        if any(gap_advances.values()):
            log.info("Gap-gating advanced: %d (pending_gap → v1), %d (v1 → v2)",
                     gap_advances["v0_to_v1"], gap_advances["v1_to_v2"])

        # ARCHITECTURAL RULE (locked 2026-04-30):
        # url_gate_gemini.py is verify-and-tag-only. It NEVER promotes
        # rows to Recalls. Promotion only happens via merge_master.py
        # AFTER claude_check.py has also passed.
        # Rows that pass this gate stay in Pending with their advanced
        # Status (pending_gap_v1 / pending_gap_v2 / pending) for Claude.
        new_approved = []           # nothing promoted from this gate
        final_pending_out = alive_rows

    # ── Assemble final state + save ────────────────────────────────────
    final_approved = sort_rows(approved)   # Recalls untouched by this gate
    final_pending = sort_rows(final_pending_out)

    save_xlsx_with_pending(final_approved, final_pending, XLSX_PATH)
    mirror_json_from_xlsx(XLSX_PATH, JSON_PATH)

    # ── Rebuild daily briefs for every date that gained new rows ───────
    # When the gate promotes Pending → Recalls, the daily HTML brief for
    # those dates must be regenerated so the dashboard's DAILY tab reflects
    # the new state. Without this, briefs only refresh at the next 10:00
    # daily-recall-search run.
    files_to_commit: List[str] = ["docs/data/recalls.xlsx",
                                   "docs/data/recalls.json"]
    rebuilt_briefs: List[str] = []
    if new_approved:
        try:
            from pipeline.daily_recall_search import (
                render_daily_html, update_daily_index, DAILY_DIR,
            )
            from scrapers._models import Recall as _Recall
            # Group newly-promoted rows by Date
            from collections import defaultdict
            by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for r in new_approved:
                d = str(r.get("Date") or "").strip()[:10]
                if d:
                    by_date[d].append(r)

            # Build a fast-lookup map of all Recalls by date so we render
            # the FULL day, not only the newly-promoted rows.
            full_by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for r in final_approved:
                d = str(r.get("Date") or "").strip()[:10]
                if d:
                    full_by_date[d].append(r)

            from datetime import date as _date
            for date_str in sorted(by_date.keys(), reverse=True):
                try:
                    y, m, d = date_str.split("-")
                    target = _date(int(y), int(m), int(d))
                except (ValueError, AttributeError):
                    log.warning("Skip brief rebuild — bad date '%s'", date_str)
                    continue
                # Convert dicts to Recall objects (the renderer expects these)
                day_rows = full_by_date.get(date_str, [])
                recalls_objs: List[_Recall] = []
                for row in day_rows:
                    try:
                        recalls_objs.append(_Recall(**{
                            k: (v if v is not None else "")
                            for k, v in row.items()
                            if k in _Recall.__annotations__
                        }))
                    except Exception as cce:
                        log.debug("skip row coerce: %s", cce)
                # Render and persist
                try:
                    render_daily_html(target, recalls_objs)
                    update_daily_index(target, recalls_objs)
                    brief_path = f"docs/daily/{date_str}.html"
                    rebuilt_briefs.append(brief_path)
                    files_to_commit.append(brief_path)
                    log.info("Rebuilt daily brief for %s (%d rows)",
                             date_str, len(recalls_objs))
                except Exception as rerr:
                    log.warning("Brief rebuild failed for %s: %s",
                                date_str, rerr)
            # daily-index.json always gets included if we updated any briefs
            if rebuilt_briefs:
                files_to_commit.append("docs/daily-index.json")
        except ImportError as ie:
            log.warning("Cannot import brief renderer (%s) — skipping "
                        "daily brief rebuild", ie)

    # ── Commit ─────────────────────────────────────────────────────────
    if not SKIP_COMMIT:
        ok = git_commit_and_push(
            repo_dir=ROOT,
            files=files_to_commit,
            message=(f"FSIS Gemini URL-gate {t0.strftime('%Y-%m-%d')} "
                     f"(+{len(new_approved)} approved, "
                     f"-{len(delete_idx)} dead, "
                     f"{len(final_pending)} pending"
                     + (f", rebuilt {len(rebuilt_briefs)} briefs"
                        if rebuilt_briefs else "")
                     + ")"),
        )
        if not ok:
            log.error("Git push failed")
            return 1

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info("=" * 60)
    log.info("DONE in %.1fs | +%d approved | -%d dead | %d pending | "
             "%d briefs rebuilt",
             elapsed, len(new_approved), len(delete_idx), len(final_pending),
             len(rebuilt_briefs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
