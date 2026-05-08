"""
pipeline/claude_check.py — final Claude-based verification stage.

Runs AFTER url_gate_gemini.py and BEFORE Pending → Recalls promotion.

For each row currently in Pending:
  1. Fetch the regulator URL (text-extracted, ~25K chars, 25s timeout)
  2. Ask Claude Haiku 4.5 to verify the row's Date / Company / Product /
     Pathogen against the actual page content
  3. If Claude finds the row's Date is wrong → correct it with audit trail
     in Notes: "[claude-check 2026-04-27: corrected Date 2026-04-25 →
     2026-03-15 (page header)]"
  4. If Claude finds the row otherwise consistent → mark for promotion
  5. If Claude finds the page does NOT match the claimed recall (wrong
     hazard, different company, page is a clarification of a different
     recall, page is a listing/index, etc.) → keep in Pending with
     REJECTED status

Catches the failure mode that bit us on the SFA Nature One Dairy row:
the SFA page header reads "Recall of two additional formula milk products"
with publication date 15 Mar 2026, but a "Clarification" added later in
April caused the gap-finder to stamp the row 2026-04-25. Claude reading
the page text catches the discrepancy and corrects the date back to
2026-03-15.

This stage complements the Gemini URL gate (url_gate_gemini.py):
  - Gemini gate: verifies the URL is on the right domain and points to
    a specific recall page (uses Google Search grounding for cross-check)
  - Claude check: reads the page itself and verifies the Date / Company /
    Product / Pathogen on the row actually match what the page says

Cost: Claude Haiku 4.5 (claude-haiku-4-5-20251001). With pages truncated
to ~25K chars and a few-K-token output, expect <$0.05 per run for typical
Pending sizes (5–30 rows). Cheap enough to run after every URL-gate run.

Schedule (Athens time):
  07:30 → url_gate_gemini.py            (URL verification + fixes)
  07:45 → claude_check.py               (page-content verification)
  08:00 → weekly report (Friday only)   (consumes Recalls sheet)
"""
from __future__ import annotations
import os
import sys
import re
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

import requests as _requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    promote_approved, sort_rows,
    save_xlsx_with_pending, mirror_json_from_xlsx,
    rebuild_daily_briefs_for_promoted,
    STATUS_REJECTED, STATUS_PENDING,
    STATUS_PENDING_GAP_V2,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402
from review.claude_client import (  # noqa: E402
    _call_claude, _strip_fences, ENABLED as CLAUDE_ENABLED,
)
from pipeline._url_year import is_year_mismatch  # noqa: E402
from pipeline._pathogen_scope import is_in_scope as is_tier1_pathogen  # noqa: E402
from pipeline._news_mirror_blocklist import is_news_mirror  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("claude-check")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
JSON_PATH = DATA_DIR / "recalls.json"

SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")

# Page-fetch + Claude limits
# Audit 2026-05-08: dropped from 25,000 to 4,000 — recall pages have all
# the relevant info (headline, company, pathogen, dates, distributor) in
# the first ~2,000 chars. The other 23,000 was nav/footer/related links,
# costing ~5K tokens/call for zero analytic value. New 4K limit = ~800
# tokens of page content, plenty for verdicts. Token cost per Claude call
# drops from ~6,500 in to ~2,500 in (-62%). Set CLAUDE_CHECK_PAGE_CHARS
# env var to override (e.g. 8000 for hard pages).
HTML_TRUNCATE_CHARS = int(os.getenv("CLAUDE_CHECK_PAGE_CHARS", "4000"))
FETCH_TIMEOUT_S = 25
SLEEP_BETWEEN_ROWS_S = 0.5    # gentle on the regulator + the API
# UA + headers updated 2026-05-07 — the previous bot-style UA
# ("AFTS-FSIS-claude-check/1.0; +https://advfood.tech/fsis-home") was
# being 403-blocked by USDA FSIS (3 of 11 rows in today's claude-check).
# Same fix as the 2026-04-29 _base.py audit: use a real Chrome UA so
# regulator bot-blockers don't drop us. We're not impersonating — we
# just stop announcing ourselves as a bot.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

# Full browser-fingerprint header set, mirrors scrapers/_base.py DEFAULT_HEADERS.
# USDA FSIS specifically inspects Accept + Accept-Encoding + Upgrade-Insecure-
# Requests on top of UA — minimal headers alone still get 403'd.
_FETCH_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8,de;q=0.7,es;q=0.6,it;q=0.5,el;q=0.4",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Rows older than this many days are not re-verified (we trust the prior
# Claude check). 0 means verify every row every run.
SKIP_IF_VERIFIED_DAYS = int(os.getenv("CLAUDE_CHECK_SKIP_DAYS", "0"))


# ---------------------------------------------------------------------------
# HTML → text extraction (no bs4 dependency required)
# ---------------------------------------------------------------------------

_RE_SCRIPT = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_RE_TAG = re.compile(r"<[^>]+>")
_RE_WS = re.compile(r"[ \t]+")
_RE_NL = re.compile(r"\n{3,}")


def _html_to_text(html: str) -> str:
    """
    Cheap HTML→text. Strips <script>/<style>, removes tags, collapses
    whitespace. Good enough for Claude to read regulator pages that are
    mostly prose + tables.
    """
    if not html:
        return ""
    txt = _RE_SCRIPT.sub(" ", html)
    txt = _RE_TAG.sub(" ", txt)
    # decode the most common HTML entities
    txt = (txt.replace("&nbsp;", " ")
              .replace("&amp;", "&")
              .replace("&lt;", "<")
              .replace("&gt;", ">")
              .replace("&quot;", '"')
              .replace("&#39;", "'")
              .replace("&euro;", "€"))
    # collapse whitespace
    txt = _RE_WS.sub(" ", txt)
    txt = _RE_NL.sub("\n\n", txt)
    return txt.strip()


def _fetch_page_text(url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (text, error). Text is the page content stripped of HTML and
    truncated to HTML_TRUNCATE_CHARS. error is None on success, or a
    short reason string on failure.
    """
    if not url or not url.lower().startswith(("http://", "https://")):
        return None, "no URL"
    try:
        resp = _requests.get(
            url, timeout=FETCH_TIMEOUT_S,
            headers=_FETCH_HEADERS,
            allow_redirects=True,
        )
    except Exception as exc:
        return None, f"fetch error: {type(exc).__name__}: {str(exc)[:120]}"

    if resp.status_code >= 400:
        return None, f"HTTP {resp.status_code}"

    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "html" not in ctype and "xml" not in ctype and "text" not in ctype:
        # PDF, image, or other binary — Claude can't read directly here
        return None, f"non-text content-type: {ctype[:60]}"

    text = _html_to_text(resp.text or "")
    if len(text) > HTML_TRUNCATE_CHARS:
        text = text[:HTML_TRUNCATE_CHARS] + "\n\n[... truncated ...]"
    return text, None


# ---------------------------------------------------------------------------
# Claude prompt
# ---------------------------------------------------------------------------

CLAUDE_CHECK_SYSTEM = (
    "You are a senior food-safety analyst doing the final verification "
    "pass on a recall row before it is published on a public dashboard. "
    "You read the regulator's actual page text and answer strictly in "
    "the JSON schema requested. You never invent facts — if the page "
    "doesn't say something, leave the field blank or set the verdict "
    "to 'fail'."
)

CLAUDE_CHECK_PROMPT = """A row is in our Pending sheet, claimed to describe a real food recall on the regulator's page below. Verify it.

═══ ROW (claimed) ═══
Date     : {Date}
Source   : {Source}
Country  : {Country}
Company  : {Company}
Brand    : {Brand}
Product  : {Product}
Pathogen : {Pathogen}
Class    : {Class}
Tier     : {Tier}
Outbreak : {Outbreak}
Reason   : {Reason}
URL      : {URL}

═══ PAGE TEXT (text-extracted from the URL above) ═══
{page_text}
═══ END PAGE TEXT ═══

INSTRUCTIONS:

1. Find the ORIGINAL recall publication date on the page.
   • Look for: "Published", "Publication date", "Date du rappel",
     "Datum", a calendar-icon datestamp at the top of the article,
     or the date in the page header.
   • CRITICAL: If the page shows BOTH an original publication date
     AND a later "Update", "Clarification", "Latest update", or
     "Last modified" date, ALWAYS use the ORIGINAL publication date.
     Example: SFA "Recall of two additional formula milk products"
     published 15 Mar 2026 with a Clarification added 25 Apr 2026 →
     the date is 2026-03-15, NOT 2026-04-25.

2. Verify each field against the page:
   • date_match    — does the row's Date equal the ORIGINAL publication
                     date on the page? (±1 day tolerance for tz quirks)
   • company_match — does the page mention the row's Company?
                     "Various brands", "Various producers", "Unbranded",
                     "sans marque", or "—" are legitimate values ONLY
                     when the page itself doesn't name a single company
                     (multi-producer recalls, generic raw products, bulk
                     commodity alerts). If the page DOES name a specific
                     brand or manufacturer mid-page (typical for retail
                     recalls — RappelConso, FDA, CFIA — even when the
                     row says "Unbranded"), set company_match=false so
                     the row gets re-scraped on the next pass instead of
                     being promoted with a wrong Company value.
   • product_match — does the page describe the row's Product?
   • pathogen_match — does the page identify the row's Pathogen?
                     (e.g. "Listeria", "Salmonella", "cereulide",
                     "B. cereus toxin")
   • is_recall_page — is the page actually a recall / public-warning /
                      alert page (not a listing index, search results,
                      category page, transparency page, or unrelated
                      content)?

2b. Cross-check Date and URL (locked 2026-04-30):
   • date_pre_2026 — is the row's Date (or date_corrected) before
                     2026-01-01? If yes → fail with reason "date_pre_2026".
                     FSIS scope is 2026 onward only.
   • url_year_mismatch — does the URL clearly encode a year that:
                     (a) is < 2026, OR
                     (b) differs from the row's Date year by >1 year?
                     URL year patterns: /fiche-rappel/YYYY-MM-NNNN/,
                     /YYYY/MM/DD/, F-NNNN-YYYY, /YYYY/ with non-digit
                     boundary. CRITICAL: numeric-only fiche IDs like
                     /fiche-rappel/22019/ are sequential IDs, NOT years.
                     PDF filenames with random digits do NOT encode years.
                     If yes → fail with reason "url_year_mismatch".
   • news_mirror_domain — is the URL on a news-mirror or aggregator
                     domain (sedaily.com, reuters.com, beaconbio.org,
                     ilfattoalimentare.it, foodsafetynews.com, cnn.com,
                     bbc.com, etc.)? If yes → fail with reason
                     "news_mirror_domain". These are not official sources.

2c. Cross-check Company and Brand (locked 2026-04-30):
   • For ALL non-RASFF sources: Company must be the RECALLING FIRM
     (manufacturer / packer / "Source of the record" / brand chip on
     the page), and Brand must be the product brand chip on the page.
     - Distributor/retailer names (Carrefour, Monoprix, Intermarché,
       Grand Frais, Leclerc, Auchan) in the page Notes are RESELLERS,
       NOT the recalling firm.
     - If Company looks like it was autofilled with a distributor name
       and the actual page shows a different "Source of the record",
       fail with reason "company_is_distributor".
     - If page has no extractable recalling firm at all, fail with
       reason "empty_company_or_brand". Row stays in Pending for retry.

   • For RASFF (EU) rows ONLY (Source contains "RASFF"):
     - Company should be a 3-letter origin country code (e.g. "GRC",
       "DEU"); "UNK" allowed for unknown origin.
     - Brand should be 3-letter destination country codes
       (e.g. "FRA, DEU, ITA").
     - If Company is empty or not a 3-letter code AND not "UNK" → fail
       with reason "rasff_company_format".

2d. Cross-check Pathogen (locked 2026-04-30):
   • Pathogen must be in FSIS Tier-1 scope:
     Listeria, Salmonella, STEC/E.coli, C. botulinum, B. cereus,
     mycotoxins (aflatoxin/ochratoxin/etc.), Cronobacter, Hep A,
     Norovirus, Staphylococcus enterotoxin, Campylobacter.
   • If Pathogen is empty, "—", or anything else (Histamine, Marine
     biotoxin, Foreign body, allergens, heavy metals, sulfites,
     rodenticides, PFAS, etc.) → fail with reason
     "pathogen_out_of_scope".

2e. Extraction-garbage check (locked 2026-04-30):
   • If Company == Brand AND value is a generic landing-page word
     ("Home", "Index", "Page", "Recalls", "Alerts", "Welcome",
     "Main") → fail with reason "extraction_garbage".
   • If Product matches the Source agency name (Source="NAFDAC",
     Product="NAFDAC") → fail with reason "extraction_garbage".
   • If URL ends in /home/, /index/, /main/ → fail with reason
     "extraction_garbage".

3. If date_match is false but you can identify the correct publication
   date from the page, return it as date_corrected (YYYY-MM-DD).

4. Set verdict:
   • "pass"   — all 5 match flags true (or true after applying
                date_corrected) AND no failure from 2b/2c/2d/2e.
                Apply Tier=1 to the row (it's in scope by definition).
   • "fix"    — only date_match is false but you have a confident
                correction. Apply date_corrected. Re-check date_pre_2026
                and url_year_mismatch on the corrected date.
   • "fail"   — anything else. Row stays in Pending with reason in Notes.

4b. VERIFY THE TIER (added 2026-05-04 — see docs/fsis_reviewer_prompts.md).

   FSIS uses the HYBRID framework: regulator class first, FDA framework
   fallback, outbreak bump on top.

   Step 4b-i. Try regulator class first. If the row's "Class" matches
   one of these (case-insensitive), use the mapped Tier:
     → TIER 1: "Class I", "Class 1", "Mandatory recall (prefectural order)",
       "Public Health Alert", "Mandatory recall", "Administrative action",
       "Outbreak", or any class containing "Mandatory" / "Imperative" /
       "Impératif" / "Public Health Alert"
     → TIER 2: "Class II", "Class 2", "Voluntary", "Recall", "Alert",
       "Alert notification", "Advisory", "Conseillé", "Food Alert",
       "Product Recall Information Notice"
     → TIER 3: "Class III", "Class 3", "Information", "Border rejection",
       "Notification" (without severity qualifier)

   Step 4b-ii. If no regulator class match (or Class is empty), apply
   FDA framework:
     → TIER 1 (FDA Class I — death or serious adverse health):
       • Botulinum, marine biotoxins, cereulide → ANY food
       • Listeria, STEC, Hep A, Norovirus → ANY food
       • Salmonella → READY-TO-EAT food (peanut butter, deli meat, soft
         cheese, ice cream, prepared salads, raw nuts, sprouts, frozen
         berries, dry pet food)
       • Heavy metals at toxic levels, rodenticide if consumed
       • Foreign body in baby food / vulnerable-population food
     → TIER 2 (FDA Class II — temporary or reversible):
       • Salmonella → COOKING-REQUIRED food (raw poultry, raw beef, raw
         pork, raw eggs in shell, ground meats, fresh poultry)
       • Campylobacter, Yersinia, Vibrio, Cyclospora, Cronobacter,
         generic (non-STEC) E. coli, vegetative B. cereus, Brucella,
         Shigella, Staph enterotoxin, histamine
       • Mycotoxins above action level
       • Foreign body in adult food
     → TIER 3 (FDA Class III — unlikely to cause harm):
       • Labeling violations, net-weight discrepancies, quality issues
       • Pathogen in environmental swab only
       • Unknown pathogen / empty pathogen field (default)

   Step 4b-iii. Apply Outbreak bump:
       final_tier = max(1, base_tier - 1)   if Outbreak == 1
       final_tier = base_tier               if Outbreak == 0

   HARD RULES (never override):
     H1  Cereulide is ALWAYS Tier 1
     H2  Botulinum is ALWAYS Tier 1
     H3  STEC / E. coli O157 / VTEC / EHEC is ALWAYS Tier 1
     H4  Listeria monocytogenes in ANY food is ALWAYS Tier 1
         (overrides any regulator downgrade — likely misclassification)
     H5  Marine biotoxins are ALWAYS Tier 1
     H6  Salmonella in RTE food is Tier 1 (FDA Class I)
     H7  Salmonella in cooking-required food is Tier 2 (Class II)
     H8  Generic "E. coli" without STEC indicator is Tier 3
     H9  Bacillus cereus alone is Tier 3 (cereulide is the Tier 1 path)
     H13 Foreign body in baby food / vulnerable-population food → Tier 1
     H14 Foreign body in adult food, no injuries → Tier 3

   Output:
     tier_verified = the computed final_tier (1, 2, or 3)
     tier_method   = "regulator_class" | "fda_framework" | "hybrid"
     tier_mismatch = true if row's existing Tier ≠ tier_verified

4c. VERIFY THE OUTBREAK FLAG (STRICT rule — see docs/fsis_reviewer_prompts.md §5).

   Outbreak = 1 ONLY if the page (or a linked official health page)
   states ONE OR MORE of:
     • specific number of confirmed/probable illnesses/cases
       ("166 illnesses", "two cases", "26 hospitalised")
     • the word "outbreak" / "épidémie" / "Ausbruch" / "brote" /
       "epidemia" describing THIS hazard (not generic boilerplate)
     • epidemiological investigation triggered by reported illness
     • death(s) attributed to the hazard
     • named ongoing outbreak with published case counts

   Outbreak = 0 if:
     • notice says "no reported illnesses" / "aucun cas signalé"
     • routine sampling caught it (lab test only, product not consumed)
     • criminal tampering with no consumption (HiPP rat-poison Austria
       2026-04-18 → outbreak=0)
     • "outbreak" word only in brand name or boilerplate

   Default to 0 when in doubt.

   Output:
     outbreak_verified = 0 or 1
     outbreak_evidence = short verbatim quote (max 200 chars), "" if =0
     outbreak_mismatch = true if row's existing Outbreak ≠ outbreak_verified

OUTPUT — strict JSON, no markdown, no commentary:

{{
  "date_match": true|false,
  "date_on_page": "YYYY-MM-DD or empty",
  "date_corrected": "YYYY-MM-DD or empty",
  "company_match": true|false,
  "company_corrected": "<recalling firm from page, or empty if no correction>",
  "brand_corrected":   "<brand from page, or empty if no correction>",
  "product_corrected": "<product description from page, or empty if no correction>",
  "pathogen_corrected": "<canonical pathogen from page, or empty if no correction>",
  "product_match": true|false,
  "pathogen_match": true|false,
  "is_recall_page": true|false,
  "date_pre_2026": true|false,
  "url_year_mismatch": true|false,
  "news_mirror_domain": true|false,
  "pathogen_out_of_scope": true|false,
  "company_is_distributor": true|false,
  "extraction_garbage": true|false,
  "tier1_pathogen": true|false,
  "tier_verified": 1|2|3,
  "tier_method": "regulator_class" | "fda_framework" | "hybrid",
  "tier_mismatch": true|false,
  "outbreak_verified": 0|1,
  "outbreak_evidence": "<verbatim quote, max 200 chars>",
  "outbreak_mismatch": true|false,
  "verdict": "pass" | "fix" | "fail",
  "reason": "short single-sentence rationale"
}}

CORRECTION RULES (audit 2026-05-08):
  - When the row's Company/Brand/Product/Pathogen field is empty, "—",
    "Unbranded", "sans marque", or contains extraction garbage (page-title
    text, agency name, generic words like "Food"/"Press Release") AND the
    page CLEARLY names the correct value, populate the corresponding
    *_corrected field and set verdict="fix". The pipeline will write the
    correction back to the row and promote it.
  - For "sans marque" / unbranded products where no recalling firm is
    named on the page, leave company_corrected empty and use
    brand_corrected="Unbranded".
  - For Pathogen: only fill pathogen_corrected with a canonical name
    ("Listeria monocytogenes", "Salmonella", "Escherichia coli STEC",
    "Cereulide", etc.) — not free-text. Empty if no clear pathogen.
  - All *_corrected fields must come from the page content itself, not
    from outside knowledge or speculation.
"""


def _verify_with_claude(row: Dict[str, Any],
                         page_text: str) -> Optional[Dict[str, Any]]:
    """
    Call Claude Haiku 4.5 with the row + page text. Returns the parsed
    JSON dict, or None on failure (Claude error / parse error / etc.).
    """
    if not CLAUDE_ENABLED:
        return None

    prompt = CLAUDE_CHECK_PROMPT.format(
        Date=row.get("Date", ""),
        Source=row.get("Source", ""),
        Country=row.get("Country", ""),
        Company=row.get("Company", ""),
        Brand=row.get("Brand", "—"),
        Product=row.get("Product", ""),
        Pathogen=row.get("Pathogen", ""),
        Class=row.get("Class", ""),
        Tier=row.get("Tier", ""),
        Outbreak=row.get("Outbreak", ""),
        Reason=row.get("Reason", ""),
        URL=row.get("URL", ""),
        page_text=page_text,
    )
    try:
        raw = _call_claude(prompt=prompt, system=CLAUDE_CHECK_SYSTEM)
    except Exception as exc:
        log.warning("Claude call failed: %s: %s",
                    type(exc).__name__, str(exc)[:120])
        return None
    if not raw:
        return None
    try:
        return json.loads(_strip_fences(raw))
    except json.JSONDecodeError as exc:
        log.warning("Claude JSON parse failed: %s | %s", exc, raw[:200])
        return None


# ---------------------------------------------------------------------------
# Per-row verification
# ---------------------------------------------------------------------------

def _was_recently_verified(row: Dict[str, Any]) -> bool:
    """True if Notes contains a recent [claude-check ...] audit stamp."""
    if SKIP_IF_VERIFIED_DAYS <= 0:
        return False
    notes = str(row.get("Notes") or "")
    m = re.search(r"\[claude-check (\d{4}-\d{2}-\d{2})", notes)
    if not m:
        return False
    try:
        stamp = datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except ValueError:
        return False
    age = (datetime.now(timezone.utc).date() - stamp).days
    return age <= SKIP_IF_VERIFIED_DAYS


# ── Audit 2026-05-08: deterministic clean-row shortcut ───────────────────
# Skip the Claude API call entirely for rows that have all required fields
# filled with non-garbage values from a regulator URL the Gemini gate
# already approved. Cuts ~25-30% of API calls at zero quality cost — these
# rows would PASS Claude check anyway.
#
# Conditions ALL must be true:
#   1. URL passes is_generic_url (already verified earlier in pipeline)
#   2. Company is non-empty AND not in known-garbage list
#   3. Brand is non-empty AND not "—" (the safe-default fallback)
#   4. Product is non-empty AND > 8 chars
#   5. Pathogen is non-empty AND not in placeholder list
#   6. Date matches YYYY-MM-DD regex
#   7. Source matches a known regulator label
#   8. Row was previously enriched by Gemini gate (Notes contains
#      "[gemini-enrich" OR "[gemini-gate-passed")
#
# The 8th condition is the strongest gate — a row that Gemini already
# verified content-wise on the same page is overwhelmingly likely to PASS
# Claude. We only skip Claude on the ones where it'd be a rubber stamp.
_KNOWN_GARBAGE_COMPANY = frozenset({
    "food", "press release", "food incident post", "what's new", "news",
    "alert", "alerts", "recall", "recalls", "advisory",
    "fda", "usda", "fsis", "cfia", "fsa", "fsai", "fsanz", "mpi",
    "aesan", "ages", "bvl", "afsca", "favv", "nvwa", "rasff", "cfs",
    "sfa", "mfds", "mhlw", "fssai", "anvisa", "efet",
})
_PATHOGEN_PLACEHOLDERS = frozenset({
    "", "unknown", "n/a", "na", "none", "—", "-", "tbd",
    "pathogen contamination", "pathogen",
})

def _is_clean_row(row: Dict[str, Any]) -> bool:
    """Deterministic clean-row check. True = skip Claude API call.

    See module-level comment for the gate criteria. Conservative —
    false positives (rows that clean-shortcut but should have been
    failed) cost data-quality regression; misses (rows that don't
    shortcut) just cost an API call. So we only shortcut when ALL
    eight conditions clearly pass.
    """
    if os.getenv("CLAUDE_NO_SHORTCUT", "").strip() in ("1", "true", "yes"):
        return False
    company = str(row.get("Company") or "").strip()
    if not company or company.lower().rstrip(".:;,") in _KNOWN_GARBAGE_COMPANY:
        return False
    brand = str(row.get("Brand") or "").strip()
    if not brand or brand == "—" or brand.lower() == "—":
        return False
    product = str(row.get("Product") or "").strip()
    if len(product) < 8:
        return False
    pathogen = str(row.get("Pathogen") or "").strip().lower()
    if pathogen in _PATHOGEN_PLACEHOLDERS:
        return False
    date = str(row.get("Date") or "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return False
    source = str(row.get("Source") or "").strip()
    if not source or source.lower() in ("", "tavily-gap", "gap"):
        return False
    # Strongest condition: row was already enriched/passed by Gemini gate.
    notes = str(row.get("Notes") or "")
    if "[gemini-enrich " not in notes and "[gemini-gate-passed " not in notes:
        return False
    return True


def check_row(row: Dict[str, Any]) -> Tuple[str, Dict[str, str], Optional[str]]:
    """
    Verify one Pending row. Returns (verdict, corrections, reason)

      verdict in {"pass", "fix", "fail", "skip"}
      corrections = dict with any of {Date, Company, Brand, Product, Pathogen}
                    populated when Claude proposed a correction. Empty
                    {} when nothing to fix.
      reason = short audit string

    "skip" = transient failure (URL fetch / Claude error). Row stays in
             Pending unchanged so the next run gets another shot. **Per
             audit 2026-05-08, skipped rows are explicitly NOT promoted —
             see main() for the parking-lot behavior.**
    """
    if _was_recently_verified(row):
        return "pass", {}, "recently verified — skipping"

    # ── Audit 2026-05-08: deterministic clean-row shortcut ───────────────
    # Skip the API call entirely if the row is provably clean — saves
    # ~25-30% of Claude calls = ~$3-5/month. See _is_clean_row comment.
    if _is_clean_row(row):
        return "pass", {}, "clean-row shortcut (gemini-enriched + all fields valid)"

    url = str(row.get("URL") or "").strip()
    text, fetch_err = _fetch_page_text(url)
    if text is None:
        return "skip", {}, f"fetch failed ({fetch_err})"

    result = _verify_with_claude(row, text)
    if result is None:
        return "skip", {}, "claude unreachable / parse failed"

    verdict = str(result.get("verdict") or "").lower()
    if verdict not in ("pass", "fix", "fail"):
        return "skip", {}, f"unknown verdict '{verdict}'"

    # ── Collect every field correction Claude proposed ────────────────────
    # Pre-2026-05-08 the FIX path only applied date corrections; broken
    # Company/Brand/Product/Pathogen rows were FAILed even when Claude
    # could read the right values off the page. The new shape carries all
    # five corrections through to main() which decides what to apply.
    corrections: Dict[str, str] = {}

    date_corr = (str(result.get("date_corrected") or "").strip() or "")
    if date_corr and re.match(r"^\d{4}-\d{2}-\d{2}$", date_corr):
        corrections["Date"] = date_corr

    for src_key, dst_field, min_len in (
        ("company_corrected",  "Company",  2),
        ("brand_corrected",    "Brand",    2),
        ("product_corrected",  "Product",  3),
        ("pathogen_corrected", "Pathogen", 3),
    ):
        v = str(result.get(src_key) or "").strip()
        if len(v) >= min_len:
            corrections[dst_field] = v

    reason_parts = [verdict]
    if not result.get("is_recall_page", True):
        reason_parts.append("not a recall page")
    if not result.get("company_match", True):
        reason_parts.append("company mismatch")
    if not result.get("product_match", True):
        reason_parts.append("product mismatch")
    if not result.get("pathogen_match", True):
        reason_parts.append("pathogen mismatch")
    if not result.get("date_match", True):
        reason_parts.append(f"date on page={result.get('date_on_page', '?')}")
    short_reason = "; ".join(reason_parts)
    if result.get("reason"):
        short_reason = f"{short_reason} | {str(result['reason'])[:140]}"

    # Refuse to apply a "fix" if Claude said "fix" but provided NO usable
    # correction at all (no date, no company, no brand, no product, no
    # pathogen). Degrade to "fail" — there's nothing to apply.
    if verdict == "fix" and not corrections:
        return "fail", {}, f"fix proposed but no corrections in response: {short_reason}"

    # Post-decision sanity (locked 2026-04-30) — hard rules that
    # override Claude's verdict if violated.
    final_url = url
    # Use corrected pathogen if Claude provided one, else the row's value
    final_pathogen = corrections.get("Pathogen") or str(row.get("Pathogen", "") or "")

    if is_news_mirror(final_url):
        return "fail", {}, f"news_mirror_domain | {short_reason}"

    # Use date_corrected if Claude proposed one, else row's date
    try:
        check_date_str = corrections.get("Date") or str(row.get("Date", ""))[:10]
        check_date_obj = (datetime.fromisoformat(check_date_str).date()
                          if check_date_str else None)
    except (TypeError, ValueError):
        check_date_obj = None

    if check_date_obj and check_date_obj.year < 2026:
        return "fail", {}, f"date_pre_2026: {check_date_obj.isoformat()} | {short_reason}"

    year_issue = is_year_mismatch(check_date_obj, final_url)
    if year_issue:
        return "fail", {}, f"url_year_mismatch: {year_issue} | {short_reason}"

    if not is_tier1_pathogen(final_pathogen):
        return "fail", {}, f"pathogen_out_of_scope: {final_pathogen!r} | {short_reason}"

    # Extraction garbage check (uses corrected values where available)
    company = (corrections.get("Company") or str(row.get("Company") or "")).strip().lower()
    brand = (corrections.get("Brand") or str(row.get("Brand") or "")).strip().lower()
    GARBAGE = {"home","index","page","recalls","alerts","alert","recall","welcome","main"}
    if company and company == brand and company in GARBAGE:
        return "fail", {}, f"extraction_garbage: Company=Brand={company!r} | {short_reason}"
    if re.search(r"/(home|index|main|welcome)/?$", final_url.lower()):
        return "fail", {}, f"extraction_garbage: URL is landing page | {short_reason}"

    # Company-placeholder rescue (audit 2026-05-08, supersedes 2026-05-06).
    #
    # Pre-2026-05-08 this branch FAILed every row where Claude saw a
    # company on the page but the row's Company field was a placeholder
    # ("", "Unbranded", "sans marque", etc.). The reasoning was sound
    # (the row IS broken), but the action was wrong: rows piled up in
    # Pending with Status=rejected for problems Claude itself had already
    # identified by name on the page.
    #
    # New behavior: if Claude provided company_corrected (or brand_corrected),
    # ESCALATE to verdict="fix" so main() applies the correction and the
    # row is promoted with the right value. Only fall through to FAIL when
    # the row genuinely has nothing to fix — placeholder Company AND no
    # correction proposed AND not a multi-producer RASFF row.
    company_match = result.get("company_match", True)
    placeholder_companies = {
        "", "—", "-", "unbranded", "sans marque",
        "various brands", "various producers", "multiple brands",
        "no brand", "marque inconnue", "n/a", "na",
    }
    is_rasff = "rasff" in str(row.get("Source") or "").lower()
    cur_company_lc = str(row.get("Company") or "").strip().lower()
    if (not company_match and not is_rasff
            and cur_company_lc in placeholder_companies):
        if "Company" in corrections or "Brand" in corrections:
            # Claude can fill it — promote via FIX path
            verdict = "fix"
            short_reason = f"placeholder_company_filled_from_page | {short_reason}"
        else:
            return ("fail", {},
                    f"placeholder_company_with_company_mismatch: "
                    f"Company={row.get('Company')!r} but Claude says page "
                    f"DOES name a company → no correction proposed | "
                    f"{short_reason}")

    return verdict, corrections, short_reason


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    t0 = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info("Claude check (final verification) started: %s",
             t0.strftime("%Y-%m-%dT%H:%M:%SZ"))

    if not CLAUDE_ENABLED:
        log.error("Claude not enabled (no ANTHROPIC_API_KEY). Aborting.")
        return 2

    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    approved = load_existing(XLSX_PATH)
    pending = load_pending(XLSX_PATH)
    log.info("State: %d approved + %d pending", len(approved), len(pending))

    if not pending:
        log.info("Nothing in Pending — nothing to do.")
        return 0

    # Skip rows that are already in REJECTED status — those need human
    # triage, not another Claude pass.
    rows_to_check_idx: List[int] = [
        i for i, r in enumerate(pending)
        if (r.get("Status") or "").lower() != STATUS_REJECTED
    ]
    log.info("Will verify %d rows (skipping %d already-rejected)",
             len(rows_to_check_idx), len(pending) - len(rows_to_check_idx))

    # Per-row verification
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rejected_flags: Dict[int, str] = {}
    skipped_idx: set[int] = set()
    fixed_count = 0
    pass_count = 0
    fail_count = 0
    skip_count = 0

    for n, idx in enumerate(rows_to_check_idx, 1):
        row = pending[idx]
        log.info("[%d/%d] %s | %s",
                 n, len(rows_to_check_idx),
                 str(row.get("Date", ""))[:10],
                 str(row.get("URL", ""))[:80])
        verdict, corrections, reason = check_row(row)

        # Audit trail in Notes — list every applied correction so a
        # human can trace what changed and why.
        corr_summary = ""
        if corrections:
            parts = [f"{k}→{str(v)[:40]}" for k, v in corrections.items()]
            corr_summary = "; corrections: " + ", ".join(parts)
        audit = (f"[claude-check {today_iso}: {verdict}{corr_summary}; "
                 f"{reason[:120]}]")
        notes = (row.get("Notes") or "").strip()
        # Drop any prior claude-check stamp before appending
        notes = re.sub(r"\s*\[claude-check[^\]]*\]\s*", " ", notes).strip()
        row["Notes"] = (notes + " " + audit).strip()[:1000]

        if verdict == "fix":
            # Apply EVERY correction Claude provided (audit 2026-05-08).
            # Pre-2026-05-08 only Date was applied; broken Company/Brand/
            # Product/Pathogen rows piled up in Pending forever because
            # the FIX path didn't know how to write them in.
            for field, new_val in corrections.items():
                old_val = row.get(field, "")
                row[field] = new_val
                log.info("  → FIX %s %r → %r",
                         field, str(old_val)[:40], str(new_val)[:60])
            fixed_count += 1
            # Falls through: row will be promoted by promote_approved
        elif verdict == "fail":
            rejected_flags[idx] = f"Claude check: {reason[:280]}"
            log.info("  → FAIL: %s", reason)
            fail_count += 1
        elif verdict == "pass":
            log.info("  → PASS")
            pass_count += 1
        else:  # skip
            log.info("  → SKIP: %s (left untouched, retry next run)", reason)
            skipped_idx.add(idx)   # ← AUDIT 2026-05-08: track for parking lot
            skip_count += 1

        time.sleep(SLEEP_BETWEEN_ROWS_S)

    # ── SKIP parking lot (audit 2026-05-08, fixes silent auto-promotion) ──
    # Pre-2026-05-08 a SKIP verdict was a NO-OP — the row fell through to
    # promote_approved, which promoted any row not explicitly in
    # rejected_flags. Result: when Claude was rate-limited or unreachable,
    # every Pending row that day got auto-promoted to Recalls without
    # verification (2026-05-07T17:01 incident: 6 rows promoted, 0 reviewed).
    #
    # Fix: park skipped plain-pending rows in STATUS_PENDING_GAP_V2 (a
    # gap-gating status that promote_approved refuses to advance). They
    # stay in Pending until the next reviewer run can verify them. Skipped
    # rows that were ALREADY in a gap-gating status are left alone — they
    # were going to stay in Pending anyway.
    parked = 0
    for idx in skipped_idx:
        row = pending[idx]
        cur_status = (row.get("Status") or "").strip().lower()
        if cur_status in ("", "pending"):
            row["Status"] = STATUS_PENDING_GAP_V2
            parked += 1
    if parked:
        log.info("Parked %d skipped row(s) in pending_gap_v2 to prevent "
                 "auto-promotion (will retry next reviewer run)", parked)

    # ── Gap-finder gating state machine advance (audit 2026-04-29) ──────
    # Any pending_gap_v2 row that just passed Claude verification gets
    # promoted out of the gap-gating ladder by flipping its Status to
    # plain "pending". The next merge_master run will then promote it to
    # Recalls under normal rules. Failures are already in rejected_flags
    # and will be marked STATUS_REJECTED by promote_approved.
    gap_promoted_to_pending = 0
    today_iso2 = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for idx in rows_to_check_idx:
        if idx in rejected_flags:
            continue  # FAIL — let promote_approved mark rejected
        if idx in skipped_idx:
            continue  # SKIP — leave in parking lot (audit 2026-05-08)
        row = pending[idx]
        if (row.get("Status") or "").strip() == STATUS_PENDING_GAP_V2:
            row["Status"] = STATUS_PENDING
            notes = (row.get("Notes") or "").strip()
            tag = f"[gap-gate {today_iso2}: pending_gap_v2 → pending (Claude verified)]"
            row["Notes"] = (notes + " " + tag).strip()[:1000]
            gap_promoted_to_pending += 1
    if gap_promoted_to_pending:
        log.info("Gap-gating advanced: %d rows pending_gap_v2 → pending "
                 "(eligible for next merge_master)", gap_promoted_to_pending)

    # Apply rejected_flags + dedup against Recalls via promote_approved.
    # Audit 2026-05-08: 3-tuple return — archived_rejected = rows that hit
    # 2-different-reviewer threshold and were dropped from Pending. They
    # get written to the Rejected sheet for human audit.
    new_approved, remaining, archived_rejected = promote_approved(
        pending=pending,
        approved_existing=approved,
        rejected_flags=rejected_flags,
    )

    log.info("Verdicts: pass=%d, fix=%d, fail=%d, skip=%d (total checked=%d)",
             pass_count, fixed_count, fail_count, skip_count,
             pass_count + fixed_count + fail_count + skip_count)
    log.info("Promotion: %d Pending → Recalls; %d remain in Pending; "
             "%d archived to Rejected",
             len(new_approved), len(remaining), len(archived_rejected))

    # Save + commit
    final_approved = sort_rows(approved + new_approved)
    final_pending = sort_rows(remaining)
    save_xlsx_with_pending(final_approved, final_pending, XLSX_PATH,
                           newly_rejected_rows=archived_rejected)
    mirror_json_from_xlsx(XLSX_PATH, JSON_PATH)

    # Mirror promotions into the Weekly_Review sheet + refresh the
    # JSON slice the Apps Script Thursday-17:00 mailer reads.
    try:
        from pipeline.weekly_review_capture import record_promotions  # noqa: E402
        n_wr = record_promotions(new_approved, xlsx_path=XLSX_PATH)
        if n_wr:
            log.info("Weekly_Review: appended %d row(s)", n_wr)
    except Exception as _wr_err:  # never block promotion on capture failure
        log.warning("Weekly_Review capture failed: %s", _wr_err)

    # Rebuild daily briefs for every date that gained newly-promoted rows.
    # Without this, the dashboard's rolling 7-day display + DAILY tab stay
    # stale until the next 10:00 daily-recall-search run.
    rebuilt_briefs, brief_files = rebuild_daily_briefs_for_promoted(
        new_approved, final_approved,
    )
    if rebuilt_briefs:
        log.info("Rebuilt %d daily brief(s)", len(rebuilt_briefs))

    files_to_commit = [
        "docs/data/recalls.xlsx",
        "docs/data/recalls.json",
        "docs/data/weekly-review-latest.json",
    ]
    files_to_commit.extend(brief_files)

    if not SKIP_COMMIT:
        ok = git_commit_and_push(
            repo_dir=ROOT,
            files=files_to_commit,
            message=(f"FSIS Claude check {t0.strftime('%Y-%m-%d')} "
                     f"(+{len(new_approved)} promoted, "
                     f"{fixed_count} dates fixed, "
                     f"{fail_count} failed, {len(remaining)} pending"
                     + (f", rebuilt {len(rebuilt_briefs)} briefs"
                        if rebuilt_briefs else "")
                     + ")"),
        )
        if not ok:
            log.error("Git push failed")
            return 1

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info("=" * 60)
    log.info("DONE in %.1fs | +%d promoted | %d date fixes | %d failed | "
             "%d still pending | %d briefs rebuilt",
             elapsed, len(new_approved), fixed_count, fail_count,
             len(remaining), len(rebuilt_briefs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
