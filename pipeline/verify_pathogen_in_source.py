"""
Pathogen-in-source verifier — Layer D of the anti-hallucination stack.

Defensive deterministic check that runs AFTER Gemini extraction has filled
the Pending sheet but BEFORE rows are promoted to Recalls. For each
newly-scraped row:

  1. Fetch the source URL (with timeout, parallelized, in-memory cached
     by URL within a single run).
  2. Convert the HTML to plain text (BeautifulSoup, lxml parser).
  3. Check whether the row's Pathogen value — OR any multilingual
     equivalent (Salmonella ↔ salmonellen / salmonelle / salmonelas;
     Listeria ↔ listerien / listéria; Ochratoxin ↔ ochratoxine /
     ocratoxina; Undeclared meat ↔ fleisch / viande / carne; etc.) —
     appears as a case-insensitive substring of the page body.
  4. If NOT found → flag the row for rejection with reason
     "Pathogen '<X>' not found in source page text" and log URL +
     pathogen + first 200 chars of body for debugging.
  5. If page fetch fails (timeout, 4xx, 5xx, network) → SKIP this row
     (do NOT reject) and log a warning. Transient network issues should
     not poison the dataset.

This catches the failure mode where Gemini fabricated BOTH the Pathogen
field AND a self-consistent Reason field (the Espido Halim case:
Pathogen="Ochratoxin" + Reason mentions ochratoxin, but the actual page
describes undeclared meat). Layer C's AI review only catches internal
inconsistency; Layer D catches consistent-but-wrong rows by going to the
ground truth — the regulator's page itself.

DESIGN CONSTRAINTS:
  * Idempotent — re-running on the same Pending state produces the same
    rejection set (no side effects beyond the returned dict).
  * No new dependencies — uses requests + bs4 (already in requirements).
  * No AI calls — pure string matching.
  * Cost: ~1–2 seconds per row, parallelized across MAX_WORKERS threads.

ENTRY POINT:
    verify_pending_rows(pending, new_indices, max_workers=8) -> Dict[int, str]

WIRING (in pipeline/run_all.py):
    pathogen_rejections = verify_pending_rows(pending, new_indices)
    # then merge into rejections at compute_rejections() step
"""
from __future__ import annotations

import logging
import re
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from scrapers._base import make_session, fetch

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------
HTTP_TIMEOUT_S = 15
MAX_PAGE_BYTES = 2_000_000   # 2 MB — cap for page download (memory safety)
MIN_BODY_CHARS = 200         # below this we cannot trust the extraction
MAX_WORKERS_DEFAULT = 8
LOG_BODY_PREVIEW = 200       # chars of body logged on rejection for debugging

# ---------------------------------------------------------------------------
# PATHOGEN_ALIASES
# ---------------------------------------------------------------------------
PATHOGEN_ALIASES: Dict[str, List[str]] = {
    # ── Microbiological pathogens (Tier 1) ────────────────────────────
    "Listeria monocytogenes": [
        "listeria", "listerien", "listéria", "l. monocytogenes",
        "monocytogenes",
    ],
    "Shiga toxin-producing E. coli (STEC)": [
        "stec", "vtec", "ehec", "shiga", "shigatoxin", "shiga-toxin",
        "shiga toxin", "e. coli o", "escherichia coli", "ecoli",
    ],
    "Clostridium botulinum": [
        "botulinum", "botulism", "botulisme", "botulismus",
        "c. botulinum", "clostridium",
    ],
    "Cereulide (B. cereus toxin)": [
        "cereulide", "emetic toxin", "b. cereus", "bacillus cereus",
    ],
    "Marine biotoxin": [
        "biotoxin", "saxitoxin", "domoic acid", "tetrodotoxin",
        "ciguatoxin", "okadaic", "asp ", "psp ", "dsp ",
    ],

    # ── Microbiological pathogens (Tier 2) ────────────────────────────
    "Salmonella": [
        "salmonella", "salmonellen", "salmonelle", "salmonelas",
        "salmonelosis", "salmonelose", "salmonella spp",
    ],
    "Hepatitis A virus": [
        "hepatitis a", "hav ", "hepatitis-a", "hépatite a", "hav,",
    ],
    "Norovirus": ["norovirus", "norwalk"],

    # ── E. coli family ────────────────────────────────────────────────
    "E. coli STEC": [
        "stec", "vtec", "ehec", "shiga", "shigatoxin", "shiga-toxin",
        "shiga toxin", "e. coli o", "escherichia coli", "ecoli",
    ],
    "STEC": [
        "stec", "vtec", "ehec", "shiga", "shigatoxin", "shiga toxin",
        "escherichia coli",
    ],
    "E. coli": [
        "e. coli", "ecoli", "escherichia coli",
    ],

    # ── Histamine ─────────────────────────────────────────────────────
    "Histamine": [
        "histamine", "histamin", "scombro", "scombroid", "scombrotoxin",
        "biotoxine endogène",
    ],

    # ── Microbiological pathogens (Tier 3) ────────────────────────────
    "Escherichia coli (generic)": [
        "e. coli", "ecoli", "escherichia coli",
    ],
    "Campylobacter": ["campylobacter"],
    "Vibrio": ["vibrio"],
    "Cyclospora cayetanensis": ["cyclospora"],
    "Yersinia enterocolitica": ["yersinia"],
    "Bacillus cereus": ["b. cereus", "bacillus cereus"],
    "Brucella": ["brucella"],
    "Cronobacter sakazakii": [
        "cronobacter", "enterobacter sakazakii", "sakazakii",
    ],
    "Staphylococcus enterotoxin": [
        "staphylococcus", "staph aureus", "staphylococcal",
        "enterotoxin",
    ],
    "Shigella": ["shigella"],
    "Histamine / scombrotoxin": [
        "histamine", "scombro", "scombroid", "scombrotoxin",
    ],

    # ── Mycotoxins / chemical contaminants ────────────────────────────
    "Aflatoxin": ["aflatoxin", "aflatoxine", "aflatossina"],
    "Ochratoxin": [
        "ochratoxin", "ochratoxine", "ocratoxina", "ocratossina",
    ],
    "Alternaria toxins": ["alternaria", "alternariol", "tenuazonic"],
    "T-2 / HT-2 toxin": ["t-2", "ht-2", "t2 toxin", "ht2 toxin"],
    "Ergot alkaloids": [
        "ergot", "claviceps", "mutterkorn", "ergot alkaloid",
    ],
    "Citrinin": ["citrinin"],
    "Mycotoxin": [
        "mycotoxin", "mykotoxin", "micotoxin", "micotossin",
        "fumonisin", "zearalenone", "deoxynivalenol", "patulin",
    ],
    "Ethylene oxide": [
        "ethylene oxide", "ethylenoxid", "oxyde d'éthylène",
        "ossido di etilene", "óxido de etileno",
    ],

    # ── Heavy metals ──────────────────────────────────────────────────
    "Lead": [
        "lead contamination", "lead level", "lead content",
        "blei", "plomb", "piombo", "plomo",
    ],
    "Cadmium": ["cadmium", "kadmium", "cadmio"],
    "Arsenic": ["arsenic", "arsen", "arsenico", "arsénico"],
    "Mercury": [
        "mercury", "quecksilber", "mercure", "mercurio",
    ],

    # ── Non-contamination hazards ─────────────────────────────────────
    "Undeclared meat": [
        "undeclared meat", "fleisch", "viande non déclarée",
        "viande non-déclarée", "carne non dichiarata",
        "ausgelobt", "wheat porridge contains meat",
    ],
    "Undeclared allergen": [
        "allergen", "allergie", "allergène", "allergeen",
        "alérgeno", "undeclared allergen", "allergenkennzeichnung",
    ],
    "Foreign body": [
        "foreign body", "fremdkörper", "corps étranger",
        "corpo estraneo", "cuerpo extraño",
        "metal fragment", "plastic fragment", "glass fragment",
        "metallteil", "kunststoffteil", "glassplitter",
    ],
    "Physical/foreign-body contamination": [
        "foreign body", "fremdkörper", "corps étranger",
        "corpo estraneo", "physical hazard", "physical contamination",
        "metal fragment", "plastic fragment", "glass fragment",
        "metallteil", "kunststoffteil", "glassplitter",
    ],
    "E. coli O157:H7": [
        "stec", "vtec", "ehec", "shiga", "shigatoxin",
        "e. coli o157", "o157:h7", "o157", "escherichia coli",
    ],
    "Mislabeling": [
        "mislabel", "mislabelled", "mislabeled", "falschdeklaration",
        "étiquetage erroné", "etichettatura errata",
        "incorrect label", "etiquetado incorrecto",
    ],
    "Packaging defect": [
        "packaging defect", "verpackungsmangel",
        "défaut d'emballage", "difetto di confezione",
        "container defect",
    ],
    "Rodenticide": [
        "rodenticide", "rodentizid", "rat poison",
        "rodenticida", "raticide", "ratticida",
    ],

    # ── Chemical / regulatory contaminants ────────────────────────────
    "PFOA / PFAS": [
        "pfoa", "pfas", "perfluoroctan", "perfluorooctan",
        "perfluorinated", "perfluor",
    ],
    "Sulfite": [
        "sulfite", "sulphite", "schwefeldioxid",
        "anhydride sulfureux", "sulfito", "soufre",
    ],

    # ── Bacillus cereus production variants ───────────────────────────
    "Bacillus cereus / cereulide": [
        "cereulide", "emetic toxin", "b. cereus", "bacillus cereus",
    ],
    "Bacillus cereus (cereulide)": [
        "cereulide", "emetic toxin", "b. cereus", "bacillus cereus",
    ],

    # ── Pest / process / mushroom toxins ──────────────────────────────
    "Pest contamination": [
        "rodent contamination", "mouse contamination",
        "rodent infestation", "rats infestation",
        "ratte", "souris", "vermin", "ungeziefer",
    ],
    "Process failure": [
        "sterilization", "sterilisation", "pasteur",
        "thermal process", "incomplete process",
        "unzureichend erhitzt", "inadeguatamente",
        "incorrectement",
    ],
    "Possible incomplete pasteurization": [
        "pasteur", "incomplete pasteur", "underpasteur",
        "thermal process", "unzureichend erhitzt",
    ],
    "Bacterial contamination": [
        "bacterial contamination", "coliform", "kolibakterien",
        "contamination bactérien",
    ],
    "Amanita muscaria toxin": [
        "muscimol", "ibotenic", "amanita", "fly agaric",
        "fliegenpilz",
    ],
    "Phytoplankton biotoxins": [
        "phytoplankton", "biotoxin", "saxitoxin", "domoic",
        "tetrodotoxin", "ciguatoxin", "okadaic",
    ],

    # ── Variant aliases for production canonicals seen in xlsx ────────
    "Hepatitis A": [
        "hepatitis a", "hav", "hepatitis-a", "hépatite a",
    ],
    "Patulin": ["patulin"],
    "rat poison": [
        "rat poison", "rodenticide", "rodentizid",
        "raticide", "ratticida",
    ],
    "Peanut": [
        "peanut", "erdnuss", "arachide", "cacahuete", "cacahuète",
        "amendoim", "arachidi",
    ],
}


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------
_NORMALIZE_PATTERNS: List[Tuple["re.Pattern[str]", str]] = [
    (re.compile(r"\s*\([^)]*\)\s*$"), ""),
    (re.compile(r"\s+spp?\.?\s*$", re.IGNORECASE), ""),
    (re.compile(r"^(salmonella)\s+[A-Z]\w+\s*$", re.IGNORECASE), r"\1"),
    (re.compile(r"^((?:ochra|afla)toxin)\s+[A-Z]\d?\s*$", re.IGNORECASE), r"\1"),
    (re.compile(
        r"^((?:ochra|afla)toxin|alternaria|biotoxin|allergen|sulfite|mycotoxin)s\b",
        re.IGNORECASE,
    ), r"\1"),
]


def _normalize_pathogen_for_lookup(p: str) -> str:
    """Strip variants (parens, spp., serotype, trailing letter, plural)
    so the remaining stem can be matched against PATHOGEN_ALIASES keys.
    Applied iteratively until stable."""
    out = (p or "").strip()
    prev: Optional[str] = None
    while out and out != prev:
        prev = out
        for pattern, repl in _NORMALIZE_PATTERNS:
            out = pattern.sub(repl, out).strip()
    return out


def _aliases_for(pathogen: str) -> List[str]:
    """Return list of substrings (lowercased) that should match this
    pathogen on a source page. Always includes the literal pathogen
    string itself; then tries direct alias lookup, then a normalised
    lookup. Empty list if input is empty/whitespace."""
    p = (pathogen or "").strip()
    if not p:
        return []
    out: List[str] = [p.lower()]
    direct = PATHOGEN_ALIASES.get(p)
    if direct is None:
        p_low = p.lower()
        for key, aliases in PATHOGEN_ALIASES.items():
            if key.lower() == p_low:
                direct = aliases
                break
    if direct is None:
        normalized = _normalize_pathogen_for_lookup(p)
        if normalized and normalized.lower() != p.lower():
            n_low = normalized.lower()
            out.append(n_low)
            for key, aliases in PATHOGEN_ALIASES.items():
                if key.lower() == n_low:
                    direct = aliases
                    break
    if direct:
        out.extend(a.lower() for a in direct)
    seen = set()
    deduped: List[str] = []
    for a in out:
        if a and a not in seen:
            seen.add(a)
            deduped.append(a)
    return deduped


def _pathogen_in_text(pathogen: str, body_text: str) -> bool:
    """Case-insensitive substring search of any alias of `pathogen` in
    `body_text`. Returns True on first match."""
    if not pathogen or not body_text:
        return False
    body_low = body_text.lower()
    for alias in _aliases_for(pathogen):
        if alias in body_low:
            return True
    return False


# Domains that JS-render recall content. Static HTTP fetch only retrieves
# the SPA shell (nav + footer), never the recall body. Layer D cannot
# verify pathogens on these hosts without a headless browser, so we SKIP
# them (do NOT reject) — same disposition as a network failure.
# Discovered via tools/run_layerD_live.py on 2026-04-28.
_JS_RENDERED_HOSTS = frozenset({
    "rappel.conso.gouv.fr",         # Vue.js SPA — fiche pages render client-side
    "webgate.ec.europa.eu",         # RASFF Window — Angular SPA
    "recalls-rappels.canada.ca",    # CFIA — Angular SPA on /alert-recall/ pages
})


def _is_js_rendered_host(url: str) -> bool:
    """True if URL host is a known JS-rendered SPA whose body cannot be
    verified by static fetch + BeautifulSoup."""
    if not url:
        return False
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return False
    if host.startswith("www."):
        host = host[4:]
    if host in _JS_RENDERED_HOSTS:
        return True
    return any(host.endswith("." + h) for h in _JS_RENDERED_HOSTS)


def _is_pdf_url(url: str) -> bool:
    """True if URL is a static PDF link — BeautifulSoup can't extract;
    skip Layer D for these."""
    if not url:
        return False
    u = url.lower().split("#", 1)[0].split("?", 1)[0]
    return u.endswith(".pdf") or ".pdf.download" in url.lower()


def _fetch_page_text(url: str, session) -> Optional[str]:
    """Fetch URL and return the visible page text, or None on failure."""
    if not url or not url.startswith(("http://", "https://")):
        return None
    resp = fetch(session, url, method="GET", timeout=HTTP_TIMEOUT_S,
                 stream=False, allow_redirects=True)
    if resp is None:
        return None
    try:
        if resp.status_code != 200:
            return None
        content = resp.content[:MAX_PAGE_BYTES]
    except Exception as exc:  # noqa: BLE001
        log.warning("verify_pathogen: read failed %s | %s", url[:80], exc)
        return None
    try:
        soup = BeautifulSoup(content, "lxml")
    except Exception:
        try:
            soup = BeautifulSoup(content, "html.parser")
        except Exception as exc:  # noqa: BLE001
            log.warning("verify_pathogen: parse failed %s | %s", url[:80], exc)
            return None
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return text


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def verify_pending_rows(
    pending: List[Dict[str, Any]],
    new_indices: List[int],
    max_workers: int = MAX_WORKERS_DEFAULT,
) -> Dict[int, str]:
    """For each pending row at one of `new_indices`, fetch the source URL
    and verify that the row's Pathogen value (or a multilingual
    equivalent) appears in the page body. Return a dict mapping
    pending-row indices to a rejection reason for rows that fail
    verification.

    Rows skipped (NOT rejected, NOT in returned dict):
      - Empty URL or empty Pathogen
      - URL is on a JS-rendered SPA host (RappelConso/RASFF/CFIA)
      - URL is a static PDF link
      - Page fetch failed (timeout / 4xx / 5xx / network error)
      - Page body too short (< MIN_BODY_CHARS)"""
    rejections: Dict[int, str] = {}
    if not new_indices:
        return rejections

    session = make_session()
    cache: Dict[str, Optional[str]] = {}
    cache_lock = threading.Lock()

    def _check_one(idx: int) -> Optional[Tuple[int, str]]:
        try:
            row = pending[idx]
        except (IndexError, KeyError):
            return None
        url = str(row.get("URL") or "").strip()
        pathogen = str(row.get("Pathogen") or "").strip()
        if not url or not pathogen:
            return None

        # SKIP (do NOT reject) JS-rendered SPAs and static PDFs
        if _is_js_rendered_host(url):
            log.info(
                "verify_pathogen: SKIP (JS-rendered SPA host) idx=%d url=%s pathogen=%s",
                idx, url[:80], pathogen,
            )
            return None
        if _is_pdf_url(url):
            log.info(
                "verify_pathogen: SKIP (PDF URL, no text extractor) idx=%d url=%s pathogen=%s",
                idx, url[:80], pathogen,
            )
            return None

        # Cache lookup
        body: Optional[str]
        with cache_lock:
            cached = cache.get(url, "MISS")
        if cached != "MISS":
            body = cached
        else:
            body = _fetch_page_text(url, session)
            with cache_lock:
                cache[url] = body

        if body is None:
            log.warning(
                "verify_pathogen: SKIP (fetch failed) idx=%d url=%s pathogen=%s",
                idx, url[:80], pathogen,
            )
            return None
        if len(body) < MIN_BODY_CHARS:
            log.warning(
                "verify_pathogen: SKIP (body too short, %d chars) idx=%d url=%s",
                len(body), idx, url[:80],
            )
            return None

        if _pathogen_in_text(pathogen, body):
            return None  # verified — pass through

        # Rejection branch
        preview = body[:LOG_BODY_PREVIEW].replace("\n", " ")
        log.warning(
            "verify_pathogen: REJECT idx=%d pathogen=%r url=%s | body[:%d]=%r",
            idx, pathogen, url[:80], LOG_BODY_PREVIEW, preview,
        )
        reason = (
            f"Pathogen '{pathogen}' not found in source page text "
            f"({len(body)} chars from {url[:60]})"
        )
        return (idx, reason)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_check_one, idx): idx for idx in new_indices}
        for fut in as_completed(futures):
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001
                idx = futures[fut]
                log.warning(
                    "verify_pathogen: thread error idx=%d: %s", idx, exc,
                )
                continue
            if result is not None:
                idx, reason = result
                rejections[idx] = reason

    log.info(
        "Pathogen-in-source verification: %d/%d new rows rejected",
        len(rejections), len(new_indices),
    )
    return rejections


# ---------------------------------------------------------------------------
# Standalone smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")

    # Quick offline tests of the matcher (no network)
    assert _pathogen_in_text("Salmonella", "Detection of salmonelle spp. in cheese")
    assert _pathogen_in_text("Listeria monocytogenes",
                             "Listerien wurden im Produkt nachgewiesen")
    assert _pathogen_in_text("Ochratoxin", "ocratoxina A rilevata nel prodotto")
    assert _pathogen_in_text("Undeclared meat",
                             "das als Weizenbrei ausgelobte Produkt enthält Fleisch")
    assert not _pathogen_in_text("Ochratoxin",
                                 "das als Weizenbrei ausgelobte Produkt enthält Fleisch")
    # JS-SPA + PDF skip helpers
    assert _is_js_rendered_host("https://rappel.conso.gouv.fr/fiche-rappel/22070/Interne")
    assert _is_js_rendered_host("https://webgate.ec.europa.eu/rasff-window/screen/x")
    assert _is_js_rendered_host("https://recalls-rappels.canada.ca/en/alert-recall/x")
    assert not _is_js_rendered_host("https://www.lebensmittelwarnung.de/x")
    assert _is_pdf_url("https://www.blv.admin.ch/x.pdf.download.pdf/y")
    assert _is_pdf_url("https://example.com/recall.pdf")
    assert not _is_pdf_url("https://example.com/recall.html")
    print("matcher + JS-SPA + PDF skip tests: OK")

    # If a URL was passed on argv, do a live check
    if len(sys.argv) > 2:
        url = sys.argv[1]
        pathogen = sys.argv[2]
        session = make_session()
        body = _fetch_page_text(url, session)
        if body is None:
            print(f"FETCH FAILED: {url}")
            sys.exit(2)
        found = _pathogen_in_text(pathogen, body)
        print(f"URL: {url}")
        print(f"Pathogen: {pathogen!r}")
        print(f"Body length: {len(body)} chars")
        print(f"Aliases tried: {_aliases_for(pathogen)}")
        print(f"Found: {found}")
        sys.exit(0 if found else 1)