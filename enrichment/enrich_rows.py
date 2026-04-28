"""
Enrichment pipeline: takes raw scraped Recall objects and improves them
- Pathogen normalization (already done in Recall.normalize, but Gemini catches edge cases)
- URL-keyword fallback: if AI enrichment fails, scan the URL for pathogen strings
- Country normalization for non-English source text
- Tier reassignment after Gemini correction
- Outbreak flag detection from Notes/Reason
"""
from __future__ import annotations
import logging
import re
from typing import List, Optional
from scrapers._models import Recall, normalize_pathogen, normalize_country, assign_tier, infer_region
from enrichment.gemini_client import enrich_row

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# URL-keyword pathogen fallback
# ---------------------------------------------------------------------------
# When the scraper and Gemini both fail to extract a pathogen (common with
# BLV/BVL PDF-based recalls), the pathogen is often visible in the URL itself.
# This map scans the URL for known substrings and returns the canonical name.
# Order matters: more specific patterns first to avoid false positives.
# ---------------------------------------------------------------------------
_URL_PATHOGEN_MAP = [
    # Bacteria
    (r"salmonell",              "Salmonella spp."),
    (r"listeria|listerio",      "Listeria monocytogenes"),
    (r"e[\.\-_]?coli|ecoli|stec|ehec|vtec", "E. coli STEC (Shiga toxin-producing)"),
    (r"botuli[ns]",             "Clostridium botulinum"),
    (r"campylobact",            "Campylobacter"),
    (r"cronobact|sakazaki",     "Cronobacter sakazakii"),
    (r"bacillus[\.\-_]?cereus|cereulide", "Bacillus cereus / cereulide"),
    (r"vibrio",                 "Vibrio"),
    (r"yersinia",               "Yersinia"),
    (r"shigell",                "Shigella"),
    (r"brucell",                "Brucella"),
    (r"staphyloco|staph[\.\-_]?aureus", "Staphylococcus aureus"),
    # Viruses
    (r"norovir",                "Norovirus"),
    (r"hepatit[\.\-_]?a",       "Hepatitis A"),
    # Parasites
    (r"cyclospora",             "Cyclospora"),
    # Mycotoxins / mold (April 2026+ scope: Alternaria + Fusarium + ergot)
    (r"aflatoxin",              "Aflatoxins"),
    (r"ochratoxin|ocratoxin|ocratossin", "Ochratoxin A"),
    (r"patulin",                "Patulin"),
    (r"alternariol|tenuazonic|alternaria[\s\-_]+toxin|toxines?[\s\-_]+(?:d['e\s]+)?alternaria",
                                "Alternaria toxins"),
    (r"\b(?:ht|t)[\s\-]?2[\s\-]?toxin", "T-2 / HT-2 toxin"),
    (r"fumonisin",              "Fumonisin"),
    (r"zearalenon|\bzea\b",     "Zearalenone"),
    (r"deoxynivalenol|\bdon\s+(?:toxin|mycotoxin)|nivalenol", "Deoxynivalenol (DON)"),
    (r"citrinin",               "Citrinin"),
    (r"\bergot\b|\bclaviceps\b|\bmutterkorn\b|\balcaloid\w*\s+(?:de|of|d['e]?)\s*ergot",
                                "Ergot alkaloids"),
    (r"mycotoxin|mykotoxin|micotoxin|micotossin", "Mycotoxin"),
    (r"mouskimol|mould|(?<!\w)mold(?!\w)", "Mycotoxin"),  # Greek μούσκα, English mold/mould
    # Marine biotoxins
    (r"scombro|histamin",       "Histamine (scombrotoxin)"),
    (r"biotoxin|dsp|psp|asp",   "Marine biotoxins"),
    # Chemical / physical
    (r"rodentici|rat[\.\-_]?poison|bromadiol|brodifac|difethial", "Rodenticide (rat poison)"),
    (r"lead[\.\-_]?contam|blei|plomb|\bpb\b", "Lead (Pb) contamination"),
    (r"cadmium|\bcd\b(?!\.)",   "Cadmium (Cd) contamination"),
    (r"arsenic|\bas\b(?!\.)",   "Arsenic (As) contamination"),
    (r"mercury|mercure|quecksilber|\bhg\b", "Mercury (Hg) contamination"),
    (r"glass[\.\-_]?fragment",  "Glass fragments"),
    (r"metal[\.\-_]?fragment",  "Metal fragments"),
    (r"plastic[\.\-_]?fragment","Plastic fragments"),
    (r"foreign[\.\-_]?bod",     "Physical/foreign-body contamination"),
]

_URL_PATTERNS = [(re.compile(pat, re.IGNORECASE), canonical) for pat, canonical in _URL_PATHOGEN_MAP]


def _pathogen_from_url(url: str) -> Optional[str]:
    """Scan a recall URL for known pathogen keywords. Returns canonical name or None."""
    if not url:
        return None
    # Decode URL-encoded characters for better matching
    from urllib.parse import unquote
    decoded = unquote(url).lower()
    for pattern, canonical in _URL_PATTERNS:
        if pattern.search(decoded):
            return canonical
    return None

# Heuristic: rows that look "incomplete enough" to warrant a Gemini call
def _needs_enrichment(r: Recall) -> bool:
    if not r.Pathogen or r.Pathogen.strip() in ("", "—", "unknown", "—"):
        return True
    if not r.Country:
        return True
    if not r.Class:
        return True
    return False


def enrich_recalls(recalls: List[Recall], use_ai: bool = True, max_ai_calls: int = 100) -> List[Recall]:
    """
    Enrich a list of recall rows.
    - First pass: deterministic normalization
    - Second pass: Gemini fills gaps (capped to max_ai_calls to preserve free quota)
    """
    out: List[Recall] = []
    ai_used = 0
    url_rescued = 0
    for r in recalls:
        # Deterministic normalization first
        r.Pathogen = normalize_pathogen(r.Pathogen)
        if r.Country:
            r.Country = normalize_country(r.Country)
        if not r.Region and r.Country:
            r.Region = infer_region(r.Country)

        # AI enrichment if needed and within budget
        if use_ai and _needs_enrichment(r) and ai_used < max_ai_calls:
            try:
                fixed = enrich_row(r.to_dict())
                # Apply only if Gemini returned something usable
                if fixed.get("Pathogen") and not r.Pathogen:
                    r.Pathogen = normalize_pathogen(fixed["Pathogen"])
                if fixed.get("Country") and not r.Country:
                    r.Country = normalize_country(fixed["Country"])
                if fixed.get("Class") and not r.Class:
                    r.Class = fixed["Class"]
                if int(fixed.get("Outbreak", 0)):
                    r.Outbreak = 1
                ai_used += 1
            except Exception as e:
                log.warning("Gemini enrichment failed for row: %s", e)

        # URL-keyword fallback: if pathogen is STILL missing after AI,
        # scan the URL for known pathogen strings (catches BLV/BVL PDF
        # recalls where the pathogen is in the filename but not in
        # machine-readable metadata)
        if not r.Pathogen and r.URL:
            url_pathogen = _pathogen_from_url(r.URL)
            if url_pathogen:
                r.Pathogen = url_pathogen
                log.info("URL-fallback rescued pathogen: %s from %s",
                         url_pathogen, r.URL[:80])
                url_rescued += 1

        # Reassign tier after enrichment
        r.Tier = assign_tier(r.Pathogen, r.Outbreak)
        if not r.Region and r.Country:
            r.Region = infer_region(r.Country)
        out.append(r)
    log.info("Enrichment complete: %d rows, %d AI-enriched, %d URL-rescued", len(out), ai_used, url_rescued)
    return out
