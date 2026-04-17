"""
Claude Haiku 4.5 client — two modes:

  1. review_tier1(rows)            — Tier-1 recall QA (unchanged)
  2. extract_recalls_from_html(...) — HTML -> structured recalls (FALLBACK scraper,
                                       used only when Gemini returns zero rows)

The fallback-only positioning is intentional: Haiku 4.5 is ~$1/M input + $5/M output,
so using it for every scrape would burn the $5 free credit in days. By running
Gemini first and falling through to Claude only when Gemini fails or returns zero
(usually on difficult non-English or JS-heavy pages), Claude plugs real coverage
gaps for a few cents a day.

Enable by setting ANTHROPIC_API_KEY. If absent, both modes no-op.
"""
from __future__ import annotations
import os
import json
import logging
from typing import List, Dict, Any, Optional
import requests

log = logging.getLogger(__name__)

API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
TIMEOUT = 60
ENABLED = bool(API_KEY)

if not ENABLED:
    log.info("ANTHROPIC_API_KEY not set. Claude review + scraper fallback will be skipped.")


# ===========================================================================
# Low-level call
# ===========================================================================
def _call_claude(prompt: str, max_tokens: int = 4000,
                 system: Optional[str] = None) -> Optional[str]:
    """Single Messages-API call. Returns text content, or None on failure."""
    if not ENABLED:
        return None
    body = {
        "model": MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
    }
    if system:
        body["system"] = system
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=body,
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            log.warning("Claude %d: %s", r.status_code, r.text[:200])
            return None
        content = r.json().get("content", [])
        # Collect all text blocks (Messages API can return multiple)
        texts = [blk.get("text", "") for blk in content if blk.get("type") == "text"]
        return "\n".join(t for t in texts if t) or None
    except Exception as e:
        log.warning("Claude call failed: %s", e)
        return None


def _strip_fences(txt: str) -> str:
    """Remove ```json / ``` wrappers if Claude added them despite instructions."""
    txt = txt.strip()
    if txt.startswith("```"):
        # drop first fence line
        txt = txt.split("\n", 1)[-1] if "\n" in txt else txt[3:]
        # drop closing fence
        if "```" in txt:
            txt = txt.rsplit("```", 1)[0]
    return txt.strip()


# ===========================================================================
# Mode 1 — Tier-1 recall reviewer (unchanged behavior)
# ===========================================================================
REVIEW_PROMPT = """Review these Tier-1 critical food recalls (Listeria/STEC/Botulinum/cereulide/biotoxins).

For each row, verify:
1. Pathogen classification is correct (Tier-1 vs Tier-2)
2. Outbreak flag accuracy (illness/cases reported = 1, single detection = 0)
3. URL points to a specific recall page, not a homepage/category
4. Critical info present: Company, Product, lot/batch identifier in Notes

Return strict JSON:
{"flags": [{"row_index": <int>, "severity": "high|medium|low", "issue": "...", "suggested_fix": {...}}]}
Only flag rows with real issues. Empty array if all clean.

Rows to review:
"""


def review_tier1(rows: List[Dict[str, Any]], batch_size: int = 15) -> List[Dict[str, Any]]:
    """Review only Tier-1 (critical) rows to stretch $5 credit."""
    if not ENABLED:
        return []
    tier1_rows = [(i, r) for i, r in enumerate(rows) if int(r.get("Tier", 2) or 2) == 1]
    if not tier1_rows:
        log.info("Claude review: no Tier-1 rows to review")
        return []
    log.info("Claude review: %d Tier-1 rows", len(tier1_rows))

    all_flags: List[Dict[str, Any]] = []
    for batch_start in range(0, len(tier1_rows), batch_size):
        chunk = tier1_rows[batch_start:batch_start + batch_size]
        chunk_data = [{"row_index": idx, **row} for idx, row in chunk]
        prompt = REVIEW_PROMPT + json.dumps(chunk_data, ensure_ascii=False, indent=2)
        txt = _call_claude(prompt)
        if not txt:
            continue
        try:
            data = json.loads(_strip_fences(txt))
            all_flags.extend(data.get("flags", []))
        except json.JSONDecodeError as e:
            log.warning("Claude JSON parse failed: %s | text=%s", e, txt[:200])
    log.info("Claude review: %d flags raised", len(all_flags))
    return all_flags


# ===========================================================================
# Mode 2 — HTML extraction (FALLBACK scraper, only when Gemini returns 0)
# ===========================================================================
EXTRACTION_SYSTEM = (
    "You are a senior food safety analyst extracting structured recall data from "
    "regulator HTML. Return ONLY strict JSON — no markdown, no prose, no commentary."
)

EXTRACTION_PROMPT = """Source: {agency} ({country})
Language: {language}
Page URL: {source_url}
{extra_hints}

Extract ONLY food recalls/alerts where the cause is a PATHOGEN, MICROBIAL CONTAMINATION,
or BIOLOGICAL TOXIN.
EXCLUDE: undeclared allergens, foreign objects (plastic/metal/glass), labeling errors,
mechanical issues, chemical/heavy metal contamination unless biological in origin.

Pathogens in scope: Listeria, Salmonella, E. coli / STEC / O157:H7, Clostridium botulinum,
Norovirus, Hepatitis A, Campylobacter, Cyclospora, Vibrio, Cronobacter sakazakii,
Bacillus cereus / cereulide, Aflatoxins, Ochratoxin A, Patulin, marine biotoxins
(DSP/PSP/ASP), Histamine, Shigella, Yersinia, other mycotoxins.

For each recall, return:
- Date (YYYY-MM-DD; recall publication or initiation date)
- Company (firm/producer name)
- Brand (commercial brand name; "—" if not stated)
- Product (full product description with size/lot if available)
- Pathogen (specific pathogen detected, e.g. "Listeria monocytogenes")
- Reason (short cause description)
- Class (recall class: "Recall", "Alert", "Class I/II/III", "Public Health Alert", etc.)
- URL (full deep-link to the specific recall page — NOT a homepage or category page)
- Outbreak (1 if illness/outbreak mentioned, else 0)
- Notes (distribution region, batch info, additional context)

CRITICAL: URL must be a specific recall page (e.g. .../fiche-rappel/12345).
NEVER return homepage URLs, category pages, or generic listing URLs.

Only include recalls published in the last {since_days} days when dates are visible.

Return exactly this JSON shape:
{{"recalls": [{{"Date":"...","Company":"...","Brand":"...","Product":"...","Pathogen":"...","Reason":"...","Class":"...","URL":"...","Outbreak":0,"Notes":"..."}}]}}

If no pathogen recalls are present, return: {{"recalls": []}}

HTML to analyze (truncated):
---
{html}
---"""


def extract_recalls_from_html(
    html: str,
    source_url: str,
    agency: str,
    country: str,
    language: str = "en",
    extra_hints: str = "",
    since_days: int = 30,
    max_tokens: int = 4000,
) -> List[Dict[str, Any]]:
    """
    Claude fallback extractor — mirrors the Gemini extraction contract.
    Returns a list of recall dicts (possibly empty). Safe to call when disabled.
    """
    if not ENABLED:
        return []
    prompt = EXTRACTION_PROMPT.format(
        html=html[:120_000],       # match Gemini's cap
        source_url=source_url,
        agency=agency,
        country=country,
        language=language,
        extra_hints=extra_hints or "",
        since_days=since_days,
    )
    txt = _call_claude(prompt, max_tokens=max_tokens, system=EXTRACTION_SYSTEM)
    if not txt:
        return []
    try:
        data = json.loads(_strip_fences(txt))
        recalls = data.get("recalls", []) or []
        if recalls:
            log.info("Claude fallback extracted %d recalls from %s", len(recalls), source_url)
        return recalls
    except json.JSONDecodeError as e:
        log.warning("Claude extract JSON parse failed: %s | text=%s", e, txt[:200])
        return []
