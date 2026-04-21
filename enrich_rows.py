"""
Gemini client with key rotation for FREE tier (1500 req/day per key).
Set up to N keys via env vars: GEMINI_API_KEY_1, GEMINI_API_KEY_2, ... GEMINI_API_KEY_5
Falls back to GEMINI_API_KEY (single key) if numbered ones absent.
"""
from __future__ import annotations
import os
import json
import time
import logging
from typing import List, Dict, Any, Optional
from itertools import cycle
from threading import Lock

log = logging.getLogger(__name__)

MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")  # free tier: 1500 req/day per key
TIMEOUT = 60


def _collect_keys() -> List[str]:
    """Read all GEMINI_API_KEY_* env vars."""
    keys = []
    for i in range(1, 11):
        v = os.getenv(f"GEMINI_API_KEY_{i}")
        if v:
            keys.append(v.strip())
    if not keys:
        v = os.getenv("GEMINI_API_KEY")
        if v:
            keys.append(v.strip())
    return keys


_keys = _collect_keys()
if not _keys:
    log.warning("No GEMINI_API_KEY_* found in env. Gemini features will fail.")
    _key_cycle = None
else:
    _key_cycle = cycle(_keys)
    log.info("Loaded %d Gemini key(s) for rotation.", len(_keys))

_lock = Lock()
_call_counts: Dict[str, int] = {k: 0 for k in _keys}
_blocked_keys: set = set()  # keys hit rate limit today


def _next_key() -> Optional[str]:
    """Get next available key (round-robin, skipping blocked)."""
    if not _key_cycle:
        return None
    with _lock:
        for _ in range(len(_keys)):
            k = next(_key_cycle)
            if k not in _blocked_keys:
                _call_counts[k] = _call_counts.get(k, 0) + 1
                return k
    return None


def _call_gemini(prompt: str, system: str = "", max_retries: int = 3) -> Optional[str]:
    """Make Gemini API call with key rotation on rate limit."""
    import requests
    for attempt in range(max_retries):
        key = _next_key()
        if not key:
            log.error("No Gemini keys available (all blocked or none configured)")
            return None
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent?key={key}"
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 8192,
                "responseMimeType": "application/json",
            },
        }
        if system:
            body["systemInstruction"] = {"parts": [{"text": system}]}
        try:
            r = requests.post(url, json=body, timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                cands = data.get("candidates", [])
                if cands and cands[0].get("content", {}).get("parts"):
                    return cands[0]["content"]["parts"][0].get("text", "")
                return None
            elif r.status_code == 429:
                log.warning("Gemini rate limit on key ...%s, rotating", key[-6:])
                with _lock:
                    _blocked_keys.add(key)
                continue
            else:
                log.warning("Gemini error %d: %s", r.status_code, r.text[:200])
                time.sleep(2)
        except Exception as e:
            log.warning("Gemini call failed: %s", e)
            time.sleep(2)
    return None


# ===== High-level functions =====

EXTRACTION_PROMPT = """You are a food safety analyst extracting structured recall data from HTML.

Source: {agency} ({country})
Language: {language}
Page URL: {source_url}
{extra_hints}

Extract food recalls/alerts where the cause is ANY of the following hazard categories:
  (a) PATHOGENS, MICROBIAL CONTAMINATION, or BIOLOGICAL TOXINS
  (b) RODENTICIDES / RAT POISON (bromadiolone, brodifacoum, difethialone,
      difenacoum, chlorophacinone — including cases of deliberate tampering)
  (c) HEAVY METAL contamination (lead, cadmium, arsenic, mercury) at levels
      exceeding regulatory limits
  (d) PHYSICAL HAZARDS (glass fragments, metal fragments, plastic fragments,
      foreign bodies posing injury or choking risk)

EXCLUDE: undeclared allergens, labeling errors, mechanical/packaging issues,
and pesticide residues above MRL — unless they are linked to one of (a)-(d).

Canonical hazard names (use these exact strings in the Pathogen field):
  Biological — Listeria monocytogenes, Salmonella spp., E. coli O157:H7,
    STEC, Clostridium botulinum, Norovirus, Hepatitis A, Campylobacter,
    Cyclospora, Vibrio, Cronobacter sakazakii, Bacillus cereus / cereulide,
    Aflatoxins, Ochratoxin A, Patulin, marine biotoxins (DSP/PSP/ASP),
    Histamine (scombrotoxin), Shigella, Yersinia, Mycotoxin.
  Rodenticides — "Rodenticide (rat poison)" (preferred), optionally suffixed
    with the active ingredient e.g. "Rodenticide (bromadiolone)".
  Heavy metals — "Lead (Pb) contamination", "Cadmium (Cd) contamination",
    "Arsenic (As) contamination", "Mercury (Hg) contamination", or the
    generic "Heavy metal contamination" if the element is not specified.
  Physical — "Glass fragments", "Metal fragments", "Plastic fragments",
    or "Physical/foreign-body contamination".

For criminal tampering cases (e.g. rat poison deliberately added to a jar):
  - Prefix Reason with "Tampering: …"
  - Set Outbreak=1 if vulnerable consumers (infants, elderly, immuno-
    compromised) are the likely target OR illnesses are already reported.

For each recall, return:
- Date (YYYY-MM-DD; recall publication or initiation date)
- Company (firm/producer name)
- Brand (commercial brand name; "—" if not stated)
- Product (full product description with size/lot if available)
- Pathogen (canonical hazard name from the lists above)
- Reason (short cause description, e.g. "Listeria contamination detected
  during routine testing" or "Tampering: jars found laced with bromadiolone")
- Class (recall class: "Recall", "Alert", "Class I/II/III", "Public Health Alert", etc.)
- URL (full deep-link URL to the specific recall page; NOT a homepage or category page)
- Outbreak (1 if illness/outbreak mentioned OR tampering targeting vulnerable consumers, else 0)
- Notes (distribution region, batch info, additional context)

CRITICAL: For URL field, use only specific recall page URLs (e.g. .../fiche-rappel/12345 or .../recall-alert/specific-product).
NEVER return homepage URLs, category pages, or generic listing URLs.

Return strictly this JSON shape (no markdown, no commentary):
{{"recalls": [{{"Date": "...", "Company": "...", "Brand": "...", "Product": "...", "Pathogen": "...", "Reason": "...", "Class": "...", "URL": "...", "Outbreak": 0, "Notes": "..."}}]}}

If no in-scope hazard recalls are present, return: {{"recalls": []}}

HTML to analyze (truncated):
---
{html}
---
"""


def extract_recalls_from_html(
    html: str,
    source_url: str,
    agency: str,
    country: str,
    language: str = "en",
    extra_hints: str = "",
    since_days: int = 30,
) -> List[Dict[str, Any]]:
    """Use Gemini to extract structured recall rows from messy HTML."""
    prompt = EXTRACTION_PROMPT.format(
        html=html, source_url=source_url, agency=agency,
        country=country, language=language, extra_hints=extra_hints,
    )
    txt = _call_gemini(prompt)
    if not txt:
        return []
    try:
        data = json.loads(txt)
        return data.get("recalls", []) or []
    except json.JSONDecodeError as e:
        log.warning("Gemini JSON parse failed: %s | text=%s", e, txt[:200])
        return []


def enrich_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Use Gemini to fill in missing/inconsistent fields on a recall row."""
    prompt = f"""You are a food safety analyst enriching a recall record.

Current record:
{json.dumps(row, ensure_ascii=False, indent=2)}

Fill in or correct any missing/inconsistent fields. Return strict JSON with the same keys.
- Normalize Pathogen to canonical name (e.g. "Listeria monocytogenes" not "listeria")
- Normalize Country to English (e.g. "Germany" not "DE" or "Deutschland")
- Set Class if missing ("Recall" or "Alert")
- Outbreak = 1 only if illnesses/cases are explicitly mentioned

Return ONLY a JSON object with the fields, no commentary."""
    txt = _call_gemini(prompt)
    if not txt:
        return row
    try:
        return {**row, **json.loads(txt)}
    except json.JSONDecodeError:
        return row


def get_call_stats() -> Dict[str, Any]:
    """Diagnostic: returns key usage + blocked status."""
    return {
        "total_keys": len(_keys),
        "blocked_keys": len(_blocked_keys),
        "calls_per_key": {f"...{k[-6:]}": _call_counts.get(k, 0) for k in _keys},
    }
