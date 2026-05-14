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

==========================================================================
AUDIT 2026-05-14 — Rate-limit hardening (shared with gap_finder_claude.py)
==========================================================================

The org-tier-1 Anthropic cap is 50,000 input tokens per minute. Web search
(used by gap_finder_claude) AND large prompts with cached system blocks
(used by claude_check.py — CLAUDE_CHECK_PROMPT is ~6K chars of instructions
on top of an 8K-char page_text payload) BOTH push per-call input-token
consumption to 8-15K tokens, exhausting the bucket after 3-6 consecutive
calls.

Production log 2026-05-14 04:02 showed claude_check.py hitting the cap on
rows 24-27, 29-30, 36, 39-40, 45 — fifteen of 48 rows skipped, including
foodstandards.gov.au cereulide-toxin-infant-formula-products (a likely
Tier-1 candidate left in Pending for 24 hours).

This module is the choke point: every caller of `_call_claude` benefits
from the fix automatically. The pattern (mirrors gap_finder_claude.py):

  1. Module-level rate-limit state populated from Anthropic's response
     headers after every call (success OR 429).
  2. Proactive pacing: before each call, if remaining input-token budget
     is below threshold, sleep until reset (capped at MAX_PACE_SLEEP_SEC
     and never longer than necessary).
  3. On HTTP 429, parse `retry-after` and retry ONCE.
  4. Error introspection: callers can inspect `last_call_error()` to
     distinguish rate-limit / timeout / HTTP-error / network-exception
     from the generic "Claude returned None" failure mode.

Env vars (all optional, sensible defaults):
  CLAUDE_PACE_THRESHOLD       (default 12000)  — sleep before next call if
                                                 remaining input tokens < this
  CLAUDE_MAX_PACE_SLEEP       (default 30)     — cap any single sleep at this
                                                 many seconds (proactive or 429)
  CLAUDE_CLIENT_HTTP_TIMEOUT  (default 60)     — per-request HTTP timeout

The retry+pacing layer is transparent to callers — `_call_claude` keeps
its existing signature and return type (`Optional[str]`). The only new
public surface is `last_call_error()` which returns one of:
  None         (call succeeded or has not been attempted yet)
  "rate_limit" (HTTP 429 after retry)
  "http_error" (non-200 non-429 HTTP response)
  "timeout"    (request exceeded HTTP timeout)
  "exception"  (network exception or other unexpected error)
"""
from __future__ import annotations
import os
import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
import requests

log = logging.getLogger(__name__)

API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
TIMEOUT = int(os.getenv("CLAUDE_CLIENT_HTTP_TIMEOUT", "60"))
ENABLED = bool(API_KEY)

# Rate-limit hardening config (audit 2026-05-14). See module docstring.
PACE_THRESHOLD_TOKENS = int(os.getenv("CLAUDE_PACE_THRESHOLD", "12000"))
MAX_PACE_SLEEP_SEC = float(os.getenv("CLAUDE_MAX_PACE_SLEEP", "30"))

if not ENABLED:
    log.info("ANTHROPIC_API_KEY not set. Claude review + scraper fallback will be skipped.")


# ===========================================================================
# Rate-limit state (module-level, shared across all _call_claude invocations)
# ===========================================================================

# Populated from Anthropic response headers after every call. None until
# the first call completes. Reset is a unix timestamp; remaining is the
# integer count of input tokens remaining in the current per-minute bucket.
_LAST_INPUT_REMAINING: Optional[int] = None
_LAST_INPUT_RESET_AT: Optional[float] = None

# Populated by _call_claude on every call so callers can introspect failure
# kind. None on success (or before first call). One of: "rate_limit",
# "http_error", "timeout", "exception" on failure.
_LAST_CALL_ERROR: Optional[str] = None


def last_call_error() -> Optional[str]:
    """Return the failure kind for the most recent _call_claude invocation.

    None means the call succeeded (or no call has been made yet). Otherwise
    one of: "rate_limit", "http_error", "timeout", "exception".

    Callers should inspect this immediately after a None return from
    `_call_claude` to produce a more specific error message. Module-level
    state is single-threaded — only safe to read between calls, not from
    parallel threads.
    """
    return _LAST_CALL_ERROR


def _update_ratelimit_state(headers) -> None:
    """Parse Anthropic rate-limit headers and stash the input-token
    remaining/reset values in module state for the next call's pacing
    decision.

    Anthropic returns several rate-limit headers per response. We track
    only the INPUT-TOKEN trio because that is the constraint that bit
    production on 2026-05-14 (50K/min cap, large prompts inflate input
    tokens beyond what Anthropic's standard tier allows).

    Silently no-ops if headers are missing (older Anthropic responses,
    or some error paths where Anthropic doesn't populate them).
    """
    global _LAST_INPUT_REMAINING, _LAST_INPUT_RESET_AT
    try:
        remaining_raw = headers.get("anthropic-ratelimit-input-tokens-remaining")
        reset_raw = headers.get("anthropic-ratelimit-input-tokens-reset")
        if remaining_raw is not None:
            _LAST_INPUT_REMAINING = int(remaining_raw)
        if reset_raw:
            # ISO-8601 with trailing Z; replace for Python's fromisoformat
            iso = reset_raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso)
            _LAST_INPUT_RESET_AT = dt.timestamp()
    except Exception as e:
        log.debug("Could not parse Anthropic rate-limit headers: %s", e)


def _maybe_pace_before_call() -> None:
    """Proactive sleep before the next call if budget is low.

    If we know the bucket has < PACE_THRESHOLD_TOKENS remaining and a
    reset is in the future, sleep until reset (capped by
    MAX_PACE_SLEEP_SEC). No-op if rate-limit state is not yet populated
    (first call of the run).

    This prevents the 429 from firing in the first place, which is
    strictly faster than firing-and-retrying.
    """
    if _LAST_INPUT_REMAINING is None or _LAST_INPUT_RESET_AT is None:
        return
    if _LAST_INPUT_REMAINING >= PACE_THRESHOLD_TOKENS:
        return

    now = time.time()
    seconds_until_reset = max(0.0, _LAST_INPUT_RESET_AT - now)
    sleep_sec = min(seconds_until_reset + 0.5, MAX_PACE_SLEEP_SEC)
    if sleep_sec <= 0.1:
        return
    log.info("Claude pacing: input-tokens-remaining=%d < %d, sleeping %.1fs "
             "(reset in %.1fs)",
             _LAST_INPUT_REMAINING, PACE_THRESHOLD_TOKENS,
             sleep_sec, seconds_until_reset)
    time.sleep(sleep_sec)


# ===========================================================================
# Low-level call
# ===========================================================================
def _call_claude(prompt: str, max_tokens: int = 4000,
                 system: Optional[str] = None,
                 cache_system: bool = True) -> Optional[str]:
    """Single Messages-API call. Returns text content, or None on failure.

    Audit 2026-05-08: prompt caching enabled by default. The `system`
    string is static across all calls in a given workflow run (Claude
    check uses CLAUDE_CHECK_SYSTEM, OpenRouter check via monkey-patch
    uses the same). Marking it `cache_control: ephemeral` makes the
    second-and-subsequent calls in a run pay the cached-input rate
    ($0.10/M Haiku, $0.30/M Sonnet) instead of fresh input. With ~70
    calls/run, this drops input cost by ~85%.

    Cache TTL is 5 minutes (Anthropic default). The Claude check + URL
    gate runs complete in ~3 minutes so caching covers the whole run.

    Set CLAUDE_NO_CACHE=1 to disable (for debugging or if you suspect
    cache poisoning is causing weird verdicts).

    Audit 2026-05-14: 429 retry + proactive pacing added. The call
    behavior is now:
      1. Read module rate-limit state. If remaining < threshold, sleep
         until reset (capped).
      2. POST to Anthropic.
      3. On HTTP 429: parse retry-after, sleep (capped), retry ONCE.
      4. On any response (success or error): parse rate-limit headers
         to update module state for the next call's pacing.
      5. On final failure: set _LAST_CALL_ERROR so the caller can
         distinguish rate-limit vs timeout vs HTTP-error vs exception.
    """
    global _LAST_CALL_ERROR
    _LAST_CALL_ERROR = None

    if not ENABLED:
        return None
    no_cache = os.getenv("CLAUDE_NO_CACHE", "").strip() in ("1", "true", "yes")
    use_cache = cache_system and not no_cache and system and len(system) > 1024 // 4
    # Anthropic minimum: cache breakpoints need ≥1024 tokens (Haiku/Sonnet).
    # System prompt of ~250+ chars is ~60 tokens — too small alone. We
    # check len > 256 chars as a proxy and fall through if too short.
    body: Dict[str, Any] = {
        "model": MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
    }
    if system:
        if use_cache and len(system) >= 256:
            # Send system as a content-block list with cache_control
            body["system"] = [
                {
                    "type": "text",
                    "text": system,
                    "cache_control": {"type": "ephemeral"},
                }
            ]
        else:
            body["system"] = system

    headers = {
        "x-api-key": API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    # Proactive pacing — sleep if Anthropic's last response said budget is low.
    _maybe_pace_before_call()

    for attempt in (1, 2):  # initial + up to one 429 retry
        try:
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=body,
                timeout=TIMEOUT,
            )
        except requests.exceptions.Timeout:
            log.warning("Claude HTTP timeout after %ds (attempt %d)",
                        TIMEOUT, attempt)
            _LAST_CALL_ERROR = "timeout"
            return None
        except Exception as e:
            log.warning("Claude call failed (attempt %d): %s", attempt, e)
            _LAST_CALL_ERROR = "exception"
            return None

        # Always update rate-limit state — Anthropic populates the headers
        # on 200, 429, and most error responses. This is what the NEXT
        # call's pacing decision reads.
        _update_ratelimit_state(r.headers)

        if r.status_code == 429:
            if attempt >= 2:
                log.warning("Claude 429 on retry — giving up: %s", r.text[:200])
                _LAST_CALL_ERROR = "rate_limit"
                return None
            retry_after_raw = (r.headers.get("retry-after")
                               or r.headers.get("Retry-After")
                               or "")
            try:
                retry_after = float(retry_after_raw)
            except (TypeError, ValueError):
                retry_after = MAX_PACE_SLEEP_SEC
            sleep_sec = min(max(retry_after, 1.0), MAX_PACE_SLEEP_SEC)
            log.warning("Claude 429: sleeping %.1fs then retrying "
                        "(retry-after=%r)", sleep_sec, retry_after_raw)
            time.sleep(sleep_sec)
            continue

        if r.status_code != 200:
            log.warning("Claude %d: %s", r.status_code, r.text[:200])
            _LAST_CALL_ERROR = "http_error"
            return None

        resp_json = r.json()
        # Audit 2026-05-08: log cache hits when present so operators can
        # verify caching is working and saving money.
        usage = resp_json.get("usage", {})
        if usage.get("cache_read_input_tokens"):
            log.debug("Claude cache HIT: %d cached, %d fresh, %d output",
                      usage.get("cache_read_input_tokens", 0),
                      usage.get("input_tokens", 0),
                      usage.get("output_tokens", 0))
        elif usage.get("cache_creation_input_tokens"):
            log.debug("Claude cache WRITE: %d cached, %d fresh, %d output",
                      usage.get("cache_creation_input_tokens", 0),
                      usage.get("input_tokens", 0),
                      usage.get("output_tokens", 0))
        content = resp_json.get("content", [])
        # Collect all text blocks (Messages API can return multiple)
        texts = [blk.get("text", "") for blk in content if blk.get("type") == "text"]
        return "\n".join(t for t in texts if t) or None

    # Unreachable in practice — the loop returns on every code path. Defensive.
    _LAST_CALL_ERROR = "exception"
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
# Mode 1b — Full-batch reviewer (replaces removed review/openai_client.py)
# ===========================================================================
# Mirrors the old openai_review's exact output schema so compute_rejections()
# in pipeline/run_all.py works without changes. Issue codes are the same set
# defined in REJECTION_CODES on the orchestrator side.
REVIEW_BATCH_SYSTEM = """You are a senior food safety analyst reviewing recall records for accuracy.
For each record, flag issues:
  - URL_INVALID: URL is a homepage, category page, or generic landing (not a specific recall)
  - URL_MISMATCH: URL likely doesn't match the described recall
  - MISSING_FIELD: required field empty (Date, Company, Product, Pathogen, URL)
  - PATHOGEN_INCONSISTENT: Pathogen value is contradicted by Reason text — e.g. Pathogen="Listeria monocytogenes" but Reason describes a Salmonella outbreak. Distinct from HALLUCINATED_PATHOGEN below: this flags ROWS WITH CONFLICTING evidence; HALLUCINATED flags rows with NO evidence.
  - DATE_FORMAT: date not in YYYY-MM-DD or seems wrong
  - COUNTRY_INCONSISTENT: Country doesn't match Source agency
  - DUPLICATE_RISK: looks like a duplicate of another recent recall
  - HALLUCINATED_PATHOGEN: Pathogen field is non-empty but neither the value NOR any source-language equivalent appears in the Reason or Notes fields (case-insensitive substring match). Source-language equivalents you must recognize:
      "Salmonella" ↔ "salmonella" / "salmonellen" (DE) / "salmonelle" (FR) / "salmonelas" (PT) / "salmonelosis" (ES)
      "Listeria monocytogenes" ↔ "listeria" / "listerien" (DE) / "listéria" (FR) / "l. monocytogenes"
      "Shiga toxin-producing E. coli (STEC)" ↔ "stec" / "vtec" / "ehec" / "shiga" / "shigatoxin" / "e. coli o157" / "escherichia coli"
      "Ochratoxin" ↔ "ochratoxin" / "ochratoxine" (FR) / "ocratoxina" (IT/ES)
      "Aflatoxin" ↔ "aflatoxin" / "aflatoxine" (FR/DE) / "aflatossina" (IT)
      "Clostridium botulinum" ↔ "botulinum" / "botulism" / "botulisme" (FR) / "botulismus" (DE)
      "Undeclared meat" ↔ "fleisch" (DE) / "viande" (FR) / "carne" (IT/ES) / "meat"
      "Undeclared allergen" ↔ "allergen" / "allergie" / "allergène" / "allergeen"
      "Foreign body" ↔ "fremdkörper" (DE) / "corps étranger" (FR) / "corpo estraneo" (IT) / "metal" / "plastic" / "glass"
      "Mislabeling" ↔ "falschdeklaration" (DE) / "étiquetage erroné" (FR) / "mislabel" / "mislabelled"
    Skip this check (do NOT flag) if Reason+Notes combined is shorter than 20 characters — absence of detail is not evidence of hallucination. This catches Gemini fabricating a pathogen unsupported by any other text in the row (e.g. Pathogen="Ochratoxin" while Reason describes undeclared meat).
  - EXTRACTION_GARBAGE: any of (a) Company == Brand byte-for-byte AND Company contains >5 whitespace-separated words; (b) Product is just a bare domain like "canada.ca", "fda.gov", "fsis.usda.gov"; (c) any of Company / Brand / Product contains an HTML or JS artifact: "{socials", "window.", "querySelector", "&nbsp;", "<title>", "</title>", "[data-progress-bar]", "(function", "document.cookie", "addEventListener".

Return strictly as JSON: {"reviews": [{"row_index": <int>, "issues": ["CODE",...], "suggested_fixes": {"FieldName": "value"}, "confidence": 0.0-1.0}]}
Only include rows with issues. Empty array if all clean."""


def review_batch(rows: List[Dict[str, Any]], batch_size: int = 20) -> List[Dict[str, Any]]:
    """Full review pass (all rows, not just Tier-1). Replaces openai_review.

    Returns list of {row_index, issues, suggested_fixes, confidence}, identical
    schema to the retired openai_client.review_batch — orchestrator code in
    pipeline/run_all.py treats both functions interchangeably.
    """
    if not ENABLED:
        return []
    all_reviews: List[Dict[str, Any]] = []
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        prompt = (
            f"Review these {len(chunk)} food recall records (row_index relative "
            f"to this batch starting at 0):\n\n"
            + json.dumps(chunk, ensure_ascii=False, indent=2)
        )
        txt = _call_claude(prompt, system=REVIEW_BATCH_SYSTEM)
        if not txt:
            continue
        try:
            data = json.loads(_strip_fences(txt))
            for rev in data.get("reviews", []):
                # Adjust row_index back to global index (matches openai_review)
                rev["row_index"] = rev.get("row_index", 0) + i
                all_reviews.append(rev)
        except json.JSONDecodeError as e:
            log.warning("Claude review_batch JSON parse failed: %s | text=%s",
                        e, txt[:200])
    log.info("Claude review_batch: %d issues across %d rows",
             len(all_reviews), len(rows))
    return all_reviews


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

Extract food recalls/alerts where the cause is ANY of the following hazard categories:
  (a) PATHOGENS, MICROBIAL CONTAMINATION, or BIOLOGICAL TOXINS
  (b) RODENTICIDES / RAT POISON (bromadiolone, brodifacoum, difethialone,
      difenacoum, chlorophacinone — including deliberate tampering)
  (c) HEAVY METAL contamination (lead, cadmium, arsenic, mercury) at levels
      exceeding regulatory limits
  (d) PHYSICAL HAZARDS (glass fragments, metal fragments, plastic fragments,
      foreign bodies posing injury or choking risk)
  (e) MYCOTOXINS at levels exceeding regulatory limits or indicative
      values, including aflatoxins (B1/B2/G1/G2/M1), ochratoxin A, patulin,
      Alternaria toxins (alternariol/AOH/AME, tenuazonic acid), Fusarium
      toxins (fumonisin, zearalenone, deoxynivalenol/DON, nivalenol, T-2,
      HT-2), citrinin, ergot alkaloids (Claviceps)

EXCLUDE: undeclared allergens, labeling errors, mechanical/packaging issues,
and pesticide residues above MRL — unless linked to one of (a)-(e).

Canonical hazard names (use these exact strings in the Pathogen field):
  Biological — Listeria monocytogenes, Salmonella spp., E. coli O157:H7,
    STEC, Clostridium botulinum, Norovirus, Hepatitis A, Campylobacter,
    Cyclospora, Vibrio, Cronobacter sakazakii, Bacillus cereus / cereulide,
    marine biotoxins (DSP/PSP/ASP), Histamine (scombrotoxin), Shigella,
    Yersinia.
  Mycotoxins — "Aflatoxins", "Ochratoxin A", "Patulin", "Alternaria toxins",
    "Fumonisin", "Zearalenone", "Deoxynivalenol (DON)", "T-2 / HT-2 toxin",
    "Citrinin", "Ergot alkaloids", or the generic "Mycotoxin" if the specific
    toxin is not named.
  Rodenticides — "Rodenticide (rat poison)" (preferred), optionally suffixed
    with the active ingredient e.g. "Rodenticide (bromadiolone)".
  Heavy metals — "Lead (Pb) contamination", "Cadmium (Cd) contamination",
    "Arsenic (As) contamination", "Mercury (Hg) contamination", or the
    generic "Heavy metal contamination".
  Physical — "Glass fragments", "Metal fragments", "Plastic fragments",
    "Physical/foreign-body contamination".

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
- Reason (short cause description)
- Class (recall class: "Recall", "Alert", "Class I/II/III", "Public Health Alert", etc.)
- URL (full deep-link to the specific recall page — NOT a homepage or category page)
- Outbreak (1 if illness/outbreak mentioned OR tampering targeting vulnerable consumers, else 0)
- Notes (distribution region, batch info, additional context)

CRITICAL: URL must be a specific recall page (e.g. .../fiche-rappel/12345).
NEVER return homepage URLs, category pages, or generic listing URLs.

Only include recalls published in the last {since_days} days when dates are visible.

Return exactly this JSON shape:
{{"recalls": [{{"Date":"...","Company":"...","Brand":"...","Product":"...","Pathogen":"...","Reason":"...","Class":"...","URL":"...","Outbreak":0,"Notes":"..."}}]}}

If no in-scope hazard recalls are present, return: {{"recalls": []}}

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
