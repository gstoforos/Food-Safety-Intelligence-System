"""
pipeline/synthesis_writer.py — AI analytical synthesis for the weekly + monthly
summary JSON files. The Option-B post-processor.

============================================================================
WHAT THIS DOES
============================================================================
Reads `docs/data/weekly-summary-latest.json` (or `monthly-summary-latest.json`),
generates a 2-3 sentence analytical synthesis of the period's recall data
grounded in the existing stats fields, writes the prose back into the
`ai_lead_paragraph` field, and commits the updated JSON.

The dashboard's AI Synthesis panel and the subscriber email's Intelligence
Summary section both already read this field — they were wired up for it
in advance. Both now light up automatically.

============================================================================
WHY A SEPARATE POST-PROCESSOR
============================================================================
The weekly builder (`docs/build_weekly_report_afts.py`, 2,259 lines) and its
monthly twin are the largest files in the repo. Inlining an LLM call there
would:
  - bloat already-huge files
  - couple synthesis failures (LLM down, rate-limited) to structural-report
    failures
  - make provider swaps (Anthropic → AFTS in-house model later this year)
    a multi-file edit

A separate post-processor decouples those concerns. The builders ship
`ai_lead_paragraph: ""` exactly as today; this script populates it after
the build commits. If this script fails, the structural report still ships
intact — subscribers just get the email without the Intelligence Summary
block (mailer already cleanly skips it on empty), and the dashboard panel
falls back to its static "Verification posture" sentence.

============================================================================
ARCHITECTURE — PROVIDER-ABSTRACTED BACKENDS
============================================================================
`_SYNTHESIS_BACKENDS` is an ordered list of `(name, call_fn)` tuples. Each
backend is tried in order; the first one to return a valid synthesis wins.
When AFTS ships its in-house model later this year, swapping in is one
line:

    _SYNTHESIS_BACKENDS = [
        ("afts_v1",   _call_afts_v1),     # ← new line at the top
        ("anthropic", _call_anthropic),
        ("gemini",    _call_gemini),
    ]

If every LLM backend fails, `_deterministic_synthesis()` writes a
data-grounded template paragraph from the stats themselves. The
`ai_lead_paragraph` field is NEVER left empty when this script succeeds in
running — only when all of (every LLM AND the deterministic fallback)
crash, which would itself be a code bug not a runtime condition.

============================================================================
CADENCE (run from .github/workflows/synthesis-writer-*.yml)
============================================================================
Weekly:  Friday 08:30 Athens — after the 08:00 weekly builder commits at ~08:15
Monthly: Day-1 09:30 Athens — after the 09:00 monthly builder commits at ~09:15

============================================================================
CLI
============================================================================
    python -m pipeline.synthesis_writer --cadence weekly
    python -m pipeline.synthesis_writer --cadence monthly
    python -m pipeline.synthesis_writer --cadence weekly --no-commit  # dry-run
    python -m pipeline.synthesis_writer --cadence weekly --force      # overwrite existing
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("synthesis-writer")


# ============================================================================
# PROMPT BUILDERS — assemble the analytical context for the LLM
# ============================================================================

_PROMPT_RULES = """\
You are the food safety analyst for AFTS (Advanced Food-Tech Solutions).
Your audience is QA directors, food safety officers, and regulatory affairs
specialists at food processors.

Write a 2-3 sentence analytical synthesis of the recall data below.
Total length: 100-220 words.

Required structure:
  Sentence 1 (the lead): the most newsworthy finding — typically the total
  count with its delta versus the prior period, the dominant pathogen with
  its share, OR the highest-severity outbreak. Cite specific numbers.

  Sentence 2 (the comparison): one comparative observation — vs prior
  period, geographic concentration, severity escalation, pathogen mix
  shift, or jurisdictional pattern.

  Sentence 3 (optional, the implication): one specific takeaway for food
  processors or QA officers — what to watch, which control to re-verify,
  which regulator's pipeline to monitor.

Banned in the output:
  - Marketing adjectives ("powerful", "robust", "innovative",
    "comprehensive", "unprecedented", "cutting-edge")
  - First-person plural ("we", "our")
  - Hedging modals ("may", "might", "could potentially")
  - Specific product or brand recommendations
  - Quoted phrases or em-dashes used as sentence breaks
  - Bullet points, headers, emojis
  - Self-reference ("this report", "this analysis", "as shown above")

Cite the pathogens, counts, percentages, and country names from the DATA
section verbatim. Plain prose. Direct sentences.

Output only the synthesis paragraph. No preamble, no headers, no closing
remarks. The first character of your response must be the first letter of
the first sentence.
"""


def _format_top_threats(threats: list, max_items: int = 5) -> str:
    """Format the top threats list as compact numbered bullets for the prompt."""
    lines = []
    for t in (threats or [])[:max_items]:
        rank = t.get("rank", "?")
        pathogen = t.get("pathogen") or t.get("pathogen_raw") or "Unknown"
        company = t.get("company") or t.get("brand") or "Unknown company"
        country = t.get("country") or "Unknown"
        tier = t.get("tier")
        ob = " [OUTBREAK]" if t.get("outbreak") else ""
        tier_tag = f" [T{tier}]" if tier in (1, 2) else ""
        lines.append(f"  {rank}. {pathogen}{tier_tag}{ob} — {company} ({country})")
    return "\n".join(lines) if lines else "  (none reported)"


def _build_weekly_prompt(summary: dict) -> str:
    """Build the weekly analytical prompt from the summary JSON."""
    stats = summary.get("stats") or {}
    leading = summary.get("leading_pathogen") or {}
    delta = stats.get("delta", 0)
    delta_pct = stats.get("delta_pct", 0)
    delta_signed = f"+{delta}" if delta > 0 else str(delta)
    delta_pct_signed = f"+{delta_pct}%" if delta_pct > 0 else f"{delta_pct}%"
    range_str = (
        f"{summary.get('week_start_display', '?')} – "
        f"{summary.get('week_end_display', '?')}"
    )
    return (
        _PROMPT_RULES
        + "\n\nDATA — Week "
        + f"{summary.get('week_num', '?')}, {summary.get('year', '?')} ({range_str}):\n"
        f"  Total recalls: {stats.get('total', 0)}\n"
        f"  Change vs prior week: {delta_signed} ({delta_pct_signed})\n"
        f"  Tier-1 critical: {stats.get('tier1', 0)}\n"
        f"  Confirmed outbreaks: {stats.get('outbreaks', 0)}\n"
        f"  Leading pathogen: {leading.get('name', 'Mixed')} — "
        f"{leading.get('cases', 0)} cases ({leading.get('pct', 0)}% of total)\n"
        f"  Countries with recalls: {summary.get('country_count', 0)}\n\n"
        "Top critical incidents this week:\n"
        + _format_top_threats(summary.get("top_threats"))
        + "\n\nWrite the synthesis paragraph now."
    )


def _build_monthly_prompt(summary: dict) -> str:
    """Build the monthly analytical prompt from the summary JSON.
    Monthly JSON has richer fields (hotspots, clusters, emerging) so the
    prompt can ask for slightly deeper analytical patterns."""
    stats = summary.get("stats") or {}
    leading = summary.get("leading_pathogen") or {}
    delta = stats.get("delta", 0)
    delta_pct = stats.get("delta_pct", 0)
    delta_signed = f"+{delta}" if delta > 0 else str(delta)
    delta_pct_signed = f"+{delta_pct}%" if delta_pct > 0 else f"{delta_pct}%"

    # Monthly enrichments. The builder writes top_countries / top_sources as
    # [name, count] tuples and `emerging` as dicts with key 'pathogen'.
    def _fmt_tuple_list(rows, n=3):
        out = []
        for row in (rows or [])[:n]:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                out.append(f"{row[0]} ({row[1]})")
            elif isinstance(row, dict):
                out.append(f"{row.get('name','?')} ({row.get('count','?')})")
        return ", ".join(out) if out else "none reported"

    top_countries = _fmt_tuple_list(summary.get("top_countries"))
    top_sources   = _fmt_tuple_list(summary.get("top_sources"))
    emerging_rows = (summary.get("emerging") or [])[:3]
    emerging = ", ".join([
        f"{e.get('pathogen', e.get('name', '?'))}"
        + (f" (+{e.get('growth_pct','?')}%)" if e.get('growth_pct') is not None else "")
        for e in emerging_rows
    ]) or "none flagged"

    month_label = f"{summary.get('month_name', '?')} {summary.get('year', '?')}"

    return (
        _PROMPT_RULES
        + "\n\nDATA — "
        + f"{month_label} (monthly review):\n"
        f"  Total recalls: {stats.get('total', 0)}\n"
        f"  Change vs prior month: {delta_signed} ({delta_pct_signed})\n"
        f"  Tier-1 critical: {stats.get('tier1', 0)}\n"
        f"  Confirmed outbreaks: {stats.get('outbreaks', 0)}\n"
        f"  Leading pathogen: {leading.get('name', 'Mixed')} — "
        f"{leading.get('cases', 0)} cases ({leading.get('pct', 0)}% of total)\n"
        f"  Top jurisdictions: {top_countries}\n"
        f"  Top regulatory sources: {top_sources}\n"
        f"  Emerging pathogen patterns: {emerging}\n\n"
        "Top critical incidents this month:\n"
        + _format_top_threats(summary.get("top_threats"))
        + "\n\nWrite the synthesis paragraph now."
    )


# ============================================================================
# LLM BACKENDS — provider-abstracted. Each returns text or None on failure.
# ============================================================================

def _call_anthropic(prompt: str, timeout: int = 60) -> Optional[str]:
    """Call Anthropic via the existing REST integration used by the weekly
    builder's review_with_claude(). Env: CLAUDE_API_KEY or ANTHROPIC_API_KEY."""
    key = os.environ.get("CLAUDE_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        log.info("anthropic backend: no API key configured — skipping")
        return None
    try:
        import requests  # type: ignore
    except ImportError:
        log.warning("anthropic backend: requests not installed — skipping")
        return None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": os.environ.get(
                    "AFTS_SYNTHESIS_MODEL", "claude-haiku-4-5-20251001"
                ),
                "max_tokens": 600,
                "temperature": 0.4,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=timeout,
        )
        if r.status_code != 200:
            log.warning("anthropic backend HTTP %d: %s",
                        r.status_code, r.text[:200])
            return None
        body = r.json()
        blocks = body.get("content") or []
        # The first text block is the synthesis (no tool use in this call).
        for blk in blocks:
            if blk.get("type") == "text":
                return blk.get("text", "").strip()
        return None
    except Exception as e:  # noqa: BLE001
        log.warning("anthropic backend exception: %s", e)
        return None


def _call_gemini(prompt: str, timeout: int = 60) -> Optional[str]:
    """Call Gemini via google.genai. Tries GEMINI_API_KEY_FREE first, then
    GEMINI_API_KEY_1..5. Mirrors the rotation used by gap_finder_gemini so
    this writer benefits from the same quota pool. No Google Search
    grounding for synthesis (the data is in the prompt; no need to search)."""
    keys = []
    for env in ("GEMINI_API_KEY_FREE", "GEMINI_API_KEY_1", "GEMINI_API_KEY_2",
                "GEMINI_API_KEY_3", "GEMINI_API_KEY_4", "GEMINI_API_KEY_5",
                "GEMINI_API_KEY", "GOOGLE_API_KEY"):
        v = (os.environ.get(env) or "").strip()
        if v and v not in keys:
            keys.append(v)
    if not keys:
        log.info("gemini backend: no API key configured — skipping")
        return None
    try:
        from google import genai  # type: ignore
        from google.genai import types  # type: ignore
    except ImportError:
        log.warning("gemini backend: google-genai not installed — skipping")
        return None
    model_name = os.environ.get("AFTS_SYNTHESIS_GEMINI_MODEL", "gemini-2.5-flash")
    for idx, key in enumerate(keys, 1):
        try:
            client = genai.Client(api_key=key)
            resp = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.4,
                    max_output_tokens=600,
                ),
            )
            text = (getattr(resp, "text", None) or "").strip()
            if text:
                log.info("gemini backend: key #%d/%d succeeded (%s)",
                         idx, len(keys), model_name)
                return text
            log.info("gemini backend: key #%d returned empty text", idx)
        except Exception as e:  # noqa: BLE001
            log.warning("gemini backend key #%d exception: %s", idx, e)
            continue
    return None


# Ordered backend list. New providers (AFTS in-house model) plug in at the top.
_SYNTHESIS_BACKENDS: list[Tuple[str, Callable[[str, int], Optional[str]]]] = [
    ("anthropic", _call_anthropic),
    ("gemini",    _call_gemini),
]


# ============================================================================
# VALIDATION — keep obvious-bad output out of production
# ============================================================================

_MIN_LEN = 80
_MAX_LEN = 1400  # ~220 words at ~6 chars/word avg
_BANNED_PHRASES = (
    "as an ai", "as a language model", "i cannot", "i'm sorry",
    "here is", "here's the synthesis", "synthesis:", "summary:",
    "in conclusion", "to summarize",
)


def _validate_synthesis(text: str) -> bool:
    """Reject obviously-bad LLM output. Catches the common AI failure modes
    (preamble, refusal, role-playing) without being so strict it rejects
    valid prose."""
    if not text or not isinstance(text, str):
        return False
    s = text.strip()
    if len(s) < _MIN_LEN:
        log.warning("synthesis too short (%d chars < %d)", len(s), _MIN_LEN)
        return False
    if len(s) > _MAX_LEN:
        log.warning("synthesis too long (%d chars > %d)", len(s), _MAX_LEN)
        return False
    low = s.lower()
    for ph in _BANNED_PHRASES:
        if ph in low:
            log.warning("synthesis contains banned phrase '%s'", ph)
            return False
    # Must contain at least one digit (we asked the model to cite numbers).
    if not re.search(r"\d", s):
        log.warning("synthesis contains no numbers — likely hallucinated")
        return False
    return True


# ============================================================================
# DETERMINISTIC FALLBACK — template prose from real stats
# ============================================================================
# If every LLM backend fails, this generates a data-grounded synthesis from
# the JSON fields directly. Less analytical than LLM output, but ALWAYS
# truthful (it can only state what the data shows) and ALWAYS non-empty.

_PATHOGEN_IMPLICATIONS = {
    "listeria": "Processors of ready-to-eat dairy, deli, and cured products "
                "should re-verify environmental monitoring frequency and "
                "post-lethality intervention validation.",
    "salmonella": "Suppliers across poultry, eggs, low-moisture ingredients, "
                  "and produce should re-confirm validation records for "
                  "kill-step interventions and process-water sanitation.",
    "e. coli": "Beef, leafy greens, and unpasteurized juice suppliers should "
               "re-examine pre-harvest controls and process-water sanitation "
               "against the persistent STEC pressure.",
    "stec": "Beef and leafy-greens suppliers should re-examine pre-harvest "
            "controls against the persistent STEC detection rate.",
    "cronobacter": "Powdered-formula manufacturers should re-confirm dry-process "
                   "environmental monitoring frequency against the FDA's "
                   "heightened watch on this pathogen.",
    "bacillus": "Cooked-rice and refrigerated ready-meal processors should "
                "re-verify hot-holding and cooling-curve compliance against "
                "cereulide-related rejections.",
    "cereulide": "Cooked-rice and refrigerated ready-meal processors should "
                 "re-verify hot-holding and cooling-curve compliance.",
    "botulin": "Low-acid canned food and refrigerated long-shelf-life "
               "processors should re-confirm thermal validation records and "
               "modified-atmosphere packaging controls.",
    "clostridium": "Low-acid canned food and refrigerated long-shelf-life "
                   "processors should re-confirm thermal validation records.",
}


def _deterministic_synthesis(summary: dict, cadence: str) -> str:
    """Template-driven synthesis when all LLM backends fail. Always returns
    a non-empty, data-grounded paragraph."""
    stats = summary.get("stats") or {}
    leading = summary.get("leading_pathogen") or {}
    total = stats.get("total", 0)
    delta = stats.get("delta", 0)
    delta_pct = stats.get("delta_pct", 0)
    tier1 = stats.get("tier1", 0)
    outbreaks = stats.get("outbreaks", 0)
    leading_name = leading.get("name") or "Mixed pathogens"
    leading_pct = leading.get("pct", 0)
    leading_cases = leading.get("cases", 0)

    if cadence == "weekly":
        period = f"Week {summary.get('week_num', '?')}"
        prior = "the prior week"
    else:
        period = f"{summary.get('month_name', 'This month')}"
        prior = "the prior month"

    # Sentence 1 — counts + delta.
    if delta > 0:
        delta_phrase = f"up {delta} versus {prior} ({delta_pct:+}%)"
    elif delta < 0:
        delta_phrase = f"down {abs(delta)} versus {prior} ({delta_pct:+}%)"
    else:
        delta_phrase = f"flat versus {prior}"

    # Tier-1 clause — grammar-aware singular/plural/zero.
    if tier1 > 1:
        tier_clause = f", of which {tier1} were Tier-1 critical incidents"
    elif tier1 == 1:
        tier_clause = ", of which 1 was a Tier-1 critical incident"
    else:
        tier_clause = ", with no Tier-1 critical incidents recorded"

    s1 = (
        f"{period} closed with {total} pathogen recalls across "
        f"{summary.get('country_count', 'multiple')} jurisdictions, "
        f"{delta_phrase}{tier_clause}"
    )
    if outbreaks > 1:
        s1 += f"; {outbreaks} confirmed outbreaks were declared in the same window"
    elif outbreaks == 1:
        s1 += "; one confirmed outbreak was declared in the same window"
    s1 += "."

    # Sentence 2 — leading pathogen.
    if leading_pct >= 30:
        share_phrase = f"dominated the pathogen mix at {leading_pct}% of total recalls"
    elif leading_pct >= 15:
        share_phrase = f"led the pathogen mix at {leading_pct}% of total recalls"
    else:
        share_phrase = f"accounted for {leading_pct}% of total recalls in a fragmented week"
    s2 = (
        f"{leading_name} {share_phrase} ({leading_cases} cases), "
        f"sustaining the pattern of multi-jurisdictional regulatory pressure "
        f"on this pathogen class."
    )

    # Sentence 3 — actionable implication tuned to dominant pathogen.
    s3 = (
        "Food processors should monitor the regulatory pipelines of the "
        "leading jurisdictions for follow-up actions and adjusted enforcement "
        "priorities."
    )
    leading_low = leading_name.lower()
    for key, msg in _PATHOGEN_IMPLICATIONS.items():
        if key in leading_low:
            s3 = msg
            break

    return " ".join([s1, s2, s3])


# ============================================================================
# DRIVER
# ============================================================================

def generate_synthesis(summary: dict, cadence: str) -> Tuple[str, str]:
    """Try every backend in order; fall back to deterministic template.
    Returns (synthesis_text, source_label) — source_label is the backend
    that succeeded (e.g. "anthropic", "gemini", "deterministic")."""
    if cadence == "weekly":
        prompt = _build_weekly_prompt(summary)
    elif cadence == "monthly":
        prompt = _build_monthly_prompt(summary)
    else:
        raise ValueError(f"Unknown cadence: {cadence}")

    for name, fn in _SYNTHESIS_BACKENDS:
        log.info("Trying backend: %s", name)
        try:
            text = fn(prompt, 60)
        except Exception as e:  # noqa: BLE001
            log.warning("Backend %s raised: %s", name, e)
            continue
        if text and _validate_synthesis(text):
            log.info("✓ Synthesis written by %s (%d chars)", name, len(text))
            return text.strip(), name
        log.info("Backend %s did not produce a usable synthesis — next", name)

    log.warning("All LLM backends failed — using deterministic fallback")
    return _deterministic_synthesis(summary, cadence), "deterministic"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cadence", choices=["weekly", "monthly"], required=True,
                    help="Which summary JSON to process.")
    ap.add_argument("--data-dir", default="docs/data",
                    help="Directory containing the summary JSON files.")
    ap.add_argument("--no-commit", action="store_true",
                    help="Write the JSON locally but do not git push.")
    ap.add_argument("--force", action="store_true",
                    help="Overwrite an existing non-empty ai_lead_paragraph.")
    args = ap.parse_args()

    filename = f"{args.cadence}-summary-latest.json"
    summary_path = Path(args.data_dir) / filename
    if not summary_path.exists():
        log.error("Summary JSON not found: %s", summary_path)
        return 1

    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        log.error("Summary JSON is malformed: %s", e)
        return 2

    existing = (summary.get("ai_lead_paragraph") or "").strip()
    if existing and not args.force:
        log.info("ai_lead_paragraph already populated (%d chars) — skipping. "
                 "Pass --force to overwrite.", len(existing))
        return 0
    if existing and args.force:
        log.info("Overwriting existing ai_lead_paragraph (%d chars, --force)",
                 len(existing))

    synthesis, source = generate_synthesis(summary, args.cadence)
    summary["ai_lead_paragraph"] = synthesis
    summary["ai_synthesis_source"] = source  # audit trail
    summary["ai_synthesis_generated_utc"] = datetime.now(timezone.utc).isoformat()

    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("Updated %s with %d-char synthesis (source=%s)",
             summary_path, len(synthesis), source)

    if args.no_commit:
        log.info("--no-commit set — skipping git push.")
        return 0

    try:
        from pipeline.commit_github import git_commit_and_push
        ok = git_commit_and_push(
            repo_dir=ROOT,
            files=[str(summary_path.relative_to(ROOT))],
            message=(
                f"AI synthesis ({args.cadence}): +{len(synthesis)} chars "
                f"via {source} ({datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%MZ')})"
            ),
        )
        if not ok:
            log.warning("Commit returned False — synthesis written locally, push failed")
            return 0  # not a hard failure; JSON is on disk
        log.info("Committed and pushed.")
    except Exception as e:  # noqa: BLE001
        log.warning("Commit step raised (synthesis still written locally): %s", e)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
