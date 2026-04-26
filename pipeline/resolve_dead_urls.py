"""
resolve_dead_urls.py — one-off scraper URL resolver.

Takes the CSV output from `python -m pipeline.audit_scrapers`, finds rows
classified URL_FIX (HTTP 404/301/302), asks Gemini 2.5 Flash with Google
Search grounding to propose the CURRENT canonical recalls-page URL on
each agency's domain, HTTP-verifies each proposal, and writes a patch
file (JSON).

Why Gemini and not OpenAI/Claude:
  Earlier versions used OpenAI gpt-4o-mini (no web search) which
  hallucinated agency-listing-page URLs that pattern-matched real layouts
  but didn't actually exist. Gemini 2.5 Flash with native Google Search
  grounding only returns URLs that appeared in real search results.

Explicit design choice: this tool does NOT auto-modify scraper files. It
writes proposed changes to url_fixes_YYYY-MM-DD.json, George reviews them,
then applies with the companion `apply_url_fixes.py` (or by hand).

This is a one-off. Not scheduled. Run manually via GitHub Actions:
  Actions -> Resolve Dead Scraper URLs -> Run workflow

Total cost: free tier on Gemini 2.5 Flash (1500 req/day per key).
"""
from __future__ import annotations
import argparse
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._base import make_session, fetch  # noqa: E402
from pipeline.url_gate_gemini import (  # noqa: E402
    _call_gemini_grounded, _collect_gemini_keys, _strip_fences,
)

GEMINI_ENABLED = bool(_collect_gemini_keys())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("resolve-dead-urls")


RESOLVE_SYSTEM = (
    "You are a web-sourcing specialist finding the CURRENT canonical URL for "
    "a food regulator's public recall / food-alert listing page, given the "
    "agency's name and its previous (now 404) URL. Return ONLY strict JSON. "
    "Do NOT invent URLs — if unsure, return an empty string. The URL must be "
    "on the SAME agency domain as the broken URL."
)


RESOLVE_SYSTEM = (
    "You are a web-sourcing specialist finding the CURRENT canonical URL "
    "for a food regulator's public recall / food-alert listing page, "
    "given the agency's name and its previous (now 404) URL. Use Google "
    "Search to find the current page. Return ONLY strict JSON. NEVER "
    "invent URLs — every URL you return MUST appear in your Google Search "
    "results. The URL must be on the SAME root domain as the broken URL "
    "(or a known sibling subdomain if the agency restructured). If your "
    "search finds no plausible current listing page, return an empty "
    "string and explain why."
)


RESOLVE_PROMPT = """A scraper was pulling food recall listings from this URL, and it now returns HTTP 404:

  Agency:   {agency}
  Country:  {country}
  Broken URL: {bad_url}

Use Google Search to find the CURRENT official listing page for food recalls / alerts / withdrawals on this agency's website.

Search strategy:
  1. site:<root_domain_of_broken_url> recalls
  2. site:<root_domain_of_broken_url> "food alert" OR withdrawal
  3. <agency> food recall listing page <year>
  4. Verify each candidate URL appears in your search results.

Hard rules:
  1. The URL MUST be on the SAME root domain as the broken URL (or a
     known sibling subdomain if the agency restructured — e.g. moved
     from www.foo.gov to alerts.foo.gov).
  2. Must be a LISTING page (index of many recalls), NOT a single-recall
     page and NOT the agency's homepage.
  3. If the agency has both EN and a local-language version, prefer the
     EN version unless the previous URL was local-language.
  4. If your Google Search finds no obvious current listing page (agency
     deprecated recall publishing, moved to a different platform, etc.),
     return empty string with reasoning.
  5. Do NOT guess. If your search results don't clearly identify the
     current listing page, return empty string.

Return strict JSON:
{{"new_url": "https://...", "confidence": 0.0-1.0, "reasoning": "one line"}}

Example for a similar case (BVL Germany):
{{"new_url": "https://www.lebensmittelwarnung.de/bvl-lmw-de/app/process/warningpublic/actualWarnings/search;jsessionid=...", "confidence": 0.8, "reasoning": "BVL moved recall list to the warningpublic/actualWarnings path in 2024"}}
"""


def propose_new_url(agency: str, country: str, bad_url: str) -> Optional[Dict]:
    if not GEMINI_ENABLED:
        log.error("GEMINI_API_KEY not set")
        return None
    prompt = RESOLVE_PROMPT.format(agency=agency, country=country, bad_url=bad_url)
    txt = _call_gemini_grounded(prompt, system=RESOLVE_SYSTEM, max_tokens=1024)
    if not txt:
        return None
    txt = _strip_fences(txt).strip()
    try:
        data = json.loads(txt)
    except json.JSONDecodeError as e:
        m = re.search(r"\{[^{}]*\}", txt, re.S)
        if not m:
            log.warning("JSON parse fail for %s: %s | %s", agency, e, txt[:200])
            return None
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError:
            log.warning("JSON parse fail for %s: %s | %s", agency, e, txt[:200])
            return None
    new_url = (data.get("new_url") or "").strip()
    if not new_url:
        return None
    if not new_url.lower().startswith(("http://", "https://")):
        return None
    if new_url.lower() == bad_url.lower():
        return None  # echo-back
    return data


def verify(url: str, session) -> Dict:
    """HEAD+GET probe. Returns dict with status, ok, detail, size."""
    resp = fetch(session, url)
    if resp is None:
        return {"ok": False, "status": 0, "detail": "network failure", "size": 0}
    status = resp.status_code
    size = len(resp.content) if resp.content else 0
    # Same classification rules as audit_scrapers.py
    if status == 200 and size >= 4000:
        return {"ok": True, "status": status, "detail": "200 OK healthy", "size": size}
    if status == 200:
        return {"ok": False, "status": status, "detail": f"200 but small ({size} bytes)", "size": size}
    if status in (301, 302, 307, 308):
        return {"ok": False, "status": status, "detail": "redirect — try the new Location", "size": size}
    if status == 403:
        return {"ok": True, "status": status, "detail": "403 (often bot-block on gov sites — acceptable)", "size": size}
    return {"ok": False, "status": status, "detail": f"HTTP {status}", "size": size}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit-csv", default="scraper_audit.csv",
                    help="Path to CSV from audit_scrapers.py")
    ap.add_argument("--output", default=None,
                    help="Path to write JSON patch file (default: url_fixes_YYYY-MM-DD.json)")
    ap.add_argument("--classifications", default="URL_FIX",
                    help="Comma-separated list of classifications to resolve (default URL_FIX)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Max rows to resolve (default: all matching)")
    args = ap.parse_args()

    if not GEMINI_ENABLED:
        log.error("GEMINI_API_KEY not set")
        return 1

    csv_path = Path(args.audit_csv)
    if not csv_path.exists():
        log.error("Audit CSV not found: %s", csv_path)
        log.error("Run `python -m pipeline.audit_scrapers --csv scraper_audit.csv` first")
        return 1

    target_cls = {c.strip() for c in args.classifications.split(",")}
    log.info("Reading audit CSV: %s", csv_path)
    with csv_path.open(encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))
    targets = [r for r in all_rows if r.get("classification") in target_cls]
    log.info("Found %d rows matching classification(s): %s",
             len(targets), ", ".join(target_cls))
    if args.limit:
        targets = targets[:args.limit]
        log.info("Capped to first %d", args.limit)

    session = make_session(timeout=20)
    fixes: List[Dict] = []

    for i, row in enumerate(targets, 1):
        agency = row.get("agency", "?")
        country = row.get("country", "?")
        bad_url = row.get("url", "")
        log.info("[%d/%d] %s (%s)", i, len(targets), agency, country)
        log.info("    bad: %s", bad_url[:80])

        proposal = propose_new_url(agency, country, bad_url)
        if not proposal:
            log.info("    -> no proposal / Gemini gave up")
            fixes.append({
                "agency": agency, "country": country,
                "old_url": bad_url, "new_url": None,
                "status": "NO_PROPOSAL",
                "detail": "Gemini could not propose a URL via grounded search",
            })
            continue

        new_url = proposal["new_url"]
        confidence = proposal.get("confidence", 0.0)
        reasoning = proposal.get("reasoning", "")
        log.info("    -> proposed: %s (conf=%.2f)", new_url[:80], confidence)
        log.info("       reasoning: %s", reasoning[:120])

        probe = verify(new_url, session)
        log.info("    -> probe: %s", probe["detail"])

        fixes.append({
            "agency": agency, "country": country,
            "old_url": bad_url,
            "new_url": new_url,
            "gemini_confidence": confidence,
            "gemini_reasoning": reasoning,
            "probe_status": probe["status"],
            "probe_ok": probe["ok"],
            "probe_detail": probe["detail"],
            "status": "VERIFIED_LIVE" if probe["ok"] else "PROPOSED_BUT_DEAD",
        })

    # Write output
    out_path = Path(args.output) if args.output else \
        ROOT / f"url_fixes_{datetime.now().strftime('%Y-%m-%d')}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "fixes": fixes,
        }, f, indent=2, ensure_ascii=False)
    log.info("Wrote patch file: %s", out_path)

    # Summary
    verified = sum(1 for f in fixes if f["status"] == "VERIFIED_LIVE")
    proposed_dead = sum(1 for f in fixes if f["status"] == "PROPOSED_BUT_DEAD")
    no_proposal = sum(1 for f in fixes if f["status"] == "NO_PROPOSAL")
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("  Verified live  : %d  (apply these, high confidence)", verified)
    log.info("  Proposed dead  : %d  (review before applying)", proposed_dead)
    log.info("  No proposal    : %d  (agency likely hopeless — rely on gap-finder)",
             no_proposal)
    log.info("")
    log.info("Review %s, then apply verified fixes by editing scrapers/<region>/<agency>.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
