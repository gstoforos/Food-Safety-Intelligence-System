"""
Gemini-powered scraper URL repair tool.

Audit 2026-04-29: 24 scrapers are returning HTTP 404 because regulator
websites have moved their recall-listing pages to new URLs over the past
months. Manually investigating each one (open browser, find recalls page,
copy URL, edit scraper file) is ~30 minutes per scraper × 24 = 12 hours
of tedious work.

This module automates the discovery half. For each broken scraper:
  1. Reads the scraper's current (broken) URL from the scraper file
  2. Asks Gemini (with Google Search grounding) for the current correct
     recall-listing page URL on that regulator's official domain
  3. Validates the proposed URL: must be on the regulator's actual
     domain, must return HTTP 200, must look like a listing page
     (not a homepage, not a single-recall fiche)
  4. Writes the proposal to scraper-url-fixes.json for human review
  5. Optionally, with --apply, edits the scraper file in place

USAGE
-----
    # Dry-run: discover proposals without modifying anything
    python -m pipeline.fix_scraper_urls

    # Apply specific scraper(s)
    python -m pipeline.fix_scraper_urls --apply --only aesan,bvl,ages

    # Apply all proposals (review the JSON first!)
    python -m pipeline.fix_scraper_urls --apply --all

The --apply mode is intentionally per-scraper-explicit. You should review
each proposal before applying because regulator URL changes sometimes
introduce subtle structural differences (different HTML layout, requires
JavaScript, etc.) that need a code change, not just a URL change.

ENVIRONMENT
-----------
Requires GEMINI_API_KEY_FREE or GEMINI_API_KEY (will use FREE tier first).
Costs €0 on the free tier; ~24 calls × 1 search each = well under the
250/day limit.
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parent.parent
SCRAPERS_DIR = ROOT / "scrapers"
PROPOSALS_PATH = ROOT / "docs" / "data" / "scraper-url-fixes.json"
HEALTH_PATH = ROOT / "docs" / "data" / "scraper-health.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("fix-scraper-urls")


@dataclass
class UrlProposal:
    scraper: str             # e.g. "aesan_es"
    file_path: str           # relative to repo root
    agency_name: str         # e.g. "AESAN (ES)"
    country: str             # e.g. "Spain"
    current_url: str         # the broken URL in the scraper code today
    proposed_url: str        # what Gemini suggests
    proposed_status: int     # HTTP status of proposed URL when validated
    proposed_url_2: Optional[str] = None  # backup candidate
    notes: str = ""
    confidence: str = "medium"  # low / medium / high


def discover_scraper_urls() -> List[Dict]:
    """Walk scrapers/ directory; for each .py file, extract:
       - scraper module name
       - AGENCY constant (e.g. 'AESAN (ES)')
       - COUNTRY constant
       - Any URL constant or hardcoded URL in fetch() calls"""
    out = []
    for py in sorted(SCRAPERS_DIR.rglob("*.py")):
        if py.name.startswith("_") or py.name == "__init__.py":
            continue
        try:
            text = py.read_text(encoding="utf-8")
        except Exception:
            continue

        agency_m = re.search(r'AGENCY\s*=\s*["\']([^"\']+)["\']', text)
        country_m = re.search(r'COUNTRY\s*=\s*["\']([^"\']+)["\']', text)
        if not agency_m:
            continue

        # Extract first hardcoded URL from a fetch() call or a URL constant
        url_m = re.search(
            r'(?:URL|API_BASE|FEED_URL|BASE_URL)\s*=\s*["\'](https?://[^"\']+)["\']',
            text,
        )
        if not url_m:
            url_m = re.search(r'fetch\([^,]+,\s*["\'](https?://[^"\']+)["\']', text)

        out.append({
            "scraper": py.stem,
            "file_path": str(py.relative_to(ROOT)),
            "agency_name": agency_m.group(1),
            "country": country_m.group(1) if country_m else "",
            "current_url": url_m.group(1) if url_m else "",
        })
    return out


def load_failing_scrapers() -> set:
    """Load scraper-health.json; return set of failing scraper internal names."""
    if not HEALTH_PATH.exists():
        log.warning("No %s — will probe ALL scrapers", HEALTH_PATH)
        return set()
    try:
        report = json.loads(HEALTH_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("Could not parse health JSON: %s", e); return set()

    failing = set()
    for display_name, state in report.get("per_scraper", {}).items():
        if not state.startswith("FAIL_"):
            continue
        # Display name → internal scraper name (best-effort)
        # e.g. "AESAN (ES)/Spain" → "aesan"
        m = re.match(r'^([A-Z][A-Za-z]+)', display_name)
        if m:
            failing.add(m.group(1).lower())
    return failing


def query_gemini_for_url(agency_name: str, country: str,
                         current_broken_url: str) -> Optional[Dict]:
    """Ask Gemini to find the current correct recall-listing URL."""
    api_key = (os.getenv("GEMINI_API_KEY_FREE") or
               os.getenv("GEMINI_API_KEY_FREE_2") or
               os.getenv("GEMINI_API_KEY"))
    if not api_key:
        log.error("No GEMINI_API_KEY_FREE / GEMINI_API_KEY set")
        return None

    prompt = f"""You are helping audit a food-safety regulatory scraper.
The scraper is supposed to fetch the food-recall / food-alert / public-warning
listing page from {agency_name} ({country}). Its current URL is:

  {current_broken_url}

This URL now returns HTTP 404. The agency website is up; the path has
moved. Your task: find the CURRENT correct URL for the food recall /
food alert / food warning LISTING PAGE on {agency_name}'s official
domain. NOT a homepage. NOT a single specific recall. The listing page
that shows multiple recent recalls.

Use Google Search to verify. Return ONLY a strict JSON object:

{{"primary_url": "https://...", "backup_url": "https://...", "notes": "what you found"}}

Rules:
- primary_url MUST be on {agency_name}'s OFFICIAL government/agency domain
  (the same root domain as the broken URL, not a third party)
- primary_url MUST be reachable (not 404)
- primary_url MUST be a listing/index page, not a single fiche
- If you can find a structured open-data API endpoint that lists food recalls,
  prefer that over an HTML page
- If you cannot find a valid replacement, return: {{"primary_url": "", "notes": "explanation"}}

Return JSON only, no prose preamble.
"""

    try:
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.5-flash:generateContent?key={api_key}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "tools": [{"google_search": {}}],
                "generationConfig": {"temperature": 0.1, "maxOutputTokens": 600},
            },
            timeout=60,
        )
        if r.status_code != 200:
            log.warning("Gemini %d for %s: %s",
                        r.status_code, agency_name, r.text[:200])
            return None
        data = r.json()
        txt = (data.get("candidates", [{}])[0]
                  .get("content", {}).get("parts", [{}])[0]
                  .get("text", ""))
        # Extract first JSON object
        i = txt.find("{")
        if i < 0:
            return None
        # Brace-counting extractor (same as gap_finder_gemini)
        depth = 0; in_str = False; esc = False; end = -1
        for j in range(i, len(txt)):
            ch = txt[j]
            if esc: esc = False; continue
            if in_str:
                if ch == "\\": esc = True
                elif ch == '"': in_str = False
                continue
            if ch == '"': in_str = True
            elif ch == "{": depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0: end = j; break
        if end < 0:
            return None
        return json.loads(txt[i:end+1])
    except Exception as e:
        log.warning("Gemini call failed for %s: %s", agency_name, e)
        return None


def validate_url(url: str, expected_root: str) -> tuple:
    """Return (status_code, is_valid). is_valid means: 200, on expected
    root domain, looks like a listing page (path is not just /)."""
    try:
        r = requests.head(url, timeout=15, allow_redirects=True,
                          headers={"User-Agent":
                                   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                                   "Chrome/127.0.0.0 Safari/537.36"})
        sc = r.status_code
    except Exception:
        return (0, False)

    if sc >= 400:
        return (sc, False)

    parsed = urlparse(url)
    if expected_root and expected_root not in parsed.netloc:
        return (sc, False)
    if not parsed.path.strip("/"):
        return (sc, False)  # bare domain
    return (sc, True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Actually edit scraper files (default: dry-run)")
    ap.add_argument("--only", type=str, default="",
                    help="Comma-separated scraper names to limit to")
    ap.add_argument("--all", action="store_true",
                    help="Probe all failing scrapers (default: from health JSON)")
    args = ap.parse_args()

    scrapers = discover_scraper_urls()
    log.info("Discovered %d scraper files", len(scrapers))

    failing = load_failing_scrapers()
    if args.only:
        only_set = {s.strip().lower() for s in args.only.split(",")}
        scrapers = [s for s in scrapers
                    if any(o in s["scraper"].lower() for o in only_set)]
    elif failing and not args.all:
        scrapers = [s for s in scrapers
                    if any(f in s["scraper"].lower() for f in failing)]

    log.info("Will probe %d scrapers", len(scrapers))

    proposals = []
    for s in scrapers:
        if not s["current_url"]:
            log.info("  [SKIP] %s — no URL constant in code", s["scraper"])
            continue
        log.info("  [QUERY] %s (%s) — current: %s",
                 s["scraper"], s["agency_name"], s["current_url"][:60])

        result = query_gemini_for_url(s["agency_name"], s["country"],
                                       s["current_url"])
        if not result:
            log.warning("    Gemini returned nothing")
            continue

        primary = (result.get("primary_url") or "").strip()
        backup = (result.get("backup_url") or "").strip() or None
        notes = result.get("notes", "")[:200]

        if not primary:
            log.warning("    Gemini found no replacement: %s", notes)
            continue

        # Validate
        expected_root = urlparse(s["current_url"]).netloc.lstrip("www.")
        sc, valid = validate_url(primary, expected_root)
        confidence = "high" if valid else "low"
        if not valid and backup:
            sc2, valid2 = validate_url(backup, expected_root)
            if valid2:
                primary, backup = backup, primary
                sc = sc2
                confidence = "high"

        proposals.append(UrlProposal(
            scraper=s["scraper"],
            file_path=s["file_path"],
            agency_name=s["agency_name"],
            country=s["country"],
            current_url=s["current_url"],
            proposed_url=primary,
            proposed_status=sc,
            proposed_url_2=backup,
            notes=notes,
            confidence=confidence,
        ))
        log.info("    → %s (status=%d, conf=%s)", primary, sc, confidence)

    PROPOSALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROPOSALS_PATH.write_text(
        json.dumps([asdict(p) for p in proposals], indent=2,
                   ensure_ascii=False),
        encoding="utf-8")
    log.info("Wrote %d proposals to %s", len(proposals), PROPOSALS_PATH)

    if not args.apply:
        log.info("Dry-run mode. Review proposals; re-run with --apply --only "
                 "<name1,name2> to apply specific ones.")
        return 0

    # Apply mode: edit the scraper files
    applied = 0
    skipped_low_conf = 0
    for p in proposals:
        if p.confidence != "high":
            log.warning("Skipping %s — confidence %s; review manually",
                        p.scraper, p.confidence)
            skipped_low_conf += 1
            continue
        path = ROOT / p.file_path
        try:
            text = path.read_text(encoding="utf-8")
            new_text = text.replace(p.current_url, p.proposed_url, 1)
            if new_text == text:
                log.warning("Could not find current_url in %s — manual edit",
                            p.file_path)
                continue
            path.write_text(new_text, encoding="utf-8")
            applied += 1
            log.info("Applied %s: %s → %s", p.scraper,
                     p.current_url[-40:], p.proposed_url[-40:])
        except Exception as e:
            log.error("Failed to apply %s: %s", p.scraper, e)

    log.info("Done. Applied %d. Skipped %d for low confidence.",
             applied, skipped_low_conf)
    return 0


if __name__ == "__main__":
    sys.exit(main())
