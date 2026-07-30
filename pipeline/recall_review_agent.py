#!/usr/bin/env python3
"""
recall_review_agent.py
======================

Self-hosted Qwen 2.5 7B review agent. Takes a candidate recall row (from
any gap-finder), visits its regulator/news page, and verifies + fills +
corrects EVERY field against the page text until it is 100% faithful to
the source — then decides APPROVE (→ Recalls) or REJECT (→ Weekly_Rejected).

Runs entirely on the AFTS Qwen VPS via LlamaClient (Tailscale, no API
key). Nothing here depends on Claude/Gemini. Future-proof: when the VPS
moves to a Mac, only LLAMA_BASE_URL changes.

Design — mirrors the existing official_feeds extractor contract:
  - Same PENDING_COLUMNS schema.
  - Same "be faithful, never invent, empty string if not stated" rules.
  - Same LlamaClient.chat() tool-calling loop.
  - Reuses article_fetcher.fetch_html for the TLS-impersonated page fetch
    and searx_search when a field needs corroboration or a better URL.

The agent's job per row:
  1. Fetch the row's URL. If dead/soft-404, use web_search to find the
     correct official page for THIS recall (company + product + hazard).
  2. Read the page text. For each field (Date, Company, Brand, Product,
     Pathogen, Reason, Country, Region) confirm it matches the page, or
     correct it. Fill any blank the page supports. Never invent.
  3. Verify the recall is real, in-scope (2026+, food, Tier-1 hazard
     universe), and not a duplicate.
  4. Return a corrected row + verdict {approve|reject|needs_human} + a
     per-field provenance note.

Verdicts:
  approve       — every required field verified against the page, in scope
  reject        — not a recall / out of scope / pre-2026 / dead URL / dup
  needs_human   — page unreadable or genuinely ambiguous; stays in Pending

CLI:
  python -m pipeline.recall_review_agent --xlsx docs/data/recalls.xlsx \\
      --commit false            # dry run prints verdicts
  --commit true                 # writes approvals→Recalls, rejects→Weekly_Rejected
  --source-filter "EFET"        # review only rows from one source
  --limit N                     # cap rows this run

Env:
  LLAMA_BASE_URL, LLAMA_MODEL     (from llama_client)
  REVIEW_MAX_PAGE_CHARS  default 12000
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# ── Reuse the existing pipeline infrastructure ───────────────────────────
# These imports match the official_feeds subsystem. If run outside that
# package context, the try/except keeps the module importable for testing.
try:
    from pipeline.official_feeds.agents import llama_client
except Exception:  # pragma: no cover
    llama_client = None
try:
    from pipeline.official_feeds.agents import searx_search
except Exception:  # pragma: no cover
    searx_search = None
try:
    from pipeline.official_feeds import article_fetcher
except Exception:  # pragma: no cover
    article_fetcher = None


PENDING_COLUMNS = [
    "Date", "Source", "Company", "Brand", "Product", "Pathogen", "Reason",
    "Class", "Country", "Region", "Tier", "Outbreak", "URL", "Notes",
    "ScrapedAt", "Status", "RejectedBy",
]

MAX_PAGE_CHARS = int(os.environ.get("REVIEW_MAX_PAGE_CHARS", "12000"))


# ─── Tool-calling schema for the agent ───────────────────────────────────

def _tool_schema() -> List[Dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": "fetch_page",
                "description": "Fetch the readable text of a web page by URL. "
                               "Use this to read the recall page before "
                               "judging any field.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string",
                                "description": "Full http(s) URL to fetch."},
                    },
                    "required": ["url"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "web_search",
                "description": "Search the web for the official regulator "
                               "page for a recall. Use ONLY if the row's URL "
                               "is dead or wrong. Query with company + product "
                               "+ hazard.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                    },
                    "required": ["query"],
                },
            },
        },
    ]


def _fetch_page_text(url: str) -> Tuple[str, str]:
    """Fetch a page's readable text. Self-contained (no CountryConfig).

    Tries curl_cffi with Chrome TLS impersonation first (matches the
    pipeline's article_fetcher / _akamai_fetch approach for sites that
    fingerprint Python's TLS), falls back to stdlib requests. Returns
    (text, status).
    """
    html = ""
    status = "ok"
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/131.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        from curl_cffi import requests as cffi_requests  # type: ignore
        resp = cffi_requests.get(url, headers=headers, timeout=30,
                                 impersonate="chrome131",
                                 allow_redirects=True)
        html = resp.text or ""
        if resp.status_code >= 400:
            return "", f"http_{resp.status_code}"
    except Exception:
        try:
            import requests as _requests
            resp = _requests.get(url, headers=headers, timeout=30,
                                 allow_redirects=True)
            html = resp.text or ""
            if resp.status_code >= 400:
                return "", f"http_{resp.status_code}"
        except Exception as e:  # noqa: BLE001
            return "", f"error_{type(e).__name__}"

    # Strip HTML to readable text
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        text = "\n".join(ln.strip() for ln in text.splitlines() if ln.strip())
    except Exception:
        text = html
    return text[:MAX_PAGE_CHARS], status


def _make_tool_executor(seen_urls: set) -> Callable[[str, dict], str]:
    def execute(name: str, args: dict) -> str:
        if name == "fetch_page":
            url = (args or {}).get("url", "").strip()
            if not url:
                return json.dumps({"error": "no url"})
            text, status = _fetch_page_text(url)
            seen_urls.add(url)
            return json.dumps({"url": url, "status": status, "text": text})
        if name == "web_search":
            q = (args or {}).get("query", "").strip()
            if searx_search is None:
                return json.dumps({"error": "searx unavailable"})
            try:
                results = searx_search.search(q)
                slim = [{"title": r.get("title", ""), "url": r.get("url", ""),
                         "content": (r.get("content", "") or "")[:200]}
                        for r in (results or [])[:6]]
                return json.dumps({"query": q, "results": slim})
            except Exception as e:  # noqa: BLE001
                return json.dumps({"error": f"{type(e).__name__}: {e}"})
        return json.dumps({"error": f"unknown tool {name}"})
    return execute


# ─── Prompts ─────────────────────────────────────────────────────────────

REVIEW_SYSTEM = (
    "You are a senior food-safety analyst performing FINAL verification of "
    "a recall record before it is published on a public dashboard. You have "
    "two tools: fetch_page (read a URL) and web_search (find the official "
    "page if the given URL is dead). You MUST read the actual regulator page "
    "before judging any field. You never invent facts: if the page does not "
    "state something, leave that field an empty string. You answer ONLY with "
    "the final JSON object described by the user — no prose, no markdown."
)


def build_review_prompt(row: Dict[str, Any]) -> str:
    def g(k):
        return row.get(k, "") if row.get(k) not in (None,) else ""
    return f"""Verify this recall row against its source page.

═══ ROW (claimed) ═══
Date     : {g('Date')}
Source   : {g('Source')}
Country  : {g('Country')}
Company  : {g('Company')}
Brand    : {g('Brand')}
Product  : {g('Product')}
Pathogen : {g('Pathogen')}
Reason   : {g('Reason')}
Class    : {g('Class')}
Region   : {g('Region')}
Outbreak : {g('Outbreak')}
URL      : {g('URL')}

STEPS:
1. Call fetch_page on the URL. If it is dead, a listing page, or clearly the
   wrong recall, call web_search with company + product + hazard to find the
   correct OFFICIAL regulator page, then fetch_page that.
2. Read the page. For EACH field, decide if the row's value matches the page.
   - If it matches: keep it.
   - If it is wrong: correct it to what the page says.
   - If it is blank and the page states it: fill it.
   - If the page does not state it: leave it empty.
   Translate Product / Pathogen / Reason / Region to English. Keep Company in
   its original language. Use ORIGINAL publication date (YYYY-MM-DD), not any
   later "update" date.
   CRITICAL HAZARD / SCOPE CHECK (a common gap-finder error):
   - This system tracks PATHOGEN / microbial-contamination food recalls only.
     If the page shows the recall is for an UNDECLARED ALLERGEN (peanuts,
     milk, soy, gluten, sulphites, egg, sesame, mustard, etc.), a FOREIGN
     BODY (glass, plastic, metal), or a purely CHEMICAL / quality / labelling
     issue with no microbial pathogen — it is OUT OF SCOPE. Set
     verdict = "reject" with reason naming the actual hazard. Do NOT approve
     it and do NOT leave a pathogen name on it.
   - Watch for the internal contradiction that signals this error: the
     Pathogen field says an organism (e.g. "Listeria") but the Reason / page
     says "undeclared allergen". The PAGE wins — if the page says allergen,
     reject as out of scope.
   - Only a recall whose actual hazard is a microbial pathogen
     (Listeria, Salmonella, E. coli/STEC, Cronobacter, botulinum, norovirus,
     hepatitis A, etc.) explicitly stated on the page is IN SCOPE.
3. COMPLETENESS (strict — no missing data may be promoted). Every field that
   the page makes available MUST be found and filled: Date, Company, Brand
   (if the page names one), Product, Pathogen, Reason, Country, Region (if
   the page/state names one). If a field is genuinely absent from the page,
   it may stay empty — but you must have actually READ the page and confirmed
   it is not there. If you cannot confirm a REQUIRED field (Date, Company OR
   Brand, Product, Pathogen, URL) from the page, do NOT approve → return
   "needs_human". Never approve a row with a required field you could not
   verify or fill from the source.
4. Decide scope:
   - reject if: not a recall page; pre-2026 date; pet/animal food; undeclared
     allergen / foreign body / chemical / labelling-only (OUT OF SCOPE per
     above); dead URL with no findable official page; duplicate.
   - approve ONLY if: it is a real 2026+ microbial-pathogen food recall AND
     every required field is verified/filled from the page AND every
     page-available field has been filled.
   - needs_human if: the page cannot be read, a required field cannot be
     confirmed, or the case is genuinely ambiguous.
5. VERIFY THE OUTBREAK FLAG (strict — this drives the tier). Set outbreak = 1
   ONLY if the page (or a linked official health page) states one or more of:
     • a specific number of confirmed/probable illnesses/cases
       ("166 illnesses", "two cases", "26 hospitalised")
     • the word outbreak / épidémie / Ausbruch / brote / epidemia describing
       THIS hazard (not generic boilerplate)
     • an epidemiological investigation triggered by reported illness
     • death(s) attributed to the hazard
     • a named ongoing outbreak with published case counts
   Set outbreak = 0 if: the notice says no reported illnesses / aucun cas
   signalé; routine sampling caught it (lab test only, product not consumed);
   criminal tampering with no consumption; or "outbreak" appears only in a
   brand name or boilerplate. Default to 0 when in doubt.

Return ONLY this JSON (no markdown):
{{
  "verdict": "approve" | "reject" | "needs_human",
  "reason": "<one line>",
  "verified_url": "<the URL you actually confirmed, may differ from input>",
  "outbreak": 0 | 1,
  "outbreak_evidence": "<verbatim quote from page, max 200 chars, '' if 0>",
  "fields": {{
    "Date": "", "Company": "", "Brand": "", "Product": "",
    "Pathogen": "", "Reason": "", "Country": "", "Region": ""
  }},
  "provenance": "<which fields you corrected/filled and from where>"
}}"""


# ─── The agent ───────────────────────────────────────────────────────────

def review_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Run the Qwen review agent on one row. Returns a dict:
       {verdict, reason, verified_url, fields{...}, provenance}."""
    if llama_client is None or not llama_client.is_configured():
        return {"verdict": "needs_human",
                "reason": "llama_client not configured (LLAMA_BASE_URL unset)",
                "verified_url": row.get("URL", ""), "fields": {},
                "provenance": ""}
    if llama_client.is_open():
        return {"verdict": "needs_human",
                "reason": "llama circuit breaker open",
                "verified_url": row.get("URL", ""), "fields": {},
                "provenance": ""}

    seen: set = set()
    messages = [
        {"role": "system", "content": REVIEW_SYSTEM},
        {"role": "user", "content": build_review_prompt(row)},
    ]
    out = llama_client.chat(
        messages=messages,
        tools=_tool_schema(),
        tool_executor=_make_tool_executor(seen),
        temperature=0.0,
        max_tokens=700,
    )
    if not out:
        return {"verdict": "needs_human",
                "reason": "no response from llama",
                "verified_url": row.get("URL", ""), "fields": {},
                "provenance": ""}
    # Parse the JSON (strip any accidental fences)
    txt = out.strip()
    if txt.startswith("```"):
        txt = txt.strip("`")
        if txt.lower().startswith("json"):
            txt = txt[4:]
    try:
        parsed = json.loads(txt)
    except json.JSONDecodeError:
        # Try to locate the first {...} block
        import re
        m = re.search(r"\{.*\}", txt, re.DOTALL)
        if not m:
            return {"verdict": "needs_human",
                    "reason": f"unparseable llama output: {txt[:120]}",
                    "verified_url": row.get("URL", ""), "fields": {},
                    "provenance": ""}
        try:
            parsed = json.loads(m.group(0))
        except json.JSONDecodeError:
            return {"verdict": "needs_human",
                    "reason": "json parse failed",
                    "verified_url": row.get("URL", ""), "fields": {},
                    "provenance": ""}
    parsed.setdefault("verdict", "needs_human")
    parsed.setdefault("fields", {})
    parsed.setdefault("verified_url", row.get("URL", ""))
    parsed.setdefault("outbreak", row.get("Outbreak", 0))
    parsed.setdefault("outbreak_evidence", "")
    return parsed


def apply_review(row: Dict[str, Any], review: Dict[str, Any]) -> Dict[str, Any]:
    """Merge the agent's verified fields back into the row (corrections win,
    but never blank a field the row already had unless the agent explicitly
    says the page contradicts it — here we take non-empty agent values)."""
    merged = dict(row)
    fields = review.get("fields") or {}
    for k in ("Date", "Company", "Brand", "Product", "Pathogen", "Reason",
              "Country", "Region"):
        v = fields.get(k)
        if v:  # only overwrite with a non-empty verified value
            merged[k] = v
    vu = review.get("verified_url")
    if vu:
        merged["URL"] = vu
    return merged


# ─── Sheet I/O ───────────────────────────────────────────────────────────

def load_pending(xlsx: Path) -> Tuple[List[Dict[str, Any]], List[str]]:
    return _load_sheet(xlsx, "Pending"), _sheet_headers(xlsx, "Pending")


def _load_sheet(xlsx: Path, sheet: str) -> List[Dict[str, Any]]:
    import openpyxl
    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:
        return []
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    rows = []
    for r in ws.iter_rows(min_row=2, values_only=True):
        row = {h: ("" if v is None else v) for h, v in zip(headers, r) if h}
        if any(str(v).strip() for v in row.values()):
            rows.append(row)
    return rows


def _sheet_headers(xlsx: Path, sheet: str) -> List[str]:
    import openpyxl
    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:
        return []
    return [c.value for c in wb[sheet][1] if c.value]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", type=Path, default=Path("docs/data/recalls.xlsx"))
    ap.add_argument("--commit", type=str, default="false")
    ap.add_argument("--source-filter", type=str, default=None)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    commit = args.commit.lower() in ("1", "true", "yes", "on")

    rows, _ = load_pending(args.xlsx)
    if args.source_filter:
        sf = args.source_filter.lower()
        rows = [r for r in rows if sf in str(r.get("Source", "")).lower()]
    if args.limit and args.limit > 0:
        rows = rows[:args.limit]

    print(f"Reviewing {len(rows)} Pending rows "
          f"(commit={commit}, source_filter={args.source_filter})")
    if not rows:
        print("Nothing to review.")
        return 0

    results = {"approve": [], "reject": [], "needs_human": []}
    for i, row in enumerate(rows, 1):
        review = review_row(row)
        review["_orig_url"] = str(row.get("URL", "")).strip()
        verdict = review.get("verdict", "needs_human")
        merged = apply_review(row, review)
        results[verdict if verdict in results else "needs_human"].append(
            (merged, review))
        print(f"  [{i}/{len(rows)}] {verdict.upper():12s} "
              f"{str(row.get('Source',''))[:14]:14s} "
              f"{str(merged.get('Product',''))[:44]:44s} "
              f"| {review.get('reason','')[:60]}")

    print(f"\n{'='*60}")
    print(f"approve: {len(results['approve'])}  "
          f"reject: {len(results['reject'])}  "
          f"needs_human: {len(results['needs_human'])}")
    print(f"{'='*60}")

    if not commit:
        print("\nDRY RUN — no writes. Set --commit true to apply:")
        print("  approvals → Recalls (corrected fields), "
              "rejects → Weekly_Rejected, needs_human stays in Pending.")
        return 0

    # ── Write-back: mirror claude_check.py's final-reviewer sequence ──
    # The review agent IS "Reviewer 2 — final verdict", so promote_approved
    # is called with archive_immediately=True, exactly as claude_check does.
    try:
        from pipeline.merge_master import (  # type: ignore
            promote_approved, sort_rows, save_xlsx_with_pending,
            mirror_json_from_xlsx)
    except Exception as e:
        print(f"ERROR importing merge_master helpers: {e}", file=sys.stderr)
        return 1

    XLSX_PATH = Path(args.xlsx)

    # Recalls (approved_existing) + FULL Pending, so indices line up.
    approved_existing = _load_sheet(args.xlsx, "Recalls")
    full_pending = _load_sheet(args.xlsx, "Pending")

    url_to_idx: Dict[str, int] = {}
    for i, pr in enumerate(full_pending):
        u = str(pr.get("URL", "")).strip()
        if u:
            url_to_idx[u] = i

    rejected_flags: Dict[int, str] = {}
    applied_corrections = 0

    # Approvals — write corrected fields IN PLACE; leave out of rejected_flags
    # Also advance gap-gating / enrichment statuses to plain 'pending' so
    # promote_approved will actually promote them (it skips pending_gap* and
    # pending_enrichment). This replicates claude_check.py's documented
    # state-machine advance (audit 2026-04-29 / 2026-05-11) — the review
    # agent is the 2nd/final reviewer, so an approval flips the gate.
    _ADVANCE_FROM = {"pending_gap_v1", "pending_gap_v2", "pending_retry",
                     "pending_enrichment"}
    today_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    gap_advanced = 0
    for merged, review in results["approve"]:
        idx = url_to_idx.get(str(merged.get("URL", "")).strip())
        if idx is None:
            idx = url_to_idx.get(str(review.get("_orig_url", "")).strip())
        if idx is not None:
            for k in ("Date", "Company", "Brand", "Product", "Pathogen",
                      "Reason", "Country", "Region", "URL"):
                if merged.get(k):
                    full_pending[idx][k] = merged[k]
            # Pathogen may be intentionally CLEARED by the agent when the
            # hazard is an allergen / foreign body / chemical (not a pathogen).
            # merged only carries non-empty values, so consult the raw review
            # fields dict: if it explicitly returned Pathogen == "" we honor it.
            rfields = review.get("fields") or {}
            if "Pathogen" in rfields and not str(rfields.get("Pathogen")).strip():
                full_pending[idx]["Pathogen"] = ""
            # Outbreak is verified explicitly (0 or 1) — always apply it,
            # including a correction from 1→0, since it drives the tier bump.
            ob = review.get("outbreak")
            if ob in (0, 1, "0", "1"):
                full_pending[idx]["Outbreak"] = int(ob)
            applied_corrections += 1
            cur = str(full_pending[idx].get("Status", "")).strip()
            if cur in _ADVANCE_FROM:
                full_pending[idx]["Status"] = "pending"
                notes = str(full_pending[idx].get("Notes", "")).strip()
                tag = (f"[review-agent {today_iso}: {cur} → pending "
                       f"(Qwen verified, final reviewer)]")
                full_pending[idx]["Notes"] = (notes + " " + tag).strip()[:1000]
                gap_advanced += 1

    # Rejects → rejected_flags (index → reason)
    for merged, review in results["reject"]:
        idx = url_to_idx.get(str(merged.get("URL", "")).strip())
        if idx is not None:
            rejected_flags[idx] = f"Review agent: {review.get('reason','')[:280]}"

    # needs_human → hold in Pending: set Status so promote_approved won't
    # promote it (it promotes rows whose Status is 'pending' and not flagged).
    for merged, review in results["needs_human"]:
        idx = url_to_idx.get(str(merged.get("URL", "")).strip())
        if idx is not None:
            full_pending[idx]["Status"] = "needs_human"

    new_approved, remaining, archived_rejected = promote_approved(
        pending=full_pending,
        approved_existing=approved_existing,
        rejected_flags=rejected_flags,
        archive_immediately=True,
    )

    print(f"\nApplied {applied_corrections} field-corrected approvals "
          f"({gap_advanced} gap/enrichment rows advanced to 'pending').")
    print(f"Promotion: {len(new_approved)} → Recalls, "
          f"{len(remaining)} remain in Pending, "
          f"{len(archived_rejected)} archived to Rejected.")

    final_approved = sort_rows(approved_existing + new_approved)
    final_pending = sort_rows(remaining)
    save_xlsx_with_pending(final_approved, final_pending, XLSX_PATH,
                           newly_rejected_rows=archived_rejected)
    try:
        from pipeline.merge_master import JSON_PATH  # type: ignore
        mirror_json_from_xlsx(XLSX_PATH, JSON_PATH)
    except Exception:
        pass

    try:
        from pipeline.weekly_review_capture import record_promotions  # noqa
        record_promotions(new_approved, xlsx_path=XLSX_PATH)
    except Exception as e:
        print(f"  (Weekly_Review capture skipped: {e})")
    try:
        from pipeline.weekly_rejected_capture import record_rejections  # noqa
        record_rejections(archived_rejected, xlsx_path=XLSX_PATH)
    except Exception as e:
        print(f"  (Weekly_Rejected capture skipped: {e})")

    print("\n✓ Write-back complete via promote_approved (final-reviewer mode).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
