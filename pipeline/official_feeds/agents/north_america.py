"""
═══════════════════════════════════════════════════════════════════════════
  AFTS North America Recall Agent — Llama + self-hosted Searx
═══════════════════════════════════════════════════════════════════════════

Dedicated AI agent for the North American food-safety market. Same shape as
the FSIS reviewer: one focused LLM agent for one specific job.

Architecture:
    GitHub Action → Tailscale → afts-llama-vps
                                    ├─ Llama/Qwen (LLAMA_BASE_URL, OpenAI-compatible)
                                    └─ Searx       (SEARX_URL, self-hosted)
                                          ↓
                                     regulator URL → Pending.xlsx

Zero external API tokens. Everything self-hosted on YOUR Hetzner box.

Coverage roadmap:
    ✓ Phase 1 — FDA          (this build, active)
    ☐ Phase 2 — USDA FSIS    (wired, set status="active" when ready)
    ☐ Phase 3 — CFIA         (wired, set status="active" when ready)

Required env (via GitHub Secrets):
    LLAMA_BASE_URL    — http://afts-llama-vps:8080/v1 (over Tailscale)
    LLAMA_MODEL       — qwen2.5-7b-instruct (or whatever you serve)
    SEARX_URL         — http://afts-llama-vps:8888/search (over Tailscale)
    TAILSCALE_AUTHKEY — joined by the workflow's tailscale/github-action step
═══════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import json
import re
from typing import Optional
from urllib.parse import urlparse

from . import llama_client, searx_search


# ╔══════════════════════════════════════════════════════════════════════════
# ║  REGULATORS — every regulator the NA agent knows.
# ╚══════════════════════════════════════════════════════════════════════════

REGULATORS: dict = {
    "FDA": {
        "name":     "U.S. Food and Drug Administration",
        "domain":   "fda.gov",
        "path_re":  re.compile(
            r"/safety/recalls-market-withdrawals-safety-alerts/[a-z0-9-]{30,}"
            r"|/food/outbreaks-foodborne-illness/[a-z0-9-]{20,}"),
        "site_q":   "site:fda.gov",
        "page_hint":
            "FDA pages live under either "
            "fda.gov/safety/recalls-market-withdrawals-safety-alerts/<slug> "
            "(individual recalls) OR fda.gov/food/outbreaks-foodborne-illness/<slug> "
            "(outbreak investigations). Both are valid.",
        "status":   "active",
    },

    "USDA_FSIS": {
        "name":     "USDA Food Safety and Inspection Service",
        "domain":   "fsis.usda.gov",
        "path_re":  re.compile(r"/recalls-alerts/[a-z0-9-]{10,}"),
        "site_q":   "site:fsis.usda.gov",
        "page_hint":
            "Each FSIS recall page lives under "
            "fsis.usda.gov/recalls-alerts/<slug>.",
        "status":   "planned",
    },

    "CFIA": {
        "name":     "Canadian Food Inspection Agency",
        "domain":   "recalls-rappels.canada.ca",
        "path_re":  re.compile(r"/en/alert-recall/[a-z0-9-]{10,}"),
        "site_q":   "site:recalls-rappels.canada.ca",
        "page_hint":
            "Each CFIA recall page lives under "
            "recalls-rappels.canada.ca/en/alert-recall/<slug>.",
        "status":   "planned",
    },
}


# ╔══════════════════════════════════════════════════════════════════════════
# ║  SYSTEM PROMPT — keeps the Llama tight and focused.
# ╚══════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """\
You are the AFTS North America Recall Agent.

Your only job: given a news headline about a North American food recall, find
the OFFICIAL regulator URL for that exact recall, using the web_search tool.

═══ HARD RULES — DO NOT VIOLATE ═══
  1. NEVER invent, fabricate, guess, construct, or extrapolate URLs.
  2. The URL you return MUST appear verbatim in a web_search tool result.
  3. If you cannot find a matching URL in tool results, you MUST output: NONE
  4. Do NOT pattern-match a slug from the headline and append it to fda.gov.
     The slug must come from a real search result.

═══ PROCEDURE ═══
  1. Read the headline. Extract company name, product, hazard (and date if
     mentioned).
  2. Call web_search with a tight query: include the company name + hazard
     and a site: filter for the regulator's domain (given in user message).
  3. Read the tool results. Find the result whose URL is on the regulator's
     domain AND whose title refers to the same company + hazard + product
     as the headline.
  4. If no result matches confidently, call web_search ONE more time with
     different terms (e.g. swap company name for product name).
  5. If STILL no matching result, output: NONE

═══ OUTPUT (STRICT) ═══
  - When found: the URL only, exactly as it appeared in the tool result.
    One line, no quotes, no prose.
  - When not found: the single word NONE.

Never explain. Never apologize. Never wrap in markdown.
Never write a URL you did not see in a tool result."""


# ╔══════════════════════════════════════════════════════════════════════════
# ║  TOOL SCHEMA & EXECUTOR (Tavily wrapper for the Llama tool loop)
# ╚══════════════════════════════════════════════════════════════════════════

def _tool_schema(reg: dict) -> list[dict]:
    return [{
        "type": "function",
        "function": {
            "name": "web_search",
            "description": (
                f"Search the web for regulator recall pages. "
                f"Add 'site:{reg['domain']}' to the query to constrain "
                f"to the regulator's domain."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query string."
                    },
                },
                "required": ["query"],
            },
        },
    }]


def _make_tool_executor(reg: dict, seen_urls: set):
    """Return a callable that the llama_client invokes for each tool_call.

    Every URL returned by Searx is recorded into the `seen_urls` set so the
    caller can verify the model's final answer corresponds to a real result
    (no fabricated slugs).
    """
    domain = reg["domain"]

    def execute(name: str, args: dict) -> str:
        if name != "web_search":
            return json.dumps({"error": f"unknown tool: {name}"})
        query = (args.get("query") or "").strip()
        if not query:
            return json.dumps({"error": "empty query"})
        # Restrict results to the regulator's domain (belt-and-braces — the
        # model is also told to add site: in the query).
        results = searx_search.search(
            query,
            max_results=8,
            include_domains=[domain],
        )
        # Record every URL we surfaced so the post-hoc validator can confirm
        # the model didn't invent a slug.
        for r in results:
            url = r.get("url", "")
            if url:
                seen_urls.add(url.rstrip("/"))
        # Compact form so the model can read it quickly
        compact = [{"url": r["url"], "title": r["title"],
                     "content": r["content"]} for r in results]
        return json.dumps({"results": compact}, ensure_ascii=False)

    return execute


# ╔══════════════════════════════════════════════════════════════════════════
# ║  STATE — per-run cache
# ╚══════════════════════════════════════════════════════════════════════════

# (legacy _CACHE removed — replaced by _RESOLVED Jaccard cache above)


# ╔══════════════════════════════════════════════════════════════════════════
# ║  JACCARD FUZZY DEDUP — collapse multi-outlet coverage to 1 agent call
# ╚══════════════════════════════════════════════════════════════════════════
#
# Multiple news outlets cover the same recall with very different wording:
#   "Champion Foods Recall: Champion Foods Cheese Bread Recalled..."
#   "Frozen cheese bread recalled nationwide over salmonella risk - MSN"
#   "Motor City Pizza Co. 5 Cheese Bread recalled over Salmonella risk"
# All three are the SAME FDA recall. Sharing one agent call across them
# cuts ~49 LLM calls → ~12 unique recalls.
#
# After the agent resolves URL X for headline A, we store (tokens_A, X)
# in _RESOLVED. New headline B's tokens are compared against every entry;
# if Jaccard ≥ JACCARD_THRESHOLD AND the hazard token matches, B reuses X
# without invoking the agent.
#
# Threshold tuned conservatively (0.30) — prefers under-grouping (extra
# agent calls) over wrong-URL collisions.

JACCARD_THRESHOLD = 0.30

_HAZARD_TOKENS = (
    "salmonella", "listeria", "e.coli", "e coli", "ecoli", "stec",
    "botulism", "clostridium", "campylobacter", "norovirus", "shigella",
    "hepatitis", "vibrio",
    "aflatoxin", "ochratoxin", "cereulide", "mycotoxin",
    "mercury", "lead", "cadmium", "arsenic",
    "metal", "plastic", "glass", "foreign matter",
    "undeclared", "allergen",
)

_STOPWORDS = {
    "the", "a", "an", "of", "for", "in", "at", "on", "to", "and", "or",
    "with", "from", "by", "as", "is", "are", "was", "be", "this", "that",
    "these", "those", "fda", "usda", "cfia", "recall", "recalls", "recalled",
    "recalling", "alert", "alerts", "alerted", "outbreak", "investigation",
    "warning", "risk", "risks", "concern", "concerns", "due", "over",
    "amid", "sold", "selling", "issue", "issued", "issues", "company",
    "co", "llc", "inc", "corp", "news", "yahoo", "msn", "aol", "newsweek",
    "patch", "kron4", "today", "yesterday", "report", "reports", "reported",
    "more", "some", "all", "may", "could", "after", "before", "linked",
    "nationwide", "national", "popular", "growing", "new", "deadly",
    "urgent", "urgently", "potential", "potentially", "possible",
    "products", "product", "national", "stores", "store", "online",
    "expansion", "expanded", "expands", "warns", "warning",
}


def _hazard_of(title: str) -> str:
    """First hazard token found in title (most specific first)."""
    t = (title or "").lower()
    for h in _HAZARD_TOKENS:
        if h in t:
            return h.replace(".", "").replace(" ", "")
    return ""


def _content_tokens(title: str) -> frozenset:
    """Distinctive content tokens for Jaccard comparison.

    Strips publisher suffix and stopwords, keeps tokens ≥3 chars.
    Hazard tokens are kept (they're highly discriminative).
    """
    t = (title or "").lower()
    if " - " in t:
        t = t.rsplit(" - ", 1)[0]
    words = re.findall(r"[a-z]{3,}", t)
    return frozenset(w for w in words if w not in _STOPWORDS)


def _jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# Per-run cache of resolved recalls. Each entry: (tokens, hazard, url).
# Reset by callers if they want a fresh cache per workflow run.
_RESOLVED: list = []


def reset_dedup_cache() -> None:
    """Clear the fuzzy dedup cache. Call between independent runs."""
    _RESOLVED.clear()


def _lookup_fuzzy(title: str) -> Optional[str]:
    """Return a previously-resolved URL if this headline's tokens match
    a prior one closely enough, else None.

    Match = same hazard AND Jaccard(tokens) >= threshold.
    """
    haz = _hazard_of(title)
    if not haz:
        return None
    tokens = _content_tokens(title)
    if len(tokens) < 3:
        return None
    best_score = 0.0
    best_url: Optional[str] = None
    for prev_tokens, prev_haz, prev_url in _RESOLVED:
        if prev_haz != haz:
            continue
        score = _jaccard(tokens, prev_tokens)
        if score >= JACCARD_THRESHOLD and score > best_score:
            best_score = score
            best_url = prev_url
    return best_url


def _record_resolved(title: str, url: str) -> None:
    """Add a resolved (title, URL) to the fuzzy cache."""
    haz = _hazard_of(title)
    tokens = _content_tokens(title)
    if tokens and haz:
        _RESOLVED.append((tokens, haz, url))


def _url_was_seen(url: str, seen: set) -> bool:
    """Anti-fabrication guard. URL must have appeared in a real Searx
    result this call. Comparison is path-exact (trailing slash ignored)."""
    norm = url.rstrip("/")
    if norm in seen:
        return True
    # Also accept if a seen URL contains our URL or vice versa (Searx
    # sometimes truncates very long URLs in display).
    for s in seen:
        if norm in s or s in norm:
            return True
    return False


# ╔══════════════════════════════════════════════════════════════════════════
# ║  RESPONSE PARSING & VALIDATION
# ╚══════════════════════════════════════════════════════════════════════════

def _extract_url(text: str) -> Optional[str]:
    if not text or text.strip().upper().startswith("NONE"):
        return None
    m = re.search(r"https?://[^\s<>\"']+", text)
    if not m:
        return None
    return m.group(0).rstrip(".,;:!)]")


def _validate(url: str, reg: dict) -> bool:
    parsed = urlparse(url)
    netloc = (parsed.netloc or "").lower()
    if reg["domain"] not in netloc:
        return False
    path_q = (f"{parsed.path}?{parsed.query}"
              if parsed.query else parsed.path)
    return bool(reg["path_re"].search(path_q))


# ╔══════════════════════════════════════════════════════════════════════════
# ║  PUBLIC API — called from Stage 3b of the pipeline
# ╚══════════════════════════════════════════════════════════════════════════

def find_url(title: str, regulator: str) -> Optional[str]:
    """
    Resolve a North American regulator URL via the Llama agent.

    Args:
        title:     the news article headline
        regulator: one of "FDA", "USDA_FSIS", "CFIA"

    Returns:
        the regulator URL, or None.
    """
    if not title or not regulator:
        return None

    reg = REGULATORS.get(regulator)
    if not reg:
        print(f"  [NA-agent] unknown regulator: {regulator!r}")
        return None
    if reg["status"] != "active":
        return None

    if not llama_client.is_configured():
        print(f"  [NA-agent] LLAMA_BASE_URL not set — agent disabled")
        return None
    if not searx_search.is_configured():
        print(f"  [NA-agent] SEARX_URL not set — agent disabled")
        return None

    # Fuzzy cache: if a prior headline about the same recall already
    # resolved to a URL, reuse it. Catches multi-outlet coverage where
    # the brand name varies wildly across articles.
    fuzzy = _lookup_fuzzy(title)
    if fuzzy:
        print(f"  [NA-agent {regulator}] ↩ fuzzy cache hit → {fuzzy[:90]}")
        return fuzzy

    print(f"  [NA-agent {regulator}] {title[:80]}")

    user_msg = (
        f"Regulator:   {reg['name']}\n"
        f"Domain:      {reg['domain']}\n"
        f"Site filter: {reg['site_q']}\n"
        f"URL path regex: {reg['path_re'].pattern}\n"
        f"Page shape:  {reg['page_hint']}\n"
        f"\n"
        f"News headline: {title!r}\n"
        f"\n"
        f"Find the official regulator URL for this exact recall. "
        f"Use web_search. Return ONLY a URL that actually appeared in a "
        f"web_search result — never invent or construct one."
    )

    # Per-call set of URLs that Searx actually returned. The validator below
    # confirms the model's final answer came from this set (anti-fabrication).
    seen_urls: set = set()

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": user_msg},
    ]
    tools    = _tool_schema(reg)
    executor = _make_tool_executor(reg, seen_urls)

    text = llama_client.chat(
        messages=messages,
        tools=tools,
        tool_executor=executor,
        temperature=0.0,
        max_tokens=256,
    )

    if not text:
        print(f"  [NA-agent {regulator}] agent returned no answer")
        return None

    url = _extract_url(text)
    if not url:
        print(f"  [NA-agent {regulator}] no URL in answer: {text[:120]!r}")
        return None

    if not _validate(url, reg):
        print(f"  [NA-agent {regulator}] validation failed (domain/path): {url}")
        return None

    # Anti-fabrication: URL must have appeared in a real Searx result.
    if not _url_was_seen(url, seen_urls):
        print(f"  [NA-agent {regulator}] ✗ FABRICATED — not in Searx results: {url}")
        return None

    print(f"  ✓ {url}")
    _record_resolved(title, url)
    return url


# ╔══════════════════════════════════════════════════════════════════════════
# ║  Phase-promotion helpers
# ╚══════════════════════════════════════════════════════════════════════════

def activate(regulator: str) -> None:
    if regulator in REGULATORS:
        REGULATORS[regulator]["status"] = "active"


def is_active(regulator: str) -> bool:
    return REGULATORS.get(regulator, {}).get("status") == "active"
