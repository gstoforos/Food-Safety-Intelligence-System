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
            r"/safety/recalls-market-withdrawals-safety-alerts/[a-z0-9-]{30,}"),
        "site_q":   "site:fda.gov",
        "page_hint":
            "Each FDA recall page lives under "
            "fda.gov/safety/recalls-market-withdrawals-safety-alerts/<slug> "
            "where <slug> is built from the company + product + reason.",
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
the OFFICIAL regulator URL for that exact recall.

You have one tool: web_search. Use it.

Procedure:
  1. Read the headline. Identify company, product, hazard.
  2. Call web_search with a tight query that includes a site: filter for the
     regulator's domain (passed to you in the user message).
  3. Read the results. Pick the URL that:
       - is on the regulator's domain,
       - matches the URL path pattern given,
       - and whose title clearly refers to the same recall as the headline.
  4. If no result fits, call web_search ONE more time with a different query.
  5. If still nothing fits, output exactly: NONE

Output format (STRICT):
  - When found: the URL only, on a single line, no quotes, no prose.
  - When not found: the single word NONE.

Never explain. Never apologize. Never wrap in markdown."""


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


def _make_tool_executor(reg: dict):
    """Return a callable that the llama_client invokes for each tool_call."""
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
        # Compact form so the model can read it quickly
        compact = [{"url": r["url"], "title": r["title"],
                     "content": r["content"]} for r in results]
        return json.dumps({"results": compact}, ensure_ascii=False)

    return execute


# ╔══════════════════════════════════════════════════════════════════════════
# ║  STATE — per-run cache
# ╚══════════════════════════════════════════════════════════════════════════

_CACHE: dict = {}


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

    norm = " ".join(title.lower().split())
    cache_key = (regulator, norm)
    if cache_key in _CACHE:
        return _CACHE[cache_key]

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
        f"Use web_search."
    )

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": user_msg},
    ]
    tools    = _tool_schema(reg)
    executor = _make_tool_executor(reg)

    text = llama_client.chat(
        messages=messages,
        tools=tools,
        tool_executor=executor,
        temperature=0.0,
        max_tokens=512,
    )

    if not text:
        print(f"  [NA-agent {regulator}] agent returned no answer")
        _CACHE[cache_key] = None
        return None

    url = _extract_url(text)
    if not url:
        print(f"  [NA-agent {regulator}] no URL in answer: {text[:120]!r}")
        _CACHE[cache_key] = None
        return None

    if not _validate(url, reg):
        print(f"  [NA-agent {regulator}] validation failed for: {url}")
        _CACHE[cache_key] = None
        return None

    print(f"  ✓ {url[:100]}")
    _CACHE[cache_key] = url
    return url


# ╔══════════════════════════════════════════════════════════════════════════
# ║  Phase-promotion helpers
# ╚══════════════════════════════════════════════════════════════════════════

def activate(regulator: str) -> None:
    if regulator in REGULATORS:
        REGULATORS[regulator]["status"] = "active"


def is_active(regulator: str) -> bool:
    return REGULATORS.get(regulator, {}).get("status") == "active"
