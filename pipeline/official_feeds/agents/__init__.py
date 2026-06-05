"""
AFTS market-specific AI agents.

Architecture:
  GitHub Action runner
    └─ Tailscale (TAILSCALE_AUTHKEY) joins the AFTS tailnet
       └─ Calls local Llama on afts-llama-vps (LLAMA_BASE_URL)
          └─ Llama uses Searx as web_search tool (SEARX_URL, also on the box)
             └─ Returns the regulator URL

The Llama is George's own model — no Claude API, no Gemini API, no
OpenAI API. Searx is self-hosted on the same Hetzner box. No external
API tokens anywhere.

Modules:
  llama_client   — OpenAI-compatible HTTP client for afts-llama-vps
  searx_search   — Searx REST wrapper (the agent's web_search tool)
  north_america  — agent that knows FDA / USDA / CFIA URL shapes
"""
