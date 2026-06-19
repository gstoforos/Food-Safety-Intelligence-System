"""
AFTS Food Safety Intelligence — Gap Finder
Extractor (parametric — works for any CountryConfig).

Reads authority announcement bodies and converts them into structured
Pending-sheet rows.

Two-phase pipeline per record:
  1. RULE GATE (rules.py) — multi-lingual classifier (Greek + Italian + EN).
     If verdict='reject', short-circuit — no LLM call needed.
  2. LLM EXTRACTION (llama_client.py) — for ACCEPTED records, extract
     structured fields. Prompt is language-aware via CountryConfig.

Migration-safe: talks to whatever LlamaClient is configured — Hetzner VPS
today, localhost on Mac later. No code changes needed.

CLI:
    python -m pipeline.gap_finder.extractor --country it
    python -m pipeline.gap_finder.extractor --country gr --dry-run
"""

from __future__ import annotations
import argparse
import json
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    from .rules import classify, Classification
    from .llama_client import LlamaClient, LlamaError
    from .countries import get as get_country
    from .countries.base import CountryConfig
except ImportError:
    from gap_finder.rules import classify, Classification                  # type: ignore
    from gap_finder.llama_client import LlamaClient, LlamaError            # type: ignore
    from gap_finder.countries import get as get_country                    # type: ignore
    from gap_finder.countries.base import CountryConfig                    # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_VERIFIED = "docs/data/gap_finder_gr/verified.jsonl"
DEFAULT_PENDING_OUT = "docs/data/gap_finder_gr/pending_candidates.jsonl"
DEFAULT_REJECTED_OUT = "docs/data/gap_finder_gr/rejected_records.jsonl"

# Hard cap on EFET body length sent to the LLM (prevents context overflow on
# unusually long announcements). Qwen 2.5 7B context is 32K but llama-server
# is configured for 8K. Leave ~2K for prompt + response.
MAX_BODY_CHARS = 1200   # was 6000 — overflowed the Llama server's
# --ctx-size 4096 when Greek bodies (≈1.5 tok/char) ran long, causing
# intermittent `LlamaError 400` on /chat/completions. The company, brand,
# pathogen, product and recall-ID always appear in an EFET notice's lead
# paragraph, so 1200 chars is sufficient and keeps total tokens (~3800)
# safely under 4096. See run 27459015513 where 5012-char protothema
# bodies 400'd while short bodies succeeded.

# Pending sheet columns (locked schema from recalls.xlsx)
PENDING_COLUMNS = [
    "Date", "Source", "Company", "Brand", "Product", "Pathogen", "Reason",
    "Class", "Country", "Region", "Tier", "Outbreak", "URL", "Notes",
    "ScrapedAt", "Status", "RejectedBy",
]

# Weekly_Rejected sheet adds two columns at the end
REJECTED_COLUMNS = PENDING_COLUMNS + ["RejectedAt", "RejectReason"]


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACTION SCHEMA (Qwen JSON-mode constraint)
# ─────────────────────────────────────────────────────────────────────────────

EXTRACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "company": {
            "type": "string",
            "description": "Legal entity / manufacturer name as it appears in the EFET text. "
                           "Keep in Greek; do NOT translate.",
        },
        "brand": {
            "type": "string",
            "description": "Consumer-facing brand name. If the brand is Greek-only "
                           "(no Latin script form in the text), keep in Greek. "
                           "If the brand has a Latin/English form in the text (e.g. 'Strudito', "
                           "'Nestlé'), use the Latin form. Empty string if not stated.",
        },
        "product_en": {
            "type": "string",
            "description": "Product description in ENGLISH. Translate from Greek "
                           "(e.g. 'παξιμάδια κανέλας' → 'cinnamon rusks').",
        },
        "pathogen_en": {
            "type": "string",
            "description": "Specific hazard/pathogen in ENGLISH (e.g. 'Listeria monocytogenes', "
                           "'Coumarin exceeding limit', 'Undeclared wheat allergen'). "
                           "Be precise; include 'exceeding limit' for chemicals.",
        },
        "reason_en": {
            "type": "string",
            "description": "Full reason for recall in ENGLISH, one to two sentences. "
                           "Translate from Greek.",
        },
        "date_iso": {
            "type": "string",
            "description": "Recall announcement date in YYYY-MM-DD format. "
                           "Empty string if not found in the text.",
        },
        "region_en": {
            "type": "string",
            "description": "Greek region/city/prefecture in ENGLISH if mentioned "
                           "(e.g. 'Lesvos', 'Attica'). Empty string if nationwide "
                           "or unspecified.",
        },
    },
    "required": ["company", "brand", "product_en", "pathogen_en", "reason_en"],
    "additionalProperties": False,
}


# ─────────────────────────────────────────────────────────────────────────────
# LLM PROMPTS
# ─────────────────────────────────────────────────────────────────────────────

def build_system_prompt(cfg: CountryConfig) -> str:
    """Build the language-aware extraction system prompt for a country."""
    return f"""You are a food-safety analyst extracting structured fields from {cfg.language_name} news articles about food recall events. These articles typically reference recall notices issued by {cfg.authority_full} ({cfg.authority_short}).

Rules:
1. Output ONLY a JSON object matching the provided schema. No prose, no markdown fences.
2. Translate product, pathogen, reason, and region into clear English.
3. Keep company names in {cfg.language_name} (legal entity names).
4. For brand names: {cfg.brand_handling_note}
5. Be literal and faithful to the source. Do NOT invent details. If a field isn't stated in the article, use empty string.
6. For dates, prefer ISO format YYYY-MM-DD. Convert {cfg.language_name} dates if needed (e.g. '14 maggio 2026' → '2026-05-14', '14 Μαΐου 2026' → '2026-05-14').
7. For chemicals exceeding regulatory limits, include 'exceeding limit' in pathogen_en (e.g. 'Coumarin exceeding limit').
8. For undeclared allergens, format as 'Undeclared <allergen>' (e.g. 'Undeclared wheat').
9. Articles may contain background info (e.g. 'Listeriosis is rare but severe...'). Ignore background; extract ONLY the specific recall facts (this brand, this product, this lot, this hazard).
10. If the article mentions a recall reference number issued by {cfg.authority_short} (e.g. 'allerta 842632', 'numero pratica 12345'), include it in reason_en (e.g. 'Recall ID 842632')."""


# Legacy module-level alias for Greek (kept for any importers expecting SYSTEM_PROMPT)
SYSTEM_PROMPT = build_system_prompt(get_country("gr"))


def build_user_prompt(efet_title: str, efet_body: str) -> str:
    body = efet_body[:MAX_BODY_CHARS]
    if len(efet_body) > MAX_BODY_CHARS:
        body += "\n\n[... text truncated ...]"
    return (
        f"EFET Announcement Title:\n{efet_title}\n\n"
        f"EFET Announcement Body:\n{body}\n\n"
        f"Extract the fields per schema. JSON only."
    )


# ─────────────────────────────────────────────────────────────────────────────
# PENDING ROW BUILDERS
# ─────────────────────────────────────────────────────────────────────────────

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe(extracted: dict, key: str, default: str = "") -> str:
    val = extracted.get(key, default)
    return str(val).strip() if val is not None else default


def build_pending_row(
    verified: dict,
    classification: Classification,
    extracted: dict,
    cfg: CountryConfig,
) -> dict:
    """Map verified+classified+extracted record → Pending sheet row dict.
    Column order matches recalls.xlsx Pending schema exactly."""
    now = _now_utc_iso()

    # Use LLM-extracted date if present; fall back to authority announcement date
    date = _safe(extracted, "date_iso") or verified.get("efet_date_iso", "")
    outbreak = "1" if classification.outbreak_qualifies else ""
    tier = str(classification.tier) if classification.tier is not None else ""

    row = {
        "Date":        date,
        "Source":      cfg.authority_short,
        "Company":     _safe(extracted, "company"),
        "Brand":       _safe(extracted, "brand"),
        "Product":     _safe(extracted, "product_en"),
        "Pathogen":    _safe(extracted, "pathogen_en"),
        "Reason":      _safe(extracted, "reason_en"),
        "Class":       "",        # National authorities don't use FSIS-style Class
        "Country":     cfg.name_en,
        "Region":      _safe(extracted, "region_en"),
        "Tier":        tier,
        "Outbreak":    outbreak,
        "URL":         verified.get("efet_url", ""),
        "Notes":       f"Discovered via news: {verified.get('news_source_domain', '')}",
        "ScrapedAt":   now,
        "Status":      "Pending",
        "RejectedBy":  "",
    }
    return row


def build_rejected_row(
    verified: dict,
    classification: Classification,
    cfg: CountryConfig,
) -> dict:
    """For rule-rejected records — fields filled from rules.py + authority metadata,
    no LLM call needed."""
    now = _now_utc_iso()
    row = {
        "Date":         verified.get("efet_date_iso", ""),
        "Source":       cfg.authority_short,
        "Company":      "",
        "Brand":        "",
        "Product":      verified.get("efet_title", ""),  # raw local-language title
        "Pathogen":     classification.matched_term or "",
        "Reason":       classification.rule,
        "Class":        "",
        "Country":      cfg.name_en,
        "Region":       "",
        "Tier":         "",
        "Outbreak":     "",
        "URL":          verified.get("efet_url", ""),
        "Notes":        f"Discovered via news: {verified.get('news_source_domain', '')}",
        "ScrapedAt":    now,
        "Status":       "Rejected",
        "RejectedBy":   f"gap_finder/{cfg.code}/rules.py",
        "RejectedAt":   now,
        "RejectReason": classification.category,
    }
    return row


# ─────────────────────────────────────────────────────────────────────────────
# CORE EXTRACTION LOGIC
# ─────────────────────────────────────────────────────────────────────────────

def extract_one(
    verified: dict,
    client: LlamaClient,
    cfg: CountryConfig,
    verbose: bool = False,
) -> tuple[Optional[dict], Optional[dict]]:
    """
    Process one verified record. Returns (pending_row, rejected_row) — exactly
    one of the two will be non-None.

    Phase 1: rule gate. Phase 2 (only if accepted): LLM extraction.
    """
    efet_body = verified.get("efet_body", "") or ""
    efet_title = verified.get("efet_title", "") or ""

    # Pull the EFET URL slug into the classifier text. The slug carries the
    # regulator-blessed hazard term (e.g. "...-listeria-monocytogenes" or
    # "...-logo-udrargyrou"). News headlines often abbreviate, so the URL
    # slug is the authoritative hazard signal. Replace hyphens with spaces
    # so multi-word terms ("listeria monocytogenes") match the lexicon.
    efet_url = verified.get("efet_url", "") or ""

    # ── Authority-URL gate ───────────────────────────────────────────────
    # A recall record MUST point at the official regulator press release
    # (e.g. efet.gr/...). The news article is only the discovery signal. If
    # the article linked to no authority page, efet_url is empty — reject the
    # candidate rather than write a news URL (news247.gr/kathimerini.gr) into
    # Recalls. This is what stops the multi-outlet duplicate rows: only the
    # single authority URL is allowed through, so two outlets reporting one
    # EFET notice collapse to the one official link.
    authority_domain = (getattr(cfg, "authority_domain", "") or "").lower().lstrip("www.")
    try:
        from .authority_url_finder import (
            host_is_authority as _host_is_authority,
            url_is_authority_item as _url_is_authority_item,
        )
    except ImportError:
        from pipeline.gap_finder.authority_url_finder import (  # type: ignore
            host_is_authority as _host_is_authority,
            url_is_authority_item as _url_is_authority_item,
        )
    # The record URL must be on the authority domain AND be an actual per-recall
    # ITEM page — never the index/category page. Without the item check, an index
    # such as nafdac.gov.ng/category/recalls-and-alerts/ or thencc.org.za/
    # product-recalls/ (which LIST many recalls) passes the domain test, and then
    # classify() matches a hazard term lifted from the aggregate listing → false
    # accept. url_is_authority_item enforces this country's per-recall item regex.
    # Countries with NO item regex keep the domain-only behaviour (their
    # url_is_authority_item matches any path), so nothing about them changes.
    _on_domain = bool(efet_url) and _host_is_authority(efet_url, cfg)
    _item_rx = (getattr(cfg, "authority_item_url_regex", "") or "").strip()
    if _item_rx:
        is_authority_url = _on_domain and bool(_url_is_authority_item(efet_url, cfg))
    else:
        is_authority_url = _on_domain

    # ── News-authority mode (portal-less countries only) ─────────────────
    # For countries with NO per-recall authority portal (cfg.news_authority_mode
    # = True, e.g. Egypt NFSA, Kenya KEBS), accept the NEWS article URL as the
    # record URL instead of force-rejecting — but strictly: the outlet must be
    # on this country's curated google_news_domains whitelist. Classification
    # (below) must still pass at tier 1/2, and cross-outlet duplicates collapse
    # via dedupe_by_recall_identity downstream. For every normal country
    # (news_authority_mode False) this block is inert and the gate is absolute.
    news_url = (verified.get("news_url", "") or "").strip()
    news_mode_url = ""
    if (not is_authority_url) and bool(getattr(cfg, "news_authority_mode", False)) and news_url:
        nd = (verified.get("news_source_domain", "") or "").lower()
        if nd.startswith("www."):
            nd = nd[4:]
        trusted = []
        for d in (getattr(cfg, "google_news_domains", []) or []):
            d = d.lower()
            if d.startswith("www."):
                d = d[4:]
            trusted.append(d)
        if nd and any(nd == t or nd.endswith("." + t) for t in trusted):
            news_mode_url = news_url

    if (not is_authority_url) and (not news_mode_url):
        if verbose:
            print(f"  [gate] no official {authority_domain or 'authority'} URL "
                  f"found in article — rejecting (news URL not allowed in Recalls)",
                  file=sys.stderr)
        gate_reject = classify(
            pathogen="", reason="__no_authority_url__", product=efet_title,
        )
        # Force a reject verdict regardless of classify() result.
        rejected = build_rejected_row(verified, gate_reject, cfg)
        rejected["Reason"] = (
            f"No official {cfg.authority_short} press-release URL found in the "
            f"source article — discovery via news only; not promoted to Recalls."
        )
        return None, rejected

    if news_mode_url and verbose:
        print(f"  [gate] news-authority mode — accepting whitelisted outlet "
              f"{verified.get('news_source_domain','')} as record URL", file=sys.stderr)

    slug = efet_url.rsplit("/", 1)[-1].replace("-", " ").lower() if efet_url else ""

    classification = classify(
        pathogen="",
        reason=f"{efet_title} {efet_body} {slug}",
        product=efet_title,
    )

    if verbose:
        print(f"  [classify] {classification.verdict}/{classification.category} "
              f"tier={classification.tier} matched={classification.matched_term!r}",
              file=sys.stderr)

    if classification.verdict == "reject":
        rejected = build_rejected_row(verified, classification, cfg)
        return None, rejected

    # News-authority mode is lower-trust than an official portal: only promote
    # high-confidence hazards (tier 1/2). Anything weaker → reject rather than
    # write a news-sourced row of uncertain severity into Recalls.
    if news_mode_url and classification.tier not in (1, 2):
        rej = build_rejected_row(verified, classification, cfg)
        rej["Reason"] = (
            f"News-authority mode ({cfg.authority_short}): classification below "
            f"tier-2 confidence — not promoted to Recalls."
        )
        return None, rej

    # Phase 2: LLM extraction with language-aware prompt
    messages = [
        {"role": "system", "content": build_system_prompt(cfg)},
        {"role": "user", "content": build_user_prompt(efet_title, efet_body)},
    ]
    try:
        extracted = client.chat_json(
            messages=messages,
            json_schema=EXTRACTION_SCHEMA,
            temperature=0.0,
            max_tokens=500,   # extraction JSON is small; was 800 — trimmed for 4096-ctx headroom
        )
    except LlamaError as e:
        if verbose:
            print(f"  [LLM ERROR] {e} — falling back to title-only", file=sys.stderr)
        extracted = {
            "company": "",
            "brand": "",
            "product_en": efet_title,
            "pathogen_en": classification.matched_term or "",
            "reason_en": "LLM extraction failed — manual review required",
            "date_iso": verified.get("efet_date_iso", ""),
            "region_en": "",
        }

    if verbose:
        print(f"  [extract]  {extracted}", file=sys.stderr)

    pending = build_pending_row(verified, classification, extracted, cfg)
    if news_mode_url:
        # No authority URL exists for this country — the whitelisted news
        # article IS the record source. Mark it so reviewers see the provenance.
        pending["URL"] = news_mode_url
        pending["Notes"] = (
            f"News-authority source: {verified.get('news_source_domain','')} "
            f"(no official {cfg.authority_short} per-recall portal)"
        )
    return pending, None


# ─────────────────────────────────────────────────────────────────────────────
# I/O
# ─────────────────────────────────────────────────────────────────────────────

def read_jsonl(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    out = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def write_jsonl(rows: list[dict], path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# MOCK CLIENT FOR DRY-RUN (no VPS needed)
# ─────────────────────────────────────────────────────────────────────────────

class MockLlamaClient:
    """Stand-in for LlamaClient that returns canned extractions for known
    fixtures. Lets us test the extractor logic end-to-end without spinning
    up the VPS."""

    base_url = "mock://local"
    model = "mock-qwen2.5-7b-instruct"
    timeout = 0
    api_key = None

    # Match on fragments of the EFET body to decide which canned response to return
    FIXTURES = [
        (
            "φέτας",  # match key — Greek text in EFET body
            {
                "company": "Τυροκομικά Βυτίνας Α.Ε.",
                "brand": "ΒΥΤΙΝΑΣ",
                "product_en": "Feta cheese PDO",
                "pathogen_en": "Listeria monocytogenes",
                "reason_en": "Presence of Listeria monocytogenes detected in feta cheese.",
                "date_iso": "2026-04-09",
                "region_en": "",
            },
        ),
        (
            "psillys",  # match key
            {
                "company": "Psillys EE",
                "brand": "Psillys",
                "product_en": "Mushroom gummies",
                "pathogen_en": "Muscimol (Amanita muscaria toxin)",
                "reason_en": "Presence of muscimol — natural fungal toxin.",
                "date_iso": "2026-04-01",
                "region_en": "",
            },
        ),
        (
            "αφλατοξ",  # aflatoxin match
            {
                "company": "AB Vassilopoulos",
                "brand": "AB",
                "product_en": "Roasted salted peanuts",
                "pathogen_en": "Aflatoxins exceeding limit",
                "reason_en": "Aflatoxin contamination exceeding regulatory limit.",
                "date_iso": "2026-03-27",
                "region_en": "",
            },
        ),
    ]

    def health(self) -> bool:
        return True

    def chat_json(self, messages, json_schema, **kwargs):
        # Find the user message body
        body = next((m["content"] for m in messages if m["role"] == "user"), "")
        body_lower = body.lower()
        for key, fixture in self.FIXTURES:
            if key in body_lower:
                return fixture
        # Default fallback
        return {
            "company": "",
            "brand": "",
            "product_en": "Unknown product",
            "pathogen_en": "Unknown",
            "reason_en": "No matching fixture in mock client.",
            "date_iso": "",
            "region_en": "",
        }


# ─────────────────────────────────────────────────────────────────────────────
# DRY-RUN — exercises rule gate + (mocked) LLM extraction end-to-end
# ─────────────────────────────────────────────────────────────────────────────

DRY_RUN_VERIFIED = [
    # ─── ACCEPT cases (LLM extraction path) ────────────────────────────────
    {
        "news_url": "https://www.protothema.gr/test/feta-listeria",
        "news_title": "Ανάκληση φέτας ΒΥΤΙΝΑΣ — Listeria",
        "news_published": "2026-04-09T18:00:00+03:00",
        "news_source_domain": "protothema.gr",
        "efet_url": "https://www.efet.gr/anakleiseis-cat/14990-feta",
        "efet_title": "ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση τυριού φέτας λόγω Listeria monocytogenes",
        "efet_date_iso": "2026-04-09",
        "efet_body": "Ο ΕΦΕΤ προχώρησε σε ανάκληση παρτίδας φέτας ΒΥΤΙΝΑΣ ΠΟΠ της "
                     "εταιρείας Τυροκομικά Βυτίνας Α.Ε. λόγω ανίχνευσης Listeria "
                     "monocytogenes. Καλούνται οι καταναλωτές να μην καταναλώσουν "
                     "το προϊόν.",
        "match_score": 0.5,
        "matched_at": "2026-04-09T19:00:00+00:00",
        "expected_verdict": "accept",
        "expected_pathogen_includes": "listeria",
    },
    {
        "news_url": "https://www.in.gr/test/psillys",
        "news_title": "Psillys gummies μουσκιμόλη ανάκληση",
        "news_published": "2026-04-01T19:00:00+03:00",
        "news_source_domain": "in.gr",
        "efet_url": "https://www.efet.gr/anakleiseis-cat/14980-gummies",
        "efet_title": "ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση τροφίμων τύπου ζελεδών (gummies)",
        "efet_date_iso": "2026-04-01",
        "efet_body": "Ανακαλούνται προϊόντα Psillys λόγω παρουσίας μουσκιμόλης — "
                     "τοξίνης από Amanita muscaria.",
        "match_score": 0.375,
        "matched_at": "2026-04-01T19:00:00+00:00",
        "expected_verdict": "accept",
        "expected_pathogen_includes": "muscimol",
    },
    {
        "news_url": "https://www.kathimerini.gr/test/ab-peanuts",
        "news_title": "Ανάκληση φιστικιών AB λόγω αφλατοξινών",
        "news_published": "2026-03-27T20:00:00+03:00",
        "news_source_domain": "kathimerini.gr",
        "efet_url": "https://www.efet.gr/anakleiseis-cat/14950-peanuts",
        "efet_title": "ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση φιστικιών λόγω αφλατοξινών",
        "efet_date_iso": "2026-03-27",
        "efet_body": "Ο ΕΦΕΤ ανακαλεί παρτίδα ψημένων αλατισμένων φιστικιών "
                     "(roasted salted peanuts) AB λόγω παρουσίας αφλατοξινών "
                     "πάνω από τα επιτρεπτά όρια.",
        "match_score": 0.4,
        "matched_at": "2026-03-27T21:00:00+00:00",
        "expected_verdict": "accept",
        "expected_pathogen_includes": "aflatoxin",
    },

    # ─── REJECT cases (no LLM call — short-circuit) ────────────────────────
    {
        "news_url": "https://www.protothema.gr/test/strudel",
        "news_title": "Ανάκληση Strudito strudel λόγω κουμαρίνης",
        "news_published": "2026-05-14T20:15:00+03:00",
        "news_source_domain": "protothema.gr",
        "efet_url": "https://www.efet.gr/anakleiseis-cat/15001-strudel",
        "efet_title": "ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση μη ασφαλούς προϊόντος (Στρουντελάκια μήλο-κανέλα)",
        "efet_date_iso": "2026-05-14",
        "efet_body": "Ανάκληση Strudito strudel μήλο/κανέλα λόγω παρουσίας "
                     "κουμαρίνης (coumarin) πάνω από τα επιτρεπτά όρια.",
        "match_score": 0.625,
        "matched_at": "2026-05-14T20:30:00+00:00",
        "expected_verdict": "reject",
        "expected_category": "synthetic_chemical",
    },
    {
        "news_url": "https://www.kathimerini.gr/test/paximadia",
        "news_title": "Ανάκληση παξιμαδιών κανέλας λόγω γλουτένης",
        "news_published": "2026-05-14T20:30:00+03:00",
        "news_source_domain": "kathimerini.gr",
        "efet_url": "https://www.efet.gr/anakleiseis-cat/15000-paximadia",
        "efet_title": "ΔΕΛΤΙΟ ΤΥΠΟΥ: Ανάκληση μη ασφαλούς προϊόντος (παξιμαδιών)",
        "efet_date_iso": "2026-05-14",
        "efet_body": "Παξιμάδια κανέλας ΚΑΡΑΓΙΑΝΝΑΚΗΣ ΑΡΤΟΠΟΙΙΑ Λέσβου — "
                     "παρουσία μη δηλωμένου αλλεργιογόνου (άλευρο σίτου / γλουτένη).",
        "match_score": 0.375,
        "matched_at": "2026-05-14T20:35:00+00:00",
        "expected_verdict": "reject",
        "expected_category": "allergen",
    },
]


def run_dry_test(cfg: CountryConfig) -> int:
    print("=" * 78)
    print(f"AFTS Gap Finder — Extractor Dry-Run Test "
          f"({cfg.code} / {cfg.name_en}, MockLlamaClient)")
    print("=" * 78)

    client = MockLlamaClient()  # type: ignore[assignment]
    passed = failed = 0
    pending_rows: list[dict] = []
    rejected_rows: list[dict] = []

    for tc in DRY_RUN_VERIFIED:
        title = tc["efet_title"][:65]
        print(f"\n→ {title}...")
        try:
            pending, rejected = extract_one(tc, client, cfg, verbose=False)  # type: ignore[arg-type]
        except Exception as e:
            print(f"   ✗ FAIL — exception: {e}")
            failed += 1
            continue

        if tc["expected_verdict"] == "accept":
            if pending is None or rejected is not None:
                print("   ✗ FAIL — expected ACCEPT but got rejection")
                failed += 1
                continue
            pathogen_ok = tc["expected_pathogen_includes"].lower() in pending["Pathogen"].lower()
            schema_ok = set(pending.keys()) == set(PENDING_COLUMNS)
            if not pathogen_ok:
                print(f"   ✗ FAIL — pathogen mismatch: {pending['Pathogen']!r}")
                failed += 1
                continue
            if not schema_ok:
                missing = set(PENDING_COLUMNS) - set(pending.keys())
                extra = set(pending.keys()) - set(PENDING_COLUMNS)
                print(f"   ✗ FAIL — schema mismatch  missing={missing} extra={extra}")
                failed += 1
                continue
            print(f"   ✓ PASS")
            print(f"     Company: {pending['Company']!r}")
            print(f"     Brand:   {pending['Brand']!r}")
            print(f"     Product: {pending['Product']!r}")
            print(f"     Pathogen:{pending['Pathogen']!r}")
            print(f"     Tier:    {pending['Tier']!r}  Source: {pending['Source']!r}")
            pending_rows.append(pending)
            passed += 1
        else:  # expected reject
            if rejected is None or pending is not None:
                print("   ✗ FAIL — expected REJECT but got pending")
                failed += 1
                continue
            cat_ok = rejected["RejectReason"] == tc["expected_category"]
            schema_ok = set(rejected.keys()) == set(REJECTED_COLUMNS)
            if not cat_ok:
                print(f"   ✗ FAIL — category mismatch: "
                      f"got {rejected['RejectReason']!r} "
                      f"expected {tc['expected_category']!r}")
                failed += 1
                continue
            if not schema_ok:
                missing = set(REJECTED_COLUMNS) - set(rejected.keys())
                extra = set(rejected.keys()) - set(REJECTED_COLUMNS)
                print(f"   ✗ FAIL — schema mismatch  missing={missing} extra={extra}")
                failed += 1
                continue
            print(f"   ✓ PASS (rejected, no LLM call)")
            print(f"     RejectReason: {rejected['RejectReason']!r}")
            print(f"     RejectedBy:   {rejected['RejectedBy']!r}")
            rejected_rows.append(rejected)
            passed += 1

    # Sanity check: column order matches xlsx schema
    print("\n" + "-" * 78)
    print("Schema integrity:")
    if pending_rows:
        cols_order_ok = list(pending_rows[0].keys()) == PENDING_COLUMNS
        print(f"  Pending column order:   "
              f"{'✓ PASS' if cols_order_ok else '✗ FAIL'}")
        if cols_order_ok:
            passed += 1
        else:
            failed += 1
    if rejected_rows:
        cols_order_ok = list(rejected_rows[0].keys()) == REJECTED_COLUMNS
        print(f"  Rejected column order:  "
              f"{'✓ PASS' if cols_order_ok else '✗ FAIL'}")
        if cols_order_ok:
            passed += 1
        else:
            failed += 1

    print("\n" + "=" * 78)
    print(f"Results: {passed} passed, {failed} failed")
    print(f"  Accepted (would call LLM):   {len(pending_rows)}")
    print(f"  Rejected (no LLM call):      {len(rejected_rows)}")
    print("=" * 78)
    return 0 if failed == 0 else 1


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="AFTS Gap Finder — Extractor")
    parser.add_argument("--country", required=True, help="ISO2 code: gr, it, ...")
    parser.add_argument("--verified", default=None)
    parser.add_argument("--pending-out", default=None)
    parser.add_argument("--rejected-out", default=None)
    parser.add_argument("--dry-run", action="store_true",
                        help="Use MockLlamaClient (no VPS needed)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    cfg = get_country(args.country)
    verified_path = args.verified or cfg.verified_path
    pending_out = args.pending_out or cfg.pending_path
    rejected_out = args.rejected_out or cfg.rejected_path

    if args.dry_run:
        return run_dry_test(cfg)

    verified = read_jsonl(verified_path)
    print(f"Loaded {len(verified)} verified records from {verified_path}",
          file=sys.stderr)

    if not verified:
        print("Nothing to do — exiting.", file=sys.stderr)
        write_jsonl([], pending_out)
        write_jsonl([], rejected_out)
        return 0

    client = LlamaClient()
    print(f"LlamaClient: base_url={client.base_url}  model={client.model}",
          file=sys.stderr)
    if not client.health():
        print(f"ERROR: cannot reach llama-server at {client.base_url}",
              file=sys.stderr)
        return 1

    pending_rows: list[dict] = []
    rejected_rows: list[dict] = []

    for i, v in enumerate(verified, 1):
        if args.verbose:
            print(f"\n[{i}/{len(verified)}] {v.get('efet_title', '')[:70]}",
                  file=sys.stderr)
        try:
            pending, rejected = extract_one(v, client, cfg, verbose=args.verbose)
        except Exception as e:
            print(f"  ERROR processing record {i}: {e}", file=sys.stderr)
            continue
        if pending:
            pending_rows.append(pending)
        if rejected:
            rejected_rows.append(rejected)

    write_jsonl(pending_rows, pending_out)
    write_jsonl(rejected_rows, rejected_out)

    print(f"\nResults: {len(pending_rows)} pending, {len(rejected_rows)} rejected",
          file=sys.stderr)
    print(f"Wrote: {pending_out}", file=sys.stderr)
    print(f"Wrote: {rejected_out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
