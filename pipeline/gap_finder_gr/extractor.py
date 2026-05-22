"""
AFTS Food Safety Intelligence — Greek Gap Finder
Module 5: Extractor

Reads EFET announcement bodies (from verified.jsonl) and converts them into
structured Pending-sheet rows ready for write_pending.

Two-phase pipeline per record:
  1. RULE GATE (rules.py) — classifies hazard category from EFET body.
                            If verdict='reject', short-circuit — no LLM call needed.
                            (Saves compute and €.)
  2. LLM EXTRACTION (llama_client.py) — for ACCEPTED records, extract structured
                            fields (Company, Brand, Product, Pathogen, Reason,
                            Date, Region) with Greek→English translation,
                            preserving Brand in Greek when product is Greek-only.

Output: two JSONL files
  • pending_candidates.jsonl  — ready to append to recalls.xlsx Pending sheet
  • rejected_records.jsonl    — ready for Weekly_Rejected (with RejectReason)

Migration-safe: this module talks to whatever LlamaClient is configured —
Hetzner VPS today, localhost on Mac in 2–3 months. No code changes needed.

CLI:
    python -m pipeline.gap_finder_gr.extractor
    python -m pipeline.gap_finder_gr.extractor --dry-run    # uses MockLlamaClient
    python -m pipeline.gap_finder_gr.extractor --verbose
"""

from __future__ import annotations
import argparse
import json
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Flexible imports — work both as package and standalone script
try:
    from .rules import classify, Classification
    from .llama_client import LlamaClient, LlamaError
except ImportError:
    from rules import classify, Classification        # type: ignore
    from llama_client import LlamaClient, LlamaError  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_VERIFIED = "docs/data/gap_finder_gr/verified.jsonl"
DEFAULT_PENDING_OUT = "docs/data/gap_finder_gr/pending_candidates.jsonl"
DEFAULT_REJECTED_OUT = "docs/data/gap_finder_gr/rejected_records.jsonl"

# Hard cap on EFET body length sent to the LLM (prevents context overflow on
# unusually long announcements). Qwen 2.5 7B context is 32K but llama-server
# is configured for 8K. Leave ~2K for prompt + response.
MAX_BODY_CHARS = 6000

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

SYSTEM_PROMPT = """You are a food-safety analyst extracting structured fields from Greek food recall announcements published by ΕΦΕΤ (the Hellenic Food Authority).

Rules:
1. Output ONLY a JSON object matching the provided schema. No prose, no markdown fences.
2. Translate product, pathogen, reason, and region into clear English.
3. Keep company names in Greek (legal entity names).
4. For brand names: keep Greek-only brands in Greek; use Latin form if the original text shows one.
5. Be literal and faithful to the source. Do NOT invent details. If a field isn't stated, use empty string.
6. For dates, prefer ISO format YYYY-MM-DD. If only a Greek date is given (e.g. '14 Μαΐου 2026'), convert it.
7. For chemicals exceeding regulatory limits, include 'exceeding limit' in pathogen_en (e.g. 'Coumarin exceeding limit').
8. For undeclared allergens, format as 'Undeclared <allergen>' (e.g. 'Undeclared wheat')."""


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
) -> dict:
    """Map verified+classified+extracted record → Pending sheet row dict.
    Column order matches recalls.xlsx Pending schema exactly."""
    now = _now_utc_iso()

    # Use LLM-extracted date if present; fall back to EFET announcement date
    date = _safe(extracted, "date_iso") or verified.get("efet_date_iso", "")

    # Outbreak: emit "1" if rules detected case counts, else empty (matches
    # existing recalls.xlsx convention — blank, not "0")
    outbreak = "1" if classification.outbreak_qualifies else ""

    # Tier: empty string for None (matches existing xlsx convention)
    tier = str(classification.tier) if classification.tier is not None else ""

    row = {
        "Date":        date,
        "Source":      "EFET",
        "Company":     _safe(extracted, "company"),
        "Brand":       _safe(extracted, "brand"),
        "Product":     _safe(extracted, "product_en"),
        "Pathogen":    _safe(extracted, "pathogen_en"),
        "Reason":      _safe(extracted, "reason_en"),
        "Class":       "",        # EFET doesn't publish FSIS-style Class
        "Country":     "Greece",
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
) -> dict:
    """For rule-rejected records — fields filled from rules.py + EFET metadata,
    no LLM call needed."""
    now = _now_utc_iso()
    row = {
        "Date":         verified.get("efet_date_iso", ""),
        "Source":       "EFET",
        "Company":      "",
        "Brand":        "",
        "Product":      verified.get("efet_title", ""),  # raw Greek title — fine for rejected
        "Pathogen":     classification.matched_term or "",
        "Reason":       classification.rule,
        "Class":        "",
        "Country":      "Greece",
        "Region":       "",
        "Tier":         "",
        "Outbreak":     "",
        "URL":          verified.get("efet_url", ""),
        "Notes":        f"Discovered via news: {verified.get('news_source_domain', '')}",
        "ScrapedAt":    now,
        "Status":       "Rejected",
        "RejectedBy":   "gap_finder_gr/rules.py",
        "RejectedAt":   now,
        "RejectReason": classification.category,  # 'allergen' | 'synthetic_chemical' | ...
    }
    return row


# ─────────────────────────────────────────────────────────────────────────────
# CORE EXTRACTION LOGIC
# ─────────────────────────────────────────────────────────────────────────────

def extract_one(
    verified: dict,
    client: LlamaClient,
    verbose: bool = False,
) -> tuple[Optional[dict], Optional[dict]]:
    """
    Process one verified record. Returns (pending_row, rejected_row) — exactly
    one of the two will be non-None.

    Phase 1: rule gate. Phase 2 (only if accepted): LLM extraction.
    """
    efet_body = verified.get("efet_body", "") or ""
    efet_title = verified.get("efet_title", "") or ""

    # Phase 1: rule classification on EFET body+title
    classification = classify(
        pathogen="",                         # let the rule lexicon find it
        reason=f"{efet_title} {efet_body}",
        product=efet_title,
    )

    if verbose:
        print(f"  [classify] {classification.verdict}/{classification.category} "
              f"tier={classification.tier} matched={classification.matched_term!r}",
              file=sys.stderr)

    if classification.verdict == "reject":
        rejected = build_rejected_row(verified, classification)
        return None, rejected

    # Phase 2: LLM extraction
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_user_prompt(efet_title, efet_body)},
    ]
    try:
        extracted = client.chat_json(
            messages=messages,
            json_schema=EXTRACTION_SCHEMA,
            temperature=0.0,
            max_tokens=800,
        )
    except LlamaError as e:
        if verbose:
            print(f"  [LLM ERROR] {e} — falling back to title-only", file=sys.stderr)
        # Fallback: write a minimal Pending row marked for manual review
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

    pending = build_pending_row(verified, classification, extracted)
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


def run_dry_test() -> int:
    print("=" * 78)
    print("AFTS Greek Gap Finder — Extractor Dry-Run Test (MockLlamaClient)")
    print("=" * 78)

    client = MockLlamaClient()  # type: ignore[assignment]
    passed = failed = 0
    pending_rows: list[dict] = []
    rejected_rows: list[dict] = []

    for tc in DRY_RUN_VERIFIED:
        title = tc["efet_title"][:65]
        print(f"\n→ {title}...")
        try:
            pending, rejected = extract_one(tc, client, verbose=False)  # type: ignore[arg-type]
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
    parser = argparse.ArgumentParser(description="AFTS Greek Gap Finder — Extractor")
    parser.add_argument("--verified", default=DEFAULT_VERIFIED,
                        help=f"Input JSONL (default: {DEFAULT_VERIFIED})")
    parser.add_argument("--pending-out", default=DEFAULT_PENDING_OUT,
                        help=f"Output pending JSONL (default: {DEFAULT_PENDING_OUT})")
    parser.add_argument("--rejected-out", default=DEFAULT_REJECTED_OUT,
                        help=f"Output rejected JSONL (default: {DEFAULT_REJECTED_OUT})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Use MockLlamaClient (no VPS needed)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.dry_run:
        return run_dry_test()

    verified = read_jsonl(args.verified)
    print(f"Loaded {len(verified)} verified records from {args.verified}",
          file=sys.stderr)

    if not verified:
        print("Nothing to do — exiting.", file=sys.stderr)
        write_jsonl([], args.pending_out)
        write_jsonl([], args.rejected_out)
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
            pending, rejected = extract_one(v, client, verbose=args.verbose)
        except Exception as e:
            print(f"  ERROR processing record {i}: {e}", file=sys.stderr)
            continue
        if pending:
            pending_rows.append(pending)
        if rejected:
            rejected_rows.append(rejected)

    write_jsonl(pending_rows, args.pending_out)
    write_jsonl(rejected_rows, args.rejected_out)

    print(f"\nResults: {len(pending_rows)} pending, {len(rejected_rows)} rejected",
          file=sys.stderr)
    print(f"Wrote: {args.pending_out}", file=sys.stderr)
    print(f"Wrote: {args.rejected_out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
