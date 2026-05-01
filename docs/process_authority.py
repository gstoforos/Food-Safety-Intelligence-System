"""
process_authority.py
====================

Process Authority (PA) trigger module for the AFTS weekly + monthly
recall briefings. Surfaces a fixed 4th paragraph in the analysis
section whenever the reporting window contains thermal-processing,
low-acid / acidified-food, anaerobic-packaging, or commercial-
sterility-relevant hazard signals.

Public API (referenced by docs/build_monthly_report_afts.py and
docs/build_weekly_report_afts.py):

    PROCESS_AUTHORITY_LABEL                 — string label used as the
                                              prefix of the PA paragraph
    detect_process_authority_trigger(rows)  — scan recalls; return dict
    build_prompt_extension(trigger)         — text appended to AI prompt
    deterministic_fallback(trigger)         — full PA paragraph for use
                                              when no Claude API is
                                              available

Design rules:
  - No numerical F-value / D-value targets are published in PA briefing
    text. Those are engagement deliverables from a qualified process
    authority and would be inappropriate without per-product validation.
  - Frame regulatory references GLOBALLY: FDA (21 CFR 113/114) + EU
    (Reg. 852/2004 + 2073/2005) + CFIA (SFCA/SFCR) + FSA / FSS UK +
    FSANZ Food Standards Code + MHLW Japan + analogous national
    frameworks. The scheduled-process / qualified-process-authority
    requirement is the common global principle.
  - Trigger thresholds err on the side of FIRING when in doubt — false
    positive (PA paragraph appears when not strictly needed) is benign
    educational content; false negative (paragraph absent when relevant)
    skips a real engineering message.
"""
from __future__ import annotations

from typing import Any, Dict, List

# ──────────────────────────────────────────────────────────────────────
# Public constant — the section label, also used by the report builder
# to detect the PA paragraph in Claude's response and apply the .pa-note
# CSS class. Match is case-insensitive.
# ──────────────────────────────────────────────────────────────────────
PROCESS_AUTHORITY_LABEL = "Process Authority Note"


# ──────────────────────────────────────────────────────────────────────
# Trigger keyword vocabularies
# ──────────────────────────────────────────────────────────────────────
# Each row is scanned across Pathogen + Reason + Product + Description
# fields, lowercased and joined with whitespace. Order doesn't matter
# for matching but is kept deliberate for log readability.

# Pathogens / hazards that DIRECTLY imply scheduled-process relevance.
# Hitting any one of these fires the trigger — no second condition needed.
_PATHOGEN_TRIGGERS = (
    "botulinum",            # C. botulinum — anaerobic spore-former
    "botulin",              # botulinum toxin / botulism
    "botulism",
)

# Product / process descriptors that indicate the hazard category.
# These trigger when paired with ANY pathogen mention OR a class-I /
# tier-1 designation. The pairing prevents firing on benign packaging
# mentions (e.g. "modified atmosphere" on a fully-aerobic product).
_LACF_TRIGGERS = (
    "low acid",
    "low-acid",
    "lacf",                 # FDA's Low-Acid Canned Foods designation
    "21 cfr 113",
    "21 cfr 114",
    "scheduled process",
    "commercial sterility",
    "commercially sterile",
    "retort",               # retorted product
    "canned",               # shelf-stable canning
)
_ASEPTIC_TRIGGERS = (
    "aseptic",
    "uht",
    "ultra-high temperature",
    "ultra high temperature",
    "htst",
    "hold tube",
    "hold-tube",
    "extended shelf life",  # ESL processing
    " esl ",
)
_ANAEROBIC_PACK_TRIGGERS = (
    "vacuum pack",          # vacuum-packed RTE
    "vacuum-pack",
    "vacuum packaged",
    "modified atmosphere",
    "modified-atmosphere",
    "map packaging",
    "rop ",                 # Reduced-Oxygen Packaging
    "reduced oxygen",
    "reduced-oxygen",
    "sous vide",
    "sous-vide",
    "fermented fish",       # historic botulism vector
    "smoked fish",          # historic botulism vector (cold-smoked)
)

# Standalone product categories that automatically pair with the LACF/
# aseptic/anaerobic vocabularies (i.e. their MERE PRESENCE triggers the
# PA note, since they're definitionally inside the scheduled-process
# universe).
_AUTO_TRIGGER_CATEGORIES = (
    "infant formula",       # 21 CFR 106/107 + LACF
    "infant cereal",
    "low-acid canned",
    "shelf-stable canned",
    "commercially sterile",
)


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
def _row_text(r: Dict[str, Any]) -> str:
    """Concatenate all free-text fields of a recall row (lowercased)."""
    parts = []
    for k in ("Pathogen", "Reason", "Hazard", "Product", "Description",
              "Issue", "Title", "Subject", "Agency", "Source"):
        v = r.get(k)
        if v:
            parts.append(str(v))
    return " ".join(parts).lower()


def _is_high_severity(r: Dict[str, Any]) -> bool:
    """Match Tier-1 / Class I / outbreak-cited rows."""
    for k in ("Tier", "Severity", "Class"):
        v = str(r.get(k) or "").strip().lower()
        if v in ("1", "tier1", "tier 1", "class i", "class 1", "i", "tier-1"):
            return True
    for k in ("Outbreak", "IsOutbreak", "Outbreak_Flag", "outbreak"):
        v = r.get(k)
        if v in (True, 1, "1", "TRUE", "True", "true", "Y", "Yes", "yes"):
            return True
    return False


def _hits(text: str, keywords) -> List[str]:
    """Return the list of keywords that appear in text. Order preserved."""
    return [kw.strip() for kw in keywords if kw in text]


# ──────────────────────────────────────────────────────────────────────
# Public: detect_process_authority_trigger
# ──────────────────────────────────────────────────────────────────────
def detect_process_authority_trigger(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Scan the recall set and decide whether the PA section should fire.

    Returns
    -------
    dict with keys:
      fired          : bool — overall trigger decision
      total_matches  : int  — count of rows that contributed
      categories     : list of category labels that fired (may include
                       "botulinum", "lacf", "aseptic", "anaerobic_packaging",
                       "auto_category")
      keywords_hit   : list of unique keywords matched (deduplicated, sorted)
      example_rows   : up to 3 representative rows (subset of the input,
                       trimmed to a few key fields) for use in the prompt
                       extension and deterministic fallback
    """
    out: Dict[str, Any] = {
        "fired":         False,
        "total_matches": 0,
        "categories":    [],
        "keywords_hit":  [],
        "example_rows":  [],
    }
    if not rows:
        return out

    cats: set = set()
    kws: set = set()
    matched_rows: List[Dict[str, Any]] = []

    for r in rows:
        text = _row_text(r)
        if not text:
            continue

        row_cats: List[str] = []
        # 1) Direct pathogen triggers — fire alone
        path_hits = _hits(text, _PATHOGEN_TRIGGERS)
        if path_hits:
            row_cats.append("botulinum")
            kws.update(path_hits)

        # 2) Process-category triggers — fire alone (definitionally LACF/aseptic)
        auto_hits = _hits(text, _AUTO_TRIGGER_CATEGORIES)
        if auto_hits:
            row_cats.append("auto_category")
            kws.update(auto_hits)

        lacf_hits = _hits(text, _LACF_TRIGGERS)
        if lacf_hits:
            row_cats.append("lacf")
            kws.update(lacf_hits)

        asep_hits = _hits(text, _ASEPTIC_TRIGGERS)
        if asep_hits:
            row_cats.append("aseptic")
            kws.update(asep_hits)

        anae_hits = _hits(text, _ANAEROBIC_PACK_TRIGGERS)
        # Anaerobic packaging alone is benign; require a second condition
        # to avoid firing on plain-yoghurt-in-vacuum-pack rows.
        if anae_hits and (path_hits or _is_high_severity(r)):
            row_cats.append("anaerobic_packaging")
            kws.update(anae_hits)

        if row_cats:
            cats.update(row_cats)
            matched_rows.append({
                "Date":     r.get("Date"),
                "Country":  r.get("Country"),
                "Source":   r.get("Source") or r.get("Agency"),
                "Company":  r.get("Company"),
                "Product":  r.get("Product") or r.get("Description"),
                "Pathogen": r.get("Pathogen"),
                "Reason":   r.get("Reason") or r.get("Hazard"),
                "_categories": row_cats,
            })

    out["fired"]         = bool(matched_rows)
    out["total_matches"] = len(matched_rows)
    out["categories"]    = sorted(cats)
    out["keywords_hit"]  = sorted(kws)
    out["example_rows"]  = matched_rows[:3]
    return out


# ──────────────────────────────────────────────────────────────────────
# Public: build_prompt_extension
# ──────────────────────────────────────────────────────────────────────
def build_prompt_extension(trigger: Dict[str, Any]) -> str:
    """Return a string to APPEND to the Claude narrative prompt.

    Empty string when trigger.fired is False (caller appends unconditionally).
    """
    if not trigger or not trigger.get("fired"):
        return ""

    n = trigger.get("total_matches", 0)
    cats = trigger.get("categories", [])
    kws  = trigger.get("keywords_hit", [])
    examples = trigger.get("example_rows", [])

    # Compact example block for grounding
    ex_lines = []
    for ex in examples:
        bits = [
            str(ex.get("Date") or "")[:10],
            ex.get("Country") or "—",
            ex.get("Pathogen") or ex.get("Reason") or "—",
            (ex.get("Product") or "—")[:80],
        ]
        ex_lines.append("  - " + " | ".join(bits))
    ex_block = "\n".join(ex_lines) if ex_lines else "  (no specific example rows captured)"

    return f"""ADDITIONAL TASK — PROCESS AUTHORITY NOTE
This reporting window contains {n} recall(s) with thermal-processing /
commercial-sterility / scheduled-process relevance:

  Trigger categories: {", ".join(cats)}
  Matched keywords:   {", ".join(kws[:8])}
  Example rows:
{ex_block}

Append a SINGLE additional paragraph AFTER your three main paragraphs,
separated by a blank line, beginning with the literal string
"{PROCESS_AUTHORITY_LABEL}:" (followed by a space and the body text).

The paragraph must:
  1. Be 3–5 sentences, professional-engineering tone, NO bullets, NO
     headers, NO emoji, NO markdown, NO numbered lists.
  2. Frame the scheduled-process / qualified-process-authority
     requirement GLOBALLY, citing FDA (21 CFR 113/114), EU
     (Reg. 852/2004 + 2073/2005), CFIA (SFCA/SFCR), FSA / FSS UK,
     FSANZ Food Standards Code, and MHLW Japan as parallel frameworks
     that share the common principle.
  3. Tie the message to the hazard category that fired (botulinum risk,
     LACF, aseptic processing, anaerobic packaging, infant formula,
     etc.) — name the most-specific failure mode (unfiled or outdated
     scheduled process; resolved-without-qualified-review process
     deviation; uncontrolled formulation, packaging, or equipment change;
     environmental harbourage in cold-smoked or RTE lines).
  4. NEVER quote a specific F-value, D-value, z-value, hold-tube length,
     or temperature target — those are engagement deliverables from a
     qualified process authority and would be inappropriate without
     per-product validation.
  5. Recommend that operators in adjacent commodity categories engage
     a qualified process authority to verify scheduled-process adequacy
     and GMP alignment before any production change.

Return the paragraph as the FOURTH paragraph of your response, after a
single blank line separating it from paragraph three.
"""


# ──────────────────────────────────────────────────────────────────────
# Public: deterministic_fallback
# ──────────────────────────────────────────────────────────────────────
# Three deterministic variants keyed off which trigger category fired.
# Used when there is no ANTHROPIC_API_KEY (the AI narrative path is
# bypassed entirely). All three lead with the global framing so the
# tone matches the AI version.
# ──────────────────────────────────────────────────────────────────────
_GLOBAL_FRAMING = (
    "Across global jurisdictions — FDA (21 CFR 113/114), EU (Reg. 852/2004 "
    "and 2073/2005, with RASFF/EFSA oversight), CFIA (Safe Food for "
    "Canadians Regulations), FSA / FSS in the UK, FSANZ in Australia and "
    "New Zealand, and MHLW in Japan — the common principle is that "
    "low-acid, aseptic, and commercially sterile products require a "
    "scheduled process established by a qualified process authority and "
    "executed under documented GMP oversight."
)


def _fallback_botulinum(trigger: Dict[str, Any]) -> str:
    return (
        f"{PROCESS_AUTHORITY_LABEL}: This window includes Clostridium "
        f"botulinum or botulism-relevant recalls — a hazard category for "
        f"which scheduled-process adequacy is a mandatory regulatory "
        f"requirement, not a recommended best practice. "
        f"{_GLOBAL_FRAMING} Recall events in this category most commonly "
        f"trace to an unfiled or outdated scheduled process, a process "
        f"deviation resolved without qualified review, or a formulation "
        f"or packaging change introduced without process re-evaluation. "
        f"Manufacturers producing low-acid canned, acidified, fermented, "
        f"or anaerobically packaged products should confirm with a "
        f"qualified process authority that filings, GMP programmes, and "
        f"deviation handling remain current before any production change."
    )


def _fallback_lacf(trigger: Dict[str, Any]) -> str:
    return (
        f"{PROCESS_AUTHORITY_LABEL}: This window includes low-acid "
        f"canned-food (LACF) or scheduled-process-relevant hazards. "
        f"{_GLOBAL_FRAMING} For LACF and acidified products specifically, "
        f"scheduled-process filings (e.g. FDA Form 2541), retort "
        f"validation studies (temperature distribution and heat "
        f"penetration), and 21 CFR 108 emergency-permit pathways are "
        f"non-discretionary control points. Operators should confirm "
        f"that filings reflect the current product, container, and "
        f"equipment configuration, and that any process deviation has "
        f"been evaluated by a qualified process authority before product "
        f"release."
    )


def _fallback_aseptic(trigger: Dict[str, Any]) -> str:
    return (
        f"{PROCESS_AUTHORITY_LABEL}: This window includes aseptic, UHT, "
        f"or extended-shelf-life processing-relevant recalls. "
        f"{_GLOBAL_FRAMING} For continuous-flow systems specifically, the "
        f"adequacy of the scheduled process rests on validated hold-tube "
        f"residence-time distribution, validated lethality at the slowest-"
        f"heating particle, and an aseptic zone whose integrity is "
        f"verified between every production run. Operators should confirm "
        f"with a qualified process authority that hold-tube design, "
        f"continuous monitoring, and post-process aseptic-zone integrity "
        f"are aligned with current filings before any formulation, flow-"
        f"rate, or equipment change."
    )


def _fallback_anaerobic(trigger: Dict[str, Any]) -> str:
    return (
        f"{PROCESS_AUTHORITY_LABEL}: This window includes recalls in "
        f"anaerobic-packaging product categories (vacuum, modified-"
        f"atmosphere, reduced-oxygen, sous-vide, fermented or smoked "
        f"fish). {_GLOBAL_FRAMING} Anaerobic packaging removes the oxygen "
        f"barrier that would otherwise suppress C. botulinum growth, so "
        f"product safety depends entirely on water-activity, pH, salt "
        f"content, refrigeration discipline, or thermal-process control "
        f"— individually or in combination. Operators should confirm "
        f"with a qualified process authority that the multi-hurdle "
        f"approach in use is documented, validated, and consistent with "
        f"the current regulatory framework before extending shelf life "
        f"or modifying formulation."
    )


def _fallback_generic(trigger: Dict[str, Any]) -> str:
    return (
        f"{PROCESS_AUTHORITY_LABEL}: This window includes recalls in "
        f"commodity categories that fall under scheduled-process or "
        f"commercial-sterility regulation. {_GLOBAL_FRAMING} Manufacturers "
        f"whose products appear in this briefing — or who produce "
        f"comparable commodities — should confirm that scheduled "
        f"processes are currently filed, that GMP programmes align with "
        f"the current regulatory framework, and that any formulation, "
        f"equipment, or process-parameter change has been re-evaluated "
        f"by a qualified process authority. Recall events in adjacent "
        f"product categories are an early warning to verify, not to "
        f"react."
    )


def deterministic_fallback(trigger: Dict[str, Any]) -> str:
    """Return the full PA paragraph (starting with the label) when fired.

    Returns empty string when trigger.fired is False, so the caller can
    append unconditionally (`body + "\\n\\n" + pa_note` is a no-op when
    pa_note is empty).
    """
    if not trigger or not trigger.get("fired"):
        return ""
    cats = set(trigger.get("categories", []))

    # Priority order: most specific → most general.
    # Botulinum is the most acute hazard and gets its own dedicated text.
    if "botulinum" in cats:
        return _fallback_botulinum(trigger)
    if "lacf" in cats or "auto_category" in cats:
        return _fallback_lacf(trigger)
    if "aseptic" in cats:
        return _fallback_aseptic(trigger)
    if "anaerobic_packaging" in cats:
        return _fallback_anaerobic(trigger)
    return _fallback_generic(trigger)


__all__ = [
    "PROCESS_AUTHORITY_LABEL",
    "detect_process_authority_trigger",
    "build_prompt_extension",
    "deterministic_fallback",
]
