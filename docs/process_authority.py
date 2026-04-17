"""
Process Authority trigger for AFTS weekly and monthly intelligence reports.

When recalls in the reporting window match thermal-processing / low-acid /
anaerobic-packaging hazard signals, the AI narrative adds a dedicated
4th paragraph — the "Process Authority Note" — with domain-specific
process-engineering commentary.

Both docs/build_weekly_report_afts.py and docs/build_monthly_report_afts.py
import from this module so the trigger logic and prompt live in ONE place.

-----------------------------------------------------------------------------
EDITING GUIDE (for George)
-----------------------------------------------------------------------------

To refine the prompt wording:
    Edit PROCESS_AUTHORITY_PROMPT below.

To adjust trigger sensitivity:
    - TIGHTER (fewer fires): trim TRIGGER_PATHOGENS / TRIGGER_PRODUCT_TERMS
    - LOOSER (more fires): add terms to those lists

To change the paragraph label that appears in the report:
    Edit PROCESS_AUTHORITY_LABEL below.
-----------------------------------------------------------------------------
"""
from __future__ import annotations
from typing import Dict, List, Tuple, Any


# =============================================================================
# EDITABLE: trigger keywords. Case-insensitive substring match on the
# pathogen/reason/product text of each recall row.
# =============================================================================

# Pathogens whose presence alone triggers the Process Authority Note.
# Keep this list focused on hazards with specific thermal-processing relevance.
TRIGGER_PATHOGENS = (
    # Spore-formers with thermal-process implications (LACF / acidified foods)
    "clostridium",
    "botulinum",
    "botulin",
    "botulism",
    "bacillus cereus",
    "cereulide",
    "clostridium perfringens",
    # Post-thermal survivors / recontamination in ROP/MAP/aseptic systems
    "cronobacter",
    "sakazakii",
)

# Product-text or reason-text cues that indicate a low-acid / thermally-
# processed / anaerobically-packaged product, even when the named pathogen
# alone wouldn't trigger the rule (e.g. Listeria in a hot-filled sauce).
TRIGGER_PRODUCT_TERMS = (
    # Low-acid canned foods & acidified foods (21 CFR 113 / 114)
    "low-acid", "low acid", "lacf", "acidified",
    "canned", "canning", "can ",
    "retort", "retorted", "sterili", "commercial sterility",
    # Aseptic & hot-fill
    "aseptic", "hot-fill", "hot fill", "hot-filled",
    # Continuous-flow thermal
    "uht", "htst", "pasteuri",
    # Reduced-oxygen / anaerobic packaging — raises C. botulinum concern
    "vacuum pack", "vacuum-pack", "vacuum sealed",
    "modified atmosphere", "map ", "rop ",
    "sous-vide", "sous vide",
    # Shelf-stable / preserved
    "shelf-stable", "shelf stable",
    "fermented",  # pH-dependent safety
    # Oil infusions (classic botulism vector)
    "infused oil", "flavored oil", "flavoured oil", "garlic in oil", "herbs in oil",
    # Specific product types historically linked to botulism outbreaks
    "pesto", "tapenade", "marinated", "pickle",
)


# =============================================================================
# EDITABLE: label shown in the report and email before the Process Authority
# paragraph. Keep it short — it's used as a heading.
# =============================================================================
PROCESS_AUTHORITY_LABEL = "Process Authority Note"


# =============================================================================
# EDITABLE: the AI prompt appended to the standard 3-paragraph request when
# at least one recall in the window matched the trigger.
#
# Placeholder {matching_incidents} is substituted at runtime with the list of
# recalls that fired the trigger. Do NOT remove that placeholder or the
# builder will crash.
#
# DESIGN NOTES (per AFTS editorial policy):
#   - Do NOT cite specific thermal-process target values (F-values, D-values,
#     lethality minutes). Those are engagement deliverables from a qualified
#     process authority, not public briefing content.
#   - Frame the note around REGULATORY COMPLIANCE and PROCESS AUTHORITY
#     OVERSIGHT. The takeaway is: a shelf-stable low-acid or acidified
#     product must be reviewed under the guidance of a qualified process
#     authority, confirming the established scheduled process meets GMPs
#     and the applicable regulatory framework (21 CFR 113 / 114, FDA 2541
#     filing, EU / CFIA equivalents).
#   - Keep the tone authoritative but do not give away engineering values.
# =============================================================================
PROCESS_AUTHORITY_PROMPT = """
Because this reporting window contains recall(s) with thermal-processing or
anaerobic-packaging hazard signals, add a FOURTH paragraph titled
"{label}:" (exactly that word, followed by a colon). This paragraph should
read like a practising Process Authority briefing operators on the compliance
implications of the matching incident(s).

MATCHING INCIDENT(S) in this window:
{matching_incidents}

In this fourth paragraph (4–6 sentences, prose — NO bullets, NO markdown):

1. Name the product class at stake in the matching incident(s): shelf-stable
   low-acid canned food (LACF) under 21 CFR 113, acidified food under 21 CFR
   114, aseptic / UHT / HTST continuous-flow product, hot-filled shelf-stable
   product, or refrigerated reduced-oxygen packaged product. Each category has
   a distinct regulatory framework and a distinct set of control points.

2. State that every shelf-stable low-acid product of this type MUST be
   reviewed to confirm it has achieved the established scheduled thermal
   process, that production is conducted under proper GMPs, and that the
   process has been validated under the guidance of a qualified process
   authority in order to control Clostridium botulinum spores. Do NOT quote
   specific F-value or D-value numerical targets — those are engagement
   deliverables from a qualified process authority, not briefing content.

3. Frame the probable compliance gap in regulatory terms: absence or lapse
   of a filed scheduled process (FDA Form 2541 / 2541e), a process deviation
   resolved without qualified process authority review, container or
   pouch-seal integrity control gaps, or a formulation change (pH, a_w,
   salt, preservative) introduced without re-evaluation by a process
   authority. Reference the analogous EU / CFIA regulatory posture where
   relevant.

4. Close with the concrete regulatory consequence operators should expect
   from a Tier-1 / Class-I classification on a low-acid product (21 CFR 108
   process filing audit, FDA Form 483 on subsequent inspection, or the
   equivalent EU / CFIA enforcement step). Do NOT prescribe a portfolio-wide
   verification step to the reader.

Keep the tone authoritative and compliance-oriented. Do not give away
specific process-engineering targets. Do not repeat content already covered
in paragraphs 1–3.
"""


# =============================================================================
# END OF EDITABLE SECTION — logic below should not need changing
# =============================================================================


def _row_matches_trigger(row: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Decide whether a single recall row should fire the Process Authority trigger.
    Returns (matched, reasons) — reasons names which keyword(s) matched so the
    narrative can point to specific evidence.
    """
    pathogen = str(row.get("Pathogen") or "").lower()
    reason   = str(row.get("Reason")   or "").lower()
    product  = str(row.get("Product")  or "").lower()
    company  = str(row.get("Company")  or "").lower()

    haystack_pathogen = f"{pathogen} {reason}"
    haystack_product  = f"{reason} {product} {company}"

    hits: List[str] = []
    for kw in TRIGGER_PATHOGENS:
        if kw in haystack_pathogen:
            hits.append(f"pathogen:{kw}")
    for kw in TRIGGER_PRODUCT_TERMS:
        if kw in haystack_product:
            hits.append(f"product:{kw.strip()}")

    return (len(hits) > 0, hits)


def detect_process_authority_trigger(recalls: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Scan a list of recall rows (for the weekly or monthly window) and return
    a dict describing whether the Process Authority Note should fire, and
    which incidents matched so the AI prompt can reference them specifically.

    Returned dict shape:
        {
            "fired": bool,
            "matched_rows": [...],   # up to 5 sanitised dicts
            "total_matches": int,
            "keywords_hit": [str, ...],
        }
    """
    matched: List[Dict[str, Any]] = []
    keywords_all: List[str] = []
    for r in recalls:
        ok, hits = _row_matches_trigger(r)
        if ok:
            matched.append({
                "date":     str(r.get("Date") or "")[:10],
                "source":   (r.get("Source") or "")[:40],
                "country":  (r.get("Country") or "")[:40],
                "company":  (r.get("Company") or "")[:80],
                "product":  (r.get("Product") or "")[:140],
                "pathogen": (r.get("Pathogen") or "")[:80],
                "reason":   (r.get("Reason")  or "")[:140],
                "tier":     r.get("Tier"),
                "outbreak": r.get("Outbreak"),
                "triggered_by": hits,
            })
            keywords_all.extend(hits)
    return {
        "fired": len(matched) > 0,
        "matched_rows": matched[:5],   # cap context for the prompt
        "total_matches": len(matched),
        "keywords_hit": sorted(set(keywords_all)),
    }


def build_prompt_extension(trigger: Dict[str, Any]) -> str:
    """
    Produce the AI-prompt extension text that should be appended to the
    standard weekly/monthly prompt ONLY when trigger['fired'] is True.
    Returns empty string when the trigger didn't fire.
    """
    if not trigger.get("fired"):
        return ""

    import json
    incidents_json = json.dumps(trigger["matched_rows"], indent=2, ensure_ascii=False)
    # Add a note about total count if more were matched than shown
    if trigger["total_matches"] > len(trigger["matched_rows"]):
        incidents_json += (
            f"\n\n(Plus {trigger['total_matches'] - len(trigger['matched_rows'])} "
            f"additional matching recall(s) in the window not shown above.)"
        )

    return PROCESS_AUTHORITY_PROMPT.format(
        label=PROCESS_AUTHORITY_LABEL,
        matching_incidents=incidents_json,
    )


def deterministic_fallback(trigger: Dict[str, Any]) -> str:
    """
    When no AI key is set, produce a compliance-angled Process Authority Note
    from the matched rows. Used by the fallback narrative path so non-AI runs
    still surface the regulatory angle. Deliberately contains NO quantitative
    F-value / D-value / lethality targets — those are engagement deliverables,
    not public briefing content.
    """
    if not trigger.get("fired"):
        return ""
    n = trigger["total_matches"]
    hits = trigger.get("keywords_hit", [])
    has_clostridium = any("clostridium" in h or "botulin" in h for h in hits)
    has_lacf_signal = any(
        h.startswith("product:") and any(k in h for k in (
            "low-acid", "lacf", "canned", "retort", "aseptic",
            "hot-fill", "sterili", "vacuum", "rop",
            "modified atmosphere", "infused oil", "sous",
            "shelf-stable",
        ))
        for h in hits
    )

    first = trigger["matched_rows"][0] if trigger["matched_rows"] else {}
    company = first.get("company", "the recalling firm")
    country = first.get("country", "")
    pathogen = first.get("pathogen", "the hazard")

    if has_clostridium:
        return (
            f"{PROCESS_AUTHORITY_LABEL}: This window contains {n} incident(s) implicating "
            f"Clostridium or botulinum toxin, with {company} ({country}) cited for {pathogen}. "
            f"Any shelf-stable low-acid canned food (LACF, 21 CFR 113) or acidified food "
            f"(21 CFR 114) in the affected category must be reviewed to confirm that the "
            f"product has achieved the established scheduled thermal process, that production "
            f"is conducted under proper GMPs and the applicable regulatory framework, and "
            f"that this has been validated under the guidance of a qualified process authority "
            f"in order to control Clostridium botulinum spores. The most common compliance "
            f"gap behind recalls of this profile is an unfiled or outdated scheduled process "
            f"(FDA Form 2541 / 2541e), a process deviation resolved without qualified process "
            f"authority review, or a formulation change — pH, a_w, salt, preservative — "
            f"introduced without re-evaluation. A Tier-1 / Class-I classification on a "
            f"low-acid product reliably triggers a 21 CFR 108 process filing audit and FDA "
            f"Form 483 citations on the subsequent inspection, with analogous EU / CFIA "
            f"enforcement steps."
        )
    elif has_lacf_signal:
        return (
            f"{PROCESS_AUTHORITY_LABEL}: {n} incident(s) in this window carry thermal-processing "
            f"or anaerobic-packaging signals. Shelf-stable low-acid canned foods (LACF, 21 CFR 113), "
            f"acidified foods (21 CFR 114), aseptic / UHT / HTST continuous-flow products, "
            f"hot-filled shelf-stable products and refrigerated reduced-oxygen-packaged products "
            f"must each be reviewed to confirm the established scheduled process has been "
            f"achieved, that production is under proper GMPs and regulatory compliance, and "
            f"that the process is validated under the guidance of a qualified process authority. "
            f"The most probable compliance gaps are an unfiled or outdated scheduled process, "
            f"a process deviation resolved without qualified review, container or pouch-seal "
            f"integrity control lapses, or a formulation change introduced without re-evaluation."
        )
    else:
        # Trigger fired on a non-LACF signal (e.g. Cronobacter in powdered infant
        # formula or Bacillus cereus in cooked RTE). Softer compliance framing.
        return (
            f"{PROCESS_AUTHORITY_LABEL}: {n} incident(s) in this window carry thermal-processing "
            f"or anaerobic-packaging signals and warrant a compliance review of the scheduled "
            f"thermal process and applicable packaging-system controls. Operators should "
            f"confirm production is conducted under proper GMPs and the applicable regulatory "
            f"framework, and that the process has been signed off by a qualified process "
            f"authority. Recall events in this product class commonly trace to an unfiled or "
            f"outdated scheduled process, a process deviation resolved without qualified "
            f"review, or a formulation or packaging change introduced without re-evaluation."
        )
