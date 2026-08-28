#!/usr/bin/env python3
"""The controlled vocabulary, as a contract an agent can be held to.

WHY THIS FILE EXISTS
--------------------
Twelve defects were found in the schema on 2026-08-28 by hand-reviewing
thirty-five randomly drawn rows. Every one of them had the same shape: a
value was produced that nobody could trace back to something the notice
actually said. "Shiga toxin-producing E. coli" filed as a biotoxin. A cooked
ham filed as raw. A chocolate spread filed as meat, because pâté and pâte à
tartiner lose the same accent. A ready-to-eat mezze bowl filed as an
ingredient because the word "semi-finished" appeared in a sentence about
where a swab was taken.

Deterministic code made those mistakes. A language model, asked to fill the
same fields, will make them faster and with more confidence, and will also
invent terms that are not in the vocabulary at all — "refrigerated-RTE",
"meat, cooked", "Class 1 recall" — each of which silently creates a new
stratum of one row.

So the vocabulary is published here, the curator validates every proposal
against it, and a proposal carrying a term that is not in it is refused with
the closest legal term named. An agent cannot widen the schema by writing
into it.

WHAT AN AGENT MAY AND MAY NOT DO
--------------------------------
May:  propose a correction to Product, Reason, Pathogen, Company, Brand,
      Country, Region, Class, URL, Date, Notes — the fields a regulator's
      page can settle, each with a quote that supports it.
May NOT: write any analytical column. Those are DERIVED, by
      pipeline/enrich_schema.py, from the fields above. An agent that fixes
      the wording gets the derived columns rebuilt for free; an agent that
      writes them directly is asserting a classification no notice made.
May NOT: write Tier, Outbreak or report_week. Those drive published
      statistics and are set by dedicated code paths.
"""
from __future__ import annotations

from typing import Dict, List, Tuple

# ── The controlled terms, per analytical column ─────────────────────────
# Every one of these appears in pipeline/product_axes.py or
# pipeline/enrich_schema.py. "unknown" is legal everywhere and is the
# correct answer far more often than a filled cell is.
VOCABULARY: Dict[str, Tuple[str, ...]] = {
    "FoodCategory": (
        "meat-poultry", "meat-other", "fish-seafood", "dairy-soft-cheese",
        "dairy-other", "eggs-egg-products", "bakery-cereal", "nuts-seeds",
        "dried-fruit", "fresh-produce", "frozen-produce", "herbs-spices",
        "confectionery-snacks", "prepared-meals", "sauces-condiments",
        "beverages", "infant-food", "supplements", "other", "unknown",
    ),
    "ProcessType": (
        "raw", "cured-smoked", "fermented", "dried", "fresh-cut",
        "heat-treated", "composite", "unknown",
    ),
    "ConsumptionState": (
        "ready-to-eat", "ready-to-heat", "cook-before-eating", "ingredient",
        "unknown",
    ),
    "StorageCondition": ("frozen", "chilled", "ambient", "unknown"),
    "PackagingType": (
        "glass", "rigid-plastic", "flexible", "metal-can", "carton",
        "vacuum", "paper", "loose", "unknown",
    ),
    "PackagingForm": ("packaged", "unpackaged", "unknown"),
    "PreservationSystem": (
        "retort-sterilised", "aseptic-uht", "frozen", "low-moisture-dried",
        "fermented-acidified", "cured-smoked", "chilled-rte", "chilled-raw",
        "chilled-other", "ambient-stable", "ambient-other", "unknown",
    ),
    "HazardGroup": (
        "pathogen-bacterial", "pathogen-viral", "pathogen-parasitic",
        "biotoxin", "mycotoxin", "heavy-metal", "chemical-residue",
        "foreign-material", "pest", "unknown",
    ),
    "HazardCertainty": (
        "serious", "potentially serious", "potential risk", "not serious",
        "undecided", "unknown",
    ),
    "NoticeType": (
        "consumer-recall", "public-warning", "withdrawal", "border-rejection",
        "information", "unknown",
    ),
    "SeverityClass": (
        "class-i", "class-ii", "class-iii", "not-classified", "unknown",
    ),
}

# Fields an agent may propose a value for. Deliberately disjoint from
# VOCABULARY: an agent corrects what the notice SAYS, never what the schema
# CONCLUDES from it.
AGENT_WRITABLE = (
    "Pathogen", "Reason", "Product", "Company", "Brand", "Country",
    "Region", "Class", "URL", "Notes", "Date",
)

DERIVED_NEVER_WRITABLE = tuple(VOCABULARY) + (
    "EventID", "EnrichedBy", "EnrichedAt", "EnrichmentTier",
)

PIPELINE_ONLY = ("Tier", "Outbreak", "report_week", "DateAdded",
                 "LastUpdated", "LastChecked")


def _closest(value: str, allowed: Tuple[str, ...]) -> str:
    """Cheapest useful hint: the legal term sharing the most leading text."""
    v = (value or "").strip().lower().replace("_", "-").replace(" ", "-")
    best, score = "", 0
    for a in allowed:
        n = len(__import__("os").path.commonprefix([v, a]))
        if n > score:
            best, score = a, n
    return best


def validate_value(column: str, value: str) -> List[str]:
    """[] if the value is a legal term for that column, else one refusal."""
    allowed = VOCABULARY.get(column)
    if allowed is None:
        return []
    if value in allowed:
        return []
    hint = _closest(value, allowed)
    return [f"{column}={value!r} is not a controlled term"
            + (f" — did you mean {hint!r}?" if hint else "")
            + f" (legal: {', '.join(allowed)})"]


def validate_changes(changes: Dict[str, str]) -> List[str]:
    """Every refusal a set of proposed changes earns, in one list.

    Three separate refusals, because they are three different mistakes:
      * writing a DERIVED column — the agent is classifying, not reporting;
      * writing a PIPELINE-ONLY column — the agent is touching published
        statistics;
      * writing a term outside the vocabulary — the agent is inventing a
        stratum.
    """
    out: List[str] = []
    for k, v in changes.items():
        if k in PIPELINE_ONLY:
            out.append(f"{k!r} is set by the pipeline, not by an agent — "
                       f"Tier, Outbreak and report_week drive published "
                       f"statistics")
        elif k in DERIVED_NEVER_WRITABLE:
            out.append(f"{k!r} is DERIVED from Product/Reason/Class by "
                       f"pipeline.enrich_schema. Correct the wording and the "
                       f"column rebuilds itself; writing it directly asserts "
                       f"a classification no notice made")
        elif k not in AGENT_WRITABLE:
            out.append(f"{k!r} is not an agent-writable field "
                       f"(allowed: {', '.join(AGENT_WRITABLE)})")
        else:
            out += validate_value(k, str(v))
    return out
