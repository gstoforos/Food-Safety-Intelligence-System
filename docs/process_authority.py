"""
Shared pathogen-name italicisation helper used by both the weekly and monthly
report builders. Hoisted from build_weekly_report_afts.py into its own module
so the monthly builder can import + reuse exactly the same behaviour.

Binomial nomenclature rules applied:
  - Full binomial "Genus species" → both italicised (Listeria monocytogenes).
  - Bare genus ("Listeria") → italicised when the next word is plain English
    ("Listeria lethality" becomes "<em>Listeria</em> lethality").
  - Abbreviated form ("E. coli") → italicised as a unit.
  - "Listeria spp." / "spp" → italicise genus only, keep "spp." roman.
  - "Norovirus" → italicised as a single word (virus name, not a binomial).

Species list is a whitelist rather than a regex catch-all, so we don't
falsely italicise "Listeria and" or "Salmonella outbreak" etc.
"""
from __future__ import annotations
import re


_PATHOGEN_GENERA = (
    "Listeria", "Clostridium", "Salmonella", "Escherichia", "Bacillus",
    "Cronobacter", "Staphylococcus", "Campylobacter", "Vibrio", "Yersinia",
    "Shigella",
)
_PATHOGEN_SPECIES = (
    "monocytogenes", "ivanovii",
    "botulinum", "perfringens", "difficile", "tetani",
    "enterica", "bongori", "typhimurium", "enteritidis",
    "coli",
    "cereus", "subtilis", "anthracis",
    "sakazakii", "malonaticus",
    "aureus",
    "jejuni",
    "parahaemolyticus", "vulnificus", "cholerae",
    "enterocolitica", "pseudotuberculosis",
    "flexneri", "sonnei", "dysenteriae",
)

_genera_alt   = "|".join(_PATHOGEN_GENERA)
_species_alt  = "|".join(_PATHOGEN_SPECIES)
_pat_binomial = re.compile(rf"\b({_genera_alt})\s+({_species_alt})\b", re.IGNORECASE)
_pat_genus    = re.compile(rf"\b({_genera_alt})\b")
_pat_abbrev   = re.compile(rf"\b([A-Z])\.\s*({_species_alt})\b")
_pat_norovirus = re.compile(r"\bNorovirus\b")

_PLACEHOLDER_OPEN  = "\x01EM\x02"
_PLACEHOLDER_CLOSE = "\x01/EM\x02"


def italicise_prose(text: str) -> str:
    """Wrap pathogen names in <em> tags; safe on already-escaped HTML strings."""
    # Pass 1: binomials
    text = _pat_binomial.sub(
        lambda m: f"{_PLACEHOLDER_OPEN}{m.group(1)} {m.group(2)}{_PLACEHOLDER_CLOSE}",
        text,
    )
    # Pass 2: abbreviated form
    text = _pat_abbrev.sub(
        lambda m: f"{_PLACEHOLDER_OPEN}{m.group(0)}{_PLACEHOLDER_CLOSE}",
        text,
    )
    # Pass 3: bare genus only in unmarked chunks
    def _wrap_plain(chunk: str) -> str:
        return _pat_genus.sub(
            lambda m: f"{_PLACEHOLDER_OPEN}{m.group(0)}{_PLACEHOLDER_CLOSE}",
            chunk,
        )
    parts = re.split(
        f"({re.escape(_PLACEHOLDER_OPEN)}.*?{re.escape(_PLACEHOLDER_CLOSE)})",
        text,
    )
    text = "".join(
        part if part.startswith(_PLACEHOLDER_OPEN) else _wrap_plain(part)
        for part in parts
    )
    # Pass 4: Norovirus (only in plain chunks)
    parts = re.split(
        f"({re.escape(_PLACEHOLDER_OPEN)}.*?{re.escape(_PLACEHOLDER_CLOSE)})",
        text,
    )
    text = "".join(
        part if part.startswith(_PLACEHOLDER_OPEN)
        else _pat_norovirus.sub(
            f"{_PLACEHOLDER_OPEN}Norovirus{_PLACEHOLDER_CLOSE}", part
        )
        for part in parts
    )
    return text.replace(_PLACEHOLDER_OPEN, "<em>").replace(_PLACEHOLDER_CLOSE, "</em>")
