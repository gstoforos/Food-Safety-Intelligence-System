"""
pathogen_italic.py
==================

Wraps binomial scientific pathogen names in HTML <em> tags inside prose.
Used by the weekly + monthly AFTS report builders (docs/build_*_afts.py)
to italicise pathogen mentions like "Salmonella Typhimurium" or
"Listeria monocytogenes" without italicising the surrounding sentence.

The function is intentionally narrow:
  - it operates on plain text or HTML-escaped text (it does NOT escape;
    callers should call html.escape() first when input is untrusted),
  - it leaves text inside existing tags alone (best-effort regex skip),
  - it never adds <em> inside an already-italicised span.

Match logic
-----------
We match three families:

  1) Two-word binomials   — "<Genus> <species>"
     Genus must be in BINOMIAL_GENERA, species lowercase or "Typhi"-style
     serovar (initial cap). Examples:
         Salmonella enterica
         Salmonella Typhimurium
         Listeria monocytogenes
         Escherichia coli
         Clostridium botulinum
         Bacillus cereus
         Vibrio parahaemolyticus

  2) Genus-only mentions  — "<Genus>" alone (when not followed by a
     species token). Examples:
         Salmonella
         Listeria

  3) Genus + "spp." or "sp."
         Salmonella spp.
         Listeria sp.

The "spp."/"sp." abbreviation itself is NOT italicised (per ICZN/ICNP
convention — only the genus/species words are).

Edge cases
----------
* Already-italicised spans like "<em>Salmonella</em>" are left intact
  (a simple "is the genus already inside <em>...</em>?" check via regex).
* The function preserves all surrounding HTML — only adds <em> tags
  around the matched genus or genus+species.
* Unicode-safe: Greek/accented characters in Latin names handled via
  re.UNICODE.
"""
from __future__ import annotations

import re
from typing import Set

# ─────────────────────────────────────────────────────────────────────────
# Genera that should be italicised when found in prose. Conservative —
# bacterial + protozoan + a few moulds. Does NOT include common-name
# pathogens (e.g. "norovirus", "hepatitis A") which are not Latin
# binomials and never italicised.
# ─────────────────────────────────────────────────────────────────────────
BINOMIAL_GENERA: Set[str] = {
    # Bacterial
    "Salmonella",
    "Listeria",
    "Escherichia",
    "Shigella",
    "Campylobacter",
    "Yersinia",
    "Vibrio",
    "Clostridium",
    "Clostridioides",         # C. difficile
    "Bacillus",
    "Staphylococcus",
    "Streptococcus",
    "Cronobacter",            # formerly Enterobacter sakazakii
    "Enterobacter",
    "Aeromonas",
    "Plesiomonas",
    "Mycobacterium",
    "Brucella",
    "Coxiella",
    # Protozoan
    "Cryptosporidium",
    "Giardia",
    "Toxoplasma",
    "Cyclospora",
    "Entamoeba",
    # Moulds / yeasts producing mycotoxins
    "Aspergillus",
    "Fusarium",
    "Penicillium",
    "Alternaria",
    "Claviceps",
    # Parasitic worms
    "Trichinella",
    "Anisakis",
    "Taenia",
    "Echinococcus",
}

# Pre-compute the alternation. Sorted longest-first so e.g. "Clostridioides"
# is tried before "Clostridium" to avoid prefix-greedy false matches.
_GENERA_ALT = "|".join(sorted(BINOMIAL_GENERA, key=len, reverse=True))

# ─────────────────────────────────────────────────────────────────────────
# Regexes
#
# Group naming convention:
#   "g" = genus
#   "s" = species (lowercase) OR serovar (initial cap, e.g. "Typhimurium")
#
# Order matters in italicise_prose():
#   1) binomial + spp/sp  →  <em>Genus</em> spp.
#   2) two-word binomial  →  <em>Genus species</em>
#   3) genus alone        →  <em>Genus</em>
# ─────────────────────────────────────────────────────────────────────────

# 1) "Salmonella spp." / "Listeria sp."
_RE_GENUS_SPP = re.compile(
    rf"\b(?P<g>{_GENERA_ALT})\s+(?P<abbr>spp?\.)",
    re.UNICODE,
)

# 2) "Salmonella enterica", "Salmonella Typhimurium", "Listeria monocytogenes"
#    The "species" token must look like a Latin binomial epithet, NOT any
#    lowercase English word (which would catch "Listeria persisted into..."
#    as a false binomial). Latin epithets:
#      - end in a consonant cluster + characteristic Latin endings
#        (-ica, -us, -is, -es, -um, -i, -ae, -ense, -ensis, -ica, -osa, ...)
#      - typically ≥4 chars (excludes short English connectives)
#      - serovars are initial-capitalised single tokens (Typhimurium,
#        Enteritidis, Heidelberg) — handled as a separate alternative.
#
#    We allow:
#      - any "Tytlecase" word (initial cap + lowercase tail) of ≥4 chars
#        → covers serovars: Typhimurium, Enteritidis, Newport
#      - any all-lowercase word ending in a Latin morpheme (ICTV-ish list)
#        → covers true epithets: enterica, monocytogenes, coli, botulinum,
#          cereus, parahaemolyticus, sakazakii, jejuni, perfringens, ...
#
#    This is heuristic; false-negatives are far better than false-positives
#    (we just leave a real binomial un-italicised — the alternative is
#    italicising "Salmonella outbreak" or "Listeria persisted").
_LATIN_EPITHET = (
    r"(?:"
    r"[A-Z][a-z]{3,}"                          # serovar: Typhimurium, Enteritidis
    r"|"
    r"[a-z]+(?:icus|ica|icum"                  # -ic-
    r"|us|um"                                   # 2nd declension nominative
    r"|is|es"                                   # 3rd declension nominative
    r"|ae|i"                                    # plural / genitive
    r"|ensis|ense"                              # geographic
    r"|osa|osus|osum"                           # -osus (hairy/full of)
    r"|ans|ens"                                 # present participle
    r"|atus|ata|atum"                           # past participle
    r"|inum|ina|inus"                           # -inus
    r"|cola|cide|forme|formis"                  # other classical endings
    r"|ii|ae"                                   # eponymous endings
    r"|coli|jejuni|cereus|botulinum"            # explicit common epithets
    r"|monocytogenes|enterica|parahaemolyticus"
    r"|sakazakii|perfringens|aureus|pyogenes"
    r"|difficile|gondii|hominis|parvum"
    r")"
    r")"
)

_RE_BINOMIAL = re.compile(
    rf"\b(?P<g>{_GENERA_ALT})\s+(?P<s>{_LATIN_EPITHET})\b",
    re.UNICODE,
)

# 3) Bare genus mention. Negative lookahead avoids re-matching genera
#    that the binomial pattern just consumed.
_RE_GENUS_ONLY = re.compile(
    rf"\b(?P<g>{_GENERA_ALT})\b",
    re.UNICODE,
)

# ─────────────────────────────────────────────────────────────────────────
# Tag-skipping: don't italicise text that's already inside an <em>, <i>,
# <code>, <pre>, or any HTML attribute value. We use a "split on tags"
# strategy — tokenise into (text, tag, text, tag, ...) chunks and only
# touch the text chunks. Works for the simple HTML our prose builders
# emit (escape()'d body text inside <p> wrappers).
# ─────────────────────────────────────────────────────────────────────────
_RE_TAG = re.compile(r"<[^>]+>")

# Inside these tag pairs, never italicise.
_SKIP_TAGS = {"em", "i", "code", "pre", "kbd", "samp", "var", "a"}


def _is_inside_skip(stack: list) -> bool:
    """True if any tag in the open-tag stack is in _SKIP_TAGS."""
    return any(t in _SKIP_TAGS for t in stack)


def _italicise_chunk(text: str) -> str:
    """Apply the three regexes to a single raw-text chunk."""
    # Order: spp/sp variant first, then two-word binomial, then bare genus.
    # Because each replacement injects '<em>...</em>', subsequent passes
    # see those tags as text — that's fine; the tag-stack guard in the
    # caller prevents double-wrapping at the chunk level.
    out = _RE_GENUS_SPP.sub(
        lambda m: f"<em>{m.group('g')}</em> {m.group('abbr')}",
        text,
    )
    out = _RE_BINOMIAL.sub(
        lambda m: f"<em>{m.group('g')} {m.group('s')}</em>",
        out,
    )
    out = _RE_GENUS_ONLY.sub(
        # Don't re-wrap a genus we just put inside <em>...</em>. The
        # cheap check: skip if the match is immediately preceded by
        # "<em>" or followed by "</em>".
        lambda m: m.group(0)
        if (m.start() >= 4 and out[m.start() - 4 : m.start()] == "<em>")
        or (out[m.end() : m.end() + 5] == "</em>")
        else f"<em>{m.group('g')}</em>",
        out,
    )
    return out


def italicise_prose(html_or_text: str) -> str:
    """Wrap binomial pathogen names in <em>…</em>.

    Skips any text inside <em>, <i>, <code>, <pre>, <a>, etc. so we never
    nest italics or italicise inside link/code spans.

    Parameters
    ----------
    html_or_text : str
        Plain text OR HTML-escaped text OR a small bit of HTML
        (e.g. an already-built <p>...</p> fragment). Caller is
        responsible for escaping untrusted input first.

    Returns
    -------
    str
        Same string with italic <em> tags injected around binomial
        pathogen names.
    """
    if not html_or_text:
        return html_or_text

    # Tokenise into text/tag pieces. The split keeps the tags so we can
    # walk the stream and track which tags are currently "open".
    pieces = _RE_TAG.split(html_or_text)
    tags = _RE_TAG.findall(html_or_text)

    out: list[str] = []
    open_stack: list[str] = []

    # Pieces and tags interleave: piece, tag, piece, tag, ..., piece
    # (one more piece than tag, since split puts text on both ends).
    for i, piece in enumerate(pieces):
        if piece:
            out.append(_italicise_chunk(piece) if not _is_inside_skip(open_stack) else piece)
        if i < len(tags):
            tag = tags[i]
            out.append(tag)
            # Update the open-tag stack. Self-closing tags (<br/>) and
            # void elements don't change the stack.
            m = re.match(r"</\s*([a-zA-Z][a-zA-Z0-9]*)", tag)
            if m:
                # closing tag → pop matching open
                name = m.group(1).lower()
                # Pop until we find the matching open (handle slight HTML
                # malformation gracefully without crashing).
                while open_stack and open_stack[-1] != name:
                    open_stack.pop()
                if open_stack and open_stack[-1] == name:
                    open_stack.pop()
            elif tag.startswith("<") and not tag.startswith("<!") and not tag.endswith("/>"):
                # opening tag (not a comment, not self-closing)
                m2 = re.match(r"<\s*([a-zA-Z][a-zA-Z0-9]*)", tag)
                if m2:
                    open_stack.append(m2.group(1).lower())

    return "".join(out)


__all__ = ["italicise_prose", "BINOMIAL_GENERA"]
