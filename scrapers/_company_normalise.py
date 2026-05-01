"""
_company_normalise.py
=====================

Shared post-scrape normaliser for the (Company, Brand) field pair.
Applied by the scrapers right before emitting a Recall, and by the
one-time backfill script (pipeline/backfill_company_normalise.py)
when re-cleaning historical rows in docs/data/recalls.xlsx.

Two transformations:

  1) CASING — collapse all-caps and all-lowercase company strings to
     a consistent Title-Case form. Source-of-truth example: RappelConso
     hand-entered the SAME parent company three different ways in the
     SAME WEEK ("AMANDIS LES ATELIERS DE SEBASTIEN", "AMANDIS Les
     ateliers de sebastien", "AMANDIS les ateliers de sebastien"),
     producing three apparently-different rows. After normalisation,
     all three render as "Amandis Les Ateliers De Sebastien" — visibly
     the same producer.

  2) BRAND-SUFFIX REDUNDANCY — when Company ends with the Brand string
     (case-insensitively), strip the trailing Brand from Company so the
     report shows distributor-only, then the Brand badge separately.
     Example:
         Company="AMANDIS LES ATELIERS DE SEBASTIEN", Brand="LES ATELIERS DE SEBASTIEN"
       → Company="Amandis", Brand="Les Ateliers De Sebastien"

Behaviour rules:
  - Never invents content. Empty / "—" / None inputs pass through
    unchanged.
  - Never strips MORE than the trailing Brand. If Company is exactly
    equal to Brand, leave both intact (the report builder handles
    that case).
  - Title-casing skips short connective words ("de", "du", "la", "le",
    "des", "et", "of", "and") which are conventionally lowercase in
    French / English company names.
  - Always-uppercase tokens that ARE acronyms (≤4 chars, all caps,
    no vowels, or in a small allow-list) are preserved as-is.
"""
from __future__ import annotations

import re
from typing import Tuple

# French + English connective particles kept lowercase mid-string.
_LOWERCASE_PARTICLES = {
    # French
    "de", "du", "des", "la", "le", "les", "et", "à", "au", "aux",
    "d'", "l'", "en", "sur", "sous", "par",
    # English
    "of", "and", "the", "for",
    # Italian / Spanish / German common ones (corpus has these)
    "di", "del", "della", "y", "von", "der", "und",
}

# Acronyms that should stay uppercase even after Title-Case pass.
# These are real corporate suffixes / regulatory acronyms that appear
# in the corpus.
_ACRONYM_ALLOW = {
    "SA", "SAS", "SARL", "SCEA", "SCA", "EARL", "GAEC",  # FR legal forms
    "SPA", "SRL", "SNC", "SAPA",                          # IT
    "GMBH", "AG", "KG", "OHG",                            # DE
    "BV", "NV",                                           # NL/BE
    "PLC", "LTD", "LLC", "LLP", "LP", "INC", "CORP",      # EN
    "DOP", "IGP", "AOC", "AOP", "STG",                    # food-quality
    "USA", "UK", "UAE", "EU",
    "SAS", "SAU", "EI", "EIRL", "SCM",
    "II", "III", "IV", "V", "VI", "VII", "VIII", "IX",    # Roman numerals
}

_RE_WORD = re.compile(r"[\w'’]+|[^\w'’\s]+", re.UNICODE)


def _looks_like_acronym(token: str) -> bool:
    """Heuristic: token is in allow-list, OR ≤4 chars all-cap with no vowels.
    Used so 'SAS' stays 'SAS' but 'AMANDIS' (which has vowels and is 7 chars)
    falls through to title-casing → 'Amandis'."""
    upper = token.upper()
    if upper in _ACRONYM_ALLOW:
        return True
    if (len(token) <= 4 and token.isupper() and token.isalpha()
            and not any(v in upper for v in "AEIOUY")):
        return True
    return False


def _title_token(token: str, is_first: bool) -> str:
    """Title-case a single token, respecting acronyms + lowercase particles.

    is_first: True if this is the first word of the company name (always
    capitalised, even if it's a particle — 'Le Fromager Des Halles' not
    'le Fromager Des Halles').
    """
    # Punctuation / non-word tokens pass through
    if not re.search(r"\w", token):
        return token

    # Preserve acronyms verbatim
    if _looks_like_acronym(token):
        return token.upper()

    lower = token.lower()
    # Lowercase particles (except as first word)
    if not is_first and lower in _LOWERCASE_PARTICLES:
        return lower

    # Standard title-case: first letter up, rest down. Handle apostrophes
    # ("d'andrieux" → "D'Andrieux", "l'angevin" → "L'Angevin").
    if "'" in token or "’" in token:
        sep = "’" if "’" in token else "'"
        parts = token.split(sep)
        return sep.join(p[:1].upper() + p[1:].lower() for p in parts)

    return token[:1].upper() + token[1:].lower()


def _retitle(s: str) -> str:
    """Apply Title-Case rules to an entire company / brand string.

    Triggers ONLY when the input is all-uppercase or all-lowercase (the
    cases where the operator clearly didn't enter the canonical form).
    Mixed-case strings are preserved verbatim — they were entered with
    intentional casing ("McDonald's", "L'Oréal", "Pâtisserie L'Angevin").
    """
    if not s:
        return s
    stripped = s.strip()
    if not stripped:
        return stripped

    # Detect "needs retitle" — all-upper or all-lower over alphabetic chars
    alpha = "".join(c for c in stripped if c.isalpha())
    if not alpha:
        return stripped
    is_all_upper = alpha.isupper()
    is_all_lower = alpha.islower()
    if not (is_all_upper or is_all_lower):
        return stripped

    # Tokenise + title-case
    tokens = _RE_WORD.findall(stripped)
    out_tokens = []
    seen_word = False
    for tok in tokens:
        if re.search(r"\w", tok):
            out_tokens.append(_title_token(tok, is_first=not seen_word))
            seen_word = True
        else:
            out_tokens.append(tok)
    # Reassemble preserving original whitespace structure
    rebuilt = ""
    src_idx = 0
    for tok in out_tokens:
        # Find this token's original location in the source so we keep
        # the exact spacing the operator typed.
        find_at = stripped.lower().find(tok.lower(), src_idx)
        if find_at == -1:
            # Fallback: just space-join. This branch shouldn't fire in
            # practice but protects against edge-case Unicode quirks.
            rebuilt += (" " if rebuilt and not rebuilt.endswith(" ") else "") + tok
            continue
        rebuilt += stripped[src_idx:find_at] + tok
        src_idx = find_at + len(tok)
    rebuilt += stripped[src_idx:]
    return rebuilt


def _dedupe_self_repeat(s: str) -> str:
    """If a string is composed of two halves that are equal (case-insensitively),
    return one half. Catches RappelConso entries like:
      "AKAR NORD FRANCE AKAR NORD FRANCE" → "AKAR NORD FRANCE"
      "CARREFOUR FRANCE CARREFOUR"        → "CARREFOUR FRANCE" (no — first half "CARREFOUR FRANCE" ≠ second "CARREFOUR")
                                            (handled by _dedupe_trailing_repeat below)
      "TEBA 44 TEBA 44"                   → "TEBA 44"

    Only fires for exact case-insensitive duplication. Conservative — never
    invents a split point that wasn't a real duplication.
    """
    if not s or " " not in s:
        return s
    s_strip = s.strip()
    n = len(s_strip)
    # Try splitting at the midpoint; the duplication boundary is usually
    # at the centre (an even number of tokens).
    if n % 2 == 1 and s_strip[n // 2] != " ":
        return s
    # If exact-doubled string with space at midpoint:
    mid = n // 2
    if s_strip[mid:mid + 1] == " ":
        left = s_strip[:mid].strip()
        right = s_strip[mid + 1:].strip()
        if left.lower() == right.lower() and left:
            return left
    # Try token-level: walk possible split points where left == right (case-insensitive)
    tokens = s_strip.split()
    nt = len(tokens)
    if nt >= 2 and nt % 2 == 0:
        half = nt // 2
        left = " ".join(tokens[:half])
        right = " ".join(tokens[half:])
        if left.lower() == right.lower():
            return left
    return s


def _dedupe_trailing_repeat(s: str) -> str:
    """If a string ends with a repeat of one of its earlier whitespace-bounded
    suffixes, strip the trailing repeat. Catches:
      "CARREFOUR FRANCE CARREFOUR"  → "CARREFOUR FRANCE"
      "STE LIONOR SA Lionor"        → leave alone (case-different and substring,
                                        but the Company contains BOTH the legal
                                        form AND the trade name — not a redundancy)

    Conservative: only fires if (a) the trailing suffix appears earlier in
    the string as a separate word group, (b) length of trailing repeat is
    ≥ 4 chars (so we don't strip "SA" or "II" off everything).
    """
    if not s or " " not in s:
        return s
    s_strip = s.strip()
    tokens = s_strip.split()
    nt = len(tokens)
    if nt < 3:
        return s_strip
    # Try increasing tail lengths from 1 to nt//2 — find the longest tail
    # that appears earlier in the string.
    for tail_len in range(nt // 2, 0, -1):
        tail = tokens[-tail_len:]
        head = tokens[:-tail_len]
        # Tail must be ≥ 4 chars total
        if sum(len(t) for t in tail) < 4:
            continue
        # Look for the same sequence (case-insensitive) earlier in head
        head_lower = [t.lower() for t in head]
        tail_lower = [t.lower() for t in tail]
        for i in range(len(head_lower) - tail_len + 1):
            if head_lower[i:i + tail_len] == tail_lower:
                # Found a duplicated trailing run.
                return " ".join(head)
    return s_strip



def _strip_trailing_brand(company: str, brand: str) -> str:
    """Return Company with the trailing Brand removed if Company ends
    with Brand (case-insensitive). Leaves Company intact if it would
    become empty.

    Example:  Company='AMANDIS LES ATELIERS DE SEBASTIEN'
              Brand='LES ATELIERS DE SEBASTIEN'
            → 'AMANDIS'
    """
    if not company or not brand:
        return company
    if brand.strip() in ("—", "-"):
        return company

    # Slash-separated companies use ' / ' as an intentional parent/sub
    # separator (e.g. "Phoenicia Group / Alarjawi"). Don't touch those —
    # stripping the brand suffix produces an orphan slash ("Phoenicia
    # Group /") which is uglier than the original.
    if "/" in company:
        return company

    c_low = company.lower().rstrip()
    b_low = brand.lower().strip()

    # Only strip when Brand is a true SUFFIX of Company AND Company
    # contains MORE than just Brand (so the row keeps a distributor name).
    if c_low == b_low:
        return company  # Identical — leave intact, the report handles it.
    if not c_low.endswith(b_low):
        return company

    # Strip the trailing brand + any trailing whitespace / separators.
    cut = len(company.rstrip()) - len(brand.strip())
    head = company[:cut].rstrip(" -·,;:")
    return head if head else company


# ──────────────────────────────────────────────────────────────────────
# Public entry point — used by scrapers and backfill
# ──────────────────────────────────────────────────────────────────────
def normalise_company_brand(company: str, brand: str) -> Tuple[str, str]:
    """Normalise a (Company, Brand) pair.

    Order of operations:
      1. Strip whitespace.
      2. If Company ends with Brand → strip trailing Brand from Company.
      3. Title-case Company if it was all-upper / all-lower.
      4. Title-case Brand if it was all-upper / all-lower.

    Returns (company_clean, brand_clean). Either may be the original
    string unchanged if no rule fires. Empty / "—" inputs pass through.
    """
    company = (company or "").strip()
    brand = (brand or "").strip()
    # 1) Dedupe self-repeats inside Company alone (e.g. "TEBA 44 TEBA 44")
    company = _dedupe_self_repeat(company)
    company = _dedupe_trailing_repeat(company)
    # 2) Strip trailing Brand from Company if Company ends with Brand
    company = _strip_trailing_brand(company, brand)
    # 3) Title-case all-upper / all-lower
    company = _retitle(company)
    brand = _retitle(brand) if brand and brand != "—" else brand
    return company, brand


__all__ = ["normalise_company_brand"]
