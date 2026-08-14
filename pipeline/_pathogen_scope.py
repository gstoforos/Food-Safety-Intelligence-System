"""FSIS Tier-1 pathogen scope — locked 2026-04-30; expanded 2026-05-12.

Only recalls whose Pathogen matches one of these get into Recalls.
Anything else is silently dropped (or stays in Pending for re-extraction
if Company/Brand is missing).

EXPANSION 2026-05-12 — undeclared pharmaceutical adulteration:
INVIMA Alert 123-2026 (BICHOTA "Concentrado de frutas...") was
notified to INFOSAN (WHO/FAO) and the EU RASFF system because the
product contained undeclared sildenafil. INFOSAN treats undeclared
pharmaceutical adulteration as a critical food-safety hazard equivalent
to bacterial contamination. The pre-2026-05-12 scope rejected such rows
as "pathogen_out_of_scope" — a false negative. Added the adulteration
vocabulary so similar cases (FDA "undeclared drug ingredient" recalls,
RASFF "unauthorized substance" notifications) are now in scope.
"""
from __future__ import annotations

TIER1_KEYWORDS = (
    # Bacterial
    "listeria", "salmonella",
    "e. coli", "e.coli", "escherichia coli", "stec",
    "o157", "o104", "o121", "o26", "o45", "o103", "o111", "o145",
    "shiga toxin", "shigatoxin",
    "botulin", "botulisme", "clostridium botulin",
    "bacillus cereus", "cereulide",
    "cronobacter", "sakazakii",
    "staphylococcus", "staph", "enterotoxin", "entérotoxine",
    "campylobacter",
    # Vibrio (added 2026-08-14 on operator instruction "Add vibrio").
    #
    # THIS WAS AN OMISSION, NOT A POLICY. Vibrio was already recognised
    # everywhere else in the pipeline and only this scope list left it out:
    #   scrapers/_models.py          PATHOGEN_RULES + _TIERS + tier_2_pathogens
    #   pipeline/_publish_gate.py    line 200, listed as an accepted pathogen
    #   pipeline/verify_pathogen_in_source.py   "Vibrio": ["vibrio"]
    #   pipeline/regulator_apis.py   "Vibrio": "vibrio"
    #   every scraper PATHOGEN_KEYWORDS (CFIA, FSA UK, FSANZ,
    #   Livsmedelsverket, ...) and every AI prompt that enumerates scope
    #   ("Campylobacter, Yersinia, Vibrio, Cyclospora, Cronobacter, ...")
    #
    # So the scrapers collected Vibrio rows, the publish gate accepted them,
    # the reviewers were told they were in scope — and is_in_scope() threw
    # them away at the Pending gate with "pathogen_out_of_scope: 'Vibrio'".
    # Confirmed cost: ZERO Vibrio rows exist across all 1415 rows of
    # Recalls, and none in Pending, Weekly_Rejected or NEWS. The register
    # has never held one. The 2026-08-13 daily run shows the mechanism
    # live, rejecting RASFF 865446 on exactly this string.
    #
    # Unlike "Histamine / scombrotoxin" and "Marine biotoxin", which are
    # kept OUT deliberately and have tests defending their exclusion
    # (test_pathogen_out_of_scope_histamine, ..._marine_biotoxin), nothing
    # anywhere defended Vibrio's absence.
    #
    # Species spellings are listed so a source that names only the species
    # ("V. vulnificus", "parahaemolyticus") is still in scope. Severity is
    # NOT decided here — see ALWAYS_TIER1_KEYWORDS below.
    # "cholera" (not "cholerae") so a source that writes only the disease
    # name is in scope too — the round-trip test caught bare "Cholera"
    # being forced to Tier 1 while is_in_scope() said False, which would
    # have escalated a row the gate was about to throw away.
    "vibrio", "vulnificus", "parahaemolyticus",
    "cholera", "alginolyticus",
    # Viral
    "hepatitis a", "hépatite a", "norovirus",
    # Toxins (mycotoxins)
    "aflatoxin", "aflatoxine",
    "ochratoxin", "ochratoxine",
    "mycotoxin", "mycotoxine",
    "fumonisin", "zearalenone", "deoxynivalenol", "patulin",
    # Undeclared pharmaceutical adulteration (expanded 2026-05-12)
    # — INFOSAN-notifiable adulterants commonly found in spiked
    # "natural" supplements (sexual enhancers, weight-loss products).
    # FDA "undeclared drug ingredient" recall category equivalents.
    "sildenafil", "tadalafil", "vardenafil",
    "sibutramine", "phenolphthalein",
    "undeclared drug", "undeclared pharmaceutical",
    "adulteration", "adulterated",
)


# Sentinel values that mean "no pathogen identified yet" — distinct from
# "pathogen identified but not in our Tier-1 scope". Empty rows are
# candidates for AI enrichment (claude_check / gemini); out-of-scope rows
# are real but ignored by FSIS scope.
_EMPTY_SENTINELS = ("—", "-", "", "unknown", "none", "n/a", "na", "tbd")


def is_empty_pathogen(pathogen: str) -> bool:
    """True if Pathogen field is empty or a placeholder sentinel.

    Distinguishes "we don't know yet, need enrichment" from "we know it's
    out of scope". Used by merge_master.validate_pending_row to route
    empty-pathogen rows to a `pending_enrichment` status instead of
    rejecting them outright at the gate.
    """
    if not pathogen:
        return True
    s = str(pathogen).strip().lower()
    return s in _EMPTY_SENTINELS


def is_in_scope(pathogen: str) -> bool:
    """True if pathogen matches FSIS Tier-1 scope.

    Returns False for both empty and out-of-scope values. Callers that
    need to distinguish those two cases must call is_empty_pathogen()
    first.
    """
    if is_empty_pathogen(pathogen):
        return False
    s = str(pathogen).strip().lower()
    return any(t in s for t in TIER1_KEYWORDS)


def is_tier1(pathogen: str) -> bool:
    """Same as is_in_scope — kept for backward compat with Tier=1 enforcement."""
    return is_in_scope(pathogen)


# ──────────────────────────────────────────────────────────────────────
# ALWAYS-Tier-1 pathogens (hard enforcement, added 2026-07-14)
# ──────────────────────────────────────────────────────────────────────
# Operator rule (emphatic, repeated): these pathogens are ALWAYS Tier 1,
# regardless of what a source's Class field or an AI reviewer assigned.
# Rows were leaking into Recalls at Tier 2/3 (e.g. RappelConso "voluntary"
# Listeria fish/cheese recalls tiered from Class rather than pathogen;
# Salmonella rows at Tier 2). This is distinct from the broader FSIS scope:
# aflatoxin/ochratoxin/adulteration ARE in scope but may legitimately sit
# at Tier 2/3, so they are NOT forced here.
#
# NOTE on Bacillus cereus (updated 2026-07-17): the emetic toxin (cereulide)
# is ALWAYS Tier 1 and is keyed unconditionally below. Bare "Bacillus cereus"
# (no cereulide named) is forced to Tier 1 ONLY when the product is a
# LOW-MOISTURE / dried / starchy matrix (rice, pasta, flour, powder, infant
# formula, spices, dried herbs, cereal, etc.) — the matrices where cereulide
# formation is the real hazard and cannot be ruled out at recall stage. In
# fresh / high-moisture products (e.g. fresh rosemary, fresh tomatoes, fresh
# pastry) bare B. cereus stays tierable (typically Tier 2, diarrheal-type
# risk). This is handled in enforce_tier1() via _is_low_moisture_product(),
# NOT by a blanket keyword, so fresh-product B. cereus is not over-tiered.
ALWAYS_TIER1_KEYWORDS = (
    "listeria",
    "salmonella",
    "e. coli", "e.coli", "escherichia coli", "stec",
    "o157", "o104", "o121", "o26", "o45", "o103", "o111", "o145",
    "shiga toxin", "shigatoxin",
    "botulin", "botulisme", "clostridium botulin",
    "cereulide",                     # emetic B. cereus toxin — always Tier 1
    "cronobacter", "sakazakii",
    "hepatitis a", "hépatite a",
    # Vibrio: SPECIES-LEVEL, not genus-level (added 2026-08-14).
    #
    # Bare "Vibrio" and V. parahaemolyticus deliberately do NOT appear here.
    # They keep the tier the existing framework already gives them — Tier 2,
    # via tier_2_pathogens in scrapers/_models.py, whose comment reads
    # "Campylobacter, Yersinia, Vibrio etc. — always Tier 2 (FDA Class II)".
    # That was already the pipeline's answer and adding the genus to scope
    # does not change it. No new judgement is introduced.
    #
    # Two species are forced to Tier 1 because leaving them at Tier 2 would
    # understate them on published evidence:
    #
    #   vulnificus — CDC, "About Vibrio Infection": "Some Vibrio species,
    #     such as Vibrio vulnificus, can cause severe and life-threatening
    #     infections" and "About 1 in 5 people with this infection die,
    #     sometimes within a day or two of becoming ill."
    #     A ~20% case-fatality organism is not FDA Class II.
    #
    #   cholera / O1 / O139 — FDA Fish and Fishery Products Hazards and
    #     Controls Guidance, Chapter 4, separates "Vibrio cholerae O1 and
    #     O139" (fecal-origin, epidemic cholera) from "Vibrio cholerae
    #     non-O1 and non-O139" (naturally occurring). Only the epidemic
    #     serogroups are forced here.
    #
    # NOTE the deliberate gap: bare "Vibrio cholerae" with NO serogroup
    # stated is NOT forced, because FDA's own guidance splits on serogroup
    # and the register does not invent one the source did not give. Such a
    # row lands at Tier 2 and is visible for review.
    #
    # Only "vulnificus" goes in this tuple. The epidemic cholera serogroups
    # CANNOT live here: is_always_tier1() matches by plain substring
    # (`any(t in s for t in ALWAYS_TIER1_KEYWORDS)`), and "cholera" is a
    # substring of "cholerae", so listing it would silently force EVERY
    # V. cholerae row to Tier 1 — the exact opposite of the serogroup split
    # above. They are handled by _is_epidemic_cholera() instead, which is a
    # regex, same shape as the _is_bare_bacillus_cereus() carve-out.
    "vulnificus",
)


# Epidemic-cholera serogroups. Kept OUT of ALWAYS_TIER1_KEYWORDS on purpose
# — see the note there. Matches "O1" / "O139" as standalone serogroup tokens
# (optionally introduced by "serogroup"/"serotype"/"group"), or the disease
# name "cholera" when it is NOT just the tail of the species word
# "cholerae".
_EPIDEMIC_CHOLERA = _re_cholera = None  # bound below, after `re` is imported


def _is_epidemic_cholera(pathogen: str) -> bool:
    """True for V. cholerae O1 / O139, or an explicit mention of cholera.

    False for bare "Vibrio cholerae" and for non-O1/non-O139, which FDA's
    Fish and Fishery Products Hazards and Controls Guidance (Chapter 4)
    treats as a different, naturally-occurring hazard rather than the
    fecal-origin epidemic one.
    """
    import re
    if is_empty_pathogen(pathogen):
        return False
    s = str(pathogen).strip().lower()
    if "cholera" not in s:          # "cholerae" contains "cholera"
        return False

    # NEGATIVE FIRST. "non-O1" and "non-O139" CONTAIN the tokens "O1" and
    # "O139", so a naive serogroup search matches the exact strings that
    # mean the opposite. Caught by the round-trip test below, which had
    # 'Vibrio cholerae non-O1' coming back True.
    if re.search(r"\bnon[\s\-]?o\s*-?\s*(1|139)\b", s):
        return False

    # Serogroup stated explicitly.
    if re.search(r"(?<!non[\s\-])\bo\s*-?\s*(1|139)\b", s):
        return True
    # "cholera" as the disease name, not the "-e" tail of the species word.
    if re.search(r"\bcholera\b", s):
        return True
    return False

# Low-moisture / dried / starchy matrix vocabulary. Bare "Bacillus cereus"
# in these products is forced to Tier 1 (cereulide-formation risk). Kept in
# sync with the weekly builder's _is_low_moisture heuristic.
_LOW_MOISTURE_KEYWORDS = (
    "peanut", "nut butter", "almond", "cashew", "pistachio", "hazelnut",
    "flour", "cereal", "granola", "oat", "rice", "pasta", "grain",
    "powder", "powdered", "infant formula", "formula", "milk powder",
    "spice", "herb", "seasoning", "dried", "chocolate", "cocoa",
    "tahini", "sesame", "cinnamon", "curry", "paprika", "pepper",
    "couscous", "semolina", "noodle", "crisp", "cracker", "biscuit",
)


def _is_low_moisture_product(row: dict) -> bool:
    """True if the row's product looks like a low-moisture / dried matrix.

    Used to decide whether bare 'Bacillus cereus' is forced to Tier 1.
    Explicit 'fresh' in the product/reason excludes it (fresh herbs, fresh
    produce are high-moisture and stay tierable).
    """
    import re as _re
    text = ((row.get("Product") or "") + " " +
            (row.get("Reason") or "")).lower()
    if not text.strip():
        return False
    if "fresh" in text:
        return False
    return any(_re.search(r"\b" + _re.escape(k), text)
               for k in _LOW_MOISTURE_KEYWORDS)


def _is_bare_bacillus_cereus(pathogen: str) -> bool:
    """True for 'Bacillus cereus' where cereulide/emetic is NOT named."""
    s = str(pathogen or "").strip().lower()
    if "cereus" not in s:
        return False
    return "cereulide" not in s and "emetic" not in s


def is_always_tier1(pathogen: str) -> bool:
    """True if pathogen must ALWAYS be Tier 1 regardless of source Class.

    Narrower than is_in_scope(): only the pathogens the operator has ruled
    are unconditionally Tier 1. Does NOT fire on aflatoxin/ochratoxin/
    adulteration (in-scope but tierable) or on bare "bacillus cereus"
    (only cereulide is forced).
    """
    if is_empty_pathogen(pathogen):
        return False
    s = str(pathogen).strip().lower()
    # Guard: bare "bacillus cereus" without cereulide/emetic is NOT forced.
    return any(t in s for t in ALWAYS_TIER1_KEYWORDS)


def enforce_tier1(row: dict) -> dict:
    """Force Tier=1 in-place when the row's Pathogen is always-Tier-1.

    Idempotent. Returns the same dict for chaining. Stamps a provenance
    note the first time it changes a value so the audit trail records the
    original tier. Safe to call on every promotion and every merge.
    """
    try:
        pathogen = row.get("Pathogen", "")
    except AttributeError:
        return row

    # Determine whether this row must be Tier 1:
    #   (a) an always-Tier-1 pathogen (Listeria/Salmonella/STEC/botulinum/
    #       cereulide/Cronobacter/HepA), OR
    #   (b) bare "Bacillus cereus" in a LOW-MOISTURE product (cereulide-
    #       formation risk — rice, pasta, powder, infant formula, spices,
    #       dried herbs, etc.). Fresh / high-moisture B. cereus is NOT forced.
    # ── Do not escalate on a pathogen the row's own Reason contradicts ──
    # Audit 2026-08-04. An FDA row arrived with Pathogen "Hepatitis A virus"
    # and Reason "Baked products have potential for presence of aluminum
    # slivers from the pans that were used" (FDA permalink: "...due-possible-
    # foreign-object"). This guard read the invented pathogen, forced Tier
    # 2 -> 1, and stamped "[tier-guard: Hepatitis A virus is always Tier 1]"
    # into Notes — so a metal-fragment recall was published as a Tier-1
    # viral event, and the tier-guard's own stamp became the evidence that
    # made it look reviewed.
    #
    # The always-Tier-1 rule is correct. What was wrong is applying it to a
    # Pathogen value the row itself disagrees with: when Pathogen and Reason
    # describe different hazard classes, Pathogen is exactly the field not to
    # trust, and escalating on it amplifies the error instead of catching it.
    #
    # Fails OPEN on any import or classification problem — an escalation
    # skipped is a Tier-2 row that a reviewer still sees; a crash here would
    # stop the promotion entirely.
    try:
        from pipeline._publish_gate import pathogen_reason_class_mismatch
        if pathogen_reason_class_mismatch(str(pathogen or ""),
                                          str(row.get("Reason") or "")):
            note = str(row.get("Notes") or "")
            stamp = ("[tier-guard 2026-08-04: escalation SKIPPED — Pathogen %r "
                     "contradicts this row's own Reason, so the pathogen is "
                     "not trustworthy enough to raise the tier on. The "
                     "publish gate blocks this row separately.]"
                     % str(pathogen).strip()[:60])
            if "tier-guard 2026-08-04" not in note:
                row["Notes"] = (note + " " + stamp).strip() if note else stamp
            return row
    except Exception:
        pass

    force = is_always_tier1(pathogen)
    reason_tag = "is always Tier 1"
    if not force and _is_bare_bacillus_cereus(pathogen) and _is_low_moisture_product(row):
        force = True
        reason_tag = "bare Bacillus cereus in low-moisture product (cereulide risk) is Tier 1"
    if not force and _is_epidemic_cholera(pathogen):
        force = True
        reason_tag = ("epidemic cholera serogroup (V. cholerae O1/O139) is "
                      "Tier 1; non-O1/non-O139 is not")
    if not force:
        return row
    try:
        cur = int(row.get("Tier") or 0)
    except (ValueError, TypeError):
        cur = 0
    if cur == 1:
        return row
    row["Tier"] = 1
    note = str(row.get("Notes") or "")
    stamp = ("[tier-guard: %s %s; forced from Tier %s]"
             % (str(pathogen).strip(), reason_tag, cur if cur else "unset"))
    row["Notes"] = (note + " " + stamp).strip() if note else stamp
    return row


# ──────────────────────────────────────────────────────────────────────
# Pet / animal food scope filter (added 2026-05-23)
# ──────────────────────────────────────────────────────────────────────
# AFTS-FSIS monitors HUMAN food recalls only. Pet food, dog/cat treats,
# animal feed, and livestock feed are out of scope even when contaminated
# with a Tier-1 pathogen (Listeria, Salmonella, etc.).
#
# Historical false-positives this filter would have caught:
#   • Raaw Energy "Dog Food" — FDA, 2026-05-22, Listeria monocytogenes
#   • RCL Foods "dry pet food (dog and cat)" — NCC ZA, 2026-03-09, Salmonella
#   • Elite Treats "Chicken Chips for Dogs ... PET FOOD" — FDA, 2026-02-24,
#     Salmonella
#
# The filter checks Product, Company, Brand, and Reason fields for
# pet/animal-food vocabulary. Multi-language coverage targets FDA / CFIA /
# FSAI / FSA UK (EN), RappelConso / FSAI (FR), RASFF / BVL / AGES (DE),
# AESAN / NCC / others (ES). Vegetative-pathogen-only feed scenarios
# (e.g. dairy-cow feed contaminated with mycotoxin that reaches the milk
# supply) remain in scope only if the recalled PRODUCT is the human food
# downstream — not the feed itself.
import re as _re

_PET_FOOD_RE = _re.compile(
    # English compound nouns
    r"\b(?:"
    r"pet[\s\-]*food|pet[\s\-]*treats?|pet[\s\-]*chew|"
    r"dog[\s\-]*food|dog[\s\-]*treats?|dog[\s\-]*biscuit|dog[\s\-]*chew|"
    r"cat[\s\-]*food|cat[\s\-]*treats?|cat[\s\-]*litter|"
    r"animal[\s\-]*feed|animal[\s\-]*food|"
    r"livestock[\s\-]*feed|poultry[\s\-]*feed|cattle[\s\-]*feed|"
    r"raw[\s\-]*dog|raw[\s\-]*cat|raw[\s\-]*pet|"
    r"kibble|"
    # ── AUDIT 2026-08-14 ──────────────────────────────────────────────
    # The species ADJECTIVES were missing. FDA labels several products
    # "Canine Food" / "Feline Food" rather than "dog food", and this
    # pattern had no word for any of them, so the row
    #     Miller Foods, Inc. | Oma's Pride | "Canine Food" | Salmonella
    # (2026-08-12) walked straight past a filter written specifically to
    # stop it and was published in W33 as a human-food recall. An
    # external review flagged it as a scope question; it was not — the
    # scope was already correct and the regex simply could not read the
    # label. Tested field by field: Company, Brand, Product and Reason
    # all returned None before this line existed.
    r"canine|feline|equine|porcine[\s\-]*feed|"
    r"puppy[\s\-]*food|kitten[\s\-]*food|"
    r"bird[\s\-]*seed|bird[\s\-]*food|ferret[\s\-]*food|"
    r"chicken[\s\-]*feed|horse[\s\-]*feed|swine[\s\-]*feed|"
    # NOTE the hyphen class. RASFF emits U+2011 NON-BREAKING HYPHEN, not
    # ASCII "-": the notification reads "day‑old chicks", and a plain
    # [\s\-] class does not match it. Every hyphen class in this pattern
    # would have the same blind spot on RASFF text; this is the one place
    # it currently matters, and it is called out so the next person adding
    # a term here does not reintroduce it.
    r"day[\s\-‐-―]*old[\s\-‐-―]*chicks?"
    r")\b"
    # "for dogs / cats / pets / puppies / kittens"
    r"|\bfor\s+(?:dogs?|cats?|pets?|puppies|kittens?)\b"
    # German
    r"|\b(?:tierfutter|hundefutter|katzenfutter|haustierfutter|heimtierfutter)\b"
    # French
    r"|\baliment[s]?\s+pour\s+(?:chien|chat|animaux|animal)"
    r"|\bnourriture\s+pour\s+(?:chien|chat|animaux|animal)"
    # Spanish
    r"|\bcomida\s+para\s+(?:perros?|gatos?|mascotas?|animales?)"
    r"|\balimento\s+para\s+(?:perros?|gatos?|mascotas?|animales?)"
    # Italian
    r"|\bcibo\s+per\s+(?:cani|gatti|animali)"
    # Trailing label seen in FDA scraped data
    r"|\bPET\s+FOOD\b",
    _re.IGNORECASE,
)


def is_pet_food_product(*fields: str) -> bool:
    """True if any of the given product-context fields indicates pet,
    veterinary, or animal-feed product. Pass Product, Company, Brand,
    and Reason — any single hit returns True.

    Examples that should match:
      "Dog Food", "dry pet food (dog and cat)", "Chicken Chips for Dogs",
      "Hundefutter mit ...", "aliment pour chien", "PET FOOD"

    Examples that should NOT match:
      "Chicken thighs", "Cathay Pacific catering", "Catfish fillets"
      (because the regex requires word boundaries and specific compounds).
    """
    for f in fields:
        if f and _PET_FOOD_RE.search(str(f)):
            return True
    return False
