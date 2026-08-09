"""Deterministic publish gate — zero tokens, zero network.

WHY THIS EXISTS (audit 2026-08-01)
==================================
A subscriber alert went out on 2026-07-31 carrying this row:

    Date      2026-07-27
    Source    NCC
    Company   BM Foods (a member of Sea Harvest Group)
    Product   Deli Hummus range
    Pathogen  Listeria Monocytogenes
    Reason    Recall ID 842632
    Class     <empty>
    Region    Not specified in the article
    Notes     Discovered via news: timeslive.co.za
    URL       https://thencc.org.za/media-statement-deli-hummus-range-...

The recall is real. It is also from **16 September 2024** — the NCC media
statement is dated then, the best-before dates run 10 Sep to 08 Oct 2024, and
the timeslive article the row itself cites as its source is dated 2024-10-04.
It was published as a 2026-07-27 recall and emailed to subscribers, roughly
22 months stale. `_MIN_VALID_DATE` did not catch it because the fabricated
date is inside 2026.

That row carried SIX defects. Not one of them needed a language model to
spot:

    Reason is only a reference number ......... 'Recall ID 842632'
    Class empty ............................... ''
    Region not in the controlled vocabulary ... 'Not specified in the article'
    Source not the canonical label ............ 'NCC'  (established: 'NCC (ZA)')
    Brand holds the retailer, not the brand ... 'Shoprite Checkers'
    No review trail at all .................... 'Discovered via news: ...'

The same sweep found more of the same shape already published:

    8 rows with NO Pathogen in a pathogen-recall database, including
      'mg3, mg3 hybrid+ voiture de tourisme'  — a passenger CAR, recalled for
          seat movement in a collision (RappelConso fiche 49461)
      'jouet de baignoire à pulvérisation'    — a bath toy, choking hazard
      'petrole lampant'                        — lamp oil
      'mento sport bottle'                     — a drinks bottle
    1 row whose URL is the AESAN HOMEPAGE, with Company, Reason, Pathogen and
      Class all empty
    7 rows whose entire Reason is 'Recall ID <n>' — 5 of them the same number,
      leaked from the extractor prompt's own worked example

The lesson is not "the LLM reviewer failed". The lesson is that these rows
should never have reached an LLM reviewer at all. A schema gate rejects every
one of them for free, and keeps working when the Gemini quota is exhausted,
when the API is down, and when rappel.conso.gouv.fr refuses a TLS handshake.

Use `publish_blockers(row)` — it returns a list of human-readable reasons, one
per violated rule, or an empty list when the row is publishable.


SECOND INCIDENT (audit 2026-08-02) — WHY THE HAZARD CROSS-CHECK MOVED HERE
=========================================================================
On 2026-08-01 a subscriber alert went out carrying this row:

    Date      2026-07-29
    Source    FSANZ (AU)
    Company   UPDATED 30.07.26 | Auxico (Perth) Pty Ltd
    Product   LGM HOT CHILLI OIL 275G
    Pathogen  Listeria monocytogenes          <- appears NOWHERE on the page
    Reason    The recall is due to the presence of an undeclared allergen
              (peanuts).
    Tier      1
    URL       .../recall-alert/updated-300726-auxico-perth-pty-ltd-...

Verified against the live FSANZ notice: the hazard is an undeclared peanut
allergen, the word "Listeria" does not occur on the page at all, and the
correct row for the very same recall was ALREADY in Recalls, correct, at
Tier 2 with Pathogen "Undeclared allergen (peanuts)".

Three separate mechanisms had to fail together:

  1. FSANZ republishes an amended alert at a NEW slug, prefixing both the
     URL ("updated-300726-<slug>") and the <h1> ("UPDATED 30.07.26 | ...").
     The URL-keyed dedup saw a new address, so it minted a second row; the
     "Company - Product" title split put the page's status banner into
     Company.
  2. claude_check DID catch the contradiction and archived the row to
     Weekly_Rejected for "pathogen mismatch" — but the cross-check lived
     inside claude_check's clean-row shortcut, so it only ever guarded
     claude_check's own path. A later, weaker reviewer (the self-hosted
     Qwen review agent) re-approved the same row and promoted it.
  3. Nothing consulted Weekly_Rejected before promoting, so a row the
     binding reviewer had already killed got a fresh verdict every time a
     re-uploaded workbook snapshot put it back into Pending.

The cross-check therefore belongs HERE, in the deterministic gate every
promotion path funnels through, and as a hard blocker rather than a hint to
go ask a model. Pathogen "Listeria monocytogenes" against Reason "undeclared
allergen (peanuts)" is a contradiction visible from the row alone, for free,
with every API key revoked.

`claude_check` now imports the tables below instead of keeping its own copy,
so the classifier cannot drift between the two callers.
"""
from __future__ import annotations
import re
from typing import Any, Dict, List, Set

# Controlled vocabularies. A value outside these is an extraction artifact,
# not a regional judgement call.
VALID_REGIONS = frozenset({
    "Europe", "North America", "Latin America", "Asia", "Africa",
    "Oceania", "Middle East", "Unknown",
})

# Free-prose tells: an LLM answering the question instead of filling the field.
_PROSE_MARKERS = (
    "not specified", "not stated", "not mentioned", "not provided",
    "not available", "unknown origin", "no information", "n/a in the",
    "in the article", "not found in", "could not determine", "unclear",
)

# A Reason that is nothing but a reference number.
_ID_ONLY_REASON = re.compile(
    r"^\s*(?:recall\s*id|id|allerta|pratica|ref(?:erence)?)\s*[:#]?\s*[0-9]{3,}\s*$",
    re.IGNORECASE,
)

# Regulator landing pages — never a specific recall notice.
#
# DELIBERATELY NARROW. The first draft of this rule tested "path ends with /"
# and "path contains /product-recalls", which flagged 20+ perfectly good
# notices whose slug happens to end in a slash
# (thencc.org.za/product-safety-recall-nutricia-aptamil-nutribiotik-2-.../).
# A gate that cries wolf on good rows gets switched off. A URL is only a
# landing page when its path, once the trailing slash is removed, is EMPTY or
# is exactly one of these listing paths — never on a substring match.
_LANDING_PATHS = frozenset({
    "",
    "/home",
    "/index.htm", "/index.html", "/index.php",
    "/aecosan/web/home/aecosan_inicio.htm",
    "/recalls", "/product-recalls", "/category/product-recalls",
    "/food-alerts", "/news-and-alerts", "/avisos", "/warnungen", "/rappels",
    "/index.php/el/enimerosi/deltia-typou",
    "/en/food-alerts", "/fr/rappels", "/nl/terugroepingen",
})

# Audit 2026-08-02: '0' added. RappelConso fiche 23067 reached Recalls with
# Company and Brand both holding the literal string "0" — an extractor writing
# a falsy sentinel into a text field, not a company called zero. Every field
# _blank() guards (Pathogen, Reason, Company, Product, Class, Date, Source,
# URL) is free text where "0" can only ever be an artifact; the numeric
# columns Tier and Outbreak are checked elsewhere and never pass through here.
#
# Audit 2026-08-09: ditto markers added. RappelConso fiche 23108 (La Fumerie
# du Coin, Listeria) is published in Recalls with Product = "idem".
#
# Worth being precise about whose defect that is: it is NOT a scraper
# artifact. The DGCCRF open-data record for 23108 carries
# modeles_ou_references = "idem" verbatim — the notifier typed a ditto mark
# into the product-name field. The pipeline captured it faithfully. But a
# ditto referring to a field the reader cannot see names no product, so it is
# a placeholder in exactly the sense this set means, and a hazard row whose
# Product names no product should not publish.
_PLACEHOLDER_VALUES = frozenset({
    "", "none", "null", "n/a", "na", "-", "—", "tbd", "unknown", "nan", "0",
    "idem", "ditto", "id.", "same as above", "voir ci-dessus", "cf. ci-dessus",
    "s.o.", "idem que ci-dessus",
})

# Regulator page-status banners that some scrapers fold into Company because
# the agency prepends them to the <h1>. FSANZ: "UPDATED 30.07.26 | Auxico
# (Perth) Pty Ltd - LGM HOT CHILLI OIL 275G".
_TITLE_STATUS_PREFIX = re.compile(
    r"^\s*(?:updated?|update|revised|corrected|amended|extended)\b[^|]{0,40}\|\s*",
    re.IGNORECASE,
)


def _blank(value: Any) -> bool:
    return str(value or "").strip().lower() in _PLACEHOLDER_VALUES


# ---------------------------------------------------------------------------
# Hazard-class classifier (canonical home — audit 2026-08-02)
# ---------------------------------------------------------------------------
# Moved here from claude_check.py so every promotion path shares one table.
# See the module docstring for the incident that forced the move.
HAZARD_CLASS_KEYWORDS = {
    "biological": (
        "listeria", "salmonella", "shiga toxin", "shigatoxi", "stec", "vtec",
        "e. coli", "ecoli", "escherichia", "botulinum", "botulism",
        "campylobacter", "shigella", "bacillus cereus", "cereulide",
        "staphylococcus", "staphyloc", "enterotoxin",
        "norovirus", "norwalk", "hepatitis a",
        # NOT a bare "hav" (audit 2026-08-04). It was intended as the
        # abbreviation for Hepatitis A virus and instead matched the word
        # "have": "Baked products HAVe potential for presence of aluminum
        # slivers" classified as BIOLOGICAL, which let a fabricated
        # "Hepatitis A virus" agree with a metal-fragment reason and pass
        # the contradiction rule. Spaced so it only matches the token.
        " hav ", "(hav)", "hav virus",
        "yersinia", "vibrio", "clostridium perfringens",
        "cronobacter", "enterobacter", "enterohaem",
    ),
    "physical": (
        "foreign matter", "foreign material", "foreign body",
        "physical contamination", "physical hazard",
        "pieces of glass", "pieces of metal", "pieces of plastic",
        "metal fragment", "plastic fragment", "glass fragment",
        "rubber fragment", "wood fragment", "stone fragment",
        "shard", "splinter",
        # ── Audit 2026-08-04 ──────────────────────────────────────────
        # An FDA row reached Recalls with Pathogen "Hepatitis A virus" and
        # Reason "Baked products have potential for presence of aluminum
        # slivers from the pans that were used". The FDA permalink itself
        # ends "...due-possible-foreign-object", and the word Hepatitis
        # appears nowhere on the notice — a fabricated viral pathogen on a
        # metal-fragment recall. The contradiction rule stayed silent
        # because NONE of the vocabulary above matches "aluminum slivers",
        # so the Reason was unclassifiable and the rule failed safe.
        #
        # Worse, the always-Tier-1 guard then read the invented pathogen
        # and escalated the row from Tier 2 to Tier 1, stamping
        # "[tier-guard: Hepatitis A virus is always Tier 1]" into Notes.
        # A foreign-object recall was published as a Tier-1 viral event.
        #
        # NOTE the shapes below are all QUALIFIED. A bare "sliver" must
        # never go in this list: "slivered almonds" is an ingredient, and
        # classifying every almond recall as a physical hazard would
        # manufacture contradictions on correct rows.
        "foreign object", "foreign objects",
        "metal sliver", "aluminum sliver", "aluminium sliver",
        "slivers of", "sliver of",
        "aluminum fragment", "aluminium fragment",
        "metal contamination", "metal pieces", "pieces of wire",
        "wire fragment", "hard plastic", "sharp object",
        "corps etranger", "corps étranger",   # RappelConso
        "fremdkoerper", "fremdkörper",        # BVL / BLV
        "corpo estraneo",                     # Ministero della Salute
        "cuerpo extrano", "cuerpo extraño",   # AESAN
    ),
    "chemical": (
        "chemical contaminant", "chemical residue", "pesticide", "fungicide",
        "herbicide", "rodenticide", "antibiotic", "veterinary",
        "nitrofurazone", "chloramphenicol", "sulphonamide", "sulfonamide",
        "semicarbazide", " sem ", " sem)", " sem,",
        "histamine",
        "heavy metal", " mercury ", " cadmium ", " arsenic ",
        # NOT a bare " lead " (audit 2026-08-02). Once the French motifs were
        # translated, "…can lead to poor preservation and microbial growth"
        # classified as CHEMICAL and manufactured a contradiction against a
        # correct Listeria row (RappelConso fiche 22408, whose official
        # risques_encourus names Listeria monocytogenes explicitly). The metal
        # sense needs its own context.
        "lead contamination", "lead content", "lead level", "excess lead",
        "plomb",
        "dioxin", "pcb", "acrylamide", "perchlorate", "melamine",
        "ethylene oxide", "chlorate",
    ),
    "mycotoxin": (
        "aflatoxin", "ochratoxin", "patulin", "fumonisin",
        "deoxynivalenol", " don ", "zearalenone", "mycotoxin", "alternaria",
    ),
    "fermentation": (
        "unintended fermentation", "yeast contamination", "wild yeast",
        "spoilage", "alcohol formation", "co2 formation", "fermenting",
    ),
    "biotoxin": (
        "saxitoxin", "tetrodotoxin", "marine biotoxin", "ciguatoxin",
        "domoic acid", "okadaic acid", "azaspiracid", "palytoxin",
        "paralytic shellfish", "amnesic shellfish", "diarrhetic shellfish",
        "psp toxin", "asp toxin", "dsp toxin",
    ),
    # DELIBERATELY FRAMING-TOKEN ONLY. Bare food names ("milk", "nut",
    # "fish") must NOT appear here: RASFF Reason text routinely carries
    # "category: milk and milk products" on genuine Listeria and STEC
    # notifications, and a bare "milk" token would classify those as
    # allergen and manufacture a false mismatch on correct rows. Every
    # real allergen recall states the framing explicitly.
    "allergen": (
        "undeclared allergen", "undeclared allergens",
        "undeclared ingredient", "undeclared ingredients",
        "undeclared milk", "undeclared egg", "undeclared peanut",
        "undeclared soy", "undeclared gluten", "undeclared wheat",
        "undeclared sesame", "undeclared mustard", "undeclared sulphite",
        "undeclared sulfite", "undeclared nut", "undeclared fish",
        "undeclared shellfish", "undeclared celery", "undeclared lupin",
        "allergen not declared", "allergen labelling", "allergen labeling",
        "not declared on the label", "missing allergen",
        "incorrect allergen", "allergen mislabel",
        "misbranding", "misbranded", "mislabelled", "mislabeled",
        "mislabelling", "mislabeling", "incorrect label", "wrong label",
        "label error", "labelling error", "labeling error",
        # non-English regulators
        "allergene non declare", "allergène non déclaré",
        "allergene non dichiarato", "alergeno no declarado",
        "alérgeno no declarado", "nicht deklariertes allergen",
        "niet-gedeclareerd allergeen", "niet gedeclareerd allergeen",
        "allergeen niet vermeld",
    ),
    # Only explicit mould vocabulary — a bare "microbial contamination" must
    # stay unclassifiable so the guard keeps failing safe on vague text.
    "spoilage": (
        "mould", "moulds", "mould contamination", "mold contamination",
        "moisissure", "muffa", "moho", "schimmel",
        "visible mould", "visible mold", "mouldy", "moldy",
    ),
}


def classify_hazard(text: str) -> Set[str]:
    """Return the set of hazard classes whose keywords appear in `text`."""
    if not text:
        return set()
    s = " " + text.lower() + " "
    classes = set()
    for cls, kws in HAZARD_CLASS_KEYWORDS.items():
        for kw in kws:
            if kw in s:
                classes.add(cls)
                break
    return classes


# Bare allergen names, matched against the WHOLE Pathogen field only.
# These can never join HAZARD_CLASS_KEYWORDS: as substrings they would match
# RASFF's "category: milk and milk products" on genuine Listeria rows and
# manufacture a false hazard class. As a whole-field equality test on Pathogen
# they are unambiguous — nothing else writes "Peanut" into a pathogen column.
_BARE_ALLERGEN_PATHOGENS = frozenset({
    "peanut", "peanuts", "tree nut", "tree nuts", "nut", "nuts",
    "milk", "cow's milk", "cows milk", "dairy", "lactose",
    "egg", "eggs", "gluten", "wheat", "barley", "rye", "oats",
    "soy", "soya", "soybean", "sesame", "sesame seed",
    "mustard", "celery", "lupin", "molluscs", "crustaceans",
    "shellfish", "fish", "sulfite", "sulfites", "sulphite", "sulphites",
    # PENDING AUDIT 2026-08-07. The AFTS scope excludes "allergen-only,
    # labeling, quality issues" — but this set only knew ALLERGENS, so two
    # Pending rows passed every deterministic check with zero blockers:
    #   Capri-Sun Orange   Pathogen "Undeclared sugar"  — a labelling defect
    #   Yopokki cups       Pathogen "Spoilage"          — a quality defect
    # Neither is a hazard this database covers. Both had already been
    # rejected once by claude-check and had found their way back to Pending.
    "undeclared sugar", "sugar", "spoilage", "mould", "mold",
    "off-odour", "off odour", "off-odor", "off odor",
    "quality", "quality defect", "labelling", "labeling",
    "mislabelling", "mislabeling", "undeclared ingredient",
})


def _is_bare_allergen(pathogen: str) -> bool:
    """True when the whole Pathogen field is just an allergen's name.

    Tolerates a trailing qualifier in brackets — 'Sulfites (undeclared)' and
    'Peanut (undeclared)' are the shapes seen in production.
    """
    p = str(pathogen or "").strip().lower()
    p = re.sub(r"\s*\((?:un)?declared\)\s*$", "", p)
    p = re.sub(r"^(?:un)?declared\s+", "", p).strip(" .")
    return p in _BARE_ALLERGEN_PATHOGENS


def pathogen_reason_class_mismatch(pathogen: str, reason: str) -> bool:
    """True if Pathogen and Reason describe DIFFERENT hazard classes.

    Conservative by construction: returns False whenever EITHER field is
    unclassifiable, and False whenever the classes overlap at all. It only
    fires when both fields classify cleanly and share nothing — e.g.
    biological vs allergen, biological vs physical.
    """
    p_cls = classify_hazard(pathogen)
    r_cls = classify_hazard(reason)
    if not p_cls or not r_cls:
        return False
    return len(p_cls & r_cls) == 0


def publish_blockers(row: Dict[str, Any]) -> List[str]:
    """Return every reason this row must not be published. Empty == publishable.

    Deterministic and offline by construction: no network, no model, no quota.
    Each rule below corresponds to a defect actually found in published data.
    """
    problems: List[str] = []

    # 1. Pathogen — this is a pathogen/hazard recall database. A row without
    #    one is either out of scope or a failed extraction. This alone rejects
    #    the car, the bath toy, the lamp oil and the sports bottle.
    if _blank(row.get("Pathogen")):
        problems.append(
            "Pathogen is empty — a hazard database row must name its hazard "
            "(this rule rejects non-food consumer-product recalls that arrive "
            "through RappelConso's other categories)")

    # 2. Reason must describe the hazard, never be a bare identifier.
    reason = str(row.get("Reason") or "").strip()
    if _blank(reason):
        problems.append("Reason is empty")
    elif _ID_ONLY_REASON.match(reason):
        problems.append(
            f"Reason is only a reference number ({reason!r}) — the hazard is "
            "not described. 'Recall ID 842632' was the extractor prompt's own "
            "worked example and leaked onto unrelated recalls")

    # 3. Controlled vocabularies, and free prose leaking into them.
    region = str(row.get("Region") or "").strip()
    if region and region not in VALID_REGIONS:
        problems.append(
            f"Region {region!r} is not one of {sorted(VALID_REGIONS)}")
    # Prose leakage is only a blocker in Region, which is a CONTROLLED
    # vocabulary. In Company an honest "(not specified in FAVV notice)" is
    # better than an invented name — that is a disclosure, not a defect, and
    # blocking it would push the enricher back toward fabricating.
    region_lc = region.lower()
    if region_lc and any(marker in region_lc for marker in _PROSE_MARKERS):
        problems.append(
            f"Region contains free prose rather than a value "
            f"({region[:60]!r})")

    # 4. Required identity fields.
    for field in ("Company", "Product", "Class", "Date", "Source", "URL"):
        if _blank(row.get(field)):
            problems.append(f"{field} is empty")

    # 5. Date shape. Value-range checks stay with merge_master's
    #    _MIN_VALID_DATE; this only catches malformed dates.
    date = str(row.get("Date") or "").strip()
    if date and not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        problems.append(f"Date {date!r} is not YYYY-MM-DD")

    # 6. URL must point at a specific notice, not a regulator landing page.
    url = str(row.get("URL") or "").strip()
    if url:
        if not url.startswith(("http://", "https://")):
            problems.append(f"URL {url[:60]!r} is not absolute")
        else:
            tail = url.split("?", 1)[0].split("#", 1)[0]
            after_host = tail.split("://", 1)[-1]
            path = ("/" + after_host.split("/", 1)[1]) if "/" in after_host else ""
            path = path.rstrip("/").lower()
            if path in _LANDING_PATHS:
                problems.append(
                    f"URL is a regulator landing page, not a recall notice "
                    f"({url[:80]!r})")

            # 6b. THE URL MUST BE ON THE REGULATOR'S OWN HOST.
            #
            # Audit 2026-08-09. FSIS recall 015-2026 (City Foods, Inc. /
            # Bea's Best Corned Beef, Listeria) was promoted into Recalls
            # with Source "USDA FSIS" and
            #     URL  https://www.usatoday.com/recalls/meat-and-poultry/...
            # It passed every reviewer, including the final one, and passed
            # this gate, because nothing here looked at the HOST. Three
            # sibling rows for the same recall were sitting in Pending at the
            # same moment carrying the correct fsis.usda.gov address — the
            # pipeline minted four rows for one recall and published the one
            # citing a newspaper.
            #
            # The check itself was not missing. pipeline/verify_urls.py has
            # had HOST_FOR_SOURCE since 2026-08-02 and names this row in one
            # line. But it is a standalone CLI meant for CI, so nothing on
            # the promotion path ever consulted it. A check that exists but
            # is not wired into the gate is a check that does not run.
            #
            # Imported, not copied — today's other finding was a fourth
            # private copy of a hazard table drifting out of sync. The import
            # is lazy because verify_urls imports publish_blockers back.
            #
            # Sources absent from the map are not checked: better silent than
            # wrong, same rule as in verify_urls.
            try:
                from pipeline.verify_urls import HOST_FOR_SOURCE, _host
                _allowed = HOST_FOR_SOURCE.get(str(row.get("Source") or "").strip())
                if _allowed:
                    _h = _host(url)
                    if not any(_h == a or _h.endswith("." + a) or a in _h
                               for a in _allowed):
                        problems.append(
                            f"URL host {_h!r} does not belong to Source "
                            f"{str(row.get('Source'))!r} (expected one of "
                            f"{list(_allowed)}) — the row cites a regulator "
                            f"that did not publish it. AFTS publishes "
                            f"official regulator sources only")
            except ImportError:                        # pragma: no cover
                pass

    # 7. Pathogen and Reason must not describe different hazard classes.
    #    See the 2026-08-02 incident in the module docstring: an invented
    #    "Listeria monocytogenes" sat on a row whose own Reason said
    #    "undeclared allergen (peanuts)", and it reached subscribers because
    #    the only copy of this check lived inside one reviewer's fast path.
    pathogen = str(row.get("Pathogen") or "").strip()
    if pathogen and reason and pathogen_reason_class_mismatch(pathogen, reason):
        problems.append(
            f"Pathogen {pathogen[:40]!r} contradicts Reason "
            f"({sorted(classify_hazard(pathogen))} vs "
            f"{sorted(classify_hazard(reason))}) — one of the two fields is "
            f"not what the source page says")

    # 8. AFTS SCOPE — allergen-only recalls do not belong in this database.
    #
    #    Policy 2026-07-29, and the footer printed on every daily brief:
    #      "Pathogens + biotoxins + mycotoxins + foreign material + pest +
    #       chemical hazards only. Allergen-only, labeling, quality issues
    #       excluded per AFTS scope."
    #
    #    That policy shipped as a ONE-OFF script (pipeline/fix_allergen_rows.py)
    #    that removed two FSANZ rows by exact URL and was never wired into any
    #    gate. Both rows came back. By 2026-08-02 there were SEVEN allergen-only
    #    rows in Recalls, and one of them had been re-promoted carrying an
    #    invented "Listeria monocytogenes" at Tier 1 and mailed to subscribers.
    #
    #    A recall that carries a real hazard AND an allergen issue stays IN
    #    scope — the rule only fires when allergen/labelling is the ONLY class
    #    the row resolves to, across both Pathogen and Reason.
    #
    #    Two of the seven did not resolve to the allergen class at all,
    #    because their Pathogen field is a BARE allergen name with no framing
    #    token: "Peanut" (FSANZ garlic powder, verified — Problem reads "The
    #    presence of an undeclared allergen (Peanut)") and "Sulfites
    #    (undeclared)" (BLV). Bare food names must never go into
    #    HAZARD_CLASS_KEYWORDS — RASFF Reason text carries "category: milk and
    #    milk products" on genuine Listeria notifications — so they are matched
    #    here against the WHOLE Pathogen field only, never as a substring and
    #    never against Reason.
    if pathogen or reason:
        _classes = classify_hazard(pathogen) | classify_hazard(reason)
        # Was `if not _classes and _is_bare_allergen(pathogen)`. The Yopokki
        # row (audit 2026-08-07) got past it: Pathogen "Spoilage" classifies
        # as {"fermentation"}, so _classes was non-empty and this branch never
        # ran — a bare quality term counted as a hazard class of its own.
        # The condition that matters is not "did the PATHOGEN classify" but
        # "does the REASON name a hazard". When the whole Pathogen field is a
        # bare allergen/quality/labelling term and the Reason names no hazard
        # at all, there is no hazard on this row. A genuine fermentation-toxin
        # row — "Cereulide (B. cereus toxin)" — is not a whole-field match and
        # is untouched.
        if _is_bare_allergen(pathogen) and not classify_hazard(reason):
            _classes = {"allergen"}
        if _classes and _classes <= {"allergen"}:
            problems.append(
                "Out of AFTS scope: allergen/labelling is the only hazard "
                "class this row resolves to (policy 2026-07-29 — allergen-"
                "only, labelling and quality recalls are excluded; pathogens, "
                "biotoxins, mycotoxins, foreign material, pest and chemical "
                "hazards only)")

    # 9. Company must not carry the page's status banner. FSANZ prepends
    #    "UPDATED DD.MM.YY | " to the <h1> of an amended alert, and a
    #    "Company - Product" title split folds it straight into Company.
    company = str(row.get("Company") or "").strip()
    if company and _TITLE_STATUS_PREFIX.match(company):
        problems.append(
            f"Company starts with a page status banner ({company[:50]!r}) — "
            f"the regulator's <h1> prefix was parsed as part of the name")

    return problems


def strip_title_status_prefix(value: Any) -> Any:
    """Remove a leading regulator status banner from a Company string.

    Identity for anything that does not carry one, so it is safe to run over
    every row on every write.
    """
    if not isinstance(value, str):
        return value
    cleaned = _TITLE_STATUS_PREFIX.sub("", value).strip()
    return cleaned if cleaned else value


def is_publishable(row: Dict[str, Any]) -> bool:
    """Convenience wrapper — True when the row violates no rule."""
    return not publish_blockers(row)
