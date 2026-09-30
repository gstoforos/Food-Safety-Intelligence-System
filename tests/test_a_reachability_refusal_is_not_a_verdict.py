"""Three faults the 2026-09-30 morning fix pass found, pinned.

FAULT 1 — `404` MATCHES INSIDE A LOT NUMBER
--------------------------------------------
``pipeline/_url_guard._NOT_FOUND_REASON`` ended with the bare alternatives
``|404|403``. A bare digit string matches ANYWHERE, and this register is made
of longer digit strings: RASFF notification ids, GTINs, lot codes, alert
numbers. Twelve strings in the 2026-09-30 workbook were misread — "lot 24045",
"GTIN 3324040001436", "notifId=874035", "RASFF #2026.4042" and eight more.

One of those twelve is a live reject REASON, and it shows what the fault costs:

    pet_food_out_of_scope - AFTS-FSIS is a HUMAN-food register. Product reads
    'Chicken Chips for Dogs (6 oz, lot 24045) - PET FOOD'.

That is a CONTENT verdict and it is correct. The guard read the ``404`` inside
``24045``, called it a claim about reachability, and refused the rejection.
Refusals are counted into Notes, and since 2026-09-25 a SECOND refusal
escalates to a promotion — so a guard built to stop a false negative became a
route to a false positive, on nothing but a lot number.

The fix is digit boundaries, NOT word boundaries. The reviewers' own
vocabulary writes these codes as ``http_404``, ``(http_404)``, ``404s``,
``HTTP 403``, ``returns 404``, and every one of those must keep matching.

FAULT 2 — A REACHABILITY REFUSAL WAS A PERMANENT BAR
-----------------------------------------------------
``_url_guard.reject_refusal`` has stopped reviewer 1 (2026-09-21) and
reviewer 2 (2026-09-26) from DISCARDING a row on "I could not fetch the page"
when the row already carries a URL on the regulator's own domain. It was never
wired into merge_master's re-promotion guard, so every such rejection ALREADY
in the archive stayed a permanent bar — the recall could not come back no
matter what was later proved about it.

Measured 2026-09-30: 19 archive rows carry a not-found refusal while holding
an authority URL. Two of them were FSAI rows this pass enriched from FSAI's
own pages, and one had been sitting barred for twelve days:

  * Kilbride Classic Cuisine Roast Chicken & Gravy Dinner — Listeria
    monocytogenes, Tier 1, FSAI alert 2026.58, 18 Sep 2026, archived as
    "URL agent: No official regulator page found for this recall".
    ``recall_review_agent.py`` already names this row in its own comment as a
    known wrongful rejection.
  * Dunnes Stores Potato Waffles — MOAH, FSAI alert 2026.59, 18 Sep 2026,
    archived as "URL agent: No official recall page found".

The exception added is as narrow as the 2026-09-27 repairable-defect one, and
rests on the same two-part proof: the archived reason must be a REFUSAL (not a
content verdict), and the row must pass the FULL publish gate now.
FAULT 3 — A HAZARD SOLD TO SUBSCRIBERS THAT THE CURATOR COULD NOT SEE
----------------------------------------------------------------------
``_publish_gate.HAZARD_CLASS_KEYWORDS["chemical"]`` had no mineral-oil
vocabulary, so FSAI alert 2026.59 — Dunnes Stores Potato Waffles, "elevated
levels of mineral oil aromatic hydrocarbons (MOAH)" — classified as NOTHING
once published. This is the fourth instance of the pattern the 2026-09-09
audit named for PFAS: the FSA UK and CFIA scrapers collect mineral oil,
``scrapers/_pathogen_vocab.py`` lists it, ``gap_finder_claude``'s scope prompt
names it, and ``tools/alert_vocab.py`` and ``AftsAlerts.gs`` OFFER "moah",
"mosh" and "mineral oil" to subscribers under "Industrial chemical
contaminant". Only the curator's class map could not see it.

The ``mosh`` token is space-bounded and the rest are not, deliberately: a bare
substring hits the Mushmoshi enoki brand (a real Listeria recall) and the
imoshion powerbank rows.
"""
from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# FAULT 1 — the digit boundary
# ---------------------------------------------------------------------------

#: Strings the reviewers really produce for an HTTP failure. Every one of
#: these MUST still be read as a reachability refusal.
REAL_HTTP_REFUSALS = (
    "http_404",
    "(http_404)",
    "the cited page does not exist (http_404)",
    "404s",
    "the URL 404s",
    "HTTP 403 from the host",
    "returns 404",
    "403 Forbidden",
)

#: Numbers from this register's own data. NONE of these is an HTTP code, and
#: every one of them appeared in a real Notes or reason string on 2026-09-30.
NUMBERS_THAT_ARE_NOT_HTTP_CODES = (
    "Product reads 'Chicken Chips for Dogs (6 oz, lot 24045) - PET FOOD'",
    "notifId=874035",
    "notifId=863403",
    "notifId=844033",
    "notifId=840389",
    "notifId=839403",
    "notifId=838404",
    "GTIN 3324040001436",
    "RASFF #2026.4042",
    "RASFF #2026.3404",
    "batch 4041",
)


@pytest.mark.parametrize("text", REAL_HTTP_REFUSALS)
def test_a_real_http_refusal_is_still_recognised(text):
    from pipeline._url_guard import _NOT_FOUND_REASON
    assert _NOT_FOUND_REASON.search(text), (
        f"{text!r} is how a reviewer says it could not fetch the page. "
        f"Tightening the 404/403 match must not stop recognising it — that "
        f"would hand reviewer 1 back the power to discard a row on a fetch "
        f"failure, which is the fault _url_guard exists to prevent.")


@pytest.mark.parametrize("text", NUMBERS_THAT_ARE_NOT_HTTP_CODES)
def test_a_lot_number_is_not_an_http_code(text):
    from pipeline._url_guard import _NOT_FOUND_REASON
    m = _NOT_FOUND_REASON.search(text)
    assert not m, (
        f"_NOT_FOUND_REASON matched {m.group(0)!r} inside {text!r}. That is a "
        f"lot code, a GTIN or a RASFF notification id, not an HTTP status. "
        f"Reading it as one makes the guard refuse a CONTENT rejection that "
        f"was correct, and two such refusals escalate to a promotion.")


def test_the_pet_food_rejection_is_a_content_verdict_not_a_refusal():
    """The row that proved the fault. Its reason is a scope judgement."""
    from pipeline._url_guard import reject_refusal
    row = {"URL": "https://www.fda.gov/safety/recalls-market-withdrawals-"
                  "safety-alerts/example-pet-treat-recall"}
    reason = ("pet_food_out_of_scope - AFTS-FSIS is a HUMAN-food register. "
              "Product reads 'Chicken Chips for Dogs (6 oz, lot 24045) - "
              "PET FOOD'. Published 2026-02-24, before the pet-food gate "
              "existed (added 2026-05-23), and never re-screened against it.")
    assert reject_refusal(row, reason) == "", (
        "reject_refusal must pass a content verdict straight through, even on "
        "an authority URL. It only ever blocks the 'I could not find the "
        "page' class.")


def test_a_genuine_refusal_on_an_authority_url_is_still_blocked():
    """The other direction: the guard must not have been defanged."""
    from pipeline._url_guard import reject_refusal
    row = {"URL": "https://www.fsai.ie/news-and-alerts/food-alerts/"
                  "recall-of-branded-roast-chicken-and-gravy-produced"}
    why = reject_refusal(row, "URL agent: No official regulator page found "
                              "for this recall")
    assert why, ("a not-found reason on fsai.ie must still be refused — the "
                 "URL is on the regulator's own domain.")
    assert "reachability" in why.lower()


# ---------------------------------------------------------------------------
# FAULT 2 — a refusal is not a permanent bar
# ---------------------------------------------------------------------------

def test_merge_master_asks_the_url_guard_before_barring_a_re_promotion():
    """Code-side pin: the exception must be wired in, and fail CLOSED."""
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    assert "from pipeline._url_guard import reject_refusal" in src, (
        "merge_master's re-promotion guard no longer consults _url_guard. "
        "Without it, every reachability refusal already in the archive is a "
        "permanent bar — 19 rows on the 2026-09-30 workbook, one of them a "
        "Tier 1 Listeria recall barred for twelve days.")
    # The proof must be two-part: a refusal AND a passing gate.
    assert "_rr(clean, _prior_reason) and not publish_blockers(clean)" in src, (
        "the exception must require BOTH that the archived reason was a "
        "refusal and that the row passes the full publish gate now. Either "
        "half alone asserts the re-entry instead of demonstrating it.")


def test_a_content_rejection_is_still_barred_for_ever():
    """The exception must not unbar a scope, duplicate or non-food verdict."""
    from pipeline._url_guard import reject_refusal
    row = {"URL": "https://www.fsai.ie/news-and-alerts/food-alerts/"
                  "recall-of-a-batch-of-dunnes-stores-potato-waffles"}
    for verdict in ("undeclared allergen (milk)",
                    "labelling error",
                    "foreign body (plastic)",
                    "non-food product",
                    "pet/animal food",
                    "duplicate of an existing register row",
                    "pathogen_out_of_scope"):
        assert reject_refusal(row, verdict) == "", (
            f"{verdict!r} is a judgement about the RECALL. The re-promotion "
            f"exception keys on reject_refusal returning non-empty, so a "
            f"content verdict leaking through here would unbar a row that "
            f"was correctly archived.")


def test_no_archive_row_is_barred_by_a_refusal_while_passing_the_gate():
    """Data-side: the backlog this fix was written to clear stays cleared."""
    pd = pytest.importorskip("pandas")
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():                                   # pragma: no cover
        pytest.skip("no workbook")
    from pipeline._url_guard import reject_refusal
    from pipeline._publish_gate import publish_blockers

    x = pd.ExcelFile(xlsx)
    published = set(pd.read_excel(x, "Recalls")["URL"]
                    .astype(str).str.strip().str.lower()) - {"", "nan"}
    stranded = []
    for sheet, col in (("Weekly_Rejected", "RejectionReason"),
                       ("Rejected", "RejectReason")):
        if sheet not in x.sheet_names:
            continue
        df = pd.read_excel(x, sheet)
        if col not in df.columns:
            continue
        for _, r in df.iterrows():
            row = r.to_dict()
            url = str(row.get("URL") or "").strip().lower()
            if not url or url in published:
                continue            # already back in the register
            if not reject_refusal(row, str(row.get(col) or "")):
                continue            # a content verdict — correctly barred
            if publish_blockers(row):
                continue            # still has a real defect — correctly held
            stranded.append((sheet, str(row.get("Date"))[:10],
                             str(row.get("Pathogen"))[:28],
                             str(row.get("Product"))[:44]))
    assert not stranded, (
        f"{len(stranded)} archived row(s) were thrown away on a REACHABILITY "
        f"refusal, carry a URL on the regulator's own domain, pass every "
        f"publish-gate rule, and are still not in the register:\n  " +
        "\n  ".join(map(str, stranded[:8])) +
        "\n\nA datacentre IP failing to fetch a page is a statement about us, "
        "not about whether the notice exists. Run "
        "`python -m pipeline.promote_gate_passing --apply`.")


# ---------------------------------------------------------------------------
# FAULT 3 — a hazard offered to subscribers that the curator could not see
# ---------------------------------------------------------------------------

MINERAL_OIL_IS_CHEMICAL = (
    "Mineral oil aromatic hydrocarbons (MOAH)",
    "elevated levels of mineral oil aromatic hydrocarbons (MOAH)",
    "MOSH/MOAH exceedance",
    "Mineralöl in Sonnenblumenöl",
    "huile minérale dans l'huile de tournesol",
    "minerale olie in zonnebloemolie",
    "aceite mineral",
    "olio minerale",
)

#: Brand names in this register that CONTAIN the letters "mosh". A bare
#: substring would class every one of them as a chemical hazard.
MOSH_LOOKALIKES = (
    "Mushmoshi",
    "Mushmoshi brand enoki mushroom — Listeria monocytogenes",
    "Rückruf: Brandgefahr bei imoshion MagSafe Powerbanks",
)


@pytest.mark.parametrize("text", MINERAL_OIL_IS_CHEMICAL)
def test_mineral_oil_classifies_as_a_chemical_hazard(text):
    from pipeline._publish_gate import classify_hazard
    assert "chemical" in classify_hazard(text), (
        f"{text!r} classified as nothing. alert_vocab.py and AftsAlerts.gs "
        f"both OFFER 'moah'/'mosh'/'mineral oil' to subscribers under "
        f"'Industrial chemical contaminant', and the FSA UK and CFIA scrapers "
        f"collect it — a hazard this register sells and cannot classify is "
        f"the PFAS gap of 2026-09-09 all over again.")


@pytest.mark.parametrize("text", MOSH_LOOKALIKES)
def test_a_brand_containing_mosh_is_not_a_chemical_hazard(text):
    from pipeline._publish_gate import classify_hazard
    assert "chemical" not in classify_hazard(text), (
        f"{text!r} was classed as a chemical hazard. 'mosh' must stay "
        f"space-bounded — Mushmoshi is a real Listeria recall and imoshion "
        f"is a powerbank.")
