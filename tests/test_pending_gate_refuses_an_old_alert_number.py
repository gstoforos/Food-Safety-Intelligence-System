# -*- coding: utf-8 -*-
"""An alert numbered 027/2022 may not even reach Pending.

THE LOOP THIS CLOSES (found 2026-10-24)
---------------------------------------
pipeline/_publish_gate._shape_blockers_2026_10_17 has refused a row whose
URL carries an alert number from another year since 2026-10-17. Nothing
else did. So the Nigeria gap finder kept re-queueing one May-2022 NAFDAC
alert — "public-alert-no-027-2022-adulterated-lots-of-japata-alcoholic-
bitters-…" — into Pending with that morning's date, over and over:

    Rejected          2026-09-24   same URL, archived
    Rejected          2026-10-13   same URL, archived
    Weekly_Rejected   2026-10-23   same URL, archived
    Pending           2026-10-23   there again on the morning of 10-24

Each copy sits in Pending until a human clears it, and while it sits,
tests/test_alert_number_year_matches_date[Pending] is red — which is how
this was found. The publish gate was doing its job; the row should never
have got that far.

WHY NOT url_year.is_year_mismatch, which validate_pending_row ALREADY
calls: url_year() reads a year out of the URL path and returns None for a
regulator slug of this shape. The two rules look for different things and
both are needed.

IMPORTED, NOT COPIED. The regex lives in _publish_gate, and
validate_pending_row imports the predicate — the same convention the
import-eligibility vocabulary follows in pipeline/gap_finder/rules.py
("IMPORTED from the publish gate rather than copied, so the two gates
cannot drift"). test_the_two_gates_share_one_rule below pins that.
"""
from __future__ import annotations

import pytest

from pipeline._publish_gate import alert_number_year_mismatch
from pipeline.merge_master import validate_pending_row

NAFDAC_2022 = (
    "https://nafdac.gov.ng/public-alert-no-027-2022-adulterated-lots-of-"
    "japata-alcoholic-bitters-100-ml-all-variants-produced-by-chigodson-"
    "international-ltd"
)

ROW = {
    "Date": "2026-10-23",
    "Source": "NAFDAC (NG)",
    "Company": "Chigodson International Ltd",
    "Brand": "Japata",
    "Product": "Alcoholic Bitters",
    "Pathogen": "Methanol",
    "Reason": "Adulterated lots",
    "Country": "Nigeria",
    "URL": NAFDAC_2022,
}


def test_the_row_that_found_this_is_refused_at_the_pending_gate():
    ok, why = validate_pending_row(dict(ROW), set())
    assert not ok, "the 2022 NAFDAC alert still enters Pending"
    assert "alert_number_year_mismatch" in why, why
    assert "2022" in why and "2026" in why, why


@pytest.mark.parametrize("url,year", [
    ("https://nafdac.gov.ng/public-alert-no-023-2025-recall-of-multivitamin",
     "2025"),
    ("https://nafdac.gov.ng/public-alert-no-041-2024-alert-on-the-recall",
     "2024"),
    ("https://nafdac.gov.ng/public-alert-no-0012-2019-resumption-of-prod",
     "2019"),
])
def test_every_year_the_archives_hold_is_refused(url, year):
    """The archives hold refused NAFDAC alerts numbered 2018-2025, every one
    stamped with its 2026 discovery date. The gate must refuse the shape,
    not one serial."""
    ok, why = validate_pending_row(dict(ROW, URL=url), set())
    assert not ok and year in why, (ok, why)


def test_an_alert_numbered_this_year_still_enters():
    """The rule is about a CONTRADICTION, not about NAFDAC."""
    ok, why = validate_pending_row(
        dict(ROW,
             URL="https://nafdac.gov.ng/public-alert-no-041-2026-recall-of-"
                 "adulterated-groundnut-oil"),
        set())
    assert ok, why


@pytest.mark.parametrize("url", [
    # A fiche id that merely looks like a year must not be read as one —
    # the same false friend tests/test_url_year_scope.py locks for
    # url_year().
    "https://rappel.conso.gouv.fr/fiche-rappel/22019/Interne",
    "https://rappel.conso.gouv.fr/fiche-rappel/23922/interne",
    # A bare year in a path is normal and harmless.
    "https://www.nvwa.nl/actueel/nieuws/2026/10/22/veiligheidswaarschuwing",
    # A notification id with four digits.
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/881247",
])
def test_an_ordinary_url_is_untouched(url):
    assert alert_number_year_mismatch(dict(ROW, URL=url)) is None, url


def test_an_empty_date_is_a_different_gate():
    """Rows arrive from the gap finders with no Date at all; that defect is
    refused elsewhere, and this rule must not claim it."""
    assert alert_number_year_mismatch(dict(ROW, Date="")) is None


def test_the_two_gates_share_one_rule():
    import inspect

    from pipeline import merge_master

    src = inspect.getsource(merge_master.validate_pending_row)
    assert "alert_number_year_mismatch" in src, (
        "validate_pending_row no longer consults the publish gate's "
        "alert-number rule")
    assert "_ALERT_NO_YEAR" not in src, (
        "the regex was copied into merge_master instead of imported — the "
        "two gates will drift")

    from pipeline._publish_gate import _shape_blockers_2026_10_17
    blockers = _shape_blockers_2026_10_17(dict(ROW))
    assert any("alert number" in b for b in blockers), (
        "the publish gate stopped reporting the shape it has refused since "
        "2026-10-17")
