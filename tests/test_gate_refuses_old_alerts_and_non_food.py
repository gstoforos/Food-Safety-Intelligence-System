"""Publish-gate rule 2d (2026-10-17): an alert number from another year than
the Date, a non-food consumer product, and a site heading as Company are
refused — and ordinary food recalls are not."""
from __future__ import annotations

import pytest

from pipeline._publish_gate import publish_blockers

BASE = {"Pathogen": "Listeria monocytogenes", "Reason": "Listeria monocytogenes detected",
        "Company": "Some Food Ltd", "Product": "Smoked salmon 200 g"}

REFUSED = [
    dict(BASE, Date="2026-10-14", Company="Nivea", Product="Roll-On Deodorant",
         Pathogen="Safety Concerns", Reason="Recalled due to safety concerns",
         URL="https://nafdac.gov.ng/public-alert-no-041-2024-alert-on-the-recall-of-nivea-roll-on-deodorant/"),
    dict(BASE, Date="2026-10-13",
         URL="https://nafdac.gov.ng/public-alert-no-027-2022-alert-on-adulterated-alcoholic-bitters/"),
    dict(BASE, Date="2026-10-16", Company="Product Warnings & Product", Product="AGES",
         URL="https://www.ages.at/en/human/product-warnings-product-recalls/produkt/x"),
]
ACCEPTED = [
    dict(BASE, Date="2026-10-13",
         URL="https://nafdac.gov.ng/public-alert-no-031-2026-recall-of-contaminated-noodles/"),
    dict(BASE, Date="2026-10-14",
         URL="https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-supervalu-smoked-salmon"),
    dict(BASE, Date="2026-10-16", Company="Not named on the notice (origin Sicily)",
         Product="Cocktail tomatoes", URL="https://www.ages.at/mensch/produktwarnungen-produktrueckrufe/produkt/x"),
]


@pytest.mark.parametrize("row", REFUSED, ids=["nivea-2024", "nafdac-2022", "section-heading"])
def test_refused(row):
    assert any("alert number" in p or "non-food" in p or "section heading" in p
               for p in publish_blockers(row)), publish_blockers(row)


@pytest.mark.parametrize("row", ACCEPTED, ids=["nafdac-2026", "fsai", "ages-de"])
def test_not_refused_by_rule_2d(row):
    assert not any("alert number" in p or "non-food" in p or "section heading" in p
                   for p in publish_blockers(row)), publish_blockers(row)
