# -*- coding: utf-8 -*-
"""A Company made only of agency acronyms names no firm (2026-10-11).

Food Standards Scotland's news item on the Ukrainian-eggs alert reached
Pending with Company = Brand = "FSS and FSA" and the offline promoter
published it, because the publish gate had no rule for it.
"""
from __future__ import annotations

import pytest

from pipeline._publish_gate import publish_blockers

ROW = {
    "Date": "2026-10-10", "Source": "FSA (UK)", "Brand": "—",
    "Product": "Raw shell-on eggs from Ukraine, product code 3UA1012066",
    "Pathogen": "Salmonella Enteritidis",
    "Reason": "Salmonella Enteritidis found in egg samples.",
    "Class": "Alert", "Country": "United Kingdom", "Region": "Europe",
    "Tier": 1, "Outbreak": 1,
    "URL": "https://alerts.food.gov.uk/news-alerts/alert/fsa-fafa-03-2026",
}


@pytest.mark.parametrize("company", ["FSS and FSA", "FSA", "FSS & FSA", "CFIA", "FDA"])
def test_an_agency_acronym_company_is_refused(company):
    assert any("is the regulator" in b for b in publish_blockers(dict(ROW, Company=company)))


@pytest.mark.parametrize("company", ["Yasensvit Ltd", "Greencore", "FSA Foods Ltd"])
def test_a_real_firm_passes_this_rule(company):
    assert not any("is the regulator" in b for b in publish_blockers(dict(ROW, Company=company)))
