# -*- coding: utf-8 -*-
"""A document type is not a firm (operator pass 2026-10-10).

CFS (HK) press release 20261009_12660 reached Pending from the daily
search with Company = Brand = "Press Release" — the page's section label —
and the publish gate passed it. Held here by shape.
"""
from __future__ import annotations

import pytest

from pipeline._publish_gate import publish_blockers

ROW = {
    "Date": "2026-10-09", "Source": "CFS (HK)", "Brand": "Marks & Spencer",
    "Product": "Marks & Spencer Collection Pistachio Pesto 115 g",
    "Pathogen": "Salmonella", "Reason": "Possibly contaminated with Salmonella.",
    "Class": "Recall", "Country": "Hong Kong", "Region": "Asia", "Tier": 1,
    "Outbreak": 0, "URL": "https://www.cfs.gov.hk/english/press/20261009_12660.html",
}


@pytest.mark.parametrize("label", ["Press Release", "press release", " News Release ",
                                   "Media release"])
def test_a_press_release_label_in_company_blocks_publication(label):
    assert publish_blockers(dict(ROW, Company=label))


def test_the_real_importer_passes():
    assert not publish_blockers(dict(ROW, Company="ALF Retail Hong Kong Limited (importer)"))
