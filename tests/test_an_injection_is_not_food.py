# -*- coding: utf-8 -*-
"""A toxin injected at a medical spa is not a food recall (operator 2026-10-10).

CDC MMWR 75(39) — three women in Colorado with symptoms after a
non-FDA-approved botulinum toxin product was injected at a medical spa —
published to Recalls as a Tier-1 Clostridium botulinum outbreak. The hazard
word matched; nothing asked whether the exposure was food.
"""
from __future__ import annotations

from pipeline._publish_gate import publish_blockers

CDC_ROW = {
    "Date": "2026-10-08", "Source": "CDC",
    "Company": ("Notes from the Field: Investigation of Unapproved Botulinum Toxin "
                "Product Administered at a Medical Spa"),
    "Brand": "Notes from the Field", "Product": "Colorado, 2025–2026",
    "Pathogen": "Clostridium botulinum", "Reason": "Clostridium botulinum — outbreak",
    "Class": "Recall", "Country": "United States", "Region": "North America",
    "Tier": 1, "Outbreak": 1,
    "URL": "https://www.cdc.gov/mmwr/volumes/75/wr/mm7539a2.htm",
}


def test_the_medical_spa_report_is_refused():
    assert any("cosmetic exposure" in b for b in publish_blockers(CDC_ROW))


def test_a_real_food_botulism_recall_still_passes():
    food = dict(CDC_ROW, Source="BLV (CH)", Company="Le Grand'Joie (Eddy Gaspoz)",
                Brand="Le Grand'Joie",
                Product="Pork shank terrine (Terrine de jarret de porc), 360 g glass jar",
                Reason="Suspected botulism; two cases of foodborne botulism in Valais.",
                Country="Switzerland", Region="Europe",
                URL="https://www.recallswiss.admin.ch/customer-access/#Recalls/1148")
    assert not any("cosmetic exposure" in b for b in publish_blockers(food))
