# -*- coding: utf-8 -*-
"""The full-name half of the regulator check, at the deterministic gate.

WHY THIS TEST EXISTS (2026-10-19)
=================================
On 2026-10-11 rule 2b closed the ACRONYM half of this gap and recorded the
reason in `_publish_gate.py`: "the reviewer's regulator-name check knows full
names, not acronyms, and the offline promoter asks only this gate." The
full-name half stayed open. At 03:27 UTC on 2026-10-19 the offline promoter
published this row into the register, Tier-1, inside week 42:

    Date      2026-10-15
    Source    Livsmedelsverket (SE)
    Company   '121'
    Brand     '121'
    Product   'Edsviken - Livsmedelsverket'
    Pathogen  'Escherichia coli (generic)'
    URL       .../sok---muselkontroll-oppna-och-stangda-produktionsomraden/
              edsviken
    Notes     '[promote 2026-10-19: ... passes every publish-gate rule]'

The Notes were accurate, which is what made it a gate defect rather than a
data defect. The URL is Sweden's shellfish production-AREA classification
page: the area is open with a restriction on oyster harvesting because of
high E. coli in the water. No firm, no brand, no product, no lot, no recall.
Company and Brand held the number 121 and Product held the page <title>.

`recall_review_agent._AGENCY_NAMES` would have refused the Product; the
offline promoter never runs the review agent. These three rules put the same
judgement where the promoter can reach it, with no model and no network.
"""
from __future__ import annotations

import pytest

from pipeline._publish_gate import publish_blockers

#: A row that is otherwise entirely publishable, so each assertion below
#: isolates exactly one rule.
GOOD = {
    "Date": "2026-10-15", "Source": "Livsmedelsverket (SE)",
    "Company": "Nordic Shellfish AB", "Brand": "—",
    "Product": "Chilled live oysters, 12-piece tray, lot 44821",
    "Pathogen": "Shiga toxin-producing E. coli (STEC)",
    "Reason": "Shiga toxin-producing E. coli found in sampled oysters.",
    "Class": "Recall", "Country": "Sweden", "Region": "Europe",
    "Tier": 1, "Outbreak": 0,
    "URL": "https://www.livsmedelsverket.se/om-oss/press/nyheter/"
           "pressmeddelanden/aterkallelse-ostron-2026-10-15",
    "Notes": "Read on the authority page 2026-10-19.",
}


def test_the_row_the_rules_were_written_for_is_refused():
    row = dict(
        GOOD,
        Company="121", Brand="121",
        Product="Edsviken - Livsmedelsverket",
        Pathogen="Escherichia coli (generic)", Reason="E. coli",
        URL="https://www.livsmedelsverket.se/foretagande-regler-kontroll/"
            "sa-kontrolleras-ditt-foretag/"
            "sok---muselkontroll-oppna-och-stangda-produktionsomraden/edsviken",
    )
    blockers = publish_blockers(row)
    assert any("contains no letter" in b for b in blockers), blockers
    assert any("authority's own name" in b for b in blockers), blockers


# --- (a) a name field must contain a letter ---------------------------------

@pytest.mark.parametrize("value", ["121", "0", "42", "2026", "—121", "1 2 3"])
@pytest.mark.parametrize("field", ["Company", "Brand"])
def test_a_number_is_not_a_firm_name(field, value):
    blockers = publish_blockers(dict(GOOD, **{field: value}))
    assert any("contains no letter" in b for b in blockers), (field, value, blockers)


@pytest.mark.parametrize("value", [
    "121 Bakery Ltd", "3M Company", "E.Leclerc", "G.D", "Wälchli",
    "CRUSTA C", "Hof Dannwisch", "Laiterie de Coaticook Limitée",
    # Any script counts as a letter. The operator rule of 2026-10-01 is that
    # a non-Latin firm name gets TRANSLATED by the morning pass, never
    # refused by the gate, so this rule must not be the thing that drops it.
    # An earlier draft spelled out script ranges by hand; str.isalpha() is
    # Unicode-aware and these are its regression cases.
    "株式会社明治", "삼성", "Ελλάς", "Мясо", "شركة",
])
def test_a_real_firm_name_passes_the_letter_rule(value):
    blockers = publish_blockers(dict(GOOD, Company=value))
    assert not any("contains no letter" in b for b in blockers), (value, blockers)


@pytest.mark.parametrize("value", ["—", "-", "", "Unbranded", "Not specified"])
def test_the_registers_own_no_value_sentinels_are_not_flagged(value):
    """Brand '—' means 'no brand'. Rule 1's empty-Company check owns the rest."""
    blockers = publish_blockers(dict(GOOD, Brand=value))
    assert not any("contains no letter" in b for b in blockers), (value, blockers)


# --- (b) the publisher of the URL is not the firm ---------------------------

def test_the_host_of_the_rows_own_url_is_not_the_firm():
    """Recalls row 1026, BVL 2026-07-07: Company = Brand = the portal."""
    row = dict(
        GOOD,
        Source="BVL (DE)", Company="lebensmittelwarnung.de",
        Brand="lebensmittelwarnung.de", Country="Germany",
        Product="Herb salad cheese (Salatkäse)",
        URL="https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/"
            "Meldungen/2026/07_Juli/260707_11_SH_Salatkaese/"
            "260707_11_SH_Salatkaese.html",
    )
    blockers = publish_blockers(row)
    assert any("published this notice" in b for b in blockers), blockers
    # and the repaired row passes
    assert not any("published this notice" in b for b in
                   publish_blockers(dict(row, Company="Hof Dannwisch", Brand="—")))


@pytest.mark.parametrize("company,url", [
    # A firm name with a dot and no space is ordinary. The first draft of
    # this rule matched on shape and flagged all five of these good rows.
    ("E.Leclerc", "https://rappel.conso.gouv.fr/fiche-rappel/23774/interne"),
    ("F.Duvacher", "https://rappel.conso.gouv.fr/fiche-rappel/19902/interne"),
    ("G.D", "https://rappel.conso.gouv.fr/fiche-rappel/20511/interne"),
    ("Pawell.Co", "https://rappel.conso.gouv.fr/fiche-rappel/23801/interne"),
    ("SUPPLX.com", "https://www.fda.gov/safety/recalls-market-withdrawals-"
                   "safety-alerts/supplx-recalls-one-lot"),
])
def test_a_dotted_firm_name_is_not_its_own_publisher(company, url):
    blockers = publish_blockers(dict(GOOD, Company=company, Brand="—", URL=url))
    assert not any("published this notice" in b for b in blockers), (company, blockers)


# --- (c) the authority's own name is not the product ------------------------

@pytest.mark.parametrize("product", [
    "Edsviken - Livsmedelsverket",
    "Notification - Europese Commissie",
    "| Food Safety Authority of Ireland",
    "Fresh 'N' Frozen Favour-ita Grated Coconut 400g "
    "| Food Standards Australia New Zealand",
    "Ansökan om godkännande för märkning av vin - Livsmedelsverket",
    "Høringer - Mattilsynet",
    "Produktwarnungen Kontakte & FAQ",
])
def test_the_authoritys_own_name_is_never_the_product(product):
    blockers = publish_blockers(dict(GOOD, Product=product))
    assert any("authority's own name" in b for b in blockers), (product, blockers)


@pytest.mark.parametrize("product", [
    "Chilled live oysters, 12-piece tray, lot 44821",
    "Fresh 'N' Frozen Favour-ita Grated Coconut 400g",
    "Infant formula and follow-on formula (specific batches)",
    "Beef meat dried in the Italian Alps - 12 slices - 80g - Carrefour Extra",
    "Lamb's lettuce bunch 125g - Notre Jardin",
    "Herb salad cheese (Salatkäse)",
])
def test_a_real_product_name_passes_the_authority_rule(product):
    blockers = publish_blockers(dict(GOOD, Product=product))
    assert not any("authority's own name" in b for b in blockers), (product, blockers)


def test_the_rasff_company_convention_survives_rule_c():
    """All 689 RASFF rows carry 'Origin: x | Notifying: y' in Company.

    'Notifying: European Commission' is a correct value on RASFF #2026.8885
    (Recalls row 134), not a defect. Rule (c) reads Product only for exactly
    this reason — checking Company would refuse the whole RASFF series, and
    the RASFF format is not ours to change.
    """
    row = dict(
        GOOD, Source="RASFF (EU)",
        Company="Origin: United Kingdom (Northern Ireland) "
                "| Notifying: European Commission",
        Brand="—", Product="Escherichia coli in mussels",
        Pathogen="Escherichia coli (generic)",
        Reason="Escherichia coli in mussels; risk: not serious; "
               "category: bivalve molluscs and products thereof",
        Class="Information", Country="United Kingdom (Northern Ireland)",
        URL="https://webgate.ec.europa.eu/rasff-window/screen/"
            "notification/878334",
    )
    assert publish_blockers(row) == []
