"""Hand-adjudicated ground truth — 20 rows drawn at random from the register.

WHAT THIS IS
------------
On 2026-08-28 the operator asked for the schema to be checked the only way a
schema can be checked: take rows at random, work every parameter out by hand
from the row's own text, and compare. Twenty rows, eight parameters, one
hundred and sixty judgements. Sample drawn with seed 20260828 from the 1,532
published rows; no row was chosen, swapped or dropped after the fact.

The first pass scored 61.2%, with seven values asserted that were false. That
review is what produced the fixes of that day, each one traceable to a row
here:

  row  2, 15  "Shiga toxin-producing E. coli" filed as a BIOTOXIN, because
              the hazard table matched "toxin" first — while bare "STEC" fell
              through to bacterial, so one organism sat in two hazard groups.
  row  7,14,18  three cooked products called RAW on the word "beef" or
              "chicken". Bare species had been removed from the consumption
              vocabulary that morning and left standing in the process one.
  row  9      an environmental swab of an egg-laying facility given a storage
              condition, on the word "egg". It is not a food.
  row 17      a ready-to-eat mezze bowl filed as an INGREDIENT because the
              Reason said Listeria was found "on analysis of a semi-finished
              product" — the sampling stage, not the thing sold.
  row  8,14,18  CFIA's "Food recall warning" and USDA's "Public Health Alert"
              both produced NO notice type: the lookup was an exact-match
              dict, and neither of those two sources ever writes anything else.
  row 19      Saint Nectaire came back unknown while storage_condition
              answered "structural:nectaire" for the same row — the cheese had
              been dropped from one list and kept in another.
  15 rows     FoodCategory unknown BY CONSTRUCTION: it was read from RASFF and
              CFIA fields only, so every FDA, RappelConso, FSANZ, BLV and USDA
              row was blank whatever the product name said.

After those fixes the same twenty rows score 93.0%, with three disagreements
left, all of them vocabulary judgement rather than error (see the test at the
bottom).

WHY IT IS A TEST AND NOT A REPORT
---------------------------------
A measurement taken once is a number. A measurement that runs on every commit
is a guarantee. The thresholds below are set at the level reached on the day,
so a change that improves the schema passes and a change that quietly undoes
one of the fixes above fails and names the row.
"""
from __future__ import annotations

import pytest

from pipeline.enrich_schema import derive

PARAMS = ("FoodCategory", "ProcessType", "ConsumptionState", "StorageCondition",
          "PackagingType", "PackagingForm", "PreservationSystem",
          "HazardGroup", "NoticeType")

# Accuracy floors, per parameter, measured 2026-08-28 on this sample.
# "Answerable" excludes rows where unknown is the correct answer.
FLOORS = {
    "FoodCategory": 0.80, "ProcessType": 0.85, "ConsumptionState": 0.95,
    "StorageCondition": 0.80, "PackagingType": 1.00, "PackagingForm": 0.85,
    "PreservationSystem": 0.90,
    "HazardGroup": 1.00, "NoticeType": 1.00,
}

SAMPLE = [
    dict(
        n=1, row=1371, source='RappelConso (FR)',
        product='Falafel and crudités wrap (GTIN 3760373402593, all lots, use-by 2026-04-02 to 2026-04-06) (RappelConso fiche 21869)',
        reason='Listeria contamination',
        notes='',
        pathogen='Listeria monocytogenes', cls='Mandatory',
        gold=dict(FoodCategory='prepared-meals', ProcessType='composite',
                  ConsumptionState='ready-to-eat', StorageCondition='chilled',
                  PackagingType='unknown', PackagingForm='packaged',
                  HazardGroup='pathogen-bacterial', PreservationSystem='chilled-rte', NoticeType='consumer-recall')),
    dict(
        n=2, row=1286, source='RappelConso (FR)',
        product='Goat cheese with 7 flowers (Lot 2604201C, best-before 2026-05-16 to 2026-05-18) (RappelConso fiche 21939)',
        reason='Microbiological risks — STEC',
        notes='',
        pathogen='Shiga toxin-producing E. coli (STEC)', cls='Voluntary',
        gold=dict(FoodCategory='dairy-other', ProcessType='fermented',
                  ConsumptionState='ready-to-eat', StorageCondition='chilled',
                  PackagingType='unknown', PackagingForm='packaged',
                  HazardGroup='pathogen-bacterial', PreservationSystem='fermented-acidified', NoticeType='consumer-recall')),
    dict(
        n=3, row=1196, source='RappelConso (FR)',
        product='Ground beef — prepared on-site at the traditional butcher counter on customer request (production day 2026-03-24)',
        reason='Campylobacter contamination',
        notes='',
        pathogen='Campylobacter', cls='Voluntary',
        gold=dict(FoodCategory='meat-other', ProcessType='raw',
                  ConsumptionState='cook-before-eating', StorageCondition='chilled',
                  PackagingType='loose', PackagingForm='unpackaged',
                  HazardGroup='pathogen-bacterial', PreservationSystem='chilled-raw', NoticeType='consumer-recall')),
    dict(
        n=4, row=1187, source='RASFF (EU)',
        product='Ochratoxin A in dried organic figs',
        reason='Ochratoxin A in dried organic figs; risk: potentially serious; category: fruits and vegetables',
        notes='[RASFF #2026.3245; classification: border rejection notification; category: fruits and vegetables; notifId=837851]',
        pathogen='Ochratoxin', cls='Border Rejection',
        gold=dict(FoodCategory='fresh-produce', ProcessType='dried',
                  ConsumptionState='ready-to-eat', StorageCondition='ambient',
                  PackagingType='unknown', PackagingForm='unknown',
                  HazardGroup='mycotoxin', PreservationSystem='low-moisture-dried', NoticeType='border-rejection')),
    dict(
        n=5, row=1179, source='BLV (CH)',
        product='White Delights-Pistazie',
        reason='Recall over foreign bodies posing injury risk; an additional batch is also affected',
        notes='',
        pathogen='Physical/foreign-body contamination', cls='Recall',
        gold=dict(FoodCategory='confectionery-snacks', ProcessType='unknown',
                  ConsumptionState='ready-to-eat', StorageCondition='ambient',
                  PackagingType='unknown', PackagingForm='unknown',
                  HazardGroup='foreign-material', PreservationSystem='ambient-stable', NoticeType='consumer-recall')),
    dict(
        n=6, row=1162, source='RappelConso (FR)',
        product='Sliced organic white ham — vacuum packs ~200 g (Lot 050326091012, use-by 2026-04-25) (RappelConso fiche 22025)',
        reason='Listeria monocytogenes detected',
        notes='',
        pathogen='Listeria monocytogenes', cls='Voluntary',
        gold=dict(FoodCategory='meat-other', ProcessType='heat-treated',
                  ConsumptionState='ready-to-eat', StorageCondition='chilled',
                  PackagingType='vacuum', PackagingForm='packaged',
                  HazardGroup='pathogen-bacterial', PreservationSystem='chilled-rte', NoticeType='consumer-recall')),
    dict(
        n=7, row=1093, source='RappelConso (FR)',
        product='Beef-snout salad — 500 g modified-atmosphere tray (GTIN 3537949991436, Lot 163, use-by 2026-05-05 and 2026-05-07)',
        reason='Listeria monocytogenes',
        notes='',
        pathogen='Listeria monocytogenes', cls='Voluntary',
        gold=dict(FoodCategory='meat-other', ProcessType='heat-treated',
                  ConsumptionState='ready-to-eat', StorageCondition='chilled',
                  PackagingType='rigid-plastic', PackagingForm='packaged',
                  HazardGroup='pathogen-bacterial', PreservationSystem='chilled-rte', NoticeType='consumer-recall')),
    dict(
        n=8, row=1012, source='CFIA',
        product='Green Zaatar (spice mix)',
        reason='Microbial contamination — Salmonella',
        notes='',
        pathogen='Salmonella', cls='Food recall warning',
        gold=dict(FoodCategory='herbs-spices', ProcessType='dried',
                  ConsumptionState='ingredient', StorageCondition='ambient',
                  PackagingType='unknown', PackagingForm='unknown',
                  HazardGroup='pathogen-bacterial', PreservationSystem='low-moisture-dried', NoticeType='consumer-recall')),
    dict(
        n=9, row=941, source='RASFF (EU)',
        product='Salmonella Enteritidis in environment of egg laying facility from Northern Ireland',
        reason='Salmonella Enteritidis in environment of egg laying facility from Northern Ireland; risk: potentially serious; category: eggs and egg products',
        notes='[RASFF #2026.4182; classification: information notification for follow-up; category: eggs and egg products; notifId=843742]',
        pathogen='Salmonella', cls='Information',
        gold=dict(FoodCategory='eggs-egg-products', ProcessType='raw',
                  ConsumptionState='unknown', StorageCondition='unknown',
                  PackagingType='unknown', PackagingForm='unknown',
                  HazardGroup='pathogen-bacterial', PreservationSystem='unknown', NoticeType='information')),
    dict(
        n=10, row=793, source='RASFF (EU)',
        product='Salmonella Infantis in frozen chicken thigh meat from Ukraine via Sweden',
        reason='Salmonella Infantis in frozen chicken thigh meat from Ukraine via Sweden; risk: potentially serious; category: poultry meat and poultry meat products',
        notes='[RASFF #2026.4716; classification: information notification for follow-up; category: poultry meat and poultry meat products; notifId=847527]',
        pathogen='Salmonella', cls='Information',
        gold=dict(FoodCategory='meat-poultry', ProcessType='raw',
                  ConsumptionState='cook-before-eating', StorageCondition='frozen',
                  PackagingType='unknown', PackagingForm='unknown',
                  HazardGroup='pathogen-bacterial', PreservationSystem='frozen', NoticeType='information')),
    dict(
        n=11, row=765, source='RASFF (EU)',
        product='Salmonella typhimurium in carne fresca di oca/Salmonella typhimurium in fresh goose meat',
        reason='Salmonella typhimurium in carne fresca di oca/Salmonella typhimurium in fresh goose meat; risk: potentially serious; category: poultry meat and poultr',
        notes='[RASFF #2026.4805; classification: alert notification; category: poultry meat and poultry meat products; notifId=848152]',
        pathogen='Salmonella', cls='Alert',
        gold=dict(FoodCategory='meat-poultry', ProcessType='raw',
                  ConsumptionState='cook-before-eating', StorageCondition='chilled',
                  PackagingType='unknown', PackagingForm='unknown',
                  HazardGroup='pathogen-bacterial', PreservationSystem='chilled-raw', NoticeType='consumer-recall')),
    dict(
        n=12, row=752, source='RappelConso (FR)',
        product='ti guémené (RappelConso fiche 22444)',
        reason='Presence of Listeria monocytogenes',
        notes='',
        pathogen='Listeria monocytogenes', cls='Voluntary',
        gold=dict(FoodCategory='unknown', ProcessType='unknown',
                  ConsumptionState='unknown', StorageCondition='unknown',
                  PackagingType='unknown', PackagingForm='unknown',
                  HazardGroup='pathogen-bacterial', PreservationSystem='unknown', NoticeType='consumer-recall')),
    dict(
        n=13, row=677, source='FDA',
        product='TNVitamins Ultra Potent Complete Green Superfood Capsules and Doctor’s Pride Complete Green Superfood capsules',
        reason='Possible Salmonella Contamination',
        notes='',
        pathogen='Salmonella', cls='Recall',
        gold=dict(FoodCategory='supplements', ProcessType='dried',
                  ConsumptionState='ready-to-eat', StorageCondition='ambient',
                  PackagingType='unknown', PackagingForm='packaged',
                  HazardGroup='pathogen-bacterial', PreservationSystem='low-moisture-dried', NoticeType='consumer-recall')),
    dict(
        n=14, row=564, source='USDA FSIS',
        product='Chicken Caesar Wrap — 8.7-oz. clear plastic wrapped packages, "Sell By: 6/24/2026", est. P-45091. Produced 2026-06-16.',
        reason='FSIS public health alert: routine FSIS product testing confirmed Listeria monocytogenes. No recall requested (product no longer available for purchase',
        notes='',
        pathogen='Listeria monocytogenes', cls='Public Health Alert',
        gold=dict(FoodCategory='prepared-meals', ProcessType='heat-treated',
                  ConsumptionState='ready-to-eat', StorageCondition='chilled',
                  PackagingType='flexible', PackagingForm='packaged',
                  HazardGroup='pathogen-bacterial', PreservationSystem='chilled-rte', NoticeType='public-warning')),
    dict(
        n=15, row=558, source='RappelConso (FR)',
        product='Crescenza',
        reason='Positif stec',
        notes='',
        pathogen='Shiga toxin-producing E. coli (STEC)', cls='Voluntary',
        gold=dict(FoodCategory='dairy-other', ProcessType='fermented',
                  ConsumptionState='ready-to-eat', StorageCondition='chilled',
                  PackagingType='unknown', PackagingForm='unknown',
                  HazardGroup='pathogen-bacterial', PreservationSystem='fermented-acidified', NoticeType='consumer-recall')),
    dict(
        n=16, row=455, source='RappelConso (FR)',
        product='TRANCHE DE BOEUF SANS MARQUE',
        reason='Salmonella',
        notes='',
        pathogen='Salmonella', cls='Voluntary',
        gold=dict(FoodCategory='meat-other', ProcessType='raw',
                  ConsumptionState='cook-before-eating', StorageCondition='chilled',
                  PackagingType='unknown', PackagingForm='unknown',
                  HazardGroup='pathogen-bacterial', PreservationSystem='chilled-raw', NoticeType='consumer-recall')),
    dict(
        n=17, row=427, source='RappelConso (FR)',
        product='bowl mezze; bowl mezze falafels',
        reason='Listeria monocytogenes detected on analysis of a semi-finished product',
        notes='',
        pathogen='Listeria monocytogenes', cls='Voluntary',
        gold=dict(FoodCategory='prepared-meals', ProcessType='composite',
                  ConsumptionState='ready-to-eat', StorageCondition='chilled',
                  PackagingType='unknown', PackagingForm='packaged',
                  HazardGroup='pathogen-bacterial', PreservationSystem='chilled-rte', NoticeType='consumer-recall')),
    dict(
        n=18, row=242, source='USDA FSIS',
        product='CURRY CHICKEN SALAD — 8-oz plastic packages, USE BY 07/30/2026, produced 2026-07-22',
        reason='Possible Listeria monocytogenes contamination found by routine establishment testing; distributed in Oregon and Washington. No recall was requested be',
        notes='',
        pathogen='Listeria monocytogenes', cls='Public Health Alert',
        gold=dict(FoodCategory='prepared-meals', ProcessType='heat-treated',
                  ConsumptionState='ready-to-eat', StorageCondition='chilled',
                  PackagingType='rigid-plastic', PackagingForm='packaged',
                  HazardGroup='pathogen-bacterial', PreservationSystem='chilled-rte', NoticeType='public-warning')),
    dict(
        n=19, row=179, source='RappelConso (FR)',
        product='Saint Nectaire fermier AOP, 140 g portions',
        reason='Presence of Listeria monocytogenes at <10 CFU/g in one portion of this lot',
        notes='',
        pathogen='Listeria monocytogenes', cls='Voluntary',
        gold=dict(FoodCategory='dairy-other', ProcessType='fermented',
                  ConsumptionState='ready-to-eat', StorageCondition='chilled',
                  PackagingType='unknown', PackagingForm='packaged',
                  HazardGroup='pathogen-bacterial', PreservationSystem='fermented-acidified', NoticeType='consumer-recall')),
    dict(
        n=20, row=60, source='FDA',
        product='Outshine Strawberry, Watermelon, Grape, Tangerine and Black Cherry 6-count 2.5 oz Fruit Bars; Outshine 24-count 2.5 oz Variety Pack (lots LLA603840–LL',
        reason='Possible foreign material contamination with glass',
        notes='',
        pathogen='Foreign material (glass)', cls='Recall',
        gold=dict(FoodCategory='confectionery-snacks', ProcessType='unknown',
                  ConsumptionState='ready-to-eat', StorageCondition='frozen',
                  PackagingType='unknown', PackagingForm='packaged',
                  HazardGroup='foreign-material', PreservationSystem='unknown', NoticeType='consumer-recall')),
]


def _machine(case):
    # Notes is carried because RASFF puts its own classification there —
    # "[RASFF #2026.4805; classification: alert notification; ...]" — and
    # dropping it turns a tier-1 regulator fact into a tier-3 guess. Only the
    # structured bracket is kept; the audit trail is noise to this module.
    return derive({"Product": case["product"], "Reason": case["reason"],
                   "Pathogen": case["pathogen"], "Class": case["cls"],
                   "Notes": case["notes"], "Source": case["source"]})[0]


@pytest.mark.parametrize("param", PARAMS)
def test_parameter_holds_its_measured_accuracy(param):
    right = answerable = 0
    misses = []
    for case in SAMPLE:
        want, got = case["gold"][param], _machine(case)[param]
        if want == "unknown":
            continue
        answerable += 1
        if want == got:
            right += 1
        else:
            misses.append(f"row {case['n']}: {got!r} != {want!r} | {case['product'][:50]}")
    acc = right / answerable if answerable else 1.0
    assert acc >= FLOORS[param], (
        f"{param} fell to {acc:.1%} (floor {FLOORS[param]:.0%}) "
        f"on {answerable} answerable rows:\n  " + "\n  ".join(misses))


def test_no_parameter_asserts_something_false_more_than_it_used_to():
    """A wrong value is worse than unknown: unknown is honest, wrong is a
    number an analyst will use. Seven values were false before the fixes;
    three remain and all three are named below."""
    wrong = []
    for case in SAMPLE:
        m = _machine(case)
        for p in PARAMS:
            want, got = case["gold"][p], m[p]
            if want != got and got != "unknown" and want != "unknown":
                wrong.append((case["n"], p, got, want))
    assert len(wrong) <= 3, f"regression — false values: {wrong}"


def test_the_three_remaining_disagreements_are_the_known_ones():
    """Named, so that a NEW disagreement cannot hide inside the tolerance.

    All three are vocabulary judgement, not error:
      row  7  "Beef-snout salad" — a charcuterie product sold as a salad.
              prepared-meals or meat-other; RASFF itself keeps prepared
              dishes separate from meat, so the module follows RASFF.
      row 15  Crescenza, and
      row 19  Saint Nectaire fermier — the module says dairy-soft-cheese,
              the hand review said dairy-other. On review the module is the
              more precise of the two: both are soft cheeses, and soft cheese
              is the Listeria vehicle the stratum exists to isolate. The hand
              label is left as written rather than edited to agree, because a
              gold set that moves to match the code is not a gold set.
    """
    got = set()
    for case in SAMPLE:
        m = _machine(case)
        for p in PARAMS:
            if case["gold"][p] != m[p] and m[p] != "unknown" and case["gold"][p] != "unknown":
                got.add((case["n"], p))
    assert got == {(7, "FoodCategory"), (15, "FoodCategory"), (19, "FoodCategory")}, got


def test_overall_accuracy_floor():
    right = answerable = 0
    for case in SAMPLE:
        m = _machine(case)
        for p in PARAMS:
            if case["gold"][p] == "unknown":
                continue
            answerable += 1
            right += case["gold"][p] == m[p]
    acc = right / answerable
    assert acc >= 0.90, f"overall {acc:.1%} on {answerable} judgements (was 61.2% before the 2026-08-28 fixes, 93.0% after)"
