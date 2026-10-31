"""A Source label nothing can host-check cannot publish.

WHY THIS EXISTS (2026-10-31) — THE NINTH RECURRENCE
===================================================
Recalls held this row, published 2026-10-28:

    Date      2026-10-28
    Source    National Authority
    Company   Fábrica de Embutidos Zanetti Ltda
    Brand     KF Embutidos
    Product   Linguiça suína defumada
    Pathogen  Listeria monocytogenes
    URL       gov.br/anvisa/pt-br/assuntos/noticias-anvisa/2026/anvisa-
              suspende-venda-de-linguica-suina-defumada-da-marca-kf-embutidos

The recall is real and in scope. "National Authority" is not a regulator:
it is pipeline/gap_finder_tavily.HOST_TO_SOURCE's placeholder for "host
known, agency not", mapped to gov.br — the whole Brazilian government.
pipeline/regulatory_domains.py has mapped that same host to ANVISA since it
was written, so the two tables disagreed on one host and the gap-finder
read the one that said nothing.

THE THREE CONSEQUENCES, THE SAME THREE EVERY TIME
-------------------------------------------------
  * _source_canon.canonical_source() returned kind='unknown', so the label
    reached no id in verify_urls.HOST_FOR_CANONICAL_SOURCE and NOTHING
    CHECKED that the row cites the authority it names. The whole purpose of
    the canonical-source indirection added on 2026-10-26 is that a renamed
    label cannot switch that check off; an UNKNOWN label switches it off.
  * the coverage and independence denominators counted it as its own
    observer, so Brazil looked like a jurisdiction with two feeds.
  * tools/monitored_sources did not count it, and the dashboard's "Sources
    monitored" tile was short by one.

WHY A GATE RULE AND NOT A TENTH ALIAS
=====================================
This defect has been repaired eight times, and the record of those repairs
is the comment history of _source_canon.CANONICAL and
merge_master.SOURCE_ALIASES:

    2026-10-03  Czechia   "SZPI"                found in Recalls
    2026-10-04  Italy     "Salute"              found in Recalls
    2026-10-07  Hungary   "NÉBIH"               found in Recalls
    2026-10-26  six registry labels             found in Recalls
    2026-10-27  NZ        "MPI NZ"              found in Recalls
    2026-10-27  thirty remaining registry labels
    2026-10-31  Brazil    "National Authority"  found in Recalls

EVERY ONE WAS FOUND BY A ROW THAT HAD ALREADY PUBLISHED, and every repair
was one more vocabulary entry for whichever authority bit that morning.
tests/test_every_collector_table_label_is_a_registry_label (2026-10-27)
closed the three collector tables ahead of publication — and
"National Authority" is listed in that test's own NOT_A_REGULATOR set,
described there as "a lead, never a Source on a Recalls row". The sentence
was right. Nothing enforced it.

So the repair this time is a rule in the publish gate rather than a tenth
alias: a label with no host allow-list cannot publish at all. The tenth
invented label becomes a blocked row with a message naming the two tables
to extend, instead of a silent gap in the host check.

WHAT IT MAY NOT COST
====================
Nothing that reaches the writer legitimately. merge_master's generic
bare-name rule (registry_source_label) still turns "FSANZ" into
"FSANZ (AU)" and "BLV" into "BLV (CH)" before the gate sees them — both
were sitting in Pending the morning this was written — and news and
aggregator labels carry their own kind and stay exempt, as do the ids named
in verify_urls.NOT_A_REGULATOR_SOURCE, so an exemption remains a decision
on the record rather than an accident of spelling.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline._publish_gate import publish_blockers            # noqa: E402
from pipeline._source_canon import canonical_source            # noqa: E402
from pipeline import merge_master as mm                        # noqa: E402
from pipeline.verify_urls import (                             # noqa: E402
    allowed_hosts_for_source, NOT_A_REGULATOR_SOURCE)
from tools.monitored_sources import SOURCES                    # noqa: E402

ANVISA_ROW = {
    "Date": "2026-10-28", "Source": "National Authority",
    "Company": "Fábrica de Embutidos Zanetti Ltda", "Brand": "KF Embutidos",
    "Product": "Smoked pork sausage", "Pathogen": "Listeria monocytogenes",
    "Reason": "Listeria monocytogenes", "Class": "Recall",
    "Country": "Brazil", "Region": "Latin America", "Tier": 1,
    "Outbreak": 0,
    "URL": ("https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa/2026/"
            "anvisa-suspende-venda-de-linguica-suina-defumada-da-marca-kf-"
            "embutidos"),
}


# ── the gate ──────────────────────────────────────────────────────────────

def test_the_row_that_found_this_is_refused():
    blockers = " ".join(publish_blockers(ANVISA_ROW))
    assert blockers, (
        "the ANVISA linguiça row published on 2026-10-28 with Source "
        "'National Authority' and passed every gate rule")
    assert "no known regulator" in blockers, blockers


def test_the_same_row_under_the_registry_label_publishes():
    """The rule is about the LABEL, not about the recall."""
    row = dict(ANVISA_ROW, Source="ANVISA (BR)")
    assert not publish_blockers(row), publish_blockers(row)


@pytest.mark.parametrize("label", [
    "National Authority",
    "Authority",
    "Food Authority",
    "Gov",
    "ANVISA BR",            # the "MPI NZ" shape: country token, no parens
    "Brazilian Ministry of Health",
])
def test_an_invented_label_cannot_publish(label):
    blockers = " ".join(publish_blockers(dict(ANVISA_ROW, Source=label)))
    assert "no known regulator" in blockers, (label, blockers)


@pytest.mark.parametrize("label", [
    "FSANZ (AU)", "BLV (CH)", "RappelConso (FR)", "RASFF (EU)",
    "NAFDAC (NG)", "AGES (AT)", "CAA (JP)", "USDA FSIS", "FDA",
    "ANVISA (BR)", "Ministero della Salute (IT)",
])
def test_a_real_regulator_label_is_not_touched(label):
    blockers = " ".join(publish_blockers(dict(ANVISA_ROW, Source=label)))
    assert "no known regulator" not in blockers, (label, blockers)


def test_a_news_label_stays_exempt():
    """News rows never reach Recalls, and this rule is not what stops them."""
    row = dict(ANVISA_ROW, Source="Food Safety News")
    assert "no known regulator" not in " ".join(publish_blockers(row))


def test_an_aggregator_label_stays_exempt():
    row = dict(ANVISA_ROW, Source="CFS (HK) - aggregator (RappelConso FR)")
    assert "no known regulator" not in " ".join(publish_blockers(row))


# ── the vocabulary the rule depends on ────────────────────────────────────

def test_the_two_collector_tables_agree_on_every_shared_host():
    """gov.br said "National Authority" in one table and "ANVISA" in the
    other. One host cannot name two different authorities.

    Compared on the AUTHORITY, not the spelling: the two tables genuinely
    use two label conventions — one writes the registry label "X (CC)" and
    the other the bare short name "X", and merge_master's
    registry_source_label resolves the bare form at write time. So
    "ANVISA (BR)" and "ANVISA" are the same claim and pass; "National
    Authority" and "ANVISA" are not and do not.
    """
    import re

    def short(label: str) -> str:
        return re.sub(r"\s*\([A-Z]{2}\)\s*$", "", label.strip()).upper()

    def table(path):
        src = (ROOT / path).read_text(encoding="utf-8")
        return {m.group(1): m.group(2) for m in re.finditer(
            r'"([a-z0-9.\-]+\.[a-z]{2,})":\s*\(\s*"([^"]*)"\s*,\s*'
            r'"([^"]*)"\s*\)', src)}

    a = table("pipeline/gap_finder_tavily.py")
    b = table("pipeline/regulatory_domains.py")
    assert a and b, "the host->label tables did not parse"
    clash = {h: (a[h], b[h]) for h in set(a) & set(b)
             if short(a[h]) != short(b[h])}
    assert not clash, (
        f"{len(clash)} host(s) name a different authority in each table: "
        f"{clash} — a gap finder reading one table publishes a label the "
        f"other would have got right")


def test_every_registry_label_is_host_checkable():
    """The rule above is only safe if the registry's own labels pass it."""
    dark = []
    for label, _country, _domains in SOURCES:
        canon, kind, _ = canonical_source(label)
        if canon in NOT_A_REGULATOR_SOURCE or kind in ("news", "aggregator"):
            continue
        if allowed_hosts_for_source(label) is None:
            dark.append((label, canon, kind))
    assert not dark, (
        f"{len(dark)} registry label(s) reach no host allow-list, so the "
        f"publish gate would now refuse rows the fleet is meant to "
        f"collect: {dark}")


def test_national_authority_is_no_longer_emitted_by_any_table():
    """The placeholder is gone at source, not only blocked at the gate."""
    for path in ("pipeline/gap_finder_tavily.py",
                 "review/gap_finder_tavily.py",
                 "pipeline/regulatory_domains.py"):
        src = (ROOT / path).read_text(encoding="utf-8")
        for line in src.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue                     # the incident note may name it
            assert '("National Authority"' not in stripped.replace(" ", ""), (
                f"{path} still emits the placeholder label: {line!r}")


def test_no_published_row_is_refused_by_this_rule():
    rows = json.loads((ROOT / "docs" / "data" / "recalls.json")
                      .read_text(encoding="utf-8"))
    bad = [(r.get("Date"), r.get("Source"))
           for r in rows
           if "no known regulator" in " ".join(publish_blockers(r))]
    assert not bad, bad


def test_the_bare_names_in_pending_still_resolve_before_the_gate():
    """Measured 2026-10-31: Pending held Source "FSANZ" and "BLV".

    Those are resolved by merge_master.registry_source_label at promotion,
    which is why the rule costs them nothing. If that generic rule is ever
    narrowed, this is the test that says so.
    """
    assert mm.registry_source_label("FSANZ", "Australia") == "FSANZ (AU)"
    assert mm.registry_source_label("BLV", "Switzerland") == "BLV (CH)"
