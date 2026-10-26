# -*- coding: utf-8 -*-
"""Every regulator with published rows must be host-checked.

WHAT THIS CAUGHT (morning-fix 2026-10-26)
=========================================
``verify_urls.HOST_FOR_SOURCE`` maps a Source label to the host that
regulator serves its notices from, and ``_publish_gate`` rule 6b imports it
to stop a row citing a regulator that did not publish it. The map's own
comment says "Sources absent from this map are not checked — better silent
than wrong", which is the right default.

What that default does not survive is a label that MOVES. The check does
not get weaker; it switches OFF, for every row of that regulator, silently.
The map already carried a note about this happening once to Hong Kong and
being fixed by hand. Measured on the live register this morning it had
happened again, twice, and had never been true for most of the register:

    44 distinct Source labels in Recalls
    16 matched a key                 2,049 rows host-checked
    28 matched nothing                 126 rows NEVER host-checked

    key 'FAVV (BE)'                matched no row; live label 'AFSCA (BE)'
    key 'Ministero della Salute'    matched no row; live label
                                   'Ministero della Salute (IT)'

Belgium (11 rows) and Italy (22 rows) were deliberately covered by whoever
wrote those two keys, and neither was being checked. Among the 126, eight
PUBLISHED rows cite a news site as the authority that issued the recall —
news.google.com, en.sedaily.com, ilfattoalimentare.it, unotv.com,
chequeado.com, vietnam.vnanet.vn, capitalfm.co.ke — which is precisely the
failure the map was built for after the 2026-08-09 USA Today / FSIS row.

WHY THIS TEST SHAPE
-------------------
Not "the map must contain label X": that is the assertion that rots the
moment a scraper is renamed, which is how this got here. This asserts the
PROPERTY — every Source label that reaches Recalls resolves to a host
allow-list, or is a named non-regulator — so the next rename fails this
test instead of disabling a gate.

``pipeline/_source_canon.py`` is the module whose whole purpose is "one
regulator, one label". Resolving through it means a label and its renames
share one host allow-list, and a label it does not know is also caught
here, because an unknown identity has no entry.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from pipeline._source_canon import canonical_source
from pipeline.verify_urls import (
    HOST_FOR_CANONICAL_SOURCE,
    HOST_FOR_SOURCE,
    NOT_A_REGULATOR_SOURCE,
    allowed_hosts_for_source,
)

ROOT = Path(__file__).resolve().parents[1]
RECALLS_JSON = ROOT / "docs" / "data" / "recalls.json"


def _published_source_labels() -> Counter:
    rows = json.loads(RECALLS_JSON.read_text("utf-8"))
    return Counter(str(r.get("Source") or "").strip() for r in rows)


def test_every_published_source_label_is_host_checked():
    """The property. A label with rows and no allow-list is a dark gate."""
    dark = {}
    for label, n in _published_source_labels().items():
        if not label or allowed_hosts_for_source(label) is not None:
            continue
        canon_id, kind, _ = canonical_source(label)
        if kind == "news" or canon_id in NOT_A_REGULATOR_SOURCE:
            continue            # exempt on the record, not missing
        dark[label] = n
    assert not dark, (
        "%d Source label(s) reach Recalls with no host allow-list, so "
        "nothing checks that their rows cite the regulator they name:\n"
        "    %s\n  \n  Add the authority's own domain to "
        "verify_urls.HOST_FOR_CANONICAL_SOURCE under the canonical id "
        "pipeline/_source_canon gives the label — or, if the label names "
        "media or a laboratory rather than a regulator, name it in "
        "verify_urls.NOT_A_REGULATOR_SOURCE so the exemption is a decision "
        "on the record instead of a gap." % (
            len(dark),
            "\n    ".join(f"{k!r} ({v} rows) -> canonical "
                          f"{canonical_source(k)[0]!r}"
                          for k, v in sorted(dark.items(),
                                             key=lambda kv: -kv[1]))))


def test_no_key_in_the_label_map_is_a_label_nothing_produces():
    """A key matching no row is a check someone believes is running."""
    published = set(_published_source_labels())
    orphans = sorted(
        k for k in HOST_FOR_SOURCE
        if k not in published
        # An orphan is harmless once the canonical table covers the live
        # spelling: the rows ARE checked, through the identity. What must
        # never happen is an orphan whose regulator is checked nowhere.
        and canonical_source(k)[0] not in HOST_FOR_CANONICAL_SOURCE)
    assert not orphans, (
        "These HOST_FOR_SOURCE keys match no published Source label AND "
        "their regulator has no canonical entry either, so the host check "
        "they look like is not running for anyone: %s" % orphans)


@pytest.mark.parametrize("label,host_must_be_allowed", [
    # The two renames that were switching the check off.
    ("AFSCA (BE)", "favv-afsca.be"),
    ("FAVV (BE)", "favv-afsca.be"),
    ("Ministero della Salute (IT)", "salute.gov.it"),
    ("Ministero della Salute", "salute.gov.it"),
    # A label and its bare collector spelling must share one allow-list.
    ("VFA (VN)", "vfa.gov.vn"),
    ("VFA", "vfa.gov.vn"),
    ("NKFH (HU)", "nkfh.gov.hu"),
    ("NÉBIH", "nkfh.gov.hu"),
])
def test_a_rename_keeps_its_host_allow_list(label, host_must_be_allowed):
    allowed = allowed_hosts_for_source(label)
    assert allowed, f"{label!r} resolves to no host allow-list"
    assert host_must_be_allowed in allowed, (
        f"{label!r} allows {list(allowed)}, which does not include the "
        f"regulator's own host {host_must_be_allowed!r}")


@pytest.mark.parametrize("label", [
    "Food Safety News (citing EODY / Region of Central Greece)",
    "BeaconBio (TW)",
])
def test_a_non_regulator_label_is_exempt_on_purpose(label):
    """Exempt, and recorded as exempt — not merely absent."""
    assert allowed_hosts_for_source(label) is None
    canon = canonical_source(label)[0]
    assert canon in NOT_A_REGULATOR_SOURCE or canonical_source(label)[1] == "news", (
        f"{label!r} is unchecked but is not recorded as a non-regulator")


def test_an_aggregator_label_keeps_the_base_regulators_hosts():
    """"CFS (HK) - aggregator (RappelConso FR)" is still Hong Kong."""
    allowed = allowed_hosts_for_source("CFS (HK) - aggregator (RappelConso FR)")
    assert allowed and "cfs.gov.hk" in allowed


def test_the_publish_gate_resolves_through_the_same_function():
    """Rule 6b must not go back to a raw-label dict lookup."""
    src = (ROOT / "pipeline" / "_publish_gate.py").read_text(encoding="utf-8")
    assert "allowed_hosts_for_source" in src, (
        "pipeline/_publish_gate.py no longer imports "
        "verify_urls.allowed_hosts_for_source — rule 6b is back to a raw "
        "label lookup, which is what let a rename switch Belgium and Italy "
        "off.")
    assert "HOST_FOR_SOURCE.get(" not in src, (
        "pipeline/_publish_gate.py looks a Source label up in "
        "HOST_FOR_SOURCE directly again.")
