"""Every monitored regulator has a canonical identity and a host allow-list.

WHAT WAS WRONG (audit 2026-10-27)
=================================
Measured against tools/monitored_sources.SOURCES on the morning of
2026-10-27: 30 of the 78 registry labels had NO entry in
pipeline/_source_canon.CANONICAL, so canonical_source() returned
kind='unknown' for each, and therefore none of them had an entry in
verify_urls.HOST_FOR_CANONICAL_SOURCE either.

    ASAE (PT)   SECLU (LU)   Fødevarestyrelsen (DK)   Livsmedelsverket (SE)
    ANSVSA (RO) BFSA (BG)    UVHVVR (SI)   PVD (LV)   VMVT (LT)
    FSA BiH (BA) FVA (MK)    ANSA (MD)     TGTHB (TR) ACHIPIA (CL)
    ISP (CL)    DIGESA (PE)  ARCSA (EC)    MHLW (JP)  SAMR (CN)
    TFDA (TW)   BPOM (ID)    KKM (MY)      FSSAI (IN) SFDA (SA)
    MoCCAE (AE) MoPH (QA)    MoH (IL)      ONSSA (MA) NAFDAC (NG)
    KEBS (KE)

Two consequences, and neither is cosmetic:

  * NOTHING HOST-CHECKED THEIR ROWS. verify_urls' own comment says
    "Sources absent from this map are not checked — better silent than
    wrong", and the canonical-source indirection added on 2026-10-26 exists
    so a renamed label cannot switch that check off. But it resolves
    THROUGH _source_canon, so a label that module has never heard of
    switches the check off just as completely as a rename does. The 11
    structural problems verify_urls reports on the live register — rows
    citing news.google.com, en.sedaily.com, chequeado.com, unotv.com and
    capitalfm.co.ke as the authority that published a recall — are what
    that silence hides.
  * COVERAGE COUNTED EACH AS ITS OWN UNKNOWN SOURCE, which is the figure
    _source_canon was written to make honest.

WHY A TEST OVER THE REGISTRY
============================
This defect has now been repaired six times, and every repair before this
one was triggered by a row that had ALREADY PUBLISHED:

    2026-10-03  SZPI             2026-10-07  NÉBIH
    2026-10-04  Salute           2026-10-26  CAA (JP), Mattilsynet, …
    2026-10-24  MPI NZ           2026-10-27  Livsmedelsverket, Fødevarestyrelsen

tools/monitored_sources.SOURCES is the list of regulators the fleet
monitors — it is, by construction, the complete set of labels that can
reach Recalls. Asking the question of that list instead of of the register
is what makes the answer arrive before the row does.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._source_canon import canonical_source                # noqa: E402
from pipeline.verify_urls import (                                 # noqa: E402
    HOST_FOR_CANONICAL_SOURCE, allowed_hosts_for_source,
)
from tools.monitored_sources import SOURCES                        # noqa: E402

try:
    from pipeline.verify_urls import NOT_A_REGULATOR_SOURCE
except ImportError:                                                # pragma: no cover
    NOT_A_REGULATOR_SOURCE = frozenset()


def test_the_registry_is_not_empty():
    assert len(SOURCES) >= 78, len(SOURCES)


@pytest.mark.parametrize("label", sorted({l for l, _c, _d in SOURCES}))
def test_every_registry_label_has_a_canonical_identity(label):
    cid, kind, _mirrors = canonical_source(label)
    assert kind == "regulator", (
        f"{label!r} is a regulator this pipeline monitors, but "
        f"pipeline/_source_canon.CANONICAL has no entry for it, so "
        f"canonical_source() returns kind={kind!r} and invents the id "
        f"{cid!r}. Coverage counts it as its own unknown source and "
        f"verify_urls.HOST_FOR_CANONICAL_SOURCE cannot be reached for it.")


@pytest.mark.parametrize("label,domains",
                         sorted({(l, d) for l, _c, d in SOURCES}))
def test_every_registry_label_is_host_checked(label, domains):
    cid = canonical_source(label)[0]
    if cid in NOT_A_REGULATOR_SOURCE:
        return
    assert cid in HOST_FOR_CANONICAL_SOURCE, (
        f"{label!r} -> {cid!r} has no host allow-list, so nothing checks "
        f"that its rows cite {domains!r} rather than a news site.")
    allowed = allowed_hosts_for_source(label) or ()
    missing = [h for h in domains.split() if h not in allowed]
    assert not missing, (
        f"{label!r}: the registry says this authority publishes on "
        f"{missing}, but the host allow-list does not accept it, so its "
        f"own rows would fail their own check.")


def test_no_two_registry_labels_share_an_identity_by_accident():
    """Two labels SHOULD share an id only where one agency has two names
    (Belgium, Germany, Italy). Within the registry — one row per authority
    — every label must have its own."""
    seen = {}
    clashes = []
    for label, _country, _dom in SOURCES:
        cid = canonical_source(label)[0]
        if cid in seen:
            clashes.append((seen[cid], label, cid))
        seen[cid] = label
    assert not clashes, clashes


def test_the_two_that_published_on_2026_10_27():
    """Pinned: the offline promoter published these the morning the whole
    remainder was filed."""
    for label, host in (("Livsmedelsverket (SE)", "livsmedelsverket.se"),
                        ("Fødevarestyrelsen (DK)", "foedevarestyrelsen.dk")):
        assert canonical_source(label)[1] == "regulator", label
        assert host in (allowed_hosts_for_source(label) or ()), label
