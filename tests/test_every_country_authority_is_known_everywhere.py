"""Every authority a country config accepts is known to every authority list.

2026-09-30. Three independent lists decide whether a URL is a regulator's:

  pipeline/_gap_finder_guards.py  REGULATOR_HOSTS   (gap-finder authority gate;
                                  a non-.gov host absent here is REJECTED)
  pipeline/regulatory_domains.py  REGULATOR_DOMAINS (promotion / resurrect)
  pipeline/_url_guard.py          _AUTHORITY_HOSTS  (the "could not read it is
                                  not it is gone" guard, and the escalation)

and a fourth thing — the country config — decides which authority the fleet
looks for. They were maintained separately and had drifted: measured on
2026-09-30, 29 country configs named at least one authority domain missing
from at least one list. NKFH (Hungary) and the CAA (Japan) were in none.
The daily search found the cost the same morning: an in-window Listeria
recall published only on nkfh.gov.hu, rejected three times.

This is a sweep, not a spot check: it walks every registered CountryConfig,
so a country added tomorrow is covered without editing this file.
"""
from __future__ import annotations

import importlib
import pkgutil

import pytest

import pipeline.gap_finder.countries as _countries_pkg
from pipeline.gap_finder.countries import base as _base
from pipeline.gap_finder.authority_url_finder import _authority_domains
from pipeline import _gap_finder_guards as guards
from pipeline import regulatory_domains as regdom
from pipeline import _url_guard as urlguard

for _m in pkgutil.iter_modules(_countries_pkg.__path__):
    if _m.name not in ("base", "__init__"):
        importlib.import_module(f"pipeline.gap_finder.countries.{_m.name}")

PAIRS = sorted((code, dom)
               for code, cfg in _base._REGISTRY.items()
               for dom in _authority_domains(cfg))


def test_the_sweep_sees_the_fleet():
    codes = {c for c, _ in PAIRS}
    assert {"hu", "jp", "kr", "id", "th", "cn"} <= codes
    assert len(PAIRS) >= 55


@pytest.mark.parametrize("code,dom", PAIRS, ids=[f"{c}:{d}" for c, d in PAIRS])
def test_gap_finder_gate_knows_it(code, dom):
    assert guards.is_regulator_url(f"https://{dom}/x/y"), (
        f"{code}: {dom} is an authority in the country config but not in "
        f"_gap_finder_guards.REGULATOR_HOSTS — a non-.gov host is rejected")


@pytest.mark.parametrize("code,dom", PAIRS, ids=[f"{c}:{d}" for c, d in PAIRS])
def test_regulatory_domains_knows_it(code, dom):
    assert regdom.is_regulator_url(f"https://{dom}/x/y"), (
        f"{code}: {dom} missing from regulatory_domains.REGULATOR_DOMAINS")


@pytest.mark.parametrize("code,dom", PAIRS, ids=[f"{c}:{d}" for c, d in PAIRS])
def test_url_guard_knows_it(code, dom):
    if dom in urlguard.UMBRELLA_HOSTS:
        pytest.skip(f"{dom} is a whole-government umbrella, excluded on purpose")
    assert urlguard.host_is_authority(f"https://{dom}/x/y"), (
        f"{code}: {dom} missing from _url_guard._AUTHORITY_HOSTS — rows on it "
        f"are unprotected from reachability rejections")


def test_umbrellas_stay_out_of_the_escalation():
    for h in urlguard.UMBRELLA_HOSTS:
        assert not urlguard.host_is_authority(f"https://{h}/a/b/c")
        assert urlguard.url_is_self_evidently_official(
            f"https://www.{h}/some/deep/page") == ""


# ── Hungary: the incident ────────────────────────────────────────────────

NKFH = ("https://nkfh.gov.hu/hirek/termekvisszahivas-auchan-nivo-fuestoelt-"
        "suelt-sonka-listeria-jelenlete-miatt")


def test_nkfh_is_a_hungarian_authority_item():
    from pipeline.gap_finder.countries import get
    from pipeline.gap_finder.authority_url_finder import (
        host_is_authority, url_is_authority_item)
    hu = get("hu")
    assert host_is_authority(NKFH, hu)
    assert url_is_authority_item(NKFH, hu)
    assert not url_is_authority_item("https://nkfh.gov.hu/hirek", hu), (
        "the news index must not pass as a per-recall notice")
    assert url_is_authority_item("https://portal.nebih.gov.hu/-/x-y", hu), (
        "NÉBIH items must still pass")


def test_the_url_guard_uses_the_extra_domains_pattern():
    """_item_url_regex_for used to read only authority_domain, so a second
    authority fell back to the generic "any deep page" test."""
    assert "per-recall URL pattern" in \
        urlguard.url_is_self_evidently_official(NKFH)
    assert urlguard.url_is_self_evidently_official(
        "https://nkfh.gov.hu/hirek/some-unrelated-news") == ""
