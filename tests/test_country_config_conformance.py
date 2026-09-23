# -*- coding: utf-8 -*-
"""Every country config must meet the bar Greece set.

THE GREEK ROUTE (the pattern every config here implements)
==========================================================
EFET blocks datacentre traffic, so Greece is not scraped directly. It is
found in LOCAL MEDIA, confirmed from the article — product, date, hazard —
and then resolved back to the regulator's own page, which is the URL that
gets published. The authority-URL gate stays absolute; only the route to
it changes.

Measured 2026-09-23, that is not an exotic case — it is the normal one.
Of 66 scrapers, 47 are silent and 34 have never placed a row. Of the 33
covering Asia, Latin America, the Middle East and Africa, ONE produced in
45 days. When I ran seven of them here every single fetch was refused at
the proxy. The direct route is the exception; the Greek route is the rule.

So new coverage is added as a CountryConfig, not as a scraper, and this
file is the bar each one has to clear.

WHAT IS CHECKED, AND WHY EACH ONE IS HERE
-----------------------------------------
``authority_item_url_regex`` must match a real notice and must NOT match
that authority's own listing page. A regex that accepts the index is how
BLV's "Mises en garde et rappels" landing page reached Recalls with
Company = Brand = the page title and Product = "aliments".

``authority_item_url_regex`` must be scoped to the agency's path when the
host is shared. gov.br is the whole Brazilian federal government and
gob.mx the whole Mexican one; a bare host match would let any ministry's
page pass as a food-safety authority.

``news_authority_mode`` must be False wherever a per-recall page exists.
It is a deliberate, scoped relaxation of the authority-pure guarantee for
countries whose regulator publishes no linkable per-recall page at all —
not a convenience.

``cron_utc_offsets`` must actually correspond to the timezone. Getting
this wrong does not fail loudly; it just runs the country at the wrong
hour forever.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.gap_finder.countries import all_codes, get  # noqa: E402

CODES = all_codes()

#: Known real per-recall URLs, and pages that must NOT be accepted.
#: Only for configs added or verified on 2026-09-23 — the older ones were
#: verified when they were written and are not re-litigated here.
KNOWN_URLS = {
    "sg": (
        ["https://www.sfa.gov.sg/news-publications/newsroom/recall-of-coolibah-"
         "herbs-gourmet-salad-mix-due-to-exceeding-levels-of-bacillus-cereus",
         "https://www.sfa.gov.sg/news-publications/newsroom/2025/recall-of-"
         "various-tasti-brand-products-due-to-possible-presence-of-metal-pieces"],
        ["https://www.sfa.gov.sg/news-publications/newsroom/sfa-annual-report-2025",
         "https://www.sfa.gov.sg/food-for-thought/article/detail/"
         "safeguarding-food-safety-through-food-recalls"],
    ),
    "hk": (
        ["https://www.cfs.gov.hk/english/press/20260416_12332.html",
         "https://www.cfs.gov.hk/tc_chi/press/20260416_12332.html",
         # The Food Alerts board. The live register produced this one
         # itself on 2026-09-23 (US raw oysters, excessive E. coli) and
         # the press-only pattern rejected it.
         "https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/2026_627.html",
         "https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/2026_616.html",
         "https://www.cfs.gov.hk/english/whatsnew/whatsnew_sfpa/2026_101.html",
         # Food Incident Posts, published as PDFs. 14 rows already in
         # Recalls use this shape; the HTML-only pattern rejected them all.
         "https://www.cfs.gov.hk/english/rc/subject/files/20260914_1.pdf",
         "https://www.cfs.gov.hk/english/rc/subject/files/20260302_1.pdf"],
        # An item is <year>_<serial>.html; the LISTING repeats the board name.
        ["https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/whatsnew_fa.html",
         "https://www.cfs.gov.hk/english/whatsnew/whatsnew_sfpa/whatsnew_sfpa.html",
         "https://www.cfs.gov.hk/english/press/press.html",
         "https://www.cfs.gov.hk/english/import/import_icfsg_08.html"],
    ),
    "kr": (
        ["https://www.mfds.go.kr/eng/brd/m_61/view.do?seq=192&srchFr=&srchTo="
         "&srchWord=&srchTp=&itm_seq_1=0&itm_seq_2=0&multi_itm_seq=0"
         "&company_cd=&company_nm=&page=1",
         "https://www.mfds.go.kr/brd/m_99/view.do?seq=48210"],
        ["https://www.mfds.go.kr/eng/brd/m_61/list.do",
         "https://www.mfds.go.kr/eng/wpge/m_11/de011002l001.do"],
    ),
    "jp": (
        ["https://www.recall.caa.go.jp/result/detail.php?rcl=00000034744&screenkbn=06",
         "https://www.recall.caa.go.jp/result/detail.php?rcl=00000034743&screenkbn=01"],
        ["https://www.recall.caa.go.jp/result/index.php?screenkbn=01&category=1",
         "https://www.recall.caa.go.jp/about/"],
    ),
    "ph": (
        ["https://www.fda.gov.ph/fda-advisory-no-2026-1226-public-health-warning-"
         "against-the-purchase-and-consumption-of-the-unregistered-food-product-"
         "in-foreign-language-da-la-pian/"],
        # The second is the trap: its slug contains "recall" but it is a
        # policy consultation, not a recall.
        ["https://www.fda.gov.ph/market-surveillance/",
         "https://www.fda.gov.ph/draft-for-comments-guidelines-on-the-recall-of-"
         "authorized-health-products-regulated-by-the-food-and-drug-administration/"],
    ),
    "tw": (
        ["https://www.fda.gov.tw/tc/newsContent.aspx?cid=4&id=t634701",
         "https://www.fda.gov.tw/tc/newsContent.aspx?cid=4&id=31722"],
        # news.aspx vs newsContent.aspx is the whole index/item distinction.
        ["https://www.fda.gov.tw/tc/news.aspx?cid=4",
         "https://www.fda.gov.tw/eng/news.aspx"],
    ),
    "id": (
        ["https://www.pom.go.id/siaran-pers/penarikan-produk-mi-instan-indonesia",
         "https://www.pom.go.id/penjelasan-publik/penarikan-4-produk-pangan-oleh-sfa"],
        ["https://www.pom.go.id/siaran-pers",
         "https://www.pom.go.id/penjelasan-publik"],
    ),
    "co": (
        ["https://invima.gov.co/biblioteca/preview/102418",
         # The one Colombian row already in Recalls — a press-room article,
         # not a library document.
         "https://www.invima.gov.co/blog/sala-de-prensa-13/alimento-para-"
         "propositos-medicos-especiales-contaminado-con-cronobacter"],
        ["https://app.invima.gov.co/alertas/alertas-alimentos-bebidas",
         "https://app.invima.gov.co/alertas/alertas-sanitarias-general",
         "https://www.invima.gov.co/blog/sala-de-prensa-13"],
    ),
    "sa": (
        ["https://www.sfda.gov.sa/en/news/2683516",
         "https://sfda.gov.sa/ar/news/17638"],
        # The drugs board has its own path and must stay out of a food register.
        ["https://www.sfda.gov.sa/en/drugscircularsandwithdrawal/89167",
         "https://www.sfda.gov.sa/en/drugs-circulars-withdrawal"],
    ),
    "cl": (
        ["https://www.achipia.gob.cl/2024/10/25/ministerio-de-salud-comunica-"
         "presencia-de-listeria-en-alimentos/",
         "https://www.achipia.gob.cl/2018/04/20/minsal-decreta-alerta-"
         "alimentaria-en-formula-lactea/"],
        # The report library shares the year/month segments but has no day.
        ["https://www.achipia.gob.cl/wp-content/uploads/2025/09/"
         "RIAL_Reporte_notificaciones_2024_16.pdf",
         "https://www.achipia.gob.cl/"],
    ),
    "vn": (
        ["https://vfa.gov.vn/tin-tuc/thu-hoi-san-pham-khong-bao-dam-an-toan-"
         "thuc-pham-theo-quyet-dinh-so-79qd-attp-ngay-1532025.html",
         "https://vfa.gov.vn/xu-ly-vi-pham-attp/cong-khai-danh-sach-co-so-vi-"
         "pham-hanh-chinh-ve-an-toan-thuc-pham-cap-nhat-tu-0142025-den-0172025.html"],
        # A SECTION landing page whose slug reads exactly like a notice.
        ["https://vfa.gov.vn/thanh-kiem-tra/xu-ly-vi-pham-ve-an-toan-thuc-pham.html",
         "https://vfa.gov.vn/tin-tuc"],
    ),
    "ae": (
        ["https://moccae.gov.ae/en/media-center/news/8/4/2022/ministry-of-"
         "climate-change-and-environment-recalls-kinder-surprise-uovo-maxi-"
         "chocolate-from-uae-mark"],
        # Standing guidance pages, not recalls.
        ["https://www.moccae.gov.ae/en/knowledge/food-safety",
         "https://www.moccae.gov.ae/en/knowledge-and-statistics/food-safety.aspx"],
    ),
    "ch": (
        ["https://www.blv.admin.ch/dam/blv/de/dokumente/rueckrufe/"
         "rr-blau-krabben.pdf.download.pdf/Rueckruf.pdf",
         "https://www.blv.admin.ch/dam/blv/de/dokumente/oeffentliche-warnungen/"
         "ow-moringa.pdf.download.pdf/260410.pdf",
         "https://www.blv.admin.ch/de/newnsb/F880wt7ttea28QG0eJI5b"],
        # The landing page the old word-match accepted on "rappel".
        ["https://www.blv.admin.ch/fr/mises-en-garde-et-rappels-aliments",
         "https://www.blv.admin.ch/de/aktuell"],
    ),
    "br": (
        ["https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa/2026/"
         "anvisa-determina-recolhimento-de-produtos-alimenticios-por-irregularidades"],
        ["https://www.gov.br/saude/pt-br/assuntos/noticias/2026/qualquer-coisa",
         "https://www.gov.br/anvisa/pt-br/centraisdeconteudo/publicacoes"],
    ),
    "mx": (
        ["https://www.gob.mx/cofepris/articulos/cofepris-emite-alertas-"
         "sanitarias-contra-cinco-productos-engano",
         "https://www.gob.mx/cofepris/prensa/algun-comunicado"],
        ["https://www.gob.mx/salud/articulos/cualquier-cosa",
         "https://www.gob.mx/cofepris/documentos/alertas-sanitarias-de-alimentos"],
    ),
}


def test_the_registry_discovers_countries_rather_than_listing_them():
    """base.get() used to carry a hand-written import tuple naming all 28
    modules, so a new config was invisible until someone remembered a
    second place — and silently, since get() only raises for the code you
    asked about."""
    src = (ROOT / "pipeline" / "gap_finder" / "countries" / "base.py"
           ).read_text(encoding="utf-8")
    assert "_import_all_countries" in src
    assert "pkgutil.iter_modules" in src
    assert "from . import (gr, it, es, pt," not in src, (
        "the hand-maintained import list is back")


def test_every_module_in_the_package_registers():
    """A file that exists but does not register is coverage the register
    claims and does not have."""
    pkg = ROOT / "pipeline" / "gap_finder" / "countries"
    mods = {p.stem for p in pkg.glob("*.py")
            if not p.stem.startswith("_") and p.stem != "base"}
    # iceland.py registers as "is" — Python keyword, see base.py.
    expected = {("is" if m == "iceland" else m) for m in mods}
    missing = sorted(expected - set(CODES))
    assert not missing, f"modules that do not register: {missing}"


@pytest.mark.parametrize("code", sorted(KNOWN_URLS))
def test_the_regex_matches_a_real_notice(code):
    cfg = get(code)
    good, _ = KNOWN_URLS[code]
    for u in good:
        assert re.search(cfg.authority_item_url_regex, u), (
            f"{code}: {cfg.authority_item_url_regex!r} does not match a "
            f"verified real notice:\n  {u}")


@pytest.mark.parametrize("code", sorted(KNOWN_URLS))
def test_the_regex_rejects_listings_and_other_agencies(code):
    """A regex that accepts the index is how BLV's landing page reached
    Recalls as a recall."""
    cfg = get(code)
    _, bad = KNOWN_URLS[code]
    for u in bad:
        assert not re.search(cfg.authority_item_url_regex, u), (
            f"{code}: {cfg.authority_item_url_regex!r} accepts a page that "
            f"is not a notice:\n  {u}")


@pytest.mark.parametrize("code", sorted(KNOWN_URLS))
def test_a_shared_government_host_is_scoped_to_the_agency(code):
    """gov.br is all of Brazil's federal government; gob.mx all of
    Mexico's. A bare host match makes any ministry an authority."""
    cfg = get(code)
    if cfg.authority_domain not in ("gov.br", "gob.mx", "gov.pl", "gouv.fr"):
        pytest.skip("not a shared government host")
    rx = cfg.authority_item_url_regex
    assert "/" in rx, f"{code}: regex must include a path, not just the host"


@pytest.mark.parametrize("code", CODES)
def test_the_config_is_internally_complete(code):
    cfg = get(code)
    assert cfg.code == code
    for field in ("name_en", "authority_short", "authority_full",
                  "authority_domain", "authority_item_url_regex",
                  "language_name", "language_code", "timezone"):
        assert str(getattr(cfg, field) or "").strip(), f"{code}: {field} empty"
    assert cfg.google_news_domains, f"{code}: no news domains — the Greek route needs them"
    assert cfg.google_news_keywords, f"{code}: no news keywords"
    assert cfg.recall_signal_terms, f"{code}: no title prefilter terms"
    assert 0 <= cfg.run_local_hour <= 23


@pytest.mark.parametrize("code", CODES)
def test_the_regex_compiles(code):
    re.compile(get(code).authority_item_url_regex)


@pytest.mark.parametrize("code", CODES)
def test_the_timezone_is_real_and_the_cron_offsets_match_it(code):
    """A wrong offset does not fail loudly — it runs the country at the
    wrong hour forever."""
    from datetime import datetime
    cfg = get(code)
    tz = ZoneInfo(cfg.timezone)                # raises if the name is bogus
    offs = set()
    for month in (1, 7):                       # winter and summer
        local = datetime(2026, month, 15, cfg.run_local_hour, tzinfo=tz)
        offs.add(int(local.utcoffset().total_seconds() // 3600))
    expected = {(cfg.run_local_hour - o) % 24 for o in offs}
    assert set(cfg.cron_utc_offsets) == expected, (
        f"{code}: cron_utc_offsets={cfg.cron_utc_offsets} but "
        f"{cfg.run_local_hour}:00 {cfg.timezone} is {sorted(expected)} UTC")


@pytest.mark.parametrize("code", CODES)
def test_news_authority_mode_is_off_where_a_real_notice_page_exists(code):
    """It relaxes the authority-pure guarantee. Only portal-less countries
    qualify — Egypt, Kenya, Ghana today."""
    cfg = get(code)
    if not cfg.news_authority_mode:
        return
    assert code in {"eg", "ke", "eg"}, (
        f"{code} has news_authority_mode=True. That lets a NEWS url enter "
        f"Recalls. It is for regulators with no linkable per-recall page at "
        f"all — if {code} has one, set this False and name the page shape "
        f"in authority_item_url_regex instead")


@pytest.mark.parametrize("code", sorted(KNOWN_URLS))
def test_the_new_configs_do_not_relax_the_authority_gate(code):
    assert get(code).news_authority_mode is False, (
        f"{code} was added with a verified per-recall page; it must not "
        f"also accept news URLs")


def test_the_new_countries_are_in_the_fleet_plan():
    """A config nobody runs is not coverage."""
    from tools.fleet_shard import plan
    covered = {c for shard in plan().values() for c in shard}
    for code in KNOWN_URLS:
        assert code in covered, f"{code} is registered but the fleet never runs it"

def _path_and_query(url: str) -> str:
    """Exactly what search_verifier builds before matching.

    pipeline/gap_finder/search_verifier.py, in the bulk-index filter:

        parsed = urlparse(h["url"])
        full_url_for_match = f"{parsed.path}?{parsed.query}" if parsed.query \
                             else parsed.path
        if not item_pattern.search(full_url_for_match): ...drop...

    Note what is NOT in that string: the netloc.
    """
    from urllib.parse import urlparse
    p = urlparse(url)
    return f"{p.path}?{p.query}" if p.query else p.path


@pytest.mark.parametrize("code", sorted(KNOWN_URLS))
def test_the_regex_matches_both_forms_the_pipeline_uses(code):
    """The regex is applied to two DIFFERENT strings, and one of them has
    no hostname in it.

    authority_url_finder and extractor match the full URL. search_verifier
    matches "path?query" with the netloc stripped. A regex that names the
    host therefore works at the first two sites and silently fails at the
    third — every bulk-index hit counted as portal_dropped, no error, no
    log line, the country just quietly loses its Tier-2 resolver.

    Found 2026-09-23 in br, hk, mx (written that morning) and in hu, which
    had been live since before June. Hungary's run_log shows the shape of
    it: nine runs, candidates found every time, extracted_accepted = 0
    every time, and every efet_url in verified.jsonl a news URL rather
    than a nebih.gov.hu one.

    The fix is the prefix gh.py and za.py already used:
        ^(?:https?://[^/]+)?/rest/of/path
    which consumes the scheme+host when it is there and matches nothing
    when it is not.
    """
    cfg = get(code)
    rx = re.compile(cfg.authority_item_url_regex, re.IGNORECASE)
    good, _ = KNOWN_URLS[code]
    for u in good:
        assert rx.search(u), f"{code}: no match against the full URL: {u}"
        pq = _path_and_query(u)
        assert rx.search(pq), (
            f"{code}: matches the full URL but NOT the string search_verifier "
            f"actually tests:\n  full: {u}\n  path?query: {pq}\n"
            f"  regex: {cfg.authority_item_url_regex}\n"
            f"Prefix it with ^(?:https?://[^/]+)? and scope by path instead "
            f"of by host.")


@pytest.mark.parametrize("code", CODES)
def test_no_regex_hard_codes_its_own_host(code):
    """The general form of the bug above, for countries that have no
    verified URL listed here yet."""
    cfg = get(code)
    rx = cfg.authority_item_url_regex
    host_bits = [b for b in cfg.authority_domain.split(".") if len(b) > 3]
    for bit in host_bits:
        if re.search(rf"{re.escape(bit)}\\?\.", rx):
            assert rx.startswith("^(?:https?://[^/]+)?"), (
                f"{code}: regex names its own host ({cfg.authority_domain}) "
                f"but does not start with the host-optional prefix, so it "
                f"cannot match in search_verifier:\n  {rx}")


# ── The research notes must stay true ───────────────────────────────────
# RESEARCH-NOTES.md records which countries were investigated on
# 2026-09-23 and which produced a config. A note that drifts out of date
# is worse than no note: the next person trusts it and skips the country.

NOTES = (ROOT / "pipeline" / "gap_finder" / "countries" / "RESEARCH-NOTES.md")

#: Named in the notes as researched with NO per-recall URL found, and so
#: deliberately NOT given a config. If one of these gains a config, the
#: notes must lose its section — hence the assertion below.
RESEARCHED_NO_CONFIG = {
    "in": "India — FSSAI",
    "my": "Malaysia — MOH / BKKM",
    "ma": "Morocco — ONSSA",
    "il": "Israel — Ministry of Health",
    "ar": "Argentina — ANMAT",
    "tr": "Turkey — Tarım ve Orman Bakanlığı",
    "th": "Thailand — FDA Thailand",
}


def test_the_research_notes_exist():
    assert NOTES.exists(), (
        "RESEARCH-NOTES.md is gone. It records which countries were checked "
        "and found to have no per-recall page — without it that research "
        "gets redone from scratch, or worse, a config gets written on a "
        "guessed pattern")


@pytest.mark.parametrize("code,heading", sorted(RESEARCHED_NO_CONFIG.items()))
def test_a_country_with_no_verified_url_has_no_config(code, heading):
    """The rule this whole audit turns on.

    A config whose regex matches nothing does not raise, does not log, and
    does not fail a workflow. It produces a country that runs daily, finds
    candidates, and accepts none of them — Hungary did that for nine runs.
    An ABSENT country is visible on any coverage count. A silently-empty
    one is not. So no config until a real per-recall URL is verified.
    """
    assert code not in CODES, (
        f"{code!r} now has a country config, but RESEARCH-NOTES.md still "
        f"lists it under 'Researched, no config written' as {heading!r}. "
        f"If a per-recall URL was found: move it to the verified table in "
        f"the notes, add its real URL and a negative to KNOWN_URLS above, "
        f"and delete it from RESEARCHED_NO_CONFIG here. All three, or the "
        f"next person reads a note that is no longer true.")


@pytest.mark.parametrize("code", sorted(RESEARCHED_NO_CONFIG))
def test_the_notes_actually_discuss_each_unresolved_country(code):
    text = NOTES.read_text(encoding="utf-8")
    assert f"`{code}`" in text or RESEARCHED_NO_CONFIG[code].split("—")[0].strip() in text, (
        f"{code!r} is listed here as researched-and-unresolved but "
        f"RESEARCH-NOTES.md does not say what was found or what to try "
        f"next. The finding is the useful part, not the verdict.")


@pytest.mark.parametrize("code", sorted(KNOWN_URLS))
def test_the_notes_claim_only_countries_that_exist(code):
    text = NOTES.read_text(encoding="utf-8")
    assert f"`{code}`" in text, (
        f"{code} has a verified URL in this test but is not in the "
        f"RESEARCH-NOTES.md table")
    assert code in CODES
