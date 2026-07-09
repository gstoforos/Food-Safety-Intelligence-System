"""
Tests for pipeline._url_identity and the FSAI duplication fix.

The single most important property under test is NEGATIVE:
non-FSAI sources must behave exactly as they did before. Every regulator
other than fsai.ie must keep URL-keyed dedup and must never be classified
as a soft 404.
"""
import pytest

from pipeline._url_identity import (
    content_key,
    fsai_url_problem,
    has_stable_id,
    is_fsai_canonical,
    is_listing_path,
    is_soft_404,
    row_rank,
)
from pipeline.merge_master import _dedup_key


# Real, live FSAI alert URLs (verified against fsai.ie 2026-07-09).
FSAI_CANONICAL = [
    "https://www.fsai.ie/news-and-alerts/food-alerts/ecall-of-specific-batches-of-horgans-and-m-s-truff",
    "https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-deluxe-spanish-castellano-she",
    "https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-specific-batches-of-various-cooked-ham-p",
    "https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-k-o-connell-fishmongers-smoke",
    # Mixed case: FSAI serves Mixed-Case paths and is case-insensitive.
    "https://www.fsai.ie/News-and-Alerts/Food-Alerts/Recall-of-specific-batches-of-various-Yopokki-rice",
    # Allergen alerts share the shape.
    "https://www.fsai.ie/news-and-alerts/allergen-alerts/undeclared-allergens-in-batches-of-moin-bio-croiss",
]

# URLs the Gemini gap-finder produced for alerts that already existed.
FSAI_FABRICATED = [
    "https://www.fsai.ie/news_centre/food_alerts/recall_horgans_mands_gouda_cheese.html",
    "https://www.fsai.ie/news_centre/food_alerts/recall_horgans_ms_truffle_gouda.html",
    "https://www.fsai.ie/news-centre/food-alerts/recall-of-a-batch-of-deluxe-spanish-castellano-sheeps-cheese",
    # Reconstructed full-length slug: 65 chars > the CMS's 50-char cap.
    "https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-k-o-connell-fishmongers-smoked-salmon-slices",
]

OTHER_REGULATORS = [
    "https://www.food.gov.uk/news-alerts/alert/fsa-prin-32-2026",
    "http://data.food.gov.uk/food-alerts/id/FSA-PRIN-33-2026",
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/856435",
    "https://rappel.conso.gouv.fr/fiche-rappel/22757/Interne",
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/frutas-y-hortalizas-del-sur-sa-i",
    "https://recalls-rappels.canada.ca/en/alert-recall/certain-pillsbury-brand-pizza-pops",
    "https://www.foodstandards.gov.au/food-recalls/recall-alert/bellevue-orchard-pty-ltd",
]


# ── canonical detection ───────────────────────────────────────────────────
@pytest.mark.parametrize("url", FSAI_CANONICAL)
def test_canonical_fsai_urls_accepted(url):
    assert is_fsai_canonical(url)
    assert fsai_url_problem(url) is None


@pytest.mark.parametrize("url", FSAI_FABRICATED)
def test_fabricated_fsai_urls_rejected(url):
    assert fsai_url_problem(url) is not None
    assert not is_fsai_canonical(url)


def test_truncated_slug_is_never_repaired():
    """The mid-word slug is what FSAI publishes. It must pass untouched."""
    assert fsai_url_problem(FSAI_CANONICAL[2]) is None  # ...cooked-ham-p


@pytest.mark.parametrize("url", OTHER_REGULATORS)
def test_other_regulators_never_flagged_by_fsai_rule(url):
    assert fsai_url_problem(url) is None


# ── soft 404 ──────────────────────────────────────────────────────────────
def test_fsai_legacy_redirect_to_listing_is_soft_404():
    assert is_soft_404(
        "https://www.fsai.ie/news_centre/food_alerts/recall_horgans_mands_gouda_cheese.html",
        "https://www.fsai.ie/news?page=2",
    )


def test_detail_page_that_stays_put_is_not_soft_404():
    u = FSAI_CANONICAL[0]
    assert not is_soft_404(u, u)


def test_url_that_was_always_a_listing_is_not_punished():
    assert not is_soft_404("https://www.fsai.ie/news", "https://www.fsai.ie/news")


def test_offsite_redirect_is_not_soft_404():
    assert not is_soft_404(
        "https://www.fsai.ie/news-and-alerts/food-alerts/x",
        "https://example.com/news",
    )


@pytest.mark.parametrize("url", OTHER_REGULATORS)
def test_unregistered_hosts_never_soft_404(url):
    """A redirect on an unregistered host must never be treated as dead."""
    assert not is_soft_404(url, "https://www.food.gov.uk/news-alerts")
    assert not is_listing_path(url)


# ── dedup keys ────────────────────────────────────────────────────────────
def _row(url, company, date="2026-07-06", pathogen="Listeria monocytogenes", **kw):
    r = {"URL": url, "Company": company, "Date": date, "Pathogen": pathogen}
    r.update(kw)
    return r


def test_fsai_same_alert_three_urls_collapses_to_one_key():
    """The exact production bug: 3 URLs, 1 alert, must yield 1 dedup key."""
    rows = [
        _row(FSAI_CANONICAL[0], "Horgans"),
        _row(FSAI_FABRICATED[0], "Horgans, M&S Food"),
        _row(FSAI_FABRICATED[1], "Horgans / M&S"),
    ]
    keys = {_dedup_key(r) for r in rows}
    assert len(keys) == 1, keys


def test_fsai_different_companies_same_day_stay_distinct():
    """Same date + pathogen but different operators are different recalls."""
    a = _row(FSAI_CANONICAL[0], "Leis Sauerampfer")
    b = _row(FSAI_CANONICAL[1], "Glenisk Baby Organic")
    assert _dedup_key(a) != _dedup_key(b)


def test_fsai_update_does_not_collapse_into_original():
    original = _row(FSAI_CANONICAL[3], "K O'Connell Fishmongers")
    update = _row(FSAI_CANONICAL[3], "K O'Connell Fishmongers",
                  Product="Update to recall of smoked salmon slices")
    assert _dedup_key(original) != _dedup_key(update)


@pytest.mark.parametrize("url", OTHER_REGULATORS)
def test_other_regulators_keep_url_dedup(url):
    """Non-FSAI rows must still key on the URL, not on content."""
    r = _row(url, "Some Operator")
    key = _dedup_key(r)
    assert not key.startswith("content|")
    assert has_stable_id(url)


def test_two_distinct_uk_alerts_stay_distinct():
    a = _row("https://www.food.gov.uk/news-alerts/alert/fsa-prin-32-2026", "A")
    b = _row("https://www.food.gov.uk/news-alerts/alert/fsa-prin-33-2026", "A")
    assert _dedup_key(a) != _dedup_key(b)


def test_empty_url_still_falls_back_to_content():
    r = _row("", "Some Operator")
    assert _dedup_key(r)


# ── survivor selection ────────────────────────────────────────────────────
def test_canonical_feed_row_outranks_fabricated_agent_row():
    feed = _row(FSAI_CANONICAL[0], "Horgans",
                Notes="[via official-feed collector]", DateAdded="2026-07-07")
    agent = _row(FSAI_FABRICATED[0], "Horgans, M&S Food",
                 Notes="[via Gemini gap-finder + Google Search]", DateAdded="2026-07-09")
    assert row_rank(feed) < row_rank(agent)
    assert min([agent, feed], key=row_rank) is feed


def test_content_key_is_stable_across_paraphrased_company_strings():
    a = content_key(_row(FSAI_CANONICAL[0], "Horgans"))
    b = content_key(_row(FSAI_FABRICATED[0], "Horgans Ltd"))
    assert a == b
