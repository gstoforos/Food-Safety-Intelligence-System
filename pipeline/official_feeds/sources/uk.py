"""
UK source — Food Standards Agency (FSA) Food Alerts API.

Endpoint: https://data.food.gov.uk/food-alerts/id  (JSON, OGL v3.0)
Covers England, Wales, Northern Ireland (Scotland handled separately in
scotland.py per AFTS spec, using country=GB-SCT).

Alert types:
  PRIN  = Product Recall Information Notice  → alert_type 'recall'
  AA    = Allergy Alert                      → alert_type 'allergy'
  FAFA  = Food Alert For Action              → alert_type 'action'

JSON item shape (from API reference + CSV export):
  @id, type, title, shortTitle, status, notation, created, modified, url,
  alertAuthor (company), problem[] {reason, allergen.label, pathogenRisk.label,
  hazardCategory.label}, productDetails[] {productName}, country[]
"""

from __future__ import annotations

from datetime import date, timedelta

from ..base import Record, FeedSource, register
from ..fetch import get_json, parse_iso

API = "https://data.food.gov.uk/food-alerts/id.json"

_TYPE_MAP = {
    "PRIN": "recall",
    "AA": "allergy",
    "FAFA": "action",
}


def _extract_type(item: dict) -> str:
    """item['type'] is a list of URIs like ['.../def/Alert', '.../def/PRIN']."""
    types = item.get("type", [])
    if isinstance(types, str):
        types = [types]
    for t in types:
        tail = str(t).rstrip("/").split("/")[-1]
        if tail in _TYPE_MAP:
            return _TYPE_MAP[tail]
    return ""


def _first(lst):
    if isinstance(lst, list) and lst:
        return lst[0]
    return lst if lst else {}


def _join_label(node, *keys) -> str:
    """Pull .label or text from a possibly-list node for given keys."""
    if isinstance(node, list):
        node = node[0] if node else {}
    parts = []
    for k in keys:
        v = node.get(k) if isinstance(node, dict) else None
        if isinstance(v, dict):
            v = v.get("label") or v.get("@id", "")
        if isinstance(v, list):
            v = " ".join(str(x.get("label", x) if isinstance(x, dict) else x) for x in v)
        if v:
            parts.append(str(v))
    return " ".join(parts)


def _hazard_text(item: dict) -> str:
    prob = _first(item.get("problem", []))
    if not isinstance(prob, dict):
        return ""
    bits = []
    for key in ("reason", "allergen", "pathogenRisk", "hazardCategory"):
        v = prob.get(key)
        if isinstance(v, dict):
            v = v.get("label") or ""
        elif isinstance(v, list):
            v = " ".join(
                (x.get("label", "") if isinstance(x, dict) else str(x)) for x in v
            )
        if v:
            bits.append(str(v))
    return " ".join(bits)


def _product(item: dict) -> str:
    pd = item.get("productDetails", [])
    if isinstance(pd, list):
        names = []
        for p in pd:
            if isinstance(p, dict) and p.get("productName"):
                names.append(str(p["productName"]))
        return "; ".join(names)
    if isinstance(pd, dict):
        return str(pd.get("productName", ""))
    return ""


def _country_codes(item: dict) -> list[str]:
    c = item.get("country", [])
    if isinstance(c, dict):
        c = [c]
    out = []
    for entry in c:
        if isinstance(entry, dict):
            cid = entry.get("@id", "")
        else:
            cid = str(entry)
        tail = cid.rstrip("/").split("/")[-1]  # e.g. GB-ENG
        if tail:
            out.append(tail)
    return out


def _derive_country(codes: list[str], include_scotland: bool):
    """Map FSA country[] codes → (country_code, country_name, authority).

    The FSA API tags each alert with GB-ENG / GB-WLS / GB-NIR / GB-SCT.
    Previously the record hardcoded GB / United Kingdom / FSA for every
    non-Scotland alert, which mislabelled cross-border notices (e.g. an
    FSAI Irish recall the FSA cross-notifies for Northern Ireland was
    written as 'United Kingdom / FSA' with an fsa-prin URL, duplicating
    the FSAI primary). Derive the jurisdiction from the actual codes.
    """
    up = {c.upper() for c in (codes or [])}
    if include_scotland or up == {"GB-SCT"}:
        return "sct", "Scotland", "FSS"
    # Any England/Wales/Northern-Ireland tag (alone or mixed) → UK/FSA.
    if up & {"GB-ENG", "GB-WLS", "GB-NIR", "GB"}:
        return "gb", "United Kingdom", "FSA"
    # Untagged alerts default to UK/FSA (the API's own scope).
    return "gb", "United Kingdom", "FSA"


def fetch(limit: int = 50, include_scotland: bool = False,
          lookback_days: int = 90, max_pages: int = 40) -> list[Record]:
    """
    Fetch recent FSA alerts. By default EXCLUDES Scotland-only alerts
    (those are emitted by scotland.py). An alert tagged for multiple
    countries including England/Wales/NI is kept here.

    `limit` IS NO LONGER SENT TO THE API and is retained only so existing
    callers (scotland.py, main.py, the tests) keep working. This endpoint
    rejects unrecognised query parameters and has no recognised paging
    parameter at all; the page cap is a server-side default reported in
    meta.limit. Passing a different value here changes nothing. See the
    note in the body.
    """
    # ---------------------------------------------------------------------
    # SILENT SCRAPER FAILURE (found 2026-08-07).
    #
    # This call used to read:
    #     get_json(API, params={"_limit": limit, "_sort": "-created"})
    #
    # The FSA endpoint is an Epimorphics Linked Data API. It accepts NEITHER
    # of those parameters and ignores both silently — no error, no warning,
    # HTTP 200. What comes back is the FIRST page of the collection in its
    # natural order: fifty alerts from January-April 2018.
    #
    # So the scraper ran green every day, returned fifty rows every day, and
    # every one of them was eight years old — dropped downstream by the
    # minimum-date rule. Zero output, zero errors. "FSA (UK)" does not appear
    # in scraper-health.json either, so nothing else flagged it.
    #
    # Cost: no FSA (UK) row entered the database between 2026-07-08 and
    # 2026-08-07. Three in-scope recalls were missed —
    #   FSA-PRIN-36-2026 (21 Jul) Waitrose brioche rolls, hard plastic + metal
    #   FSA-PRIN-37-2026 (22 Jul) Graham's Family Dairy milk, veterinary
    #                             medicines including penicillin
    #   FSA-PRIN-38-2026 (07 Aug) Greencore, three pasta products, Listeria
    #                             monocytogenes
    #
    # The parameter this API actually honours is `min-created`. Verified by
    # hand: min-created=2026-07-01 returns twelve 2026 alerts, while
    # _sort=-created returns FSA-AA-01-2018. A rolling date window is used
    # rather than a page count so the query cannot drift back in time again.
    # ---------------------------------------------------------------------
    #
    # ---------------------------------------------------------------------
    # THE PAGE SIZE IS CAPPED AND `_page` IS IGNORED — SLICE BY DATE
    # (fix 2026-08-15, replacing the 2026-08-13 `_page` attempt).
    #
    # History of three wrong answers to the same question:
    #
    #   1. `_sort=-created`      ignored by the API; served 2018 alerts.
    #   2. `_pageSize=250`       400 Client Error. Took out UK AND Scotland
    #                            (scotland.py delegates here) on every run.
    #   3. `_page=<n>`           ALSO 400 from GitHub Actions, and useless
    #                            anyway. Both established by measurement on
    #                            2026-08-15:
    #
    #        _pageSize=200  -> 200 accepted, metadata "limit": 50.
    #                          The server CLAMPS to 50; it does not error.
    #        _pageSize=250  -> 400.
    #        _page=0 vs _page=1 -> identical first item (FSA-PRIN-23-2026),
    #                          and the payload carries no page/next/prev
    #                          metadata at all. `_page` does nothing.
    #
    # So: at most 50 records per request, and no pagination parameter that
    # works. The ONLY lever that changes the result set is the date window,
    # and `min-created` + `max-created` are both honoured (verified: the
    # window 2026-04-27..2026-06-15 returns exactly 15 alerts).
    #
    # Therefore the lookback is walked in DATE SLICES. A slice that comes
    # back full (50) is ambiguous — it may have been truncated — so it is
    # SPLIT IN HALF and re-fetched until every slice returns short. That
    # makes truncation impossible rather than merely unlikely, which is the
    # failure mode that cost a month of UK recalls twice already.
    #
    # ── THE ANSWER, 2026-08-19: `_pageSize` IS NOT A PARAMETER AT ALL ────
    #
    # Four wrong diagnoses preceded this one, and every one of them was a
    # guess made without the server's own explanation, because get_json
    # discarded the response body and all anyone ever saw was
    #     400 Client Error:  for url: ...
    # with an empty reason. Once _raise_with_body() started printing the
    # body, the FSA answered in one line:
    #
    #     "message": "These parameters haven't been recognized: _pageSize"
    #
    # Not the page size. Not `_page`. Not the date window. Not the TLS
    # fingerprint or the User-Agent. The parameter itself is unknown to
    # this API, and the service now REJECTS unknown parameters instead of
    # ignoring them. From the response metadata:
    #
    #     "comment": "Updated beta-service, API backward compatible",
    #     "version": "2.0.1",
    #     "limit": 50
    #
    # `limit: 50` is returned when NOTHING is asked for. The cap is a
    # server-side default that no query parameter controls — which is why
    # `_pageSize=200` was silently clamped to 50 back when unknown
    # parameters were still ignored, and why `_pageSize=250` looked like a
    # size problem when it was a recognition problem all along.
    #
    # So the parameter is simply dropped. Sending nothing is valid against
    # the strict service AND against any lenient deployment, which the
    # earlier "fixes" were not.
    #
    # The cap is READ FROM THE RESPONSE (`meta.limit`) rather than
    # hardcoded, so if the FSA raises or lowers it the slicing adapts
    # instead of silently over- or under-fetching. PAGE_CAP_DEFAULT is only
    # the value used before the first response comes back.
    #
    # Date slicing stays and is still load-bearing: the window
    # 2025-01-01..2026-08-19 returns exactly 50 items with limit 50, i.e.
    # truncated with no indication in the payload. Nothing about this API
    # tells you a result set was cut short — the slicing is the only thing
    # that makes truncation impossible.
    # ---------------------------------------------------------------------
    PAGE_CAP_DEFAULT = 50  # meta.limit as served on 2026-08-19
    page_cap = PAGE_CAP_DEFAULT
    today = date.today()
    start = today - timedelta(days=lookback_days)
    since = start.isoformat()      # used in the log lines and the guards below
    items: list[dict] = []
    seen_ids: set[str] = set()
    slices: list[tuple[date, date]] = [(start, today)]
    n_req = 0

    def _add(batch) -> int:
        added = 0
        for it in batch:
            key = str(it.get("@id") or it.get("notation") or "")
            if key and key in seen_ids:
                continue
            if key:
                seen_ids.add(key)
            items.append(it)
            added += 1
        return added

    while slices:
        if n_req >= max_pages:
            print(f"  [WARN] FSA food-alerts: hit the {max_pages}-request "
                  f"backstop with {len(slices)} date slice(s) unfetched. "
                  f"Coverage may be incomplete.")
            break
        lo, hi = slices.pop(0)
        n_req += 1
        # NO PAGING PARAMETER IS SENT. See the note above: this API
        # rejects unrecognised parameters, and there is no recognised one.
        data = get_json(API, params={"min-created": lo.isoformat(),
                                     "max-created": hi.isoformat()})
        batch = data.get("items", []) or []
        _add(batch)

        # Take the cap from the server rather than trusting a constant.
        served = data.get("meta") or {}
        try:
            served_limit = int(served.get("limit") or 0)
        except (TypeError, ValueError):
            served_limit = 0
        if served_limit > 0 and served_limit != page_cap:
            print(f"  [FSA] page cap is {served_limit} (was assuming "
                  f"{page_cap}) — adapting")
            page_cap = served_limit

        if len(batch) >= page_cap and (hi - lo).days > 1:
            # Full page: the slice may be truncated. Halve it and redo.
            mid = lo + (hi - lo) / 2
            slices.append((lo, mid))
            slices.append((mid, hi))
        elif len(batch) >= page_cap:
            print(f"  [WARN] FSA food-alerts: {lo}..{hi} is a single day and "
                  f"still returned a full page ({len(batch)}). Cannot narrow "
                  f"further; that day may be truncated.")

    # A window this wide is never legitimately empty — the FSA publishes
    # several alerts a week. Empty means the query stopped working again,
    # and that has to be loud rather than green.
    if not items:
        raise RuntimeError(
            f"FSA food-alerts returned 0 items for min-created={since}. "
            f"The FSA publishes several alerts a week, "
            f"so an empty {lookback_days}-day window means the query is "
            f"being ignored — which is exactly how a month of UK recalls was "
            f"lost in July 2026. Check the parameter names against "
            f"https://data.food.gov.uk/food-alerts/ before concluding it was "
            f"a quiet month.")

    # The endpoint ignores unknown parameters rather than rejecting them, so
    # a working filter must be confirmed from the DATA, not the HTTP status.
    newest = max((str(i.get("created") or "") for i in items), default="")
    if newest and newest[:10] < since:
        raise RuntimeError(
            f"FSA food-alerts ignored min-created={since}: the newest item "
            f"returned is {newest[:10]}. This is the July 2026 failure mode "
            f"— the API served its oldest page and the scraper reported "
            f"success.")

    print(f"  [FSA] {len(items)} alerts since {since} "
          f"across {n_req} request(s), page cap {page_cap}; "
          f"newest {newest[:10] or '?'}")

    records: list[Record] = []
    for item in items:
        notation = item.get("notation") or item.get("@id", "").split("/")[-1]
        countries = _country_codes(item)
        is_scotland_only = countries == ["GB-SCT"]
        if include_scotland:
            keep = "GB-SCT" in countries
        else:
            keep = (not countries) or (not is_scotland_only)
        if not keep:
            continue

        company = item.get("alertAuthor") or item.get("sender") or ""
        if isinstance(company, dict):
            company = company.get("label", "") or company.get("name", "")

        cc, cname, auth = _derive_country(countries, include_scotland)

        # THE FIELD IS alertURL, NOT url (audit 2026-08-09).
        #
        # The intent below was already right — "prefer the public news-alerts
        # URL over the data.food.gov.uk JSON @id" — but it read a field this
        # API does not publish. item.get("url") was always None, so every FSA
        # row fell through to the @id and was stored as
        #     https://data.food.gov.uk/food-alerts/id/FSA-PRIN-38-2026
        # which is the machine-readable record (JSON/RDF/CSV/Turtle), not a
        # page a subscriber can open. Eight published rows carry it.
        #
        # The human page is published by the record itself, in alertURL:
        #     alertURL  https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-38-2026
        #
        # Read from the record rather than derived by transform. The obvious
        # transform — lowercase the notation onto
        # www.food.gov.uk/news-alerts/alert/ — is what a 2018 record suggests
        # and it is WRONG for 2026:
        #     www.food.gov.uk/news-alerts/alert/fsa-prin-38-2026     -> 404
        #     alerts.food.gov.uk/news-alerts/alert/fsa-prin-38-2026  -> the notice
        # The FSA moved its alert host; older alerts still answer on www, so a
        # rule inferred from an old record produces dead links for new ones
        # while looking verified. The record's own field cannot drift that way.
        #
        # If a record ever lacks alertURL the @id is NOT used as a fallback: a
        # metadata endpoint is not a notice, and publishing one is the defect
        # this comment exists to describe.
        pub_url = (item.get("alertURL") or item.get("alerturl")
                   or item.get("url") or "")
        if not pub_url:
            print(f"  [WARN] {notation}: no alertURL in the record "
                  f"({item.get('@id', '')}) — emitting the row without a URL "
                  f"rather than citing the metadata endpoint")

        rec = Record(
            source_id=notation,
            country_code=cc,
            country_name=cname,
            authority=auth,
            title=item.get("title", ""),
            company=str(company),
            product=_product(item),
            hazard=_hazard_text(item),
            alert_type=_extract_type(item),
            published=parse_iso(item.get("created", "")),
            url=pub_url,
            raw=item,
        )
        records.append(rec)
    return records


UK = FeedSource(
    code="uk",
    name_en="United Kingdom",
    authority_short="FSA",
    fetcher=fetch,
    region="Europe",
    timezone="Europe/London",
    run_local_hour=9,
    cron_utc_offsets=(8, 9),  # 09:00 London = 08:00 UTC (BST) / 09:00 UTC (GMT)
    gnews_authority="FSA Food Standards Agency",
    gnews_terms=("salmonella", "listeria", "E. coli", "botulism",
                 "undeclared allergen"),
    gnews_hl="en-GB", gnews_gl="GB", gnews_ceid="GB:en",
    gnews_days_back=3,
)

register(UK)
