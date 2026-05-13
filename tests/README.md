# AFTS FSIS — Test Suite

End-to-end tests for the highest-blast-radius modules in `pipeline/`.
Designed to catch the regressions that have hit production in the past
30 days **before** they ship again.

## What runs

```
tests/
  conftest.py                       — shared fixtures (xlsx factory, sample rows)
  test_url_year_scope.py            — URL year extractor + pathogen scope filter
  test_dedup.py                     — dedup_key + URL normalization
  test_report_week.py               — Friday-rollover sticky stamp
  test_xlsx_merge.py                — push-retry merge end-to-end (slow)
  test_weekly_review_capture.py     — Thursday-17:00-Athens cutoff + idempotency
```

Five test files, roughly 60 test cases. Run time: ~3–5 seconds total,
including filesystem I/O for the xlsx tests.

## Setup (one-time)

```bash
pip install -r requirements-dev.txt
```

## Run

```bash
pytest                     # full suite, verbose-ish (-ra summary)
pytest -v                  # full verbose
pytest -m "not slow"       # skip filesystem tests (~0.5s, logic only)
pytest tests/test_dedup.py # one file
pytest -k "weekly_rejected" # match test name pattern
```

Or via `make`:

```bash
make install-dev   # one-time
make test          # full suite
make test-fast     # skip slow
make coverage      # line-by-line coverage report → htmlcov/index.html
```

## What each file catches

### `test_url_year_scope.py` — URL year extraction + scope filters

Catches regressions in three single-purpose gatekeepers:
- `_url_year.url_year()` — extracts year from URLs (RappelConso new
  format, FDA recall numbers, FSIS slugs, etc). The 22019 fiche-rappel
  ID must NEVER be misread as year 2019 or 2202.
- `_url_year.is_year_mismatch()` — gating rule. Locks behavior that
  pre-2026 URLs always fail, and that an unextractable year is NOT a
  mismatch.
- `_pathogen_scope.is_in_scope()` — Listeria/Salmonella/Campylobacter/
  E. coli O157/Botulinum/Cronobacter pass; marine biotoxin, histamine,
  empty/None/dash do not.
- `_news_mirror_blocklist.is_news_mirror()` — sedaily, beaconbio,
  ilfattoalimentare, produktwarnung are blocklisted; clean regulators
  are not.

Why these matter: silent failures here either (a) drop valid recalls
that should have made the dashboard, or (b) admit invalid ones that
should have been rejected.

### `test_dedup.py` — URL normalization + dedup key

**Catches the 2026-05-06 dupe-slip bug**. Pre-2026-05-06 the dedup key
was `url.strip().lower()` — meaning `http://www.fsis.usda.gov/foo` and
`https://fsis.usda.gov/foo` produced DIFFERENT keys and both got
written to Recalls.

The fix normalizes: strip protocol, strip `www.`, strip trailing `/`,
strip `#fragment`, strip tracking query params, preserve identifier
query params (`permalink`, `id`, `fiche`, `ref`, `recall_id`).

The full battery of cases is in `TestNormalizeUrl`. If any fail,
http/https or utm-tagged variants are slipping through dedup again.

### `test_report_week.py` — Friday-rollover sticky stamp

Locks `compute_report_week()` behavior:
- Mon–Thu → next Friday's ISO week
- **Fri → following Friday** (strict next, +7 days)
- Sat/Sun → next Friday's ISO week
- Empty/garbage/None inputs → empty string, NEVER raises

The non-obvious rule: a recall dated ON Friday rolls to the NEXT week,
not the current one. This produces the clean Friday-09:00-Athens
publishing cadence by keeping Friday-late recalls out of that morning's
report.

### `test_xlsx_merge.py` (slow) — push-retry merge

**Catches the 2026-05-12 Weekly_Rejected wipe bug** and its older
cousin, the 2026-05-06 Weekly_Review wipe bug. Both bugs were the same
mechanism: `merge_xlsx_with_remote` created a fresh workbook with only
Recalls/Pending/NEWS, silently destroying any other sheets.

Five invariants tested:
1. **Recalls** only grows; remote wins on collision.
2. **Pending** is unioned then stripped of rows already in Recalls
   (no "shadow" Pending entries for published recalls).
3. **Weekly_Review** survives any merge from either side.
4. **Weekly_Rejected** survives any merge from either side.
5. **NEWS** union by Link/Title+date.

Plus a `TestMergeCountsReporting` test that locks the shape of the
returned `counts` dict (the operator's only visibility into what the
merge did).

### `test_weekly_review_capture.py` (slow) — Thursday cutoff

Tests two functions:
- `review_thursday_for(now_utc)` — the cutoff math. Thursday 17:00
  Athens local is the **strict** boundary. Tests cover Wed/Thu-before
  /Thu-at-1700/Thu-after/Fri/Sat/Sun/Mon plus winter EET vs summer EEST.
- `record_promotions(rows, xlsx, json)` — appends to Weekly_Review,
  idempotent on (URL, Date), creates the sheet if missing, generates
  the JSON sidecar for the Apps Script Thursday mailer.

Uses `freezegun` to lock UTC time at known instants so the Athens
timezone math is deterministic.

## What's intentionally NOT covered

These are testable in theory but cost more to mock than they're worth:

- **Scrapers** (149 modules). Tested by the live scraper-health workflow
  which fires Sundays 04:30 Athens.
- **AI review calls** (claude_check, url_gate_gemini, gemini_reviewer).
  The gating ladder lives inside `main()` entangled with API calls.
  Refactoring to extract a pure `advance_gap_states(rows) → rows`
  function would be required for clean tests; not done yet.
- **GitHub Actions / Apps Script integration**. Tested by the Apps
  Script editor + manual `dryRunNow()`.
- **Report HTML rendering** (build_weekly_report_afts, build_monthly_report_afts).
  Visual regression — would need golden-file comparison + a way to
  ignore date stamps. Not worth it; manual review on each Friday
  catches drift.
- **Mailer (.gs files)**. Apps Script has no offline test runner that
  doesn't require building a full V8 + Apps Script API mock. Lock-step
  with Apps Script editor's debugger.

## Adding new tests

Naming: `tests/test_<module_under_test>.py`. Inside, use class names
that group related tests (`TestNormalizeUrl`, `TestStickyStampRule`)
and method names that describe behavior (`test_friday_rolls_to_next_week`).

When adding a fixture, put it in `tests/conftest.py` so all test files
can use it without imports.

Slow tests (filesystem I/O, network) get `@pytest.mark.slow` so they
can be skipped during fast TDD loops with `pytest -m "not slow"`.

## CI integration (not yet wired)

Once the suite is green, add a `.github/workflows/tests.yml`:

```yaml
name: tests
on: [push, pull_request]
jobs:
  pytest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements.txt -r requirements-dev.txt
      - run: pytest
```

Then add a branch protection rule requiring this check before merge.
That's the staging gate item 3 of the audit roadmap.
