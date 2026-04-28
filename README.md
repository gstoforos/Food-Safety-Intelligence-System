# AFTS FSIS Automation Fix

This bundle applies the full automation you described:

- **Scrapers 2×/day Athens time** — 08:00 (critical-12) + 17:00 (full 66)
- **URL gate every 4h** — kept as-is (interval, no timezone concern)
- **Merge master hourly** (Pending → Recalls) — **new workflow**
- **News feed hourly** + **7-day rolling purge** of the NEWS sheet
- **Weekly report Friday 08:00 Athens** — HTML + PDF committed, live on index by 10:00, email via Google Apps Script 12:00 Athens (external — not this repo)
- **Monthly report 1st of month 09:00 Athens** — gap-fill (builds only months missing from `monthly-index.json`, never overwrites); produces subscriber HTML + George's PDF; sets `pdf_url` so hub auto-renders new month cards

All scheduled workflows use the **two-line DST cron + Athens hour-guard** pattern so they always fire at the intended Athens hour year-round.

---

## What's in this bundle

```
AFTS-FIX/
├── README.md                    ← this file
├── cleanup.sh                   ← deletes all scrambled root-level dupes
├── requirements-additions.txt   ← append to requirements.txt (weasyprint)
├── .github/workflows/
│   ├── afts-monthly-report.yml     REPLACE
│   ├── afts-weekly-report.yml      REPLACE
│   ├── daily-scrape.yml            REPLACE (17:00 Athens)
│   ├── morning-critical-scrape.yml REPLACE (08:00 Athens)
│   ├── news-feed.yml               REPLACE (adds 7-day purge)
│   └── merge-master.yml            NEW     (hourly Pending→Recalls)
└── pipeline/
    ├── build_missing_monthly_reports.py  NEW gap-fill orchestrator
    ├── build_missing_weekly_reports.py   NEW gap-fill orchestrator
    ├── html_to_pdf.py                    NEW PDF generator (WeasyPrint)
    ├── set_pdf_urls.py                   NEW patches monthly-index.json
    └── purge_old_news.py                 NEW 7-day retention for NEWS sheet
```

---

## Apply steps (in order)

### 1. Clean up the scrambled repo

```bash
cd ~/Food-Safety-Intelligence-System
bash /path/to/AFTS-FIX/cleanup.sh
git status                      # review what's staged for deletion
git commit -m "chore: remove scrambled duplicates (root -> subdirs)"
git push
```

The cleanup removes:
- 63 agency `.py` files at root (duplicates of `scrapers/*.py`)
- 22 scrambled `.py` at root (HTML content in `.py` names, or duplicates of `pipeline/`/`reports/`/`tools/`)
- 9 rename-artifact files inside `scrapers/` (`__init__ (1).py` …)
- 8 HTML + 1 PDF duplicates at root (canonical in `docs/`)
- 6 JSON duplicates at root (canonical in `docs/data/`)
- 8 workflow-YAML duplicates at root (canonical in `.github/workflows/`)
- Entire stale `data/` directory (content moved to `docs/data/` first)

### 2. Drop in the new workflows + pipeline scripts

```bash
cp -r /path/to/AFTS-FIX/.github/workflows/*.yml .github/workflows/
cp /path/to/AFTS-FIX/pipeline/*.py pipeline/

git add .github/workflows/ pipeline/
git commit -m "feat: Athens-time automation + gap-fill + PDF pipeline"
git push
```

### 3. Update requirements.txt

Append `weasyprint==63.1` to your `requirements.txt`:

```bash
cat /path/to/AFTS-FIX/requirements-additions.txt >> requirements.txt
git add requirements.txt
git commit -m "deps: add weasyprint for PDF generation"
git push
```

### 4. Verify the workflow file paths exist in your repo

The monthly workflow calls `python docs/build_monthly_report_afts.py` and the weekly workflow calls `python docs/build_weekly_report_afts.py`. **Verify those two files exist with real Python content in your real repo.** In the zip you gave me they were scrambled (HTML in `.py` filenames, or content swapped between files), but the workflows assume the canonical paths. Quick check:

```bash
head -3 docs/build_monthly_report_afts.py docs/build_weekly_report_afts.py
```

Both should start with `"""` (a Python docstring), not `<!DOCTYPE html>`.

### 5. `scrapers/news.py` is empty (0 bytes)

In the zip you uploaded, `scrapers/news.py` is a 0-byte file — the news-feed workflow runs it every hour but it does nothing. Verify this in your real repo:

```bash
wc -l scrapers/news.py
```

If it's 0 in your repo too, restore it from a prior good commit (`git log -- scrapers/news.py` to find when it was last non-empty) or re-author. The new `news-feed.yml` still adds the 7-day purge step, so even if `news.py` is broken, expired NEWS rows get cleaned up.

---

## Schedule summary (all Athens time)

| Workflow | When | What |
|---|---|---|
| `morning-critical-scrape.yml` | Daily 08:00 | 12 critical agencies → Pending |
| `fsis-url-guardian.yml` | Every 4h | URL validation |
| `merge-master.yml` | Every hour (:05) | Pending → Recalls |
| `news-feed.yml` | Every hour (:00) | Fetch news + purge >7d |
| `daily-scrape.yml` | Daily 17:00 | Full 66 agencies → Pending |
| `afts-weekly-report.yml` | Fridays 08:00 | Build HTML+PDF+AI review, commit by ~10:00 |
| Apps Script mailer (external) | Fridays 12:00 | Read weekly summary JSON, email |
| `afts-monthly-report.yml` | 1st of month 09:00 | Gap-fill missing months, HTML+PDF+hub |

---

## How the gap-fill works

**Monthly (`pipeline/build_missing_monthly_reports.py`)**:
1. Reads `docs/data/monthly-index.json`
2. Lists every month from `2026-03` through the previous-closed month
3. Skips months already covered (either with real `total` stats or a legacy manual `pdf_url`)
4. Builds only the missing ones — calls `python docs/build_monthly_report_afts.py --month-end <last-day-of-month>`
5. Then `html_to_pdf.py` produces the PDF, `set_pdf_urls.py` wires `pdf_url` into the index, hub auto-renders

**Weekly (`pipeline/build_missing_weekly_reports.py`)**:
1. Scans `docs/20*-W*.html` to see what's already present
2. `PRESERVED_WEEKS = {(2026, 15), (2026, 16)}` — never touched (George's manual versions)
3. The week ending on Friday's Sunday is ALWAYS rebuilt (fresh weekly run)
4. Prior missing weeks get filled in (never overwrites an existing one except the current)

Both orchestrators are idempotent — rerunning them mid-day or after a failure does the right thing.

---

## Verification I ran

Every Python file compiles (`python3 -m py_compile`). Every YAML parses. I ran the gap-fill orchestrators against your real `docs/data/monthly-index.json` and confirmed:

- Today (Apr 21): no months need building (March covered, April not yet closed) ✓
- Simulated May 1: builds April only, skips March ✓
- Friday Apr 24: rebuilds W17, skips W15 + W16 (preserved) ✓
- Friday May 1: rebuilds W18, skips W17 (auto-built prior week), skips W15 + W16 ✓

`set_pdf_urls.py` test: after simulating April's HTML+PDF build, it correctly set `pdf_url` for M04 to the GitHub Pages URL and left M01/M02/M03 Wix URLs untouched.

`purge_old_news.py` dry-run on your real NEWS sheet: 6 rows (Apr 10–14) would be deleted, 17 rows (Apr 14–20) kept.

---

## One thing I did NOT touch

- Your actual weekly builder (`docs/build_weekly_report_afts.py` in the real repo)
- Your actual monthly builder (`docs/build_monthly_report_afts.py` in the real repo)
- `docs/hub.html`, `docs/index.html`, `docs/alerts.html`
- The Google Apps Script mailer (it lives in Apps Script, not in this repo)
- Your scrapers in `scrapers/`

Builders already write `docs/data/monthly-summary-latest.json` and `docs/data/weekly-summary-latest.json` with an `email_html` field — that's what the Apps Script mailer reads to send the Friday 12:00 email. No changes needed there.
