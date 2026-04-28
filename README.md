# FSIS 7-day lookback audit + cleanup bundle

Audit 2026-04-28: standardise all AI gap-finders and primary scrapers to a 7-day
lookback window, with the Sunday Gemini QA at 14 days as the back-publishing
safety net.

---

## Part 1 — Replace 5 files (the lookback fix)

Drop-in replacements; all changes are single-token edits (5→7 or 30→7).

| File | Change |
|---|---|
| `.github/workflows/daily-scrape.yml` | `SINCE_DAYS` default `30` → `7` |
| `pipeline/run_all.py` | `SINCE_DAYS` default `30` → `7` (also docstring) |
| `pipeline/gap_finder_gemini.py` | `SINCE_DAYS` default `5` → `7` |
| `pipeline/gap_finder_tavily.py` | `SINCE_DAYS` default `5` → `7` |
| `pipeline/gap_finder_exa.py` | `SINCE_DAYS` default `5` → `7` |

### Deploy via Windows xcopy

```cmd
set SRC=C:\Users\georg\Downloads\fsis-7day-audit-bundle
set DST=C:\Users\georg\Food-Safety-Intelligence-System
xcopy /Y "%SRC%\.github\workflows\*" "%DST%\.github\workflows\"
xcopy /Y "%SRC%\pipeline\*"          "%DST%\pipeline\"
cd %DST%
python -m compileall pipeline\run_all.py pipeline\gap_finder_gemini.py pipeline\gap_finder_tavily.py pipeline\gap_finder_exa.py
git add .github/workflows/daily-scrape.yml pipeline/run_all.py pipeline/gap_finder_gemini.py pipeline/gap_finder_tavily.py pipeline/gap_finder_exa.py
git commit -m "audit 2026-04-28: standardise all AI gap-finders + scrapers to 7-day lookback"
git push
```

---

## Part 2 — Delete 3 dead duplicate files (the cleanup)

These files exist in your repo but no workflow loads them. Workflows call the
canonical `pipeline/` versions only. Removing them prevents future confusion if
anyone hand-edits the wrong file.

| File | Status | Workflow that calls it |
|---|---|---|
| `gap_finder_gemini.py` (root) | DEAD — 393 lines | none — `gemini-gap-finder.yml` calls `pipeline.gap_finder_gemini` |
| `reports/gap_finder_tavily.py` | DEAD — 399 lines | none — `tavily-gap-finder.yml` calls `pipeline.gap_finder_tavily` |
| `review/gap_finder_tavily.py` | DEAD — 515 lines | none — same as above |

### Delete via Windows cmd

```cmd
cd %DST%
git rm gap_finder_gemini.py
git rm reports/gap_finder_tavily.py
git rm review/gap_finder_tavily.py
git commit -m "cleanup: remove dead duplicate gap-finder files (canonical lives in pipeline/)"
git push
```

If you'd rather keep them locally and just stop tracking, swap `git rm` for
`git rm --cached`.

---

## Part 3 — Tavily snippet-regex fix (Ghirardelli case)

**Already in the file you uploaded.** Your `pipeline/gap_finder_tavily.py`
modified at 19:11 today already contains:

- Broader FDA queries (`site:fda.gov "voluntarily recalls" OR "issues recall"`,
  plus the canonical `site:fda.gov/safety/recalls-market-withdrawals-safety-alerts`
  path)
- `IMPLICIT_HAZARD_PATTERNS` list catching FDA boilerplate ("potential
  foodborne illness", "may contain X", "undeclared allergen", "potential
  health hazard", etc.)
- Inline comments referencing "audit 2026-04-28: a Ghirardelli Salmonella
  recall on FDA was dropped"

The bundle's `gap_finder_tavily.py` includes this fix automatically since
it's a drop-in copy of your edited file (with the `SINCE_DAYS 5→7` change
on top). If you've already committed your Tavily fix, the bundle is just
the lookback edit. If you haven't committed yet, both ship together.

---

## Verification after deploy

The Sunday Gemini QA at 14 days is your back-publishing safety net (any
recall posted >7 days after its actual publication date gets caught
weekly). Check it's still at 14:

```cmd
findstr /C:"QA_DAYS" .github\workflows\gemini-sunday-qa.yml
```

Expected output:
```
QA_DAYS:           ${{ github.event.inputs.days || '14' }}
```

Then the next morning-critical-scrape (04:00 Athens) and the next
gap-finder runs (gemini at 03:00, tavily at 22:00, etc.) will use the new
7-day window automatically.
