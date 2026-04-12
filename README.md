# FSIS Agent – FDA Module

Automated food safety pathogen recall monitor.  
**Stack:** Python · openFDA API · Gemini 2.0 Flash · GitHub Actions · GitHub Pages

---

## Architecture

```
openFDA API ──► fda_scraper.py ──► SQLite DB
                                       │
                               gemini_enricher.py  (AI classification)
                                       │
                               report_generator.py ──► docs/index.html
                                                              │
                                                       GitHub Pages
                                                              │
                                                       Wix iframe embed
```

---

## Setup (5 minutes)

### 1. Fork / clone this repo

### 2. Add GitHub Secrets
Go to **Settings → Secrets → Actions**:
| Secret | Value |
|---|---|
| `GEMINI_API_KEY` | Your Gemini API key (free at aistudio.google.com) |
| `OPENFDA_API_KEY` | *(optional)* openFDA key for higher rate limits |

### 3. Enable GitHub Pages
- **Settings → Pages → Source:** `Deploy from branch`
- **Branch:** `main` · **Folder:** `/docs`

### 4. First run
Go to **Actions → FSIS Daily Run → Run workflow → full**  
This pulls the last 90 days and generates the first report.

After ~5 minutes your report is live at:
```
https://<your-username>.github.io/<repo-name>/
```

### 5. Wix embed
In Wix Editor → **Add → Embed → HTML iframe**  
Set source URL to your GitHub Pages URL above.  
Resize to your preferred dimensions (recommended: 100% width, 900px height).

---

## Running locally

```bash
pip install -r requirements.txt

export GEMINI_API_KEY=your_key_here
export OPENFDA_API_KEY=your_key_here  # optional

# First run (90-day backfill)
python main.py --full

# Daily incremental
python main.py

# Regenerate report without re-fetching
python main.py --report-only
```

---

## Files

| File | Purpose |
|---|---|
| `config.py` | All settings, pathogen whitelist, palette |
| `database.py` | SQLite schema + query helpers |
| `fda_scraper.py` | openFDA API fetcher + pathogen filter |
| `gemini_enricher.py` | AI enrichment via Gemini 2.0 Flash |
| `report_generator.py` | Self-contained HTML report |
| `main.py` | Orchestrator |
| `.github/workflows/daily.yml` | GitHub Actions cron (06:00 UTC) |
| `docs/index.html` | Auto-generated report (GitHub Pages) |

---

## Extending to other sources

Each new source is one file in a `scrapers/` folder implementing:
```python
def fetch(date_from: str, date_to: str) -> tuple[int, int]:
    # returns (total_fetched, new_inserted)
```
Then add it to `main.py`. Planned: RASFF (EU), FSA UK, FSANZ, CFIA, EFSA.

---

© AFTS – Advanced Food-Tech Solutions · advfood.tech
