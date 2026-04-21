# Food Safety Intelligence System (FSIS) — Pipeline

Automated daily monitoring of **66 food safety regulatory agencies across 60+ countries**.
Detects pathogen-related food recalls, enriches with AI, deduplicates against the master
list, and commits results to GitHub for the live web dashboard.

**Built by:** Advanced Food-Tech Solutions (AFTS) — advfood.tech

---

## How it works

```
                    ┌─────────────────────────────────────────┐
                    │  GitHub Actions cron — daily 17:00 UTC  │
                    └────────────────┬────────────────────────┘
                                     ▼
       ┌──────────────────────────────────────────────────────────┐
       │ pipeline/run_all.py — orchestrator                       │
       │                                                          │
       │  1. Load existing data/recalls.xlsx (197 seed records)   │
       │  2. Discover 66 scraper classes auto from scrapers/*     │
       │  3. Run all in parallel (8 threads)                      │
       │  4. Enrich with Gemini (free tier, key rotation)         │
       │  5. Merge into seed (dedup by URL primary)               │
       │  6. Validate every URL (HEAD check, generic detection)   │
       │  7. Review with OpenAI (paid, full pass)                 │
       │  8. Review with Claude Haiku (free $5, Tier-1 only)      │
       │  9. Apply suggested fixes                                │
       │ 10. Save xlsx + json, preserve NEWS sheet                │
       │ 11. Git commit + push                                    │
       └──────────────────────────────────────────────────────────┘
```

## Coverage — 66 agencies

| Region | Count | Agencies |
|--------|-------|----------|
| North America | 3 | FDA, USDA FSIS, CFIA |
| EU member states | 24 | RappelConso (FR), BVL (DE), Salute (IT), AESAN (ES), EFET (GR), AFSCA (BE), NVWA (NL), ASAE (PT), FSAI (IE), AGES (AT), Livsmedelsverket (SE), Fødevarestyrelsen (DK), Ruokavirasto (FI), GIS (PL), SZPI (CZ), Nébih (HU), ANSVSA (RO), ŠVPS (SK), UVHVVR (SI), HAH (HR), BFSA (BG), VMVT (LT), PVD (LV), VTA (EE) |
| Non-EU Europe | 4 | FSA (UK), BLV (CH), Mattilsynet (NO), MAST (IS) |
| EU-wide | 1 | RASFF Window |
| Asia | 12 | MHLW (JP), MFDS (KR), SAMR (CN), CFS (HK), SFA (SG), TFDA (TW), FSSAI (IN), Thai FDA, VFA (VN), KKM (MY), BPOM (ID), FDA (PH) |
| Oceania | 2 | FSANZ (AU), MPI (NZ) |
| Africa | 7 | NCC (ZA), KEBS (KE), NAFDAC (NG), NFSA (EG), ONSSA (MA), FDA (GH), COMESA |
| Latin America | 8 | ANVISA (BR), ANMAT (AR), COFEPRIS (MX), ISP (CL), INVIMA (CO), DIGESA (PE), ARCSA (EC), MSP (UY) |
| Middle East | 5 | MoH (IL), MoCCAE (AE), SFDA (SA), MoPH (QA), TGTHB (TR) |

**7 custom scrapers** (FDA openFDA API, USDA FSIS API, CFIA RSS, RappelConso open data API, FSA UK JSON, FSANZ HTML, RASFF Window) +  
**59 generic scrapers** that fetch agency pages and use Gemini to extract structured rows.

---

## Setup

### 1. Add the pipeline files to your repo

Drop everything from this package into `gstoforos/Food-Safety-Intelligence-System/`:

```
.github/workflows/daily-scrape.yml
scrapers/                         (66 scraper modules)
enrichment/                       (Gemini client + row enrichment)
review/                           (OpenAI + Claude + URL validator)
pipeline/                         (orchestrator + merge + git push)
data/recalls.xlsx                 (your existing 197 records — preserved as seed)
data/recalls.json
requirements.txt
.env.example
```

### 2. Add API keys to GitHub Secrets

Go to **Settings → Secrets and variables → Actions → New repository secret** and add:

**Required (for scraping to work):**
- `GEMINI_API_KEY_1` — get free at https://aistudio.google.com/apikey
  - Each key = 1,500 req/day on free tier
  - For full safety with 66 scrapers, add up to 5 keys: `GEMINI_API_KEY_2`, `GEMINI_API_KEY_3`, etc.
  - The pipeline rotates keys round-robin and skips rate-limited ones automatically

**Required (for review pass):**
- `OPENAI_API_KEY` — your existing key from https://platform.openai.com/api-keys
  - Cost: ~$0.005/recall reviewed × ~50 new recalls/day = ~$0.25/day = **~$8/month**

**Optional (3rd reviewer for Tier-1 critical only):**
- `ANTHROPIC_API_KEY` — get $5 free credit at https://console.anthropic.com
  - Throttled to Tier-1 rows only ($5 lasts ~50 days)
  - Leave empty to skip

`GITHUB_TOKEN` is automatic — no setup needed.

### 3. Test locally first (optional but recommended)

```bash
git clone https://github.com/gstoforos/Food-Safety-Intelligence-System.git
cd Food-Safety-Intelligence-System

python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env, add your keys

# Quick test (no commit, AI off)
SKIP_AI=true SKIP_REVIEW=true SKIP_COMMIT=true SINCE_DAYS=7 \
  python -m pipeline.run_all

# Full dry run (with AI, no commit)
SKIP_COMMIT=true SINCE_DAYS=7 python -m pipeline.run_all
```

### 4. Trigger first run

In GitHub: **Actions tab → "FSIS Daily Scrape" → Run workflow → Run**

After this, it runs automatically every day at 17:00 UTC (5 PM UTC).

---

## Configuration knobs

All set via env vars (in workflow YAML or .env locally):

| Variable | Default | Effect |
|----------|---------|--------|
| `SINCE_DAYS` | 30 | How far back each scraper looks |
| `MAX_PARALLEL` | 8 | Parallel scraper threads |
| `SKIP_AI` | false | Disable Gemini (debug mode) |
| `SKIP_REVIEW` | false | Disable OpenAI/Claude review |
| `SKIP_COMMIT` | false | Don't push to GitHub (local testing) |
| `GEMINI_MODEL` | gemini-2.0-flash | Free tier model |
| `OPENAI_MODEL` | gpt-4o-mini | Cheap review model |
| `ANTHROPIC_MODEL` | claude-haiku-4-5-20251001 | Cheap reviewer |

---

## Adding a new scraper

For most agencies, just create a 10-line file:

```python
# scrapers/asia/new_agency.py
from scrapers._base import GenericGeminiScraper

class NewAgencyScraper(GenericGeminiScraper):
    AGENCY = "Agency Name (XX)"
    COUNTRY = "Country Name"
    INDEX_URLS = ["https://agency.gov/recalls"]
    LANGUAGE = "en"   # or "es", "fr", "de", "ja", etc.
```

The orchestrator auto-discovers it on next run. No need to edit any other files.

For agencies with clean APIs (like FDA openFDA), subclass `BaseScraper` and implement `scrape()` directly — see `scrapers/north_america/fda.py` for an example.

---

## Schema (data/recalls.xlsx, sheet "Recalls")

14 columns — never change without updating `pipeline/merge_master.py:SCHEMA`:

| # | Column | Type | Notes |
|---|--------|------|-------|
| 1 | Date | str | YYYY-MM-DD |
| 2 | Source | str | e.g. "FDA", "RappelConso (FR)" |
| 3 | Company | str | Firm/producer |
| 4 | Brand | str | Commercial brand, "—" if none |
| 5 | Product | str | Full product description |
| 6 | Pathogen | str | Canonical name (e.g. "Listeria monocytogenes") |
| 7 | Reason | str | Cause description |
| 8 | Class | str | "Recall", "Alert", "Class I/II/III", etc. |
| 9 | Country | str | English country name |
| 10 | Region | str | "Europe", "North America", etc. |
| 11 | Tier | int | 1 = critical (Listeria/STEC/Botulinum/biotoxin/cereulide/HepA/outbreak), 2 = standard |
| 12 | Outbreak | int | 1 if illness/outbreak mentioned, else 0 |
| 13 | URL | str | Deep-link to specific recall page |
| 14 | Notes | str | Distribution, batch IDs, additional context |

Sheet "NEWS" is preserved by the pipeline but populated by the separate `news.gs` Apps Script.

---

## Cost estimates

- **Gemini free tier**: 1,500 req/day per key. 5 keys = 7,500/day, more than enough.
- **OpenAI gpt-4o-mini**: ~$0.005/recall × 50 new/day = **~$0.25/day = ~$8/month**.
- **Claude Haiku 4.5**: $5 free credit, throttled to ~10 Tier-1 rows/day = **~$0.05/day**, $5 lasts ~3 months.
- **GitHub Actions**: Free for public repos, free 2000 min/month for private.

**Total monthly cost: ~$8 (OpenAI) + $0 if Gemini/Claude on free tiers.**

---

## Troubleshooting

**"No GEMINI_API_KEY_* found in env"**
→ Add at least one `GEMINI_API_KEY_1` to GitHub Secrets, or to `.env` locally.

**Workflow times out**
→ Reduce `SINCE_DAYS` (look back fewer days) or `MAX_PARALLEL`.

**A specific scraper fails**
→ Check Actions log. If a single agency's page format changed, the GenericGeminiScraper usually adapts. For persistent failures, you may need to add `EXTRACTION_HINTS` to that scraper's class.

**"Git push failed: 403"**
→ Check workflow has `permissions: contents: write` (it does in our YAML).

**Existing 197 records got modified or lost**
→ Pipeline is APPEND-ONLY against existing rows. Existing rows are never deleted, only enriched if the URL matches a new scrape. If you suspect data loss, the previous version is in the previous git commit — `git log data/recalls.xlsx`.

---

## Architecture notes

- **Append-only:** Your manually-curated 197 rows are sacred. Pipeline only adds new ones with new URLs.
- **Deduplication:** Primary by URL (lowercase trim). Fallback by `date|normalized_company|pathogen`.
- **Pathogens-only filter:** Hardcoded across all scrapers + Gemini prompts. Allergens, foreign bodies, labeling errors are excluded.
- **URL quality enforcement:** `review/url_validator.py` catches generic landing pages (e.g. `/categorie/`, `/anakleiseis-cat`) before commit.
- **Region inference:** Set automatically from Country via `COUNTRY_REGION` map in `_models.py`.
- **Tier assignment:** Deterministic — Tier 1 = Listeria/STEC/Botulinum/cereulide/biotoxin/HepA/Cronobacter or any outbreak.

---

## Next phase

Once this pipeline runs cleanly for a week (~350 new recalls accumulated), the next build is **reports & statistics models**:

- Pathogen trend forecasting (seasonality, regional clustering)
- Outbreak early warning (alert when N+ recalls of same pathogen in same week)
- Distributor risk scoring (which retailers appear most often)
- Auto-generated weekly newsletter (HTML email)

Ask in the next session: *"build the reports module on top of recalls.xlsx"*.
