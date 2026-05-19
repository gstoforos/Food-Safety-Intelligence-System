# Tier Classifier — Shadow Mode Comparison — 2026-05-19

Compared the trained MiniLM tier classifier against Claude's stored Tier decisions on **200 recent Recalls rows**. Production data unchanged — read-only analysis.

## Headline

- **Overall agreement with Claude**: 90.5% (181/200)
- **High-confidence agreement** (≥0.85): **0.0%** (0/0)
- **Calls that would skip API** (high-conf): **0.0%** of total
- **Calls that fall back to API** (low-conf): 100.0% of total

## Cost projection (assuming this sample is representative)

If your pipeline processes ~30 tier-check calls/day:

| | Without model | With model |
|---|---:|---:|
| API calls / day | 30 | 30 (100%) |
| API calls / year | 10,950 | 10950 |
| Reduction | — | **0%** |

## Per-tier breakdown

| Tier | Sample | Agreement | Avg confidence |
|---|---:|---:|---:|
| Tier 1 | 148 | 98.0% | 0.484 |
| Tier 2 | 43 | 83.7% | 0.602 |
| Tier 3 | 9 | 0.0% | 0.480 |

## Confusion matrix

Rows = Claude's stored Tier, columns = model's prediction:

```
              Pred T1   Pred T2   Pred T3
  True T1       145         3         0
  True T2         7        36         0
  True T3         9         0         0
```

## Confidence distribution

| Confidence | Count | % |
|---|---:|---:|
| 0.00–0.50 | 162 | 81.0% |
| 0.50–0.70 | 38 | 19.0% |
| 0.70–0.85 | 0 | 0.0% |
| 0.85–0.95 | 0 | 0.0% |
| 0.95–1.01 | 0 | 0.0% |

## High-confidence disagreements (0)

Cases where model is ≥0.85 confident but disagrees with Claude. These are the most interesting — either the model is wrong (training data shortcoming) or Claude was wrong (silent labeling error in Recalls).

_None — model never disagreed at high confidence._ ✓

## Low-confidence cases (200)

Cases where model's max probability is <0.85. These would fall through to Claude/Gemini API in production.

Breakdown by stored Tier:

- Tier 1: 148 low-confidence rows
- Tier 2: 43 low-confidence rows
- Tier 3: 9 low-confidence rows

Top 15 lowest-confidence cases:

| # | Stored | Pred | Conf | Pathogen | URL |
|---:|:---:|:---:|---:|---|---|
| 1 | T2 | T1 | 0.412 | Possible incomplete pasteurization (process deviat | https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/sa |
| 2 | T2 | T2 | 0.443 | Coliform / total bacterial count | https://en.sedaily.com/society/2026/03/26/bacteria-found-in-ice-at-caf |
| 3 | T1 | T1 | 0.456 | Listeria monocytogenes | https://webgate.ec.europa.eu/rasff-window/screen/notification/841333 |
| 4 | T1 | T1 | 0.456 | Norovirus | https://www.fda.gov/food/alerts-advisories-safety-information/fda-advi |
| 5 | T1 | T1 | 0.458 | Listeria monocytogenes | https://webgate.ec.europa.eu/rasff-window/screen/notification/843926 |
| 6 | T1 | T1 | 0.464 | Salmonella | https://www.quebec.ca/nouvelles/actualites/details/mise-en-garde-a-la- |
| 7 | T1 | T1 | 0.464 | Listeria monocytogenes | https://webgate.ec.europa.eu/rasff-window/screen/notification/836283 |
| 8 | T1 | T1 | 0.464 | Listeria monocytogenes | https://webgate.ec.europa.eu/rasff-window/screen/notification/836200 |
| 9 | T1 | T1 | 0.465 | Shiga toxin-producing E. coli (STEC) | https://webgate.ec.europa.eu/rasff-window/screen/notification/840838 |
| 10 | T1 | T1 | 0.466 | Salmonella | https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/su |
| 11 | T1 | T1 | 0.467 | Salmonella Enteritidis | https://rappel.conso.gouv.fr/fiche-rappel/21234/Interne |
| 12 | T1 | T1 | 0.469 | Salmonella | https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/jo |
| 13 | T1 | T1 | 0.470 | Shiga toxin-producing E. coli (STEC) | https://webgate.ec.europa.eu/rasff-window/screen/notification/841107 |
| 14 | T1 | T1 | 0.470 | Listeria monocytogenes | https://webgate.ec.europa.eu/rasff-window/screen/notification/836999 |
| 15 | T1 | T1 | 0.472 | Cereulide (B. cereus toxin) | https://webgate.ec.europa.eu/rasff-window/screen/notification/841474 |

## Recommendation

⚠ **Not yet ready.** High-confidence accuracy is 0.0% — below 90%. Inspect the high-confidence disagreements above. Most likely fix: add the disagreement cases to training data and re-train.
