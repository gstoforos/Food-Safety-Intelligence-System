# Tier Classifier — Shadow Mode Comparison — 2026-06-22

Compared the trained MiniLM tier classifier against Claude's stored Tier decisions on **200 recent Recalls rows**. Production data unchanged — read-only analysis.

## Headline

- **Overall agreement with Claude**: 85.5% (171/200)
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
| Tier 1 | 154 | 90.9% | 0.730 |
| Tier 2 | 40 | 77.5% | 0.566 |
| Tier 3 | 6 | 0.0% | 0.751 |

## Confusion matrix

Rows = Claude's stored Tier, columns = model's prediction:

```
              Pred T1   Pred T2   Pred T3
  True T1       140        14         0
  True T2         9        31         0
  True T3         6         0         0
```

## Confidence distribution

| Confidence | Count | % |
|---|---:|---:|
| 0.00–0.50 | 2 | 1.0% |
| 0.50–0.70 | 44 | 22.0% |
| 0.70–0.85 | 154 | 77.0% |
| 0.85–0.95 | 0 | 0.0% |
| 0.95–1.01 | 0 | 0.0% |

## High-confidence disagreements (0)

Cases where model is ≥0.85 confident but disagrees with Claude. These are the most interesting — either the model is wrong (training data shortcoming) or Claude was wrong (silent labeling error in Recalls).

_None — model never disagreed at high confidence._ ✓

## Low-confidence cases (200)

Cases where model's max probability is <0.85. These would fall through to Claude/Gemini API in production.

Breakdown by stored Tier:

- Tier 1: 154 low-confidence rows
- Tier 2: 40 low-confidence rows
- Tier 3: 6 low-confidence rows

Top 15 lowest-confidence cases:

| # | Stored | Pred | Conf | Pathogen | URL |
|---:|:---:|:---:|---:|---|---|
| 1 | T1 | T2 | 0.469 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/843898 |
| 2 | T2 | T2 | 0.494 | Aflatoxins | https://webgate.ec.europa.eu/rasff-window/screen/notification/817723 |
| 3 | T2 | T1 | 0.516 | Aflatoxins | https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-ca |
| 4 | T2 | T2 | 0.519 | Ochratoxin | https://webgate.ec.europa.eu/rasff-window/screen/notification/844452 |
| 5 | T2 | T2 | 0.519 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/840191 |
| 6 | T2 | T2 | 0.520 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/837477 |
| 7 | T2 | T2 | 0.521 | Ochratoxin | https://webgate.ec.europa.eu/rasff-window/screen/notification/838147 |
| 8 | T2 | T2 | 0.521 | Aflatoxin | https://webgate.ec.europa.eu/rasff-window/screen/notification/850992 |
| 9 | T2 | T2 | 0.521 | Aflatoxin | https://webgate.ec.europa.eu/rasff-window/screen/notification/841142 |
| 10 | T1 | T2 | 0.521 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/2026.454 |
| 11 | T2 | T2 | 0.521 | Ochratoxin | https://webgate.ec.europa.eu/rasff-window/screen/notification/845771 |
| 12 | T2 | T2 | 0.521 | Aflatoxin | https://webgate.ec.europa.eu/rasff-window/screen/notification/835929 |
| 13 | T2 | T2 | 0.521 | Aflatoxin | https://webgate.ec.europa.eu/rasff-window/screen/notification/839010 |
| 14 | T2 | T2 | 0.521 | Norovirus | https://webgate.ec.europa.eu/rasff-window/screen/notification/850982 |
| 15 | T2 | T2 | 0.521 | Aflatoxin | https://webgate.ec.europa.eu/rasff-window/screen/notification/836380 |

## Recommendation

⚠ **Not yet ready.** High-confidence accuracy is 0.0% — below 90%. Inspect the high-confidence disagreements above. Most likely fix: add the disagreement cases to training data and re-train.
