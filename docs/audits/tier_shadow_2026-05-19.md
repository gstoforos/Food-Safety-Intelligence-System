# Tier Classifier — Shadow Mode Comparison — 2026-05-19

Compared the trained MiniLM tier classifier against Claude's stored Tier decisions on **200 recent Recalls rows**. Production data unchanged — read-only analysis.

## Headline

- **Overall agreement with Claude**: 92.0% (184/200)
- **High-confidence agreement** (≥0.50): **83.3%** (45/54)
- **Calls that would skip API** (high-conf): **27.0%** of total
- **Calls that fall back to API** (low-conf): 73.0% of total

## Cost projection (assuming this sample is representative)

If your pipeline processes ~30 tier-check calls/day:

| | Without model | With model |
|---|---:|---:|
| API calls / day | 30 | 22 (73%) |
| API calls / year | 10,950 | 7994 |
| Reduction | — | **27%** |

## Per-tier breakdown

| Tier | Sample | Agreement | Avg confidence |
|---|---:|---:|---:|
| Tier 1 | 149 | 93.3% | 0.488 |
| Tier 2 | 47 | 95.7% | 0.629 |
| Tier 3 | 4 | 0.0% | 0.482 |

## Confusion matrix

Rows = Claude's stored Tier, columns = model's prediction:

```
              Pred T1   Pred T2   Pred T3
  True T1       139        10         0
  True T2         2        45         0
  True T3         4         0         0
```

## Confidence distribution

| Confidence | Count | % |
|---|---:|---:|
| 0.00–0.50 | 146 | 73.0% |
| 0.50–0.70 | 54 | 27.0% |
| 0.70–0.85 | 0 | 0.0% |
| 0.85–0.95 | 0 | 0.0% |
| 0.95–1.01 | 0 | 0.0% |

## High-confidence disagreements (9)

Cases where model is ≥0.50 confident but disagrees with Claude. These are the most interesting — either the model is wrong (training data shortcoming) or Claude was wrong (silent labeling error in Recalls).

| # | Stored | Pred | Conf | Pathogen | Product | URL |
|---:|:---:|:---:|---:|---|---|---|
| 1 | T1 | T2 | 0.595 | Salmonella | Pistachio nuts | https://webgate.ec.europa.eu/rasff-window/screen/notification/842548 |
| 2 | T1 | T2 | 0.637 | Salmonella | Salmonella and ochratoxin A in pistachio | https://webgate.ec.europa.eu/rasff-window/screen/notification/2026.401 |
| 3 | T1 | T2 | 0.638 | Salmonella | Salmonella spp. in salted and dried fish | https://webgate.ec.europa.eu/rasff-window/screen/notification/842255 |
| 4 | T1 | T2 | 0.519 | Salmonella | Grape & Berry Medley 230 g | https://www.food.gov.uk/news-alerts/alert/fsa-prin-10-2026 |
| 5 | T1 | T2 | 0.634 | Salmonella | Salmonella Infantis in frozen chicken mi | https://webgate.ec.europa.eu/rasff-window/screen/notification/843447 |
| 6 | T1 | T2 | 0.589 | Salmonella | Bio-Power Mix | https://www.blv.admin.ch/dam/blv/de/dokumente/oeffentliche-warnungen/o |
| 7 | T1 | T2 | 0.636 | Salmonella | Salmonella Stanley in instant noodle mea | https://webgate.ec.europa.eu/rasff-window/screen/notification/843316 |
| 8 | T1 | T2 | 0.589 | Salmonella | Various Western Brand chicken products ( | https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-various-west |
| 9 | T1 | T2 | 0.614 | Salmonella | Dried herb mix | https://www.lebensmittelwarnung.de/bvl-lmw-de/detail/lebensmittel/9333 |

## Low-confidence cases (146)

Cases where model's max probability is <0.50. These would fall through to Claude/Gemini API in production.

Breakdown by stored Tier:

- Tier 1: 140 low-confidence rows
- Tier 2: 2 low-confidence rows
- Tier 3: 4 low-confidence rows

Top 15 lowest-confidence cases:

| # | Stored | Pred | Conf | Pathogen | URL |
|---:|:---:|:---:|---:|---|---|
| 1 | T1 | T1 | 0.372 | Cereulide (B. cereus toxin) | https://webgate.ec.europa.eu/rasff-window/screen/notification/836861 |
| 2 | T1 | T1 | 0.456 | Salmonella spp. | https://recalls-rappels.canada.ca/en/alert-recall/various-pistachios-a |
| 3 | T1 | T1 | 0.464 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/829727 |
| 4 | T1 | T1 | 0.466 | Salmonella | https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/su |
| 5 | T1 | T1 | 0.467 | Listeria monocytogenes | https://www.fsis.usda.gov/recalls-alerts/fsis-issues-public-health-ale |
| 6 | T1 | T1 | 0.468 | Shiga toxin-producing E. coli (STEC) | https://webgate.ec.europa.eu/rasff-window/screen/notification/841044 |
| 7 | T1 | T1 | 0.469 | Salmonella | https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/jo |
| 8 | T1 | T1 | 0.470 | Shiga toxin-producing E. coli (STEC) | https://webgate.ec.europa.eu/rasff-window/screen/notification/841107 |
| 9 | T1 | T1 | 0.470 | Listeria monocytogenes | https://webgate.ec.europa.eu/rasff-window/screen/notification/844028 |
| 10 | T1 | T1 | 0.470 | Norovirus | https://www.fda.gov/food/alerts-advisories-safety-information/fda-advi |
| 11 | T1 | T1 | 0.472 | Salmonella | https://recalls-rappels.canada.ca/en/alert-recall/ghirardelli-brand-po |
| 12 | T1 | T1 | 0.473 | Shiga toxin-producing E. coli (STEC) | https://webgate.ec.europa.eu/rasff-window/screen/notification/840167 |
| 13 | T1 | T1 | 0.473 | Shiga toxin-producing E. coli (STEC) | https://webgate.ec.europa.eu/rasff-window/screen/notification/842861 |
| 14 | T1 | T1 | 0.474 | Listeria monocytogenes | https://webgate.ec.europa.eu/rasff-window/screen/notification/840843 |
| 15 | T1 | T1 | 0.474 | Shiga toxin-producing E. coli (STEC) | https://rappel.conso.gouv.fr/fiche-rappel/22129/Interne |

## Recommendation

⚠ **Not yet ready.** High-confidence accuracy is 83.3% — below 90%. Inspect the high-confidence disagreements above. Most likely fix: add the disagreement cases to training data and re-train.
