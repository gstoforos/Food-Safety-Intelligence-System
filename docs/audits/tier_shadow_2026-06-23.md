# Tier Classifier — Shadow Mode Comparison — 2026-06-23

Compared the trained MiniLM tier classifier against Claude's stored Tier decisions on **200 recent Recalls rows**. Production data unchanged — read-only analysis.

## Headline

- **Overall agreement with Claude**: 92.5% (185/200)
- **High-confidence agreement** (≥0.85): **96.6%** (140/145)
- **Calls that would skip API** (high-conf): **72.5%** of total
- **Calls that fall back to API** (low-conf): 27.5% of total

## Cost projection (assuming this sample is representative)

If your pipeline processes ~30 tier-check calls/day:

| | Without model | With model |
|---|---:|---:|
| API calls / day | 30 | 8 (28%) |
| API calls / year | 10,950 | 3011 |
| Reduction | — | **72%** |

## Per-tier breakdown

| Tier | Sample | Agreement | Avg confidence |
|---|---:|---:|---:|
| Tier 1 | 154 | 98.1% | 0.878 |
| Tier 2 | 40 | 85.0% | 0.740 |
| Tier 3 | 6 | 0.0% | 0.839 |

## Confusion matrix

Rows = Claude's stored Tier, columns = model's prediction:

```
              Pred T1   Pred T2   Pred T3
  True T1       151         3         0
  True T2         6        34         0
  True T3         3         3         0
```

## Confidence distribution

| Confidence | Count | % |
|---|---:|---:|
| 0.00–0.50 | 5 | 2.5% |
| 0.50–0.70 | 13 | 6.5% |
| 0.70–0.85 | 37 | 18.5% |
| 0.85–0.95 | 145 | 72.5% |
| 0.95–1.01 | 0 | 0.0% |

## High-confidence disagreements (5)

Cases where model is ≥0.85 confident but disagrees with Claude. These are the most interesting — either the model is wrong (training data shortcoming) or Claude was wrong (silent labeling error in Recalls).

| # | Stored | Pred | Conf | Pathogen | Product | URL |
|---:|:---:|:---:|---:|---|---|---|
| 1 | T3 | T1 | 0.898 | Staphylococcus enterotoxin | Fresh raw-milk goat cheese (round) | https://rappel.conso.gouv.fr/fiche-rappel/22009/Interne |
| 2 | T2 | T1 | 0.900 | Escherichia coli | huitres st vaast n°2 – 5kg et n°3 – 6kg | https://rappel.conso.gouv.fr/fiche-rappel/22220/Interne |
| 3 | T3 | T1 | 0.899 | Listeria monocytogenes | Leis Sauerampfer (Sorrel) | https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-j |
| 4 | T2 | T1 | 0.898 | Possible incomplete pasteurization (proc | Cottage Cheese (24 states) | https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/sa |
| 5 | T3 | T1 | 0.899 | Listeria monocytogenes | produits de la pêche et d'aquaculture | https://rappel.conso.gouv.fr/fiche-rappel/22005/Interne |

## Low-confidence cases (55)

Cases where model's max probability is <0.85. These would fall through to Claude/Gemini API in production.

Breakdown by stored Tier:

- Tier 1: 14 low-confidence rows
- Tier 2: 38 low-confidence rows
- Tier 3: 3 low-confidence rows

Top 15 lowest-confidence cases:

| # | Stored | Pred | Conf | Pathogen | URL |
|---:|:---:|:---:|---:|---|---|
| 1 | T1 | T2 | 0.463 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/851868 |
| 2 | T1 | T2 | 0.463 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/843447 |
| 3 | T2 | T1 | 0.469 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/836603 |
| 4 | T2 | T2 | 0.473 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/837926 |
| 5 | T2 | T1 | 0.496 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/838294 |
| 6 | T2 | T1 | 0.547 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/839461 |
| 7 | T2 | T2 | 0.563 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/836057 |
| 8 | T2 | T2 | 0.588 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/837896 |
| 9 | T2 | T2 | 0.588 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/837740 |
| 10 | T1 | T1 | 0.595 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/848007 |
| 11 | T1 | T1 | 0.595 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/848000 |
| 12 | T2 | T2 | 0.637 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/840191 |
| 13 | T1 | T1 | 0.637 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/843576 |
| 14 | T1 | T1 | 0.653 | Marine biotoxin | https://rappel.conso.gouv.fr/fiche-rappel/22157/Interne |
| 15 | T1 | T1 | 0.673 | Salmonella | https://webgate.ec.europa.eu/rasff-window/screen/notification/842725 |

## Recommendation

✅ **Ready for production cutover.** High-confidence accuracy is ≥95% and the model is confident on ≥70% of rows. Deploy with confidence threshold as the API-fallback gate.
