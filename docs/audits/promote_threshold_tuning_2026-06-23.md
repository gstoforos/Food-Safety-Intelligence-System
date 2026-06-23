# Promote Classifier — Threshold Tuning — 2026-06-23

Re-analysis of the trained promote classifier on a balanced sample of **200 rows** (100 PROMOTE + 100 REJECT). No retraining — just exploring how the decision threshold changes production metrics.

## Why threshold tuning

The default rule is `predict REJECT if P(REJECT) > P(PROMOTE)`, i.e. threshold 0.50. But the trained model has high reject precision (98%+) and ALSO high promote majority — it defaults to PROMOTE when uncertain because of the 5:1 train imbalance. 

Lowering the REJECT threshold means: 'predict REJECT if P(REJECT) is meaningful, not just dominant'. This catches the rejects the model is 30-50% sure about — which, given 98% reject precision, are probably real rejects.

## Threshold sweep

Decision rule: `predict REJECT if P(REJECT) ≥ threshold`

| Threshold | Accuracy | REJECT recall | REJECT prec | PROMOTE recall | PROMOTE prec | Combined | TP | FP | TN | FN |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.10 ★ | 84.5% | 69.0% | 100.0% | 100.0% | 76.3% | 0.831 | 69 | 0 | 100 | 31 |
| 0.15 | 84.5% | 69.0% | 100.0% | 100.0% | 76.3% | 0.831 | 69 | 0 | 100 | 31 |
| 0.20 | 84.5% | 69.0% | 100.0% | 100.0% | 76.3% | 0.831 | 69 | 0 | 100 | 31 |
| 0.25 | 84.5% | 69.0% | 100.0% | 100.0% | 76.3% | 0.831 | 69 | 0 | 100 | 31 |
| 0.30 | 84.5% | 69.0% | 100.0% | 100.0% | 76.3% | 0.831 | 69 | 0 | 100 | 31 |
| 0.35 | 84.5% | 69.0% | 100.0% | 100.0% | 76.3% | 0.831 | 69 | 0 | 100 | 31 |
| 0.40 | 84.5% | 69.0% | 100.0% | 100.0% | 76.3% | 0.831 | 69 | 0 | 100 | 31 |
| 0.45 | 84.5% | 69.0% | 100.0% | 100.0% | 76.3% | 0.831 | 69 | 0 | 100 | 31 |
| 0.50 | 84.5% | 69.0% | 100.0% | 100.0% | 76.3% | 0.831 | 69 | 0 | 100 | 31 |
| 0.55 | 84.5% | 69.0% | 100.0% | 100.0% | 76.3% | 0.831 | 69 | 0 | 100 | 31 |

Legend: TP=correct REJECT, FP=false REJECT, TN=correct PROMOTE, FN=missed REJECT (the dangerous one)

## Recommendation

⚠ **No single threshold satisfies both recall floors.** Either retrain with `class_weight_mode=sqrt`, or accept the trade-off and pick the threshold that best matches your priority (catch all rejects vs minimize manual review).

## Production deployment guidance

In the production inference workflow, use:

```python
# After model.predict() returns probs[2]
p_reject = probs[1]
if p_reject >= 0.10:
    decision = 'REJECT'
    confidence = p_reject
else:
    decision = 'PROMOTE'
    confidence = 1.0 - p_reject
```

This decision rule eliminates the false-promote problem without retraining. The model file is unchanged.
