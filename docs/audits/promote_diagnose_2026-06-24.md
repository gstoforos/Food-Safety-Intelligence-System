# Promote Classifier — Failure Diagnosis — 2026-06-24

Ran the trained promote classifier against **all 194 live Weekly_Rejected rows** to identify the population of rejects the model fails on.

## Headline

- Live reject recall: **69.6%** (135/194)
- Failures: **59** rows
- Of failures, in training corpus: 7 (12%)
- Of failures, OUT of training corpus: **52** (88%)

> If most failures are OUT of training corpus, the fix is to re-export v2.3 with the new rejects and retrain. If most are IN training corpus, the model needs architectural changes.

## Failures by Source

| Source | Failed | Correct | Recall |
|---|---:|---:|---:|
| AESAN | 20 | 2 | 9.1% |
| AGES | 0 | 1 | 100.0% |
| ASAE | 7 | 0 | 0.0% |
| BVL | 0 | 2 | 100.0% |
| CDC | 1 | 5 | 83.3% |
| CFIA | 0 | 15 | 100.0% |
| CFS | 0 | 3 | 100.0% |
| EFET | 15 | 0 | 0.0% |
| FAVV | 0 | 1 | 100.0% |
| FDA | 0 | 5 | 100.0% |
| FDA PH | 0 | 1 | 100.0% |
| FSA | 0 | 5 | 100.0% |
| FSA (via Google News) | 0 | 1 | 100.0% |
| FSAI | 0 | 4 | 100.0% |
| FSAI (IE) | 0 | 6 | 100.0% |
| FSAI (via Google News) | 0 | 13 | 100.0% |
| FSANZ | 0 | 1 | 100.0% |
| FSANZ (AU) | 0 | 1 | 100.0% |
| FSS (via Google News) | 0 | 1 | 100.0% |
| Livsmedelsverket | 0 | 1 | 100.0% |
| MFDS | 0 | 2 | 100.0% |
| MPI / NZFS | 0 | 4 | 100.0% |
| Min. Salute | 0 | 2 | 100.0% |
| NVWA | 0 | 2 | 100.0% |
| RASFF | 0 | 1 | 100.0% |
| RASFF (EU) | 0 | 18 | 100.0% |
| Rappel Conso | 1 | 2 | 66.7% |
| RappelConso (FR) | 1 | 28 | 96.6% |
| Salute | 14 | 8 | 36.4% |

## Failures by reject category

| Category | Failed | Correct | Recall |
|---|---:|---:|---:|
| date_pre_2026 | 0 | 7 | 100.0% |
| multi_field_mismatch | 1 | 9 | 90.0% |
| not_a_recall_page | 0 | 77 | 100.0% |
| other | 56 | 20 | 26.3% |
| pathogen_out_of_scope | 0 | 6 | 100.0% |
| single_field_mismatch | 2 | 16 | 88.9% |

## Top failed pathogens

| Pathogen | Failures |
|---|---:|
| (empty) | 46 |
| listeria | 3 |
| λιστερια | 2 |
| Listeria monocytogenes | 2 |
| Clostridium botulinum | 1 |
| pesticidi | 1 |
| caseina | 1 |
| non dichiarata | 1 |
| salmonella | 1 |
| plastico | 1 |

## Sample failure rows (top 20 by P(PROMOTE))

These are the rejects the model was MOST confident were promotes — the cleanest mismatches.

| # | Source | Category | P(promote) | In corpus | Pathogen | RejectionReason |
|---:|---|---|---:|:---:|---|---|
| 1 | RappelConso (FR) | single_field_mismatch | 0.961 | Y | Listeria monocytogenes | stoeffler_chorizo_dup: truncated dup of R4 (fiche 22601, same flam'fine Chorizo  |
| 2 | AESAN | other | 0.961 | N |  |  |
| 3 | AESAN | other | 0.961 | Y |  |  |
| 4 | AESAN | other | 0.961 | Y |  |  |
| 5 | AESAN | other | 0.961 | Y |  |  |
| 6 | Rappel Conso | single_field_mismatch | 0.961 | Y | Listeria monocytogenes | stoeffler_chorizo_dup: gap-finder dup of R4 chorizo recall with malformed fiche  |
| 7 | EFET | other | 0.961 | N |  |  |
| 8 | EFET | other | 0.961 | N |  |  |
| 9 | EFET | other | 0.961 | N |  |  |
| 10 | EFET | other | 0.961 | N | λιστερια |  |
| 11 | Salute | other | 0.961 | Y |  |  |
| 12 | EFET | other | 0.961 | N |  |  |
| 13 | CDC | multi_field_mismatch | 0.961 | Y | Clostridium botulinum | cdc_press_release_dupe_REPEATED: 3rd time the gap finder has re-added this CDC m |
| 14 | EFET | other | 0.961 | N |  |  |
| 15 | EFET | other | 0.961 | N |  |  |
| 16 | AESAN | other | 0.961 | N |  |  |
| 17 | EFET | other | 0.961 | N |  |  |
| 18 | AESAN | other | 0.961 | N | listeria |  |
| 19 | AESAN | other | 0.961 | N |  |  |
| 20 | AESAN | other | 0.961 | N | salmonella |  |

## Recommendation

✅ **Drift issue.** Most failures are rejects added to Weekly_Rejected after the training corpus snapshot. Fix: re-export v2.3 (or rerun export-training-data-v2-2.yml), retrain, and recall should jump significantly.
