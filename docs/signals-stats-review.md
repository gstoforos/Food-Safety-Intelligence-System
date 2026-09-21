# Signal statistics review — 2026-09-14/2026-09-20

**statistics consistent with the published board**

- corpus this week: **58** records
- strata scored: **15** (471 too sparse to test)
- signals published: **3**
- board generated: 2026-09-21T12:21:11+00:00

## Benjamini-Hochberg, recomputed independently

q = 0.1, m = 15 (board declared m = 15)

| k | stratum | p | threshold | | verdict |
|--:|---|--:|--:|---|---|
| 1 | Shiga toxin-producing E. coli (STEC) · France | 0.00464 | 0.00667 | `1*0.1/15` | PASS |
| 2 | Salmonella · France | 0.00781 | 0.01333 | `2*0.1/15` | PASS |

## Per signal

### Shiga toxin-producing E. coli (STEC) · France  ·  proportion  ·  obs 6  ·  p 0.0046  ·  FDR pass

- **overdispersion** (caution) — the baseline varies 7.0x more than Binomial(N=58, pi=0.0269) assumes, so the exact p is anti-conservative — the direction is probably right, the number is soft
- **fragile_baseline** (caution) — one week supplies 75% of the baseline (9 of 12); the comparison rests on a single prior observation and moves a lot if it is wrong
- **trend** (note) — 4 consecutive rising weeks ending in the alarm week (0 -> 2 -> 3 -> 6); the last 2 of them sit inside the guard band, so the test scored this as a single elevated week rather than a ramp
- **publisher** (supporting) — 83% of this stratum is RappelConso (FR), but RappelConso (FR)'s own share of corpus FELL 48.2% -> 39.7% (30.71 -> 23 records). The stratum rose inside a contracting feed, which is the opposite of a publication artefact.

### Salmonella · France  ·  proportion  ·  obs 10  ·  p 0.0078  ·  FDR pass

- **publisher** (supporting) — 100% of this stratum is RappelConso (FR), but RappelConso (FR)'s own share of corpus FELL 48.2% -> 39.7% (30.71 -> 23 records). The stratum rose inside a contracting feed, which is the opposite of a publication artefact.
- **re_alarm** (caution) — alarmed 1x in the last 4 weeks; a LARGER week (15 on 2026-08-31/2026-09-06) sits inside the guard band and is therefore excluded from the baseline this week is judged against — the bar is lower because of the earlier spike

### Aflatoxin · Europe  ·  count-only  ·  obs 5  ·  p 0.0269  ·  FDR not-applicable

- **trend** (note) — 3 consecutive rising weeks ending in the alarm week (2 -> 3 -> 5); the last 2 of them sit inside the guard band, so the test scored this as a single elevated week rather than a ramp
- **publisher** (caution) — 100% of this stratum is RASFF (EU), and RASFF (EU)'s own share of corpus ROSE 35.6% -> 48.3% this week. The stratum and its publisher moved together; they cannot be separated.

**corpus_volume** (note) — corpus is below its own baseline by 0.40 sd — inside the normal band, so share readings are not confounded by a volume swing

**coverage** (note) — Baseline lies wholly after mature collection.

---

Advisory. Deterministic, no model. This reviewer annotates a board that has already been published; it does not suppress, alarm, or write to the register.
