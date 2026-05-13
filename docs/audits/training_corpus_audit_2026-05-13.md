# Training Corpus Audit — recalls.xlsx

Source: `docs/data/recalls.xlsx`

## Sheet inventory

| Sheet | Rows | Role in training corpus |
|---|---:|---|
| Recalls | 588 | **NOT YET USED** — vetted promote examples |
| Weekly_Review | 44 | currently used → promote examples |
| Weekly_Rejected | 36 | currently used → reject examples |
| Pending | 9 | not used (un-reviewed) |

## Tier distribution: current vs proposed corpus

| Tier | Current (Weekly_Review only) | Proposed (+Recalls) | Multiplier |
|---|---:|---:|---:|
| Tier 1 | 36 | 442 | 12.3× |
| Tier 2 | 7 | 160 | 22.9× |
| Tier 3 | 1 | 30 | 30.0× |
| **Total** | **44** | **632** | **14.4×** |

## Pathogen diversity (Recalls sheet only)

Top 15 pathogens in Recalls (will appear in tier-assignment training):

| Pathogen | Count |
|---|---:|
| Listeria monocytogenes | 209 |
| Salmonella | 147 |
| Aflatoxin | 49 |
| Ochratoxin | 25 |
| Shiga toxin-producing E. coli (STEC) | 22 |
| Cereulide (Bacillus cereus toxin) | 11 |
| Norovirus | 10 |
| Cereulide (B. cereus toxin) | 10 |
| Salmonella spp. | 8 |
| Clostridium botulinum | 7 |
| Bacillus cereus / cereulide | 7 |
| Histamine / scombrotoxin | 6 |
| Escherichia coli (generic) | 4 |
| Aflatoxins | 4 |
| Bacillus cereus | 3 |

## Field-correction recovery — current parser vs v2 parser

- Rows with parseable corrections (**current** parser): **31**
- Rows with parseable corrections (**v2** parser):      **59**
- Gain: **28 additional examples** (1.9× over current)

Field-correction distribution comparison:

| Field | Current | v2 |
|---|---:|---:|
| Brand | 8 | 35 |
| Company | 25 | 25 |
| Date | 3 | 6 |
| Pathogen | 16 | 16 |
| Product | 6 | 6 |

## Notes annotation inventory (all sheets)

How many reviewer/enrichment annotations exist in Notes columns:

| Tag | Occurrences |
|---|---:|
| `dedupe-fix` | 97 |
| `url-gate` | 64 |
| `gemini-check` | 48 |
| `gemini-enrich` | 45 |
| `claude-check` | 44 |
| `manual-promote` | 22 |
| `gap-gate` | 21 |
| `date-unknown` | 14 |
| `rebuild` | 8 |
| `claude-flag` | 7 |
| `strict-outbreak-audit` | 6 |
| `outbreak` | 4 |
| `audit-2026-04-28` | 3 |
| `enrich-gate` | 2 |
| `region-fix` | 2 |
| `scraper-flag` | 2 |
| `manual-fix` | 1 |
| `manual-add` | 1 |
| `tier-fix` | 1 |

## Promote/reject training balance

- Current PROMOTE examples (Weekly_Review): **44**
- Current REJECT examples (Weekly_Rejected): **36**
- Proposed PROMOTE examples (Recalls + Weekly_Review): **632**
- Reject pool unchanged — **bottleneck shifts to needing more rejects**
  - Rejects are gone weekly (sheet gets wiped); historical rejects must come from git history of recalls.xlsx OR GH Actions logs

## Recommendation

✅ **Proceed with v2 export.** Adding the Recalls sheet gives 14× more PROMOTE examples and balances the tier distribution.

**Imbalance after v2 export:**
- PROMOTE: ~632
- REJECT:  ~36
- Ratio:   18:1 — needs class weighting at training time, OR rescue more rejects from git history.

---
_Run: `python audit_training_corpus.py --xlsx docs/data/recalls.xlsx`_
