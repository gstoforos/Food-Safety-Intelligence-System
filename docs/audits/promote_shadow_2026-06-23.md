# Promote Classifier — Shadow Mode Comparison — 2026-06-23

Compared the trained promote classifier against Claude's stored PROMOTE/REJECT decisions on **200 sample rows** (Recalls ∪ Weekly_Rejected). Production data unchanged.

## Headline

- **Overall agreement with Claude**: 83.0% (166/200)
- **High-confidence agreement** (≥0.85): **83.4%** (166/199)
- **Calls that would skip API**: **99.5%** of total
- **REJECT recall** (catches rejects): **67.0%** (67/100)
- **REJECT precision**: **98.5%** (67/68)

> **Why REJECT recall matters most:** in production, missing a reject (false promote) lets a bad row into Recalls. Missing a promote (false reject) just sends a good row to manual review. The first is worse.

## Cost projection

Assuming ~30 promote-check calls/day in production:

| | Without model | With model |
|---|---:|---:|
| API calls / day | 30 | 0 (1%) |
| API calls / year | 10,950 | 55 |
| Reduction | — | **100%** |

## Per-class breakdown

| Class | Sample | Agreement | Avg confidence |
|---|---:|---:|---:|
| PROMOTE | 100 | 99.0% | 0.975 |
| REJECT | 100 | 67.0% | 0.971 |

## Confusion matrix

Rows = Claude's stored decision, columns = model's prediction:

```
                  Pred PROMOTE   Pred REJECT
  True PROMOTE          99             1
  True REJECT           33            67
```

## Confidence distribution

| Confidence | Count | % |
|---|---:|---:|
| 0.00–0.50 | 0 | 0.0% |
| 0.50–0.70 | 0 | 0.0% |
| 0.70–0.85 | 1 | 0.5% |
| 0.85–0.95 | 0 | 0.0% |
| 0.95–1.01 | 199 | 99.5% |

## High-confidence disagreements (33)

Cases where model is ≥0.85 confident but disagrees with Claude. These are the most diagnostic — either the model is wrong (training shortcoming) or Claude/the reviewer was wrong (silent labeling error).

| # | Gold | Pred | Conf | Source | Pathogen | Product | URL |
|---:|:---:|:---:|---:|---|---|---|---|
| 1 | REJECT | PROMOTE | 0.977 | Salute |  | Lasciare il ritiro ai Mondiali per assistere alla nascita de |  |
| 2 | REJECT | PROMOTE | 0.977 | EFET |  | Στον εισαγγελέα Εφετών Πειραιά ο Αλέξανδρος Γιωτόπουλος - Ne |  |
| 3 | REJECT | PROMOTE | 0.977 | AESAN |  | Un acta del 10 de febrero confirma la orden de no avisar a l | https://elpais.com/espana/madrid/2026-06-19/un-acta-del-10-de-febrero-confirma-la-orden-de-no-avisar-a-las-personas-sin- |
| 4 | REJECT | PROMOTE | 0.977 | AESAN |  | La AESAN retira estas semillas de lino: detectan ácido cianh |  |
| 5 | REJECT | PROMOTE | 0.977 | NVWA | Cereulide (B. cereus toxin) | Als cereulide in voedsel terecht komt kan dat braken veroorz | https://www.nvwa.nl/actueel/nieuws/2026/01/23/nvwa-monitort-situatie-zuigelingenvoeding-nauwgezet |
| 6 | REJECT | PROMOTE | 0.977 | Salute | non dichiarata | Richiamato un lotto di Würstel Fiorucci: caseina non dichiar |  |
| 7 | REJECT | PROMOTE | 0.977 | AESAN |  | Ferraz normaliza los sobresaltos con Zapatero sin intención  |  |
| 8 | REJECT | PROMOTE | 0.977 | AESAN |  | La AESAN activa una alerta alimentaria en dos marcas de cere |  |
| 9 | REJECT | PROMOTE | 0.977 | Salute | caseina | Richiamato un lotto di Würstel Fiorucci: caseina non dichiar |  |
| 10 | REJECT | PROMOTE | 0.977 | AESAN | listeria | Alerta alimentaria por presencia de listeria en foie gras de |  |
| 11 | REJECT | PROMOTE | 0.977 | EFET |  | Ο ΕΦΕΤ προχωρά σε μαζική ανάκληση καπνιστής πέστροφας από τη |  |
| 12 | REJECT | PROMOTE | 0.977 | Salute |  | Begona Gomez, la moglie di Pedro Sanchez rinviata a giudizio |  |
| 13 | REJECT | PROMOTE | 0.977 | AESAN |  | Un polémico general retirado aspira a adelantar a Meloni por | https://elpais.com/internacional/2026-06-22/un-polemico-general-retirado-aspira-a-adelantar-a-meloni-por-la-derecha-en-i |
| 14 | REJECT | PROMOTE | 0.977 | ASAE |  | Buscas em Lousada acabam com 7 detidos e idosos retirados de |  |
| 15 | REJECT | PROMOTE | 0.977 | EFET |  | ΕΦΕΤ: Ανακαλείται καπνιστή πέστροφα – Βρέθηκε παθογόνος μικρ |  |
| 16 | REJECT | PROMOTE | 0.977 | Salute |  | Ghiaccio alimentare ritirato per presenza di batteri, avviso |  |
| 17 | REJECT | PROMOTE | 0.977 | AESAN |  | Alerta en el consumo de lino: la AESAN detecta ácido cianhíd |  |
| 18 | REJECT | PROMOTE | 0.977 | FDA PH | Cronobacter sakazakii | OF LACTUM ... | https://www.fda.gov.ph/fda-advisory-no-2019-257-voluntary-recall-of-lactum-infant-formula-powder-0-6-months-350-grams-wi |
| 19 | REJECT | PROMOTE | 0.977 | AESAN | salmonella | La Aesan amplía la alerta por salmonella en nuevos productos |  |
| 20 | REJECT | PROMOTE | 0.977 | AESAN | listeria | La AESAN alerta de la presencia de 'Listeria' en un lote de  |  |
| 21 | REJECT | PROMOTE | 0.977 | Salute | pesticidi | Peperoncino richiamato per presenza di pesticidi oltre la so |  |
| 22 | REJECT | PROMOTE | 0.977 | Salute |  | Ghiaccio richiamato dai supermercati, presenza entero-cocchi |  |
| 23 | REJECT | PROMOTE | 0.977 | Salute |  | Insetto in una confezione di formaggio Dop, scatta il ritiro |  |
| 24 | REJECT | PROMOTE | 0.977 | CDC | Clostridium botulinum | Nara Organics Whole Milk Organic Infant Formula | https://www.cdc.gov/media/releases/2026/infant-botulism-outbreak-linked-to-powdered-infant-formula-june-2026.html |
| 25 | REJECT | PROMOTE | 0.977 | EFET |  | ΕΦΕΤ: Ανάκληση μαριναρισμένης καπνιστής πέστροφας λόγω παθογ |  |
| 26 | REJECT | PROMOTE | 0.977 | ASAE |  | Infarmed ordena retirada do mercado de produto para alívio d |  |
| 27 | REJECT | PROMOTE | 0.977 | Salute |  | Ostigard diventa papà in diretta su FaceTime mentre è in rit |  |
| 28 | REJECT | PROMOTE | 0.977 | AESAN |  | Israel asegura que el pacto no le vincula y que no está "sub |  |
| 29 | REJECT | PROMOTE | 0.977 | AESAN |  | La inversión extranjera neta en España cae un 17% este año y |  |
| 30 | REJECT | PROMOTE | 0.977 | Salute |  | Ondata di caldo in Italia: bollino giallo in 8 città, ecco d |  |

_…and 3 more_

## Low-confidence cases (1)

Cases where model max-prob is <0.85 — would fall back to Claude API in production.

Breakdown:
- PROMOTE (gold): 1
- REJECT (gold): 0

Lowest-confidence 15:

| # | Gold | Pred | Conf | Pathogen | URL |
|---:|:---:|:---:|---:|---|---|
| 1 | PROMOTE | REJECT | 0.807 | Aflatoxin | https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/Meldungen/2026/06_Juni/260617_14_NW_Zimtrolle/260617_14_NW_ |

## Recommendation

⚠ **Not yet ready.** Either accuracy is too low or reject recall is below safe threshold. Add disagreement cases to training corpus and re-train.
