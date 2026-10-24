# Gold Business Analysis: Observed Findings

**Evidence:** six queries executed in `11_gold_business_analysis` by the project owner (results shared on 2026-10-24). This file records **observed outputs**, not new queries executed during documentation. Dataset observations are historical; no current-commerce claims should be inferred.

## 1. Monthly order and financial performance

- The earliest reported purchase month was **September 2016** (4 orders); the latest was **October 2018** (4 orders). Calendar coverage beyond the order window does not establish later business activity.
- **November 2017** had **7,544 orders**, the highest monthly order count in the supplied query output, including 7,289 with delivered status in the extracted historical snapshot.
- **January 2018:** 7,269 orders and BRL 1,107,301.89 ordered total value (products plus freight).
- The reported purchase months September and October 2018 had **16 and 4 orders**, respectively, and zero delivered orders in these monthly cohorts. These are sparse boundary months; do not interpret them as equivalent to fully observed sales months.
- Recorded payments and ordered item totals are different measures and may differ from each other; do not present either as recognized revenue.

**Interpretation limit:** monthly delivered-order counts are grouped by the order **purchase month**, not the delivery-completion month. The timeline includes partial historical coverage at the edges and should not be presented as an uninterrupted modern retail trend.

## 2. Delivery delays by customer state

The query included states with at least 100 eligible deliveries, and the results use customer state (not seller state).

| Customer state | Eligible delivered orders | Late orders | Late rate | Average days late among late orders |
|---|---:|---:|---:|---:|
| SP | 40,494 | 1,820 | 4.49% | 8.33 |
| RJ | 12,350 | 1,495 | 12.11% | 13.52 |
| MG | 11,354 | 519 | 4.57% | 8.40 |
| ES | 1,995 | 214 | 10.73% | 11.30 |
| CE | 1,279 | 176 | 13.76% | 15.18 |
| MA | 717 | 125 | 17.43% | 10.50 |
| AL | 397 | 85 | 21.41% | 9.54 |
| AM | 145 | 4 | 2.76% | 30.25 |

**Insight:** rates vary widely across customer states. SP and RJ have large sample sizes and notably different observed delay rates. AL has the highest late rate among the included states, but its denominator (397) is much smaller than SP's. AM's average days late is based on just four late orders and should not be over-interpreted.

The globally validated historical rate is **6,534 late orders / 96,470 eligible deliveries = 6.77%**, using a date-based comparison. This denominator does not include eight delivered-status orders missing actual delivery dates.

## 3. Product category performance

The category ranking filtered item rows to orders with `order_status = 'delivered'` and ranked **item values excluding freight**.

| Product category (source label) | Item rows | Delivered item value (BRL) | Freight value (BRL) |
|---|---:|---:|---:|
| `beleza_saude` | 9,465 | 1,233,131.72 | 178,957.81 |
| `relogios_presentes` | 5,859 | 1,166,176.98 | 98,156.14 |
| `cama_mesa_banho` | 10,953 | 1,023,434.76 | 201,774.50 |
| `esporte_lazer` | 8,431 | 954,852.55 | 163,404.36 |
| `informatica_acessorios` | 7,644 | 888,724.61 | 143,999.16 |

**Insight:** highest item value is not the same as highest number of item rows. `cama_mesa_banho` has more reported item rows than `beleza_saude`, but a lower delivered item value in the supplied top-five output. This is a distinction between value and volume, not an inference about profit.

## 4. Seller ranking

- The top seller by delivered item value was `4869f7a5dfa277a7dca6462dcf3b52b2` (SP), at **BRL 226,987.93** item value and **1,124** orders containing that seller.
- The second-ranked seller was `53243585a1d6dc2643021fd1853d8905` (BA), at **BRL 217,940.44** item value and **348** orders containing that seller.
- The top-15 ranking contained 12 sellers from SP, 2 from RJ and 1 from BA.

**Interpretation limit:** one multi-seller order may contribute to more than one seller's order count. The corresponding item values are attributed to the seller of each item; payment totals are not allocated by seller.

## 5. Customer repeat-order segmentation

| Segment | Consumers | Delivered orders | Mean delivered orders per consumer | Consumer share |
|---|---:|---:|---:|---:|
| One delivered order | 90,557 | 90,557 | 1.00 | 97.00% |
| Repeat consumer (2+) | 2,801 | 5,921 | 2.11 | 3.00% |
| **Total** | **93,358** | **96,478** | | **100.00%** |

**Insight:** only 3.00% of distinct consumers with at least one delivered order in this historic snapshot had two or more delivered orders. This is a historical repeat-order rate within an observed population, not an estimate of customer retention or future lifetime behavior.

## 6. Delivery outcomes and review scores

The analysis first reduced reviews to order grain, then joined to the eligible delivered-order fact.

| Delivery group | Reviewed eligible orders | Review records | Mean review score **per order** | Orders with at least one score ≤ 2 | Low-score order rate |
|---|---:|---:|---:|---:|---:|
| LATE | 6,381 | 6,409 | 2.27 | 3,986 | 62.47% |
| ON_TIME | 89,443 | 89,944 | 4.29 | 8,324 | 9.31% |

**Insight:** in the reviewed eligible subset, late delivery is associated with a lower order-level average rating (2.27 versus 4.29), a **2.02-point difference**, and a much higher incidence of at least one low review score (62.47% versus 9.31%).

**Caveats:** association is not causation; only reviewed eligible delivered orders are included, reviews can be multi-record, and the score is averaged at order level. The 'low-score' flag uses the minimum review score of each order, not the average score. The overall late rate and reviewed-order breakdown have different denominators.

## 7. Open analytical questions (not results)

- Are the observed geographic differences explained by seller/customer distance, item mix, freight value, or particular historical periods? This would require additional analysis, not causal attribution.
- How do purchase-month cohorts compare when excluding thin boundary months? No cohort adjustment has been made.
- How do score distributions change by category or seller when reviews belong to multi-item orders? Per-item attribution would require an explicit allocation/attribution rule.

These questions are possible future **business analysis**, not commitments to implement ML or new tables.
