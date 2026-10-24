# CommerceAI | Gold Layer

> **Status: IMPLEMENTED AND VALIDATED (original execution).** Eight managed Delta tables; **82 checks passed / 0 failed**. The streamlined notebooks delivered in this review are **not yet re-executed** in Databricks. Last confirmed result comes from the original notebooks.

## Purpose and scope

The Gold layer delivers a dimensionally modeled, historical Olist analytical dataset in the `commerceai.gold` schema. The notebooks use Spark SQL embedded in Python for builds and native SQL for independent validation and ad hoc business analysis. Gold is **not a machine-learning feature store**; future ML and feedback AI are separate projects.

## Table inventory and grain

| Table | Grain / key | Verified rows | Original gate |
|---|---|---:|---:|
| `dim_date` | one row per `full_date` / `date_key` | 1,827 | 7 PASS |
| `dim_customer` | one row per `customer_id` | 99,441 | 9 PASS |
| `dim_product` | one row per `product_id` | 32,951 | 8 PASS |
| `dim_seller` | one row per `seller_id` | 3,095 | 9 PASS |
| `fct_orders` | one row per `order_id` | 99,441 | 12 PASS |
| `fct_order_items` | one row per (`order_id`, `order_item_id`) | 112,650 | 12 PASS |
| `fct_payments` | one row per (`order_id`, `payment_sequential`) | 103,886 | 12 PASS |
| `fct_reviews` | one row per (`review_id`, `order_id`) | 99,224 | 13 PASS |
| **Total** | | | **82 PASS / 0 FAIL** |

**Identity:** `customer_id` is the per-order customer-record key; `customer_unique_id` is the consumer identifier for repeat behavior. The customer dimension retains 96,096 unique consumer identifiers.

**Geography:** customer/seller location fields come from their respective Silver source; optional ZIP-prefix coordinates are approximate references. Missing ZIP matches remain explicit (278 customer rows; 7 seller rows). `dim_product` retains 610 products with missing category and 13 products with category lacking English translation.

**Date:** `dim_date` spans **2016-01-01 to 2020-12-31**, because the observed maximum `shipping_limit_date` is **2020-04-09**. Date casting in Gold was validated using an `Etc/UTC` Spark session; source timestamp provenance does **not** establish an original UTC timezone.

## Dependency and execution model

```text
Silver Delta (9 validated tables)
         |
         v
08_gold_dimensions.py       # Four Gold dimensions; build only
         |
         v
09_gold_facts.py            # Four Gold facts; build only
         |
         v
10_gold_validation.sql      # Eight independent contract sets; strict integrated gate

11_gold_business_analysis.sql  # Manual, read-only exploration; NOT part of build job
```

The build notebooks contain only a timezone check plus four `CREATE OR REPLACE TABLE ... USING DELTA AS SELECT` transformations each. Rebuilding tables is full-load replacement **per table**; the entire Gold layer is **not** an atomic multi-table publication. A fully deployed Databricks Job has not been confirmed.

### Validation gate and version status

- **Confirmed original gate:** 7 date + 9 customer + 8 product + 9 seller + 12 order + 12 order-item + 12 payment + 13 review checks = **82 PASS, 0 FAIL**.
- **Refactored gate:** the **same eight `CREATE TEMP VIEW` validation definitions** are preserved, followed by one unified result view, one detailed output, and one integrated `ASSERT_TRUE` gate. This version is pending re-execution.
- Checks independently cover business-key uniqueness, row preservation, attribute fidelity, financial reconciliation, dimension references, historical delivery rules, review text preservation, and per-order review multiplicity.
- `ASSERT_TRUE` produces `NULL` for a passing result and raises an error for a failing condition. The unified gate also verifies exactly 82 **distinct** (`dataset`, `check_name`) combinations and expected per-table check counts.
- The gate operates on current table states: it does not guarantee atomicity of the underlying full-load refresh.

## Metric contracts

| Metric | Fact and denominator / meaning | Caution |
|---|---|---|
| `total_orders` | `COUNT(*)` from `fct_orders` at order grain | Includes all statuses unless explicitly filtered |
| `delivered_orders` | `COUNT_IF(order_status = 'delivered')` from `fct_orders` | Purchase-month analysis groups by order purchase date, not delivery date |
| `unique_customers` | `COUNT(DISTINCT customer_unique_id)` after the order-to-customer relationship | Do not substitute `customer_id` |
| `ordered_item_value_brl` | Sum of item price (`fct_order_items.item_value_brl`), or corresponding independent order aggregate | Not accounting-recognized revenue |
| `ordered_freight_value_brl` | Sum of item freight | Do not add the same order and item amounts together |
| `ordered_total_value_brl` | Sum of item price plus freight; null for orders with no items | Distinct from payments |
| `recorded_paid_value_brl` | Sum of `fct_payments.payment_value_brl`, or matching independent order aggregate | May include orders without items and differ from ordered values |
| `late_delivery_rate_pct` | `100 * late eligible delivered orders / eligible delivered orders` | `is_delivery_eligible` requires `delivered` status and both dates; lateness uses `DATE`, not full timestamp |
| `avg_days_late_among_late_orders` | Mean `days_late` where `is_late_delivery = TRUE` | Excludes on-time deliveries |
| `repeat_customer_share_pct` | Share of consumers with **2+ delivered orders** among consumers with **1+ delivered order** | Historical repeat-order segment, not longitudinal retention |
| `avg_review_score_per_order` | First `AVG(review_score)` per order, then average over reviewed eligible orders | Avoid extra weighting from orders with multiple reviews |
| `low_review_order_rate_pct` | Reviewed eligible orders with at least one score <= 2 divided by all reviewed eligible orders | Observational association, not causal analysis |

**Grain safeguards:** payments and items are aggregated **separately** before joining orders. Never join order items, payments, and reviews directly at their original grains and then sum amounts. A seller filter on `fct_order_items` does not automatically filter order-level measures in `fct_orders`; Power BI relationships must be designed explicitly.

## Preserved source exceptions

| Exception | Last observed count |
|---|---:|
| Payments without item records (by order) | 775 |
| Items without payment records (by order) | 1 |
| Orders with > BRL 0.01 payment/item amount difference | 303 |
| Delivered orders without delivery timestamp | 8 |
| Carrier timestamp later than customer-delivery timestamp | 23 |
| Zero-value payment records | 9 |
| Zero-installment payment records | 2 |
| Orders with more than one review record | 547 |

No rows were removed or artificially balanced to conceal these conditions.

## Verified business analysis results (notebook 11)

Six **read-only** SQL analyses were run: monthly performance, delay by customer state, category performance, seller ranking, delivered-order repeat customers, and review/late-delivery association. Selected **observed**, not simulated, results:

- Across 96,470 delivery-eligible orders, **6,534 were late** (**6.77%**) according to date-based comparison.
- Delay rates for eligible delivered orders: **SP 4.49%** (40,494), **RJ 12.11%** (12,350), **ES 10.73%** (1,995). Per-state calculations reflect customer state, not seller location.
- Delivered-order consumer segment: **90,557** with one delivered order; **2,801** with two or more (**3.00%** among those with a delivered order).
- Among eligible delivered orders **with reviews**, the average order-level score was **2.27 for late** vs **4.29 for on-time**; low-score order rates were **62.47%** vs **9.31%**. Associations do not establish causality.
- The largest product category by delivered item value in the top-category query was `beleza_saude` (**BRL 1,233,131.72**). Category totals do not represent mutually exclusive order counts.

## Operational and semantic-model limitations

- The source is a static historical sample, not a realtime streaming dataset; full-load overwrites are used.
- No end-to-end production job, all-table rollback, or SQL Warehouse/Power BI connection has been confirmed.
- The Gold fact constellation is **not** a single universally joinable flat fact. Use conformed dimensions carefully; avoid ambiguous active date relationships or bidirectional filters.
- `fct_orders` does not contain one seller key per order; multi-seller order reporting requires distinct order counts or explicit allocations.
- Review fields are post-purchase/post-delivery observations; do **not** use them as early-stage delivery-prediction inputs.
- The Gold documentation refers only to reported executions; replace statuses only after new validations have run.

## Version-controlled notebooks

- `notebooks/03_gold/08_gold_dimensions.py` — build: 4 dimensions
- `notebooks/03_gold/09_gold_facts.py` — build: 4 facts
- `notebooks/03_gold/10_gold_validation.sql` — independent SQL quality gate
- `notebooks/03_gold/11_gold_business_analysis.sql` — exploratory SQL; intentionally unchanged by the review

All technical artifacts, cell titles, SQL code and documentation are authored in English. The raw CSV files and workspace credentials must never be committed to Git.
