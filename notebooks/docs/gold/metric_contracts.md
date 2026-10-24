# Gold Business Metric Contracts

**Status:** Documented from executed Gold transformations and the read-only `11_gold_business_analysis` queries.  
**Currency:** BRL. **Dataset:** historical Olist orders; values are not inflation-adjusted.  
**Principle:** specify the fact grain, time role, filter population and missing-data behavior for every measure.

## 1. Order and customer KPIs

| Metric | Source and grain | Formula / filter | Important restriction |
|---|---|---|---|
| `total_orders` | `fct_orders`, 1/order | `COUNT(*)`, using the active analytical filter context | All statuses unless filtered. Purchase-period totals use `order_purchase_date` |
| `delivered_orders` | `fct_orders`, 1/order | `COUNT_IF(order_status = 'delivered')` | Final observed status attributed to the order's purchase month unless using a delivery-date role |
| `unique_consumers` | `fct_orders` JOIN `dim_customer` | `COUNT(DISTINCT customer_unique_id)` | **Not** `COUNT(DISTINCT customer_id)` |
| `orders_with_items` | `fct_orders`, 1/order | `COUNT_IF(has_order_items)` | Does not infer that all paid orders have items |
| `avg_ordered_value_brl` | `fct_orders`, 1/order | `AVG(ordered_total_value_brl)` | Excludes NULL no-item orders; always specify status and purchase/delivery date filter |
| `repeat_consumer_rate_pct` | Delivered-order subset of `fct_orders` JOIN `dim_customer` | `100 * consumers_with_at_least_two_delivered_orders / consumers_with_at_least_one_delivered_order` | Observed historical delivered-order repetition; **not** retention rate or a rolling cohort KPI |

## 2. Financial KPIs

| Metric | Source and grain | SQL expression | Recommended use |
|---|---|---|---|
| `ordered_item_value_brl` | `fct_order_items` at item grain, or pre-aggregated `fct_orders` at order grain | `SUM(item_value_brl)` | Item/catalog/product/category analysis; avoid adding totals from both facts |
| `ordered_freight_value_brl` | `fct_order_items` or `fct_orders` | `SUM(freight_value_brl)` | Freight as charged on items; the scope/status must match other metrics |
| `ordered_total_value_brl` | `fct_order_items` or `fct_orders` | `SUM(ordered_total_value_brl)` | Product price + freight. Not automatically net GMV or recognized revenue |
| `recorded_paid_value_brl` | `fct_payments` or `fct_orders` | `SUM(payment_value_brl)` / `SUM(paid_value_brl)` | Payment-method slicing must use `fct_payments`; there is no actual payment timestamp |
| `payment_difference_brl` | `fct_orders` where items **and** payments exist | `paid_value_brl - ordered_total_value_brl` | Only comparable orders; NULL when either source side is absent |
| `payment_item_mismatch_orders` | `fct_orders`, 1/order | `COUNT_IF(payment_item_mismatch_flag = TRUE)` | Difference threshold **strictly greater than BRL 0.01**, not `>=` |

**Explicit non-equivalences:**

- `recorded_paid_value_brl` is not automatically revenue, recognized sales, net GMV, or a refund-adjusted measure.
- `ordered_item_value_brl` is not freight-inclusive; `ordered_total_value_brl` is freight-inclusive.
- Item totals are not allocated to payment types; payment amounts are not allocated to individual sellers/products.
- An order with no items has `ordered_total_value_brl = NULL`, not necessarily zero. An order with no payments has `paid_value_brl = NULL`; an actual payment record with a zero amount is still a record.
- A month grouped by `order_purchase_date` is an **order purchase cohort**. Payments shown alongside it are payments associated with orders bought in that month, **not** payments processed during that month.

### Example: monthly order cohort

```sql
SELECT
    TRUNC(order_purchase_date, 'MONTH') AS purchase_month,
    COUNT(*) AS total_orders,
    COUNT_IF(order_status = 'delivered') AS delivered_orders,
    SUM(ordered_total_value_brl) AS ordered_total_value_brl,
    SUM(paid_value_brl) AS recorded_paid_value_brl,
    AVG(ordered_total_value_brl) AS avg_ordered_value_brl
FROM commerceai.gold.fct_orders
WHERE order_purchase_date IS NOT NULL
GROUP BY TRUNC(order_purchase_date, 'MONTH')
ORDER BY purchase_month;
```

## 3. Delivery KPIs

| Metric | Population / denominator | Formula | Missing outcomes |
|---|---|---|---|
| `eligible_delivered_orders` | `fct_orders` | `COUNT_IF(is_delivery_eligible = TRUE)` | Delivered status + actual date + estimated date required |
| `late_delivered_orders` | Eligible delivered orders | `COUNT_IF(is_late_delivery = TRUE)` | Ineligible orders are not treated as on-time |
| `late_delivery_rate_pct` | **Eligible delivered orders** | `100 * COUNT_IF(is_late_delivery = TRUE) / NULLIF(COUNT_IF(is_delivery_eligible = TRUE), 0)` | Undefined if no eligible orders |
| `avg_days_late_among_late_orders` | Eligible and late | `AVG(CASE WHEN is_late_delivery = TRUE THEN days_late END)` | On-time zero values excluded from this average |
| `delivery_duration_days` | Delivered orders with purchase and actual dates | Date difference actual delivery minus purchase | Calendar days, not 24-hour blocks; NULL for ineligible inputs |
| `promised_delivery_days` | Orders with purchase and estimated delivery dates | Date difference estimated delivery minus purchase | Promise-window measure, not elapsed shipping time |

**Date rule:** historical lateness compares `DATE(actual_delivery_timestamp)` to `DATE(estimated_delivery_timestamp)` in the configured Spark session. Deliveries made on the promise date are on time, even if the source timestamp represents midnight. This is a modeling choice because a contractual promise hour was not supplied.

### Example: correctly filtered rate

```sql
SELECT
    COUNT_IF(is_delivery_eligible = TRUE) AS eligible_delivered_orders,
    COUNT_IF(is_late_delivery = TRUE) AS late_delivered_orders,
    ROUND(
        100.0 * COUNT_IF(is_late_delivery = TRUE)
        / NULLIF(COUNT_IF(is_delivery_eligible = TRUE), 0),
        2
    ) AS late_delivery_rate_pct
FROM commerceai.gold.fct_orders;
```

## 4. Item, category, and seller KPIs

| Metric | Valid fact / grain | Definition | Non-additivity caution |
|---|---|---|---|
| `delivered_item_rows` | `fct_order_items` | `COUNT(*)` filtered to `order_status = 'delivered'` | Item rows, not orders |
| `delivered_item_value_brl` | `fct_order_items` | `SUM(item_value_brl)` with delivered-order filter | Attribute only at the item/product/seller grain |
| `delivered_orders_with_category` | `fct_order_items` JOIN `dim_product` | `COUNT(DISTINCT order_id)` per category | Sum across categories can exceed total unique orders |
| `delivered_orders_with_seller` | `fct_order_items` JOIN `dim_seller` | `COUNT(DISTINCT order_id)` per seller | Sum across sellers can exceed total unique orders |
| `distinct_products_sold` | `fct_order_items` | `COUNT(DISTINCT product_id)` in selected context | Counts products represented in items, not necessarily all catalog entries |

`dim_product.product_category_status` distinguishes untranslated from missing original categories. `COALESCE(category, 'uncategorized')` in reporting is a presentation label, not a source value correction.

## 5. Review KPIs

| Metric | Grain / denominator | Calculation | Interpretation |
|---|---|---|---|
| `review_records` | `fct_reviews` review grain | `COUNT(*)` | Multiple reviews per order are distinct observations |
| `orders_with_reviews` | `fct_reviews` | `COUNT(DISTINCT order_id)` | Different from the raw review count |
| `review_average_per_record` | `fct_reviews` | `AVG(review_score)` | Every review record gets equal weight |
| `review_average_per_order` | First average scores by `order_id`; then average those per-order means | `AVG(avg_review_score)` on an order-grain intermediate | Every reviewed order gets equal weight |
| `low_review_order_rate_pct` | Reviewed orders in selected delivery group | `100 * COUNT_IF(min_review_score <= 2) / COUNT(*)` | A low-reviewed order has **at least one** score of 1 or 2 |
| `orders_with_multiple_reviews` | Review counts grouped by `order_id` | `COUNT_IF(review_count > 1)` | Observed 547 orders; keep all review rows |

**Descriptive, not causal:** comparing ratings for late and on-time orders establishes an association only. Selection into reviewed orders and other factors may influence the comparison.

### Example: correct grain alignment for delivery/review comparison

```sql
WITH reviews_by_order AS (
    SELECT
        order_id,
        AVG(CAST(review_score AS DOUBLE)) AS avg_review_score,
        MIN(review_score) AS min_review_score
    FROM commerceai.gold.fct_reviews
    GROUP BY order_id
)
SELECT
    o.is_late_delivery,
    COUNT(*) AS reviewed_eligible_orders,
    ROUND(AVG(r.avg_review_score), 2) AS avg_review_score_per_order,
    ROUND(100.0 * COUNT_IF(r.min_review_score <= 2) / COUNT(*), 2)
        AS low_review_order_rate_pct
FROM commerceai.gold.fct_orders o
INNER JOIN reviews_by_order r ON o.order_id = r.order_id
WHERE o.is_delivery_eligible = TRUE
GROUP BY o.is_late_delivery;
```

## 6. Default semantic-layer rules

1. Always label financial metrics with the event scope and whether freight is included.
2. Use single-direction filtering from dimensions to facts by default.
3. Avoid summing order-level values after joining to item/payment/review grain.
4. Only aggregate reviewed orders after reducing `fct_reviews` to one record per `order_id` when computing per-order review KPIs.
5. Treat missing dates, categories, ZIP coordinates, and financial counterparts as meaningful missingness; do not silently convert them to zero/false.
6. Do not use recorded reviews, delivery completion timestamps, or post-outcome measures as pre-delivery predictive inputs.
