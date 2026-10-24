# Power BI Semantic Model Design Notes — Proposal Only

**Status:** Architecture review; **not implemented or tested** in Power BI.

## 1. Recommended initial import model

For the static historical dataset, evaluate Power BI **Import Mode** first, subject to free tooling/export and connectivity feasibility in the actual environment. Do not claim that direct Databricks SQL Warehouse refresh has been configured.

Use the following logical relationships, with a single filter direction from dimensions to facts:

| Dimension | Fact | Join | Suggested behavior |
|---|---|---|---|
| `dim_customer` | `fct_orders` | `customer_id` | Dimension filters fact; values are unique on both sides in the observed historical dataset |
| `dim_customer` | `fct_order_items` | `customer_id` | Dimension filters item rows |
| `dim_customer` | `fct_payments` | `customer_id` | Dimension filters payment rows |
| `dim_customer` | `fct_reviews` | `customer_id` | Dimension filters review rows |
| `dim_product` | `fct_order_items` | `product_id` | Product/category filters **items only** |
| `dim_seller` | `fct_order_items` | `seller_id` | Seller attributes filter **items only** |
| `dim_date` | Each fact | Relevant purchase-date key | Prefer one clearly identified active purchase-date role per fact; other roles inactive or separate role-playing dimensions |

Power BI relationship cardinality must be checked against actual loaded data. Do not interpret the diagram as a mandate for all edges to be set to an identical many-side cardinality when both columns happen to be unique.

## 2. Date roles require explicit treatment

- `fct_orders`: `order_purchase_date_key`, `estimated_delivery_date_key`, `actual_delivery_date_key`.
- `fct_order_items`: `order_purchase_date_key`, `shipping_limit_date_key`.
- `fct_payments`: `order_purchase_date_key` **only**; this is not an actual payment transaction date.
- `fct_reviews`: `order_purchase_date_key`, `review_creation_date_key`, `review_answer_date_key`.

A single active date relationship per fact can anchor purchase-month reports. For delivery- or review-date trends, consider inactive relationships activated within specific DAX measures or separate role-playing calendar dimensions. Avoid ambiguous filter paths.

## 3. Measure placement and non-additivity

| Report view | Primary fact | Reason |
|---|---|---|
| Order overview and historical lateness | `fct_orders` | Exactly one row per order |
| Product and category performance | `fct_order_items` | Amounts attributable to individual product items |
| Seller performance | `fct_order_items` | Seller is attached to the item, not the order header |
| Payment mix | `fct_payments` | A single order may have different payment methods |
| Review distribution | `fct_reviews` | One row per source review, not one per order |
| Delivery versus review comparison | Pre-aggregate review scores to order grain, then join orders | Avoid giving multiple-review orders excess weight |

**Do not:** make naive many-to-many relationships among fact tables; count order rows from item facts without distinctness; sum `fct_orders` totals by seller using fact-to-fact joins; treat customer counts from `customer_id` as repeat-customer counts; label total payments as accounting revenue.

### Seller-scoped order-level metrics

`fct_orders` has no seller key because an order may contain several sellers. Seller-filtered KPIs from the order fact **will not** automatically follow a `dim_seller` selection. Any future seller-scoped order measure needs an explicit interpretation (e.g., distinct orders containing that seller) and a tested filter propagation strategy such as a deduplicated order-seller bridge or an explicit DAX virtual relationship. Do not allocate the full order payment amount to every seller.

### Customer geography semantics

Customer state and ZIP represent the order customer record, not a guaranteed shipping address or verified physical coordinate. Latitude/longitude are selected reference points at ZIP-prefix grain and are not suitable for precision routing without qualification.

## 4. Metric contracts and formatting

Keep canonical definitions in [metric_contracts.md](metric_contracts.md). Explicitly separate:

- ordered item value (excluding freight)
- freight value
- ordered total (including freight)
- recorded paid value
- delivered order count (status-based)
- eligible deliveries and late rate (actual/estimated date-based)
- unique consumers (`customer_unique_id`)
- review record average versus average of per-order review scores

Use consistent BRL formatting and clear date-role labels. Maintain separate calculated measures for presentation/filter context; the Gold already supplies reusable row-level and order-level derived attributes.

## 5. Validation before publishing

1. Confirm imported row counts and column types match Gold.
2. Confirm relationship filtering does not multiply monetary amounts.
3. Validate late-rate calculations against 6,534 / 96,470 = 6.77% in the unfiltered historical population.
4. Validate order and item total reconciliation in an unfiltered model.
5. Confirm that category and seller distinct order counts are not summed across groups to create an overall total.
6. Compare reviewed-order metrics at order grain, not raw review grain.
7. Confirm any export/refresh workflow works within available free plans and quotas before documenting it as implemented.
