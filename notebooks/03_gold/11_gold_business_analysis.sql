-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # 11 — Gold Business Analysis
-- MAGIC
-- MAGIC ## Purpose
-- MAGIC
-- MAGIC Explore business performance using validated CommerceAI Gold tables and native Spark SQL.
-- MAGIC
-- MAGIC ## Inputs
-- MAGIC
-- MAGIC - `commerceai.gold.dim_date`
-- MAGIC - `commerceai.gold.dim_customer`
-- MAGIC - `commerceai.gold.dim_product`
-- MAGIC - `commerceai.gold.dim_seller`
-- MAGIC - `commerceai.gold.fct_orders`
-- MAGIC - `commerceai.gold.fct_order_items`
-- MAGIC - `commerceai.gold.fct_payments`
-- MAGIC - `commerceai.gold.fct_reviews`
-- MAGIC
-- MAGIC ## Outputs
-- MAGIC
-- MAGIC Read-only SQL query results covering:
-- MAGIC
-- MAGIC - Monthly order performance
-- MAGIC - Delivery delays by customer state
-- MAGIC - Product category performance
-- MAGIC - Seller performance and ranking
-- MAGIC - Customer repeat-order behavior
-- MAGIC - Relationship between delivery delays and review scores
-- MAGIC
-- MAGIC ## Main Responsibilities
-- MAGIC
-- MAGIC - Demonstrate joins, CTEs, aggregations, conditional calculations and window functions.
-- MAGIC - Respect the grain of each fact table.
-- MAGIC - Prevent financial duplication across facts.
-- MAGIC - Distinguish ordered item value, freight value and recorded payments.
-- MAGIC - Support future Power BI business metric definitions.
-- MAGIC
-- MAGIC ## Execution / Validation Notes
-- MAGIC
-- MAGIC - Run after the Gold integrated quality gate.
-- MAGIC - Use native Spark SQL only.
-- MAGIC - Do not create or overwrite Gold tables.
-- MAGIC - Use the order purchase date for purchase-cohort analyses.
-- MAGIC - Preserve documented source-data limitations.
-- MAGIC - Treat results as historical business analysis, not causal evidence.
-- MAGIC - This notebook is exploratory and is not part of the production build workflow.

-- COMMAND ----------

-- DBTITLE 1,01 - Monthly Order and Financial Performance

-- Analyze monthly order activity using the purchase date.
-- Ordered values and recorded payments are separate metrics.

SELECT
    TRUNC(order_purchase_date, 'MONTH') AS purchase_month,

    COUNT(*) AS total_orders,

    COUNT_IF(order_status = 'delivered') AS delivered_orders,

    COUNT_IF(has_order_items) AS orders_with_items,

    SUM(item_value_brl) AS ordered_item_value_brl,

    SUM(freight_value_brl) AS ordered_freight_value_brl,

    SUM(ordered_total_value_brl) AS ordered_total_value_brl,

    SUM(paid_value_brl) AS recorded_paid_value_brl,

    ROUND(
        AVG(ordered_total_value_brl),
        2
    ) AS avg_ordered_value_brl

FROM commerceai.gold.fct_orders

WHERE order_purchase_date IS NOT NULL

GROUP BY
    TRUNC(order_purchase_date, 'MONTH')

ORDER BY purchase_month;

-- COMMAND ----------

-- DBTITLE 1,02 - Delivery Delay Performance by Customer State

-- Compare delivery delay rates across customer states.
-- Include only orders with a valid historical delivery outcome.

SELECT
    c.customer_state,

    COUNT(*) AS eligible_delivered_orders,

    COUNT_IF(
        o.is_late_delivery = TRUE
    ) AS late_delivered_orders,

    ROUND(
        100.0 * COUNT_IF(o.is_late_delivery = TRUE)
        / NULLIF(COUNT(*), 0),
        2
    ) AS late_delivery_rate_pct,

    ROUND(
        AVG(
            CASE
                WHEN o.is_late_delivery = TRUE
                THEN o.days_late
            END
        ),
        2
    ) AS avg_days_late_among_late_orders

FROM commerceai.gold.fct_orders AS o

INNER JOIN commerceai.gold.dim_customer AS c
    ON o.customer_id = c.customer_id

WHERE o.is_delivery_eligible = TRUE

GROUP BY c.customer_state

HAVING COUNT(*) >= 100

ORDER BY
    late_delivery_rate_pct DESC,
    eligible_delivered_orders DESC;


-- COMMAND ----------

-- DBTITLE 1,03 - Product Category Performance

-- Analyze item values from delivered orders by product category.
-- Missing categories remain visible in the analytical output.

SELECT
    COALESCE(
        p.product_category_name,
        'uncategorized'
    ) AS product_category,

    p.product_category_status,

    COUNT(*) AS delivered_item_rows,

    COUNT(DISTINCT i.order_id)
        AS delivered_orders_with_category,

    COUNT(DISTINCT i.product_id)
        AS distinct_products,

    SUM(i.item_value_brl)
        AS delivered_item_value_brl,

    SUM(i.freight_value_brl)
        AS delivered_freight_value_brl,

    ROUND(
        AVG(i.item_value_brl),
        2
    ) AS avg_item_value_brl

FROM commerceai.gold.fct_order_items AS i

INNER JOIN commerceai.gold.dim_product AS p
    ON i.product_id = p.product_id

WHERE i.order_status = 'delivered'

GROUP BY
    p.product_category_name,
    p.product_category_status

ORDER BY delivered_item_value_brl DESC

LIMIT 15;


-- COMMAND ----------

-- DBTITLE 1,04 - Seller Performance Ranking

-- Rank sellers by item value from delivered orders.
-- Aggregate at seller grain before applying the ranking window.

WITH seller_performance AS (

    SELECT
        i.seller_id,
        s.seller_state,

        COUNT(*) AS delivered_item_rows,

        COUNT(DISTINCT i.order_id)
            AS delivered_orders_with_seller,

        SUM(i.item_value_brl)
            AS delivered_item_value_brl,

        SUM(i.freight_value_brl)
            AS delivered_freight_value_brl

    FROM commerceai.gold.fct_order_items AS i

    INNER JOIN commerceai.gold.dim_seller AS s
        ON i.seller_id = s.seller_id

    WHERE i.order_status = 'delivered'

    GROUP BY
        i.seller_id,
        s.seller_state
),

ranked_sellers AS (

    SELECT
        seller_id,
        seller_state,
        delivered_item_rows,
        delivered_orders_with_seller,
        delivered_item_value_brl,
        delivered_freight_value_brl,

        ROW_NUMBER() OVER (
            ORDER BY
                delivered_item_value_brl DESC,
                seller_id
        ) AS seller_rank

    FROM seller_performance
)

SELECT
    seller_rank,
    seller_id,
    seller_state,
    delivered_item_rows,
    delivered_orders_with_seller,
    delivered_item_value_brl,
    delivered_freight_value_brl

FROM ranked_sellers

WHERE seller_rank <= 15

ORDER BY seller_rank;


-- COMMAND ----------

-- DBTITLE 1,05 - Customer Repeat Order Segmentation

-- Identify repeat customers using customer_unique_id.
-- Repeat behavior is defined as two or more delivered orders.

WITH customer_order_history AS (

    SELECT
        c.customer_unique_id,

        COUNT(*) AS delivered_order_count,

        MIN(o.order_purchase_date)
            AS first_delivered_order_purchase_date,

        MAX(o.order_purchase_date)
            AS last_delivered_order_purchase_date

    FROM commerceai.gold.fct_orders AS o

    INNER JOIN commerceai.gold.dim_customer AS c
        ON o.customer_id = c.customer_id

    WHERE o.order_status = 'delivered'

    GROUP BY c.customer_unique_id
),

customer_segments AS (

    SELECT
        customer_unique_id,
        delivered_order_count,

        CASE
            WHEN delivered_order_count >= 2
                THEN 'REPEAT_CUSTOMER'
            ELSE 'ONE_DELIVERED_ORDER'
        END AS customer_segment

    FROM customer_order_history
),

segment_summary AS (

    SELECT
        customer_segment,

        COUNT(*) AS unique_customers,

        SUM(delivered_order_count)
            AS delivered_orders,

        ROUND(
            AVG(delivered_order_count),
            2
        ) AS avg_delivered_orders_per_customer

    FROM customer_segments

    GROUP BY customer_segment
)

SELECT
    customer_segment,
    unique_customers,
    delivered_orders,
    avg_delivered_orders_per_customer,

    ROUND(
        100.0 * unique_customers
        / SUM(unique_customers) OVER (),
        2
    ) AS customer_share_pct

FROM segment_summary

ORDER BY unique_customers DESC;


-- COMMAND ----------

-- DBTITLE 1,06 - Delivery Delay and Customer Review Analysis

-- Compare review scores between late and on-time deliveries.
-- Aggregate reviews per order before joining with delivery outcomes.

WITH reviews_by_order AS (

    SELECT
        order_id,

        COUNT(*) AS review_count,

        AVG(CAST(review_score AS DOUBLE))
            AS avg_review_score,

        MIN(review_score)
            AS lowest_review_score

    FROM commerceai.gold.fct_reviews

    GROUP BY order_id
),

eligible_reviewed_orders AS (

    SELECT
        o.order_id,

        o.is_late_delivery,
        o.days_late,

        r.review_count,
        r.avg_review_score,
        r.lowest_review_score

    FROM commerceai.gold.fct_orders AS o

    INNER JOIN reviews_by_order AS r
        ON o.order_id = r.order_id

    WHERE o.is_delivery_eligible = TRUE
)

SELECT
    CASE
        WHEN is_late_delivery = TRUE
            THEN 'LATE'
        ELSE 'ON_TIME'
    END AS delivery_group,

    COUNT(*) AS reviewed_orders,

    SUM(review_count) AS review_records,

    ROUND(
        AVG(avg_review_score),
        2
    ) AS avg_review_score_per_order,

    COUNT_IF(lowest_review_score <= 2)
        AS orders_with_low_review_score,

    ROUND(
        100.0 * COUNT_IF(lowest_review_score <= 2)
        / NULLIF(COUNT(*), 0),
        2
    ) AS low_review_order_rate_pct,

    ROUND(
        AVG(days_late),
        2
    ) AS avg_days_late

FROM eligible_reviewed_orders

GROUP BY
    CASE
        WHEN is_late_delivery = TRUE
            THEN 'LATE'
        ELSE 'ON_TIME'
    END

ORDER BY delivery_group;
