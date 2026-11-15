-- Databricks notebook source
-- DBTITLE 1,00 - ML Source Assessment Overview
-- MAGIC %md
-- MAGIC
-- MAGIC # Customer Repeat Purchase — ML Source Assessment
-- MAGIC
-- MAGIC ## Purpose
-- MAGIC Assess the feasibility of predicting whether a customer places another order within 90 days of their first observed order.
-- MAGIC
-- MAGIC ## Inputs
-- MAGIC - `commerceai.gold.fct_orders`
-- MAGIC - `commerceai.gold.dim_customer`
-- MAGIC
-- MAGIC ## Outputs
-- MAGIC - Source integrity and temporal coverage diagnostics
-- MAGIC - Session-scoped temporary views
-- MAGIC - Eligible customer cohorts
-- MAGIC - Preliminary 90-day repeat purchase labels
-- MAGIC - Class balance and candidate feature completeness diagnostics
-- MAGIC
-- MAGIC ## Main Responsibilities
-- MAGIC 1. Validate customer identity mapping.
-- MAGIC 2. Inspect historical order coverage.
-- MAGIC 3. Identify the first observed order per consumer.
-- MAGIC 4. Evaluate 90-day observation eligibility.
-- MAGIC 5. Compare provisional repeat purchase definitions.
-- MAGIC 6. Assess class imbalance and temporal limitations.
-- MAGIC
-- MAGIC ## Execution / Validation Notes
-- MAGIC - Execute all SQL cells in order.
-- MAGIC - Do not modify Bronze, Silver, or Gold tables.
-- MAGIC - Do not train or register ML models.
-- MAGIC - Final order status must not be used as an at-purchase feature.
-- MAGIC - Review the results before approving the predictive target.
-- MAGIC - The first observed order may not be the customer's first lifetime order.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,01 - Validate Order and Consumer Identity

SELECT
    COUNT(*) AS total_orders,
    COUNT(DISTINCT o.order_id) AS distinct_orders,
    COUNT(DISTINCT c.customer_unique_id) AS distinct_consumers,

    COUNT_IF(
        c.customer_unique_id IS NULL
    ) AS orders_missing_consumer_identity,

    COUNT_IF(
        o.order_purchase_timestamp IS NULL
    ) AS orders_missing_purchase_timestamp,

    MIN(o.order_purchase_timestamp) AS min_purchase_timestamp,
    MAX(o.order_purchase_timestamp) AS max_purchase_timestamp

FROM commerceai.gold.fct_orders AS o

LEFT JOIN commerceai.gold.dim_customer AS c
    ON o.customer_id = c.customer_id;


-- COMMAND ----------

-- DBTITLE 1,02 - Inspect Monthly Purchase Coverage

SELECT
    DATE_FORMAT(
        o.order_purchase_timestamp,
        'yyyy-MM'
    ) AS purchase_month,

    COUNT(*) AS total_orders,

    COUNT(DISTINCT c.customer_unique_id) AS unique_consumers

FROM commerceai.gold.fct_orders AS o

LEFT JOIN commerceai.gold.dim_customer AS c
    ON o.customer_id = c.customer_id

GROUP BY
    DATE_FORMAT(
        o.order_purchase_timestamp,
        'yyyy-MM'
    )

ORDER BY purchase_month;


-- COMMAND ----------

-- DBTITLE 1,03 - Identify First Observed Customer Orders

CREATE OR REPLACE TEMP VIEW vw_ml_repeat_purchase_order_events AS

SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_state,

    o.order_purchase_timestamp,
    o.order_status,

    o.item_count,
    o.ordered_total_value_brl,

    ROW_NUMBER() OVER (
        PARTITION BY c.customer_unique_id
        ORDER BY
            o.order_purchase_timestamp ASC,
            o.order_id ASC
    ) AS customer_order_rank

FROM commerceai.gold.fct_orders AS o

INNER JOIN commerceai.gold.dim_customer AS c
    ON o.customer_id = c.customer_id

WHERE
    c.customer_unique_id IS NOT NULL
    AND o.order_purchase_timestamp IS NOT NULL;


-- COMMAND ----------

-- DBTITLE 1,04 - Build Eligible Cohorts and Provisional Labels

CREATE OR REPLACE TEMP VIEW vw_ml_repeat_purchase_cohorts AS

WITH first_orders AS (

    SELECT
        customer_unique_id,
        order_id AS first_order_id,
        order_purchase_timestamp AS as_of_timestamp,
        order_status AS first_order_final_status,
        customer_state AS first_order_customer_state,
        item_count AS first_order_item_count,
        ordered_total_value_brl AS first_order_value_brl

    FROM vw_ml_repeat_purchase_order_events

    WHERE customer_order_rank = 1
),

followup_counts AS (

    SELECT
        f.customer_unique_id,

        COUNT(n.order_id) AS subsequent_orders_90d,

        SUM(
            CASE
                WHEN n.order_status = 'delivered' THEN 1
                ELSE 0
            END
        ) AS subsequent_delivered_orders_90d

    FROM first_orders AS f

    LEFT JOIN vw_ml_repeat_purchase_order_events AS n
        ON f.customer_unique_id = n.customer_unique_id
        AND n.order_purchase_timestamp > f.as_of_timestamp
        AND n.order_purchase_timestamp <= f.as_of_timestamp + INTERVAL 90 DAYS

    GROUP BY f.customer_unique_id
),

observation_scenarios AS (

    SELECT
        'OBSERVED_MAX' AS observation_scenario,
        MAX(order_purchase_timestamp) AS observation_end_timestamp
    FROM vw_ml_repeat_purchase_order_events

    UNION ALL

    SELECT
        'AUG_2018_SENSITIVITY' AS observation_scenario,
        TO_TIMESTAMP('2018-08-31 23:59:59') AS observation_end_timestamp
)

SELECT
    s.observation_scenario,
    s.observation_end_timestamp,

    f.customer_unique_id,
    f.first_order_id,
    f.as_of_timestamp,
    f.first_order_final_status,
    f.first_order_customer_state,
    f.first_order_item_count,
    f.first_order_value_brl,

    n.subsequent_orders_90d,
    n.subsequent_delivered_orders_90d,

    CASE
        WHEN n.subsequent_orders_90d > 0 THEN 1
        ELSE 0
    END AS repeat_order_90d_label,

    CASE
        WHEN n.subsequent_delivered_orders_90d > 0 THEN 1
        ELSE 0
    END AS repeat_delivered_order_90d_label

FROM first_orders AS f

INNER JOIN followup_counts AS n
    ON f.customer_unique_id = n.customer_unique_id

CROSS JOIN observation_scenarios AS s

WHERE
    f.as_of_timestamp <= s.observation_end_timestamp - INTERVAL 90 DAYS;


-- COMMAND ----------

-- DBTITLE 1,05 - Assess Label Balance and Feature Completeness

SELECT
    observation_scenario,

    MIN(observation_end_timestamp) AS observation_end_timestamp,

    COUNT(*) AS eligible_consumers,

    SUM(repeat_order_90d_label) AS repeat_order_positives,

    ROUND(
        AVG(repeat_order_90d_label) * 100,
        2
    ) AS repeat_order_rate_pct,

    SUM(repeat_delivered_order_90d_label)
        AS repeat_delivered_order_positives,

    ROUND(
        AVG(repeat_delivered_order_90d_label) * 100,
        2
    ) AS repeat_delivered_order_rate_pct,

    COUNT_IF(
        first_order_final_status <> 'delivered'
    ) AS first_orders_not_delivered,

    COUNT_IF(
        first_order_customer_state IS NULL
    ) AS missing_customer_state,

    COUNT_IF(
        first_order_item_count IS NULL
        OR first_order_item_count = 0
    ) AS first_orders_without_items,

    COUNT_IF(
        first_order_value_brl IS NULL
    ) AS missing_first_order_value

FROM vw_ml_repeat_purchase_cohorts

GROUP BY observation_scenario

ORDER BY observation_scenario;


-- COMMAND ----------

-- DBTITLE 1,06 - Inspect Monthly Eligible Cohorts

SELECT
    DATE_FORMAT(
        as_of_timestamp,
        'yyyy-MM'
    ) AS first_purchase_month,

    COUNT(*) AS eligible_consumers,

    SUM(repeat_order_90d_label) AS repeat_buyers_90d,

    ROUND(
        AVG(repeat_order_90d_label) * 100,
        2
    ) AS repeat_purchase_rate_pct

FROM vw_ml_repeat_purchase_cohorts

WHERE observation_scenario = 'AUG_2018_SENSITIVITY'

GROUP BY
    DATE_FORMAT(
        as_of_timestamp,
        'yyyy-MM'
    )

ORDER BY first_purchase_month;


-- COMMAND ----------

-- DBTITLE 1,07 - Review Positive and Negative Label Examples

WITH examples AS (

    SELECT
        customer_unique_id,
        first_order_id,
        as_of_timestamp,
        subsequent_orders_90d,
        subsequent_delivered_orders_90d,
        repeat_order_90d_label,

        ROW_NUMBER() OVER (
            PARTITION BY repeat_order_90d_label
            ORDER BY as_of_timestamp, customer_unique_id
        ) AS example_rank

    FROM vw_ml_repeat_purchase_cohorts

    WHERE observation_scenario = 'AUG_2018_SENSITIVITY'
)

SELECT
    customer_unique_id,
    first_order_id,
    as_of_timestamp,
    subsequent_orders_90d,
    subsequent_delivered_orders_90d,
    repeat_order_90d_label

FROM examples

WHERE example_rank <= 3

ORDER BY
    repeat_order_90d_label DESC,
    example_rank;


-- COMMAND ----------

-- DBTITLE 1,08 - Analyze Time to Second Purchase

WITH first_orders AS (

    SELECT
        customer_unique_id,
        as_of_timestamp

    FROM vw_ml_repeat_purchase_cohorts

    WHERE observation_scenario = 'AUG_2018_SENSITIVITY'
      AND repeat_order_90d_label = 1
),

second_purchases AS (

    SELECT
        f.customer_unique_id,
        f.as_of_timestamp,

        MIN(
            e.order_purchase_timestamp
        ) AS second_purchase_timestamp

    FROM first_orders AS f

    INNER JOIN vw_ml_repeat_purchase_order_events AS e
        ON f.customer_unique_id = e.customer_unique_id

       AND e.order_purchase_timestamp > f.as_of_timestamp

       AND e.order_purchase_timestamp <=
           f.as_of_timestamp + INTERVAL 90 DAYS

    GROUP BY
        f.customer_unique_id,
        f.as_of_timestamp
)

SELECT
    COUNT(*) AS repeat_buyers_90d,

    COUNT_IF(
        second_purchase_timestamp <=
        as_of_timestamp + INTERVAL 1 HOUR
    ) AS repeat_within_1_hour,

    COUNT_IF(
        second_purchase_timestamp <=
        as_of_timestamp + INTERVAL 1 DAY
    ) AS repeat_within_1_day,

    COUNT_IF(
        second_purchase_timestamp <=
        as_of_timestamp + INTERVAL 7 DAYS
    ) AS repeat_within_7_days,

    COUNT_IF(
        second_purchase_timestamp <=
        as_of_timestamp + INTERVAL 30 DAYS
    ) AS repeat_within_30_days,

    COUNT_IF(
        second_purchase_timestamp >
        as_of_timestamp + INTERVAL 30 DAYS
    ) AS repeat_after_30_days,

    ROUND(
        PERCENTILE_APPROX(
            (
                UNIX_TIMESTAMP(second_purchase_timestamp)
                - UNIX_TIMESTAMP(as_of_timestamp)
            ) / 86400.0,
            0.5
        ),
        2
    ) AS median_days_to_second_purchase

FROM second_purchases;
