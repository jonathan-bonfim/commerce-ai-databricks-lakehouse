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