# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 09 — Gold Facts
# MAGIC
# MAGIC **Purpose:** Build analytical fact tables in the CommerceAI Gold layer using embedded Spark SQL.
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `commerceai.silver.sl_orders`
# MAGIC - `commerceai.silver.sl_order_items`
# MAGIC - `commerceai.silver.sl_payments`
# MAGIC
# MAGIC **Outputs:**
# MAGIC - `commerceai.gold.fct_orders`
# MAGIC - `commerceai.gold.fct_order_items`
# MAGIC - `commerceai.gold.fct_payments`
# MAGIC - `commerceai.gold.fct_reviews`
# MAGIC
# MAGIC **Main Responsibilities:**
# MAGIC - Preserve one row per order.
# MAGIC - Aggregate order items and payments independently.
# MAGIC - Preserve financial differences and missing source relationships.
# MAGIC - Create historical delivery performance metrics.
# MAGIC - Use Delta tables for analytical consumption.
# MAGIC
# MAGIC **Execution / Validation Notes:**
# MAGIC - Run after the Silver validation gate and Gold dimensions.
# MAGIC - Execute transformations using `spark.sql()`.
# MAGIC - Do not join item-level and payment-level records directly.
# MAGIC - Keep monetary values as exact decimals.
# MAGIC - Apply date-based delivery comparisons without assuming source timezone.
# MAGIC - Run `10_gold_validation` separately after building the facts.
# MAGIC - The current implementation uses full-load replacement.

# COMMAND ----------

# DBTITLE 1,01 - Verify Session Time Zone
# Keep date interpretation consistent with the existing Gold dimensions.

session_timezone = spark.conf.get("spark.sql.session.timeZone")

print(f"Spark session time zone: {session_timezone}")

if session_timezone not in {"UTC", "Etc/UTC"}:
    raise ValueError(
        "Review the Spark session time zone before building "
        "date-based Gold metrics."
    )

# COMMAND ----------

# DBTITLE 1,02 - Build Order Fact With Embedded SQL

# Grain: one row per order_id.
# Item and payment aggregates must remain independent.

spark.sql("""
CREATE OR REPLACE TABLE commerceai.gold.fct_orders
USING DELTA
COMMENT 'Order-level analytical fact with independent financial aggregates and historical delivery metrics.'
AS

WITH item_aggregates AS (

    SELECT
        order_id,
        COUNT(*) AS item_count,
        COUNT(DISTINCT product_id) AS distinct_product_count,
        COUNT(DISTINCT seller_id) AS distinct_seller_count,

        SUM(price) AS item_value_brl,
        SUM(freight_value) AS freight_value_brl,
        SUM(price + freight_value) AS ordered_total_value_brl

    FROM commerceai.silver.sl_order_items
    GROUP BY order_id

),

payment_aggregates AS (

    SELECT
        order_id,
        COUNT(*) AS payment_count,
        SUM(payment_value) AS paid_value_brl

    FROM commerceai.silver.sl_payments
    GROUP BY order_id

),

order_enriched AS (

    SELECT
        o.order_id,
        o.customer_id,
        o.order_status,

        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        CAST(o.order_purchase_timestamp AS DATE)
            AS order_purchase_date,

        CAST(o.order_estimated_delivery_date AS DATE)
            AS estimated_delivery_date,

        CAST(o.order_delivered_customer_date AS DATE)
            AS actual_delivery_date,

        CAST(
            DATE_FORMAT(o.order_purchase_timestamp, 'yyyyMMdd')
            AS INT
        ) AS order_purchase_date_key,

        CAST(
            DATE_FORMAT(o.order_estimated_delivery_date, 'yyyyMMdd')
            AS INT
        ) AS estimated_delivery_date_key,

        CAST(
            DATE_FORMAT(o.order_delivered_customer_date, 'yyyyMMdd')
            AS INT
        ) AS actual_delivery_date_key,

        (i.order_id IS NOT NULL) AS has_order_items,
        (p.order_id IS NOT NULL) AS has_payments,

        COALESCE(i.item_count, 0) AS item_count,
        COALESCE(i.distinct_product_count, 0)
            AS distinct_product_count,
        COALESCE(i.distinct_seller_count, 0)
            AS distinct_seller_count,
        COALESCE(p.payment_count, 0) AS payment_count,

        i.item_value_brl,
        i.freight_value_brl,
        i.ordered_total_value_brl,
        p.paid_value_brl,

        CASE
            WHEN i.order_id IS NOT NULL
             AND p.order_id IS NOT NULL
            THEN p.paid_value_brl - i.ordered_total_value_brl
        END AS payment_difference_brl,

        CASE
            WHEN i.order_id IS NOT NULL
             AND p.order_id IS NOT NULL
            THEN ABS(
                p.paid_value_brl - i.ordered_total_value_brl
            ) > CAST(0.01 AS DECIMAL(18, 2))
        END AS payment_item_mismatch_flag,

        (
            o.order_status = 'delivered'
            AND o.order_delivered_customer_date IS NOT NULL
            AND o.order_estimated_delivery_date IS NOT NULL
        ) AS is_delivery_eligible,

        (
            o.order_status = 'delivered'
            AND o.order_delivered_customer_date IS NULL
        ) AS delivered_without_delivery_date,

        CASE
            WHEN o.order_delivered_carrier_date IS NOT NULL
             AND o.order_delivered_customer_date IS NOT NULL
            THEN o.order_delivered_carrier_date >
                 o.order_delivered_customer_date
        END AS carrier_after_customer_delivery_flag

    FROM commerceai.silver.sl_orders AS o

    LEFT JOIN item_aggregates AS i
        ON o.order_id = i.order_id

    LEFT JOIN payment_aggregates AS p
        ON o.order_id = p.order_id
)

SELECT
    order_id,
    customer_id,
    order_status,

    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    order_purchase_date,
    estimated_delivery_date,
    actual_delivery_date,

    order_purchase_date_key,
    estimated_delivery_date_key,
    actual_delivery_date_key,

    has_order_items,
    has_payments,

    item_count,
    distinct_product_count,
    distinct_seller_count,
    payment_count,

    item_value_brl,
    freight_value_brl,
    ordered_total_value_brl,
    paid_value_brl,

    payment_difference_brl,
    payment_item_mismatch_flag,

    is_delivery_eligible,

    CASE
        WHEN is_delivery_eligible
        THEN actual_delivery_date > estimated_delivery_date
    END AS is_late_delivery,

    CASE
        WHEN is_delivery_eligible
        THEN GREATEST(
            DATEDIFF(actual_delivery_date, estimated_delivery_date),
            0
        )
    END AS days_late,

    CASE
        WHEN order_status = 'delivered'
         AND actual_delivery_date IS NOT NULL
         AND order_purchase_date IS NOT NULL
        THEN DATEDIFF(actual_delivery_date, order_purchase_date)
    END AS delivery_duration_days,

    CASE
        WHEN estimated_delivery_date IS NOT NULL
         AND order_purchase_date IS NOT NULL
        THEN DATEDIFF(
            estimated_delivery_date,
            order_purchase_date
        )
    END AS promised_delivery_days,

    delivered_without_delivery_date,
    carrier_after_customer_delivery_flag

FROM order_enriched
""")

print("Gold order fact built: commerceai.gold.fct_orders")


# COMMAND ----------

# DBTITLE 1,03 - Preview Order Fact
display(spark.sql("""
SELECT
    order_id,
    customer_id,
    order_status,
    item_count,
    distinct_seller_count,
    payment_count,
    ordered_total_value_brl,
    paid_value_brl,
    payment_difference_brl,
    is_delivery_eligible,
    is_late_delivery,
    days_late
FROM commerceai.gold.fct_orders
ORDER BY order_purchase_timestamp, order_id
LIMIT 10
"""))

# COMMAND ----------

# DBTITLE 1,04 - Build Order Items Fact With Embedded SQL

# Grain: one row per order_id and order_item_id.
# Preserve transactional item values without allocating order payments.
# Enrich each item with its order-level customer, status and purchase date.

spark.sql("""
CREATE OR REPLACE TABLE commerceai.gold.fct_order_items
USING DELTA
COMMENT 'Transactional order item fact with product, seller, customer, purchase date and item-level amounts.'
AS

SELECT
    -- Business keys and dimensional relationships
    i.order_id,
    i.order_item_id,
    o.customer_id,
    i.product_id,
    i.seller_id,

    -- Order context
    o.order_status,
    o.order_purchase_timestamp,

    CAST(o.order_purchase_timestamp AS DATE)
        AS order_purchase_date,

    CAST(
        DATE_FORMAT(o.order_purchase_timestamp, 'yyyyMMdd')
        AS INT
    ) AS order_purchase_date_key,

    -- Item shipping deadline, not the promised customer delivery date
    i.shipping_limit_date,

    CAST(i.shipping_limit_date AS DATE)
        AS shipping_limit_calendar_date,

    CAST(
        DATE_FORMAT(i.shipping_limit_date, 'yyyyMMdd')
        AS INT
    ) AS shipping_limit_date_key,

    -- Item-level financial metrics
    i.price AS item_value_brl,

    i.freight_value AS freight_value_brl,

    (i.price + i.freight_value)
        AS ordered_total_value_brl

FROM commerceai.silver.sl_order_items AS i

LEFT JOIN commerceai.silver.sl_orders AS o
    ON i.order_id = o.order_id
""")

print("Gold order items fact built: commerceai.gold.fct_order_items")


# COMMAND ----------

# DBTITLE 1,05 - Preview Order Items Fact
display(spark.sql("""
SELECT
    order_id,
    order_item_id,
    customer_id,
    product_id,
    seller_id,
    order_status,
    order_purchase_date,
    shipping_limit_calendar_date,
    item_value_brl,
    freight_value_brl,
    ordered_total_value_brl
FROM commerceai.gold.fct_order_items
ORDER BY order_id, order_item_id
LIMIT 10
"""))

# COMMAND ----------

# DBTITLE 1,06 - Build Payments Fact With Embedded SQL

# Grain: one row per order_id and payment_sequential.
# Preserve payment records, methods, installments and exact amounts.
# The order purchase date is contextual, not an actual payment timestamp.

spark.sql("""
CREATE OR REPLACE TABLE commerceai.gold.fct_payments
USING DELTA
COMMENT 'Payment-level fact preserving payment methods, installments, exact amounts and order context.'
AS

SELECT
    -- Payment business keys
    p.order_id,
    p.payment_sequential,

    -- Order context and dimensional relationships
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,

    CAST(o.order_purchase_timestamp AS DATE)
        AS order_purchase_date,

    CAST(
        DATE_FORMAT(o.order_purchase_timestamp, 'yyyyMMdd')
        AS INT
    ) AS order_purchase_date_key,

    -- Original payment attributes
    p.payment_type,
    p.payment_installments,
    p.payment_value AS payment_value_brl,

    -- Payment quality indicators
    (
        p.payment_value = CAST(0.00 AS DECIMAL(18, 2))
    ) AS is_zero_value_payment,

    (
        p.payment_installments = 0
    ) AS has_zero_installments

FROM commerceai.silver.sl_payments AS p

LEFT JOIN commerceai.silver.sl_orders AS o
    ON p.order_id = o.order_id
""")

print("Gold payments fact built: commerceai.gold.fct_payments")


# COMMAND ----------

# DBTITLE 1,07 - Preview Payments Fact
display(spark.sql("""
SELECT
    order_id,
    payment_sequential,
    customer_id,
    order_status,
    order_purchase_date,
    payment_type,
    payment_installments,
    payment_value_brl,
    is_zero_value_payment,
    has_zero_installments
FROM commerceai.gold.fct_payments
ORDER BY order_id, payment_sequential
LIMIT 10
"""))

# COMMAND ----------

# DBTITLE 1,08 - Build Reviews Fact With Embedded SQL

# Grain: one row per review_id and order_id.
# Preserve review scores and original text without normalization.
# Do not deduplicate reviews by review_id or order_id.

spark.sql("""
CREATE OR REPLACE TABLE commerceai.gold.fct_reviews
USING DELTA
COMMENT 'Review-level fact preserving original feedback text, review scores, timestamps and order context.'
AS

SELECT
    -- Review business keys
    r.review_id,
    r.order_id,

    -- Order context
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,

    CAST(o.order_purchase_timestamp AS DATE)
        AS order_purchase_date,

    CAST(
        DATE_FORMAT(o.order_purchase_timestamp, 'yyyyMMdd')
        AS INT
    ) AS order_purchase_date_key,

    -- Original review attributes
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp,

    -- Review calendar dates
    CAST(r.review_creation_date AS DATE)
        AS review_creation_calendar_date,

    CAST(r.review_answer_timestamp AS DATE)
        AS review_answer_calendar_date,

    -- Date dimension keys
    CAST(
        DATE_FORMAT(r.review_creation_date, 'yyyyMMdd')
        AS INT
    ) AS review_creation_date_key,

    CAST(
        DATE_FORMAT(r.review_answer_timestamp, 'yyyyMMdd')
        AS INT
    ) AS review_answer_date_key,

    -- Text presence indicators
    (r.review_comment_title IS NOT NULL)
        AS has_review_title,

    (r.review_comment_message IS NOT NULL)
        AS has_review_message

FROM commerceai.silver.sl_reviews AS r

LEFT JOIN commerceai.silver.sl_orders AS o
    ON r.order_id = o.order_id
""")

print("Gold reviews fact built: commerceai.gold.fct_reviews")


# COMMAND ----------

# DBTITLE 1,09 - Preview Reviews Fact
display(spark.sql("""
SELECT
    review_id,
    order_id,
    customer_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_calendar_date,
    review_answer_calendar_date,
    has_review_title,
    has_review_message
FROM commerceai.gold.fct_reviews
ORDER BY review_id, order_id
LIMIT 10
"""))