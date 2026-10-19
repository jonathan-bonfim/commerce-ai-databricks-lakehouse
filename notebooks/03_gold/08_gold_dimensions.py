# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08 - Gold Dimensions
# MAGIC
# MAGIC **Purpose:** Build conformed Gold dimensions using Spark SQL embedded directly in Python notebook cells.
# MAGIC
# MAGIC **Inputs:** `commerceai.silver.sl_orders`, `commerceai.silver.sl_order_items`, `commerceai.silver.sl_reviews`, `commerceai.silver.sl_customers`, and `commerceai.silver.sl_geolocation`.
# MAGIC
# MAGIC **Outputs:** `commerceai.gold.dim_date` (one record per calendar date) and `commerceai.gold.dim_customer` (one record per `customer_id`).
# MAGIC
# MAGIC **Main Responsibilities:** Execute self-contained Delta table builds with `spark.sql(...)`; preserve consumer identity, source attributes, and missing geolocation; present lightweight previews. No external SQL file paths are required.
# MAGIC
# MAGIC **Execution/Validation Notes:** Builds use full `CREATE OR REPLACE TABLE` statements. Execute only the target build cell for incremental development, or run this notebook to rebuild both dimensions. Use `10_gold_validation` independently as the Gold quality gate. Source timestamps are cast with the active session time zone, without an arbitrary source time-zone conversion. Changes to the orchestration format do not require source Bronze/Silver changes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 - Inspect Session Time Zone

# COMMAND ----------

# DBTITLE 1,01 - Inspect Session Time Zone
# Record the session time zone used for timestamp-to-date conversions.
display(spark.sql("SELECT current_timezone() AS session_time_zone"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 - Build Date Dimension

# COMMAND ----------

# DBTITLE 1,02 - Build Date Dimension With Embedded SQL
# Grain: one record per calendar day. SQL is the complete transformation.
spark.sql("""
-- CommerceAI | Gold date dimension
-- Grain: one row per calendar date.
-- Coverage: complete calendar years containing order, item-shipping, and review dates.
-- Source timestamps are cast to DATE in the active session time zone.
-- No source time-zone conversion or hard-coded historical date range is applied.

CREATE OR REPLACE TABLE commerceai.gold.dim_date
USING DELTA
COMMENT 'Conformed calendar dimension with one row per date, covering source business-event dates.'
AS
WITH source_date_ranges AS (
    SELECT
        LEAST(
            MIN(TO_DATE(order_purchase_timestamp)),
            MIN(TO_DATE(order_approved_at)),
            MIN(TO_DATE(order_delivered_carrier_date)),
            MIN(TO_DATE(order_delivered_customer_date)),
            MIN(TO_DATE(order_estimated_delivery_date))
        ) AS first_event_date,
        GREATEST(
            MAX(TO_DATE(order_purchase_timestamp)),
            MAX(TO_DATE(order_approved_at)),
            MAX(TO_DATE(order_delivered_carrier_date)),
            MAX(TO_DATE(order_delivered_customer_date)),
            MAX(TO_DATE(order_estimated_delivery_date))
        ) AS last_event_date
    FROM commerceai.silver.sl_orders

    UNION ALL

    SELECT
        MIN(TO_DATE(shipping_limit_date)),
        MAX(TO_DATE(shipping_limit_date))
    FROM commerceai.silver.sl_order_items

    UNION ALL

    SELECT
        LEAST(
            MIN(TO_DATE(review_creation_date)),
            MIN(TO_DATE(review_answer_timestamp))
        ),
        GREATEST(
            MAX(TO_DATE(review_creation_date)),
            MAX(TO_DATE(review_answer_timestamp))
        )
    FROM commerceai.silver.sl_reviews
),
date_bounds AS (
    SELECT
        MAKE_DATE(YEAR(MIN(first_event_date)), 1, 1) AS first_date,
        MAKE_DATE(YEAR(MAX(last_event_date)), 12, 31) AS last_date
    FROM source_date_ranges
),
calendar_dates AS (
    SELECT
        EXPLODE(SEQUENCE(first_date, last_date, INTERVAL 1 DAY)) AS calendar_date
    FROM date_bounds
),
calendar_attributes AS (
    SELECT
        calendar_date,
        PMOD(DAYOFWEEK(calendar_date) + 5, 7) + 1 AS iso_weekday_number
    FROM calendar_dates
)
SELECT
    CAST(DATE_FORMAT(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
    calendar_date AS full_date,
    YEAR(calendar_date) AS year_number,
    QUARTER(calendar_date) AS quarter_number,
    MONTH(calendar_date) AS month_number,
    ELEMENT_AT(
        ARRAY('January', 'February', 'March', 'April', 'May', 'June',
              'July', 'August', 'September', 'October', 'November', 'December'),
        MONTH(calendar_date)
    ) AS month_name,
    DATE_FORMAT(calendar_date, 'yyyy-MM') AS year_month,
    TRUNC(calendar_date, 'MM') AS month_start_date,
    LAST_DAY(calendar_date) AS month_end_date,
    DAYOFMONTH(calendar_date) AS day_of_month,
    iso_weekday_number,
    ELEMENT_AT(
        ARRAY('Monday', 'Tuesday', 'Wednesday', 'Thursday',
              'Friday', 'Saturday', 'Sunday'),
        iso_weekday_number
    ) AS weekday_name,
    WEEKOFYEAR(calendar_date) AS iso_week_number,
    YEAR(DATE_ADD(calendar_date, 4 - iso_weekday_number)) AS iso_week_year,
    (iso_weekday_number IN (6, 7)) AS is_weekend
FROM calendar_attributes;
""")
print("Gold date dimension built: commerceai.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 - Preview Date Dimension

# COMMAND ----------

# DBTITLE 1,03 - Preview Date Dimension
display(spark.sql("""
SELECT
    date_key,
    full_date,
    year_number,
    month_name,
    iso_week_number,
    iso_week_year,
    is_weekend
FROM commerceai.gold.dim_date
ORDER BY full_date
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04 - Build Customer Dimension

# COMMAND ----------

# DBTITLE 1,04 - Build Customer Dimension With Embedded SQL
# Grain: one record per customer_id. LEFT JOIN preserves unmatched ZIP prefixes.
spark.sql("""
-- CommerceAI | Gold customer dimension
-- Grain: one row per customer_id (order-level customer record).
-- The same customer_unique_id may appear in multiple rows because the source
-- assigns a different customer_id for different orders.
-- LEFT JOIN preserves customers without a geolocation match.
-- Customer address attributes remain authoritative; reference geolocation
-- attributes are additional information, not replacements.

CREATE OR REPLACE TABLE commerceai.gold.dim_customer
USING DELTA
COMMENT 'Customer dimension at customer_id grain with original customer attributes and optional ZIP-level reference geolocation.'
AS
SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    g.geolocation_lat,
    g.geolocation_lng,
    g.geolocation_city AS reference_geolocation_city,
    g.geolocation_state AS reference_geolocation_state,
    (g.geolocation_zip_code_prefix IS NOT NULL) AS has_geolocation_match,
    (
        g.geolocation_lat IS NOT NULL
        AND g.geolocation_lng IS NOT NULL
    ) AS has_geolocation_coordinates,
    CASE
        WHEN g.geolocation_zip_code_prefix IS NULL THEN 'NOT_FOUND'
        ELSE g.geolocation_coordinate_status
    END AS geolocation_coordinate_status,
    g.geolocation_has_state_conflict,
    g.geolocation_has_city_variation
FROM commerceai.silver.sl_customers AS c
LEFT JOIN commerceai.silver.sl_geolocation AS g
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix;
""")
print("Gold customer dimension built: commerceai.gold.dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05 - Preview Customer Dimension

# COMMAND ----------

# DBTITLE 1,05 - Preview Customer Dimension
display(spark.sql("""
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_state,
    has_geolocation_match,
    has_geolocation_coordinates,
    geolocation_coordinate_status
FROM commerceai.gold.dim_customer
ORDER BY customer_id
LIMIT 10
"""))