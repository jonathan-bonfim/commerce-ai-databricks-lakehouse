# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08 — Gold Dimensions
# MAGIC
# MAGIC **Purpose:** Build four conformed Gold dimensions using Spark SQL embedded in Python cells.
# MAGIC
# MAGIC **Inputs:** `commerceai.silver.sl_orders`, `sl_order_items`, `sl_reviews`, `sl_customers`, `sl_geolocation`, `sl_products`, `sl_product_category_translation`, and `sl_sellers`.
# MAGIC
# MAGIC **Outputs:** `commerceai.gold.dim_date`, `dim_customer`, `dim_product`, and `dim_seller`.
# MAGIC
# MAGIC **Main Responsibilities:** Preserve source entity grains, customer identity, optional ZIP-level geolocation, product category gaps, and a complete calendar. All business transformations remain in `spark.sql(...)`.
# MAGIC
# MAGIC **Execution / Validation Notes:** This notebook is a **build step**. Run in order before `09_gold_facts`; validate with `10_gold_validation` after both build notebooks. The SQL uses `CREATE OR REPLACE TABLE` and replaces each table independently. Review the session timezone before converting timestamps to dates. No previews, exploratory profiling, or independent quality gates run here.

# COMMAND ----------

# DBTITLE 1,01 - Verify Session Time Zone
# Keep timestamp-to-date conversion consistent with the validated UTC-session Gold baseline.
session_timezone = spark.conf.get("spark.sql.session.timeZone")
print(f"Spark session time zone: {session_timezone}")
if session_timezone not in {"UTC", "Etc/UTC"}:
    raise ValueError("Review the Spark session time zone before rebuilding Gold dimensions.")

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

# DBTITLE 1,03 - Build Customer Dimension With Embedded SQL
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

# DBTITLE 1,04 - Build Product Dimension With Embedded SQL
# Grain: one row per product_id. Preserve nullable product attributes and translation gaps.
spark.sql("""
-- CommerceAI | Gold product dimension
-- Grain: exactly one row per product_id.
-- Product attributes remain faithful to Silver, including NULL and zero values.
-- Missing category and missing English translation are distinct statuses.
-- A LEFT JOIN to the unique translation key does not discard source products.

CREATE OR REPLACE TABLE commerceai.gold.dim_product
USING DELTA
COMMENT 'Product dimension at product_id grain with original product attributes and nullable English category translation.'
AS
SELECT
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    CASE
        WHEN p.product_category_name IS NULL THEN 'MISSING_CATEGORY'
        WHEN t.product_category_name_english IS NULL THEN 'UNTRANSLATED'
        ELSE 'TRANSLATED'
    END AS product_category_status,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM commerceai.silver.sl_products AS p
LEFT JOIN commerceai.silver.sl_product_category_translation AS t
    ON p.product_category_name = t.product_category_name;
""")
print("Gold product dimension built: commerceai.gold.dim_product")

# COMMAND ----------

# DBTITLE 1,05 - Build Seller Dimension With Embedded SQL
# Grain: one row per seller_id.
# Preserve seller attributes and enrich with optional ZIP-level geolocation.

spark.sql("""
CREATE OR REPLACE TABLE commerceai.gold.dim_seller
USING DELTA
COMMENT 'Seller dimension at seller_id grain with original seller attributes and optional reference geolocation.'
AS
SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,

    g.geolocation_lat,
    g.geolocation_lng,
    g.geolocation_city AS reference_geolocation_city,
    g.geolocation_state AS reference_geolocation_state,

    (g.geolocation_zip_code_prefix IS NOT NULL)
        AS has_geolocation_match,

    (
        g.geolocation_lat IS NOT NULL
        AND g.geolocation_lng IS NOT NULL
    ) AS has_geolocation_coordinates,

    CASE
        WHEN g.geolocation_zip_code_prefix IS NULL
            THEN 'NOT_FOUND'
        ELSE g.geolocation_coordinate_status
    END AS geolocation_coordinate_status,

    g.geolocation_has_state_conflict,
    g.geolocation_has_city_variation

FROM commerceai.silver.sl_sellers AS s

LEFT JOIN commerceai.silver.sl_geolocation AS g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
""")

print("Gold seller dimension built: commerceai.gold.dim_seller")