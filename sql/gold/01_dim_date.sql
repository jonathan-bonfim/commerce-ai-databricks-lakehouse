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
