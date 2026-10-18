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
