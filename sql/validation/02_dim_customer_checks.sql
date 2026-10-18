-- CommerceAI | Gold customer dimension validation
-- Grain: one row per customer_id.
-- This query returns independent PASS/FAIL contracts. Missing geolocation
-- remains valid when faithfully preserved from the Silver source.
-- The reference counts (99,441 rows; 96,096 unique consumers; 278 unmatched
-- geolocation records) are historical baselines, not hard-coded filters.

WITH source_stats AS (
    SELECT
        COUNT(*) AS source_rows,
        COUNT(DISTINCT customer_unique_id) AS source_unique_consumers
    FROM commerceai.silver.sl_customers
),
source_geo_stats AS (
    SELECT
        SUM(CASE WHEN g.geolocation_zip_code_prefix IS NULL THEN 1 ELSE 0 END)
            AS source_unmatched_geolocation_rows
    FROM commerceai.silver.sl_customers AS c
    LEFT JOIN commerceai.silver.sl_geolocation AS g
        ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
),
gold_stats AS (
    SELECT
        COUNT(*) AS gold_rows,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        COUNT(DISTINCT customer_unique_id) AS distinct_unique_consumers,
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
        SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END)
            AS null_unique_consumer_ids,
        SUM(CASE WHEN has_geolocation_match = FALSE THEN 1 ELSE 0 END)
            AS gold_unmatched_geolocation_rows,
        SUM(
            CASE
                WHEN has_geolocation_match IS NULL
                  OR has_geolocation_coordinates IS NULL
                  OR geolocation_coordinate_status IS NULL
                  OR geolocation_coordinate_status NOT IN ('AVAILABLE', 'MISSING', 'NOT_FOUND')
                  OR (has_geolocation_match = FALSE AND (
                       geolocation_coordinate_status <> 'NOT_FOUND'
                       OR has_geolocation_coordinates = TRUE
                       OR geolocation_lat IS NOT NULL
                       OR geolocation_lng IS NOT NULL
                  ))
                  OR (has_geolocation_match = TRUE AND geolocation_coordinate_status = 'NOT_FOUND')
                  OR NOT (
                       has_geolocation_coordinates <=>
                       (geolocation_lat IS NOT NULL AND geolocation_lng IS NOT NULL)
                  )
                  OR (geolocation_coordinate_status = 'AVAILABLE' AND has_geolocation_coordinates = FALSE)
                  OR (geolocation_coordinate_status = 'MISSING' AND has_geolocation_coordinates = TRUE)
                THEN 1 ELSE 0
            END
        ) AS invalid_geolocation_flag_rows
    FROM commerceai.gold.dim_customer
),
extra_gold_keys AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.gold.dim_customer AS d
    LEFT ANTI JOIN commerceai.silver.sl_customers AS c
        ON d.customer_id = c.customer_id
),
customer_attribute_issues AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.gold.dim_customer AS d
    INNER JOIN commerceai.silver.sl_customers AS c
        ON d.customer_id = c.customer_id
    WHERE NOT (
        (d.customer_unique_id <=> c.customer_unique_id)
        AND (d.customer_zip_code_prefix <=> c.customer_zip_code_prefix)
        AND (d.customer_city <=> c.customer_city)
        AND (d.customer_state <=> c.customer_state)
    )
),
geolocation_attribute_issues AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.gold.dim_customer AS d
    INNER JOIN commerceai.silver.sl_customers AS c
        ON d.customer_id = c.customer_id
    LEFT JOIN commerceai.silver.sl_geolocation AS g
        ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
    WHERE NOT (
        (d.geolocation_lat <=> g.geolocation_lat)
        AND (d.geolocation_lng <=> g.geolocation_lng)
        AND (d.reference_geolocation_city <=> g.geolocation_city)
        AND (d.reference_geolocation_state <=> g.geolocation_state)
        AND (d.has_geolocation_match <=> (g.geolocation_zip_code_prefix IS NOT NULL))
        AND (d.has_geolocation_coordinates <=>
             (g.geolocation_lat IS NOT NULL AND g.geolocation_lng IS NOT NULL))
        AND (d.geolocation_coordinate_status <=>
             CASE
                 WHEN g.geolocation_zip_code_prefix IS NULL THEN 'NOT_FOUND'
                 ELSE g.geolocation_coordinate_status
             END)
        AND (d.geolocation_has_state_conflict <=> g.geolocation_has_state_conflict)
        AND (d.geolocation_has_city_variation <=> g.geolocation_has_city_variation)
    )
),
checks AS (
    SELECT
        'row_count_matches_silver' AS check_name,
        CAST(s.source_rows AS STRING) AS expected_value,
        CAST(d.gold_rows AS STRING) AS observed_value,
        IF(s.source_rows = d.gold_rows, 'PASS', 'FAIL') AS status
    FROM source_stats AS s CROSS JOIN gold_stats AS d

    UNION ALL
    SELECT
        'customer_id_is_not_null_and_unique',
        '0 null keys and one unique key per row',
        CONCAT('null=', CAST(d.null_customer_ids AS STRING),
               ', distinct=', CAST(d.distinct_customer_ids AS STRING),
               ', rows=', CAST(d.gold_rows AS STRING)),
        IF(d.null_customer_ids = 0 AND d.distinct_customer_ids = d.gold_rows, 'PASS', 'FAIL')
    FROM gold_stats AS d

    UNION ALL
    SELECT
        'customer_unique_id_is_not_null',
        '0',
        CAST(d.null_unique_consumer_ids AS STRING),
        IF(d.null_unique_consumer_ids = 0, 'PASS', 'FAIL')
    FROM gold_stats AS d

    UNION ALL
    SELECT
        'consumer_identity_count_matches_silver',
        CAST(s.source_unique_consumers AS STRING),
        CAST(d.distinct_unique_consumers AS STRING),
        IF(s.source_unique_consumers = d.distinct_unique_consumers, 'PASS', 'FAIL')
    FROM source_stats AS s CROSS JOIN gold_stats AS d

    UNION ALL
    SELECT
        'no_extra_customer_ids',
        '0',
        CAST(k.issue_count AS STRING),
        IF(k.issue_count = 0, 'PASS', 'FAIL')
    FROM extra_gold_keys AS k

    UNION ALL
    SELECT
        'source_customer_attributes_preserved',
        '0',
        CAST(a.issue_count AS STRING),
        IF(a.issue_count = 0, 'PASS', 'FAIL')
    FROM customer_attribute_issues AS a

    UNION ALL
    SELECT
        'unmatched_geolocation_rows_preserved',
        CAST(s.source_unmatched_geolocation_rows AS STRING),
        CAST(d.gold_unmatched_geolocation_rows AS STRING),
        IF(s.source_unmatched_geolocation_rows = d.gold_unmatched_geolocation_rows, 'PASS', 'FAIL')
    FROM source_geo_stats AS s CROSS JOIN gold_stats AS d

    UNION ALL
    SELECT
        'geolocation_attributes_match_silver',
        '0',
        CAST(g.issue_count AS STRING),
        IF(g.issue_count = 0, 'PASS', 'FAIL')
    FROM geolocation_attribute_issues AS g

    UNION ALL
    SELECT
        'geolocation_flags_are_consistent',
        '0',
        CAST(d.invalid_geolocation_flag_rows AS STRING),
        IF(d.invalid_geolocation_flag_rows = 0, 'PASS', 'FAIL')
    FROM gold_stats AS d
)
SELECT check_name, expected_value, observed_value, status
FROM checks
ORDER BY check_name;
