-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 10 - Gold Validation
-- MAGIC
-- MAGIC **Purpose:** Validate Gold dimensions using pure Spark SQL and stop execution when a contract fails.
-- MAGIC
-- MAGIC **Inputs:** `commerceai.gold.dim_date`, `commerceai.gold.dim_customer`, `commerceai.gold.dim_product`, `commerceai.gold.dim_seller` and their Silver source tables.
-- MAGIC
-- MAGIC **Outputs:** Session-scoped validation views and visible quality-check tables; no persisted Gold validation datasets.
-- MAGIC
-- MAGIC **Main Responsibilities:** Preserve seven date and nine customer validation contracts, add eight product contracts, report results, and apply SQL failure gates with `assert_true`.
-- MAGIC
-- MAGIC **Execution/Validation Notes:** Run independently after building Gold dimensions. Each `CREATE TEMP VIEW` computes checks lazily and uses the current session. `assert_true` must be evaluated in the following gate cells; a non-PASS check should fail the notebook/job. No external `.sql` files or Python cells required.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 01 - Prepare Date Dimension Validation Checks

-- COMMAND ----------

-- DBTITLE 1,01 - Prepare Date Dimension Validation Checks
CREATE OR REPLACE TEMP VIEW gold_date_validation_results AS
-- CommerceAI | Gold date dimension validation
-- Run independently of the Gold build notebook.
-- Expected output: one PASS/FAIL result for each contract.

WITH metrics AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT date_key) AS unique_date_keys,
        MIN(full_date) AS first_date,
        MAX(full_date) AS last_date,
        SUM(CASE
            WHEN date_key IS NULL OR full_date IS NULL THEN 1 ELSE 0
        END) AS null_key_or_date_rows,
        SUM(CASE
            WHEN date_key <> CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT)
            THEN 1 ELSE 0
        END) AS incorrect_date_key_rows,
        SUM(CASE
            WHEN iso_weekday_number NOT BETWEEN 1 AND 7
                 OR is_weekend <> (iso_weekday_number IN (6, 7))
                 OR iso_weekday_number IS NULL
                 OR is_weekend IS NULL
            THEN 1 ELSE 0
        END) AS incorrect_weekday_rows
    FROM commerceai.gold.dim_date
),
checks AS (
    SELECT
        'dimension_is_not_empty' AS check_name,
        '> 0' AS expected_value,
        CAST(total_rows AS STRING) AS observed_value,
        IF(total_rows > 0, 'PASS', 'FAIL') AS status
    FROM metrics

    UNION ALL
    SELECT
        'date_key_is_unique',
        CAST(total_rows AS STRING),
        CAST(unique_date_keys AS STRING),
        IF(total_rows = unique_date_keys, 'PASS', 'FAIL')
    FROM metrics

    UNION ALL
    SELECT
        'date_and_key_are_not_null',
        '0',
        CAST(null_key_or_date_rows AS STRING),
        IF(null_key_or_date_rows = 0, 'PASS', 'FAIL')
    FROM metrics

    UNION ALL
    SELECT
        'calendar_has_no_date_gaps',
        CAST(DATEDIFF(last_date, first_date) + 1 AS STRING),
        CAST(total_rows AS STRING),
        IF(total_rows = DATEDIFF(last_date, first_date) + 1, 'PASS', 'FAIL')
    FROM metrics

    UNION ALL
    SELECT
        'calendar_covers_complete_years',
        'January 1 through December 31',
        CONCAT(CAST(first_date AS STRING), ' through ', CAST(last_date AS STRING)),
        IF(
            MONTH(first_date) = 1 AND DAYOFMONTH(first_date) = 1
            AND MONTH(last_date) = 12 AND DAYOFMONTH(last_date) = 31,
            'PASS', 'FAIL'
        )
    FROM metrics

    UNION ALL
    SELECT
        'date_key_matches_full_date',
        '0',
        CAST(incorrect_date_key_rows AS STRING),
        IF(incorrect_date_key_rows = 0, 'PASS', 'FAIL')
    FROM metrics

    UNION ALL
    SELECT
        'weekday_attributes_are_consistent',
        '0',
        CAST(incorrect_weekday_rows AS STRING),
        IF(incorrect_weekday_rows = 0, 'PASS', 'FAIL')
    FROM metrics
)
SELECT
    check_name,
    expected_value,
    observed_value,
    status
FROM checks;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 02 - Review Date Dimension Validation Results

-- COMMAND ----------

-- DBTITLE 1,02 - Review Date Dimension Validation Results
SELECT
    check_name,
    expected_value,
    observed_value,
    status
FROM gold_date_validation_results
ORDER BY check_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 03 - Enforce Date Dimension Quality Gate

-- COMMAND ----------

-- DBTITLE 1,03 - Enforce Date Dimension Quality Gate
SELECT
    COUNT(*) AS total_checks,
    COUNT_IF(status = 'PASS') AS passed_checks,
    COUNT(*) - COUNT_IF(status = 'PASS') AS failed_checks,
    ASSERT_TRUE(
        COUNT(*) = 7 AND COUNT_IF(status = 'PASS') = 7,
        'Gold date dimension validation failed. Review the preceding checks.'
    ) AS validation_gate
FROM gold_date_validation_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 04 - Prepare Customer Dimension Validation Checks

-- COMMAND ----------

-- DBTITLE 1,04 - Prepare Customer Dimension Validation Checks
CREATE OR REPLACE TEMP VIEW gold_customer_validation_results AS
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
FROM checks;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 05 - Review Customer Dimension Validation Results

-- COMMAND ----------

-- DBTITLE 1,05 - Review Customer Dimension Validation Results
SELECT
    check_name,
    expected_value,
    observed_value,
    status
FROM gold_customer_validation_results
ORDER BY check_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 06 - Enforce Customer Dimension Quality Gate

-- COMMAND ----------

-- DBTITLE 1,06 - Enforce Customer Dimension Quality Gate
SELECT
    COUNT(*) AS total_checks,
    COUNT_IF(status = 'PASS') AS passed_checks,
    COUNT(*) - COUNT_IF(status = 'PASS') AS failed_checks,
    ASSERT_TRUE(
        COUNT(*) = 9 AND COUNT_IF(status = 'PASS') = 9,
        'Gold customer dimension validation failed. Review the preceding checks.'
    ) AS validation_gate
FROM gold_customer_validation_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 07 - Prepare Product Dimension Validation Checks

-- COMMAND ----------

-- DBTITLE 1,07 - Prepare Product Dimension Validation Checks
CREATE OR REPLACE TEMP VIEW gold_product_validation_results AS
-- CommerceAI | Gold product dimension validation
-- Grain: one row per product_id.
-- Missing product categories and untranslated product categories are distinct.
-- Baselines in Silver: 32,951 products; 610 without category;
-- 13 with a category name but no matching English translation.
-- Comparisons use current Silver values rather than fixed hard-coded counts.
WITH source_stats AS (
    SELECT
        COUNT(*) AS source_rows,
        COUNT_IF(product_category_name IS NULL) AS source_missing_category_rows
    FROM commerceai.silver.sl_products
),
source_translation_stats AS (
    SELECT
        COUNT_IF(
            p.product_category_name IS NOT NULL
            AND t.product_category_name_english IS NULL
        ) AS source_untranslated_rows
    FROM commerceai.silver.sl_products AS p
    LEFT JOIN commerceai.silver.sl_product_category_translation AS t
        ON p.product_category_name = t.product_category_name
),
gold_stats AS (
    SELECT
        COUNT(*) AS gold_rows,
        COUNT(DISTINCT product_id) AS distinct_product_ids,
        COUNT_IF(product_id IS NULL) AS null_product_ids,
        COUNT_IF(product_category_name IS NULL) AS gold_missing_category_rows,
        COUNT_IF(product_category_status = 'UNTRANSLATED') AS gold_untranslated_rows,
        COUNT_IF(
            NOT (
                product_category_status <=>
                CASE
                    WHEN product_category_name IS NULL THEN 'MISSING_CATEGORY'
                    WHEN product_category_name_english IS NULL THEN 'UNTRANSLATED'
                    ELSE 'TRANSLATED'
                END
            )
        ) AS invalid_category_status_rows
    FROM commerceai.gold.dim_product
),
extra_gold_keys AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.gold.dim_product AS d
    LEFT ANTI JOIN commerceai.silver.sl_products AS p
        ON d.product_id = p.product_id
),
product_attribute_issues AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.gold.dim_product AS d
    INNER JOIN commerceai.silver.sl_products AS p
        ON d.product_id = p.product_id
    WHERE NOT (
        (d.product_category_name <=> p.product_category_name)
        AND (d.product_name_length <=> p.product_name_length)
        AND (d.product_description_length <=> p.product_description_length)
        AND (d.product_photos_qty <=> p.product_photos_qty)
        AND (d.product_weight_g <=> p.product_weight_g)
        AND (d.product_length_cm <=> p.product_length_cm)
        AND (d.product_height_cm <=> p.product_height_cm)
        AND (d.product_width_cm <=> p.product_width_cm)
    )
),
translation_attribute_issues AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.gold.dim_product AS d
    INNER JOIN commerceai.silver.sl_products AS p
        ON d.product_id = p.product_id
    LEFT JOIN commerceai.silver.sl_product_category_translation AS t
        ON p.product_category_name = t.product_category_name
    WHERE NOT (
        d.product_category_name_english <=> t.product_category_name_english
    )
),
checks AS (
    SELECT
        'row_count_matches_silver' AS check_name,
        CAST(s.source_rows AS STRING) AS expected_value,
        CAST(g.gold_rows AS STRING) AS observed_value,
        IF(s.source_rows = g.gold_rows, 'PASS', 'FAIL') AS status
    FROM source_stats AS s CROSS JOIN gold_stats AS g

    UNION ALL
    SELECT
        'product_id_is_not_null_and_unique',
        '0 null keys and one unique key per row',
        CONCAT('null=', CAST(g.null_product_ids AS STRING),
               ', distinct=', CAST(g.distinct_product_ids AS STRING),
               ', rows=', CAST(g.gold_rows AS STRING)),
        IF(g.null_product_ids = 0 AND g.distinct_product_ids = g.gold_rows, 'PASS', 'FAIL')
    FROM gold_stats AS g

    UNION ALL
    SELECT
        'no_extra_product_ids',
        '0',
        CAST(k.issue_count AS STRING),
        IF(k.issue_count = 0, 'PASS', 'FAIL')
    FROM extra_gold_keys AS k

    UNION ALL
    SELECT
        'source_product_attributes_preserved',
        '0',
        CAST(a.issue_count AS STRING),
        IF(a.issue_count = 0, 'PASS', 'FAIL')
    FROM product_attribute_issues AS a

    UNION ALL
    SELECT
        'category_translation_matches_silver',
        '0',
        CAST(t.issue_count AS STRING),
        IF(t.issue_count = 0, 'PASS', 'FAIL')
    FROM translation_attribute_issues AS t

    UNION ALL
    SELECT
        'missing_category_products_preserved',
        CAST(s.source_missing_category_rows AS STRING),
        CAST(g.gold_missing_category_rows AS STRING),
        IF(s.source_missing_category_rows = g.gold_missing_category_rows, 'PASS', 'FAIL')
    FROM source_stats AS s CROSS JOIN gold_stats AS g

    UNION ALL
    SELECT
        'untranslated_category_products_preserved',
        CAST(s.source_untranslated_rows AS STRING),
        CAST(g.gold_untranslated_rows AS STRING),
        IF(s.source_untranslated_rows = g.gold_untranslated_rows, 'PASS', 'FAIL')
    FROM source_translation_stats AS s CROSS JOIN gold_stats AS g

    UNION ALL
    SELECT
        'category_status_is_consistent',
        '0',
        CAST(g.invalid_category_status_rows AS STRING),
        IF(g.invalid_category_status_rows = 0, 'PASS', 'FAIL')
    FROM gold_stats AS g
)
SELECT
    check_name,
    expected_value,
    observed_value,
    status
FROM checks;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 08 - Review Product Dimension Validation Results

-- COMMAND ----------

-- DBTITLE 1,08 - Review Product Dimension Validation Results
SELECT
    check_name,
    expected_value,
    observed_value,
    status
FROM gold_product_validation_results
ORDER BY check_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 09 - Enforce Product Dimension Quality Gate

-- COMMAND ----------

-- DBTITLE 1,09 - Enforce Product Dimension Quality Gate
SELECT
    COUNT(*) AS total_checks,
    COUNT_IF(status = 'PASS') AS passed_checks,
    COUNT(*) - COUNT_IF(status = 'PASS') AS failed_checks,
    ASSERT_TRUE(
        COUNT(*) = 8 AND COUNT_IF(status = 'PASS') = 8,
        'Gold product dimension validation failed. Review the preceding checks.'
    ) AS validation_gate
FROM gold_product_validation_results;

-- COMMAND ----------

-- DBTITLE 1,10 - Prepare Seller Dimension Validation Checks
CREATE OR REPLACE TEMP VIEW gold_seller_validation_results AS

WITH source_stats AS (
    SELECT
        COUNT(*) AS source_rows
    FROM commerceai.silver.sl_sellers
),

expected_seller AS (
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
),

expected_geo_stats AS (
    SELECT
        COUNT_IF(NOT has_geolocation_match)
            AS unmatched_rows,

        COUNT_IF(has_geolocation_coordinates)
            AS coordinate_rows
    FROM expected_seller
),

gold_stats AS (
    SELECT
        COUNT(*) AS gold_rows,
        COUNT(DISTINCT seller_id) AS distinct_seller_ids,
        COUNT_IF(seller_id IS NULL) AS null_seller_ids,

        COUNT_IF(has_geolocation_match = FALSE)
            AS unmatched_rows,

        COUNT_IF(has_geolocation_coordinates = TRUE)
            AS coordinate_rows,

        SUM(
            CASE
                WHEN has_geolocation_match IS NULL
                  OR has_geolocation_coordinates IS NULL
                  OR geolocation_coordinate_status IS NULL
                  OR geolocation_coordinate_status
                     NOT IN ('AVAILABLE', 'MISSING', 'NOT_FOUND')

                  OR NOT (
                      has_geolocation_coordinates <=>
                      (
                          geolocation_lat IS NOT NULL
                          AND geolocation_lng IS NOT NULL
                      )
                  )

                  OR (
                      has_geolocation_match = FALSE
                      AND (
                          geolocation_coordinate_status <> 'NOT_FOUND'
                          OR has_geolocation_coordinates = TRUE
                          OR geolocation_lat IS NOT NULL
                          OR geolocation_lng IS NOT NULL
                          OR reference_geolocation_city IS NOT NULL
                          OR reference_geolocation_state IS NOT NULL
                          OR geolocation_has_state_conflict IS NOT NULL
                          OR geolocation_has_city_variation IS NOT NULL
                      )
                  )

                  OR (
                      has_geolocation_match = TRUE
                      AND (
                          geolocation_coordinate_status = 'NOT_FOUND'
                          OR (
                              has_geolocation_coordinates = TRUE
                              AND geolocation_coordinate_status <> 'AVAILABLE'
                          )
                          OR (
                              has_geolocation_coordinates = FALSE
                              AND geolocation_coordinate_status <> 'MISSING'
                          )
                      )
                  )
                THEN 1
                ELSE 0
            END
        ) AS invalid_geo_flag_rows

    FROM commerceai.gold.dim_seller
),

extra_gold_keys AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.gold.dim_seller AS d
    LEFT ANTI JOIN commerceai.silver.sl_sellers AS s
        ON d.seller_id = s.seller_id
),

missing_gold_keys AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.silver.sl_sellers AS s
    LEFT ANTI JOIN commerceai.gold.dim_seller AS d
        ON s.seller_id = d.seller_id
),

seller_attribute_issues AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.gold.dim_seller AS d
    INNER JOIN commerceai.silver.sl_sellers AS s
        ON d.seller_id = s.seller_id
    WHERE NOT (
        (d.seller_zip_code_prefix <=> s.seller_zip_code_prefix)
        AND (d.seller_city <=> s.seller_city)
        AND (d.seller_state <=> s.seller_state)
    )
),

geolocation_attribute_issues AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.gold.dim_seller AS d
    INNER JOIN expected_seller AS e
        ON d.seller_id = e.seller_id
    WHERE NOT (
        (d.geolocation_lat <=> e.geolocation_lat)
        AND (d.geolocation_lng <=> e.geolocation_lng)
        AND (d.reference_geolocation_city <=> e.reference_geolocation_city)
        AND (d.reference_geolocation_state <=> e.reference_geolocation_state)
        AND (d.has_geolocation_match <=> e.has_geolocation_match)
        AND (d.has_geolocation_coordinates <=> e.has_geolocation_coordinates)
        AND (d.geolocation_coordinate_status <=> e.geolocation_coordinate_status)
        AND (d.geolocation_has_state_conflict <=> e.geolocation_has_state_conflict)
        AND (d.geolocation_has_city_variation <=> e.geolocation_has_city_variation)
    )
),

checks AS (
    SELECT
        'row_count_matches_silver' AS check_name,
        CAST(s.source_rows AS STRING) AS expected_value,
        CAST(g.gold_rows AS STRING) AS observed_value,
        IF(s.source_rows = g.gold_rows, 'PASS', 'FAIL') AS status
    FROM source_stats AS s
    CROSS JOIN gold_stats AS g

    UNION ALL

    SELECT
        'seller_id_is_not_null_and_unique',
        '0 null keys and one unique key per row',
        CONCAT(
            'null=', CAST(null_seller_ids AS STRING),
            ', distinct=', CAST(distinct_seller_ids AS STRING),
            ', rows=', CAST(gold_rows AS STRING)
        ),
        IF(
            null_seller_ids = 0
            AND distinct_seller_ids = gold_rows,
            'PASS', 'FAIL'
        )
    FROM gold_stats

    UNION ALL

    SELECT
        'no_extra_seller_ids',
        '0',
        CAST(issue_count AS STRING),
        IF(issue_count = 0, 'PASS', 'FAIL')
    FROM extra_gold_keys

    UNION ALL

    SELECT
        'no_missing_seller_ids',
        '0',
        CAST(issue_count AS STRING),
        IF(issue_count = 0, 'PASS', 'FAIL')
    FROM missing_gold_keys

    UNION ALL

    SELECT
        'source_seller_attributes_preserved',
        '0',
        CAST(issue_count AS STRING),
        IF(issue_count = 0, 'PASS', 'FAIL')
    FROM seller_attribute_issues

    UNION ALL

    SELECT
        'geolocation_attributes_match_silver',
        '0',
        CAST(issue_count AS STRING),
        IF(issue_count = 0, 'PASS', 'FAIL')
    FROM geolocation_attribute_issues

    UNION ALL

    SELECT
        'unmatched_geolocation_rows_preserved',
        CAST(e.unmatched_rows AS STRING),
        CAST(g.unmatched_rows AS STRING),
        IF(e.unmatched_rows = g.unmatched_rows, 'PASS', 'FAIL')
    FROM expected_geo_stats AS e
    CROSS JOIN gold_stats AS g

    UNION ALL

    SELECT
        'geolocation_coordinate_coverage_matches_silver',
        CAST(e.coordinate_rows AS STRING),
        CAST(g.coordinate_rows AS STRING),
        IF(e.coordinate_rows = g.coordinate_rows, 'PASS', 'FAIL')
    FROM expected_geo_stats AS e
    CROSS JOIN gold_stats AS g

    UNION ALL

    SELECT
        'geolocation_flags_are_consistent',
        '0',
        CAST(invalid_geo_flag_rows AS STRING),
        IF(invalid_geo_flag_rows = 0, 'PASS', 'FAIL')
    FROM gold_stats
)

SELECT
    check_name,
    expected_value,
    observed_value,
    status
FROM checks;

-- COMMAND ----------

-- DBTITLE 1,11 - Review Seller Dimension Validation Results
SELECT
    check_name,
    expected_value,
    observed_value,
    status
FROM gold_seller_validation_results
ORDER BY check_name;

-- COMMAND ----------

-- DBTITLE 1,12 - Enforce Seller Dimension Quality Gate
SELECT
    COUNT(*) AS total_checks,

    COUNT_IF(status = 'PASS') AS passed_checks,

    COUNT(*) - COUNT_IF(status = 'PASS') AS failed_checks,

    ASSERT_TRUE(
        COUNT(*) = 9
        AND COUNT_IF(status = 'PASS') = 9,
        'Gold seller dimension validation failed. Review the preceding checks.'
    ) AS validation_gate

FROM gold_seller_validation_results;

-- COMMAND ----------

-- DBTITLE 1,13 - Prepare Order Fact Validation Checks

CREATE OR REPLACE TEMP VIEW gold_orders_validation_results AS

WITH source_items AS (
    SELECT
        order_id,
        COUNT(*) AS item_count,
        COUNT(DISTINCT product_id) AS product_count,
        COUNT(DISTINCT seller_id) AS seller_count,
        SUM(price) AS item_value,
        SUM(freight_value) AS freight_value,
        SUM(price + freight_value) AS total_value
    FROM commerceai.silver.sl_order_items
    GROUP BY order_id
),

source_payments AS (
    SELECT
        order_id,
        COUNT(*) AS payment_count,
        SUM(payment_value) AS paid_value
    FROM commerceai.silver.sl_payments
    GROUP BY order_id
),

source_orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        i.order_id AS item_order_id,
        i.item_count,
        i.product_count,
        i.seller_count,
        i.item_value,
        i.freight_value,
        i.total_value,

        p.order_id AS payment_order_id,
        p.payment_count,
        p.paid_value

    FROM commerceai.silver.sl_orders AS o

    LEFT JOIN source_items AS i
        ON o.order_id = i.order_id

    LEFT JOIN source_payments AS p
        ON o.order_id = p.order_id
),

row_comparison AS (
    SELECT
        s.order_id,
        f.order_id AS fact_order_id,

        NOT (
            (f.customer_id <=> s.customer_id)
            AND (f.order_status <=> s.order_status)
            AND (f.order_purchase_timestamp <=> s.order_purchase_timestamp)
            AND (f.order_approved_at <=> s.order_approved_at)
            AND (f.order_delivered_carrier_date <=> s.order_delivered_carrier_date)
            AND (f.order_delivered_customer_date <=> s.order_delivered_customer_date)
            AND (f.order_estimated_delivery_date <=> s.order_estimated_delivery_date)
        ) AS source_attributes_mismatch,

        NOT (
            (f.has_order_items <=> (s.item_order_id IS NOT NULL))
            AND (f.item_count <=> COALESCE(s.item_count, 0))
            AND (f.distinct_product_count <=> COALESCE(s.product_count, 0))
            AND (f.distinct_seller_count <=> COALESCE(s.seller_count, 0))
            AND (f.item_value_brl <=> s.item_value)
            AND (f.freight_value_brl <=> s.freight_value)
            AND (f.ordered_total_value_brl <=> s.total_value)
        ) AS item_metrics_mismatch,

        NOT (
            (f.has_payments <=> (s.payment_order_id IS NOT NULL))
            AND (f.payment_count <=> COALESCE(s.payment_count, 0))
            AND (f.paid_value_brl <=> s.paid_value)
        ) AS payment_metrics_mismatch,

        NOT (
            (
                f.payment_difference_brl <=>
                CASE
                    WHEN s.item_order_id IS NOT NULL
                     AND s.payment_order_id IS NOT NULL
                    THEN s.paid_value - s.total_value
                END
            )
            AND (
                f.payment_item_mismatch_flag <=>
                CASE
                    WHEN s.item_order_id IS NOT NULL
                     AND s.payment_order_id IS NOT NULL
                    THEN ABS(s.paid_value - s.total_value)
                         > CAST(0.01 AS DECIMAL(18, 2))
                END
            )
        ) AS financial_metrics_mismatch,

        NOT (
            (
                f.is_delivery_eligible <=>
                (
                    s.order_status = 'delivered'
                    AND s.order_delivered_customer_date IS NOT NULL
                    AND s.order_estimated_delivery_date IS NOT NULL
                )
            )
            AND (
                f.is_late_delivery <=>
                CASE
                    WHEN s.order_status = 'delivered'
                     AND s.order_delivered_customer_date IS NOT NULL
                     AND s.order_estimated_delivery_date IS NOT NULL
                    THEN CAST(s.order_delivered_customer_date AS DATE)
                       > CAST(s.order_estimated_delivery_date AS DATE)
                END
            )
            AND (
                f.days_late <=>
                CASE
                    WHEN s.order_status = 'delivered'
                     AND s.order_delivered_customer_date IS NOT NULL
                     AND s.order_estimated_delivery_date IS NOT NULL
                    THEN GREATEST(
                        DATEDIFF(
                            CAST(s.order_delivered_customer_date AS DATE),
                            CAST(s.order_estimated_delivery_date AS DATE)
                        ),
                        0
                    )
                END
            )
            AND (
                f.delivery_duration_days <=>
                CASE
                    WHEN s.order_status = 'delivered'
                     AND s.order_delivered_customer_date IS NOT NULL
                     AND s.order_purchase_timestamp IS NOT NULL
                    THEN DATEDIFF(
                        CAST(s.order_delivered_customer_date AS DATE),
                        CAST(s.order_purchase_timestamp AS DATE)
                    )
                END
            )
            AND (
                f.promised_delivery_days <=>
                CASE
                    WHEN s.order_purchase_timestamp IS NOT NULL
                     AND s.order_estimated_delivery_date IS NOT NULL
                    THEN DATEDIFF(
                        CAST(s.order_estimated_delivery_date AS DATE),
                        CAST(s.order_purchase_timestamp AS DATE)
                    )
                END
            )
            AND (
                f.delivered_without_delivery_date <=>
                (
                    s.order_status = 'delivered'
                    AND s.order_delivered_customer_date IS NULL
                )
            )
            AND (
                f.carrier_after_customer_delivery_flag <=>
                CASE
                    WHEN s.order_delivered_carrier_date IS NOT NULL
                     AND s.order_delivered_customer_date IS NOT NULL
                    THEN s.order_delivered_carrier_date >
                         s.order_delivered_customer_date
                END
            )
        ) AS delivery_metrics_mismatch,

        NOT (
            (
                f.order_purchase_date <=>
                CAST(s.order_purchase_timestamp AS DATE)
            )
            AND (
                f.estimated_delivery_date <=>
                CAST(s.order_estimated_delivery_date AS DATE)
            )
            AND (
                f.actual_delivery_date <=>
                CAST(s.order_delivered_customer_date AS DATE)
            )
            AND (
                f.order_purchase_date_key <=>
                CAST(DATE_FORMAT(s.order_purchase_timestamp, 'yyyyMMdd') AS INT)
            )
            AND (
                f.estimated_delivery_date_key <=>
                CAST(DATE_FORMAT(s.order_estimated_delivery_date, 'yyyyMMdd') AS INT)
            )
            AND (
                f.actual_delivery_date_key <=>
                CAST(DATE_FORMAT(s.order_delivered_customer_date, 'yyyyMMdd') AS INT)
            )
        ) AS date_key_mismatch

    FROM source_orders AS s
    LEFT JOIN commerceai.gold.fct_orders AS f
        ON s.order_id = f.order_id
),

source_comparison_stats AS (
    SELECT
        COUNT(*) AS source_rows,
        COUNT_IF(fact_order_id IS NULL) AS missing_orders,
        COUNT_IF(source_attributes_mismatch) AS source_attribute_issues,
        COUNT_IF(item_metrics_mismatch) AS item_issues,
        COUNT_IF(payment_metrics_mismatch) AS payment_issues,
        COUNT_IF(financial_metrics_mismatch) AS financial_issues,
        COUNT_IF(delivery_metrics_mismatch) AS delivery_issues,
        COUNT_IF(date_key_mismatch) AS date_key_issues
    FROM row_comparison
),

gold_stats AS (
    SELECT
        COUNT(*) AS gold_rows,
        COUNT(DISTINCT order_id) AS distinct_order_ids,
        COUNT_IF(order_id IS NULL) AS null_order_ids
    FROM commerceai.gold.fct_orders
),

extra_orders AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.gold.fct_orders AS f
    LEFT ANTI JOIN commerceai.silver.sl_orders AS o
        ON f.order_id = o.order_id
),

missing_customer_keys AS (
    SELECT COUNT(*) AS issue_count
    FROM commerceai.gold.fct_orders AS f
    LEFT ANTI JOIN commerceai.gold.dim_customer AS c
        ON f.customer_id = c.customer_id
),

date_dimension_issues AS (
    SELECT
        COUNT_IF(
            (f.order_purchase_date_key IS NOT NULL AND dp.date_key IS NULL)
            OR (f.estimated_delivery_date_key IS NOT NULL AND de.date_key IS NULL)
            OR (f.actual_delivery_date_key IS NOT NULL AND da.date_key IS NULL)
        ) AS issue_count

    FROM commerceai.gold.fct_orders AS f

    LEFT JOIN commerceai.gold.dim_date AS dp
        ON f.order_purchase_date_key = dp.date_key

    LEFT JOIN commerceai.gold.dim_date AS de
        ON f.estimated_delivery_date_key = de.date_key

    LEFT JOIN commerceai.gold.dim_date AS da
        ON f.actual_delivery_date_key = da.date_key
),

checks AS (
    SELECT
        'row_count_matches_silver' AS check_name,
        CAST(s.source_rows AS STRING) AS expected_value,
        CAST(g.gold_rows AS STRING) AS observed_value,
        IF(s.source_rows = g.gold_rows, 'PASS', 'FAIL') AS status
    FROM source_comparison_stats AS s
    CROSS JOIN gold_stats AS g

    UNION ALL

    SELECT
        'order_id_is_not_null_and_unique',
        '0 null keys and one unique key per row',
        CONCAT(
            'null=', CAST(null_order_ids AS STRING),
            ', distinct=', CAST(distinct_order_ids AS STRING),
            ', rows=', CAST(gold_rows AS STRING)
        ),
        IF(
            null_order_ids = 0 AND distinct_order_ids = gold_rows,
            'PASS', 'FAIL'
        )
    FROM gold_stats

    UNION ALL

    SELECT 'no_missing_order_ids', '0',
           CAST(missing_orders AS STRING),
           IF(missing_orders = 0, 'PASS', 'FAIL')
    FROM source_comparison_stats

    UNION ALL

    SELECT 'no_extra_order_ids', '0',
           CAST(issue_count AS STRING),
           IF(issue_count = 0, 'PASS', 'FAIL')
    FROM extra_orders

    UNION ALL

    SELECT 'source_order_attributes_preserved', '0',
           CAST(source_attribute_issues AS STRING),
           IF(source_attribute_issues = 0, 'PASS', 'FAIL')
    FROM source_comparison_stats

    UNION ALL

    SELECT 'item_aggregates_match_silver', '0',
           CAST(item_issues AS STRING),
           IF(item_issues = 0, 'PASS', 'FAIL')
    FROM source_comparison_stats

    UNION ALL

    SELECT 'payment_aggregates_match_silver', '0',
           CAST(payment_issues AS STRING),
           IF(payment_issues = 0, 'PASS', 'FAIL')
    FROM source_comparison_stats

    UNION ALL

    SELECT 'financial_reconciliation_matches_silver', '0',
           CAST(financial_issues AS STRING),
           IF(financial_issues = 0, 'PASS', 'FAIL')
    FROM source_comparison_stats

    UNION ALL

    SELECT 'delivery_metrics_match_contract', '0',
           CAST(delivery_issues AS STRING),
           IF(delivery_issues = 0, 'PASS', 'FAIL')
    FROM source_comparison_stats

    UNION ALL

    SELECT 'date_keys_match_source_timestamps', '0',
           CAST(date_key_issues AS STRING),
           IF(date_key_issues = 0, 'PASS', 'FAIL')
    FROM source_comparison_stats

    UNION ALL

    SELECT 'calendar_date_keys_resolve', '0',
           CAST(issue_count AS STRING),
           IF(issue_count = 0, 'PASS', 'FAIL')
    FROM date_dimension_issues

    UNION ALL

    SELECT 'customer_keys_resolve', '0',
           CAST(issue_count AS STRING),
           IF(issue_count = 0, 'PASS', 'FAIL')
    FROM missing_customer_keys
)

SELECT
    check_name,
    expected_value,
    observed_value,
    status
FROM checks;


-- COMMAND ----------

-- DBTITLE 1,14 - Review Order Fact Validation Results
SELECT
    check_name,
    expected_value,
    observed_value,
    status
FROM gold_orders_validation_results
ORDER BY check_name;

-- COMMAND ----------

-- DBTITLE 1,15 - Inspect Order Fact Quality Exceptions
SELECT
    COUNT(*) AS total_orders,

    COUNT_IF(
        has_payments AND NOT has_order_items
    ) AS payments_without_items,

    COUNT_IF(
        has_order_items AND NOT has_payments
    ) AS items_without_payments,

    COUNT_IF(
        payment_item_mismatch_flag = TRUE
    ) AS payment_item_mismatches,

    COUNT_IF(
        delivered_without_delivery_date
    ) AS delivered_without_delivery_date,

    COUNT_IF(
        carrier_after_customer_delivery_flag = TRUE
    ) AS carrier_after_customer_delivery,

    COUNT_IF(
        is_delivery_eligible
    ) AS eligible_delivered_orders,

    COUNT_IF(
        is_late_delivery = TRUE
    ) AS late_delivered_orders,

    ROUND(
        100.0 * COUNT_IF(is_late_delivery = TRUE)
        / NULLIF(COUNT_IF(is_delivery_eligible), 0),
        2
    ) AS late_delivery_rate_pct

FROM commerceai.gold.fct_orders;

-- COMMAND ----------

-- DBTITLE 1,16 - Enforce Order Fact Quality Gate
SELECT
    COUNT(*) AS total_checks,

    COUNT_IF(status = 'PASS') AS passed_checks,

    COUNT(*) - COUNT_IF(status = 'PASS') AS failed_checks,

    ASSERT_TRUE(
        COUNT(*) = 12
        AND COUNT_IF(status = 'PASS') = 12,
        'Gold order fact validation failed. Review the preceding checks.'
    ) AS validation_gate

FROM gold_orders_validation_results;

-- COMMAND ----------

-- DBTITLE 1,17 - Prepare Order Items Fact Validation Checks

CREATE OR REPLACE TEMP VIEW gold_order_items_validation_results AS

WITH source_stats AS (

    SELECT COUNT(*) AS source_rows
    FROM commerceai.silver.sl_order_items

),

gold_stats AS (

    SELECT
        COUNT(*) AS gold_rows,

        COUNT_IF(
            order_id IS NULL OR order_item_id IS NULL
        ) AS null_business_keys,

        COUNT(
            DISTINCT order_id, order_item_id
        ) AS distinct_business_keys

    FROM commerceai.gold.fct_order_items

),

missing_source_keys AS (

    SELECT COUNT(*) AS issue_count

    FROM commerceai.silver.sl_order_items AS i

    LEFT ANTI JOIN commerceai.gold.fct_order_items AS f
        ON i.order_id = f.order_id
       AND i.order_item_id = f.order_item_id

),

extra_gold_keys AS (

    SELECT COUNT(*) AS issue_count

    FROM commerceai.gold.fct_order_items AS f

    LEFT ANTI JOIN commerceai.silver.sl_order_items AS i
        ON f.order_id = i.order_id
       AND f.order_item_id = i.order_item_id

),

source_attribute_issues AS (

    SELECT
        COUNT_IF(
            NOT (
                (f.product_id <=> i.product_id)
                AND (f.seller_id <=> i.seller_id)
                AND (f.shipping_limit_date <=> i.shipping_limit_date)
                AND (f.item_value_brl <=> i.price)
                AND (f.freight_value_brl <=> i.freight_value)
                AND (f.customer_id <=> o.customer_id)
                AND (f.order_status <=> o.order_status)
                AND (
                    f.order_purchase_timestamp
                    <=> o.order_purchase_timestamp
                )
            )
        ) AS issue_count

    FROM commerceai.gold.fct_order_items AS f

    LEFT JOIN commerceai.silver.sl_order_items AS i
        ON f.order_id = i.order_id
       AND f.order_item_id = i.order_item_id

    LEFT JOIN commerceai.silver.sl_orders AS o
        ON f.order_id = o.order_id

),

derived_attribute_issues AS (

    SELECT
        COUNT_IF(
            NOT (
                ordered_total_value_brl
                <=> (item_value_brl + freight_value_brl)
            )
        ) AS amount_issues,

        COUNT_IF(
            NOT (
                (
                    order_purchase_date
                    <=> CAST(order_purchase_timestamp AS DATE)
                )
                AND (
                    order_purchase_date_key
                    <=> CAST(
                        DATE_FORMAT(
                            order_purchase_timestamp, 'yyyyMMdd'
                        ) AS INT
                    )
                )
                AND (
                    shipping_limit_calendar_date
                    <=> CAST(shipping_limit_date AS DATE)
                )
                AND (
                    shipping_limit_date_key
                    <=> CAST(
                        DATE_FORMAT(
                            shipping_limit_date, 'yyyyMMdd'
                        ) AS INT
                    )
                )
            )
        ) AS date_issues

    FROM commerceai.gold.fct_order_items

),

reference_issues AS (

    SELECT
        COUNT_IF(o.order_id IS NULL) AS missing_orders,
        COUNT_IF(c.customer_id IS NULL) AS missing_customers,
        COUNT_IF(p.product_id IS NULL) AS missing_products,
        COUNT_IF(s.seller_id IS NULL) AS missing_sellers,

        COUNT_IF(
            (
                f.order_purchase_date_key IS NOT NULL
                AND dp.date_key IS NULL
            )
            OR
            (
                f.shipping_limit_date_key IS NOT NULL
                AND ds.date_key IS NULL
            )
        ) AS missing_calendar_keys

    FROM commerceai.gold.fct_order_items AS f

    LEFT JOIN commerceai.gold.fct_orders AS o
        ON f.order_id = o.order_id

    LEFT JOIN commerceai.gold.dim_customer AS c
        ON f.customer_id = c.customer_id

    LEFT JOIN commerceai.gold.dim_product AS p
        ON f.product_id = p.product_id

    LEFT JOIN commerceai.gold.dim_seller AS s
        ON f.seller_id = s.seller_id

    LEFT JOIN commerceai.gold.dim_date AS dp
        ON f.order_purchase_date_key = dp.date_key

    LEFT JOIN commerceai.gold.dim_date AS ds
        ON f.shipping_limit_date_key = ds.date_key

),

item_aggregates AS (

    SELECT
        order_id,

        COUNT(*) AS item_count,
        COUNT(DISTINCT product_id) AS product_count,
        COUNT(DISTINCT seller_id) AS seller_count,

        SUM(item_value_brl) AS item_value,
        SUM(freight_value_brl) AS freight_value,
        SUM(ordered_total_value_brl) AS total_value

    FROM commerceai.gold.fct_order_items
    GROUP BY order_id

),

order_reconciliation AS (

    SELECT
        COUNT_IF(
            o.order_id IS NULL
            OR NOT (
                (a.item_count <=> o.item_count)
                AND (
                    a.product_count
                    <=> o.distinct_product_count
                )
                AND (
                    a.seller_count
                    <=> o.distinct_seller_count
                )
            )
        ) AS count_issues,

        COUNT_IF(
            o.order_id IS NULL
            OR NOT (
                (a.item_value <=> o.item_value_brl)
                AND (
                    a.freight_value
                    <=> o.freight_value_brl
                )
                AND (
                    a.total_value
                    <=> o.ordered_total_value_brl
                )
            )
        ) AS amount_issues

    FROM item_aggregates AS a

    LEFT JOIN commerceai.gold.fct_orders AS o
        ON a.order_id = o.order_id

),

checks AS (

    SELECT
        'row_count_matches_silver' AS check_name,
        CAST(s.source_rows AS STRING) AS expected_value,
        CAST(g.gold_rows AS STRING) AS observed_value,
        IF(s.source_rows = g.gold_rows, 'PASS', 'FAIL') AS status

    FROM source_stats AS s
    CROSS JOIN gold_stats AS g

    UNION ALL

    SELECT
        'item_business_key_is_not_null_and_unique',
        '0 null keys and one unique composite key per row',
        CONCAT(
            'null=', CAST(null_business_keys AS STRING),
            ', distinct=', CAST(distinct_business_keys AS STRING),
            ', rows=', CAST(gold_rows AS STRING)
        ),
        IF(
            null_business_keys = 0
            AND distinct_business_keys = gold_rows,
            'PASS', 'FAIL'
        )
    FROM gold_stats

    UNION ALL

    SELECT
        'no_missing_source_item_keys',
        '0',
        CAST(issue_count AS STRING),
        IF(issue_count = 0, 'PASS', 'FAIL')
    FROM missing_source_keys

    UNION ALL

    SELECT
        'no_extra_gold_item_keys',
        '0',
        CAST(issue_count AS STRING),
        IF(issue_count = 0, 'PASS', 'FAIL')
    FROM extra_gold_keys

    UNION ALL

    SELECT
        'source_item_and_order_attributes_preserved',
        '0',
        CAST(issue_count AS STRING),
        IF(issue_count = 0, 'PASS', 'FAIL')
    FROM source_attribute_issues

    UNION ALL

    SELECT
        'item_total_value_is_consistent',
        '0',
        CAST(amount_issues AS STRING),
        IF(amount_issues = 0, 'PASS', 'FAIL')
    FROM derived_attribute_issues

    UNION ALL

    SELECT
        'date_attributes_are_consistent',
        '0',
        CAST(date_issues AS STRING),
        IF(date_issues = 0, 'PASS', 'FAIL')
    FROM derived_attribute_issues

    UNION ALL

    SELECT
        'order_and_customer_keys_resolve',
        '0',
        CAST(missing_orders + missing_customers AS STRING),
        IF(
            missing_orders = 0 AND missing_customers = 0,
            'PASS', 'FAIL'
        )
    FROM reference_issues

    UNION ALL

    SELECT
        'product_and_seller_keys_resolve',
        '0',
        CAST(missing_products + missing_sellers AS STRING),
        IF(
            missing_products = 0 AND missing_sellers = 0,
            'PASS', 'FAIL'
        )
    FROM reference_issues

    UNION ALL

    SELECT
        'calendar_date_keys_resolve',
        '0',
        CAST(missing_calendar_keys AS STRING),
        IF(missing_calendar_keys = 0, 'PASS', 'FAIL')
    FROM reference_issues

    UNION ALL

    SELECT
        'item_counts_reconcile_to_order_fact',
        '0',
        CAST(count_issues AS STRING),
        IF(count_issues = 0, 'PASS', 'FAIL')
    FROM order_reconciliation

    UNION ALL

    SELECT
        'item_amounts_reconcile_to_order_fact',
        '0',
        CAST(amount_issues AS STRING),
        IF(amount_issues = 0, 'PASS', 'FAIL')
    FROM order_reconciliation

)

SELECT
    check_name,
    expected_value,
    observed_value,
    status
FROM checks;


-- COMMAND ----------

-- DBTITLE 1,18 - Review Order Items Fact Validation Results
SELECT
    check_name,
    expected_value,
    observed_value,
    status
FROM gold_order_items_validation_results
ORDER BY check_name;

-- COMMAND ----------

-- DBTITLE 1,19 - Inspect Order Items Fact Summary
SELECT
    COUNT(*) AS total_order_items,

    COUNT(DISTINCT order_id) AS orders_with_items,

    COUNT(DISTINCT product_id) AS products_with_items,

    COUNT(DISTINCT seller_id) AS sellers_with_items,

    COUNT_IF(order_status = 'delivered') AS delivered_item_rows,

    SUM(item_value_brl) AS total_item_value_brl,

    SUM(freight_value_brl) AS total_freight_value_brl,

    SUM(ordered_total_value_brl) AS total_ordered_value_brl

FROM commerceai.gold.fct_order_items;

-- COMMAND ----------

-- DBTITLE 1,20 - Enforce Order Items Fact Quality Gate
SELECT
    COUNT(*) AS total_checks,

    COUNT_IF(status = 'PASS') AS passed_checks,

    COUNT(*) - COUNT_IF(status = 'PASS') AS failed_checks,

    ASSERT_TRUE(
        COUNT(*) = 12
        AND COUNT_IF(status = 'PASS') = 12,
        'Gold order items fact validation failed. Review the preceding checks.'
    ) AS validation_gate

FROM gold_order_items_validation_results;