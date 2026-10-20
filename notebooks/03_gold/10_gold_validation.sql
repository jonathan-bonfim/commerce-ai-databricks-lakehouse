-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 10 - Gold Validation
-- MAGIC
-- MAGIC **Purpose:** Validate Gold dimensions using pure Spark SQL and stop execution when a contract fails.
-- MAGIC
-- MAGIC **Inputs:** `commerceai.gold.dim_date`, `commerceai.gold.dim_customer`, `commerceai.gold.dim_product`, and their Silver source tables.
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