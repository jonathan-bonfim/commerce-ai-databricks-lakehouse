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
FROM checks
ORDER BY check_name;
