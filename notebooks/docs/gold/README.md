# CommerceAI | Gold Layer — Dimensions

## Status

- `commerceai.gold.dim_date`: built and validated in Databricks on 2026-10-18 (1,827 rows, 7 PASS, 0 FAIL).
- `commerceai.gold.dim_customer`: SQL and notebooks prepared; execution and validation pending.
- `commerceai.gold.dim_date`: full-calendar years 2016–2020. The Silver `shipping_limit_date` has a documented maximum of `2020-04-09`; this explains why the calendar extends beyond the purchase period.
- `commerceai.ml` and the Gold fact tables are not implemented.

## Repository layout

```text
notebooks/03_gold/08_gold_dimensions.ipynb
notebooks/03_gold/10_gold_validation.ipynb
sql/gold/01_dim_date.sql                  # Existing, unchanged
sql/gold/02_dim_customer.sql              # New
sql/validation/01_dim_date_checks.sql      # Existing, unchanged
sql/validation/02_dim_customer_checks.sql  # New
docs/gold/README.md                        # This file
```

## Customer dimension contract

- **Grain:** exactly one row per `customer_id`.
- **Primary/business key:** `customer_id`.
- **Consumer identity:** `customer_unique_id` is not unique in the dimension and is retained to support repeat-purchase analysis after joining to orders.
- **Inputs:** `commerceai.silver.sl_customers` and `commerceai.silver.sl_geolocation`.
- **Join:** `LEFT JOIN` by five-character ZIP prefix; `sl_geolocation` is one record per ZIP prefix.
- **Source customer attributes:** preserved without overwriting `customer_city` or `customer_state` using geographic reference values.
- **Geo attributes:** `geolocation_lat`, `geolocation_lng`, `reference_geolocation_city`, `reference_geolocation_state`, nullable geography quality flags, plus explicit match and coordinate availability indicators.
- **Known reference gap:** 278 Silver customer records lack geolocation matches. These are retained with `has_geolocation_match = FALSE` and `geolocation_coordinate_status = 'NOT_FOUND'`. A matched ZIP with missing coordinates is represented with status `MISSING` instead, preserving the distinction.
- **Historically observed counts:** 99,441 `customer_id` records and 96,096 unique `customer_unique_id` values. The validation SQL compares with current Silver dynamically rather than requiring fixed numbers.
- **Load strategy:** `CREATE OR REPLACE TABLE ... USING DELTA AS SELECT`, full rebuild, no artificial unknown ZIPs or inferred coordinates.
- **Historical tracking:** SCD Type 2 is not implemented for this static-source project.

## Step 02 setup

1. Place `02_dim_customer.sql` under `<repo root>/sql/gold/` and `02_dim_customer_checks.sql` under `<repo root>/sql/validation/` as **files**, not Databricks notebook objects.
2. Replace the existing `08_gold_dimensions` and `10_gold_validation` notebooks under `<repo root>/notebooks/03_gold/` using the updated notebooks in the patch package. Original date build and validation cells are retained.
3. Optionally update `docs/gold/README.md` with this file. Keep the root `README.md` unchanged.
4. In notebook 08, execute **only cell 04** to build the new customer dimension; use cell 05 to preview it. No need to rebuild `dim_date` just for this step.
5. Run **both cells** in notebook 10 to validate both dimensions. Expect 7 date checks and 9 customer checks, not a single precomputed result.
6. If a customer check fails, inspect its observed and expected values. Do not automatically modify Silver or delete customer records.

## Code organization

The `.sql` files are the canonical transformations and validation contracts; notebook Python only loads the appropriate SQL files and prints results. Native Databricks cell title metadata and Markdown titles are included for each code cell.
