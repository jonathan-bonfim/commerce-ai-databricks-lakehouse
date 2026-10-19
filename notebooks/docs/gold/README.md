# CommerceAI — Gold Layer

## Current implementation status

- `commerceai.gold.dim_date`: implemented and validated (1,827 rows, 7 PASS, 0 FAIL).
- `commerceai.gold.dim_customer`: implemented and validated (99,441 rows, 9 PASS, 0 FAIL, including 278 preserved unmatched geolocation records).
- `commerceai.gold.dim_product`, `commerceai.gold.dim_seller`, and Gold facts: not implemented.
- `shipping_limit_date` in Silver reaches 2020-04-09; the complete-year date dimension consequently includes 2016-2020.
- Last observed Spark SQL session time zone: `Etc/UTC`. This does not establish the source data time zone.

## Execution architecture

- `notebooks/03_gold/08_gold_dimensions.ipynb` is a Python notebook whose transformations are complete Spark SQL statements embedded in `spark.sql(...)`. It does not load external `.sql` files.
- `notebooks/03_gold/10_gold_validation.sql` is a **Databricks source-format SQL notebook**, with a SQL default language. It contains seven date checks and nine customer checks, and it raises SQL errors when any contract fails.
- The root `.gitattributes` includes authored `.sql` files in GitHub Linguist language statistics. The SQL validation notebook is meaningful SQL; it is not dummy content created for language statistics.

## Migration steps

1. Back up the current `08_gold_dimensions` and `10_gold_validation` notebooks.
2. Import/replace `08_gold_dimensions.ipynb` under `notebooks/03_gold/`.
3. Import `10_gold_validation.sql` into the same folder as a **Databricks source notebook**. Avoid retaining two notebooks with exactly the same display name. Before removing the Python validation notebook, check that the imported notebook's default language is SQL, that six gate sections execute in order, and that 7 + 9 checks pass.
4. Both existing Gold tables already exist. Running `08_gold_dimensions` again is optional unless you want to confirm the refactored build code. Running `10_gold_validation` is required to validate the new SQL-only gate.
5. Once the new notebooks have been tested and committed, remove old external SQL scripts from the project if they have become unused, to avoid duplicate sources of truth. Do not delete them before confirming the migration.
6. Commit `.gitattributes` and `10_gold_validation.sql` to the default branch. GitHub language statistics may update asynchronously; no exact SQL percentage is guaranteed.

## Dimensional contracts

- `dim_date`: one record per calendar date. The calendar spans complete years based on order, item shipping, and review events.
- `dim_customer`: one row per `customer_id`, preserving non-unique `customer_unique_id` for repeat-purchase analytics. `LEFT JOIN` with a single-record-per-ZIP Silver geolocation table preserves unmatched customers; original customer city and state remain authoritative.
- Both dimensions use Delta full replacement builds. The new orchestration format introduces no intentional schema or business-rule changes.

## Next milestone

Implement `commerceai.gold.dim_product` inside the Python `08_gold_dimensions` notebook using `spark.sql(...)`, keeping the independent SQL validation gate in `10_gold_validation.sql`.
