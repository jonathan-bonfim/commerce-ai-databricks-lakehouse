# CommerceAI | Gold Layer — Step 01

## Purpose

Build and validate the first Gold dimension (`commerceai.gold.dim_date`) using Spark SQL stored as a first-class `.sql` artifact.

## Repository layout

```text
notebooks/03_gold/08_gold_dimensions.ipynb
notebooks/03_gold/10_gold_validation.ipynb
sql/gold/01_dim_date.sql
sql/validation/01_dim_date_checks.sql
docs/gold/README.md
```

The root repository `README.md` is **not** replaced by this document.

## Input contracts

- `commerceai.silver.sl_orders`
- `commerceai.silver.sl_order_items`
- `commerceai.silver.sl_reviews`

## Outputs

- `commerceai.gold.dim_date`: one row per calendar date, from January 1 of the earliest source event year to December 31 of the latest source event year.

## Setup

1. Create the `sql/gold` and `sql/validation` directories at the **repository root**, alongside `notebooks`, not inside `notebooks/03_gold`.
2. Place each SQL file in its corresponding directory as a plain `.sql` workspace file.
3. Import the two notebooks into `notebooks/03_gold`.
4. Confirm that `PROJECT_ROOT` in both notebooks points to the repository's actual workspace folder. The supplied notebooks are configured for the original author's Databricks workspace and should be adapted when cloned by someone else.
5. Run `08_gold_dimensions`, then `10_gold_validation`. `09_gold_facts` will be added later; it is not a dependency of date-dimension validation.

## Code ownership and execution

- SQL is the canonical transformation and validation logic.
- Python only reads and executes SQL files using `spark.sql`.
- Validation is separate from construction.
- Markdown headings precede code cells so the titles remain visible even when Databricks does not display notebook-specific cell-title metadata. Native UI cell titles can also be added using **Add title**.

## Time zone and calendar semantics

The Gold calendar applies `TO_DATE()` to source timestamps in the active Spark session time zone; no conversion is applied. For reproducibility, review the time zone and keep it consistent between relevant Silver and Gold runs.

## Validation

`10_gold_validation` runs seven independent SQL checks: non-empty dimension, unique date key, non-null date/key, no calendar gaps, full-year coverage, matching date key, and weekday consistency. Stop and investigate if any check fails.

## Project status

- Bronze: completed
- Silver: completed, last confirmed gate 83 checks (69 PASS, 14 WARN, 0 FAIL)
- Gold: Step 01 code prepared; Databricks execution not confirmed
- ML, AI/LLM, Power BI: not started
