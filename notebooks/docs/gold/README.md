# CommerceAI | Gold Layer

## Status (October 2026)

| Table | Grain | Implementation | Validation |
| --- | --- | --- | --- |
| `commerceai.gold.dim_date` | One row per calendar date | Completed | 7 PASS, 0 FAIL |
| `commerceai.gold.dim_customer` | One row per `customer_id` | Completed | 9 PASS, 0 FAIL |
| `commerceai.gold.dim_product` | One row per `product_id` | Prepared, not executed | 8 proposed checks, not executed |
| `commerceai.gold.dim_seller` | One row per `seller_id` | Not started | Not started |
| Gold fact tables | Defined during fact modeling | Not started | Not started |

Both previously implemented dimensions remain unchanged in Step 03.

## Current repository organization

```text
notebooks/03_gold/08_gold_dimensions.ipynb   # Python notebook, transformations in spark.sql(...)
notebooks/03_gold/10_gold_validation.sql     # Databricks SQL-source notebook, independent quality gate
docs/gold/README.md                          # This documentation
```

The current notebooks require **no external `.sql` files** for dimension builds or validation. Archived standalone SQL scripts from the earlier implementation are not used by these notebooks.

## Product dimension contract

- **Target:** `commerceai.gold.dim_product`
- **Grain/business key:** exactly one row per `product_id`; no invented surrogate key.
- **Inputs:** `commerceai.silver.sl_products` and `commerceai.silver.sl_product_category_translation`.
- **Join:** `LEFT JOIN` on `product_category_name` to preserve every source product.
- **Source attributes:** `product_category_name`, `product_name_length`, `product_description_length`, `product_photos_qty`, `product_weight_g`, `product_length_cm`, `product_height_cm`, and `product_width_cm` are preserved as-is, including NULL and zero values.
- **Translation:** `product_category_name_english` remains NULL when no translation is available; do not fabricate one or replace the source category.
- **Reporting status:** `product_category_status` is `MISSING_CATEGORY` (original category is NULL), `UNTRANSLATED` (original category exists but English translation is NULL), or `TRANSLATED` (English translation exists).
- **Observed Silver baselines:** 32,951 total products; 610 without a category; 13 products with categories not covered by translation. Baselines are not assumed to be Gold results before execution.
- **No derived product volume or ML features:** those will be considered later based on clearly defined use cases and prediction-time availability.
- **Load strategy:** `CREATE OR REPLACE TABLE ... USING DELTA AS SELECT` (full rebuild).

## Validation contracts

The SQL-native `10_gold_validation.sql` includes:

- 7 `dim_date` checks (existing)
- 9 `dim_customer` checks (existing)
- 8 `dim_product` checks (new): row count, non-null/unique product keys, no extra products, original attribute fidelity, translation fidelity, missing-category preservation, untranslated-category preservation, status consistency.

Total **24 checks** when all three sets are executed. This count is planned, not confirmed.

Each dimension uses a `CREATE OR REPLACE TEMP VIEW`, a readable result query, and a separate `ASSERT_TRUE` gate. A failure should halt the independent validation notebook/job.

## Step 03 execution

1. Back up the existing `08_gold_dimensions` and `10_gold_validation` notebooks.
2. Replace `notebooks/03_gold/08_gold_dimensions.ipynb` with the supplied update.
3. Replace/import `notebooks/03_gold/10_gold_validation.sql` as a **Databricks SQL source notebook**, not as a generic file outside notebooks.
4. In notebook 08, run only **06 - Build Product Dimension With Embedded SQL**. Then run **07 - Preview Product Dimension**. Rebuilding `dim_date` and `dim_customer` is unnecessary for this step.
5. Run notebook 10 to validate the three completed dimensions. Verify all **24 checks** (7 + 9 + 8) pass before considering the Gold product dimension complete.
6. Update this README only after execution results are confirmed. Do not claim `dim_product` is complete until validation passes.

## Additional implementation notes

- The source dataset is historical. SCD Type 2 and surrogate keys are not added without a demonstrated requirement.
- A SQL-native notebook (`10_gold_validation.sql`) is maintained to provide standalone, substantive SQL code in the repository. `*.sql linguist-detectable=true` in `.gitattributes` may allow SQL to appear in GitHub language statistics once changes are committed and pushed; GitHub classification updates may take time.
- The Silver source contains known quality gaps. Gold preserves those gaps and exposes them rather than imputing unsupported business values.
