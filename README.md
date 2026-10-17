# CommerceAI — Databricks Lakehouse, SQL Analytics & Machine Learning

CommerceAI is an end-to-end Data Engineering portfolio project built around the **Brazilian E-Commerce Public Dataset by Olist**. Its goal is to turn raw transactional data into trustworthy, reusable analytical datasets and, in future phases, delivery-delay predictions, AI-assisted customer feedback analysis, and Power BI dashboards.

The project uses a **Medallion Architecture** on **Databricks Free Edition** with **Unity Catalog**, **Delta Lake**, **PySpark**, and **Spark SQL**. The forthcoming Gold layer will be **SQL-first**, with substantive `.sql` scripts tracked in GitHub alongside the Databricks notebooks.

> **Project status — October 17, 2026:** ✅ Bronze completed · ✅ Silver completed (**PASS WITH WARNINGS**) · ⏳ Gold, Machine Learning, Review AI, and Power BI not started.
>
> **Last executed integrated Silver gate:** **83 checks — 69 PASS, 14 WARN, 0 FAIL**, across all **9 Silver tables**. Warnings are documented source-data and coverage limitations, not silent corrections.

---

## Project objectives

- Build a realistic, reproducible Lakehouse pipeline using Databricks, Delta Lake, and Unity Catalog.
- Demonstrate ingestion, data profiling, data contracts, controlled transformations, referential integrity, and quality gates.
- Design reusable **SQL-based Gold data marts** for dimensional analytics and Power BI.
- Learn practical Machine Learning with **Spark ML** and experiment tracking with **MLflow**.
- Explore customer-review sentiment and feedback classification with AI/LLM tools when available under free usage limits.
- Publish transparent architecture, technical decisions, SQL, and Python code for an international Data Engineering portfolio.

**Cost constraint:** The project is designed to use free resources only. Access to individual Databricks AI features, SQL Warehouse capabilities, or Power BI connectivity will be verified before they are adopted.

---

## Business context

The source represents anonymized Brazilian e-commerce orders, customers, order items, sellers, products, payments, deliveries, geolocation records, and customer reviews. CommerceAI aims to support analysis of order performance, customer purchasing patterns, product and seller performance, payment behavior, logistics, and customer feedback.

Two later analytical tracks are planned:

1. **Delivery Delay Prediction:** predict whether an eligible order will arrive later than the estimated delivery date, using only information that would have been available at prediction time.
2. **Review Intelligence:** classify customer feedback independently from the delivery prediction workflow, preventing post-delivery data leakage.

Neither track has been implemented yet.

---

## Architecture

```text
                 Olist CSV files (9 sources)
                            |
                            v
             Unity Catalog Volume (raw_files)
                            |
                            v
                 +--------------------+
                 | Bronze Delta       |  COMPLETED
                 | Source-aligned     |
                 +--------------------+
                            |
                 Bronze validation gate
                            |
                            v
                 +--------------------+
                 | Silver Delta       |  COMPLETED
                 | Conformed entities |
                 +--------------------+
                            |
                 Silver validation gate
                 69 PASS / 14 WARN / 0 FAIL
                            |
                            v
                 +--------------------+
                 | Gold Delta         |  PLANNED — SQL-FIRST
                 | Dimensional marts  |
                 +--------------------+
                      |          |
                      v          v
             Business analytics  Future ML / review AI outputs
                      |
                      v
           Databricks SQL / available access path
                      |
                      v
                Power BI Desktop       PLANNED
```

The exact Gold dimensional model, ML feature pipeline, review-AI output tables, and reporting integration are **design decisions for future phases**, not existing assets.

### Unity Catalog structure

```text
commerceai
├── bronze
│   ├── br_customers
│   ├── br_geolocation
│   ├── br_order_items
│   ├── br_payments
│   ├── br_reviews
│   ├── br_orders
│   ├── br_products
│   ├── br_sellers
│   ├── br_product_category_translation
│   └── Volume: raw_files
├── silver
│   ├── sl_customers
│   ├── sl_geolocation
│   ├── sl_order_items
│   ├── sl_payments
│   ├── sl_reviews
│   ├── sl_orders
│   ├── sl_products
│   ├── sl_sellers
│   └── sl_product_category_translation
├── gold    # Schema available; marts not yet implemented
└── ml      # Schema available; ML datasets not yet implemented
```

Raw files are uploaded to `/Volumes/commerceai/bronze/raw_files/`, separated into source-specific directories. **Raw Olist CSV files are not committed to this repository.**

---

## Dataset and table inventory

**Source:** [Brazilian E-Commerce Public Dataset by Olist (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| Source | Raw CSV | Bronze rows | Silver rows |
|---|---|---:|---:|
| Customers | `olist_customers_dataset.csv` | 99,441 | 99,441 |
| Geolocation | `olist_geolocation_dataset.csv` | 1,000,163 | 19,015 |
| Order Items | `olist_order_items_dataset.csv` | 112,650 | 112,650 |
| Payments | `olist_order_payments_dataset.csv` | 103,886 | 103,886 |
| Reviews | `olist_order_reviews_dataset.csv` | 99,224 | 99,224 |
| Orders | `olist_orders_dataset.csv` | 99,441 | 99,441 |
| Products | `olist_products_dataset.csv` | 32,951 | 32,951 |
| Sellers | `olist_sellers_dataset.csv` | 3,095 | 3,095 |
| Category Translation | `product_category_name_translation.csv` | 71 | 71 |

The reduction in Geolocation is **intentional**: the Silver table consolidates observations into **one representative record per ZIP code prefix**. The eight other Silver tables preserve source row counts.

### Source schemas and Silver grain

| Silver table | Grain / key | Main fields |
|---|---|---|
| `sl_customers` | `customer_id` | `customer_unique_id`, ZIP, city, state |
| `sl_geolocation` | `geolocation_zip_code_prefix` | representative latitude/longitude, selected city/state, quality flags and counts |
| `sl_order_items` | `order_id`, `order_item_id` | `product_id`, `seller_id`, `shipping_limit_date`, `price`, `freight_value` |
| `sl_payments` | `order_id`, `payment_sequential` | `payment_type`, `payment_installments`, `payment_value` |
| `sl_reviews` | `review_id`, `order_id` | `review_score`, full comment title/message, review timestamps |
| `sl_orders` | `order_id` | `customer_id`, status, purchase/approval/shipping/delivery timestamps |
| `sl_products` | `product_id` | original category, product-description attributes, weight and dimensions |
| `sl_sellers` | `seller_id` | ZIP, city, state |
| `sl_product_category_translation` | `product_category_name` | `product_category_name_english` |

**Customer identity matters:** Olist uses `customer_id` as an order-level customer identifier, while `customer_unique_id` identifies a repeat consumer. There are **99,441 distinct `customer_id` values** and **96,096 distinct `customer_unique_id` values**. These identifiers must not be used interchangeably.

---

## Completed: Bronze layer

The Bronze layer performs source-specific CSV ingestion and preserves source fidelity in Delta tables, including:

- Reusable ingestion for all nine source files.
- Source-specific CSV options, including multiline review parsing.
- Ingestion metadata: `bronze_ingestion_timestamp`, `source_file_name`, `source_file_path`.
- Full-load Delta writes with `overwrite` and schema overwrite in the current historical-data implementation.
- Raw-to-Bronze row-count reconciliation: **zero differences for all nine sources**.
- Source-level profiling of keys, nulls, relationship integrity, geolocation cardinality, and reviews.

### Review CSV parsing regression

The Olist reviews CSV contains line breaks and quotes inside free-text comments. Incorrect parsing initially produced **104,162 rows**; using `multiLine` alone produced **99,249**. The validated parsing options produced **99,224 correctly structured review records**:

```python
"multiLine": True
"quote": '"'
"escape": '"'
"unescapedQuoteHandling": "BACK_TO_DELIMITER"
```

The pipeline validates a previously problematic multiline review to protect against regression. Reviews are **not re-parsed in Silver**.

---

## Completed: Silver layer

The Silver layer applies explicit data contracts and controlled transformations while retaining source information and stable keys.

### Key transformations

- **ZIP codes:** represent ZIP prefixes as five-character strings. Numeric inference in Bronze had dropped leading zeros; Silver restores the normalized width for customers, sellers, and geolocation.
- **Geolocation:** remove **261,831 exact duplicate source rows**, select a dominant state/city, and choose a representative observed coordinate near the ZIP-level median of valid observations. Preserve coverage/conflict indicators and keep invalid coordinates as null.
- **Orders:** normalize status and timestamps without filtering canceled or undelivered orders.
- **Order Items and Payments:** preserve each entity's composite key and use `DECIMAL(18,2)` for monetary fields. Avoid joining two one-to-many child tables without aggregation.
- **Products:** preserve missing attributes and standardize the source column spellings `product_name_lenght` → `product_name_length` and `product_description_lenght` → `product_description_length`.
- **Category Translation:** preserve all 71 mappings and explicitly retain products with missing or untranslated categories.
- **Reviews:** preserve comment text **character-for-character**, including quotes and line breaks; keep review-level cardinality and both review timestamps.

The Silver implementation intentionally avoids deriving ML labels, removing source anomalies automatically, or implementing dashboard-specific aggregations.

### Individual and integrated validation

The nine Silver tables were written and individually checked for relevant row counts, keys, data types, referential integrity, numerical consistency, and/or record-level equivalence. The integrated `07_silver_validation` run reported:

| Outcome | Count |
|---|---:|
| PASS | **69** |
| WARN | **14** |
| FAIL | **0** |
| Total | **83** |

**Final result: `PASS WITH WARNINGS`.** Warnings below remain visible by design rather than being silently dropped or overwritten.

### Documented data-quality warnings

| Domain | Observed issue | Count |
|---|---|---:|
| Orders | Carrier timestamp after customer delivery | 23 |
| Orders | Delivered orders missing actual delivery date | 8 |
| Payments | Order-level payment vs. item/freight amount mismatch | 303 |
| Payments | Orders with items but no payment records | 1 |
| Payments | Orders with payments but no item records | 775 |
| Payments | Zero installment count | 2 |
| Payments | Zero-value payments | 9 |
| Products | Products missing category | 610 |
| Products | Products with category lacking translation | 13 |
| Customers | Customer rows without ZIP match in Silver Geolocation | 278 |
| Geolocation | ZIP prefixes with city variation | 8,555 |
| Geolocation | ZIP prefixes without usable representative coordinates | 4 |
| Geolocation | ZIP prefixes with conflicting states | 8 |
| Sellers | Seller rows without ZIP match in Silver Geolocation | 7 |

**Interpretation notes:** The 278 and 7 geolocation gaps refer to **customer/seller rows**, not necessarily distinct ZIP prefixes. Four additional geolocation ZIP prefixes exist but do not have valid coordinates. There are **19,015 unique ZIP prefixes** in Silver, of which **19,011 have representative coordinates**.

Product category translation gaps consist of `portateis_cozinha_e_preparadores_de_alimentos` (10 products) and `pc_gamer` (3 products). The 303 amount mismatches are observed after independently aggregating payments and item/freight values per order; **299 mismatches are on delivered orders**, so they cannot be attributed solely to cancellations. These checks are diagnostic and do not justify rewriting the source records.

### Serverless compatibility

Databricks Free Edition Serverless did not support explicit DataFrame `persist()` in the environment used for this project. Silver transformations were adapted not to depend on `persist()`, `cache()`, or `unpersist()`.

---

## Databricks notebooks and execution responsibilities

| Notebook | Purpose | Suggested execution |
|---|---|---|
| `00_setup_environment` | Create catalog, schemas, and raw Volume | Initial setup / manual |
| `01_source_profiling` | Inspect raw CSV schema and source-specific parsing | Exploration / manual |
| `02_bronze_ingestion` | Ingest nine CSV sources to Bronze Delta | Pipeline step |
| `03_bronze_validation` | Reconcile source and Bronze, validate ingestion metadata | Quality gate |
| `04_bronze_data_quality` | Inspect source keys, relationships, duplicates, and cardinality | Exploration / manual |
| `05_silver_source_assessment` | Assess actual types, ZIP formats, geolocation conflicts, and category coverage | Exploration / manual |
| `06_silver_transformations` | Build the nine conformed Silver Delta tables | Pipeline step |
| `07_silver_validation` | Validate inventory, schemas, keys, relationships, and quality | Quality gate |

**Refactoring note:** To avoid running exploratory diagnostics during every pipeline execution, revised versions of notebooks `02`, `06`, and `07` were prepared. The revised `06` contains **15 code cells plus an introductory Markdown cell**, reduced from **52 code cells**. The revised `07` adds **13 numerical-fidelity checks** (for **96 planned checks in total**). **The 96-check revision has not yet been confirmed as executed**, so the validated result remains **83 checks: 69 PASS, 14 WARN, 0 FAIL**. Any imported revision should be rerun before claiming operational equivalence.

A future workflow may run the stages in this order:

```text
02_bronze_ingestion
   -> 03_bronze_validation
   -> 06_silver_transformations
   -> 07_silver_validation
```

This is a **recommended workflow design**, not a claim that a Databricks Job has already been created or deployed. The current full-load approach overwrites tables individually; it does not offer a single transaction or automatic rollback spanning all nine tables.

---

## Next phase: SQL-first Gold modeling

**Gold has not been implemented.** The next task is to design the business-facing model: agree on fact/dimension grain, relationship cardinalities, KPI definitions, time semantics, conformed dimensions, and treatment of missing/unmapped data before writing transformations.

The Gold implementation will prioritize **Spark SQL / Databricks SQL** for joins, aggregation, window functions, CTEs, dimensional transformations, and Delta table materialization. The repository will include **actual `.sql` source files**, in addition to notebooks and PySpark orchestration where useful, so SQL is both maintainable and visible as a language in GitHub's source-language statistics. GitHub percentages will depend on the committed files and language detection.

Key modeling safeguards:

- Preserve the difference between **99,441 order-associated customer IDs** and **96,096 unique consumers**.
- Preserve `order_id` as a business key and resolve one-to-many relationships before creating order-grain facts.
- Define payment measures and product/freight value measures separately; do not treat their totals as universally identical.
- Avoid multiplying order totals by joining order items, payments, and reviews directly at incompatible grains.
- Use appropriate missing-location handling before distance-based features or maps.
- Define delivery lateness and eligibility carefully; do not treat a missing delivery date as automatically on-time or late.
- Keep delivery prediction features free of post-event customer reviews and actual delivery outcomes.

No Gold fact or dimension names are final yet.

---

## Future roadmap

### Machine Learning — not started

**Delivery Delay Prediction** will use a one-row-per-order feature dataset with an explicit prediction moment and eligibility criteria. Candidate features include seller/customer region, number of items and sellers, freight and order values, product attributes, approximate geographic distance, and purchase/date characteristics.

The label will use the actual delivery outcome and promised date **only in training/evaluation**, never as predictive input. Modeling will favor a **time-based train/test split**, starting with Logistic Regression and comparing with Random Forest using Spark ML. Planned tracking: MLflow. Planned metrics: Precision, Recall for late deliveries, F1, ROC-AUC, PR-AUC, and confusion matrix.

### AI / LLM review analysis — not started

A separate future workflow will classify feedback sentiment (positive, negative, neutral, mixed) and categories such as delivery delay, damaged product, wrong product, product quality, or customer service. AI tool usage will depend on actual free-tier availability. Review data must not leak into pre-delivery prediction features.

### Power BI — not started

Power BI Desktop will be the analytics frontend, likely using Import Mode for this historical dataset. Potential pages: **Executive Overview**, **Delivery Intelligence**, **Customer Intelligence**, and **ML Performance**. Connectivity and free-tier constraints will be verified before choosing an ingestion method.

---

## Reproducibility and project status

Raw Olist files must be obtained from the cited dataset and uploaded to the declared Unity Catalog Volume. Notebooks assume the `commerceai` catalog and schema names shown above. The raw CSV files are intentionally excluded from version control.

| Stage | Status |
|---|---|
| Raw ingestion & Bronze | ✅ Completed |
| Bronze validation & source data quality | ✅ Completed |
| Silver transformations (9 tables) | ✅ Completed |
| Silver integrated validation | ✅ Completed with 14 documented warnings |
| Refactored notebook revalidation | ⏳ Pending confirmation |
| SQL-first Gold dimensional model | ⏳ Not started |
| Delivery Delay Prediction / MLflow | ⏳ Not started |
| Review AI / LLM | ⏳ Not started |
| Power BI dashboard | ⏳ Not started |

*CommerceAI is an ongoing portfolio project. This README distinguishes completed, validated work from planned functionality and documents known data limitations rather than hiding them.*
