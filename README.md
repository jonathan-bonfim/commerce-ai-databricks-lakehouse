# CommerceAI — Databricks Lakehouse & Machine Learning

CommerceAI is an end-to-end Data Engineering portfolio project built with Databricks, Delta Lake, Machine Learning, Generative AI, and Power BI.

The project uses the Brazilian E-Commerce Public Dataset by Olist to build a complete Lakehouse architecture, from raw data ingestion to analytical dashboards and predictive models.

> 🚧 **Work in progress — Bronze layer completed.**

---

## Project Goals

The main goal of CommerceAI is to build a realistic end-to-end Data Engineering project while exploring advanced capabilities of the Databricks ecosystem.

The project is designed to demonstrate practical experience with:

- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- Medallion Architecture
- Data Quality
- Data Modeling
- Machine Learning with Spark ML
- MLflow
- Generative AI / LLM use cases
- Databricks SQL
- Power BI

The project is being developed using only free resources, including Databricks Free Edition and Power BI Desktop.

---

## Business Context

CommerceAI simulates an e-commerce analytics platform capable of processing transactional data and generating insights about:

- orders;
- customers;
- sellers;
- products;
- payments;
- logistics;
- delivery performance;
- customer satisfaction.

Future project stages will also include:

- delivery delay prediction using Machine Learning;
- customer review classification using AI / LLMs;
- analytical modeling;
- Power BI dashboards.

---

## Dataset

The project uses the **Brazilian E-Commerce Public Dataset by Olist**, publicly available on Kaggle.

The dataset contains approximately 100,000 anonymized e-commerce orders and includes information about:

- customers;
- orders;
- order items;
- payments;
- reviews;
- products;
- sellers;
- geolocation;
- product category translations.

The following source files are used:

```text
olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_orders_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

Raw datasets are intentionally **not stored in this repository**.

They are stored in a Unity Catalog Volume inside Databricks.

---

## Architecture

The planned architecture follows the Medallion Architecture pattern:

```text
Olist CSV Files
      │
      ▼
Unity Catalog Volume
      │
      ▼
┌──────────────┐
│    Bronze    │
│  Raw Delta   │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    Silver    │
│ Cleaned Data │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│     Gold     │
│ Data Marts   │
└──────┬───────┘
       │
       ├──────────────► Machine Learning
       │
       ├──────────────► AI / LLM
       │
       ▼
Databricks SQL
       │
       ▼
Power BI
```

---

## Databricks Structure

The project uses the following Unity Catalog structure:

```text
Catalog
└── commerceai
    ├── bronze
    ├── silver
    ├── gold
    └── ml
```

Raw source files are stored under:

```text
/Volumes/commerceai/bronze/raw_files/
```

Each dataset has its own source directory.

---

## Current Status

### Bronze Layer ✅

The Bronze layer is completed.

The implementation includes:

- source profiling;
- schema inspection;
- source-specific CSV parsing;
- reusable ingestion logic;
- Delta table creation;
- ingestion metadata;
- source-to-Bronze reconciliation;
- primary and composite key validation;
- null profiling;
- referential integrity analysis;
- cardinality analysis;
- geolocation quality analysis;
- review parsing validation;
- automated validation gates.

---

## Bronze Tables

The following Delta tables are currently available:

```text
commerceai.bronze.br_customers
commerceai.bronze.br_geolocation
commerceai.bronze.br_order_items
commerceai.bronze.br_orders
commerceai.bronze.br_payments
commerceai.bronze.br_product_category_translation
commerceai.bronze.br_products
commerceai.bronze.br_reviews
commerceai.bronze.br_sellers
```

Each Bronze table also contains technical ingestion metadata:

```text
bronze_ingestion_timestamp
source_file_name
source_file_path
```

---

## Bronze Row Counts

Final validated row counts:

| Table | Rows |
|---|---:|
| br_customers | 99,441 |
| br_geolocation | 1,000,163 |
| br_order_items | 112,650 |
| br_orders | 99,441 |
| br_payments | 103,886 |
| br_product_category_translation | 71 |
| br_products | 32,951 |
| br_reviews | 99,224 |
| br_sellers | 3,095 |

All source-to-Bronze reconciliations completed successfully with:

```text
row_count_difference = 0
status = PASS
```

---

## Data Quality Highlights

Several data quality and modeling characteristics were identified during Bronze validation.

### Orders

`order_id` is unique and contains no null values.

```text
1 row = 1 order
```

---

### Customers

The dataset contains:

```text
99,441 distinct customer_id
96,096 distinct customer_unique_id
```

Olist assigns a different `customer_id` to each order.

The `customer_unique_id` identifies the same customer across multiple purchases and will later support customer repurchase analysis.

---

### Order Items

The validated composite key is:

```text
order_id + order_item_id
```

A single order can contain multiple items.

```text
1 order
   ↓
N order_items
```

---

### Payments

The validated composite key is:

```text
order_id + payment_sequential
```

A single order can contain multiple payment records.

---

### Reviews

Customer reviews required special handling during ingestion.

The original CSV contains:

- multiline review comments;
- embedded quotation marks;
- long free-text fields.

Without source-specific parsing, parts of customer comments were incorrectly interpreted as new CSV records.

The final parsing configuration uses:

```text
multiLine = true
quote = "
escape = "
unescapedQuoteHandling = BACK_TO_DELIMITER
```

After correcting the parser, the final validation produced:

```text
Rows:                         99,224
Null review IDs:                   0
Null order IDs:                    0
Invalid review ID formats:         0
Invalid order ID formats:          0
Invalid review scores:             0
Orphan reviews:                    0
```

A known multiline review was also explicitly validated to ensure the complete comment remained inside a single record.

---

### Review Granularity

`review_id` alone is not unique.

The following candidate composite key was validated successfully:

```text
review_id + order_id
```

It contains:

```text
0 null key rows
0 duplicate key groups
```

Additionally:

```text
547 orders
```

contain multiple review records.

This means future analytical models must not assume a strict one-to-one relationship between orders and reviews.

---

### Geolocation

The geolocation source contains:

```text
1,000,163 rows
19,015 distinct ZIP code prefixes
261,831 exact duplicate rows
```

Therefore:

```text
geolocation_zip_code_prefix
```

is not unique.

This finding will directly influence the Silver-layer design.

A representative location per ZIP code prefix will need to be created before joining geolocation data with customers or sellers.

---

### Product Categories

Among the product records:

```text
610 products
```

do not contain a product category.

Additionally:

```text
13 category records
```

do not find a corresponding English translation.

These records are preserved in Bronze and will be handled explicitly in the Silver layer instead of being silently removed.

---

## Referential Integrity

The following relationships were validated with zero orphan records:

```text
orders → customers

order_items → orders

payments → orders

reviews → orders

order_items → products

order_items → sellers
```

Product category translation contains known source-level gaps that will be handled downstream.

---

## Repository Structure

Current repository structure:

```text
commerce-ai-databricks-lakehouse/
│
├── notebooks/
│   ├── 01_source_profiling
│   ├── 02_bronze_ingestion
│   ├── 03_bronze_validation
│   └── 04_bronze_data_quality
│
├── README.md
└── .gitignore
```

---

## Bronze Notebooks

### 01 — Source Profiling

Responsible for:

- source configuration;
- reusable CSV reading;
- row and column profiling;
- schema inspection;
- review parsing diagnostics;
- known multiline review validation.

---

### 02 — Bronze Ingestion

Responsible for:

- source configuration;
- source-specific parsing options;
- reusable ingestion logic;
- Delta table creation;
- ingestion metadata;
- Bronze ingestion summary.

---

### 03 — Bronze Validation

Responsible for:

- source-to-Bronze reconciliation;
- metadata column validation;
- validation results;
- automated validation gate.

---

### 04 — Bronze Data Quality

Responsible for:

- Bronze table inventory;
- key validation;
- null profiling;
- referential integrity;
- geolocation analysis;
- duplicate analysis;
- categorical distributions;
- customer identity analysis;
- review structural validation;
- review cardinality analysis;
- data quality summary.

---

## Machine Learning Roadmap

A future Machine Learning pipeline will predict whether an order is likely to be delivered after its promised delivery date.

The initial use case will be:

```text
Delivery Delay Prediction
```

The target will be derived from:

```text
order_delivered_customer_date
>
order_estimated_delivery_date
```

Potential features include:

- customer location;
- seller location;
- seller-to-customer distance;
- number of items;
- number of sellers;
- product value;
- freight value;
- product weight;
- product dimensions;
- purchase date features;
- promised delivery time.

The initial models planned are:

```text
Logistic Regression
Random Forest
```

Experiments will be tracked using MLflow.

---

## AI / LLM Roadmap

Customer review text will be processed separately from the delivery prediction model.

Potential AI use cases include:

```text
Sentiment Classification

POSITIVE
NEGATIVE
NEUTRAL
MIXED
```

and:

```text
Feedback Classification

DELIVERY_DELAY
DAMAGED_PRODUCT
WRONG_PRODUCT
PRODUCT_QUALITY
CUSTOMER_SERVICE
OTHER
```

Reviews will not be used as delivery prediction features because they occur after the purchase and could introduce data leakage.

---

## Power BI Roadmap

Power BI will be used as the analytical frontend.

Planned dashboard areas include:

```text
Executive Overview
Delivery Intelligence
ML Performance
Customer Intelligence
```

The project will favor performing heavy transformations in Databricks and exposing business-ready Gold tables to Power BI.

---

## Next Steps

The next development phase is the **Silver layer**.

Planned topics include:

- data type enforcement;
- text normalization;
- geolocation consolidation;
- customer normalization;
- seller normalization;
- product category enrichment;
- review granularity treatment;
- preparation for analytical modeling.

Future stages:

```text
Bronze ✅
   ↓
Silver
   ↓
Gold
   ↓
Machine Learning
   ↓
Generative AI
   ↓
Power BI
```

---

## Project Status

```text
Bronze        ██████████  Completed
Silver        ░░░░░░░░░░  Planned
Gold          ░░░░░░░░░░  Planned
ML            ░░░░░░░░░░  Planned
AI / LLM      ░░░░░░░░░░  Planned
Power BI      ░░░░░░░░░░  Planned
```