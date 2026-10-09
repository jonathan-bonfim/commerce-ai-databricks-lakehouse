# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Validation
# MAGIC
# MAGIC This notebook reconciles the raw source row counts against the Bronze Delta tables and validates that the required Bronze metadata columns are present. It fails fast when reconciliation does not pass.
# MAGIC

# COMMAND ----------

# DBTITLE 1,01 - Imports and Environment Configuration
from pyspark.sql import functions as F

BASE_PATH = "/Volumes/commerceai/bronze/raw_files"
BRONZE_SCHEMA = "commerceai.bronze"


# COMMAND ----------

# DBTITLE 1,02 - Source Configuration
sources = {
    "customers": {
        "file_name": "olist_customers_dataset.csv",
        "target_table": "br_customers",
        "read_options": {} 
    },
    "geolocation": {
        "file_name": "olist_geolocation_dataset.csv",
        "target_table": "br_geolocation",
        "read_options": {}
    },
    "order_items": {
        "file_name": "olist_order_items_dataset.csv",
        "target_table": "br_order_items",
        "read_options": {}
    },
    "payments": {
        "file_name": "olist_order_payments_dataset.csv",
        "target_table": "br_payments",
        "read_options": {}
    },
    # Reviews contain multiline text and embedded quotation marks.
    "reviews": {
        "file_name": "olist_order_reviews_dataset.csv",
        "target_table": "br_reviews",
        "read_options": {
            "multiLine": True,
            "quote": '"',
            "escape": '"',
            "unescapedQuoteHandling": "BACK_TO_DELIMITER"
        }
    },
    "orders": {
        "file_name": "olist_orders_dataset.csv",
        "target_table": "br_orders",
        "read_options": {}
    },
    "products": {
        "file_name": "olist_products_dataset.csv",
        "target_table": "br_products",
        "read_options": {}
    },
    "sellers": {
        "file_name": "olist_sellers_dataset.csv",
        "target_table": "br_sellers",
        "read_options": {}
    },
    "product_category_translation": {
        "file_name": "product_category_name_translation.csv",
        "target_table": "br_product_category_translation",
        "read_options": {}
    }
}


# COMMAND ----------

# DBTITLE 1,03 - CSV Reader Helper
def read_csv_source(source_name: str, config: dict):
    """Read a source CSV using its source-specific parsing options."""

    source_path = f"{BASE_PATH}/{source_name}/{config['file_name']}"

    reader = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
    )

    for option_name, option_value in config.get("read_options", {}).items():
        reader = reader.option(option_name, option_value)

    return reader.csv(source_path)


# COMMAND ----------

# DBTITLE 1,04 - Source-to-Bronze Reconciliation
reconciliation_results = []

for source_name, config in sources.items():
    source_df = read_csv_source(source_name, config)

    target_table = f"{BRONZE_SCHEMA}.{config['target_table']}"
    bronze_df = spark.table(target_table)

    source_row_count = source_df.count()
    bronze_row_count = bronze_df.count()

    reconciliation_results.append({
        "source": source_name,
        "target_table": target_table,
        "source_row_count": source_row_count,
        "bronze_row_count": bronze_row_count,
        "row_count_difference": bronze_row_count - source_row_count,
        "status": "PASS" if source_row_count == bronze_row_count else "FAIL"
    })

reconciliation_df = spark.createDataFrame(reconciliation_results)


# COMMAND ----------

# DBTITLE 1,05 - Metadata Column Validation
REQUIRED_METADATA_COLUMNS = {
    "bronze_ingestion_timestamp",
    "source_file_name",
    "source_file_path"
}

metadata_validation_results = []

for source_name, config in sources.items():
    target_table = f"{BRONZE_SCHEMA}.{config['target_table']}"
    bronze_df = spark.table(target_table)

    available_columns = set(bronze_df.columns)
    missing_columns = sorted(REQUIRED_METADATA_COLUMNS - available_columns)

    metadata_validation_results.append({
        "source": source_name,
        "target_table": target_table,
        "missing_metadata_columns": ", ".join(missing_columns),
        "status": "PASS" if not missing_columns else "FAIL"
    })

metadata_validation_df = spark.createDataFrame(metadata_validation_results)


# COMMAND ----------

# DBTITLE 1,06 - Validation Results
print("Source-to-Bronze row-count reconciliation")
display(reconciliation_df.orderBy("source"))

print("Required Bronze metadata columns")
display(metadata_validation_df.orderBy("source"))


# COMMAND ----------

# DBTITLE 1,1. 07 - Validation Gate
failed_reconciliations = [
    row.asDict()
    for row in reconciliation_df.filter(F.col("status") == "FAIL").collect()
]

failed_metadata_checks = [
    row.asDict()
    for row in metadata_validation_df.filter(F.col("status") == "FAIL").collect()
]

if failed_reconciliations or failed_metadata_checks:
    raise ValueError(
        "Bronze validation failed. "
        f"Reconciliation failures: {failed_reconciliations}. "
        f"Metadata failures: {failed_metadata_checks}."
    )

print("All Bronze validations passed.")
