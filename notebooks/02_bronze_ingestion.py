# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC
# MAGIC **Purpose:** Ingest the nine raw Olist CSV files from the Unity Catalog Volume into Bronze Delta tables.
# MAGIC
# MAGIC **Inputs:** `/Volumes/commerceai/bronze/raw_files/`  
# MAGIC **Outputs:** `commerceai.bronze.br_*` (nine Delta tables)  
# MAGIC **Execution:** Full refresh; suitable for the ingestion task in a pipeline.  
# MAGIC **Validation owner:** `03_bronze_validation` reconciles raw row counts and metadata after ingestion.  
# MAGIC **Scope:** Retain source fidelity, source-specific CSV parsing (including multiline reviews), and ingestion metadata. No data quality profiling or repeated row-count scans here.
# MAGIC
# MAGIC > Run this notebook only when you intentionally want to refresh the Bronze layer. `overwrite` replaces the existing Bronze tables.
# MAGIC

# COMMAND ----------

# DBTITLE 1,01 - Configure Bronze Sources
from pyspark.sql.functions import current_timestamp, col

BASE_PATH = "/Volumes/commerceai/bronze/raw_files"
BRONZE_SCHEMA = "commerceai.bronze"

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

# DBTITLE 1,02 - Define Bronze Ingestion
def ingest_csv_to_bronze(source_name: str, config: dict) -> dict:
    """Load one source CSV and overwrite its Bronze Delta table."""
    source_path = f"{BASE_PATH}/{source_name}/{config['file_name']}"
    target_table = f"{BRONZE_SCHEMA}.{config['target_table']}"

    reader = spark.read.option("header", True).option("inferSchema", True)
    for option_name, option_value in config.get("read_options", {}).items():
        reader = reader.option(option_name, option_value)

    source_df = reader.csv(source_path)
    bronze_df = (
        source_df
        .withColumn("bronze_ingestion_timestamp", current_timestamp())
        .withColumn("source_file_name", col("_metadata.file_name"))
        .withColumn("source_file_path", col("_metadata.file_path"))
    )
    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    return {
        "source": source_name,
        "target_table": target_table,
        "column_count": len(bronze_df.columns)
    }


# COMMAND ----------

# DBTITLE 1,03 - Ingest All Sources
ingestion_results = []

for source_name, config in sources.items():
    ingestion_results.append(ingest_csv_to_bronze(source_name, config))
    print(f"Bronze table written: {ingestion_results[-1]['target_table']}")


# COMMAND ----------

# DBTITLE 1,04 - Report Ingestion Results
print(f"Bronze tables written: {len(ingestion_results)}")
display(spark.createDataFrame(ingestion_results).orderBy("source"))
print("Run 03_bronze_validation to reconcile source and Bronze row counts.")
