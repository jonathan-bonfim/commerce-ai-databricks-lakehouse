# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC
# MAGIC This notebook ingests all raw Olist CSV sources into Delta tables in the Bronze layer. Source-specific CSV parsing rules are driven by configuration so that no manual reprocessing step is required.
# MAGIC

# COMMAND ----------

# DBTITLE 1,01 - Imports
from pyspark.sql.functions import current_timestamp, col


# COMMAND ----------

# DBTITLE 1,02 - Environment Configuration
BASE_PATH = "/Volumes/commerceai/bronze/raw_files"
BRONZE_SCHEMA = "commerceai.bronze"


# COMMAND ----------

# DBTITLE 1,03 - Source Configuration
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

# DBTITLE 1,04 - Bronze Ingestion Function
def ingest_csv_to_bronze(
    source_name: str,
    file_name: str,
    target_table: str,
    read_options: dict = None
):
    """
    Read a raw CSV file from the landing volume and write it
    as a Delta table in the Bronze layer.

    Source-specific CSV options can be provided through read_options.
    """

    source_path = f"{BASE_PATH}/{source_name}/{file_name}"
    full_target_table = f"{BRONZE_SCHEMA}.{target_table}"

    reader = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
    )

    for option_name, option_value in (read_options or {}).items():
        reader = reader.option(option_name, option_value)

    df = reader.csv(source_path)

    df_bronze = (
        df
        .withColumn("bronze_ingestion_timestamp", current_timestamp())
        .withColumn("source_file_name", col("_metadata.file_name"))
        .withColumn("source_file_path", col("_metadata.file_path"))
    )

    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_target_table)
    )

    return {
        "source": source_name,
        "target_table": full_target_table,
        "row_count": df_bronze.count(),
        "column_count": len(df_bronze.columns)
    }


# COMMAND ----------

# DBTITLE 1,05 - Run Bronze Ingestion
ingestion_results = []

for source_name, config in sources.items():
    result = ingest_csv_to_bronze(
        source_name=source_name,
        file_name=config["file_name"],
        target_table=config["target_table"],
        read_options=config.get("read_options", {})
    )

    ingestion_results.append(result)


# COMMAND ----------

# DBTITLE 1,06 - Ingestion Summary
ingestion_results_df = spark.createDataFrame(ingestion_results)

display(
    ingestion_results_df.orderBy("source")
)