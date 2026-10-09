# Databricks notebook source
# MAGIC %md
# MAGIC # Source Profiling
# MAGIC
# MAGIC This notebook profiles the raw Olist CSV files before Bronze ingestion. It is intentionally read-only and applies the same source-specific parsing rules used by the Bronze pipeline.
# MAGIC

# COMMAND ----------

# DBTITLE 1,01 - Imports and Environment Configuration
from pyspark.sql import functions as F

BASE_PATH = "/Volumes/commerceai/bronze/raw_files"


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

# DBTITLE 1,04 - Source Profiling
profiling_results = []

for source_name, config in sources.items():
    df = read_csv_source(source_name, config)

    profiling_results.append({
        "source": source_name,
        "file_name": config["file_name"],
        "row_count": df.count(),
        "column_count": len(df.columns)
    })

profiling_df = spark.createDataFrame(profiling_results)

display(
    profiling_df.orderBy(F.desc("row_count"))
)


# COMMAND ----------

# DBTITLE 1,05 - Source Schema Inspection
schema_results = []

for source_name, config in sources.items():
    df = read_csv_source(source_name, config)

    for field in df.schema.fields:
        schema_results.append({
            "source": source_name,
            "column_name": field.name,
            "data_type": field.dataType.simpleString(),
            "nullable": field.nullable
        })

schema_df = spark.createDataFrame(schema_results)

display(
    schema_df.orderBy("source", "column_name")
)


# COMMAND ----------

# DBTITLE 1,06 - Review Source Parsing Diagnostics
HEX_ID_PATTERN = r"^[0-9a-f]{32}$"

reviews_source_df = read_csv_source(
    "reviews",
    sources["reviews"]
)

review_source_diagnostics = (
    reviews_source_df
    .agg(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("review_id").isNull(), 1).otherwise(0)).alias("null_review_id_rows"),
        F.sum(F.when(F.col("order_id").isNull(), 1).otherwise(0)).alias("null_order_id_rows"),
        F.sum(
            F.when(
                F.col("review_id").isNotNull()
                & ~F.col("review_id").rlike(HEX_ID_PATTERN),
                1
            ).otherwise(0)
        ).alias("invalid_review_id_format_rows"),
        F.sum(
            F.when(
                F.col("order_id").isNotNull()
                & ~F.col("order_id").rlike(HEX_ID_PATTERN),
                1
            ).otherwise(0)
        ).alias("invalid_order_id_format_rows"),
        F.sum(
            F.when(
                F.col("review_score").isNull()
                | ~F.col("review_score").cast("string").isin("1", "2", "3", "4", "5"),
                1
            ).otherwise(0)
        ).alias("invalid_review_score_rows"),
        F.countDistinct("review_id").alias("distinct_review_ids"),
        F.countDistinct("order_id").alias("distinct_order_ids")
    )
)

display(review_source_diagnostics)


# COMMAND ----------

# DBTITLE 1,07 - Known Multiline Review Sample Check
KNOWN_REVIEW_ID = "5090899e38cd55729c9783745f2706f5"

known_review_sample = (
    reviews_source_df
    .filter(F.col("review_id") == KNOWN_REVIEW_ID)
    .select(
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
        F.length("review_comment_message").alias("comment_length"),
        F.size(F.split(F.col("review_comment_message"), "\\n")).alias("comment_line_count"),
        F.lower(F.col("review_comment_message")).contains("mercado livre").alias("contains_expected_phrase")
    )
)

display(known_review_sample)
