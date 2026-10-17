# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver Transformations
# MAGIC
# MAGIC **Purpose:** Build and overwrite nine conformed Silver Delta tables from validated Bronze sources.
# MAGIC
# MAGIC **Inputs:** `commerceai.bronze.br_*`  
# MAGIC **Outputs:** `commerceai.silver.sl_*` (nine Delta tables)  
# MAGIC **Execution:** Full refresh; one Silver transformation task.  
# MAGIC **Validation owner:** `07_silver_validation` must run **after** this notebook as a separate quality gate.  
# MAGIC **Design:** Preserve original entity grain for eight sources. Consolidate geolocation to one row per ZIP prefix. Normalize ZIP codes, selected text fields and data types without inventing missing values. Preserve reviews verbatim.
# MAGIC
# MAGIC This is an **execution notebook**, not an exploratory or test notebook. Only lightweight source-column checks remain as pre-write safeguards. Detailed reconciliation, profiling, and quality metrics belong in `05_silver_source_assessment` (manual investigation) or `07_silver_validation` (operational validation). The nine Delta overwrites are atomic **per table**, not as one transaction across all nine tables.
# MAGIC
# MAGIC > Replacing the existing notebook does not change Delta data until you explicitly run it. Run `07_silver_validation` after the new version has executed.
# MAGIC

# COMMAND ----------

# DBTITLE 1,01 - Configure Silver Tables
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import Column

CATALOG = "commerceai"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

SOURCES = [
    "customers", "sellers", "geolocation", "orders", "order_items",
    "payments", "products", "product_category_translation", "reviews"
]
SOURCE_TABLES = {
    name: f"{CATALOG}.{BRONZE_SCHEMA}.br_{name}" for name in SOURCES
}
TARGET_TABLES = {
    name: f"{CATALOG}.{SILVER_SCHEMA}.sl_{name}" for name in SOURCES
}
METADATA_COLUMNS = [
    "bronze_ingestion_timestamp", "source_file_name", "source_file_path"
]
PUBLISHED_TABLES = []


# COMMAND ----------

# DBTITLE 1,02 - Define Transformation Helpers
def normalize_zip_prefix(column: Column) -> Column:
    zip_text = F.trim(column.cast("string"))
    return F.when(
        zip_text.rlike(r"^[0-9]{1,5}$"),
        F.lpad(zip_text, 5, "0")
    ).otherwise(F.lit(None).cast("string"))


def normalize_city(column: Column) -> Column:
    return F.lower(F.regexp_replace(F.trim(column.cast("string")), r"\s+", " "))


def normalize_state(column: Column) -> Column:
    return F.upper(F.trim(column.cast("string")))


def normalize_category(column: Column) -> Column:
    category_text = F.lower(F.trim(column.cast("string")))
    return F.when(
        category_text.isNull() | (category_text == ""),
        F.lit(None).cast("string")
    ).otherwise(category_text)


def read_bronze(source: str, required_columns: list):
    """Fail early if a required source field or lineage column is absent."""
    df = spark.table(SOURCE_TABLES[source])
    missing = sorted(set(required_columns + METADATA_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(f"Missing Bronze columns for {source}: {missing}")
    return df


def write_silver(source: str, df) -> None:
    """Overwrite one Silver Delta table; data-quality checks run separately."""
    target = TARGET_TABLES[source]
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    PUBLISHED_TABLES.append(target)
    print(f"Written: {target}")


# COMMAND ----------

# DBTITLE 1,03 - Build Silver Customers
source = read_bronze("customers", [
    "customer_id", "customer_unique_id", "customer_zip_code_prefix",
    "customer_city", "customer_state"
])

result = source.select(
    F.col("customer_id").cast("string").alias("customer_id"),
    F.col("customer_unique_id").cast("string").alias("customer_unique_id"),
    normalize_zip_prefix(F.col("customer_zip_code_prefix")).alias("customer_zip_code_prefix"),
    normalize_city(F.col("customer_city")).alias("customer_city"),
    normalize_state(F.col("customer_state")).alias("customer_state"),
    *[F.col(name) for name in METADATA_COLUMNS]
)
write_silver("customers", result)


# COMMAND ----------

# DBTITLE 1,04 - Build Silver Sellers
source = read_bronze("sellers", [
    "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
])

result = source.select(
    F.col("seller_id").cast("string").alias("seller_id"),
    normalize_zip_prefix(F.col("seller_zip_code_prefix")).alias("seller_zip_code_prefix"),
    normalize_city(F.col("seller_city")).alias("seller_city"),
    normalize_state(F.col("seller_state")).alias("seller_state"),
    *[F.col(name) for name in METADATA_COLUMNS]
)
write_silver("sellers", result)


# COMMAND ----------

# DBTITLE 1,05 - Normalize Geolocation
ZIP_COLUMN = "geolocation_zip_code_prefix"
BRAZIL_STATES = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE",
    "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]

source = read_bronze("geolocation", [
    ZIP_COLUMN, "geolocation_lat", "geolocation_lng",
    "geolocation_city", "geolocation_state"
])

geolocation_normalized = source.select(
    normalize_zip_prefix(F.col(ZIP_COLUMN)).alias(ZIP_COLUMN),
    F.col("geolocation_lat").cast("double").alias("geolocation_lat"),
    F.col("geolocation_lng").cast("double").alias("geolocation_lng"),
    normalize_city(F.col("geolocation_city")).alias("geolocation_city"),
    normalize_state(F.col("geolocation_state")).alias("geolocation_state")
)

# Duplicate business rows are dropped without considering ingestion metadata.
geolocation_distinct = geolocation_normalized.dropDuplicates()
VALID_COORDINATES = (
    F.col("geolocation_lat").between(-34.0, 6.0)
    & F.col("geolocation_lng").between(-74.0, -28.0)
)


# COMMAND ----------

# DBTITLE 1,06 - Select Canonical State and City
state_counts = (
    geolocation_distinct
    .filter(F.col("geolocation_state").isin(*BRAZIL_STATES))
    .groupBy(ZIP_COLUMN, "geolocation_state")
    .agg(F.count("*").alias("state_record_count"))
)

state_window = Window.partitionBy(ZIP_COLUMN).orderBy(
    F.desc("state_record_count"), F.asc("geolocation_state")
)
dominant_state = (
    state_counts
    .withColumn("state_rank", F.row_number().over(state_window))
    .filter(F.col("state_rank") == 1)
    .select(ZIP_COLUMN, "geolocation_state")
)

city_counts = (
    geolocation_distinct
    .join(dominant_state, [ZIP_COLUMN, "geolocation_state"], "inner")
    .filter(F.col("geolocation_city").isNotNull() & (F.col("geolocation_city") != ""))
    .groupBy(ZIP_COLUMN, "geolocation_city")
    .agg(F.count("*").alias("city_record_count"))
)

city_window = Window.partitionBy(ZIP_COLUMN).orderBy(
    F.desc("city_record_count"), F.asc("geolocation_city")
)
dominant_city = (
    city_counts
    .withColumn("city_rank", F.row_number().over(city_window))
    .filter(F.col("city_rank") == 1)
    .select(ZIP_COLUMN, "geolocation_city")
)


# COMMAND ----------

# DBTITLE 1,07 - Select Representative Coordinates
valid_geolocation = (
    geolocation_distinct
    .join(dominant_state, [ZIP_COLUMN, "geolocation_state"], "inner")
    .filter(VALID_COORDINATES)
)
coordinate_medians = valid_geolocation.groupBy(ZIP_COLUMN).agg(
    F.percentile_approx("geolocation_lat", 0.5, 10000).alias("_median_lat"),
    F.percentile_approx("geolocation_lng", 0.5, 10000).alias("_median_lng")
)
coordinate_candidates = (
    valid_geolocation.join(coordinate_medians, ZIP_COLUMN, "inner")
    .withColumn(
        "_distance_score",
        F.pow(F.col("geolocation_lat") - F.col("_median_lat"), 2)
        + F.pow(
            (F.col("geolocation_lng") - F.col("_median_lng"))
            * F.cos(F.radians(F.col("_median_lat"))),
            2
        )
    )
)
coordinate_window = Window.partitionBy(ZIP_COLUMN).orderBy(
    F.asc("_distance_score"),
    F.asc("geolocation_lat"),
    F.asc("geolocation_lng"),
    F.asc_nulls_last("geolocation_city")
)
representative_coordinates = (
    coordinate_candidates
    .withColumn("_coordinate_rank", F.row_number().over(coordinate_window))
    .filter(F.col("_coordinate_rank") == 1)
    .select(ZIP_COLUMN, "geolocation_lat", "geolocation_lng")
)


# COMMAND ----------

# DBTITLE 1,08 - Build and Write Silver Geolocation
source_metrics = geolocation_normalized.groupBy(ZIP_COLUMN).agg(
    F.count("*").alias("geolocation_source_record_count"),
    F.countDistinct("geolocation_state").alias("geolocation_state_variants"),
    F.countDistinct("geolocation_city").alias("geolocation_city_variants")
)
distinct_metrics = geolocation_distinct.groupBy(ZIP_COLUMN).agg(
    F.count("*").alias("geolocation_distinct_record_count"),
    F.sum(F.when(VALID_COORDINATES, 1).otherwise(0)).alias(
        "geolocation_valid_coordinate_records"
    )
)
selected_coordinate_metrics = valid_geolocation.groupBy(ZIP_COLUMN).agg(
    F.count("*").alias("geolocation_selected_state_coordinate_count")
)

silver_geolocation = (
    source_metrics
    .join(distinct_metrics, ZIP_COLUMN, "left")
    .join(dominant_state, ZIP_COLUMN, "left")
    .join(dominant_city, ZIP_COLUMN, "left")
    .join(representative_coordinates, ZIP_COLUMN, "left")
    .join(selected_coordinate_metrics, ZIP_COLUMN, "left")
    .fillna({"geolocation_selected_state_coordinate_count": 0})
    .withColumn(
        "geolocation_has_state_conflict",
        F.col("geolocation_state_variants") > 1
    )
    .withColumn(
        "geolocation_has_city_variation",
        F.col("geolocation_city_variants") > 1
    )
    .withColumn(
        "geolocation_coordinate_status",
        F.when(
            F.col("geolocation_lat").isNotNull()
            & F.col("geolocation_lng").isNotNull(),
            F.lit("AVAILABLE")
        ).otherwise(F.lit("MISSING"))
    )
    .withColumn("source_table_name", F.lit(SOURCE_TABLES["geolocation"]))
    .select(
        ZIP_COLUMN, "geolocation_lat", "geolocation_lng",
        "geolocation_city", "geolocation_state",
        "geolocation_source_record_count", "geolocation_distinct_record_count",
        "geolocation_valid_coordinate_records",
        "geolocation_selected_state_coordinate_count",
        "geolocation_state_variants", "geolocation_city_variants",
        "geolocation_has_state_conflict", "geolocation_has_city_variation",
        "geolocation_coordinate_status", "source_table_name"
    )
)
write_silver("geolocation", silver_geolocation)


# COMMAND ----------

# DBTITLE 1,09 - Build Silver Orders
ORDER_TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date"
]
source = read_bronze("orders", ["order_id", "customer_id", "order_status", *ORDER_TIMESTAMP_COLUMNS])

result = source.select(
    F.col("order_id").cast("string").alias("order_id"),
    F.col("customer_id").cast("string").alias("customer_id"),
    F.lower(F.trim(F.col("order_status").cast("string"))).alias("order_status"),
    *[F.col(name).cast("timestamp").alias(name) for name in ORDER_TIMESTAMP_COLUMNS],
    *[F.col(name) for name in METADATA_COLUMNS]
)
write_silver("orders", result)


# COMMAND ----------

# DBTITLE 1,10 - Build Silver Order Items
source = read_bronze("order_items", [
    "order_id", "order_item_id", "product_id", "seller_id",
    "shipping_limit_date", "price", "freight_value"
])
result = source.select(
    F.col("order_id").cast("string").alias("order_id"),
    F.col("order_item_id").cast("int").alias("order_item_id"),
    F.col("product_id").cast("string").alias("product_id"),
    F.col("seller_id").cast("string").alias("seller_id"),
    F.col("shipping_limit_date").cast("timestamp").alias("shipping_limit_date"),
    F.col("price").cast("decimal(18,2)").alias("price"),
    F.col("freight_value").cast("decimal(18,2)").alias("freight_value"),
    *[F.col(name) for name in METADATA_COLUMNS]
)
write_silver("order_items", result)


# COMMAND ----------

# DBTITLE 1,11 - Build Silver Payments
source = read_bronze("payments", [
    "order_id", "payment_sequential", "payment_type",
    "payment_installments", "payment_value"
])
result = source.select(
    F.col("order_id").cast("string").alias("order_id"),
    F.col("payment_sequential").cast("int").alias("payment_sequential"),
    F.lower(F.trim(F.col("payment_type").cast("string"))).alias("payment_type"),
    F.col("payment_installments").cast("int").alias("payment_installments"),
    F.col("payment_value").cast("decimal(18,2)").alias("payment_value"),
    *[F.col(name) for name in METADATA_COLUMNS]
)
write_silver("payments", result)


# COMMAND ----------

# DBTITLE 1,12 - Build Silver Products
PRODUCT_INTEGER_COLUMNS = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
    "product_photos_qty": "product_photos_qty"
}
PRODUCT_DOUBLE_COLUMNS = [
    "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
]
source = read_bronze("products", [
    "product_id", "product_category_name",
    *PRODUCT_INTEGER_COLUMNS.keys(), *PRODUCT_DOUBLE_COLUMNS
])
result = source.select(
    F.col("product_id").cast("string").alias("product_id"),
    normalize_category(F.col("product_category_name")).alias("product_category_name"),
    *[
        F.col(source_name).cast("int").alias(target_name)
        for source_name, target_name in PRODUCT_INTEGER_COLUMNS.items()
    ],
    *[F.col(name).cast("double").alias(name) for name in PRODUCT_DOUBLE_COLUMNS],
    *[F.col(name) for name in METADATA_COLUMNS]
)
write_silver("products", result)


# COMMAND ----------

# DBTITLE 1,13 - Build Category Translation
source = read_bronze("product_category_translation", [
    "product_category_name", "product_category_name_english"
])
result = source.select(
    normalize_category(F.col("product_category_name")).alias("product_category_name"),
    normalize_category(F.col("product_category_name_english")).alias("product_category_name_english"),
    *[F.col(name) for name in METADATA_COLUMNS]
)
write_silver("product_category_translation", result)


# COMMAND ----------

# DBTITLE 1,14 - Build Silver Reviews
REVIEW_TIMESTAMP_COLUMNS = ["review_creation_date", "review_answer_timestamp"]
source = read_bronze("reviews", [
    "review_id", "order_id", "review_score", "review_comment_title",
    "review_comment_message", *REVIEW_TIMESTAMP_COLUMNS
])
result = source.select(
    F.col("review_id").cast("string").alias("review_id"),
    F.col("order_id").cast("string").alias("order_id"),
    F.col("review_score").cast("int").alias("review_score"),
    F.col("review_comment_title").cast("string").alias("review_comment_title"),
    F.col("review_comment_message").cast("string").alias("review_comment_message"),
    *[F.col(name).cast("timestamp").alias(name) for name in REVIEW_TIMESTAMP_COLUMNS],
    *[F.col(name) for name in METADATA_COLUMNS]
)
write_silver("reviews", result)


# COMMAND ----------

# DBTITLE 1,15 - Report Silver Build
expected_sources = set(SOURCES)
written_sources = {
    table_name.rsplit(".sl_", 1)[-1] for table_name in PUBLISHED_TABLES
}

if written_sources != expected_sources:
    missing = sorted(expected_sources - written_sources)
    raise ValueError(f"Silver build incomplete. Missing sources: {missing}")

print(f"Silver tables written: {len(PUBLISHED_TABLES)}")
for table_name in PUBLISHED_TABLES:
    print(table_name)
print("Run 07_silver_validation as the next, separate quality-gate task.")
