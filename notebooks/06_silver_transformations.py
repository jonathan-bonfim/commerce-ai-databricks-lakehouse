# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,01 - Configure Silver Environment
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column

CATALOG = "commerceai"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

METADATA_COLUMNS = [
    "bronze_ingestion_timestamp",
    "source_file_name",
    "source_file_path"
]

SOURCE_TABLES = {
    "customers": f"{CATALOG}.{BRONZE_SCHEMA}.br_customers",
    "sellers": f"{CATALOG}.{BRONZE_SCHEMA}.br_sellers"
}

TARGET_TABLES = {
    "customers": f"{CATALOG}.{SILVER_SCHEMA}.sl_customers",
    "sellers": f"{CATALOG}.{SILVER_SCHEMA}.sl_sellers"
}

print("Silver environment configured.")
print(f"Catalog: {CATALOG}")
print(f"Sources configured: {len(SOURCE_TABLES)}")

# COMMAND ----------

# DBTITLE 1,02 - Define Normalization Functions
def normalize_zip_prefix(column: Column) -> Column:

    zip_text = F.trim(column.cast("string"))

    return (
        F.when(
            zip_text.rlike(r"^[0-9]{1,5}$"),
            F.lpad(zip_text, 5, "0")
        )
        .otherwise(F.lit(None).cast("string"))
    )


def normalize_city(column: Column) -> Column:

    return F.lower(
        F.regexp_replace(
            F.trim(column.cast("string")),
            r"\s+",
            " "
        )
    )


def normalize_state(column: Column) -> Column:

    return F.upper(
        F.trim(column.cast("string"))
    )

# COMMAND ----------

# DBTITLE 1,03 - Transform Customers
bronze_customers = spark.table(
    SOURCE_TABLES["customers"]
)

silver_customers = bronze_customers.select(

    F.col("customer_id").cast("string").alias(
        "customer_id"
    ),

    F.col("customer_unique_id").cast("string").alias(
        "customer_unique_id"
    ),

    normalize_zip_prefix(
        F.col("customer_zip_code_prefix")
    ).alias("customer_zip_code_prefix"),

    normalize_city(
        F.col("customer_city")
    ).alias("customer_city"),

    normalize_state(
        F.col("customer_state")
    ).alias("customer_state"),

    *[F.col(column) for column in METADATA_COLUMNS]
)

display(silver_customers.limit(10))

# COMMAND ----------

# DBTITLE 1,04 - Transform Sellers
bronze_sellers = spark.table(
    SOURCE_TABLES["sellers"]
)

silver_sellers = bronze_sellers.select(

    F.col("seller_id").cast("string").alias(
        "seller_id"
    ),

    normalize_zip_prefix(
        F.col("seller_zip_code_prefix")
    ).alias("seller_zip_code_prefix"),

    normalize_city(
        F.col("seller_city")
    ).alias("seller_city"),

    normalize_state(
        F.col("seller_state")
    ).alias("seller_state"),

    *[F.col(column) for column in METADATA_COLUMNS]
)

display(silver_sellers.limit(10))

# COMMAND ----------

# DBTITLE 1,05 - Validate Silver Transformations
def validate_silver_entity(
    source_name: str,
    silver_df: DataFrame,
    key_column: str,
    zip_column: str
) -> dict:

    bronze_df = spark.table(
        SOURCE_TABLES[source_name]
    )

    bronze_count = bronze_df.count()
    silver_count = silver_df.count()

    null_key_rows = silver_df.filter(
        F.col(key_column).isNull()
        | (F.trim(F.col(key_column)) == "")
    ).count()

    duplicate_key_groups = (
        silver_df
        .groupBy(key_column)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    invalid_zip_rows = silver_df.filter(
        F.col(zip_column).isNull()
        | ~F.col(zip_column).rlike(r"^[0-9]{5}$")
    ).count()

    validation = {
        "source": source_name,
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "null_key_rows": null_key_rows,
        "duplicate_key_groups": duplicate_key_groups,
        "invalid_zip_rows": invalid_zip_rows
    }

    checks_passed = (
        bronze_count == silver_count
        and null_key_rows == 0
        and duplicate_key_groups == 0
        and invalid_zip_rows == 0
    )

    validation["status"] = (
        "PASS" if checks_passed else "FAIL"
    )

    return validation


validation_results = [
    validate_silver_entity(
        "customers",
        silver_customers,
        "customer_id",
        "customer_zip_code_prefix"
    ),
    validate_silver_entity(
        "sellers",
        silver_sellers,
        "seller_id",
        "seller_zip_code_prefix"
    )
]

validation_df = spark.createDataFrame(validation_results)

display(validation_df)

if any(
    result["status"] == "FAIL"
    for result in validation_results
):
    raise ValueError(
        "Silver transformation validation failed."
    )

print("All Silver transformation validations passed.")

# COMMAND ----------

# DBTITLE 1,06 - Write Silver Delta Tables
SILVER_DATAFRAMES = {
    "customers": silver_customers,
    "sellers": silver_sellers
}

for source, df in SILVER_DATAFRAMES.items():

    target_table = TARGET_TABLES[source]

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"Successfully written: {target_table}")

# COMMAND ----------

# DBTITLE 1,07 - Validate Persisted Silver Tables
persisted_results = []

for source, table_name in TARGET_TABLES.items():

    df = spark.table(table_name)

    zip_column = (
        "customer_zip_code_prefix"
        if source == "customers"
        else "seller_zip_code_prefix"
    )

    row_count = df.count()

    zip_type = df.schema[
        zip_column
    ].dataType.simpleString()

    padded_zip_rows = df.filter(
        F.col(zip_column).startswith("0")
    ).count()

    expected_count = spark.table(
        SOURCE_TABLES[source]
    ).count()

    status = (
        "PASS"
        if row_count == expected_count
        and zip_type == "string"
        else "FAIL"
    )

    persisted_results.append({
        "source": source,
        "table_name": table_name,
        "row_count": row_count,
        "zip_data_type": zip_type,
        "padded_zip_rows": padded_zip_rows,
        "status": status
    })

persisted_df = spark.createDataFrame(persisted_results)

display(persisted_df)

if any(
    row["status"] == "FAIL"
    for row in persisted_results
):
    raise ValueError(
        "Persisted Silver table validation failed."
    )

print("Persisted Silver table validation passed.")

# COMMAND ----------

# DBTITLE 1,08 - Configure Geolocation Rules
from pyspark import StorageLevel
from pyspark.sql import Window

SOURCE_TABLES["geolocation"] = (
    f"{CATALOG}.{BRONZE_SCHEMA}.br_geolocation"
)

TARGET_TABLES["geolocation"] = (
    f"{CATALOG}.{SILVER_SCHEMA}.sl_geolocation"
)

ZIP_COLUMN = "geolocation_zip_code_prefix"

BRAZIL_STATES = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF",
    "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS",
    "RO", "RR", "SC", "SP", "SE", "TO"
]

VALID_COORDINATES = (
    F.col("geolocation_lat").between(-34.0, 6.0)
    & F.col("geolocation_lng").between(-74.0, -28.0)
)

print("Geolocation rules configured.")

# COMMAND ----------

# DBTITLE 1,09 - Normalize and Deduplicate Geolocation
bronze_geolocation = spark.table(
    SOURCE_TABLES["geolocation"]
)

geolocation_normalized = bronze_geolocation.select(

    normalize_zip_prefix(
        F.col("geolocation_zip_code_prefix")
    ).alias("geolocation_zip_code_prefix"),

    F.col("geolocation_lat").cast("double").alias(
        "geolocation_lat"
    ),

    F.col("geolocation_lng").cast("double").alias(
        "geolocation_lng"
    ),

    normalize_city(
        F.col("geolocation_city")
    ).alias("geolocation_city"),

    normalize_state(
        F.col("geolocation_state")
    ).alias("geolocation_state")
)

geolocation_distinct = (
    geolocation_normalized
    .dropDuplicates()
)

source_count = bronze_geolocation.count()
distinct_count = geolocation_distinct.count()

print(f"Source rows: {source_count}")
print(f"Distinct normalized rows: {distinct_count}")
print(f"Removed duplicate rows: {source_count - distinct_count}")

# COMMAND ----------

# DBTITLE 1,10 - Select Dominant State and City
state_counts = (
    geolocation_distinct
    .filter(
        F.col("geolocation_state").isin(*BRAZIL_STATES)
    )
    .groupBy(
        ZIP_COLUMN,
        "geolocation_state"
    )
    .agg(
        F.count("*").alias("state_record_count")
    )
)

state_window = (
    Window
    .partitionBy(ZIP_COLUMN)
    .orderBy(
        F.desc("state_record_count"),
        F.asc("geolocation_state")
    )
)

dominant_state = (
    state_counts
    .withColumn(
        "state_rank",
        F.row_number().over(state_window)
    )
    .filter(F.col("state_rank") == 1)
    .select(
        ZIP_COLUMN,
        "geolocation_state"
    )
)

city_counts = (
    geolocation_distinct
    .join(
        dominant_state,
        on=[ZIP_COLUMN, "geolocation_state"],
        how="inner"
    )
    .filter(
        F.col("geolocation_city").isNotNull()
        & (F.col("geolocation_city") != "")
    )
    .groupBy(
        ZIP_COLUMN,
        "geolocation_city"
    )
    .agg(
        F.count("*").alias("city_record_count")
    )
)

city_window = (
    Window
    .partitionBy(ZIP_COLUMN)
    .orderBy(
        F.desc("city_record_count"),
        F.asc("geolocation_city")
    )
)

dominant_city = (
    city_counts
    .withColumn(
        "city_rank",
        F.row_number().over(city_window)
    )
    .filter(F.col("city_rank") == 1)
    .select(
        ZIP_COLUMN,
        "geolocation_city"
    )
)

print("Dominant state and city selection configured.")

# COMMAND ----------

# DBTITLE 1,11 - Select Representative Coordinates
valid_geolocation = (
    geolocation_distinct
    .join(
        dominant_state,
        on=[ZIP_COLUMN, "geolocation_state"],
        how="inner"
    )
    .filter(VALID_COORDINATES)
)

coordinate_medians = (
    valid_geolocation
    .groupBy(ZIP_COLUMN)
    .agg(
        F.percentile_approx(
            "geolocation_lat", 0.5, 10000
        ).alias("_median_lat"),

        F.percentile_approx(
            "geolocation_lng", 0.5, 10000
        ).alias("_median_lng")
    )
)

coordinate_candidates = (
    valid_geolocation
    .join(
        coordinate_medians,
        on=ZIP_COLUMN,
        how="inner"
    )
    .withColumn(
        "_distance_score",
        F.pow(
            F.col("geolocation_lat") - F.col("_median_lat"),
            2
        )
        +
        F.pow(
            (
                F.col("geolocation_lng")
                - F.col("_median_lng")
            )
            * F.cos(F.radians(F.col("_median_lat"))),
            2
        )
    )
)

coordinate_window = (
    Window
    .partitionBy(ZIP_COLUMN)
    .orderBy(
        F.asc("_distance_score"),
        F.asc("geolocation_lat"),
        F.asc("geolocation_lng"),
        F.asc_nulls_last("geolocation_city")
    )
)

representative_coordinates = (
    coordinate_candidates
    .withColumn(
        "_coordinate_rank",
        F.row_number().over(coordinate_window)
    )
    .filter(F.col("_coordinate_rank") == 1)
    .select(
        ZIP_COLUMN,
        "geolocation_lat",
        "geolocation_lng"
    )
)

print("Representative coordinate selection configured.")

# COMMAND ----------

# DBTITLE 1,12 - Build Silver Geolocation Dataset
source_metrics = (
    geolocation_normalized
    .groupBy(ZIP_COLUMN)
    .agg(
        F.count("*").alias(
            "geolocation_source_record_count"
        ),

        F.countDistinct("geolocation_state").alias(
            "geolocation_state_variants"
        ),

        F.countDistinct("geolocation_city").alias(
            "geolocation_city_variants"
        )
    )
)

distinct_metrics = (
    geolocation_distinct
    .groupBy(ZIP_COLUMN)
    .agg(
        F.count("*").alias(
            "geolocation_distinct_record_count"
        ),

        F.sum(
            F.when(VALID_COORDINATES, 1).otherwise(0)
        ).alias(
            "geolocation_valid_coordinate_records"
        )
    )
)

selected_coordinate_metrics = (
    valid_geolocation
    .groupBy(ZIP_COLUMN)
    .agg(
        F.count("*").alias(
            "geolocation_selected_state_coordinate_count"
        )
    )
)

silver_geolocation = (
    source_metrics
    .join(distinct_metrics, ZIP_COLUMN, "left")
    .join(dominant_state, ZIP_COLUMN, "left")
    .join(dominant_city, ZIP_COLUMN, "left")
    .join(representative_coordinates, ZIP_COLUMN, "left")
    .join(selected_coordinate_metrics, ZIP_COLUMN, "left")

    .fillna({
        "geolocation_selected_state_coordinate_count": 0
    })

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

    .withColumn(
        "source_table_name",
        F.lit(SOURCE_TABLES["geolocation"])
    )

    .select(
        ZIP_COLUMN,
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
        "geolocation_source_record_count",
        "geolocation_distinct_record_count",
        "geolocation_valid_coordinate_records",
        "geolocation_selected_state_coordinate_count",
        "geolocation_state_variants",
        "geolocation_city_variants",
        "geolocation_has_state_conflict",
        "geolocation_has_city_variation",
        "geolocation_coordinate_status",
        "source_table_name"
    )
)

display(silver_geolocation.limit(10))

# COMMAND ----------

# DBTITLE 1,13 - Validate Geolocation Transformation
geo_validation = silver_geolocation.agg(

    F.count("*").alias("silver_rows"),

    F.countDistinct(ZIP_COLUMN).alias(
        "distinct_zip_prefixes"
    ),

    F.sum("geolocation_source_record_count").alias(
        "reconciled_source_rows"
    ),

    F.sum(
        F.when(
            F.col(ZIP_COLUMN).isNull()
            | ~F.col(ZIP_COLUMN).rlike(r"^[0-9]{5}$"),
            1
        ).otherwise(0)
    ).alias("invalid_zip_rows"),

    F.sum(
        F.when(
            F.col("geolocation_lat").isNull()
            != F.col("geolocation_lng").isNull(),
            1
        ).otherwise(0)
    ).alias("incomplete_coordinate_pairs"),

    F.sum(
        F.when(
            F.col("geolocation_has_state_conflict"), 1
        ).otherwise(0)
    ).alias("prefixes_with_state_conflicts"),

    F.sum(
        F.when(
            F.col("geolocation_has_city_variation"), 1
        ).otherwise(0)
    ).alias("prefixes_with_city_variations"),

    F.sum(
        F.when(
            F.col("geolocation_coordinate_status") == "MISSING",
            1
        ).otherwise(0)
    ).alias("prefixes_without_coordinates")

).first().asDict()

expected_zip_count = (
    geolocation_normalized
    .select(ZIP_COLUMN)
    .distinct()
    .count()
)

validation_passed = (
    geo_validation["silver_rows"] == expected_zip_count
    and geo_validation["distinct_zip_prefixes"] == expected_zip_count
    and geo_validation["reconciled_source_rows"] == source_count
    and geo_validation["invalid_zip_rows"] == 0
    and geo_validation["incomplete_coordinate_pairs"] == 0
)

display(spark.createDataFrame([geo_validation]))

print(
    "Geolocation validation: "
    + ("PASS" if validation_passed else "FAIL")
)

if not validation_passed:
    raise ValueError("Silver geolocation validation failed.")

# COMMAND ----------

# DBTITLE 1,13A - Investigate Geolocation Quality Exceptions
# Identify ZIP prefixes without representative coordinates
missing_coordinates = (
    silver_geolocation
    .filter(
        F.col("geolocation_coordinate_status") == "MISSING"
    )
)

print("ZIP prefixes without representative coordinates:")

display(
    missing_coordinates.select(
        ZIP_COLUMN,
        "geolocation_state",
        "geolocation_city",
        "geolocation_source_record_count",
        "geolocation_distinct_record_count",
        "geolocation_valid_coordinate_records",
        "geolocation_selected_state_coordinate_count",
        "geolocation_state_variants"
    )
)

# Investigate available coordinates by state
missing_zip_prefixes = missing_coordinates.select(
    ZIP_COLUMN
)

missing_coordinate_details = (
    geolocation_distinct
    .join(
        missing_zip_prefixes,
        on=ZIP_COLUMN,
        how="inner"
    )
    .groupBy(
        ZIP_COLUMN,
        "geolocation_state"
    )
    .agg(
        F.count("*").alias("distinct_records"),

        F.sum(
            F.when(VALID_COORDINATES, 1).otherwise(0)
        ).alias("valid_coordinate_records"),

        F.min("geolocation_lat").alias("min_latitude"),
        F.max("geolocation_lat").alias("max_latitude"),

        F.min("geolocation_lng").alias("min_longitude"),
        F.max("geolocation_lng").alias("max_longitude")
    )
)

print("Coordinate availability by ZIP prefix and state:")

display(
    missing_coordinate_details.orderBy(
        ZIP_COLUMN,
        F.desc("distinct_records")
    )
)

# Inspect ZIP prefixes associated with multiple states
print("ZIP prefixes with conflicting states:")

display(
    silver_geolocation
    .filter(
        F.col("geolocation_has_state_conflict")
    )
    .select(
        ZIP_COLUMN,
        "geolocation_state",
        "geolocation_city",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_state_variants",
        "geolocation_source_record_count"
    )
    .orderBy(ZIP_COLUMN)
)

# COMMAND ----------

# DBTITLE 1,14 - Write and Validate Silver Geolocation
if not validation_passed:
    raise ValueError(
        "Geolocation validation must pass before writing."
    )

target_table = TARGET_TABLES["geolocation"]

(
    silver_geolocation.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

persisted_geo = spark.table(target_table)

persisted_count = persisted_geo.count()
persisted_distinct_count = (
    persisted_geo
    .select(ZIP_COLUMN)
    .distinct()
    .count()
)

if (
    persisted_count != expected_zip_count
    or persisted_distinct_count != expected_zip_count
):
    raise ValueError(
        "Persisted geolocation validation failed."
    )

print(f"Successfully written: {target_table}")
print(f"Persisted records: {persisted_count}")
print("Persisted validation: PASS")

display(
    persisted_geo
    .filter(
        F.col(ZIP_COLUMN).isin("72915", "04011")
    )
)

# COMMAND ----------

# DBTITLE 1,15 - Transform Orders
from pyspark.sql import functions as F

# Register source and target tables
SOURCE_TABLES["orders"] = (
    f"{CATALOG}.{BRONZE_SCHEMA}.br_orders"
)

TARGET_TABLES["orders"] = (
    f"{CATALOG}.{SILVER_SCHEMA}.sl_orders"
)

# Define expected source columns
ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

ORDER_TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

# Read Bronze orders
bronze_orders = spark.table(
    SOURCE_TABLES["orders"]
)

# Validate source columns
expected_columns = set(
    ORDER_COLUMNS + METADATA_COLUMNS
)

missing_columns = sorted(
    expected_columns - set(bronze_orders.columns)
)

if missing_columns:
    raise ValueError(
        f"Missing source columns: {missing_columns}"
    )

# Apply Silver transformations
silver_orders = bronze_orders.select(

    F.col("order_id")
        .cast("string")
        .alias("order_id"),

    F.col("customer_id")
        .cast("string")
        .alias("customer_id"),

    F.lower(
        F.trim(F.col("order_status").cast("string"))
    ).alias("order_status"),

    *[
        F.col(column_name)
            .cast("timestamp")
            .alias(column_name)
        for column_name in ORDER_TIMESTAMP_COLUMNS
    ],

    *[
        F.col(column_name)
        for column_name in METADATA_COLUMNS
    ]
)

print("Silver Orders transformation configured.")

print("Source table:", SOURCE_TABLES["orders"])
print("Target table:", TARGET_TABLES["orders"])

display(silver_orders.limit(10))

# COMMAND ----------

# DBTITLE 1,16 - Validate Orders Granularity and Relationships
# Compare Bronze and Silver row counts
bronze_count = bronze_orders.count()
silver_count = silver_orders.count()

# Validate primary and foreign keys
key_metrics = silver_orders.agg(

    F.countDistinct("order_id").alias(
        "distinct_order_ids"
    ),

    F.sum(
        F.when(
            F.col("order_id").isNull()
            | (F.trim("order_id") == ""),
            1
        ).otherwise(0)
    ).alias("invalid_order_id_rows"),

    F.sum(
        F.when(
            F.col("customer_id").isNull()
            | (F.trim("customer_id") == ""),
            1
        ).otherwise(0)
    ).alias("invalid_customer_id_rows"),

    F.sum(
        F.when(
            F.col("order_status").isNull()
            | (F.col("order_status") == ""),
            1
        ).otherwise(0)
    ).alias("invalid_order_status_rows")

).first().asDict()

# Validate customer relationships
silver_customers_reference = (
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.sl_customers"
    )
    .select("customer_id")
    .distinct()
)

orphan_orders = (
    silver_orders
    .select("order_id", "customer_id")
    .join(
        silver_customers_reference,
        on="customer_id",
        how="left_anti"
    )
    .count()
)

# Check timestamp data types and null preservation
invalid_timestamp_types = []

timestamp_null_losses = {}

for column_name in ORDER_TIMESTAMP_COLUMNS:

    actual_type = (
        silver_orders.schema[column_name]
        .dataType.simpleString()
    )

    if actual_type != "timestamp":
        invalid_timestamp_types.append(column_name)

bronze_timestamp_counts = bronze_orders.agg(
    *[
        F.count(column_name).alias(column_name)
        for column_name in ORDER_TIMESTAMP_COLUMNS
    ]
).first()

silver_timestamp_counts = silver_orders.agg(
    *[
        F.count(column_name).alias(column_name)
        for column_name in ORDER_TIMESTAMP_COLUMNS
    ]
).first()

for column_name in ORDER_TIMESTAMP_COLUMNS:

    timestamp_null_losses[column_name] = (
        bronze_timestamp_counts[column_name]
        - silver_timestamp_counts[column_name]
    )

# Build validation result
orders_validation = {
    "bronze_count": bronze_count,
    "silver_count": silver_count,
    "distinct_order_ids": key_metrics["distinct_order_ids"],
    "invalid_order_id_rows": key_metrics["invalid_order_id_rows"],
    "invalid_customer_id_rows": key_metrics["invalid_customer_id_rows"],
    "invalid_order_status_rows": key_metrics["invalid_order_status_rows"],
    "orphan_orders": orphan_orders,
    "invalid_timestamp_types": len(invalid_timestamp_types),
    "timestamp_null_losses": sum(
        abs(value)
        for value in timestamp_null_losses.values()
    )
}

orders_validation_passed = (
    bronze_count == silver_count
    and silver_count == key_metrics["distinct_order_ids"]
    and key_metrics["invalid_order_id_rows"] == 0
    and key_metrics["invalid_customer_id_rows"] == 0
    and key_metrics["invalid_order_status_rows"] == 0
    and orphan_orders == 0
    and len(invalid_timestamp_types) == 0
    and all(
        value == 0
        for value in timestamp_null_losses.values()
    )
)

orders_validation["status"] = (
    "PASS" if orders_validation_passed else "FAIL"
)

display(
    spark.createDataFrame([orders_validation])
)

print("Timestamp null preservation:")
print(timestamp_null_losses)

if not orders_validation_passed:
    raise ValueError(
        "Silver Orders validation failed."
    )

print("All Silver Orders validations passed.")

# COMMAND ----------

# DBTITLE 1,17 - Profile Order Status and Timestamp Consistency
# Inspect session timezone without changing it
print(
    "Session timezone:",
    spark.conf.get("spark.sql.session.timeZone")
)

# Inspect order status distribution
print("Order status distribution:")

display(
    silver_orders
    .groupBy("order_status")
    .count()
    .orderBy(F.desc("count"))
)

# Identify unexpected status values
KNOWN_ORDER_STATUSES = [
    "delivered",
    "shipped",
    "canceled",
    "unavailable",
    "invoiced",
    "processing",
    "created",
    "approved"
]

unexpected_status_count = (
    silver_orders
    .filter(
        ~F.col("order_status").isin(*KNOWN_ORDER_STATUSES)
    )
    .count()
)

# Profile temporal consistency
temporal_metrics = silver_orders.agg(

    F.min("order_purchase_timestamp").alias(
        "earliest_purchase"
    ),

    F.max("order_purchase_timestamp").alias(
        "latest_purchase"
    ),

    F.sum(
        F.when(
            F.col("order_purchase_timestamp").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_purchase_timestamp"),

    F.sum(
        F.when(
            F.col("order_estimated_delivery_date").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_estimated_delivery_date"),

    F.sum(
        F.when(
            (F.col("order_status") == "delivered")
            & F.col("order_delivered_customer_date").isNull(),
            1
        ).otherwise(0)
    ).alias("delivered_without_delivery_date"),

    F.sum(
        F.when(
            F.col("order_approved_at")
            < F.col("order_purchase_timestamp"),
            1
        ).otherwise(0)
    ).alias("approval_before_purchase"),

    F.sum(
        F.when(
            F.col("order_estimated_delivery_date")
            < F.col("order_purchase_timestamp"),
            1
        ).otherwise(0)
    ).alias("estimated_delivery_before_purchase"),

    F.sum(
        F.when(
            F.col("order_delivered_customer_date")
            < F.col("order_purchase_timestamp"),
            1
        ).otherwise(0)
    ).alias("delivery_before_purchase"),

    F.sum(
        F.when(
            F.col("order_delivered_carrier_date")
            > F.col("order_delivered_customer_date"),
            1
        ).otherwise(0)
    ).alias("carrier_timestamp_after_customer_delivery")

)

print("Temporal quality profile:")

display(temporal_metrics)

print(
    f"Unexpected order statuses: {unexpected_status_count}"
)

# COMMAND ----------

# DBTITLE 1,18 - Write Silver Orders Delta Table
# Prevent writing invalid transformed data
if not orders_validation_passed:
    raise ValueError(
        "Silver Orders validation must pass before writing."
    )

target_table = TARGET_TABLES["orders"]

# Write the Silver table
(
    silver_orders.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Successfully written: {target_table}")

# COMMAND ----------

# DBTITLE 1,19 - Validate Persisted Silver Orders
# Read persisted Silver table
persisted_orders = spark.table(
    TARGET_TABLES["orders"]
)

# Validate row count and key uniqueness
persisted_metrics = persisted_orders.agg(

    F.count("*").alias("persisted_count"),

    F.countDistinct("order_id").alias(
        "distinct_order_ids"
    ),

    F.sum(
        F.when(
            F.col("order_id").isNull(), 1
        ).otherwise(0)
    ).alias("null_order_ids")

).first().asDict()

# Align column order for exact comparison
expected_orders = silver_orders.select(
    *silver_orders.columns
)

actual_orders = persisted_orders.select(
    *silver_orders.columns
)

# Compare rows in both directions
missing_from_target = (
    expected_orders
    .exceptAll(actual_orders)
    .count()
)

unexpected_in_target = (
    actual_orders
    .exceptAll(expected_orders)
    .count()
)

# Build validation result
persisted_validation = {
    "source": "orders",
    "target_table": TARGET_TABLES["orders"],
    "bronze_count": bronze_count,
    "persisted_count": persisted_metrics["persisted_count"],
    "distinct_order_ids": persisted_metrics["distinct_order_ids"],
    "null_order_ids": persisted_metrics["null_order_ids"],
    "missing_from_target": missing_from_target,
    "unexpected_in_target": unexpected_in_target
}

persisted_validation_passed = (
    orders_validation_passed
    and persisted_metrics["persisted_count"] == bronze_count
    and persisted_metrics["distinct_order_ids"] == bronze_count
    and persisted_metrics["null_order_ids"] == 0
    and missing_from_target == 0
    and unexpected_in_target == 0
)

persisted_validation["status"] = (
    "PASS" if persisted_validation_passed else "FAIL"
)

display(
    spark.createDataFrame([persisted_validation])
)

if not persisted_validation_passed:
    raise ValueError(
        "Persisted Silver Orders validation failed."
    )

print("Persisted Silver Orders validation: PASS")

# COMMAND ----------

# DBTITLE 1,20 - Transform Order Items
# Register source and target tables
SOURCE_TABLES["order_items"] = (
    f"{CATALOG}.{BRONZE_SCHEMA}.br_order_items"
)

TARGET_TABLES["order_items"] = (
    f"{CATALOG}.{SILVER_SCHEMA}.sl_order_items"
)

# Define expected source columns
ORDER_ITEM_COLUMNS = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value"
]

# Read Bronze source
bronze_order_items = spark.table(
    SOURCE_TABLES["order_items"]
)

# Validate source contract
expected_columns = set(
    ORDER_ITEM_COLUMNS + METADATA_COLUMNS
)

missing_columns = sorted(
    expected_columns - set(bronze_order_items.columns)
)

if missing_columns:
    raise ValueError(
        f"Missing source columns: {missing_columns}"
    )

# Apply Silver transformations
silver_order_items = bronze_order_items.select(

    F.col("order_id")
        .cast("string")
        .alias("order_id"),

    F.col("order_item_id")
        .cast("int")
        .alias("order_item_id"),

    F.col("product_id")
        .cast("string")
        .alias("product_id"),

    F.col("seller_id")
        .cast("string")
        .alias("seller_id"),

    F.col("shipping_limit_date")
        .cast("timestamp")
        .alias("shipping_limit_date"),

    F.col("price")
        .cast("decimal(18,2)")
        .alias("price"),

    F.col("freight_value")
        .cast("decimal(18,2)")
        .alias("freight_value"),

    *[
        F.col(column_name)
        for column_name in METADATA_COLUMNS
    ]
)

print("Silver Order Items transformation configured.")

silver_order_items.printSchema()

display(silver_order_items.limit(10))

# COMMAND ----------

# DBTITLE 1,21 - Validate Order Item Keys and Relationships
# Reconcile source and transformed row counts
bronze_item_count = bronze_order_items.count()
silver_item_count = silver_order_items.count()

# Validate required identifiers
item_key_metrics = silver_order_items.agg(

    F.sum(
        F.when(
            F.col("order_id").isNull()
            | (F.trim("order_id") == ""),
            1
        ).otherwise(0)
    ).alias("invalid_order_id_rows"),

    F.sum(
        F.when(
            F.col("order_item_id").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_order_item_id_rows"),

    F.sum(
        F.when(
            F.col("product_id").isNull()
            | (F.trim("product_id") == ""),
            1
        ).otherwise(0)
    ).alias("invalid_product_id_rows"),

    F.sum(
        F.when(
            F.col("seller_id").isNull()
            | (F.trim("seller_id") == ""),
            1
        ).otherwise(0)
    ).alias("invalid_seller_id_rows")

).first().asDict()

# Validate composite key uniqueness
duplicate_item_keys = (
    silver_order_items
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Load relationship references
orders_reference = (
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.sl_orders"
    )
    .select("order_id")
    .distinct()
)

sellers_reference = (
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.sl_sellers"
    )
    .select("seller_id")
    .distinct()
)

products_reference = (
    spark.table(
        f"{CATALOG}.{BRONZE_SCHEMA}.br_products"
    )
    .select("product_id")
    .distinct()
)

# Check orphan records
orphan_order_items = (
    silver_order_items
    .select("order_id")
    .join(orders_reference, "order_id", "left_anti")
    .count()
)

orphan_seller_items = (
    silver_order_items
    .select("seller_id")
    .join(sellers_reference, "seller_id", "left_anti")
    .count()
)

orphan_product_items = (
    silver_order_items
    .select("product_id")
    .join(products_reference, "product_id", "left_anti")
    .count()
)

# Build validation results
item_validation = {
    "bronze_count": bronze_item_count,
    "silver_count": silver_item_count,
    "duplicate_key_groups": duplicate_item_keys,
    "orphan_order_items": orphan_order_items,
    "orphan_seller_items": orphan_seller_items,
    "orphan_product_items": orphan_product_items,
    **item_key_metrics
}

item_validation_passed = (
    bronze_item_count == silver_item_count
    and duplicate_item_keys == 0
    and orphan_order_items == 0
    and orphan_seller_items == 0
    and orphan_product_items == 0
    and all(
        value == 0
        for value in item_key_metrics.values()
    )
)

item_validation["status"] = (
    "PASS" if item_validation_passed else "FAIL"
)

display(spark.createDataFrame([item_validation]))

if not item_validation_passed:
    raise ValueError(
        "Silver Order Items validation failed."
    )

print("All Order Items key and relationship validations passed.")

# COMMAND ----------

# DBTITLE 1,22 - Audit Monetary Precision and Item Quality
# Define financial columns
FINANCIAL_COLUMNS = [
    "price",
    "freight_value"
]

# Build financial conversion checks
financial_checks = []

for column_name in FINANCIAL_COLUMNS:

    original_value = F.col(column_name).cast("double")

    decimal_value = F.col(column_name).cast(
        "decimal(18,2)"
    )

    financial_checks.extend([

        F.sum(
            F.when(
                F.col(column_name).isNotNull()
                & decimal_value.isNull(),
                1
            ).otherwise(0)
        ).alias(f"{column_name}_conversion_failures"),

        F.sum(
            F.when(
                original_value.isNotNull()
                & (
                    F.abs(
                        original_value
                        - decimal_value.cast("double")
                    ) > 0.000001
                ),
                1
            ).otherwise(0)
        ).alias(f"{column_name}_precision_changes")

    ])

# Evaluate financial conversion safety
financial_metrics = (
    bronze_order_items
    .agg(*financial_checks)
    .first()
    .asDict()
)

print("Financial conversion assessment:")

display(spark.createDataFrame([financial_metrics]))

# Check additional data quality conditions
item_quality_metrics = (
    silver_order_items
    .agg(

        F.sum(
            F.when(
                F.col("price").isNull(), 1
            ).otherwise(0)
        ).alias("missing_price_rows"),

        F.sum(
            F.when(
                F.col("freight_value").isNull(), 1
            ).otherwise(0)
        ).alias("missing_freight_rows"),

        F.sum(
            F.when(
                F.col("price") < 0, 1
            ).otherwise(0)
        ).alias("negative_price_rows"),

        F.sum(
            F.when(
                F.col("freight_value") < 0, 1
            ).otherwise(0)
        ).alias("negative_freight_rows"),

        F.sum(
            F.when(
                F.col("price") == 0, 1
            ).otherwise(0)
        ).alias("zero_price_rows"),

        F.sum(
            F.when(
                F.col("shipping_limit_date").isNull(), 1
            ).otherwise(0)
        ).alias("missing_shipping_limit_rows")

    )
    .first()
    .asDict()
)

print("Order Items quality profile:")

display(spark.createDataFrame([item_quality_metrics]))

# Compare shipping deadlines with purchase timestamps
shipping_deadline_profile = (
    silver_order_items
    .join(
        spark.table(
            f"{CATALOG}.{SILVER_SCHEMA}.sl_orders"
        ).select(
            "order_id",
            "order_purchase_timestamp"
        ),
        on="order_id",
        how="left"
    )
    .agg(
        F.sum(
            F.when(
                F.col("shipping_limit_date")
                < F.col("order_purchase_timestamp"),
                1
            ).otherwise(0)
        ).alias("shipping_deadline_before_purchase")
    )
    .first()
    .asDict()
)

print("Shipping deadline assessment:")

display(spark.createDataFrame([shipping_deadline_profile]))

# Define financial precision gate
financial_precision_passed = all(
    financial_metrics[
        f"{column_name}_conversion_failures"
    ] == 0
    and financial_metrics[
        f"{column_name}_precision_changes"
    ] == 0
    for column_name in FINANCIAL_COLUMNS
)

print(
    "Financial precision validation: "
    + ("PASS" if financial_precision_passed else "FAIL")
)

if not financial_precision_passed:
    raise ValueError(
        "Financial conversion introduced unexpected changes."
    )

# COMMAND ----------

# DBTITLE 1,23 - Write Silver Order Items Delta Table
# Validate all required gates before writing
if not item_validation_passed:
    raise ValueError(
        "Order Items structural validation failed."
    )

if not financial_precision_passed:
    raise ValueError(
        "Order Items financial precision validation failed."
    )

target_table = TARGET_TABLES["order_items"]

# Write Delta table using full-load strategy
(
    silver_order_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Successfully written: {target_table}")

# COMMAND ----------

# DBTITLE 1,24 - Validate Persisted Silver Order Items
# Read persisted Silver table
persisted_order_items = spark.table(
    TARGET_TABLES["order_items"]
)

# Validate persisted count
persisted_item_count = persisted_order_items.count()

# Validate persisted composite key
persisted_duplicate_keys = (
    persisted_order_items
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Align columns before comparing datasets
expected_items = silver_order_items.select(
    *silver_order_items.columns
)

actual_items = persisted_order_items.select(
    *silver_order_items.columns
)

# Compare expected and persisted content
missing_from_target = (
    expected_items
    .exceptAll(actual_items)
    .count()
)

unexpected_in_target = (
    actual_items
    .exceptAll(expected_items)
    .count()
)

# Verify persisted financial data types
financial_types_valid = all(
    persisted_order_items.schema[column_name]
    .dataType.simpleString() == "decimal(18,2)"
    for column_name in FINANCIAL_COLUMNS
)

# Define persisted validation
persisted_item_validation_passed = (
    item_validation_passed
    and financial_precision_passed
    and persisted_item_count == bronze_item_count
    and persisted_duplicate_keys == 0
    and missing_from_target == 0
    and unexpected_in_target == 0
    and financial_types_valid
)

validation_result = {
    "source": "order_items",
    "bronze_count": bronze_item_count,
    "persisted_count": persisted_item_count,
    "duplicate_key_groups": persisted_duplicate_keys,
    "missing_from_target": missing_from_target,
    "unexpected_in_target": unexpected_in_target,
    "financial_types_valid": financial_types_valid,
    "status": (
        "PASS"
        if persisted_item_validation_passed
        else "FAIL"
    )
}

display(
    spark.createDataFrame([validation_result])
)

if not persisted_item_validation_passed:
    raise ValueError(
        "Persisted Order Items validation failed."
    )

print("Persisted Silver Order Items validation: PASS")

# COMMAND ----------

# DBTITLE 1,25 - Transform Payments
# Register source and target tables
SOURCE_TABLES["payments"] = (
    f"{CATALOG}.{BRONZE_SCHEMA}.br_payments"
)

TARGET_TABLES["payments"] = (
    f"{CATALOG}.{SILVER_SCHEMA}.sl_payments"
)

# Define expected source columns
PAYMENT_COLUMNS = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value"
]

# Read Bronze source
bronze_payments = spark.table(
    SOURCE_TABLES["payments"]
)

# Validate source contract
expected_columns = set(
    PAYMENT_COLUMNS + METADATA_COLUMNS
)

missing_columns = sorted(
    expected_columns - set(bronze_payments.columns)
)

if missing_columns:
    raise ValueError(
        f"Missing source columns: {missing_columns}"
    )

# Apply Silver transformations
silver_payments = bronze_payments.select(

    F.col("order_id")
        .cast("string")
        .alias("order_id"),

    F.col("payment_sequential")
        .cast("int")
        .alias("payment_sequential"),

    F.lower(
        F.trim(F.col("payment_type").cast("string"))
    ).alias("payment_type"),

    F.col("payment_installments")
        .cast("int")
        .alias("payment_installments"),

    F.col("payment_value")
        .cast("decimal(18,2)")
        .alias("payment_value"),

    *[
        F.col(column_name)
        for column_name in METADATA_COLUMNS
    ]
)

print("Silver Payments transformation configured.")

silver_payments.printSchema()

display(silver_payments.limit(10))

# COMMAND ----------

# DBTITLE 1,26 - Validate Payment Keys and Relationships
# Reconcile source and transformed row counts
bronze_payment_count = bronze_payments.count()
silver_payment_count = silver_payments.count()

# Validate required keys
payment_key_metrics = silver_payments.agg(

    F.countDistinct(
        "order_id",
        "payment_sequential"
    ).alias("distinct_payment_keys"),

    F.sum(
        F.when(
            F.col("order_id").isNull()
            | (F.trim("order_id") == ""),
            1
        ).otherwise(0)
    ).alias("invalid_order_id_rows"),

    F.sum(
        F.when(
            F.col("payment_sequential").isNull(),
            1
        ).otherwise(0)
    ).alias("null_payment_sequential_rows"),

    F.sum(
        F.when(
            F.col("payment_sequential") <= 0,
            1
        ).otherwise(0)
    ).alias("nonpositive_payment_sequential_rows")

).first().asDict()

# Validate composite key uniqueness
duplicate_payment_keys = (
    silver_payments
    .groupBy("order_id", "payment_sequential")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Load Silver Orders reference
orders_reference = (
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.sl_orders"
    )
    .select("order_id")
    .distinct()
)

# Validate referential integrity
orphan_payment_rows = (
    silver_payments
    .select("order_id")
    .join(
        orders_reference,
        on="order_id",
        how="left_anti"
    )
    .count()
)

# Validate column types
payment_key_types_valid = (
    silver_payments.schema["order_id"]
        .dataType.simpleString() == "string"
    and
    silver_payments.schema["payment_sequential"]
        .dataType.simpleString() == "int"
)

# Build validation results
payment_validation = {
    "bronze_count": bronze_payment_count,
    "silver_count": silver_payment_count,
    "distinct_payment_keys": payment_key_metrics[
        "distinct_payment_keys"
    ],
    "duplicate_key_groups": duplicate_payment_keys,
    "invalid_order_id_rows": payment_key_metrics[
        "invalid_order_id_rows"
    ],
    "null_payment_sequential_rows": payment_key_metrics[
        "null_payment_sequential_rows"
    ],
    "nonpositive_payment_sequential_rows": payment_key_metrics[
        "nonpositive_payment_sequential_rows"
    ],
    "orphan_payment_rows": orphan_payment_rows,
    "payment_key_types_valid": payment_key_types_valid
}

payment_validation_passed = (
    bronze_payment_count == silver_payment_count
    and payment_key_metrics["distinct_payment_keys"]
        == silver_payment_count
    and duplicate_payment_keys == 0
    and payment_key_metrics["invalid_order_id_rows"] == 0
    and payment_key_metrics["null_payment_sequential_rows"] == 0
    and payment_key_metrics[
        "nonpositive_payment_sequential_rows"
    ] == 0
    and orphan_payment_rows == 0
    and payment_key_types_valid
)

payment_validation["status"] = (
    "PASS" if payment_validation_passed else "FAIL"
)

display(
    spark.createDataFrame([payment_validation])
)

if not payment_validation_passed:
    raise ValueError(
        "Silver Payments key and relationship validation failed."
    )

print("All Payment key and relationship validations passed.")

# COMMAND ----------

# DBTITLE 1,27A - Audit Payment Quality and Order Amounts
# Validate monetary conversion safety
converted_payment_value = F.expr(
    "try_cast(`payment_value` AS DECIMAL(18,2))"
)

payment_financial_metrics = bronze_payments.agg(

    F.sum(
        F.when(
            F.col("payment_value").isNotNull()
            & converted_payment_value.isNull(),
            1
        ).otherwise(0)
    ).alias("payment_value_conversion_failures"),

    F.sum(
        F.when(
            F.col("payment_value").isNotNull()
            & converted_payment_value.isNotNull()
            & (
                F.abs(
                    F.col("payment_value").cast("double")
                    - converted_payment_value.cast("double")
                ) > 0.000001
            ),
            1
        ).otherwise(0)
    ).alias("payment_value_precision_changes")

).first().asDict()

print("Payment financial conversion assessment:")

display(
    spark.createDataFrame([payment_financial_metrics])
)

# Validate required financial type
payment_financial_type_valid = (
    silver_payments.schema["payment_value"]
        .dataType.simpleString() == "decimal(18,2)"
)

payment_financial_passed = (
    payment_financial_metrics[
        "payment_value_conversion_failures"
    ] == 0
    and payment_financial_metrics[
        "payment_value_precision_changes"
    ] == 0
    and payment_financial_type_valid
)

print(
    "Payment financial precision: "
    + ("PASS" if payment_financial_passed else "FAIL")
)

if not payment_financial_passed:
    raise ValueError(
        "Payment monetary conversion validation failed."
    )

# COMMAND ----------

# DBTITLE 1,27B - Audit Payment Quality and Order Amounts
# Profile payment quality
payment_quality_metrics = silver_payments.agg(

    F.sum(
        F.when(
            F.col("payment_value").isNull(), 1
        ).otherwise(0)
    ).alias("missing_payment_value_rows"),

    F.sum(
        F.when(
            F.col("payment_value") < 0, 1
        ).otherwise(0)
    ).alias("negative_payment_value_rows"),

    F.sum(
        F.when(
            F.col("payment_value") == 0, 1
        ).otherwise(0)
    ).alias("zero_payment_value_rows"),

    F.sum(
        F.when(
            F.col("payment_installments").isNull(), 1
        ).otherwise(0)
    ).alias("missing_installments_rows"),

    F.sum(
        F.when(
            F.col("payment_installments") == 0, 1
        ).otherwise(0)
    ).alias("zero_installments_rows"),

    F.sum(
        F.when(
            F.col("payment_installments") < 0, 1
        ).otherwise(0)
    ).alias("negative_installments_rows"),

    F.sum(
        F.when(
            F.col("payment_type").isNull()
            | (F.col("payment_type") == ""),
            1
        ).otherwise(0)
    ).alias("missing_payment_type_rows")

).first().asDict()

print("Payment quality profile:")

display(
    spark.createDataFrame([payment_quality_metrics])
)

# Inspect payment method distribution
print("Payment method distribution:")

display(
    silver_payments
    .groupBy("payment_type")
    .agg(
        F.count("*").alias("payment_records"),
        F.sum("payment_value").alias("total_payment_value")
    )
    .orderBy(F.desc("payment_records"))
)

# Inspect number of payments per order
payments_per_order = (
    silver_payments
    .groupBy("order_id")
    .agg(
        F.count("*").alias("payment_record_count")
    )
)

print("Payments per order distribution:")

display(
    payments_per_order
    .groupBy("payment_record_count")
    .count()
    .orderBy("payment_record_count")
)

# COMMAND ----------

# DBTITLE 1,27C- Audit Payment Quality and Order Amounts C
# Aggregate payments to order level
payment_totals = (
    silver_payments
    .groupBy("order_id")
    .agg(
        F.sum("payment_value").alias(
            "total_payment_value"
        ),
        F.count("*").alias(
            "payment_record_count"
        )
    )
)

# Aggregate order items to order level
order_item_totals = (
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.sl_order_items"
    )
    .groupBy("order_id")
    .agg(
        F.sum(
            F.col("price") + F.col("freight_value")
        ).alias("total_items_and_freight"),

        F.count("*").alias("item_record_count")
    )
)

# Compare independently aggregated totals
order_amount_comparison = (
    payment_totals
    .join(
        order_item_totals,
        on="order_id",
        how="full_outer"
    )
    .withColumn(
        "amount_difference",
        F.col("total_payment_value")
        - F.col("total_items_and_freight")
    )
)

# Build reconciliation summary
amount_reconciliation = order_amount_comparison.agg(

    F.count("*").alias("orders_compared"),

    F.sum(
        F.when(
            F.col("payment_record_count").isNotNull()
            & F.col("item_record_count").isNull(),
            1
        ).otherwise(0)
    ).alias("payment_orders_without_items"),

    F.sum(
        F.when(
            F.col("item_record_count").isNotNull()
            & F.col("payment_record_count").isNull(),
            1
        ).otherwise(0)
    ).alias("item_orders_without_payments"),

    F.sum(
        F.when(
            F.col("total_payment_value").isNotNull()
            & F.col("total_items_and_freight").isNotNull()
            & (F.abs(F.col("amount_difference")) > 0.01),
            1
        ).otherwise(0)
    ).alias("orders_with_amount_difference"),

    F.max(
        F.abs(F.col("amount_difference"))
    ).alias("maximum_absolute_difference")

).first().asDict()

print("Order amount reconciliation:")

display(
    spark.createDataFrame([amount_reconciliation])
)

# Inspect examples of mismatched amounts
print("Largest payment-to-item differences:")

display(
    order_amount_comparison
    .filter(
        F.col("amount_difference").isNotNull()
        & (F.abs(F.col("amount_difference")) > 0.01)
    )
    .orderBy(
        F.desc(F.abs(F.col("amount_difference")))
    )
    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,27D - Investigate Payment Reconciliation Exceptions
# Load Silver Orders for status analysis
orders_status = (
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.sl_orders"
    )
    .select(
        "order_id",
        "order_status"
    )
)

# Classify financial reconciliation results
reconciliation_detail = (
    order_amount_comparison
    .join(
        orders_status,
        on="order_id",
        how="left"
    )
    .withColumn(
        "reconciliation_category",

        F.when(
            F.col("payment_record_count").isNull(),
            "ITEMS_WITHOUT_PAYMENT"
        )

        .when(
            F.col("item_record_count").isNull(),
            "PAYMENT_WITHOUT_ITEMS"
        )

        .when(
            F.abs(F.col("amount_difference")) > 0.01,
            "AMOUNT_MISMATCH"
        )

        .otherwise("MATCHED")
    )
)

# Analyze exceptions by order status
print("Reconciliation categories by order status:")

display(
    reconciliation_detail
    .groupBy(
        "reconciliation_category",
        "order_status"
    )
    .count()
    .orderBy(
        "reconciliation_category",
        F.desc("count")
    )
)

# Inspect zero-value and zero-installment payments
print("Payments requiring additional review:")

display(
    silver_payments
    .filter(
        (F.col("payment_value") == 0)
        | (F.col("payment_installments") == 0)
    )
    .select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    )
    .orderBy(
        "payment_type",
        "order_id"
    )
)

# COMMAND ----------

# DBTITLE 1,28 - Write Silver Payments Delta Table
# Check validation gates before writing
if not payment_validation_passed:
    raise ValueError(
        "Payment key and relationship validation failed."
    )

if not payment_financial_passed:
    raise ValueError(
        "Payment financial validation failed."
    )

target_table = TARGET_TABLES["payments"]

# Write Silver Payments using full-load strategy
(
    silver_payments.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Successfully written: {target_table}")

# COMMAND ----------

# DBTITLE 1,29 - Validate Persisted Silver Payments
# Read persisted Silver table
persisted_payments = spark.table(
    TARGET_TABLES["payments"]
)

# Validate persisted row count
persisted_payment_count = persisted_payments.count()

# Validate composite key uniqueness
persisted_duplicate_payment_keys = (
    persisted_payments
    .groupBy("order_id", "payment_sequential")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Align columns for full-data comparison
expected_payments = silver_payments.select(
    *silver_payments.columns
)

actual_payments = persisted_payments.select(
    *silver_payments.columns
)

# Compare expected and persisted records
missing_from_target = (
    expected_payments
    .exceptAll(actual_payments)
    .count()
)

unexpected_in_target = (
    actual_payments
    .exceptAll(expected_payments)
    .count()
)

# Validate persisted schema
persisted_types_valid = (
    persisted_payments.schema["payment_sequential"]
        .dataType.simpleString() == "int"
    and
    persisted_payments.schema["payment_installments"]
        .dataType.simpleString() == "int"
    and
    persisted_payments.schema["payment_value"]
        .dataType.simpleString() == "decimal(18,2)"
)

# Define final validation
persisted_payment_validation_passed = (
    payment_validation_passed
    and payment_financial_passed
    and persisted_payment_count == bronze_payment_count
    and persisted_duplicate_payment_keys == 0
    and missing_from_target == 0
    and unexpected_in_target == 0
    and persisted_types_valid
)

validation_result = {
    "source": "payments",
    "target_table": TARGET_TABLES["payments"],
    "bronze_count": bronze_payment_count,
    "persisted_count": persisted_payment_count,
    "duplicate_key_groups": persisted_duplicate_payment_keys,
    "missing_from_target": missing_from_target,
    "unexpected_in_target": unexpected_in_target,
    "persisted_types_valid": persisted_types_valid,
    "status": (
        "PASS"
        if persisted_payment_validation_passed
        else "FAIL"
    )
}

display(
    spark.createDataFrame([validation_result])
)

if not persisted_payment_validation_passed:
    raise ValueError(
        "Persisted Silver Payments validation failed."
    )

print("Persisted Silver Payments validation: PASS")

# COMMAND ----------

# DBTITLE 1,30 - Transform Products
from pyspark.sql import functions as F

# Register source and target tables
SOURCE_TABLES["products"] = (
    f"{CATALOG}.{BRONZE_SCHEMA}.br_products"
)

TARGET_TABLES["products"] = (
    f"{CATALOG}.{SILVER_SCHEMA}.sl_products"
)

# Define source-to-target column contracts
PRODUCT_INTEGER_COLUMNS = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
    "product_photos_qty": "product_photos_qty"
}

PRODUCT_DOUBLE_COLUMNS = [
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]

PRODUCT_SOURCE_COLUMNS = [
    "product_id",
    "product_category_name",
    *PRODUCT_INTEGER_COLUMNS.keys(),
    *PRODUCT_DOUBLE_COLUMNS
]

# Read Bronze source
bronze_products = spark.table(
    SOURCE_TABLES["products"]
)

# Validate source column contract
missing_columns = sorted(
    set(PRODUCT_SOURCE_COLUMNS + METADATA_COLUMNS)
    - set(bronze_products.columns)
)

if missing_columns:
    raise ValueError(
        f"Missing product source columns: {missing_columns}"
    )

# Normalize category without replacing missing values
category_text = F.trim(
    F.col("product_category_name").cast("string")
)

normalized_category = (
    F.when(
        category_text.isNull() | (category_text == ""),
        F.lit(None).cast("string")
    )
    .otherwise(F.lower(category_text))
)

# Build Silver Products
silver_products = bronze_products.select(

    F.col("product_id")
        .cast("string")
        .alias("product_id"),

    normalized_category.alias("product_category_name"),

    *[
        F.col(source_column)
            .cast("int")
            .alias(target_column)
        for source_column, target_column
        in PRODUCT_INTEGER_COLUMNS.items()
    ],

    *[
        F.col(column_name)
            .cast("double")
            .alias(column_name)
        for column_name in PRODUCT_DOUBLE_COLUMNS
    ],

    *[
        F.col(column_name)
        for column_name in METADATA_COLUMNS
    ]
)

print("Silver Products transformation configured.")

silver_products.printSchema()

display(silver_products.limit(10))

# COMMAND ----------

# DBTITLE 1,31 - Validate Product Keys and Relationships
# Reconcile row counts
bronze_product_count = bronze_products.count()
silver_product_count = silver_products.count()

# Validate primary key
product_key_metrics = silver_products.agg(

    F.countDistinct("product_id").alias(
        "distinct_product_ids"
    ),

    F.sum(
        F.when(
            F.col("product_id").isNull()
            | (F.trim("product_id") == ""),
            1
        ).otherwise(0)
    ).alias("invalid_product_id_rows")

).first().asDict()

# Check duplicate primary keys
duplicate_product_keys = (
    silver_products
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Load Silver Order Items
order_item_products = (
    spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.sl_order_items"
    )
    .select("product_id")
)

# Check whether any order item references a missing product
orphan_product_items = (
    order_item_products
    .join(
        silver_products.select("product_id"),
        on="product_id",
        how="left_anti"
    )
    .count()
)

# Define expected numeric types
expected_numeric_types = {
    **{
        target_column: "int"
        for target_column
        in PRODUCT_INTEGER_COLUMNS.values()
    },
    **{
        column_name: "double"
        for column_name in PRODUCT_DOUBLE_COLUMNS
    }
}

# Validate Silver schema
invalid_numeric_types = [
    column_name
    for column_name, expected_type
    in expected_numeric_types.items()
    if (
        silver_products.schema[column_name]
        .dataType.simpleString() != expected_type
    )
]

# Build validation result
product_validation = {
    "bronze_count": bronze_product_count,
    "silver_count": silver_product_count,
    "distinct_product_ids": product_key_metrics[
        "distinct_product_ids"
    ],
    "duplicate_key_groups": duplicate_product_keys,
    "invalid_product_id_rows": product_key_metrics[
        "invalid_product_id_rows"
    ],
    "orphan_product_items": orphan_product_items,
    "invalid_numeric_types": len(invalid_numeric_types)
}

product_validation_passed = (
    bronze_product_count == silver_product_count
    and product_key_metrics["distinct_product_ids"]
        == silver_product_count
    and duplicate_product_keys == 0
    and product_key_metrics["invalid_product_id_rows"] == 0
    and orphan_product_items == 0
    and len(invalid_numeric_types) == 0
)

product_validation["status"] = (
    "PASS" if product_validation_passed else "FAIL"
)

display(
    spark.createDataFrame([product_validation])
)

if not product_validation_passed:
    raise ValueError(
        "Silver Products structural validation failed."
    )

print("All Product key and relationship validations passed.")

# COMMAND ----------

# DBTITLE 1,32A - Audit Product Attributes and Category Coverage
# Define numeric conversion contracts
NUMERIC_CONTRACTS = {
    **{
        column_name: "int"
        for column_name in PRODUCT_INTEGER_COLUMNS
    },
    **{
        column_name: "double"
        for column_name in PRODUCT_DOUBLE_COLUMNS
    }
}

conversion_results = []

for column_name, target_type in NUMERIC_CONTRACTS.items():

    original = F.col(column_name)

    converted = F.expr(
        f"try_cast(`{column_name}` AS {target_type})"
    )

    metrics = bronze_products.agg(

        F.sum(
            F.when(
                original.isNotNull()
                & converted.isNull(),
                1
            ).otherwise(0)
        ).alias("conversion_failures"),

        F.sum(
            F.when(
                original.isNotNull()
                & converted.isNotNull()
                & (
                    F.abs(
                        original.cast("double")
                        - converted.cast("double")
                    ) > 0.000001
                ),
                1
            ).otherwise(0)
        ).alias("value_changes")

    ).first().asDict()

    conversion_results.append({
        "column_name": column_name,
        "target_type": target_type,
        **metrics
    })

display(
    spark.createDataFrame(conversion_results)
)

product_conversion_passed = all(
    row["conversion_failures"] == 0
    and row["value_changes"] == 0
    for row in conversion_results
)

print(
    "Product numeric conversion validation: "
    + ("PASS" if product_conversion_passed else "FAIL")
)

if not product_conversion_passed:
    raise ValueError(
        "Product numeric conversion validation failed."
    )

# COMMAND ----------

# DBTITLE 1,32B - Audit Product Attributes and Category Coverage
# Define Silver numeric columns
PRODUCT_NUMERIC_COLUMNS = [
    *PRODUCT_INTEGER_COLUMNS.values(),
    *PRODUCT_DOUBLE_COLUMNS
]

attribute_quality_results = []

for column_name in PRODUCT_NUMERIC_COLUMNS:

    metrics = silver_products.agg(

        F.sum(
            F.when(
                F.col(column_name).isNull(),
                1
            ).otherwise(0)
        ).alias("null_rows"),

        F.sum(
            F.when(
                F.col(column_name) < 0,
                1
            ).otherwise(0)
        ).alias("negative_rows"),

        F.sum(
            F.when(
                F.col(column_name) == 0,
                1
            ).otherwise(0)
        ).alias("zero_rows")

    ).first().asDict()

    attribute_quality_results.append({
        "column_name": column_name,
        **metrics
    })

print("Product attribute quality profile:")

display(
    spark.createDataFrame(attribute_quality_results)
)

# COMMAND ----------

# DBTITLE 1,32C - Audit Product Attributes and Category Coverage
# Count products without a valid category
missing_category_rows = (
    silver_products
    .filter(
        F.col("product_category_name").isNull()
    )
    .count()
)

# Load and normalize reference categories
translation_reference = (
    spark.table(
        f"{CATALOG}.{BRONZE_SCHEMA}.br_product_category_translation"
    )
    .select(
        F.lower(
            F.trim(
                F.col("product_category_name")
            )
        ).alias("product_category_name")
    )
    .filter(
        F.col("product_category_name").isNotNull()
        & (F.col("product_category_name") != "")
    )
    .distinct()
)

# Identify products without translation mapping
unmatched_product_categories = (
    silver_products
    .filter(
        F.col("product_category_name").isNotNull()
    )
    .join(
        translation_reference,
        on="product_category_name",
        how="left_anti"
    )
)

unmatched_category_rows = (
    unmatched_product_categories.count()
)

print("Product category coverage:")

category_metrics = {
    "total_products": silver_product_count,
    "missing_category_rows": missing_category_rows,
    "unmatched_translation_rows": unmatched_category_rows
}

display(
    spark.createDataFrame([category_metrics])
)

print("Categories without translation mapping:")

display(
    unmatched_product_categories
    .groupBy("product_category_name")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# DBTITLE 1,33 - Write Silver Products Delta Table
# Validate structural and numeric gates
if not product_validation_passed:
    raise ValueError(
        "Product structural validation failed."
    )

if not product_conversion_passed:
    raise ValueError(
        "Product numeric conversion validation failed."
    )

target_table = TARGET_TABLES["products"]

# Write Silver Products using full-load strategy
(
    silver_products.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Successfully written: {target_table}")

# COMMAND ----------

# DBTITLE 1,34 - Validate Persisted Silver Products
# Read persisted Silver table
persisted_products = spark.table(
    TARGET_TABLES["products"]
)

# Validate persisted row count and primary key
persisted_product_metrics = persisted_products.agg(

    F.count("*").alias("persisted_count"),

    F.countDistinct("product_id").alias(
        "distinct_product_ids"
    ),

    F.sum(
        F.when(
            F.col("product_id").isNull()
            | (F.trim("product_id") == ""),
            1
        ).otherwise(0)
    ).alias("invalid_product_ids"),

    F.sum(
        F.when(
            F.col("product_category_name").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_category_rows")

).first().asDict()

# Validate persisted schema
persisted_schema_valid = (
    persisted_products.schema["product_id"]
        .dataType.simpleString() == "string"
    and
    persisted_products.schema["product_category_name"]
        .dataType.simpleString() == "string"
    and
    all(
        persisted_products.schema[column_name]
            .dataType.simpleString() == expected_type
        for column_name, expected_type
        in expected_numeric_types.items()
    )
)

# Align columns before comparison
expected_products = silver_products.select(
    *silver_products.columns
)

actual_products = persisted_products.select(
    *silver_products.columns
)

# Compare expected and persisted datasets
missing_from_target = (
    expected_products
    .exceptAll(actual_products)
    .count()
)

unexpected_in_target = (
    actual_products
    .exceptAll(expected_products)
    .count()
)

# Revalidate referential integrity after persistence
persisted_orphan_product_items = (
    order_item_products
    .join(
        persisted_products.select("product_id"),
        on="product_id",
        how="left_anti"
    )
    .count()
)

# Define persisted validation result
persisted_product_validation_passed = (
    product_validation_passed
    and product_conversion_passed
    and persisted_product_metrics["persisted_count"]
        == bronze_product_count
    and persisted_product_metrics["distinct_product_ids"]
        == bronze_product_count
    and persisted_product_metrics["invalid_product_ids"] == 0
    and persisted_product_metrics["missing_category_rows"]
        == missing_category_rows
    and persisted_orphan_product_items == 0
    and missing_from_target == 0
    and unexpected_in_target == 0
    and persisted_schema_valid
)

validation_result = {
    "source": "products",
    "target_table": TARGET_TABLES["products"],
    "bronze_count": bronze_product_count,
    "persisted_count": persisted_product_metrics[
        "persisted_count"
    ],
    "distinct_product_ids": persisted_product_metrics[
        "distinct_product_ids"
    ],
    "missing_category_rows": persisted_product_metrics[
        "missing_category_rows"
    ],
    "orphan_product_items": persisted_orphan_product_items,
    "missing_from_target": missing_from_target,
    "unexpected_in_target": unexpected_in_target,
    "schema_valid": persisted_schema_valid,
    "status": (
        "PASS"
        if persisted_product_validation_passed
        else "FAIL"
    )
}

display(
    spark.createDataFrame([validation_result])
)

if not persisted_product_validation_passed:
    raise ValueError(
        "Persisted Silver Products validation failed."
    )

print("Persisted Silver Products validation: PASS")

# COMMAND ----------

# DBTITLE 1,35 - Transform Product Category Translation
# Register source and target tables
SOURCE_TABLES["product_category_translation"] = (
    f"{CATALOG}.{BRONZE_SCHEMA}.br_product_category_translation"
)

TARGET_TABLES["product_category_translation"] = (
    f"{CATALOG}.{SILVER_SCHEMA}.sl_product_category_translation"
)

# Define expected columns
TRANSLATION_COLUMNS = [
    "product_category_name",
    "product_category_name_english"
]

# Read Bronze source
bronze_translation = spark.table(
    SOURCE_TABLES["product_category_translation"]
)

# Validate source contract
expected_columns = set(
    TRANSLATION_COLUMNS + METADATA_COLUMNS
)

missing_columns = sorted(
    expected_columns - set(bronze_translation.columns)
)

if missing_columns:
    raise ValueError(
        f"Missing translation columns: {missing_columns}"
    )

# Define reusable category normalization
def normalize_category_value(column):
    normalized = F.lower(
        F.trim(column.cast("string"))
    )

    return (
        F.when(
            normalized == "",
            F.lit(None).cast("string")
        )
        .otherwise(normalized)
    )


# Apply Silver transformations
silver_translation = bronze_translation.select(

    normalize_category_value(
        F.col("product_category_name")
    ).alias("product_category_name"),

    normalize_category_value(
        F.col("product_category_name_english")
    ).alias("product_category_name_english"),

    *[
        F.col(column_name)
        for column_name in METADATA_COLUMNS
    ]
)

print("Silver Category Translation configured.")

silver_translation.printSchema()

display(silver_translation.limit(10))

# COMMAND ----------

# DBTITLE 1,36 - Validate Category Translation Keys
# Reconcile source and target counts
bronze_translation_count = bronze_translation.count()
silver_translation_count = silver_translation.count()

# Validate category keys and values
translation_metrics = silver_translation.agg(

    F.countDistinct(
        "product_category_name"
    ).alias("distinct_category_keys"),

    F.sum(
        F.when(
            F.col("product_category_name").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_category_keys"),

    F.sum(
        F.when(
            F.col("product_category_name_english").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_english_translations")

).first().asDict()

# Identify duplicate keys after normalization
duplicate_category_keys = (
    silver_translation
    .groupBy("product_category_name")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Validate output types
translation_types_valid = all(
    silver_translation.schema[column_name]
        .dataType.simpleString() == "string"
    for column_name in TRANSLATION_COLUMNS
)

# Build validation results
translation_validation = {
    "bronze_count": bronze_translation_count,
    "silver_count": silver_translation_count,
    "distinct_category_keys": translation_metrics[
        "distinct_category_keys"
    ],
    "duplicate_key_groups": duplicate_category_keys,
    "invalid_category_keys": translation_metrics[
        "invalid_category_keys"
    ],
    "missing_english_translations": translation_metrics[
        "missing_english_translations"
    ],
    "translation_types_valid": translation_types_valid
}

translation_validation_passed = (
    bronze_translation_count == silver_translation_count
    and silver_translation_count
        == translation_metrics["distinct_category_keys"]
    and duplicate_category_keys == 0
    and translation_metrics["invalid_category_keys"] == 0
    and translation_metrics[
        "missing_english_translations"
    ] == 0
    and translation_types_valid
)

translation_validation["status"] = (
    "PASS" if translation_validation_passed else "FAIL"
)

display(
    spark.createDataFrame([translation_validation])
)

if not translation_validation_passed:
    raise ValueError(
        "Category Translation validation failed."
    )

print("All Category Translation key validations passed.")

# COMMAND ----------

# DBTITLE 1,37 - Validate Product Category Mapping Coverage
# Load Silver Products
products_reference = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.sl_products"
)

# Identify products with missing categories
products_without_category = (
    products_reference
    .filter(
        F.col("product_category_name").isNull()
    )
    .count()
)

# Select products with known categories
valid_product_categories = (
    products_reference
    .filter(
        F.col("product_category_name").isNotNull()
    )
    .select(
        "product_id",
        "product_category_name"
    )
)

# Prepare Silver translation keys
silver_translation_keys = (
    silver_translation
    .select("product_category_name")
    .distinct()
)

# Prepare normalized Bronze translation keys
bronze_translation_keys = (
    bronze_translation
    .select(
        normalize_category_value(
            F.col("product_category_name")
        ).alias("product_category_name")
    )
    .distinct()
)

# Find products without Silver translation
unmatched_silver_products = (
    valid_product_categories
    .join(
        silver_translation_keys,
        on="product_category_name",
        how="left_anti"
    )
)

# Find products without Bronze translation
unmatched_bronze_products = (
    valid_product_categories
    .join(
        bronze_translation_keys,
        on="product_category_name",
        how="left_anti"
    )
)

# Count unmatched product records
unmatched_silver_count = unmatched_silver_products.count()
unmatched_bronze_count = unmatched_bronze_products.count()

# Count distinct categories without translation
unmatched_category_count = (
    unmatched_silver_products
    .select("product_category_name")
    .distinct()
    .count()
)

# Check unused translation categories
unused_translation_categories = (
    silver_translation_keys
    .join(
        valid_product_categories
            .select("product_category_name")
            .distinct(),
        on="product_category_name",
        how="left_anti"
    )
    .count()
)

# Build coverage assessment
category_coverage = {
    "total_products": products_reference.count(),
    "products_without_category": products_without_category,
    "unmatched_bronze_products": unmatched_bronze_count,
    "unmatched_silver_products": unmatched_silver_count,
    "unmatched_category_count": unmatched_category_count,
    "unused_translation_categories": unused_translation_categories
}

display(
    spark.createDataFrame([category_coverage])
)

print("Categories without English translation:")

display(
    unmatched_silver_products
    .groupBy("product_category_name")
    .count()
    .orderBy(F.desc("count"))
)

# Validate that Silver preserved source coverage
category_coverage_passed = (
    unmatched_silver_count == unmatched_bronze_count
)

print(
    "Category mapping coverage validation: "
    + ("PASS" if category_coverage_passed else "FAIL")
)

if not category_coverage_passed:
    raise ValueError(
        "Category mapping coverage changed unexpectedly."
    )

# COMMAND ----------

# DBTITLE 1,38 - Write Silver Product Category Translation
# Validate required gates before writing
if not translation_validation_passed:
    raise ValueError(
        "Category Translation structural validation failed."
    )

if not category_coverage_passed:
    raise ValueError(
        "Category Translation coverage validation failed."
    )

target_table = TARGET_TABLES[
    "product_category_translation"
]

# Write Silver reference table
(
    silver_translation.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Successfully written: {target_table}")

# COMMAND ----------

# DBTITLE 1,39 - Validate Persisted Product Category Translation
# Read persisted Silver translation
persisted_translation = spark.table(
    TARGET_TABLES["product_category_translation"]
)

# Validate persisted rows and keys
persisted_translation_metrics = (
    persisted_translation
    .agg(
        F.count("*").alias("persisted_count"),

        F.countDistinct(
            "product_category_name"
        ).alias("distinct_category_keys"),

        F.sum(
            F.when(
                F.col("product_category_name").isNull(),
                1
            ).otherwise(0)
        ).alias("invalid_category_keys")
    )
    .first()
    .asDict()
)

# Compare expected and persisted records
expected_translation = silver_translation.select(
    *silver_translation.columns
)

actual_translation = persisted_translation.select(
    *silver_translation.columns
)

missing_from_target = (
    expected_translation
    .exceptAll(actual_translation)
    .count()
)

unexpected_in_target = (
    actual_translation
    .exceptAll(expected_translation)
    .count()
)

# Validate category coverage after persistence
persisted_translation_keys = (
    persisted_translation
    .select("product_category_name")
    .distinct()
)

persisted_unmatched_products = (
    valid_product_categories
    .join(
        persisted_translation_keys,
        on="product_category_name",
        how="left_anti"
    )
    .count()
)

# Validate final result
persisted_translation_passed = (
    translation_validation_passed
    and category_coverage_passed
    and persisted_translation_metrics["persisted_count"]
        == bronze_translation_count
    and persisted_translation_metrics["distinct_category_keys"]
        == bronze_translation_count
    and persisted_translation_metrics["invalid_category_keys"] == 0
    and persisted_unmatched_products == unmatched_bronze_count
    and missing_from_target == 0
    and unexpected_in_target == 0
)

validation_result = {
    "source": "product_category_translation",
    "bronze_count": bronze_translation_count,
    "persisted_count": persisted_translation_metrics[
        "persisted_count"
    ],
    "distinct_category_keys": persisted_translation_metrics[
        "distinct_category_keys"
    ],
    "unmatched_products": persisted_unmatched_products,
    "missing_from_target": missing_from_target,
    "unexpected_in_target": unexpected_in_target,
    "status": (
        "PASS"
        if persisted_translation_passed else "FAIL"
    )
}

display(
    spark.createDataFrame([validation_result])
)

if not persisted_translation_passed:
    raise ValueError(
        "Persisted Category Translation validation failed."
    )

print("Persisted Category Translation validation: PASS")