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