# Databricks notebook source
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