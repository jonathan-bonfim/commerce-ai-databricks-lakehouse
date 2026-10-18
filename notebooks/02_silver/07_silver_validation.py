# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver Validation
# MAGIC
# MAGIC **Purpose:** Run the integrated, read-only quality gate for all nine conformed Silver Delta tables.
# MAGIC
# MAGIC **Inputs:** `commerceai.bronze.br_*` and `commerceai.silver.sl_*`  
# MAGIC **Outputs:** PASS, WARN, and FAIL results; no Delta tables are modified.  
# MAGIC **Execution:** Run as a **separate task after** `06_silver_transformations`; do not embed these checks in the transformation notebook.  
# MAGIC **Coverage:** Inventory, schemas, primary/composite keys, relationships, ZIP/geolocation quality, known business-quality exceptions, review preservation, timestamp null preservation, and numeric conversion fidelity.
# MAGIC
# MAGIC Known Olist exceptions are documented as warnings. Critical violations fail the notebook. Row-count baselines refer to the current historical dataset snapshot; review the baselines if source files change. The previous notebook version had 83 checks; this revision adds numeric-fidelity checks that previously lived only in `06_silver_transformations`.
# MAGIC

# COMMAND ----------

# DBTITLE 1,01 - Define Silver Validation Contracts
from pyspark.sql import functions as F
from functools import reduce
from operator import or_

CATALOG = "commerceai"

EXPECTED_COUNTS = {
    "customers": 99441,
    "geolocation": 19015,
    "order_items": 112650,
    "payments": 103886,
    "reviews": 99224,
    "orders": 99441,
    "products": 32951,
    "sellers": 3095,
    "product_category_translation": 71
}

BRONZE_TABLES = {
    source: f"{CATALOG}.bronze.br_{source}"
    for source in EXPECTED_COUNTS
}

SILVER_TABLES = {
    source: f"{CATALOG}.silver.sl_{source}"
    for source in EXPECTED_COUNTS
}

PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "geolocation": ["geolocation_zip_code_prefix"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id", "payment_sequential"],
    "reviews": ["review_id", "order_id"],
    "orders": ["order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "product_category_translation": [
        "product_category_name"
    ]
}

TECHNICAL_METADATA = {
    "bronze_ingestion_timestamp": "timestamp",
    "source_file_name": "string",
    "source_file_path": "string"
}

SILVER_SCHEMA_CONTRACTS = {
    "customers": {
        "customer_id": "string",
        "customer_unique_id": "string",
        "customer_zip_code_prefix": "string",
        "customer_city": "string",
        "customer_state": "string"
    },
    "geolocation": {
        "geolocation_zip_code_prefix": "string",
        "geolocation_lat": "double",
        "geolocation_lng": "double",
        "geolocation_city": "string",
        "geolocation_state": "string",
        "geolocation_coordinate_status": "string",
        "geolocation_has_state_conflict": "boolean",
        "geolocation_has_city_variation": "boolean",
        "geolocation_source_record_count": "bigint",
        "geolocation_distinct_record_count": "bigint",
        "source_table_name": "string"
    },
    "order_items": {
        "order_id": "string",
        "order_item_id": "int",
        "product_id": "string",
        "seller_id": "string",
        "shipping_limit_date": "timestamp",
        "price": "decimal(18,2)",
        "freight_value": "decimal(18,2)"
    },
    "payments": {
        "order_id": "string",
        "payment_sequential": "int",
        "payment_type": "string",
        "payment_installments": "int",
        "payment_value": "decimal(18,2)"
    },
    "reviews": {
        "review_id": "string",
        "order_id": "string",
        "review_score": "int",
        "review_comment_title": "string",
        "review_comment_message": "string",
        "review_creation_date": "timestamp",
        "review_answer_timestamp": "timestamp"
    },
    "orders": {
        "order_id": "string",
        "customer_id": "string",
        "order_status": "string",
        "order_purchase_timestamp": "timestamp",
        "order_approved_at": "timestamp",
        "order_delivered_carrier_date": "timestamp",
        "order_delivered_customer_date": "timestamp",
        "order_estimated_delivery_date": "timestamp"
    },
    "products": {
        "product_id": "string",
        "product_category_name": "string",
        "product_name_length": "int",
        "product_description_length": "int",
        "product_photos_qty": "int",
        "product_weight_g": "double",
        "product_length_cm": "double",
        "product_height_cm": "double",
        "product_width_cm": "double"
    },
    "sellers": {
        "seller_id": "string",
        "seller_zip_code_prefix": "string",
        "seller_city": "string",
        "seller_state": "string"
    },
    "product_category_translation": {
        "product_category_name": "string",
        "product_category_name_english": "string"
    }
}

# Add source metadata to row-preserving entities
for source, contract in SILVER_SCHEMA_CONTRACTS.items():
    if source != "geolocation":
        contract.update(TECHNICAL_METADATA)

VALIDATION_RESULTS = []

def register_check(
    group,
    source,
    check_name,
    observed,
    expected,
    passed,
    severity="CRITICAL"
):
    status = (
        "PASS" if passed
        else "WARN" if severity == "WARNING"
        else "FAIL"
    )

    VALIDATION_RESULTS.append({
        "group": str(group),
        "source": str(source),
        "check_name": str(check_name),
        "observed": str(observed),
        "expected": str(expected),
        "severity": severity,
        "status": status
    })


def display_group_results(group):
    rows = [
        row for row in VALIDATION_RESULTS
        if row["group"] == group
    ]

    display(
        spark.createDataFrame(rows)
        .orderBy("source", "check_name")
    )


print("Silver validation contracts configured.")
print(f"Expected Silver tables: {len(SILVER_TABLES)}")

# COMMAND ----------

# DBTITLE 1,02 - Validate Silver Inventory and Schemas
bronze_dataframes = {}
silver_dataframes = {}

print("Validating Silver inventory and schemas...")

for source, expected_count in EXPECTED_COUNTS.items():

    bronze_df = spark.table(
        BRONZE_TABLES[source]
    )

    silver_df = spark.table(
        SILVER_TABLES[source]
    )

    bronze_dataframes[source] = bronze_df
    silver_dataframes[source] = silver_df

    bronze_count = bronze_df.count()
    silver_count = silver_df.count()

    # Validate expected Silver row count
    register_check(
        "INVENTORY",
        source,
        "expected_row_count",
        silver_count,
        expected_count,
        silver_count == expected_count
    )

    # Validate row preservation for non-aggregated entities
    if source != "geolocation":

        register_check(
            "INVENTORY",
            source,
            "bronze_to_silver_row_count",
            silver_count,
            bronze_count,
            silver_count == bronze_count
        )

    # Validate required columns and data types
    contract = SILVER_SCHEMA_CONTRACTS[source]

    missing_columns = []
    invalid_types = []

    for column_name, expected_type in contract.items():

        if column_name not in silver_df.columns:
            missing_columns.append(column_name)
            continue

        actual_type = (
            silver_df.schema[column_name]
            .dataType.simpleString()
        )

        if actual_type != expected_type:
            invalid_types.append(
                f"{column_name}: {actual_type}"
            )

    schema_passed = (
        not missing_columns
        and not invalid_types
    )

    schema_details = (
        f"Missing: {missing_columns}; "
        f"Invalid types: {invalid_types}"
    )

    register_check(
        "SCHEMA",
        source,
        "required_columns_and_types",
        "VALID" if schema_passed else schema_details,
        "VALID",
        schema_passed
    )

display_group_results("INVENTORY")
display_group_results("SCHEMA")

# COMMAND ----------

# DBTITLE 1,03 - Validate Primary and Composite Keys
print("Validating Silver primary and composite keys...")

for source, key_columns in PRIMARY_KEYS.items():

    df = silver_dataframes[source]

    # Identify null or blank key components
    invalid_conditions = []

    for column_name in key_columns:

        condition = F.col(column_name).isNull()

        column_type = (
            df.schema[column_name]
            .dataType.simpleString()
        )

        if column_type == "string":
            condition = (
                condition
                | (F.trim(F.col(column_name)) == "")
            )

        invalid_conditions.append(condition)

    invalid_key_condition = reduce(
        or_, invalid_conditions
    )

    invalid_key_rows = (
        df.filter(invalid_key_condition).count()
    )

    # Identify duplicated key combinations
    duplicate_key_groups = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    register_check(
        "KEYS",
        source,
        "invalid_key_rows",
        invalid_key_rows,
        0,
        invalid_key_rows == 0
    )

    register_check(
        "KEYS",
        source,
        "duplicate_key_groups",
        duplicate_key_groups,
        0,
        duplicate_key_groups == 0
    )

display_group_results("KEYS")

# COMMAND ----------

# DBTITLE 1,04 - Validate Referential Integrity
RELATIONSHIPS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("payments", "order_id", "orders", "order_id"),
    ("reviews", "order_id", "orders", "order_id")
]

print("Validating Silver referential integrity...")

for (
    source,
    source_key,
    target,
    target_key
) in RELATIONSHIPS:

    source_df = silver_dataframes[source]
    target_df = silver_dataframes[target]

    source_keys = source_df.select(
        F.col(source_key).alias("_relationship_key")
    )

    target_keys = (
        target_df.select(
            F.col(target_key).alias("_relationship_key")
        )
        .distinct()
    )

    orphan_rows = (
        source_keys
        .join(
            target_keys,
            on="_relationship_key",
            how="left_anti"
        )
        .count()
    )

    register_check(
        "RELATIONSHIPS",
        source,
        f"{source_key}_to_{target}",
        orphan_rows,
        0,
        orphan_rows == 0
    )

display_group_results("RELATIONSHIPS")

# COMMAND ----------

# DBTITLE 1,05 - Validate Geolocation and ZIP Quality
geo = silver_dataframes["geolocation"]
bronze_geo = bronze_dataframes["geolocation"]

ZIP_COLUMN = "geolocation_zip_code_prefix"

# Reconcile original observations with consolidated records
geo_metrics = geo.agg(

    F.sum("geolocation_source_record_count").alias(
        "reconciled_source_rows"
    ),

    F.sum(
        F.when(
            F.col("geolocation_coordinate_status") == "MISSING",
            1
        ).otherwise(0)
    ).alias("missing_coordinate_prefixes"),

    F.sum(
        F.when(
            F.col("geolocation_has_state_conflict"),
            1
        ).otherwise(0)
    ).alias("state_conflict_prefixes"),

    F.sum(
        F.when(
            F.col("geolocation_has_city_variation"),
            1
        ).otherwise(0)
    ).alias("city_variation_prefixes")

).first().asDict()

original_geo_count = bronze_geo.count()

register_check(
    "GEOLOCATION",
    "geolocation",
    "source_record_reconciliation",
    geo_metrics["reconciled_source_rows"],
    original_geo_count,
    geo_metrics["reconciled_source_rows"] == original_geo_count
)

# Validate ZIP format across all three entities
ZIP_FIELDS = {
    "customers": "customer_zip_code_prefix",
    "sellers": "seller_zip_code_prefix",
    "geolocation": "geolocation_zip_code_prefix"
}

for source, zip_column in ZIP_FIELDS.items():

    df = silver_dataframes[source]

    invalid_zip_rows = df.filter(
        F.col(zip_column).isNull()
        | ~F.col(zip_column).rlike(r"^[0-9]{5}$")
    ).count()

    register_check(
        "GEOLOCATION",
        source,
        "invalid_zip_format",
        invalid_zip_rows,
        0,
        invalid_zip_rows == 0
    )

# Validate coordinate pairs and coordinate status
latitude = F.col("geolocation_lat")
longitude = F.col("geolocation_lng")

valid_status = (
    (
        (F.col("geolocation_coordinate_status") == "AVAILABLE")
        & latitude.isNotNull()
        & longitude.isNotNull()
    )
    |
    (
        (F.col("geolocation_coordinate_status") == "MISSING")
        & latitude.isNull()
        & longitude.isNull()
    )
)

invalid_coordinate_status = geo.filter(
    ~F.coalesce(valid_status, F.lit(False))
).count()

invalid_geographic_coordinates = geo.filter(
    latitude.isNotNull()
    & longitude.isNotNull()
    & ~(
        latitude.between(-34.0, 6.0)
        & longitude.between(-74.0, -28.0)
    )
).count()

register_check(
    "GEOLOCATION",
    "geolocation",
    "invalid_coordinate_status",
    invalid_coordinate_status,
    0,
    invalid_coordinate_status == 0
)

register_check(
    "GEOLOCATION",
    "geolocation",
    "coordinates_outside_allowed_bounds",
    invalid_geographic_coordinates,
    0,
    invalid_geographic_coordinates == 0
)

# Record known data quality exceptions
for check_name, value in [
    (
        "missing_coordinate_prefixes",
        geo_metrics["missing_coordinate_prefixes"]
    ),
    (
        "state_conflict_prefixes",
        geo_metrics["state_conflict_prefixes"]
    ),
    (
        "city_variation_prefixes",
        geo_metrics["city_variation_prefixes"]
    )
]:
    register_check(
        "GEOLOCATION",
        "geolocation",
        check_name,
        value,
        "0 (review known exceptions)",
        value == 0,
        severity="WARNING"
    )

# Assess geolocation coverage for customers and sellers
geo_keys = geo.select(
    F.col(ZIP_COLUMN).alias("zip_prefix")
)

for source, zip_column in [
    ("customers", "customer_zip_code_prefix"),
    ("sellers", "seller_zip_code_prefix")
]:

    unmatched_rows = (
        silver_dataframes[source]
        .select(
            F.col(zip_column).alias("zip_prefix")
        )
        .join(
            geo_keys,
            on="zip_prefix",
            how="left_anti"
        )
        .count()
    )

    register_check(
        "GEOLOCATION",
        source,
        "rows_without_geolocation_match",
        unmatched_rows,
        "0 (coverage target)",
        unmatched_rows == 0,
        severity="WARNING"
    )

display_group_results("GEOLOCATION")

# COMMAND ----------

# DBTITLE 1,06 - Validate Business Data Quality
products = silver_dataframes["products"]
translations = silver_dataframes[
    "product_category_translation"
]
orders = silver_dataframes["orders"]
payments = silver_dataframes["payments"]
order_items = silver_dataframes["order_items"]

# Validate product category coverage
missing_categories = products.filter(
    F.col("product_category_name").isNull()
).count()

unmatched_categories = (
    products
    .filter(F.col("product_category_name").isNotNull())
    .select("product_id", "product_category_name")
    .join(
        translations.select("product_category_name"),
        on="product_category_name",
        how="left_anti"
    )
    .count()
)

register_check(
    "BUSINESS_QUALITY",
    "products",
    "products_without_category",
    missing_categories,
    "0 (known baseline: 610)",
    missing_categories == 0,
    severity="WARNING"
)

register_check(
    "BUSINESS_QUALITY",
    "products",
    "products_without_translation",
    unmatched_categories,
    "0 (known baseline: 13)",
    unmatched_categories == 0,
    severity="WARNING"
)

# Assess order temporal anomalies
order_quality = orders.agg(

    F.sum(
        F.when(
            (F.col("order_status") == "delivered")
            & F.col("order_delivered_customer_date").isNull(),
            1
        ).otherwise(0)
    ).alias("delivered_without_delivery_date"),

    F.sum(
        F.when(
            F.col("order_delivered_carrier_date")
            > F.col("order_delivered_customer_date"),
            1
        ).otherwise(0)
    ).alias("carrier_after_customer_delivery")

).first().asDict()

for check_name, value in order_quality.items():

    register_check(
        "BUSINESS_QUALITY",
        "orders",
        check_name,
        value,
        "0 (review temporal anomaly)",
        value == 0,
        severity="WARNING"
    )

# Assess payment anomalies
payment_quality = payments.agg(

    F.sum(
        F.when(
            F.col("payment_value") == 0,
            1
        ).otherwise(0)
    ).alias("zero_value_payments"),

    F.sum(
        F.when(
            F.col("payment_installments") == 0,
            1
        ).otherwise(0)
    ).alias("zero_installment_payments")

).first().asDict()

for check_name, value in payment_quality.items():

    register_check(
        "BUSINESS_QUALITY",
        "payments",
        check_name,
        value,
        "0 (review payment anomaly)",
        value == 0,
        severity="WARNING"
    )

# Aggregate payments and items independently
payment_totals = payments.groupBy("order_id").agg(
    F.sum("payment_value").alias("payment_total"),
    F.count("*").alias("payment_records")
)

item_totals = order_items.groupBy("order_id").agg(
    F.sum(
        F.col("price") + F.col("freight_value")
    ).alias("item_total"),
    F.count("*").alias("item_records")
)

amount_comparison = (
    payment_totals
    .join(item_totals, "order_id", "full_outer")
)

amount_metrics = amount_comparison.agg(

    F.sum(
        F.when(
            F.col("payment_records").isNotNull()
            & F.col("item_records").isNull(),
            1
        ).otherwise(0)
    ).alias("payments_without_items"),

    F.sum(
        F.when(
            F.col("item_records").isNotNull()
            & F.col("payment_records").isNull(),
            1
        ).otherwise(0)
    ).alias("items_without_payments"),

    F.sum(
        F.when(
            F.col("payment_total").isNotNull()
            & F.col("item_total").isNotNull()
            & (
                F.abs(
                    F.col("payment_total") - F.col("item_total")
                ) > 0.01
            ),
            1
        ).otherwise(0)
    ).alias("amount_mismatches")

).first().asDict()

for check_name, value in amount_metrics.items():

    register_check(
        "BUSINESS_QUALITY",
        "payments",
        check_name,
        value,
        "0 (investigate reconciliation)",
        value == 0,
        severity="WARNING"
    )

display_group_results("BUSINESS_QUALITY")

# COMMAND ----------

# DBTITLE 1,07 - Validate Review Integrity and Timestamp Preservation
bronze_reviews = bronze_dataframes["reviews"]
silver_reviews = silver_dataframes["reviews"]

REVIEW_TEXT_COLUMNS = [
    "review_id",
    "order_id",
    "review_comment_title",
    "review_comment_message"
]

# Compare complete review text records
bronze_text = bronze_reviews.select(
    *REVIEW_TEXT_COLUMNS
)

silver_text = silver_reviews.select(
    *REVIEW_TEXT_COLUMNS
)

missing_review_text = (
    bronze_text.exceptAll(silver_text).count()
)

unexpected_review_text = (
    silver_text.exceptAll(bronze_text).count()
)

register_check(
    "REVIEWS",
    "reviews",
    "missing_original_text_records",
    missing_review_text,
    0,
    missing_review_text == 0
)

register_check(
    "REVIEWS",
    "reviews",
    "unexpected_text_records",
    unexpected_review_text,
    0,
    unexpected_review_text == 0
)

# Validate score values
invalid_review_scores = silver_reviews.filter(
    F.col("review_score").isNull()
    | ~F.col("review_score").between(1, 5)
).count()

register_check(
    "REVIEWS",
    "reviews",
    "invalid_review_scores",
    invalid_review_scores,
    0,
    invalid_review_scores == 0
)

# Preserve multiple reviews per order
multiple_review_orders = (
    silver_reviews
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

register_check(
    "REVIEWS",
    "reviews",
    "orders_with_multiple_reviews",
    multiple_review_orders,
    547,
    multiple_review_orders == 547
)

# Regression test for known multiline comment
KNOWN_REVIEW_ID = (
    "5090899e38cd55729c9783745f2706f5"
)

KNOWN_ORDER_ID = (
    "1b76802b72eb5517c97481ee2f7b7037"
)

EXPECTED_PHRASE = (
    'No "mercado livre" isso não acontece!!'
)

known_review_matches = silver_reviews.filter(
    (F.col("review_id") == KNOWN_REVIEW_ID)
    & (F.col("order_id") == KNOWN_ORDER_ID)
    & (
        F.instr(
            F.col("review_comment_message"),
            EXPECTED_PHRASE
        ) > 0
    )
).count()

register_check(
    "REVIEWS",
    "reviews",
    "known_multiline_review",
    known_review_matches,
    1,
    known_review_matches == 1
)

# Validate non-null timestamp preservation
TIMESTAMP_CONTRACTS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],
    "order_items": [
        "shipping_limit_date"
    ],
    "reviews": [
        "review_creation_date",
        "review_answer_timestamp"
    ]
}

for source, timestamp_columns in TIMESTAMP_CONTRACTS.items():

    bronze_df = bronze_dataframes[source]
    silver_df = silver_dataframes[source]

    bronze_counts = bronze_df.agg(
        *[
            F.count(column_name).alias(column_name)
            for column_name in timestamp_columns
        ]
    ).first()

    silver_counts = silver_df.agg(
        *[
            F.count(column_name).alias(column_name)
            for column_name in timestamp_columns
        ]
    ).first()

    for column_name in timestamp_columns:

        source_non_null = bronze_counts[column_name]
        silver_non_null = silver_counts[column_name]

        register_check(
            "TIMESTAMPS",
            source,
            f"{column_name}_non_null_preservation",
            silver_non_null,
            source_non_null,
            silver_non_null == source_non_null
        )

display_group_results("REVIEWS")
display_group_results("TIMESTAMPS")

# COMMAND ----------

# DBTITLE 1,08 - Validate Numeric Transformation Fidelity
# Detect conversion changes without running these checks inside the Silver build.
# Each source is matched to its Silver table using its validated key.
NUMERIC_FIDELITY_CONTRACTS = {
    "order_items": {
        "price": "price",
        "freight_value": "freight_value"
    },
    "payments": {
        "payment_value": "payment_value"
    },
    "products": {
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length",
        "product_photos_qty": "product_photos_qty",
        "product_weight_g": "product_weight_g",
        "product_length_cm": "product_length_cm",
        "product_height_cm": "product_height_cm",
        "product_width_cm": "product_width_cm"
    }
}

for source, numeric_columns in NUMERIC_FIDELITY_CONTRACTS.items():
    source_df = bronze_dataframes[source].alias("bronze")
    silver_df = silver_dataframes[source].alias("silver")
    key_columns = PRIMARY_KEYS[source]

    key_condition = reduce(
        lambda left, right: left & right,
        [
            F.col(f"bronze.`{name}`") == F.col(f"silver.`{name}`")
            for name in key_columns
        ]
    )

    joined = source_df.join(silver_df, key_condition, "left")
    unmatched_condition = F.col(f"silver.`{key_columns[0]}`").isNull()

    metrics = [
        F.sum(F.when(unmatched_condition, 1).otherwise(0)).alias("unmatched_source_rows")
    ]

    for source_column, target_column in numeric_columns.items():
        original_value = F.col(f"bronze.`{source_column}`")
        silver_value = F.col(f"silver.`{target_column}`")

        null_mismatch = original_value.isNull() != silver_value.isNull()
        value_mismatch = (
            original_value.isNotNull()
            & silver_value.isNotNull()
            & (
                F.abs(
                    original_value.cast("double") - silver_value.cast("double")
                ) > 0.000001
            )
        )

        metrics.append(
            F.sum(
                F.when(null_mismatch | value_mismatch, 1).otherwise(0)
            ).alias(f"changed_{target_column}")
        )

    observed = joined.agg(*metrics).first().asDict()

    register_check(
        "FIDELITY", source, "unmatched_source_rows",
        observed["unmatched_source_rows"], 0,
        observed["unmatched_source_rows"] == 0
    )

    for target_column in numeric_columns.values():
        changed_count = observed[f"changed_{target_column}"]
        register_check(
            "FIDELITY", source, f"{target_column}_value_changes",
            changed_count, 0, changed_count == 0
        )

display_group_results("FIDELITY")


# COMMAND ----------

# DBTITLE 1,09 - Execute Final Silver Validation Gate
# Consolidate validation results
validation_df = spark.createDataFrame(
    VALIDATION_RESULTS
)

# Summarize validation statuses
validation_summary = (
    validation_df
    .groupBy("status")
    .count()
    .orderBy("status")
)

print("=== SILVER VALIDATION SUMMARY ===")

display(validation_summary)

# Count results by status
passed_checks = sum(
    row["status"] == "PASS"
    for row in VALIDATION_RESULTS
)

warning_checks = sum(
    row["status"] == "WARN"
    for row in VALIDATION_RESULTS
)

failed_checks = sum(
    row["status"] == "FAIL"
    for row in VALIDATION_RESULTS
)

total_checks = len(VALIDATION_RESULTS)

print(f"Total checks: {total_checks}")
print(f"Passed checks: {passed_checks}")
print(f"Warning checks: {warning_checks}")
print(f"Failed checks: {failed_checks}")

# Display failures and warnings
print("=== CHECKS REQUIRING ATTENTION ===")

attention_df = validation_df.filter(
    F.col("status").isin("FAIL", "WARN")
)

display(
    attention_df.orderBy(
        "status",
        "group",
        "source",
        "check_name"
    )
)

# Define final Silver validation status
if failed_checks > 0:
    final_status = "FAIL"

elif warning_checks > 0:
    final_status = "PASS WITH WARNINGS"

else:
    final_status = "PASS"

print("===================================")
print("COMMERCEAI SILVER VALIDATION")
print(f"Tables validated: {len(SILVER_TABLES)}")
print(f"Validation checks: {total_checks}")
print(f"Final status: {final_status}")
print("===================================")

if failed_checks > 0:
    raise ValueError(
        "Silver validation gate failed. "
        "Review critical validation errors."
    )

print("Silver layer is ready for Gold modeling.")