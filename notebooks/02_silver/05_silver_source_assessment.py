# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver Source Assessment
# MAGIC
# MAGIC **Purpose:** Assess Bronze readiness before defining or changing Silver transformation rules.
# MAGIC
# MAGIC **Inputs:** `commerceai.bronze.br_*` (nine Bronze Delta tables).  
# MAGIC **Outputs:** Read-only source contracts, observed schemas, ZIP-code diagnostics, type-conversion assessment, geolocation conflicts, and category-coverage findings.  
# MAGIC **Execution:** Manual development/investigation notebook; **not** part of a routine pipeline.  
# MAGIC **Validation owner:** `07_silver_validation` handles the repeatable operational gate after the Silver build.  
# MAGIC **Scope:** Diagnostics only. No Bronze or Silver tables are modified.

# COMMAND ----------

# DBTITLE 1,01 - Define Source Contracts
from pyspark.sql import functions as F

CATALOG = "commerceai"
BRONZE_SCHEMA = "bronze"

METADATA_COLUMNS = [
    "bronze_ingestion_timestamp",
    "source_file_name",
    "source_file_path"
]

SOURCE_COLUMNS = {
    "customers": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    ],
    "geolocation": [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state"
    ],
    "order_items": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value"
    ],
    "payments": [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    ],
    "reviews": [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp"
    ],
    "orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],
    "products": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ],
    "sellers": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    ],
    "product_category_translation": [
        "product_category_name",
        "product_category_name_english"
    ]
}

BRONZE_TABLES = {
    source: f"{CATALOG}.{BRONZE_SCHEMA}.br_{source}"
    for source in SOURCE_COLUMNS
}

print(f"Configured sources: {len(BRONZE_TABLES)}")

# COMMAND ----------

# DBTITLE 1,02 - Validate Source Column Contracts
structure_results = []

for source, table_name in BRONZE_TABLES.items():

    df = spark.table(table_name)

    expected_columns = set(
        SOURCE_COLUMNS[source] + METADATA_COLUMNS
    )

    actual_columns = set(df.columns)

    missing_columns = sorted(
        expected_columns - actual_columns
    )

    unexpected_columns = sorted(
        actual_columns - expected_columns
    )

    status = "FAIL" if missing_columns else "PASS"

    structure_results.append({
        "source": source,
        "table_name": table_name,
        "missing_columns": ", ".join(missing_columns),
        "unexpected_columns": ", ".join(unexpected_columns),
        "status": status
    })

structure_df = spark.createDataFrame(structure_results)

display(
    structure_df.select(
        "source",
        "table_name",
        "missing_columns",
        "unexpected_columns",
        "status"
    )
)

if any(row["status"] == "FAIL" for row in structure_results):
    raise ValueError("Bronze source contract validation failed.")

print("All source column contracts passed.")

# COMMAND ----------

# DBTITLE 1,03 - Inspect Bronze Column Types
schema_results = []

for source, table_name in BRONZE_TABLES.items():

    df = spark.table(table_name)

    for field in df.schema.fields:

        if field.name not in METADATA_COLUMNS:

            schema_results.append({
                "source": source,
                "column_name": field.name,
                "bronze_data_type": field.dataType.simpleString(),
                "nullable": field.nullable
            })

schema_df = spark.createDataFrame(schema_results)

display(
    schema_df.select(
        "source",
        "column_name",
        "bronze_data_type",
        "nullable"
    ).orderBy("source", "column_name")
)

# COMMAND ----------

# DBTITLE 1,04 - Profile ZIP Code Formats
from pyspark.sql import functions as F

ZIP_COLUMNS = {
    "customers": "customer_zip_code_prefix",
    "sellers": "seller_zip_code_prefix",
    "geolocation": "geolocation_zip_code_prefix"
}

zip_results = []

for source, column_name in ZIP_COLUMNS.items():

    df = spark.table(BRONZE_TABLES[source])

    # Validate that the configured column exists
    if column_name not in df.columns:
        raise ValueError(
            f"Invalid column for {source}: {column_name!r}. "
            f"Available columns: {df.columns}"
        )

    # Convert the ZIP prefix to text for profiling
    zip_text = F.trim(
        F.col(column_name).cast("string")
    )

    # Calculate ZIP format metrics
    result = df.agg(
        F.count("*").alias("total_rows"),

        F.sum(
            F.when(
                F.col(column_name).isNull(), 1
            ).otherwise(0)
        ).alias("null_zip_rows"),

        F.sum(
            F.when(
                zip_text.rlike(r"^[0-9]{1,4}$"), 1
            ).otherwise(0)
        ).alias("short_zip_rows"),

        F.sum(
            F.when(
                F.col(column_name).isNotNull()
                & ~zip_text.rlike(r"^[0-9]{1,5}$"),
                1
            ).otherwise(0)
        ).alias("invalid_format_rows"),

        F.sum(
            F.when(
                zip_text.rlike(r"^0[0-9]{4}$"), 1
            ).otherwise(0)
        ).alias("leading_zero_rows")

    ).first()

    zip_results.append({
        "source": source,
        "bronze_data_type": (
            df.schema[column_name]
            .dataType.simpleString()
        ),
        "total_rows": int(result["total_rows"]),
        "null_zip_rows": int(result["null_zip_rows"]),
        "short_zip_rows": int(result["short_zip_rows"]),
        "invalid_format_rows": int(
            result["invalid_format_rows"]
        ),
        "leading_zero_rows": int(
            result["leading_zero_rows"]
        )
    })

zip_df = spark.createDataFrame(zip_results)

display(
    zip_df.select(
        "source",
        "bronze_data_type",
        "total_rows",
        "null_zip_rows",
        "short_zip_rows",
        "invalid_format_rows",
        "leading_zero_rows"
    )
)

# COMMAND ----------

# DBTITLE 1,05 - Validate Type Conversion Safety
TYPE_CONTRACTS = {
    "geolocation": {
        "double": [
            "geolocation_lat",
            "geolocation_lng"
        ]
    },
    "order_items": {
        "int": [
            "order_item_id"
        ],
        "decimal(18,2)": [
            "price",
            "freight_value"
        ],
        "timestamp": [
            "shipping_limit_date"
        ]
    },
    "payments": {
        "int": [
            "payment_sequential",
            "payment_installments"
        ],
        "decimal(18,2)": [
            "payment_value"
        ]
    },
    "reviews": {
        "int": [
            "review_score"
        ],
        "timestamp": [
            "review_creation_date",
            "review_answer_timestamp"
        ]
    },
    "orders": {
        "timestamp": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
    },
    "products": {
        "int": [
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty"
        ],
        "double": [
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        ]
    }
}

cast_results = []

for source, type_rules in TYPE_CONTRACTS.items():

    df = spark.table(BRONZE_TABLES[source])

    checks = [
        (column_name, target_type)
        for target_type, column_names in type_rules.items()
        for column_name in column_names
    ]

    expressions = []

    for index, (column_name, target_type) in enumerate(checks):

        converted_value = F.expr(
            f"try_cast(`{column_name}` AS {target_type})"
        )

        expressions.append(
            F.sum(
                F.when(
                    F.col(column_name).isNotNull()
                    & converted_value.isNull(),
                    1
                ).otherwise(0)
            ).alias(f"failed_cast_{index}")
        )

    measured = df.agg(*expressions).first()

    for index, (column_name, target_type) in enumerate(checks):

        cast_results.append({
            "source": source,
            "column_name": column_name,
            "target_type": target_type,
            "failed_cast_rows": int(
                measured[f"failed_cast_{index}"]
            )
        })

cast_df = spark.createDataFrame(cast_results)

display(
    cast_df.select(
        "source",
        "column_name",
        "target_type",
        "failed_cast_rows"
    ).orderBy(
        F.desc("failed_cast_rows"),
        "source",
        "column_name"
    )
)

# COMMAND ----------

# DBTITLE 1,06 - Inspect Geolocation Consistency
geo = spark.table(
    BRONZE_TABLES["geolocation"]
)

geo = (
    geo
    .withColumn(
        "_latitude",
        F.expr(
            "try_cast(`geolocation_lat` AS DOUBLE)"
        )
    )
    .withColumn(
        "_longitude",
        F.expr(
            "try_cast(`geolocation_lng` AS DOUBLE)"
        )
    )
)

geo_profile = (
    geo
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.count("*").alias("rows_per_prefix"),

        F.countDistinct(
            "geolocation_state"
        ).alias("state_variants"),

        F.countDistinct(
            "geolocation_city"
        ).alias("city_variants"),

        F.min("_latitude").alias("min_latitude"),
        F.max("_latitude").alias("max_latitude"),

        F.min("_longitude").alias("min_longitude"),
        F.max("_longitude").alias("max_longitude")
    )
    .withColumn(
        "has_coordinate_variation",
        (F.col("min_latitude") != F.col("max_latitude"))
        | (F.col("min_longitude") != F.col("max_longitude"))
    )
)

geo_summary = geo_profile.agg(
    F.count("*").alias("zip_prefix_groups"),

    F.sum(
        F.when(F.col("state_variants") > 1, 1)
         .otherwise(0)
    ).alias("prefixes_with_multiple_states"),

    F.sum(
        F.when(F.col("city_variants") > 1, 1)
         .otherwise(0)
    ).alias("prefixes_with_multiple_cities"),

    F.sum(
        F.when(F.col("has_coordinate_variation"), 1)
         .otherwise(0)
    ).alias("prefixes_with_coordinate_variation"),

    F.max("rows_per_prefix").alias(
        "maximum_rows_per_prefix"
    )
).first()

print("Geolocation consistency summary")

for metric, value in geo_summary.asDict().items():
    print(f"{metric}: {value}")

display(
    geo_profile
    .filter(
        (F.col("state_variants") > 1)
        | (F.col("city_variants") > 1)
        | F.col("has_coordinate_variation")
    )
    .orderBy(
        F.desc("state_variants"),
        F.desc("city_variants"),
        F.desc("rows_per_prefix")
    )
    .limit(20)
)

# COMMAND ----------

# DBTITLE 1,07 - Inspect Product Category Coverage
products = (
    spark.table(BRONZE_TABLES["products"])
    .select("product_id", "product_category_name")
)

translations = (
    spark.table(
        BRONZE_TABLES["product_category_translation"]
    )
    .select("product_category_name")
)

missing_category_rows = products.filter(
    F.col("product_category_name").isNull()
    | (F.trim("product_category_name") == "")
).count()

valid_products = products.filter(
    F.col("product_category_name").isNotNull()
    & (F.trim("product_category_name") != "")
)

raw_unmatched = valid_products.join(
    translations,
    on="product_category_name",
    how="left_anti"
)

raw_unmatched_count = raw_unmatched.count()

normalized_products = valid_products.withColumn(
    "category_key",
    F.lower(F.trim("product_category_name"))
)

normalized_translations = (
    translations
    .withColumn(
        "category_key",
        F.lower(F.trim("product_category_name"))
    )
    .select("category_key")
    .distinct()
)

normalized_unmatched = normalized_products.join(
    normalized_translations,
    on="category_key",
    how="left_anti"
)

normalized_unmatched_count = normalized_unmatched.count()

print(f"Products with missing category: {missing_category_rows}")

print(
    f"Unmatched product rows (raw): "
    f"{raw_unmatched_count}"
)

print(
    f"Unmatched product rows (normalized): "
    f"{normalized_unmatched_count}"
)

display(
    normalized_unmatched
    .groupBy("product_category_name")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# DBTITLE 1,08 - Summarize Silver Assessment
structure_failures = [
    row
    for row in structure_results
    if row["status"] == "FAIL"
]

cast_failures = [
    row
    for row in cast_results
    if row["failed_cast_rows"] > 0
]

zip_format_issues = [
    row
    for row in zip_results
    if row["invalid_format_rows"] > 0
]

print("=== SILVER SOURCE ASSESSMENT ===")

print(f"Sources assessed: {len(BRONZE_TABLES)}")

print(
    f"Source contract failures: "
    f"{len(structure_failures)}"
)

print(
    f"Columns with cast failures: "
    f"{len(cast_failures)}"
)

print(
    f"Sources with invalid ZIP formats: "
    f"{len(zip_format_issues)}"
)

print(
    f"Products with missing categories: "
    f"{missing_category_rows}"
)

print(
    f"Products without normalized category mapping: "
    f"{normalized_unmatched_count}"
)

print(
    f"ZIP prefixes with multiple states: "
    f"{geo_summary['prefixes_with_multiple_states']}"
)

print(
    f"ZIP prefixes with coordinate variation: "
    f"{geo_summary['prefixes_with_coordinate_variation']}"
)

if structure_failures or cast_failures:
    print("Assessment status: REVIEW REQUIRED")
else:
    print("Assessment status: NO STRUCTURAL OR CAST BLOCKERS")

print("No Bronze or Silver tables were modified.")