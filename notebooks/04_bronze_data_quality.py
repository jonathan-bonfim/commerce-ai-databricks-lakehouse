# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Data Quality
# MAGIC
# MAGIC This notebook profiles key quality, nulls, referential integrity, geolocation granularity, and review parsing quality before any Silver transformations are applied.
# MAGIC

# COMMAND ----------

# DBTITLE 1,01 - Imports and Bronze Configuration
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.functions import col

BRONZE_SCHEMA = "commerceai.bronze"


# COMMAND ----------

# DBTITLE 1,02 - Bronze Table Configuration
bronze_tables = [
    "br_customers",
    "br_geolocation",
    "br_order_items",
    "br_payments",
    "br_reviews",
    "br_orders",
    "br_products",
    "br_sellers",
    "br_product_category_translation"
]


# COMMAND ----------

# DBTITLE 1,03 - Bronze Table Inventory
inventory_results = []

for table_name in bronze_tables:
    full_table_name = f"{BRONZE_SCHEMA}.{table_name}"
    df = spark.table(full_table_name)

    inventory_results.append({
        "table_name": table_name,
        "row_count": df.count(),
        "column_count": len(df.columns)
    })

inventory_df = spark.createDataFrame(inventory_results)

display(
    inventory_df.orderBy(F.desc("row_count"))
)


# COMMAND ----------

# DBTITLE 1,04 - Key Definitions
key_definitions = [
    {"table_name": "br_orders", "key_columns": ["order_id"], "key_type": "expected_primary_key"},
    {"table_name": "br_customers", "key_columns": ["customer_id"], "key_type": "expected_primary_key"},
    {"table_name": "br_products", "key_columns": ["product_id"], "key_type": "expected_primary_key"},
    {"table_name": "br_sellers", "key_columns": ["seller_id"], "key_type": "expected_primary_key"},
    {"table_name": "br_order_items", "key_columns": ["order_id", "order_item_id"], "key_type": "expected_composite_key"},
    {"table_name": "br_payments", "key_columns": ["order_id", "payment_sequential"], "key_type": "expected_composite_key"},
    {"table_name": "br_product_category_translation", "key_columns": ["product_category_name"], "key_type": "expected_primary_key"},
    {"table_name": "br_reviews", "key_columns": ["review_id"], "key_type": "candidate_key"},
    {"table_name": "br_reviews", "key_columns": ["review_id", "order_id"], "key_type": "candidate_composite_key"}
]


# COMMAND ----------

# DBTITLE 1,05 - Key Quality Check Function
def check_key_quality(
    table_name: str,
    key_columns: list,
    key_type: str
):
    full_table_name = f"{BRONZE_SCHEMA}.{table_name}"
    df = spark.table(full_table_name)

    null_condition = reduce(
        lambda x, y: x | y,
        [col(column_name).isNull() for column_name in key_columns]
    )

    null_key_rows = df.filter(null_condition).count()

    duplicate_keys_df = (
        df
        .groupBy(*key_columns)
        .count()
        .filter(col("count") > 1)
    )

    duplicate_key_groups = duplicate_keys_df.count()

    duplicate_excess_rows = (
        duplicate_keys_df
        .select(F.sum(col("count") - 1).alias("duplicate_excess_rows"))
        .first()["duplicate_excess_rows"]
    ) or 0

    return {
        "table_name": table_name,
        "key_columns": ", ".join(key_columns),
        "key_type": key_type,
        "null_key_rows": null_key_rows,
        "duplicate_key_groups": duplicate_key_groups,
        "duplicate_excess_rows": duplicate_excess_rows
    }


# COMMAND ----------

# DBTITLE 1,06 - Key Quality Results
key_quality_results = []

for key_definition in key_definitions:
    key_quality_results.append(
        check_key_quality(
            table_name=key_definition["table_name"],
            key_columns=key_definition["key_columns"],
            key_type=key_definition["key_type"]
        )
    )

key_quality_df = spark.createDataFrame(key_quality_results)

display(key_quality_df)


# COMMAND ----------

# DBTITLE 1,07 - Null Profiling Function
TECHNICAL_COLUMNS = {
    "bronze_ingestion_timestamp",
    "source_file_name",
    "source_file_path"
}


def profile_nulls(table_name: str):
    full_table_name = f"{BRONZE_SCHEMA}.{table_name}"
    df = spark.table(full_table_name)

    total_rows = df.count()

    business_columns = [
        column_name
        for column_name in df.columns
        if column_name not in TECHNICAL_COLUMNS
    ]

    null_expressions = [
        F.sum(
            F.when(col(column_name).isNull(), 1).otherwise(0)
        ).alias(column_name)
        for column_name in business_columns
    ]

    null_counts = df.agg(*null_expressions).first().asDict()

    return [
        {
            "table_name": table_name,
            "column_name": column_name,
            "total_rows": total_rows,
            "null_count": null_count,
            "null_percentage": (null_count / total_rows * 100) if total_rows else 0.0
        }
        for column_name, null_count in null_counts.items()
    ]


# COMMAND ----------

# DBTITLE 1,08 - Null Profiling Results
null_profile_results = []

for table_name in bronze_tables:
    null_profile_results.extend(profile_nulls(table_name))

null_profile_df = spark.createDataFrame(null_profile_results)

display(
    null_profile_df
    .filter(col("null_count") > 0)
    .orderBy(F.desc("null_percentage"))
)


# COMMAND ----------

# DBTITLE 1,09 - Relationship Definitions
relationships = [
    {
        "relationship_name": "orders_to_customers",
        "child_table": "br_orders",
        "parent_table": "br_customers",
        "column_mapping": [("customer_id", "customer_id")]
    },
    {
        "relationship_name": "order_items_to_orders",
        "child_table": "br_order_items",
        "parent_table": "br_orders",
        "column_mapping": [("order_id", "order_id")]
    },
    {
        "relationship_name": "payments_to_orders",
        "child_table": "br_payments",
        "parent_table": "br_orders",
        "column_mapping": [("order_id", "order_id")]
    },
    {
        "relationship_name": "reviews_to_orders",
        "child_table": "br_reviews",
        "parent_table": "br_orders",
        "column_mapping": [("order_id", "order_id")]
    },
    {
        "relationship_name": "order_items_to_products",
        "child_table": "br_order_items",
        "parent_table": "br_products",
        "column_mapping": [("product_id", "product_id")]
    },
    {
        "relationship_name": "order_items_to_sellers",
        "child_table": "br_order_items",
        "parent_table": "br_sellers",
        "column_mapping": [("seller_id", "seller_id")]
    },
    {
        "relationship_name": "products_to_category_translation",
        "child_table": "br_products",
        "parent_table": "br_product_category_translation",
        "column_mapping": [("product_category_name", "product_category_name")]
    }
]


# COMMAND ----------

# DBTITLE 1,10 - Referential Integrity Check Function
def check_referential_integrity(
    relationship_name: str,
    child_table: str,
    parent_table: str,
    column_mapping: list
):
    child_df = spark.table(
        f"{BRONZE_SCHEMA}.{child_table}"
    ).alias("child")

    parent_df = spark.table(
        f"{BRONZE_SCHEMA}.{parent_table}"
    ).alias("parent")

    child_columns = [
        child_column
        for child_column, _ in column_mapping
    ]

    valid_child_condition = reduce(
        lambda x, y: x & y,
        [
            F.col(f"child.{column_name}").isNotNull()
            for column_name in child_columns
        ]
    )

    join_condition = reduce(
        lambda x, y: x & y,
        [
            F.col(f"child.{child_column}") == F.col(f"parent.{parent_column}")
            for child_column, parent_column in column_mapping
        ]
    )

    total_child_rows = child_df.count()
    valid_child_df = child_df.filter(valid_child_condition)
    valid_child_rows = valid_child_df.count()
    null_key_rows = total_child_rows - valid_child_rows

    orphan_rows = (
        valid_child_df
        .join(parent_df, join_condition, "left_anti")
        .count()
    )

    return {
        "relationship_name": relationship_name,
        "child_table": child_table,
        "parent_table": parent_table,
        "total_child_rows": total_child_rows,
        "valid_child_rows": valid_child_rows,
        "null_key_rows": null_key_rows,
        "orphan_rows": orphan_rows
    }


# COMMAND ----------

# DBTITLE 1,11 - Referential Integrity Results
relationship_results = []

for relationship in relationships:
    relationship_results.append(
        check_referential_integrity(
            relationship_name=relationship["relationship_name"],
            child_table=relationship["child_table"],
            parent_table=relationship["parent_table"],
            column_mapping=relationship["column_mapping"]
        )
    )

relationship_results_df = spark.createDataFrame(relationship_results)

display(relationship_results_df)


# COMMAND ----------

# DBTITLE 1,12 - Geolocation Granularity Analysis
geolocation_df = spark.table(
    f"{BRONZE_SCHEMA}.br_geolocation"
)

geolocation_summary = (
    geolocation_df
    .agg(
        F.count("*").alias("total_rows"),
        F.countDistinct("geolocation_zip_code_prefix").alias("distinct_zip_code_prefixes")
    )
)

display(geolocation_summary)


# COMMAND ----------

# DBTITLE 1,13 - Geolocation Records per ZIP Code
geolocation_zip_stats = (
    geolocation_df
    .groupBy("geolocation_zip_code_prefix")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.desc("row_count"))
)

display(geolocation_zip_stats)


# COMMAND ----------

# DBTITLE 1,14 - Geolocation Exact Duplicate Analysis
geolocation_total_rows = geolocation_df.count()

geolocation_distinct_rows = (
    geolocation_df
    .drop(*TECHNICAL_COLUMNS)
    .dropDuplicates()
    .count()
)

geolocation_exact_duplicates = (
    geolocation_total_rows - geolocation_distinct_rows
)

display(
    spark.createDataFrame([
        {
            "total_rows": geolocation_total_rows,
            "distinct_rows": geolocation_distinct_rows,
            "exact_duplicate_rows": geolocation_exact_duplicates
        }
    ])
)


# COMMAND ----------

# DBTITLE 1,15 - Order Status Distribution
display(
    spark.table(f"{BRONZE_SCHEMA}.br_orders")
    .groupBy("order_status")
    .count()
    .orderBy(F.desc("count"))
)


# COMMAND ----------

# DBTITLE 1,16 - Payment Type Distribution
display(
    spark.table(f"{BRONZE_SCHEMA}.br_payments")
    .groupBy("payment_type")
    .count()
    .orderBy(F.desc("count"))
)


# COMMAND ----------

# DBTITLE 1,17 - Review Score Distribution
reviews_df = spark.table(
    f"{BRONZE_SCHEMA}.br_reviews"
)

display(
    reviews_df
    .groupBy("review_score")
    .count()
    .orderBy("review_score")
)


# COMMAND ----------

# DBTITLE 1,18 - Customer Identity Analysis
customers_df = spark.table(
    f"{BRONZE_SCHEMA}.br_customers"
)

customer_identity_summary = (
    customers_df
    .agg(
        F.count("*").alias("total_customer_records"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("customer_unique_id").alias("distinct_unique_customers")
    )
)

display(customer_identity_summary)


# COMMAND ----------

# DBTITLE 1,19 - Review Structural Validation
HEX_ID_PATTERN = r"^[0-9a-f]{32}$"

review_structural_validation = (
    reviews_df
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
        ).alias("invalid_review_score_rows")
    )
)

display(review_structural_validation)


# COMMAND ----------

# DBTITLE 1,20 - Invalid Review Record Samples
invalid_review_records = (
    reviews_df
    .filter(
        F.col("review_id").isNull()
        | F.col("order_id").isNull()
        | ~F.col("review_id").rlike(HEX_ID_PATTERN)
        | (
            F.col("order_id").isNotNull()
            & ~F.col("order_id").rlike(HEX_ID_PATTERN)
        )
        | F.col("review_score").isNull()
        | ~F.col("review_score").cast("string").isin("1", "2", "3", "4", "5")
    )
)

display(
    invalid_review_records.select(
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
        "source_file_name"
    )
)


# COMMAND ----------

# DBTITLE 1,21 - Review ID Duplicate Analysis
review_id_duplicates = (
    reviews_df
    .filter(F.col("review_id").isNotNull())
    .groupBy("review_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("order_id").alias("distinct_order_count")
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.desc("row_count"))
)

display(review_id_duplicates)


# COMMAND ----------

# DBTITLE 1,22 - Review and Order Composite Key Duplicates
review_order_duplicates = (
    reviews_df
    .filter(
        F.col("review_id").isNotNull()
        & F.col("order_id").isNotNull()
    )
    .groupBy("review_id", "order_id")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
)

display(review_order_duplicates)


# COMMAND ----------

# DBTITLE 1,23 - Orders with Multiple Reviews
orders_with_multiple_reviews = (
    reviews_df
    .filter(F.col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(
        F.count("*").alias("review_row_count"),
        F.countDistinct("review_id").alias("distinct_review_count")
    )
    .filter(F.col("review_row_count") > 1)
    .orderBy(F.desc("review_row_count"))
)

display(orders_with_multiple_reviews)


# COMMAND ----------

# DBTITLE 1,24 - Known Multiline Review Validation
KNOWN_REVIEW_ID = "5090899e38cd55729c9783745f2706f5"

known_multiline_review = (
    reviews_df
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

display(known_multiline_review)


# COMMAND ----------

# DBTITLE 1,25 - Data Quality Summary
data_quality_summary = spark.createDataFrame([
    {
        "metric": "key_checks_with_issues",
        "value": key_quality_df.filter(
            (F.col("null_key_rows") > 0)
            | (F.col("duplicate_key_groups") > 0)
        ).count()
    },
    {
        "metric": "relationships_with_null_or_orphan_rows",
        "value": relationship_results_df.filter(
            (F.col("null_key_rows") > 0)
            | (F.col("orphan_rows") > 0)
        ).count()
    },
    {
        "metric": "geolocation_exact_duplicate_rows",
        "value": geolocation_exact_duplicates
    },
    {
        "metric": "invalid_review_records",
        "value": invalid_review_records.count()
    },
    {
        "metric": "orders_with_multiple_reviews",
        "value": orders_with_multiple_reviews.count()
    }
])

display(data_quality_summary)
