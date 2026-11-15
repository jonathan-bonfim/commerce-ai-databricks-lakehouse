# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Customer Repeat Purchase — Feature Preparation
# MAGIC
# MAGIC ## Purpose
# MAGIC Prepare a customer-level supervised dataset to predict repeat orders occurring more than 24 hours and up to 90 days after the first observed order.
# MAGIC
# MAGIC ## Inputs
# MAGIC - `commerceai.gold.fct_orders`
# MAGIC - `commerceai.gold.dim_customer`
# MAGIC
# MAGIC ## Outputs
# MAGIC - Customer-level first-order snapshots
# MAGIC - Preliminary feature DataFrame
# MAGIC - Binary repeat purchase target
# MAGIC - Dataset integrity checks
# MAGIC
# MAGIC ## Main Responsibilities
# MAGIC 1. Identify the first observed order per consumer.
# MAGIC 2. Apply the approved observation cohort.
# MAGIC 3. Extract candidate features available at prediction time.
# MAGIC 4. Generate the 24-hour to 90-day repeat purchase label.
# MAGIC 5. Validate row counts, class balance, and missing values.
# MAGIC
# MAGIC ## Execution / Validation Notes
# MAGIC - Use PySpark DataFrames.
# MAGIC - Preserve the existing Bronze, Silver, and Gold tables.
# MAGIC - Do not create persistent ML tables yet.
# MAGIC - Do not train any models.
# MAGIC - Do not use future outcomes as predictive features.
# MAGIC - Treat final Gold item counts and order values as an at-purchase availability assumption requiring documentation.
# MAGIC - Use the current session timestamp semantics without arbitrary timezone conversions.
# MAGIC

# COMMAND ----------

# DBTITLE 1,01 - Build First Observed Order Snapshots

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load validated Gold sources
orders = spark.table(
    "commerceai.gold.fct_orders"
).select(
    "order_id",
    "customer_id",
    "order_purchase_timestamp",
    "item_count",
    "ordered_total_value_brl"
)

customers = spark.table(
    "commerceai.gold.dim_customer"
).select(
    "customer_id",
    "customer_unique_id",
    "customer_state"
)

# Resolve consumer identity
order_events = (
    orders
    .join(customers, on="customer_id", how="inner")
    .filter(
        F.col("customer_unique_id").isNotNull()
        & F.col("order_purchase_timestamp").isNotNull()
    )
)

# Rank all observed orders before filtering cohorts
customer_window = (
    Window
    .partitionBy("customer_unique_id")
    .orderBy(
        F.col("order_purchase_timestamp").asc(),
        F.col("order_id").asc()
    )
)

first_orders = (
    order_events
    .withColumn(
        "order_rank",
        F.row_number().over(customer_window)
    )
    .filter(F.col("order_rank") == 1)
    .filter(
        (F.col("order_purchase_timestamp") >=
         F.lit("2017-01-01").cast("timestamp"))
        &
        (F.col("order_purchase_timestamp") <
         F.lit("2018-06-01").cast("timestamp"))
    )
    .select(
        "customer_unique_id",
        F.col("order_id").alias("first_order_id"),
        F.col("order_purchase_timestamp").alias(
            "as_of_timestamp"
        ),
        F.col("customer_state").alias(
            "first_order_customer_state"
        ),
        F.col("item_count").alias(
            "first_order_item_count"
        ),
        F.col("ordered_total_value_brl").alias(
            "first_order_value_brl"
        ),
        F.dayofweek(
            "order_purchase_timestamp"
        ).alias("purchase_weekday"),
        F.month(
            "order_purchase_timestamp"
        ).alias("purchase_month")
    )
)

display(first_orders.limit(10))

# COMMAND ----------

# DBTITLE 1,02 - Generate Repeat Purchase Labels

# Identify consumers with qualifying future orders
returning_customers = (
    first_orders.alias("f")
    .join(
        order_events.select(
            "customer_unique_id",
            "order_purchase_timestamp"
        ).alias("r"),
        on=(
            (F.col("f.customer_unique_id") ==
             F.col("r.customer_unique_id"))
            &
            (
                F.col("r.order_purchase_timestamp") >
                F.expr("f.as_of_timestamp + INTERVAL 1 DAY")
            )
            &
            (
                F.col("r.order_purchase_timestamp") <=
                F.expr("f.as_of_timestamp + INTERVAL 90 DAYS")
            )
        ),
        how="left_semi"
    )
    .select("customer_unique_id")
    .distinct()
    .withColumn(
        "repeat_after_24h_90d_label",
        F.lit(1)
    )
)

# Attach labels to all eligible consumers
ml_dataset = (
    first_orders
    .join(
        returning_customers,
        on="customer_unique_id",
        how="left"
    )
    .withColumn(
        "repeat_after_24h_90d_label",
        F.coalesce(
            F.col("repeat_after_24h_90d_label"),
            F.lit(0)
        )
    )
)

display(
    ml_dataset.select(
        "customer_unique_id",
        "as_of_timestamp",
        "first_order_item_count",
        "first_order_value_brl",
        "repeat_after_24h_90d_label"
    ).limit(10)
)


# COMMAND ----------

# DBTITLE 1,03 - Validate Feature Dataset Integrity

validation = ml_dataset.agg(
    F.count("*").alias("total_consumers"),

    F.countDistinct(
        "customer_unique_id"
    ).alias("distinct_consumers"),

    F.sum(
        "repeat_after_24h_90d_label"
    ).alias("positive_labels"),

    F.sum(
        F.when(
            F.col("repeat_after_24h_90d_label") == 0,
            1
        ).otherwise(0)
    ).alias("negative_labels"),

    F.sum(
        F.when(
            F.col("first_order_value_brl").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_order_values"),

    F.sum(
        F.when(
            F.col("first_order_item_count").isNull()
            | (F.col("first_order_item_count") == 0),
            1
        ).otherwise(0)
    ).alias("missing_or_zero_item_counts")
)

display(validation)


# COMMAND ----------

# DBTITLE 1,04 - Create Leakage-Aware Temporal Splits

from pyspark.sql import functions as F

# Define temporal boundaries
train_end = "2017-10-01 00:00:00"

validation_start = "2018-01-01 00:00:00"
validation_end = "2018-02-01 00:00:00"

test_start = "2018-05-02 00:00:00"
test_end = "2018-06-01 00:00:00"

# Assign each consumer to a temporal partition
split_dataset = (
    ml_dataset
    .withColumn(
        "dataset_split",
        F.when(
            F.col("as_of_timestamp")
            < F.lit(train_end).cast("timestamp"),
            F.lit("TRAIN")
        )
        .when(
            (
                F.col("as_of_timestamp")
                >= F.lit(validation_start).cast("timestamp")
            )
            &
            (
                F.col("as_of_timestamp")
                < F.lit(validation_end).cast("timestamp")
            ),
            F.lit("VALIDATION")
        )
        .when(
            (
                F.col("as_of_timestamp")
                >= F.lit(test_start).cast("timestamp")
            )
            &
            (
                F.col("as_of_timestamp")
                < F.lit(test_end).cast("timestamp")
            ),
            F.lit("TEST")
        )
        .otherwise(F.lit("EXCLUDED"))
    )
)

# Keep only the selected experiment cohorts
experiment_dataset = split_dataset.filter(
    F.col("dataset_split") != "EXCLUDED"
)

# Create separate DataFrames
train_df = experiment_dataset.filter(
    F.col("dataset_split") == "TRAIN"
)

validation_df = experiment_dataset.filter(
    F.col("dataset_split") == "VALIDATION"
)

test_df = experiment_dataset.filter(
    F.col("dataset_split") == "TEST"
)
