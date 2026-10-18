# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08 - Gold Dimensions
# MAGIC
# MAGIC **Purpose:** Build conformed Gold dimensions with SQL-first, version-controlled transformation files.
# MAGIC
# MAGIC **Inputs:** `commerceai.silver.sl_orders`, `commerceai.silver.sl_order_items`, `commerceai.silver.sl_reviews`, `commerceai.silver.sl_customers`, and `commerceai.silver.sl_geolocation`; SQL files in `sql/gold/`.
# MAGIC
# MAGIC **Outputs:** `commerceai.gold.dim_date` (one row per calendar date) and `commerceai.gold.dim_customer` (one row per `customer_id`).
# MAGIC
# MAGIC **Main Responsibilities:** Inspect the session time zone; run external `.sql` builds via `spark.sql(...)`; keep customer identity and missing geolocation intact; present small previews. Business transformations remain in `.sql` files.
# MAGIC
# MAGIC **Execution/Validation Notes:** `PROJECT_ROOT` is the absolute Databricks workspace path of this repository. The SQL files must exist in `sql/gold/` at the repository root. To build only the new customer dimension, run cell 04; to rebuild all currently implemented dimensions, run cells 01–05. Run `10_gold_validation` separately after building. No timezone conversion is applied to source timestamps. Both builds use full replacement of the target Delta table.
# MAGIC

# COMMAND ----------

# DBTITLE 1,01 - Inspect Session Time Zone
# MAGIC %sql
# MAGIC SELECT current_timezone() AS session_time_zone;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Date Dimension From SQL File

# COMMAND ----------

# DBTITLE 1,01 - Build Date Dimension From SQL File
from pathlib import Path

# Set the absolute path to your CommerceAI Git/workspace folder.
PROJECT_ROOT = Path("/Workspace/Users/jonathan.bonfim.data@gmail.com/commerce-ai-databricks-lakehouse")
sql_file = PROJECT_ROOT / "sql" / "gold" / "01_dim_date.sql"

if not sql_file.is_file():
    raise FileNotFoundError(f"SQL file not found: {sql_file}")

spark.sql(sql_file.read_text(encoding="utf-8"))
print("Gold date dimension SQL executed: commerceai.gold.dim_date")

# COMMAND ----------

# DBTITLE 1,02 - Preview Date Dimension
# MAGIC %sql
# MAGIC SELECT date_key, full_date, year_number, month_name, iso_week_number, iso_week_year, is_weekend
# MAGIC FROM commerceai.gold.dim_date
# MAGIC ORDER BY full_date
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Customer Dimension From SQL File
# MAGIC

# COMMAND ----------

# DBTITLE 1,03 - Build Customer Dimension From SQL File
from pathlib import Path

PROJECT_ROOT = Path(
    "/Workspace/Users/jonathan.bonfim.data@gmail.com/commerce-ai-databricks-lakehouse"
)
sql_file = PROJECT_ROOT / "sql" / "gold" / "02_dim_customer.sql"

if not sql_file.is_file():
    raise FileNotFoundError(f"SQL file not found: {sql_file}")

spark.sql(sql_file.read_text(encoding="utf-8"))
print("Gold customer dimension SQL executed: commerceai.gold.dim_customer")


# COMMAND ----------

# DBTITLE 1,04 - Preview Customer Dimension
display(
    spark.sql("""
        SELECT
            customer_id,
            customer_unique_id,
            customer_state,
            customer_zip_code_prefix,
            has_geolocation_match,
            has_geolocation_coordinates,
            geolocation_coordinate_status
        FROM commerceai.gold.dim_customer
        ORDER BY customer_id
        LIMIT 10
    """)
)
