# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 08 - Gold Dimensions
# MAGIC
# MAGIC **Purpose:** Build conformed Gold dimensions using version-controlled Spark SQL files.
# MAGIC
# MAGIC **Inputs:** `commerceai.silver.sl_orders`, `commerceai.silver.sl_order_items`, and `commerceai.silver.sl_reviews` for the first date-dimension build.
# MAGIC
# MAGIC **Outputs:** `commerceai.gold.dim_date` (one row per calendar date).
# MAGIC
# MAGIC **Main Responsibilities:** Review the session time zone, load the canonical `.sql` source file from the project repository, and execute it without duplicating business logic in Python.
# MAGIC
# MAGIC **Execution/Validation Notes:** Set `PROJECT_ROOT` to the absolute Databricks workspace path of this repository. Run this notebook before `10_gold_validation`. The date range is derived from source event dates and includes entire calendar years. Time-zone interpretation must remain consistent with Silver; no time-zone conversion is performed here. Additional dimensions will be added in later steps.
# MAGIC

# COMMAND ----------

# DBTITLE 1,01 - Inspect Session Time Zone
# MAGIC %sql
# MAGIC SELECT current_timezone() AS session_time_zone;

# COMMAND ----------

# DBTITLE 1,02 - Build Date Dimension From SQL File
from pathlib import Path

# Set the absolute path to your CommerceAI Git/workspace folder.
PROJECT_ROOT = Path("/Workspace/Users/jonathan.bonfim.data@gmail.com/commerce-ai-databricks-lakehouse")
sql_file = PROJECT_ROOT / "sql" / "gold" / "01_dim_date.sql"

if not sql_file.is_file():
    raise FileNotFoundError(f"SQL file not found: {sql_file}")

spark.sql(sql_file.read_text(encoding="utf-8"))
print("Gold date dimension SQL executed: commerceai.gold.dim_date")

# COMMAND ----------

# DBTITLE 1,03 - Preview Date Dimension
# MAGIC %sql
# MAGIC SELECT date_key, full_date, year_number, month_name, iso_week_number, iso_week_year, is_weekend
# MAGIC FROM commerceai.gold.dim_date
# MAGIC ORDER BY full_date
# MAGIC LIMIT 10;