# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 10 - Gold Validation
# MAGIC
# MAGIC **Purpose:** Run the independent Gold data-quality gate for implemented dimensions and facts.
# MAGIC
# MAGIC **Inputs:** `commerceai.gold.dim_date` and `sql/validation/01_dim_date_checks.sql`.
# MAGIC
# MAGIC **Outputs:** A PASS/FAIL summary; raises an exception if any check fails.
# MAGIC
# MAGIC **Main Responsibilities:** Execute version-controlled SQL validation contracts separately from transformation notebooks.
# MAGIC
# MAGIC **Execution/Validation Notes:** Set `PROJECT_ROOT` to the same absolute workspace folder used by `08_gold_dimensions`. This first version validates only `dim_date`. Additional checks will be incorporated as other Gold tables are implemented. Run after building `dim_date`.
# MAGIC

# COMMAND ----------

# DBTITLE 1,01 - Validate Date Dimension From SQL File
from pathlib import Path

# Use the same repository path configured in the Gold build notebook.
PROJECT_ROOT = Path("/Workspace/Users/jonathan.bonfim.data@gmail.com/commerce-ai-databricks-lakehouse")
sql_file = PROJECT_ROOT / "sql" / "validation" / "01_dim_date_checks.sql"

if not sql_file.is_file():
    raise FileNotFoundError(f"SQL file not found: {sql_file}")

check_results = spark.sql(sql_file.read_text(encoding="utf-8")).collect()

for result in check_results:
    print(f"{result['status']}: {result['check_name']} | observed={result['observed_value']} | expected={result['expected_value']}")

failed_checks = [result['check_name'] for result in check_results if result['status'] != "PASS"]
if failed_checks:
    raise RuntimeError(f"Gold validation failed: {failed_checks}")

print(f"Gold date dimension validation: {len(check_results)} PASS, 0 FAIL")