# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 10 - Gold Validation
# MAGIC
# MAGIC **Purpose:** Run independent quality checks on Gold tables after builds complete.
# MAGIC
# MAGIC **Inputs:** `commerceai.gold.dim_date`, `commerceai.gold.dim_customer`, source Silver reference tables, and SQL checks in `sql/validation/`.
# MAGIC
# MAGIC **Outputs:** Separate PASS/FAIL summaries for implemented dimensions; raises an exception for any failed check.
# MAGIC
# MAGIC **Main Responsibilities:** Run SQL validation separately from dimension builds. Check key integrity, source reconciliation, geolocation coverage, and value preservation.
# MAGIC
# MAGIC **Execution/Validation Notes:** SQL files must be available at `PROJECT_ROOT/sql/validation/`. Cell 01 validates the date dimension (already verified at 7 PASS, 0 FAIL in the previous run). Cell 02 validates the customer dimension with 9 checks. Run after `08_gold_dimensions`; both validations belong to the same independent gate. Known Silver geolocation gaps are preserved, not suppressed or treated as failures solely for being missing.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 - Validate Date Dimension From SQL File

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 - Validate Customer Dimension From SQL File
# MAGIC

# COMMAND ----------

# DBTITLE 1,02 - Validate Customer Dimension From SQL File
from pathlib import Path

PROJECT_ROOT = Path(
    "/Workspace/Users/jonathan.bonfim.data@gmail.com/commerce-ai-databricks-lakehouse"
)
sql_file = PROJECT_ROOT / "sql" / "validation" / "02_dim_customer_checks.sql"

if not sql_file.is_file():
    raise FileNotFoundError(f"SQL file not found: {sql_file}")

check_results = spark.sql(sql_file.read_text(encoding="utf-8")).collect()

for result in check_results:
    print(
        f"{result['status']}: {result['check_name']} | "
        f"observed={result['observed_value']} | "
        f"expected={result['expected_value']}"
    )

failed_checks = [
    result['check_name'] for result in check_results
    if result['status'] != "PASS"
]
if failed_checks:
    raise RuntimeError(f"Gold customer dimension validation failed: {failed_checks}")

print(f"Gold customer dimension validation: {len(check_results)} PASS, 0 FAIL")
