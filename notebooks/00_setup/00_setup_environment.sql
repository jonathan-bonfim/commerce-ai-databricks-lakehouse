-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Environment Setup
-- MAGIC
-- MAGIC **Purpose:** Create the CommerceAI Unity Catalog, layer schemas, and the Bronze landing Volume.
-- MAGIC
-- MAGIC **Inputs:** Databricks workspace with Unity Catalog permissions.  
-- MAGIC **Outputs:** `commerceai` catalog; `bronze`, `silver`, `gold`, and `ml` schemas; `/Volumes/commerceai/bronze/raw_files/`.  
-- MAGIC **Execution:** One-time or manual infrastructure setup; **not** part of the recurring ingestion/transformation workflow.  
-- MAGIC **Scope:** Idempotent DDL only. No CSV ingestion, data profiling, or transformation.

-- COMMAND ----------

-- DBTITLE 1,01 - Catalog
CREATE CATALOG IF NOT EXISTS commerceai;

-- COMMAND ----------

-- DBTITLE 1,02 - Schemas
CREATE SCHEMA IF NOT EXISTS commerceai.bronze;
CREATE SCHEMA IF NOT EXISTS commerceai.silver;
CREATE SCHEMA IF NOT EXISTS commerceai.gold;
CREATE SCHEMA IF NOT EXISTS commerceai.ml;

-- COMMAND ----------

-- DBTITLE 1,03 - Volume
CREATE VOLUME IF NOT EXISTS commerceai.bronze.raw_files;