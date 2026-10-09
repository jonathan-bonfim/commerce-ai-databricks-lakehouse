-- Databricks notebook source
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