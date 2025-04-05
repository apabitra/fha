-- Databricks notebook source
use catalog bronze_dev;

USE AA4;

-- COMMAND ----------

DROP TABLE IF EXISTS AA4.BRONZE_METRIC;

DROP TABLE AA4.BRONZE_METADATA;

DROP TABLE AA4.BRONZE_METADATA_LOG;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # List of file locations to delete
-- MAGIC file_locations = [
-- MAGIC    "abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/external/aa4/BRONZE_METRIC",
-- MAGIC     "abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/external/aa4/BRONZE_METADATA",
-- MAGIC    "abfss://bronze@adlsdidevcus.dfs.core.windows.net/fha/external/aa4/BRONZE_METADATA_LOG"
-- MAGIC
-- MAGIC  ]
-- MAGIC
-- MAGIC # Iterate over the list of file locations and delete them
-- MAGIC for location in file_locations:
-- MAGIC     print(f"Deleting files in location: {location}")
-- MAGIC     dbutils.fs.rm(location, recurse=True)
-- MAGIC     print(f"Deleted files in: {location}\n")