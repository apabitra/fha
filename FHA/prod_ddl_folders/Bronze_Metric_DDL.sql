-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("Catalog_name", "", "")
-- MAGIC Catalog_name = dbutils.widgets.get("Catalog_name")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql(f"use catalog {Catalog_name}")

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS AA4
MANAGED LOCATION 'abfss://bronze@adlsdiprodcus.dfs.core.windows.net/fha/managed/aa4';				
USE AA4;

CREATE TABLE AA4.BRONZE_METRIC
(
ID INTEGER NOT NULL,
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING	,
METRIC_NAME STRING,
METRIC_DETAILS STRING,
METRIC_TYPE STRING,
METRIC_FUNCTION STRING
) USING DELTA
location 'abfss://bronze@adlsdiprodcus.dfs.core.windows.net/fha/external/aa4/BRONZE_METRIC';

-- COMMAND ----------

INSERT INTO AA4.BRONZE_METRIC (ID, METRIC_NAME, METRIC_DETAILS, xmd_InsertDateTimestamp, xmd_InsertUserId)
VALUES 
(1, 'Sparseness', 'The percent of NULL values.', current_timestamp(), 'Manual One Time Load - smishra'),
(2, 'Mean', 'Average value', current_timestamp(), 'Manual One Time Load - smishra'),
(3, 'Standard Deviation', 'A measure of variability among the values that is based on the mean', current_timestamp(), 'Manual One Time Load - smishra'),
(4, 'Minimum', 'The minimum value', current_timestamp(), 'Manual One Time Load - smishra'),
(5, 'Maximum', 'The maximum value', current_timestamp(), 'Manual One Time Load - smishra'),
(6, 'Range', 'The difference between the minimum and maximum', current_timestamp(), 'Manual One Time Load - smishra'),
(7, 'Variance', 'The average of the squared differences from the mean', current_timestamp(), 'Manual One Time Load - smishra'),
(8, 'Skewness', 'A measure of the asymmetry of the distribution of values', current_timestamp(), 'Manual One Time Load - smishra'),
(9, 'Kurtosis', 'A measure of the ‘tailedness’ of the distribution of values.', current_timestamp(), 'Manual One Time Load - smishra'),
(10, 'Sum', 'The total of all values', current_timestamp(), 'Manual One Time Load - smishra'),
(11, 'Missing Value Count', 'The number of NULL or missing values', current_timestamp(), 'Manual One Time Load - smishra'),
(12, 'Unique Value Count', 'The number of distinct values that are not NULL or missing', current_timestamp(), 'Manual One Time Load - smishra'),
(13, 'Zero Count', 'The number of zero values', current_timestamp(), 'Manual One Time Load - smishra'),
(14, 'Negative Value Count', 'The number of values that are less than zero', current_timestamp(), 'Manual One Time Load - smishra'),
(15, 'Mode', 'The value that occurs most frequently', current_timestamp(), 'Manual One Time Load - smishra'),
(16, 'Median', 'The value that is in the middle of a sorted list, either ascending or descending, when the entry count is odd, or the average of the two middle values when the entry count is even.', current_timestamp(), 'Manual One Time Load - smishra');



-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS AA4
MANAGED LOCATION 'abfss://bronze@adlsdiprodcus.dfs.core.windows.net/fha/managed/aa4';				
USE AA4;

CREATE TABLE AA4.BRONZE_METADATA
(
  ID BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
SCHEMA_NAME STRING,
TABLE_NAME STRING,
COLUMN_NAME STRING,
DATA_TYPE STRING,
Is_Active BOOLEAN 	
) USING DELTA
location 'abfss://bronze@adlsdiprodcus.dfs.core.windows.net/fha/external/aa4/BRONZE_METADATA';

-- COMMAND ----------

INSERT INTO AA4.BRONZE_METADATA(SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DATA_TYPE,Is_Active,xmd_InsertDateTimestamp,xmd_InsertUserId)
SELECT 
    table_schema AS schema_name,
    table_name,
    column_name,
    data_type,
    TRUE as Is_Active,
    current_timestamp(),
    'Manual One Time Laod- smishra'
FROM 
    information_schema.columns
    where table_schema in ('cba14','cbr16','cbr17','cbr18','cbr20','cbr4') and
    table_schema in ('cba14','cbr16','cbr17','cbr18','cbr20','cbr4') and (table_name not like '%dim' and table_name not like '%fact%' and table_name not like '%bronze%' and table_name not like '%bkp%') 
ORDER BY 
    schema_name, table_name, ordinal_position

-- COMMAND ----------


CREATE SCHEMA IF NOT EXISTS AA4
MANAGED LOCATION 'abfss://bronze@adlsdiprodcus.dfs.core.windows.net/fha/managed/aa4';				
USE AA4;

CREATE TABLE AA4.BRONZE_METADATA_LOG
(
ID BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
xmd_InsertDateTimestamp	TIMESTAMP		NOT NULL	,
xmd_InsertUserId	STRING		NOT NULL	,
xmd_UpdateDateTimestamp	TIMESTAMP			,
xmd_UpdateUserId	STRING			,
xmd_DeleteDateTimestamp	TIMESTAMP			,
xmd_DeleteUserId	STRING			,
xmd_ChangeOperationCode	STRING			,
Metadata_Id BIGINT NOT NULL,
Metric_Name STRING,
Metric_Value STRING
--,CONSTRAINT BRONZE_METADATA_LOG_FK FOREIGN KEY(Metadata_Id) REFERENCES AA4.BRONZE_METADATA(ID)
)USING DELTA
location 'abfss://bronze@adlsdiprodcus.dfs.core.windows.net/fha/external/aa4/BRONZE_METADATA_LOG';