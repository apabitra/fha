-- Databricks notebook source
USE catalog silver_dev;

-- COMMAND ----------


USE catalog silver_dev;

USE CBA14;
DROP TABLE IF EXISTS FHA_USER_EVENT_TYPE;

USE CBA14;
DROP TABLE IF EXISTS FHA_USER_EVENT;

USE CBA14;
DROP TABLE IF EXISTS DISPOSITION_PATH_TYPE;

USE CBA14;
DROP TABLE IF EXISTS DISPOSITION_STATUS_TYPE;

USE CBA14;
DROP TABLE IF EXISTS FHA_USER_SESSIONS ;

USE CBA14;

DROP TABLE IF EXISTS SERVICER_ASSET_DISPOSITION_SUMMARY;

USE CBA14;

DROP TABLE IF EXISTS WORKER_SERVICE_PROCESS_INFO;


USE CBA14;
DROP TABLE IF EXISTS SERVICER_SOURCE_FILE_STATUS;

USE CBA14;
DROP TABLE IF EXISTS FHA_USER_STATUS;

USE CBA14;
DROP TABLE IF EXISTS SERVICER_ASSET_DISPOSITION_PATH;

USE CBA14;
DROP TABLE IF EXISTS SERVICER_ASSET_DISPOSITION;

USE CBA14;

DROP TABLE IF EXISTS USER_SERVICER;

USE CBA14;
DROP TABLE IF EXISTS MODULE;

USE CBA14;
DROP TABLE IF EXISTS PERMISSION;

USE CBA14;
DROP TABLE IF EXISTS ROLE_PERMISSION;

USE CBA14;
DROP TABLE IF EXISTS USER_ROLE;

USE CBA14;
DROP TABLE IF EXISTS ROLE;

USE CBA14;
DROP TABLE IF EXISTS FHA_USER;

USE CBA14;

DROP TABLE IF EXISTS SERVICER_ASSET;

USE CBA14;

DROP TABLE IF EXISTS SERVICER_SOURCE_FILE;

USE CBR4;
DROP TABLE IF EXISTS SERVICER;	

USE CBR20;			
DROP TABLE IF EXISTS USFN_EVICTION;

USE CBR17;			
DROP TABLE IF EXISTS FED_DEBENTURE_RATE;

USE CBR16;			
DROP TABLE IF EXISTS XOME_FHA_MARKETING ;

USE CBR16;			
DROP TABLE IF EXISTS XOME_TITLE_CURE;

USE CBR16;			
DROP TABLE IF EXISTS XOME_ESCROW;

USE CBR16;			
DROP TABLE IF EXISTS XOME_CLOSING_COST ;


USE  CBR18;							
DROP TABLE IF EXISTS HUD_FUTURE_EXPENSES;

USE  CBR18;						
DROP TABLE IF EXISTS HUD_DUE_DILIGENCE_TIMELINE;


USE CBR18;						
                       
DROP TABLE IF EXISTS HUD_CAFMV;






-- COMMAND ----------

-- MAGIC %python
-- MAGIC # List of multiple ADLS locations
-- MAGIC file_locations = [
-- MAGIC "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr18/HUD_CAFMV",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr18/HUD_DUE_DILIGENCE_TIMELINE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr18/HUD_FUTURE_EXPENSES",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_CLOSING_COST",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_ESCROW",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_TITLE_CURE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_FHA_MARKETING",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr17/FED_DEBENTURE_RATE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr20/TABLE USFN_EVICTION",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr4/SERVICER",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr14/SERVICER_SOURCE_FILE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/FHA_USER",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/ROLE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/USER_ROLE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/ROLE_PERMISSION",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/PERMISSION",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/MODULE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/USER_SERVICER",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/FHA_USER_STATUS",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION_PATH",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_SOURCE_FILE_STATUS",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/WORKER_SERVICE_PROCESS_INFO",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/FHA_USER_SESSIONS",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION_SUMMARY",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/DISPOSITION_PATH_TYPE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/DISPOSITION_STATUS_TYPE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/fha_user_event",
-- MAGIC "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/fha_user_event_type"
-- MAGIC     ]
-- MAGIC
-- MAGIC # Iterate over the list of locations and list files in each
-- MAGIC for location in file_locations:
-- MAGIC     print(f"Files in location: {location}")
-- MAGIC     files = dbutils.fs.ls(location)
-- MAGIC     for file in files:
-- MAGIC         print(file.name)
-- MAGIC     print("\n")  # Add a separator between locations
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # List of file locations to delete
-- MAGIC file_locations = [
-- MAGIC "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr18/HUD_CAFMV",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr18/HUD_DUE_DILIGENCE_TIMELINE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr18/HUD_FUTURE_EXPENSES",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_CLOSING_COST",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_ESCROW",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_TITLE_CURE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr16/XOME_FHA_MARKETING",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr17/FED_DEBENTURE_RATE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr20/TABLE USFN_EVICTION",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr4/SERVICER",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cbr14/SERVICER_SOURCE_FILE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/FHA_USER",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/ROLE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/USER_ROLE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/ROLE_PERMISSION",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/PERMISSION",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/MODULE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/USER_SERVICER",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/FHA_USER_STATUS",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION_PATH",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_SOURCE_FILE_STATUS",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/WORKER_SERVICE_PROCESS_INFO",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/FHA_USER_SESSIONS",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/SERVICER_ASSET_DISPOSITION_SUMMARY",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/DISPOSITION_PATH_TYPE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/DISPOSITION_STATUS_TYPE",
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/fha_user_event",
-- MAGIC "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/fha_user_event_type"
-- MAGIC ]
-- MAGIC
-- MAGIC # Iterate over the list of file locations and delete them
-- MAGIC for location in file_locations:
-- MAGIC     print(f"Deleting files in location: {location}")
-- MAGIC     dbutils.fs.rm(location, recurse=True)
-- MAGIC     print(f"Deleted files in: {location}\n")
-- MAGIC

-- COMMAND ----------

USE catalog silver_dev;



USE CBA14;
DROP TABLE IF EXISTS FHA_USER_EVENT;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # List of file locations to delete
-- MAGIC file_locations = [
-- MAGIC     "abfss://silver@adlsdidevcus.dfs.core.windows.net/fha/external/cba14/fha_user_event"
-- MAGIC
-- MAGIC ]
-- MAGIC
-- MAGIC # Iterate over the list of file locations and delete them
-- MAGIC for location in file_locations:
-- MAGIC     print(f"Deleting files in location: {location}")
-- MAGIC     dbutils.fs.rm(location, recurse=True)
-- MAGIC     print(f"Deleted files in: {location}\n")
-- MAGIC