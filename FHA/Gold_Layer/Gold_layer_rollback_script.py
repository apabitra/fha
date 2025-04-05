# Databricks notebook source
# MAGIC %sql
# MAGIC use dwh ;
# MAGIC drop table if exists dwh.asset_dim ;
# MAGIC
# MAGIC use dwh ;
# MAGIC drop table if exists dwh.date_dim ;
# MAGIC
# MAGIC use dwh ;
# MAGIC drop table if exists dwh.servicer_asset_disposition_fact ;
# MAGIC
# MAGIC use dwh ;
# MAGIC drop table if exists dwh.servicer_dim ;
# MAGIC
# MAGIC use dwh ;
# MAGIC drop table if exists dwh.servicer_upload_fact ;
# MAGIC
# MAGIC use dwh ;
# MAGIC drop table if exists dwh.user_dim ;
# MAGIC
# MAGIC use dwh ;
# MAGIC drop table if exists dwh.user_event_fact ;
# MAGIC
# MAGIC use dwh ;
# MAGIC drop table if exists dwh.user_session_fact ;

# COMMAND ----------

# MAGIC %sql
# MAGIC use dwh ;
# MAGIC drop table if exists dwh.date_dim ;

# COMMAND ----------

# MAGIC %sql
# MAGIC use dwh ;
# MAGIC drop table if exists dwh.asset_dim ;

# COMMAND ----------

location_path_list=['abfss://gold@adlsdidevcus.dfs.core.windows.net/fha/managed/dwh/asset_dim', 
                    'abfss://gold@adlsdidevcus.dfs.core.windows.net/fha/managed/dwh/data_dim', 
                    'abfss://gold@adlsdidevcus.dfs.core.windows.net/fha/managed/dwh/servicer_asset_disposition_fact', 
                    'abfss://gold@adlsdidevcus.dfs.core.windows.net/fha/managed/dwh/servicer_dim', 
                    'abfss://gold@adlsdidevcus.dfs.core.windows.net/fha/managed/dwh/servicer_upload_fact', 
                    'abfss://gold@adlsdidevcus.dfs.core.windows.net/fha/managed/dwh/user_dim', 
                    'abfss://gold@adlsdidevcus.dfs.core.windows.net/fha/managed/dwh/user_event_fact',
                    'abfss://gold@adlsdidevcus.dfs.core.windows.net/fha/managed/dwh/user_session_fact']

for location in file_locations:
    print(f"Files in location: {location}")
    files = dbutils.fs.ls(location)
    for file in files:
        print(file.name)
    print("\n")  