-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Function to get all files under given path

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def getFileList(path):
-- MAGIC   """getFileList expects a path and returns a list of files"""
-- MAGIC   modifiedlist=[]
-- MAGIC   lists = dbutils.fs.ls(path)
-- MAGIC   global num
-- MAGIC   for i in lists:
-- MAGIC     modifiedlist.append(i.name)
-- MAGIC   return modifiedlist

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function to get actual fully qualified path for the run date

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import date
-- MAGIC
-- MAGIC def getTodaysPath(constant_path):
-- MAGIC   """getTodaysPath expects a path and returns actual path for that given day"""
-- MAGIC   today = date.today()
-- MAGIC   if(len(str(today.month))<2):
-- MAGIC     month = '0'+str(today.month)
-- MAGIC   else:
-- MAGIC     month = str(today.month)
-- MAGIC   if(len(str(today.day))<2):
-- MAGIC     day = '0'+str(today.day)
-- MAGIC   else:
-- MAGIC     day = str(today.day)
-- MAGIC   todays_path = f'{constant_path}/{today.year}/{month}/{day}'
-- MAGIC   return todays_path

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function to get all columns of a table(comma seperated)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def getColumns(tableName):
-- MAGIC   """getColumns expects a table name and returns all columns of the given table comma sepearted string type"""
-- MAGIC   df_column = spark.sql(f"show columns in {tableName}")
-- MAGIC   list_column = list(df_column.select('col_name').toPandas()['col_name'])
-- MAGIC   list_column.sort()
-- MAGIC   str_column = ",".join(list_column)
-- MAGIC   return str_column

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function to insert data into delta table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def insertIntoTable(temp_table, catalog_name, schema_name, table_name,islookup=True):
-- MAGIC   """insertIntoTable temp_table, catalog_name, schema_name, table_name,islookup as parameter, returns nothing. islookup is true then it will delete the data from the table before inserting the data to the table. by default it is true"""
-- MAGIC   sinkColumnNames = getColumns(f'{catalog_name}.{schema_name}.{table_name}')
-- MAGIC   sourceColumnNames = getColumns(temp_table)
-- MAGIC   insert_sql = (f"""INSERT INTO {catalog_name}.{schema_name}.{table_name} ({sinkColumnNames})
-- MAGIC              SELECT {sourceColumnNames},'I',Null,Null,CURRENT_TIMESTAMP(),current_user(),CURRENT_TIMESTAMP(),current_user() FROM {temp_table}""")
-- MAGIC   # print(insert_sql)
-- MAGIC   if islookup:
-- MAGIC     spark.sql(f'delete from {catalog_name}.{schema_name}.{table_name}')
-- MAGIC   spark.sql(insert_sql)
-- MAGIC     
-- MAGIC   # spark.sql

-- COMMAND ----------

-- MAGIC %python
-- MAGIC MERGE INTO {catalog_name}.{schema_name}.{table_name} AS target
-- MAGIC USING (SELECT {sourceColumnNames}, 'I' AS operation, NULL AS deleted_at, NULL AS deleted_by, CURRENT_TIMESTAMP() AS created_at, current_user() AS created_by, CURRENT_TIMESTAMP() AS updated_at, current_user() AS updated_by FROM {temp_table}) AS source
-- MAGIC ON target.id = source.id
-- MAGIC WHEN MATCHED THEN
-- MAGIC   UPDATE SET
-- MAGIC     {sinkColumnNames} = source.{sourceColumnNames},
-- MAGIC     xmd_ChangeOperationCode = source.operation,
-- MAGIC     xmd_DeleteDateTimestamp = source.deleted_at,
-- MAGIC     xmd_DeletetUserId = source.deleted_by,
-- MAGIC     xmd_InsertDateTimestamp = source.created_at,
-- MAGIC     xmd_InsertUserId = source.created_by,
-- MAGIC     xmd_UpdateDateTimestamp = source.updated_at,
-- MAGIC     xmd_UpdateUserId = source.updated_by
-- MAGIC WHEN NOT MATCHED THEN
-- MAGIC   INSERT ({sinkColumnNames})
-- MAGIC   VALUES (source.{sourceColumnNames}, source.operation, source.deleted_at, source.deleted_by, source.created_at, source.created_by, source.updated_at, source.updated_by);

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### initialize actual and fully qualified path for the day and get all files available under that fully qualified path

-- COMMAND ----------

-- MAGIC %python
-- MAGIC constant_path = 'abfss://landing@adlsdidevcus.dfs.core.windows.net/fha'
-- MAGIC todays_path = getTodaysPath(constant_path)
-- MAGIC print(todays_path)
-- MAGIC file_list = getFileList(todays_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_list

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Segregating catalog, schema and table name and call insertIntoTable function

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # file_list
-- MAGIC for file in file_list:
-- MAGIC   schema_name = file.split('_')[0]
-- MAGIC   table_name = file.split('.')[0].split(f'{schema_name}_')[1]
-- MAGIC   catalog_name = 'bronze_dev'
-- MAGIC   
-- MAGIC   sql_query = f"SELECT * FROM `parquet`.`{todays_path}/{file}` WITH (CREDENTIAL `acdb-unity-access-connector-dev-cus`)"
-- MAGIC   temp_table = f"temp_{file[:-8]}"
-- MAGIC   print(temp_table)
-- MAGIC   spark.sql(sql_query).createOrReplaceTempView(temp_table)
-- MAGIC   print(f'{catalog_name}.{schema_name}.{table_name} -- {temp_table} - {sql_query}')
-- MAGIC   try:
-- MAGIC     print(f'{schema_name}.{table_name}: Data Load Initiated')
-- MAGIC     insertIntoTable(temp_table, catalog_name, schema_name, table_name)
-- MAGIC     print(f'{schema_name}.{table_name}: Data Load Completed')
-- MAGIC   except Exception as e:
-- MAGIC     print(f'{schema_name}.{table_name}: Data Load Failed --> {e}')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### code debug

-- COMMAND ----------

-- MAGIC %python
-- MAGIC temp_table = 'temp_CBR16_XOME_CLOSING_COST'
-- MAGIC schema_name = 'CBR16'
-- MAGIC table_name = 'XOME_CLOSING_COST'
-- MAGIC sinkColumnNames = getColumns(f'bronze_dev.{schema_name}.{table_name}')
-- MAGIC sourceColumnNames = getColumns(temp_table)
-- MAGIC insert_sql = (f"""INSERT INTO {catalog_name}.{schema_name}.{table_name} ({sinkColumnNames})
-- MAGIC             SELECT {sourceColumnNames},'I',Null,current_user(),CURRENT_TIMESTAMP(),current_user(),CURRENT_TIMESTAMP(),current_user() FROM {temp_table}""")
-- MAGIC print(insert_sql)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.sql(f"SELECT {sourceColumnNames} FROM {temp_table}")
-- MAGIC display(df)

-- COMMAND ----------

-- -- Alter table bronze_dev.CBR18.HUD_CAFMV alter column ValueEndAmount decimal(12,0) null;
-- ALTER TABLE bronze_dev.CBR16.XOME_TITLE_CURE CHANGE COLUMN TitleGradeTypeCode DROP NOT NULL;
-- delete from bronze_dev.CBR18.HUD_CAFMV
-- select * from bronze_dev.CBR18.HUD_CAFMV
--FED_DEBENTURE_RATE - data column mismatch
--USFN_EVICTION - data column mismatch

-- COMMAND ----------

INSERT INTO silver_dev.cba14.fha_user
SELECT
    Id,
    xmd_InsertDateTimestamp AS src_xmd_InsertDateTimestamp,
    xmd_InsertUserId AS src_xmd_InsertUserId,
    xmd_UpdateDateTimestamp AS src_xmd_UpdateDateTimestamp,
    xmd_UpdateUserId AS src_xmd_UpdateUserId,
    xmd_DeleteDateTimestamp AS src_xmd_DeleteDateTimestamp,
    xmd_DeleteUserId AS src_xmd_DeleteUserId,
    xmd_ChangeOperationCode AS src_xmd_ChangeOperationCode,
    current_timestamp(),
    current_user(),
    null,
    null,
    null,
    null,
    i,
    DeleteInd,
    Email,
    FirstName,
    LastName,
    StatusId,
    LastLoginDateTimestamp,
    B2CUserId
FROM bronze_dev.cba14.fha_user

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC full_path=todays_path+'/'+file_list[1]
-- MAGIC table_name = file_list[1][6:-8].lower()
-- MAGIC print(table_name)
-- MAGIC table_schema=file_list[1][:5].lower()
-- MAGIC print(table_schema)
-- MAGIC column_list=spark.sql(f"select column_name from bronze_dev.information_schema.columns where table_name='{table_name}' and table_schema='{table_schema}' and is_nullable='NO'")
-- MAGIC
-- MAGIC display(column_list)
-- MAGIC # print(full_path)
-- MAGIC # df=spark.read.parquet(full_path)
-- MAGIC # display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for t_name in file_list:
-- MAGIC   table_name=t_name[6:-8].lower()
-- MAGIC   table_schema=t_name[:5].lower()
-- MAGIC   column_list=spark.sql(f"select column_name from bronze_dev.information_schema.columns where table_name='{t_name[6:-8]}' and table_schema='t_name[:6]' and is_nullable='NO'")
-- MAGIC   column_list=column_list.collect()
-- MAGIC   print(column_list)
-- MAGIC
-- MAGIC

-- COMMAND ----------

select column_name,is_nullable from bronze_dev.information_schema.columns where table_schema like 'cb%' and is_nullable ='NO'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sinkColumnNames=getColumns(f"bronze_dev.cbr16.xome_closing_cost")
-- MAGIC print(sinkColumnNames)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for col in sinkColumnNames:
-- MAGIC   if(col.startswith("src_")):
-- MAGIC     sinkColumnNames.remove(col)
-- MAGIC     print(col)
-- MAGIC