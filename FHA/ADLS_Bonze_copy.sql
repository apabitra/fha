-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Function to get all files under given path

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

-- MAGIC %md
-- MAGIC ### initialize actual and fully qualified path for the day and get all files available under that fully qualified path

-- COMMAND ----------

-- MAGIC %python
-- MAGIC constant_path = 'abfss://landing@adlsdidevcus.dfs.core.windows.net/fha'
-- MAGIC todays_path = getTodaysPath(constant_path)
-- MAGIC # print(todays_path)
-- MAGIC file_list = getFileList(todays_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_list

-- COMMAND ----------



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
-- MAGIC   spark.sql(sql_query).createOrReplaceTempView(temp_table)
-- MAGIC   # print(f'{catalog_name}.{schema_name}.{table_name} -- {temp_table} - {sql_query}')
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


-- %python
-- temp_table = 'temp_cbr20_USFN_EVICTION'
-- schema_name = 'cbr20'
-- table_name = 'USFN_EVICTION'
-- sinkColumnNames = getColumns(f'bronze_dev.{schema_name}.{table_name}')
-- sourceColumnNames = getColumns(temp_table)
-- insert_sql = (f"""INSERT INTO {catalog_name}.{schema_name}.{table_name} ({sinkColumnNames})
--               SELECT {sourceColumnNames},'I',Null,current_user(),CURRENT_TIMESTAMP(),current_user(),CURRENT_TIMESTAMP(),current_user() FROM {temp_table}""")
-- print(insert_sql)

-- COMMAND ----------

-- -- Alter table bronze_dev.CBR18.HUD_CAFMV alter column ValueEndAmount decimal(12,0) null;
-- ALTER TABLE bronze_dev.CBR16.XOME_TITLE_CURE CHANGE COLUMN TitleGradeTypeCode DROP NOT NULL;
-- delete from bronze_dev.CBR18.HUD_CAFMV
-- select * from bronze_dev.CBR18.HUD_CAFMV
--FED_DEBENTURE_RATE - data column mismatch
--USFN_EVICTION - data column mismatch

-- COMMAND ----------

SELECT * FROM bronze_dev.cba14.fha_user

-- COMMAND ----------

