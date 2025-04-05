-- Databricks notebook source
-- MAGIC %run /Workspace/DAX/FHA/utils/generic_functions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Creating widgets for leveraging parameters, and printing the parameters
-- MAGIC
-- MAGIC dbutils.widgets.text("file_name", "","")
-- MAGIC dbutils.widgets.text("IsIncremental", "","")
-- MAGIC
-- MAGIC dbutils.widgets.text("todays_date", "","")
-- MAGIC dbutils.widgets.text("MergeColumns", "","")
-- MAGIC dbutils.widgets.text("StorageAccount", "","")
-- MAGIC dbutils.widgets.text("ContainerName", "","")
-- MAGIC dbutils.widgets.text("RelativePath", "","")
-- MAGIC dbutils.widgets.text("CatalogName", "","")
-- MAGIC dbutils.widgets.text("ManagedIdentityConnector", "","")
-- MAGIC full_file_name = dbutils.widgets.get("file_name")
-- MAGIC isIncremental = dbutils.widgets.get("IsIncremental")
-- MAGIC
-- MAGIC todays_date = dbutils.widgets.get("todays_date")
-- MAGIC MergeColumns = dbutils.widgets.get("MergeColumns")
-- MAGIC StorageAccount = dbutils.widgets.get("StorageAccount")
-- MAGIC ContainerName=dbutils.widgets.get("ContainerName")
-- MAGIC RelativePath=dbutils.widgets.get("RelativePath")
-- MAGIC CatalogName=dbutils.widgets.get("CatalogName")
-- MAGIC ManagedIdentityConnector=dbutils.widgets.get("ManagedIdentityConnector")
-- MAGIC print ("Param -\'file_name':")
-- MAGIC print (full_file_name)
-- MAGIC print ("Param -\'IsIncremental':")
-- MAGIC print (isIncremental)
-- MAGIC
-- MAGIC print ("Param - \'todays_date':")
-- MAGIC print (todays_date)
-- MAGIC print ("Param - \'MergeColumns':")
-- MAGIC print (MergeColumns)
-- MAGIC print ("Param - \'StorageAccount':")
-- MAGIC print (StorageAccount)
-- MAGIC print ("Param - \'ContainerName':")
-- MAGIC print (ContainerName)
-- MAGIC print ("Param - \ 'RelativePath':")
-- MAGIC print (RelativePath)
-- MAGIC print ("Param - \ 'CatalogName':")
-- MAGIC print (ManagedIdentityConnector)
-- MAGIC print ("Param - \ 'ManagedIdentityConnector':")
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storage_account = StorageAccount.split('//')[1]
-- MAGIC
-- MAGIC relative_path = RelativePath.split('/')[1]
-- MAGIC
-- MAGIC full_path=storage_account +  relative_path
-- MAGIC print(full_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC catalog_name =  CatalogName
-- MAGIC print(catalog_name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import date,datetime

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function to get all files under given path

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function to get actual fully qualified path for the run date

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function to get all columns of a table(comma seperated , prefix optional)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function to build merge sql query , it accepts target table , source table, merge column comma seperated column as string and watermark colmn

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Function to insert data into delta table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### initialize actual and fully qualified path for the day and get all files available under that fully qualified path

-- COMMAND ----------

-- MAGIC %python
-- MAGIC constant_path = f"abfss://{ContainerName}@{full_path}"
-- MAGIC
-- MAGIC todays_path = f"{constant_path}/{todays_date}"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Segregating catalog, schema and table name and call insertIntoTable function

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import sys
-- MAGIC dic_status_and_count = {'load_status': 'Success', 'source_record_count': 0, 'target_record_count': 0, 'error_type': '', 'error_message': ''}
-- MAGIC file = full_file_name
-- MAGIC schema_name = file.split('_')[0]
-- MAGIC table_name = file.split('.')[0].split(f'{schema_name}_')[1]
-- MAGIC catalog_name =  CatalogName
-- MAGIC df=spark.sql(f"SELECT * FROM `parquet`.`{todays_path}/{file}` WITH (CREDENTIAL `{ManagedIdentityConnector}`)")
-- MAGIC
-- MAGIC src_count = df.count()
-- MAGIC if src_count == 0:
-- MAGIC   try:
-- MAGIC     dic_status_and_count['load_status'] = 'Success'
-- MAGIC     dic_status_and_count['source_record_count'] = 0
-- MAGIC     dic_status_and_count['target_record_count'] = 0
-- MAGIC     dic_status_and_count['error_type'] = 'No Data'
-- MAGIC     dic_status_and_count['error_message'] = 'No Data'
-- MAGIC   
-- MAGIC   finally:
-- MAGIC     dbutils.notebook.exit(dic_status_and_count)
-- MAGIC   
-- MAGIC else: 
-- MAGIC   temp_table = f"temp_{file[:-8]}"
-- MAGIC   # print(f'{catalog_name}.{schema_name}.{table_name} -- {temp_table} - {sql_query}')
-- MAGIC   try:
-- MAGIC     if isIncremental.lower() == 'false':
-- MAGIC       spark.sql(f"truncate table {catalog_name}.{schema_name}.{table_name}")
-- MAGIC     df.createOrReplaceTempView(temp_table)
-- MAGIC
-- MAGIC     source_datacount = spark.sql(f'select count(*) from {temp_table}')
-- MAGIC     
-- MAGIC     dic_status_and_count['source_record_count'] = source_datacount.first()[0]
-- MAGIC
-- MAGIC     print(f'{schema_name}.{table_name}: Data Load Initiated')
-- MAGIC     
-- MAGIC     record_status = insertIntoTable(temp_table, catalog_name, schema_name, table_name)
-- MAGIC     # target_datacount = spark.sql(f'select count(*) from {catalog_name}.{schema_name}.{table_name}')
-- MAGIC     dic_status_and_count['target_record_count'] = record_status.first()[0]
-- MAGIC     # dic_status_and_count['load_duration']
-- MAGIC     print(f'{schema_name}.{table_name}: Data Load/Update Completed')
-- MAGIC   except Exception as e:
-- MAGIC     # print(f'{schema_name}.{table_name}: Data Load Failed --> {e}')
-- MAGIC     exctype, value = sys.exc_info()[:2]
-- MAGIC     print(f'{exctype}===>>{value}')
-- MAGIC     dic_status_and_count['load_status'] = 'Failed'
-- MAGIC     # dic_status_and_count['target_record_count'] = record_status.first()[1]
-- MAGIC     dic_status_and_count['error_type'] = exctype
-- MAGIC     dic_status_and_count['error_message'] = f'{schema_name}.{table_name}: Data Load Failed'
-- MAGIC   finally:
-- MAGIC     dbutils.notebook.exit(dic_status_and_count)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(record_status)