# Databricks notebook source
# MAGIC %run ../utils/generic_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting table_name and schema_name

# COMMAND ----------


dbutils.widgets.text("schema_table_name", "", "")
schema_table_name = dbutils.widgets.get("schema_table_name")
# Now you can split the schema_table_name if it's not empty
if schema_table_name:  # Check if the widget input is not empty
    (schema_name, table_name) = schema_table_name.split(".")
dbutils.widgets.text("Isincremental", "", "")
Isincremental = dbutils.widgets.get("Isincremental")
dbutils.widgets.text("Mergecolumns", "", "")
Mergecolumns = dbutils.widgets.get("Mergecolumns")
dbutils.widgets.text("Watermarkcolumn", "", "")
Watermarkcolumn = dbutils.widgets.get("Watermarkcolumn")



# COMMAND ----------

# df=spark.sql(f"select table_schema, table_name from silver_dev.information_schema.tables where table_schema like 'cb%' ")

# COMMAND ----------

# schema_table_name_list = [tuple(row) for row in df.collect()]

# COMMAND ----------

# print(len(schema_table_name_list))

# COMMAND ----------

# for schema_name, table_name in schema_table_name_list:
#   bronze_catalog_name = "bronze_dev"
#   silver_catalog_name = "silver_dev"
#   src_table = f"{bronze_catalog_name}.{schema_name}.{table_name}"
#   try:
#     print(f'{schema_name}.{table_name}: Data Load Initiated')
#     insertIntoTable(src_table,silver_catalog_name,schema_name,table_name)
#     print(f'{schema_name}.{table_name}: Data Load Completed')
#   except Exception as e:
#     print(f'{schema_name}.{table_name}: Data Load Failed --> {e}')

# COMMAND ----------

import sys
dic_status_and_count = {'load_status': 'Success', 'source_record_count': 0, 'target_record_count': 0, 'error_type': '', 'error_message': ''}
bronze_catalog_name = "bronze_dev"
silver_catalog_name = "silver_dev"
# sql_query = f"select * from {bronze_catalog_name}.{schema_name}.{table_name}"
#df = spark.sql(sql_query)
src_table = f"{bronze_catalog_name}.{schema_name}.{table_name}"

try:
  # spark.sql(sql_query).createOrReplaceTempView(temp_table)
  source_datacount = spark.sql(f'select count(*) from {src_table}')
  dic_status_and_count['source_record_count'] = source_datacount.first()[0]
  print(f'{schema_name}.{table_name}: Data Load Initiated')
  source_datacount = spark.sql(f'select count(*) from {src_table}')
  record_status = insertIntoTable(src_table,silver_catalog_name,schema_name,table_name)
  dic_status_and_count['target_record_count'] = record_status.first()[1]
  #dic_status_and_count['load_duration']
  print(f'{schema_name}.{table_name}: Data Load Completed')
except Exception as e:
  print(f'{schema_name}.{table_name}: Data Load Failed --> {e}')
  exctype, value = sys.exc_info()[:2]
  print(f'{exctype}===>>{value}')
  dic_status_and_count['load_status'] = 'Failed'
  dic_status_and_count['target_record_count'] = record_status.first()[1]
  dic_status_and_count['error_type'] = exctype
  dic_status_and_count['error_message'] = f'{schema_name}.{table_name}: Data Load Failed --> {value}'
finally:
  dbutils.notebook.exit(dic_status_and_count)

# COMMAND ----------

# MAGIC %md
# MAGIC # TO DO THINGS

# COMMAND ----------

# MAGIC %md
# MAGIC ### implement_Incremental_Load:

# COMMAND ----------

# MAGIC %md
# MAGIC ### decimal_Column_typecasting:

# COMMAND ----------

# MAGIC %md
# MAGIC # DEBUG

# COMMAND ----------

spark.sql(f"select * from {catalog_name}.{schema_name}.{table_name}")

# COMMAND ----------

# columns,clounmlist=get_column_list(schema_name,table_name)
# print(columns)
# print(clounmlist)


# COMMAND ----------

#df.createOrReplaceTempView(temp_table)
# try:
#     spark.sql(sql_query).createOrReplaceTempView(temp_table)
#     source_datacount = spark.sql(f'select count(*) from {temp_table}')
#     print(f"total records in source table: {source_datacount}")
#     print(f'{schema_name}.{table_name}: Data Load Initiated')
#     insert_into_silver(temp_table, table_name, schema_name)
#     target_datacount = spark.sql(f'select count(*) from {catalog_name}.{schema_name}.{table_name}')
#     print(f"total records in target table: {target_datacount}")
#     print(f'{schema_name}.{table_name}: Data Load Completed')
# except Exception as e:
#     print(f'{schema_name}.{table_name}: Data Load Failed---->due to {e}')