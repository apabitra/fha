# Databricks notebook source
dbutils.widgets.text("schema_table_name", "","")
dbutils.widgets.text("Isincremental", "","")
dbutils.widgets.text("Mergecolumns", "","")
dbutils.widgets.text("CatalogName", "","")
dbutils.widgets.text("source_catalog", "","")
schema_table_name=dbutils.widgets.get("schema_table_name")
isIncremental=dbutils.widgets.get("Isincremental")
MergeColumns=dbutils.widgets.get("Mergecolumns")
catalog_name=dbutils.widgets.get("CatalogName")
source_catalog=dbutils.widgets.get("source_catalog")
print ("Param -\'schema_table_name':")
print (schema_table_name)
print ("Param -\'isIncremental':")
print (isIncremental)
print ("Param -\'Mergecolumns':")
print (MergeColumns)
print ("Param -\'CatalogName':")
print (catalog_name)
print ("Param -\'source_catalog':")
print (source_catalog)


# COMMAND ----------

# MAGIC %run /Workspace/DAX/FHA/utils/generic_functions

# COMMAND ----------

schema_name,table_name=schema_table_name.split('.')
print (schema_name,table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define and assignment of waterMark columns

# COMMAND ----------

src_table = f"{source_catalog}.{schema_name}.{table_name}"
str_sourceColumnNames = removeColumns('xmd_', getColumns(src_table))

catalog_name = f"{catalog_name}"

if isIncremental == 'True':
    insert_DateTimeStamp = spark.sql(
        f"select max(src_xmd_InsertDateTimestamp) from {catalog_name}.{schema_name}.{table_name}"
    ).collect()[0][0]
    update_DateTimeStamp = spark.sql(
        f"select max(src_xmd_UpdateDateTimestamp) from {catalog_name}.{schema_name}.{table_name}"
    ).collect()[0][0]
    delete_DateTimeStamp = spark.sql(
        f"select max(src_xmd_DeleteDateTimestamp) from {catalog_name}.{schema_name}.{table_name}"
    ).collect()[0][0]

    if insert_DateTimeStamp is None:
        insert_DateTimeStamp = '1970-01-01 00:00:00'
    if update_DateTimeStamp is None:
        update_DateTimeStamp = '1970-01-01 00:00:00'
    if delete_DateTimeStamp is None:
        delete_DateTimeStamp = '1970-01-01 00:00:00'

    df = spark.sql(
        f"select {str_sourceColumnNames} from {src_table} where src_xmd_InsertDateTimestamp > '{insert_DateTimeStamp}' or src_xmd_UpdateDateTimestamp > '{update_DateTimeStamp}'"
    )
else:
    spark.sql(f" truncate table {catalog_name}.{schema_name}.{table_name}")
    data_count = spark.sql(f"select count(*) from {catalog_name}.{schema_name}.{table_name}").collect()[0][0]
    print(data_count)
    df = spark.sql(f"select {str_sourceColumnNames} from {src_table}")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## build queary for extract the updated and newly inserted record

# COMMAND ----------

import sys
dic_status_and_count = {'load_status': 'Success', 'source_record_count': 0, 'target_record_count': 0, 'error_type': '', 'error_message': ''}
src_count = df.count()
if src_count == 0:
    try:
        dic_status_and_count['load_status'] = 'Success'
        dic_status_and_count['source_record_count'] = 0
        dic_status_and_count['target_record_count'] = 0
        dic_status_and_count['error_type'] = 'No Data'
        dic_status_and_count['error_message'] = 'No Data'
    finally:
        dbutils.notebook.exit(dic_status_and_count)
else:
    temp_table = f"temp_{table_name}"
    print(f'{catalog_name}.{schema_name}.{table_name} -- {temp_table}')
    try:
        df.createOrReplaceTempView(temp_table)
        source_datacount = spark.sql(f'select count(*) from {temp_table}')
        dic_status_and_count['source_record_count'] = source_datacount.first()[0]
        print(f'{schema_name}.{table_name}: Data Load Initiated')
        record_status = insertIntoTable(temp_table, catalog_name, schema_name, table_name)
        # target_datacount = spark.sql(f'select count(*) from {catalog_name}.{schema_name}.{table_name}')
        dic_status_and_count['target_record_count'] = record_status.first()[0]
        print(f'{schema_name}.{table_name}: Data Load/Update Completed')
    except Exception as e:
        exctype, value = sys.exc_info()[:2]
        print(f'{exctype}===>>{value}')
        dic_status_and_count['load_status'] = 'Failed'
        dic_status_and_count['error_type'] = exctype
        dic_status_and_count['error_message'] = f'{schema_name}.{table_name}: Data Load Failed'
    finally:
        dbutils.notebook.exit(dic_status_and_count)