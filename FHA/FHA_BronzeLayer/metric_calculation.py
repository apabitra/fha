# Databricks notebook source
# MAGIC %run /Workspace/DAX/FHA/utils/metric_calculation_utils

# COMMAND ----------

dbutils.widgets.text("Catalog_name", "", "")
Catalog_name = dbutils.widgets.get("Catalog_name")







# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare quality metric for Delta tables using DBSQL(Databricks Data Quality Library)

# COMMAND ----------

def insert_into_bronze_metadata_log(schema_name, table_name,Catalog_name):
    print("****************************************************************************")
    print(f"Initiating metadata calculation for {schema_name}.{table_name}")
    print("****************************************************************************")
    df_table_data = spark.sql(f"SELECT * FROM {Catalog_name}.{schema_name}.{table_name}")
    if df_table_data.count() == 0:
        print(f"No data found in {schema_name}.{table_name}")
        return
    df_active_column = spark.sql(f'''
        SELECT column_name 
        FROM {Catalog_name}.aa4.bronze_metadata 
        WHERE table_name='{table_name}' 
        AND Is_Active=true
    ''')
    active_column_list = [row.column_name for row in df_active_column.collect()]
    
    
    list(map(lambda col: process_column(col, active_column_list, df_table_data, schema_name, table_name, Catalog_name), df_table_data.dtypes))

# COMMAND ----------


def process_column(column_tuple, active_column_list, df_table_data, schema_name, table_name, Catalog_name):
    result_list=[]

    column, column_datatype = column_tuple
    if column not in active_column_list:
        return
    meta_data_id = spark.sql(f'''
        SELECT id 
        FROM {Catalog_name}.aa4.bronze_metadata
        WHERE table_name='{table_name}' 
        AND column_name='{column}'
    ''').collect()[0][0]
    if column_datatype not in ['datetime', 'boolean', 'string', 'date', 'timestamp']:
        metric_type = "numeric"
        
        result=calculate_numeric_metric_value(df_table_data, column)
    elif column_datatype in ['timestamp', 'date']:
        metric_type = "timestamp"
        # print(f"Initiating {metric_type} metric calculation for {schema_name}.{table_name}.{column}")
        result=calculate_timestamp_metric_value(df_table_data, column)
    else:
        metric_type = "categorical"
        # print(f"Initiating {metric_type} metric calculation for {schema_name}.{table_name}.{column}")
        result=calculate_string_metric_value(df_table_data, column)
    for result_name, result_value in result.items():
        result_list.append([meta_data_id, result_name, result_value])
    df = spark.createDataFrame(result_list, schema="Metadata_ID Integer, Metric_Name string, Metric_Value string")
    temp_table=f'temp_table_{table_name}_{column}'
    df.createOrReplaceTempView(temp_table)
    print('--------------------------------------------------------------------------------------')
    print(f"Initiating {metric_type} metric calculation for {schema_name}.{table_name}.{column}")
    spark.sql(f"INSERT INTO {Catalog_name}.aa4.bronze_metadata_log (Metadata_id, Metric_Name, Metric_Value, xmd_InsertDateTimestamp, xmd_InsertUserId, xmd_UpdateDateTimestamp, xmd_UpdateUserId, xmd_DeleteDateTimestamp, xmd_DeleteUserId, xmd_ChangeOperationCode) SELECT Metadata_id, Metric_Name, Metric_Value,'{datetime.now()}', 'databricks', '{datetime.now()}', 'databricks', NULL, NULL, 'I' FROM {temp_table}")
    print(f"Completed {metric_type} metric calculation for {schema_name}.{table_name}.{column}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### For calculate Quality metric value for all table

# COMMAND ----------

spark.sql(f"truncate table {Catalog_name}.aa4.bronze_metadata_log")

# COMMAND ----------



df_table_info=spark.sql(f"SELECT schema_name, table_name FROM {Catalog_name}.aa4.bronze_metadata where Is_Active = true GROUP BY schema_name, table_name ")
schema_table_list=[(row.schema_name, row.table_name) for row in df_table_info.collect()]
for schema_name,table_name in schema_table_list:
  insert_into_bronze_metadata_log(schema_name,table_name,Catalog_name)