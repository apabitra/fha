# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as f
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


# COMMAND ----------

def calculate_string_metric_value(df,column):
  return{
    'Count':str(df.agg(f.count(f.col(column))).collect()[0][0]),
    'Sparseness':str(df.agg(f.count(f.when(f.col(column).isNull(),1))).collect()[0][0]/df.count()),
    'Missing Value Count':str(df.filter(f.col(column).isNull()).count()),
    'Unique Value Count':str(df.agg(f.countDistinct(f.col(column))).collect()[0][0]),
    'Mode':str(df.agg(f.mode(f.col(column))).collect()[0][0]),
    'Mode Frequency':str(df.filter(f.col(column)==df.agg(f.mode(f.col(column))).collect()[0][0]).count())
  }
 

# COMMAND ----------



def calculate_numeric_metric_value(df,column):
   
  
  if  df.count() == 1:
      return {
        "Mean":None,
        "Standard Deviation":None,
        "Minimum":None,
        "Maximum":None,
        "Range":None,
        "Variance":None,
        "Skewness":None,
        "Kurtosis":None,
        "Sum":None,
        "Missing Value Count":None,
        "Unique Value Count":None,
        "Zero Count":None,
        "Negative Value Count":None,
        "Mode":None,
        "Median":None,
        "Sparseness":None
        }
  else :
      count=round(df.agg(f.count(f.col(column))).collect()[0][0],3)
      Maximun=round(df.agg(f.max(f.col(column))).collect()[0][0],3)
      Minimun=round(df.agg(f.min(f.col(column))).collect()[0][0],3)
      Range=Maximun-Minimun
      Variance=round(df.agg(f.var_pop(f.col(column))).collect()[0][0],3)
      Skewness=df.agg(f.skewness(f.col(column))).collect()[0][0]
      Kurtosis=df.agg(f.kurtosis(f.col(column))).collect()[0][0]
      Sum=round(df.agg(f.sum(f.col(column))).collect()[0][0],3)
      Missing_Value_Count=df.filter(f.col(column).isNull()).count()
      Unique_Value_Count=df.agg(f.countDistinct(f.col(column))).collect()[0][0]
      Zero_Count=df.filter(f.col(column)==0).count()
      Negative_Value_Count=df.filter(f.col(column)<0).count()
      Median=df.agg(f.percentile_approx(f.col(column), 0.5)).collect()[0][0]
      Mode=df.agg(f.mode(f.col(column))).collect()[0][0]
      Mean=round(df.agg(f.mean(f.col(column))).collect()[0][0],3)
      standard_deviation=round(df.agg(f.stddev(f.col(column))).collect()[0][0],3)
      Sparseness=df.agg(f.count(f.when(f.col(column)==0,1))).collect()[0][0]/df.count() 
      return {
         "Count":str(count),
         "Mean":str(Mean),
         "Standard Deviation":str(standard_deviation) ,
         "Minimum":str(Minimun),
         "Maximum":str(Maximun),
         "Range":str(Range) ,
         "Variance":str(Variance) ,
         "Skewness":str( Skewness),
         "Kurtosis":str( Kurtosis),
         "Sum":str(Sum) ,
         "Missing Value Count": str(Missing_Value_Count),
         "Unique Value Count":str(Unique_Value_Count) ,
         "Zero Count":str(Zero_Count) ,
         "Negative Value Count":str(Negative_Value_Count) ,
         "Mode":str(Mode) ,
         "Median":str(Median),
         "Sparseness":str(Sparseness)
      }
      


# COMMAND ----------


def calculate_timestamp_metric_value(df,column):
  if df.agg(f.count(f.when(f.col(column).isNull(),1))).collect()[0][0]/df.count() == 1:
    return {
    'Sparseness':str(df.agg(f.count(f.when(f.col(column).isNull(),1))).collect()[0][0]/df.count()),
    'Minimum':None,
    'Maximum':None,
    'Range':None,
    'Missing Value Count':str(df.filter(f.col(column).isNull()).count()),
    'Unique Value Count':None,
    'Mode':None,
    'Median':None
    }
  else:
    return {
    'Sparseness':str(df.agg(f.count(f.when(f.col(column).isNull(),1))).collect()[0][0]/df.count()),
    'Mininum':str(df.agg(f.min(f.col(column))).collect()[0][0]),
    'Maximum':str(df.agg(f.max(f.col(column))).collect()[0][0]),
    'Range':str(df.agg(f.max(f.col(column))).collect()[0][0]-df.agg(f.min(f.col(column))).collect()[0][0]),
    'Missing Value Count':str(df.filter(f.col(column).isNull()).count()),
    'Unique Value Count':str(df.agg(f.countDistinct(f.col(column))).collect()[0][0]),
    'Mode':str(df.agg(f.mode(f.col(column))).collect()[0][0]),
    'Median':str(df.agg(f.percentile_approx(f.col(column), 0.5)).collect()[0][0])
  }
  

# COMMAND ----------


# def process_column(column_tuple, active_column_list, df_table_data, schema_name, table_name):
#     result_list=[]

#     column, column_datatype = column_tuple
#     if column not in active_column_list:
#         return
#     meta_data_id = spark.sql(f'''
#         SELECT id 
#         FROM bronze_dev.aa4.bronze_metadata
#         WHERE table_name='{table_name}' 
#         AND column_name='{column}'
#     ''').collect()[0][0]
#     if column_datatype not in ['datetime', 'boolean', 'string', 'date', 'timestamp']:
#         metric_type = "numeric"
        
#         result=calculate_numeric_metric_value(df_table_data, column)
#     elif column_datatype in ['timestamp', 'date']:
#         metric_type = "timestamp"
#         # print(f"Initiating {metric_type} metric calculation for {schema_name}.{table_name}.{column}")
#         result=calculate_timestamp_metric_value(df_table_data, column)
#     else:
#         metric_type = "categorical"
#         # print(f"Initiating {metric_type} metric calculation for {schema_name}.{table_name}.{column}")
#         result=calculate_string_metric_value(df_table_data, column)
#     for result_name, result_value in result.items():
#         result_list.append([meta_data_id, result_name, result_value])
#     df = spark.createDataFrame(result_list, schema="Metadata_ID Integer, Metric_Name string, Metric_Value string")
#     temp_table=f'temp_table_{table_name}_{column}'
#     df.createOrReplaceTempView(temp_table)
#     print('--------------------------------------------------------------------------------------')
#     print(f"Initiating {metric_type} metric calculation for {schema_name}.{table_name}.{column}")
#     spark.sql(f"INSERT INTO bronze_dev.aa4.bronze_metadata_log (Metadata_id, Metric_Name, Metric_Value, xmd_InsertDateTimestamp, xmd_InsertUserId, xmd_UpdateDateTimestamp, xmd_UpdateUserId, xmd_DeleteDateTimestamp, xmd_DeleteUserId, xmd_ChangeOperationCode) SELECT Metadata_id, Metric_Name, Metric_Value,'{datetime.now()}', 'databricks', '{datetime.now()}', 'databricks', NULL, NULL, 'I' FROM {temp_table}")
#     print(f"Completed {metric_type} metric calculation for {schema_name}.{table_name}.{column}")


# COMMAND ----------

# def insert_into_bronze_metadata_log(schema_name, table_name):
#     print("****************************************************************************")
#     print(f"Initiating metadata calculation for {schema_name}.{table_name}")
#     print("****************************************************************************")
#     df_table_data = spark.sql(f"SELECT * FROM bronze_dev.{schema_name}.{table_name}")
#     if df_table_data.count() == 0:
#         print(f"No data found in {schema_name}.{table_name}")
#         return
#     df_active_column = spark.sql(f'''
#         SELECT column_name 
#         FROM bronze_dev.aa4.bronze_metadata 
#         WHERE table_name='{table_name}' 
#         AND Is_Active=true
#     ''')
#     active_column_list = [row.column_name for row in df_active_column.collect()]
    
    
#     list(map(lambda col: process_column(col, active_column_list, df_table_data, schema_name, table_name), df_table_data.dtypes))

# COMMAND ----------


# def generate_html(pivoted_data):
#     html_content = """
#     <style>
#         .card {
#             border: 1px solid #ccc;
#             border-radius: 8px;
#             padding: 16px;
#             margin: 8px;
#             display: grid;
#             grid-template-columns: repeat(2, auto);
#             vertical-align: top;
#             width: 100%;
#             box-shadow: 2px 2px 12px #aaa;
#         }
#         .metric-name {
#             font-weight: bold;
#             color: #2a9d8f;
#         }
#         .metric-value {
#             color: #e76f51;
#         }
#     </style>
#     <div>
#     """
#     for row in pivoted_data.collect():
#         html_content += f"""
#         <div class="card">
#             <div><strong>Schema Name:</strong> {row['schema_name']}</div>
#             <div><strong>Table Name:</strong> {row['table_name']}</div>
#             <div><strong>Column Name:</strong> {row['column_name']}</div>
#             <div><strong>Data Type:</strong> {row['DATA_TYPE']}</div>
#             <div><strong>Metrics:</strong></div>
#         """
#         for metric, value in row.asDict().items():
#             if metric not in ['schema_name', 'table_name', 'column_name', 'DATA_TYPE']:
#                 html_content += f"""
#                 <div>
#                     <span class="metric-name">{metric}:</span> 
#                     <span class="metric-value">{value}</span>
#                 </div>
#                 """
#         html_content += "</div>"
#     html_content += "</div>"
#     return html_content



# COMMAND ----------

# def display_metrics(schema_name, table_name):
#     get_metrics(schema_name, table_name)
#     display(get_metrics(schema_name, table_name))
#     df_pivoted = get_metrics(schema_name, table_name)
#     html_content = generate_html(df_pivoted)
#     displayHTML(html_content)

# COMMAND ----------

# from concurrent.futures import ThreadPoolExecutor

# def insert_into_bronze_metadata_log_parallel(schema_name, table_name):
#     print("****************************************************************************")
#     print(f"Initiating metadata calculation for {schema_name}.{table_name}")
#     print("****************************************************************************")
#     df_table_data = spark.sql(f"SELECT * FROM bronze_dev.{schema_name}.{table_name}")
#     if df_table_data.count() == 0:
#         print(f"No data found in {schema_name}.{table_name}")
#         return
#     df_active_column = spark.sql(f'''
#         SELECT column_name 
#         FROM bronze_dev.cba14.bronze_metadata 
#         WHERE table_name='{table_name}' 
#         AND Is_Active=true
#     ''')
#     active_column_list = [row.column_name for row in df_active_column.collect()]
    
#     def process_column(column_tuple):
#         column, column_datatype = column_tuple
#         if column not in active_column_list:
#             return
        
#         meta_data_id = spark.sql(f'''
#             SELECT id 
#             FROM bronze_dev.cba14.bronze_metadata 
#             WHERE table_name='{table_name}' 
#             AND column_name='{column}'
#         ''').collect()[0][0]
        
#         if column_datatype not in ['datetime', 'boolean', 'string', 'date', 'timestamp']:
#             metric_type = "numeric"
#             result = calculate_numeric_metric_value(df_table_data, column)
#         elif column_datatype in ['timestamp', 'date']:
#             metric_type = "timestamp"
#             result = calculate_timestamp_metric_value(df_table_data, column)
#         else:
#             metric_type = "categorical"
#             result = calculate_string_metric_value(df_table_data, column)
        
#         print(f"Initiating {metric_type} metric calculation for {schema_name}.{table_name}.{column}")
#         for result_name, result_value in result.items():
#             spark.sql(f'''
#                 INSERT INTO bronze_dev.cba14.bronze_metadata_log_v3
#                 (Metadata_id, Metric_Name, Metric_Value, xmd_InsertDateTimestamp, xmd_InsertUserId, xmd_UpdateDateTimestamp, xmd_UpdateUserId, xmd_DeleteDateTimestamp, xmd_DeleteUserId, xmd_ChangeOperationCode)
#                 VALUES
#                 ('{meta_data_id}', '{result_name}', '{result_value}', '{datetime.now()}', 'databricks', '{datetime.now()}', 'databricks', NULL, NULL, 'I')
#             ''')
        
#         print(f"Completed {metric_type} metric calculation for {schema_name}.{table_name}.{column}")
    
#     with ThreadPoolExecutor() as executor:
#         executor.map(process_column, df_table_data.dtypes)
        

# COMMAND ----------


# def insert_into_bronze_metadata_log_parallel(schema_name, table_name):
#     print("****************************************************************************")
#     print(f"Initiating metadata calculation for {schema_name}.{table_name}")
#     print("****************************************************************************")
#     df_table_data = spark.sql(f"SELECT * FROM bronze_dev.{schema_name}.{table_name}")
#     if df_table_data.count() == 0:
#         print(f"No data found in {schema_name}.{table_name}")
#         return
#     df_active_column = spark.sql(f'''
#         SELECT column_name 
#         FROM bronze_dev.cba14.bronze_metadata 
#         WHERE table_name='{table_name}' 
#         AND Is_Active=true
#     ''')
#     active_column_list = [row.column_name for row in df_active_column.collect()]
#     with ThreadPoolExecutor() as executor:
#         futures = [executor.submit(process_column, col, active_column_list, df_table_data, schema_name, table_name) for col in df_table_data.dtypes]
#         for future in futures:
#             future.result()