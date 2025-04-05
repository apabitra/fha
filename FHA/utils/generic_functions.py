# Databricks notebook source
def getColumns(tableName,prfix = ''):
  """getColumns expects a table name and returns all columns of the given table comma sepearted string type"""
  df_column = spark.sql(f"show columns in {tableName}")
  list_column = list(df_column.select('col_name').toPandas()['col_name'])
  list_column.sort()
  str_column = f",{prfix}".join(list_column)
  return f"{prfix}{str_column}"

# COMMAND ----------

def contains_src(column_list_str):
  column_list = column_list_str.split(",")
  return any("src_" in col for col in column_list)

# COMMAND ----------

# def removeColumns(keyword, prefix_or_suffix, column_list_str):
#   column_list = column_list_str.split(",")
#   # return column_list
#   new_column_list = [x for x in column_list if not (keyword in x and prefix_or_suffix == 'prefix' or keyword in x and prefix_or_suffix == 'suffix')]
#   new_column_list.sort()
#   return ",".join(new_column_list)

# COMMAND ----------

def removeColumns(keyword,  column_list_str):
  column_list = column_list_str.split(",")
  # return column_list
  new_column_list = [x for x in column_list if not x.startswith(keyword)]
  new_column_list.sort()
  return ",".join(new_column_list)

# COMMAND ----------

# def insertIntoTable(src_table, catalog_name, schema_name, table_name,islookup=True):
#   """insertIntoTable temp_table, catalog_name, schema_name, table_name,islookup as parameter, returns nothing. islookup is true then it will delete the data from the table before inserting the data to the table. by default it is true"""
#   sinkColumnNames = getColumns(f'{catalog_name}.{schema_name}.{table_name}')
#   sourceColumnNames = getColumns(src_table)
#   if contains_src(sourceColumnNames):
#     str_sourceColumnNames =removeColumns('xmd_',sourceColumnNames) 
#   else :
#     str_sourceColumnNames = sourceColumnNames
#   # str_sourceColumnNames =removeColumns('src_','prefix',sourceColumnNames)
#   insert_sql = (f"""INSERT INTO {catalog_name}.{schema_name}.{table_name} ({sinkColumnNames})
#              SELECT {str_sourceColumnNames},'I',Null,Null,CURRENT_TIMESTAMP(),current_user(),CURRENT_TIMESTAMP(),current_user() FROM {src_table}""")
#   # print(insert_sql)
#   if islookup:
#     spark.sql(f'truncate table {catalog_name}.{schema_name}.{table_name}')
#   # else:
#   return(spark.sql(insert_sql))

# COMMAND ----------

def insertIntoTable(temp_table, catalog_name, schema_name, table_name, isIncremental=True):
  """insertIntoTable temp_table, catalog_name, schema_name, table_name,islookup as parameter, returns nothing. islookup is true then it will delete the data from the table before inserting the data to the table. by default it is true"""
  return_val = ""
  targetTableName = f'{catalog_name}.{schema_name}.{table_name}'
  sourceTableName = temp_table
  sinkColumnNames = getColumns(targetTableName)
  sourceColumnNames = getColumns(sourceTableName)
    # print(insert_sql)

  print('Incremental Merge here')
  merge_sql = build_merge_query(targetTableName,sourceTableName,MergeColumns)
    # print(merge_sql)
  return_val = spark.sql(merge_sql)
  return return_val

# COMMAND ----------

def build_merge_query(targetTable, sourceTable, mergeColumns):
  merge_columns = mergeColumns.split(',')
  merge_query = f"MERGE INTO {targetTable} as T using {sourceTable} as S on T.{mergeColumns} = S.{mergeColumns} WHEN MATCHED THEN UPDATE SET"
  column_list=getColumns(sourceTable)
  column_list = list(column_list.split(','))
  counter = 0
  repeat_time = len(column_list)
  for column in column_list:
    if column.split('_')[0] == 'xmd':
      t_column=f'src_{column}'
    else:
      t_column=column
    if (counter == repeat_time-1):
      merge_query = f"{merge_query} T.{t_column} = S.{column}"
    else:
      merge_query = f"{merge_query} T.{t_column} = S.{column}, "
    counter = counter + 1
  merge_query = f"{merge_query} ,T.xmd_ChangeOperationCode = 'U',T.xmd_UpdateUserId = current_user(),T.xmd_UpdateDateTimestamp = CURRENT_TIMESTAMP()"

  sinkColumnNames = getColumns(targetTable,'T.')
  # sinkColumnNames = f'T.{sinkColumnNames}'
  sourceColumnNames = getColumns(sourceTable,'S.')
  # sourceColumnNames = f'S.{sourceColumnNames}'
  merge_query = f"{merge_query} WHEN NOT MATCHED THEN INSERT ({sinkColumnNames}) values ({sourceColumnNames},'I',Null,Null,CURRENT_TIMESTAMP(),current_user(),CURRENT_TIMESTAMP(),current_user())"
  # print(merge_query)
  return merge_query