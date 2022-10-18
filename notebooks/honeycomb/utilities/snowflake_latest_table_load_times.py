# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## The purpose of this application is find tables based on the last load timestamp
# MAGIC 
# MAGIC ## Arguments
# MAGIC 
# MAGIC * storage_account - The ADLS storage account name. (Copy and paste from the portal). Mutually exclusive with connection_string
# MAGIC * access_key - The ADLS storage account name. (Copy and paste from the portal)
# MAGIC * connection_string - The ADLS connection string. (Copy and paste from portal). Mutually exclusive with storage_account
# MAGIC * control_table_name - The storage account control table name (default: ExtractDataEntities)
# MAGIC * database_name - The snowflake database name
# MAGIC * load_time - Search for all tables loaded before the provided timestamp: yyyy-MM-dd, defaults to current day at midnight
# MAGIC 
# MAGIC #### Dependencies
# MAGIC * azure-data-tables

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC 
# MAGIC dbutils.widgets.text('storage_account', '')
# MAGIC dbutils.widgets.text('access_key', '')
# MAGIC dbutils.widgets.text('connection_string', '')
# MAGIC dbutils.widgets.text('database_name', '')
# MAGIC dbutils.widgets.text('control_table_name', 'ExtractDataEntities')
# MAGIC dbutils.widgets.text('load_time', 'yyyy-MM-dd')

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC from datetime import date
# MAGIC from distutils.util import strtobool
# MAGIC 
# MAGIC 
# MAGIC storage_account = dbutils.widgets.get('storage_account')
# MAGIC access_key = dbutils.widgets.get('access_key')
# MAGIC connection_string = dbutils.widgets.get('connection_string')
# MAGIC control_table_name = dbutils.widgets.get('control_table_name')
# MAGIC database_name = dbutils.widgets.get('database_name')
# MAGIC load_time = dbutils.widgets.get('load_time')
# MAGIC load_time = date.today().strftime("%Y-%m-%d") if not load_time else load_time
# MAGIC 
# MAGIC 
# MAGIC if not connection_string and not (storage_account and access_key):
# MAGIC   raise ValueError('Missing required option: connection_string or storage_account/access_key')
# MAGIC   
# MAGIC if connection_string and (storage_account or access_key):
# MAGIC   raise ValueError('Options connection_string and storage_account/access_key are mutually exclusive')
# MAGIC   
# MAGIC if storage_account and not access_key:
# MAGIC   raise ValueError('Option access_key is required with storage_account')
# MAGIC   
# MAGIC if access_key and not storage_account:
# MAGIC   raise ValueError('Option storage_account is required with access_key')
# MAGIC   
# MAGIC if not control_table_name:
# MAGIC   raise ValueError('Missing required option: control_table_name')
# MAGIC   
# MAGIC if not database_name:
# MAGIC   raise ValueError('Missing required option: database_name')
# MAGIC     
# MAGIC print(f'storage_account: {storage_account}')
# MAGIC print(f'access_key: {access_key}')
# MAGIC print(f'connection_string: {connection_string}')
# MAGIC print(f'control_table_name: {control_table_name}')
# MAGIC print(f'database_name: {database_name}')
# MAGIC print(f'load_time: {load_time}')

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC from azure.core.credentials import AzureNamedKeyCredential
# MAGIC from azure.data.tables import TableServiceClient
# MAGIC 
# MAGIC 
# MAGIC table_service_client: TableServiceClient = None
# MAGIC if connection_string:
# MAGIC   table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string)
# MAGIC else:
# MAGIC   credential: AzureNamedKeyCredential = AzureNamedKeyCredential(storage_account, access_key)
# MAGIC   table_service_client = TableServiceClient(endpoint=f'https://{storage_account}.table.core.windows.net', credential=credential)
# MAGIC   
# MAGIC table_client = table_service_client.get_table_client(table_name=control_table_name)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import DataFrame
# MAGIC import pyspark.sql.functions as F
# MAGIC import pyspark.sql.types as T
# MAGIC 
# MAGIC 
# MAGIC table_entities_df: DataFrame = spark.createDataFrame(table_client.query_entities('true'))
# MAGIC table_entities = table_entities_df.select('warehouseSchema', 'warehouseTable').filter(F.col('include')==True).collect()
# MAGIC 
# MAGIC display(table_entities)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC 
# MAGIC import honeycomb.snowflake
# MAGIC 
# MAGIC import asyncio
# MAGIC from functools import reduce
# MAGIC from pyspark.sql import DataFrame
# MAGIC 
# MAGIC 
# MAGIC SNOWFLAKE_SOURCE_NAME: str = "net.snowflake.spark.snowflake"
# MAGIC secret_scope: str = 'honeycomb-secrets-kv'
# MAGIC snowflake_connection_key: str = 'ds-snowflake-connection'
# MAGIC 
# MAGIC snowflake = honeycomb.snowflake.Snowflake(
# MAGIC   spark, 
# MAGIC   secret_scope=secret_scope,
# MAGIC   connection_key=snowflake_connection_key
# MAGIC )
# MAGIC 
# MAGIC async def create_df(table_entity):
# MAGIC   result_df: DataFrame = None
# MAGIC   schema: str = table_entity.warehouseSchema
# MAGIC   table: str = table_entity.warehouseTable
# MAGIC   sql: str = f"SELECT '{database_name}' AS database_name, '{schema}' AS schema_name, '{table}' AS table_name, MAX(hc_load_ts) AS hc_load_ts FROM {database_name}.{schema}.VW_{table}"
# MAGIC   print(sql)
# MAGIC   try:
# MAGIC     result_df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
# MAGIC       .options(**snowflake.options) \
# MAGIC       .option("query", sql) \
# MAGIC       .load() 
# MAGIC   except Exception as e:
# MAGIC     schemaType = T.StructType([
# MAGIC       T.StructField('database_name', T.StringType()),
# MAGIC       T.StructField('schema_name', T.StringType()),
# MAGIC       T.StructField('table_name', T.StringType()),
# MAGIC       T.StructField('hc_load_ts', T.TimestampType())
# MAGIC     ])
# MAGIC     result_df = spark.createDataFrame([[database_name, schema, table, None]], schemaType)
# MAGIC   
# MAGIC   return result_df
# MAGIC   
# MAGIC 
# MAGIC dfs = await asyncio.gather(*map(create_df, table_entities))
# MAGIC print('Combining dataframes...')
# MAGIC latest_table_load_times_df = reduce(DataFrame.unionAll, dfs)
# MAGIC print('Complete')
# MAGIC latest_table_load_times_df = latest_table_load_times_df.persist()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC print(load_time)
# MAGIC 
# MAGIC tables_that_did_not_load_today_df: DataFrame = latest_table_load_times_df.filter(F.col('hc_load_ts') < F.lit(load_time)).sort(F.col('hc_load_ts'))
# MAGIC display(tables_that_did_not_load_today_df)
