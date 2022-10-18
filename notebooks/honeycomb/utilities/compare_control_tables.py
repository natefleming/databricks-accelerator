# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## The purpose of this application is to compare V1 and V2 control tables
# MAGIC 
# MAGIC ## Arguments
# MAGIC 
# MAGIC * storage_account - The ADLS storage account name. (Copy and paste from the portal). Mutually exclusive with connection_string
# MAGIC * access_key - The ADLS storage account name. (Copy and paste from the portal)
# MAGIC * connection_string - The ADLS connection string. (Copy and paste from portal). Mutually exclusive with storage_account
# MAGIC * container - The ADLS container which contains the path to an input entity file.
# MAGIC * control_table_name - The storage account control table name (default: ExtractDataEntities)
# MAGIC * path - The ADLS path which contains the path to an input entity file. Mutually exclusive with control_table_name
# MAGIC * job_sources_table - The snowflake table name which contains the input entities (ie JOBSOURCES). Mutually exclusive with path
# MAGIC * dry_run - Dry Run (default: True)
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
# MAGIC dbutils.widgets.text('container', 'working')
# MAGIC dbutils.widgets.text('control_table_name', 'ExtractDataEntities')
# MAGIC dbutils.widgets.text('path', '')
# MAGIC dbutils.widgets.text('job_sources_table', '')

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC from distutils.util import strtobool
# MAGIC 
# MAGIC 
# MAGIC storage_account = dbutils.widgets.get('storage_account')
# MAGIC access_key = dbutils.widgets.get('access_key')
# MAGIC connection_string = dbutils.widgets.get('connection_string')
# MAGIC control_table_name = dbutils.widgets.get('control_table_name')
# MAGIC container = dbutils.widgets.get('container')
# MAGIC path = dbutils.widgets.get('path')
# MAGIC job_sources_table = dbutils.widgets.get('job_sources_table')
# MAGIC dry_run = strtobool(dbutils.widgets.get('dry_run'))
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
# MAGIC if not container and not job_sources_table:
# MAGIC   raise ValueError('Missing required option: container')
# MAGIC   
# MAGIC if not path and not job_sources_table:
# MAGIC   raise ValueError('Missing required option: path or job_sources_table')
# MAGIC   
# MAGIC if path and job_sources_table:
# MAGIC   raise ValueError('Options path and job_sources_table are mutually exclusive')
# MAGIC 
# MAGIC   
# MAGIC print(f'storage_account: {storage_account}')
# MAGIC print(f'access_key: {access_key}')
# MAGIC print(f'connection_string: {connection_string}')
# MAGIC print(f'control_table_name: {control_table_name}')
# MAGIC print(f'container: {container}')
# MAGIC print(f'path: {path}')
# MAGIC print(f'job_sources_table: {job_sources_table}')
# MAGIC print(f'dry_run: {dry_run}')

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
# MAGIC import honeycomb
# MAGIC 
# MAGIC hc = honeycomb.Honeycomb(spark)
# MAGIC if container:
# MAGIC   mount_info, exception = hc.filesystem.mount(container, storage_account=storage_account) 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC 
# MAGIC import honeycomb.snowflake
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
# MAGIC snowflake_entities_df: DataFrame = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
# MAGIC   .options(**snowflake.options) \
# MAGIC   .option('spark.sql.caseSensitive', True) \
# MAGIC   .option("query", f"SELECT DATABASE as PARTITIONKEY, TABLENAME as ROWKEY, NOT(SKIP) as INCLUDE FROM {job_sources_table} WHERE (PARTITIONKEY IS NOT NULL OR TRIM(PARTITIONKEY) = '') AND (ROWKEY IS NOT NULL OR TRIM(ROWKEY) = '')") \
# MAGIC   .load() \
# MAGIC   .select([
# MAGIC     F.upper(F.col('PartitionKey')).alias('PartitionKey'), 
# MAGIC     F.upper(F.col('RowKey')).alias('RowKey'), 
# MAGIC     F.coalesce(F.col('Include'), F.lit(False)).alias('Include')
# MAGIC ])
# MAGIC 
# MAGIC snowflake_entities_df.createOrReplaceTempView('v1')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import pyspark.sql.functions as F
# MAGIC import pyspark.sql.types as T
# MAGIC 
# MAGIC predicate: str = 'Include eq true'
# MAGIC azure_table_entities_df = spark.createDataFrame(table_client.query_entities('true')) \
# MAGIC   .select([
# MAGIC     F.upper(F.col('PartitionKey')).alias('PartitionKey'), 
# MAGIC     F.upper(F.col('RowKey')).alias('RowKey'), 
# MAGIC     F.col('Include')
# MAGIC ])
# MAGIC azure_table_entities_df.createOrReplaceTempView('v2')

# COMMAND ----------

# MAGIC %md # Number of tables loaded from V1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1) from v1 where Include is true;

# COMMAND ----------

# MAGIC %md # Number of tables loaded from V2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(1) from v2 where Include is true;

# COMMAND ----------

# MAGIC %md # Entries are in V1 but not V2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT v1.PartitionKey, v1.RowKey
# MAGIC FROM v1
# MAGIC LEFT JOIN v2
# MAGIC ON
# MAGIC v1.PartitionKey = v2.PartitionKey AND
# MAGIC v1.RowKey = v2.RowKey 
# MAGIC WHERE 
# MAGIC v2.PartitionKey is NULL 

# COMMAND ----------

# MAGIC %md # Entries are in V2 but not V1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT v2.PartitionKey, v2.RowKey
# MAGIC FROM v2
# MAGIC LEFT JOIN v1
# MAGIC ON
# MAGIC v1.PartitionKey = v2.PartitionKey AND
# MAGIC v1.RowKey = v2.RowKey 
# MAGIC WHERE 
# MAGIC v1.PartitionKey is NULL 

# COMMAND ----------

# MAGIC %md # Entries are Included in V1 but NOT V2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT v1.PartitionKey, v1.RowKey
# MAGIC FROM v1
# MAGIC JOIN v2
# MAGIC ON
# MAGIC v1.PartitionKey = v2.PartitionKey AND
# MAGIC v1.RowKey = v2.RowKey 
# MAGIC WHERE 
# MAGIC v1.Include <> v2.Include

# COMMAND ----------

# MAGIC %md # Entries are Included in V2 but NOT V1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT v2.PartitionKey, v2.RowKey
# MAGIC FROM v2
# MAGIC JOIN v1
# MAGIC ON
# MAGIC v1.PartitionKey = v2.PartitionKey AND
# MAGIC v1.RowKey = v2.RowKey 
# MAGIC WHERE 
# MAGIC v1.Include <> v2.Include

# COMMAND ----------

v1_information_schema: str = 'SMC.INFORMATION_SCHEMA.TABLES'

v1_information_schema_df: DataFrame = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**snowflake.options) \
  .option('spark.sql.caseSensitive', True) \
  .option("query", f"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM {v1_information_schema}") \
  .load() 
v1_information_schema_df.createOrReplaceTempView('v1_information_schema')
  
v2_information_schema: str = 'SMC_NA.INFORMATION_SCHEMA.TABLES'

v2_information_schema_df: DataFrame = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**snowflake.options) \
  .option('spark.sql.caseSensitive', True) \
  .option("query", f"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM {v2_information_schema}") \
  .load() 
v2_information_schema_df.createOrReplaceTempView('v2_information_schema')


# COMMAND ----------

# MAGIC %md # Determine the number of tables loaded into Snowflake TRUSTED schema V1 and V2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   (SELECT COUNT(1) FROM v1_information_schema WHERE TABLE_SCHEMA = 'TRUSTED' AND TABLE_TYPE LIKE '%TABLE%') AS `(V1) Total Tables in SMC.TRUSTED`,
# MAGIC   (SELECT COUNT(1) FROM v2_information_schema WHERE TABLE_SCHEMA = 'TRUSTED' AND TABLE_TYPE LIKE '%TABLE%' AND TABLE_NAME NOT LIKE '%_MANIFEST') AS  `(V2) Total Tables in SMC_NA.TRUSTED`
