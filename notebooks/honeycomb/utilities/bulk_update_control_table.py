# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## The purpose of this application is to bulk update an azure storage table from a csv. This is useful for updating many rows in the control table.
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
# MAGIC dbutils.widgets.dropdown('dry_run', 'true', ['true', 'false'])

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
# MAGIC def target_entities_from_snowflake():
# MAGIC   SNOWFLAKE_SOURCE_NAME: str = "net.snowflake.spark.snowflake"
# MAGIC   secret_scope: str = 'honeycomb-secrets-kv'
# MAGIC   snowflake_connection_key: str = 'ds-snowflake-connection'
# MAGIC 
# MAGIC   snowflake = honeycomb.snowflake.Snowflake(
# MAGIC     spark, 
# MAGIC     secret_scope=secret_scope,
# MAGIC     connection_key=snowflake_connection_key
# MAGIC   )
# MAGIC 
# MAGIC   target_entities_df: DataFrame = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
# MAGIC     .options(**snowflake.options) \
# MAGIC     .option('spark.sql.caseSensitive', True) \
# MAGIC     .option("query", f"SELECT DATABASE as PARTITIONKEY, TABLENAME as ROWKEY, NOT(SKIP) as INCLUDE FROM {job_sources_table}") \
# MAGIC     .load() 
# MAGIC 
# MAGIC   target_entities = target_entities_df.rdd.map(lambda row: row.asDict(True)).collect()
# MAGIC 
# MAGIC   return target_entities

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import csv
# MAGIC from azure.data.tables import UpdateMode
# MAGIC 
# MAGIC def target_entities_from_file():
# MAGIC   
# MAGIC   filename: str = f'/dbfs/{mount_info.mountPoint}/{path}'
# MAGIC   print(filename)
# MAGIC   target_entities = []
# MAGIC   with open(filename, newline='') as fin:
# MAGIC     reader = csv.DictReader(fin)
# MAGIC     for row in reader:
# MAGIC       row['Include'] = True
# MAGIC       target_entities += [row]
# MAGIC     
# MAGIC   return target_entities
# MAGIC         

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC target_entities = target_entities_from_file() if path else target_entities_from_snowflake()
# MAGIC 
# MAGIC update_count: int = 0
# MAGIC total_count: int = 0
# MAGIC missing_count: int = 0
# MAGIC ignored_count: int = 0
# MAGIC   
# MAGIC for target_entity in target_entities:
# MAGIC     total_count = total_count + 1
# MAGIC     partition_key: str = target_entity['PARTITIONKEY'].strip()
# MAGIC     row_key: str = target_entity['ROWKEY'].strip()
# MAGIC     include: bool = target_entity['INCLUDE']
# MAGIC     predicate: str = f"PartitionKey eq '{partition_key.upper()}' and RowKey eq '{row_key.upper()}'"
# MAGIC     entities = table_client.query_entities(predicate)
# MAGIC     for entity in entities:
# MAGIC         if entity['Include'] == include:
# MAGIC           ignored_count = ignored_count + 1
# MAGIC           continue
# MAGIC         entity['Include'] = include
# MAGIC         prefix = '**DRY-RUN** ' if dry_run else ''
# MAGIC         print(f'{prefix}Updating entity: partition_key={partition_key}, row_key={row_key}, include={include}')
# MAGIC         if not dry_run:
# MAGIC           table_client.update_entity(mode=UpdateMode.MERGE, entity=entity)
# MAGIC         update_count = update_count + 1
# MAGIC print(f'Updated: {update_count} of {total_count} entities')
# MAGIC print(f'Unchanged: {ignored_count} entities')
