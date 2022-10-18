# Databricks notebook source
# MAGIC %md
# MAGIC # Control Table Generation (ExtractDataEntities)
# MAGIC 
# MAGIC The purpose of this application is to expedite generation of the ExtractDataEntities control table which is used to drive the Honeycomb ELT process.
# MAGIC The goal is to provide datasource independent processing logic driven by simple ansi standard queries to export information schema information.
# MAGIC 
# MAGIC This application will make a best effort when populating the control table. By default all datasource table destinations (ADLS) will configured to be replaced and disabled are default. They will need to be manually enabled in the control table and their write disposition will need to be updated to merge/append if appropriate.
# MAGIC 
# MAGIC The resulting file can be imported directly into the ExtractDataEntities storage table. 
# MAGIC 
# MAGIC ### Arguments
# MAGIC 
# MAGIC  * client - The client name. **Required**
# MAGIC  * src-path - The location of the initial information schema export. **Required**
# MAGIC  * export-path - The location of the export of the existing Azure Table (ExtractDataEntities). **Required**
# MAGIC  * dst-path - The desintation of the generated control table script. Default: [dbfs:/path/to/source]/ExtractDataEntities.csv 
# MAGIC  * connection-string - The name of the key in the Azure keyvault which contains the data source connection string. Default: ds-mssql-sql-auth-connection
# MAGIC  * src-type - The type of this data source. Default: mssql
# MAGIC  * connection-token - An optional connection token
# MAGIC  * connection-username - An optional connection username
# MAGIC  * connection-password - An optional connection password
# MAGIC  * containers - A comma separated list of ADLS containers to mount. Default: trusted, elt, staging, working
# MAGIC  * partition-key - The value to use as the partition key. Default: The database name
# MAGIC  * data-format - The incoming data format. Default: Inferred from the src-type
# MAGIC  * include-views - A boolean determining if views should be acquired. Default: false
# MAGIC  * tables - A comma separated list of tables/views to include. Default: All tables/views in schema
# MAGIC  * load-warehouse - A boolean determining if the data should be loaded into the warehouse
# MAGIC  
# MAGIC  
# MAGIC  ### Information Schema Source
# MAGIC  
# MAGIC  A sample schema for the incoming CSV file is below. 
# MAGIC ```
# MAGIC root
# MAGIC  |-- TABLE_CATALOG: string (nullable = true)
# MAGIC  |-- TABLE_SCHEMA: string (nullable = true)
# MAGIC  |-- TABLE_NAME: string (nullable = true)
# MAGIC  |-- TABLE_TYPE: string (nullable = true)
# MAGIC  |-- ORDINAL_POSITION: integer (nullable = true)
# MAGIC  |-- COLUMN_NAME: string (nullable = true)
# MAGIC  |-- DATA_TYPE: string (nullable = true)
# MAGIC  |-- COLUMN_DEFAULT: string (nullable = true)
# MAGIC  |-- CHARACTER_MAXIMUM_LENGTH: integer (nullable = true)
# MAGIC  |-- CHARACTER_OCTET_LENGTH: integer (nullable = true)
# MAGIC  |-- NUMERIC_PRECISION: integer (nullable = true)
# MAGIC  |-- NUMERIC_SCALE: integer (nullable = true)
# MAGIC  |-- IS_NULLABLE: string (nullable = true)
# MAGIC  |-- KEY_TYPE: string (nullable = true)
# MAGIC  ```
# MAGIC 
# MAGIC  
# MAGIC  A sample query used to generate the incoming CSV file for MSSQL (This specific example comes from DataFactory)
# MAGIC ```sql
# MAGIC SELECT c.TABLE_CATALOG
# MAGIC 	,c.TABLE_SCHEMA
# MAGIC 	,c.TABLE_NAME
# MAGIC 	,t.TABLE_TYPE
# MAGIC 	,c.ORDINAL_POSITION
# MAGIC 	,c.COLUMN_NAME
# MAGIC 	,c.DATA_TYPE
# MAGIC 	,c.COLUMN_DEFAULT
# MAGIC 	,c.CHARACTER_MAXIMUM_LENGTH
# MAGIC 	,c.CHARACTER_OCTET_LENGTH
# MAGIC 	,c.NUMERIC_PRECISION
# MAGIC 	,c.NUMERIC_SCALE
# MAGIC 	,c.IS_NULLABLE
# MAGIC 	,CASE 
# MAGIC 		WHEN pk.COLUMN_NAME IS NOT NULL
# MAGIC 			THEN 'PRIMARY KEY'
# MAGIC 		ELSE ''
# MAGIC 		END AS KEY_TYPE
# MAGIC FROM INFORMATION_SCHEMA.COLUMNS c
# MAGIC JOIN INFORMATION_SCHEMA.TABLES t ON t.TABLE_CATALOG = c.TABLE_CATALOG
# MAGIC 	AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
# MAGIC 	AND t.TABLE_NAME = c.TABLE_NAME
# MAGIC LEFT JOIN (
# MAGIC 	SELECT ku.TABLE_CATALOG
# MAGIC 		,ku.TABLE_SCHEMA
# MAGIC 		,ku.TABLE_NAME
# MAGIC 		,ku.COLUMN_NAME
# MAGIC 	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
# MAGIC 	INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
# MAGIC 		AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
# MAGIC 	) pk ON c.TABLE_CATALOG = pk.TABLE_CATALOG
# MAGIC 	AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
# MAGIC 	AND c.TABLE_NAME = pk.TABLE_NAME
# MAGIC 	AND c.COLUMN_NAME = pk.COLUMN_NAME
# MAGIC WHERE c.TABLE_SCHEMA = '@{pipeline().parameters.schemaName}'
# MAGIC ORDER BY c.TABLE_SCHEMA
# MAGIC 	,c.TABLE_NAME
# MAGIC 	,c.ORDINAL_POSITION
# MAGIC 	,c.COLUMN_NAME
# MAGIC ```

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC DEFAULT_SRC_TYPE = 'mssql'
# MAGIC DEFAULT_CONNECTION_STRING = 'ds-mssql-sql-auth-connection'
# MAGIC DEFAULT_DST_FILE = 'ExtractDataEntities.csv'
# MAGIC 
# MAGIC dbutils.widgets.text('client', '')
# MAGIC dbutils.widgets.text('connection_string', DEFAULT_CONNECTION_STRING)
# MAGIC dbutils.widgets.text('src_type', DEFAULT_SRC_TYPE)
# MAGIC dbutils.widgets.text('src_path', '')
# MAGIC dbutils.widgets.text('export_path', '')
# MAGIC dbutils.widgets.text('dst_path', '')
# MAGIC dbutils.widgets.text('containers', 'trusted, elt, staging, working')
# MAGIC dbutils.widgets.text('connection_token', '')
# MAGIC dbutils.widgets.text('connection_username', '')
# MAGIC dbutils.widgets.text('connection_password', '')
# MAGIC dbutils.widgets.text('partition_key', '')
# MAGIC dbutils.widgets.text('data_format', '')
# MAGIC dbutils.widgets.dropdown('include_views', 'true', ['true', 'false'])
# MAGIC dbutils.widgets.dropdown('load_warehouse', 'false', ['true', 'false'])
# MAGIC dbutils.widgets.text('tables', '')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import os
# MAGIC import re
# MAGIC from distutils.util import strtobool
# MAGIC 
# MAGIC 
# MAGIC client = dbutils.widgets.get('client')
# MAGIC if not client:
# MAGIC   raise ValueError('missing required option: client')
# MAGIC 
# MAGIC connection_string = dbutils.widgets.get('connection_string')
# MAGIC if not connection_string:
# MAGIC   raise ValueError('missing required option: connection_string')
# MAGIC 
# MAGIC src_type = dbutils.widgets.get('src_type')
# MAGIC if not src_type:
# MAGIC   m = re.search('-([^-]+)-', connection_string)
# MAGIC   if m:
# MAGIC     src_type = m.group(1)
# MAGIC   else:
# MAGIC     raise ValueError('missing required option: src_type')
# MAGIC 
# MAGIC src_path = dbutils.widgets.get('src_path')
# MAGIC if not src_path:
# MAGIC   raise ValueError('missing required option: src_path')
# MAGIC   
# MAGIC export_path = dbutils.widgets.get('export_path')
# MAGIC if not export_path:
# MAGIC   raise ValueError('missing required option: export_path')
# MAGIC   
# MAGIC dst_path = dbutils.widgets.get('dst_path')
# MAGIC dst_path = dst_path if dst_path else os.path.join(os.path.dirname(src_path), DEFAULT_DST_FILE)
# MAGIC if not dst_path:
# MAGIC   raise ValueError('missing required option: dst_path')
# MAGIC 
# MAGIC containers = dbutils.widgets.get('containers')
# MAGIC containers = re.split('\s*,\s*', containers)
# MAGIC   
# MAGIC connection_token = dbutils.widgets.get('connection_token').strip()
# MAGIC connection_token = connection_token if connection_token else None
# MAGIC 
# MAGIC connection_username = dbutils.widgets.get('connection_username').strip()
# MAGIC connection_username = connection_username if connection_username else None
# MAGIC 
# MAGIC connection_password = dbutils.widgets.get('connection_password').strip()
# MAGIC connection_password = connection_password if connection_password else None
# MAGIC 
# MAGIC partition_key = dbutils.widgets.get('partition_key').strip()
# MAGIC partition_key = partition_key if partition_key else None
# MAGIC 
# MAGIC load_warehouse = dbutils.widgets.get('load_warehouse').strip()
# MAGIC load_warehouse = bool(strtobool(load_warehouse)) if load_warehouse else False
# MAGIC 
# MAGIC data_format = dbutils.widgets.get('data_format').strip()
# MAGIC data_format = data_format if data_format else None
# MAGIC if not data_format:
# MAGIC   data_format = { 
# MAGIC     'mssql': 'parquet', 
# MAGIC     'asql': 'parquet',
# MAGIC     'postgres': 'parquet',
# MAGIC     'mysql': 'parquet',
# MAGIC     'oracle': 'parquet',
# MAGIC     'flat_file': None,
# MAGIC     'salesforce': 'parquet',
# MAGIC     'jira': 'json'
# MAGIC   }.get(src_type, None)  
# MAGIC 
# MAGIC include_views = bool(strtobool(dbutils.widgets.get('include_views')))
# MAGIC tables = [x.strip().lower() for x in dbutils.widgets.get('tables').split(',') if x.strip()]
# MAGIC 
# MAGIC print(f'client={client}')
# MAGIC print(f'src_type={src_type}')
# MAGIC print(f'src_path={src_path}')
# MAGIC print(f'export_path={export_path}')
# MAGIC print(f'dst_path={dst_path}')
# MAGIC print(f'containers={containers}')
# MAGIC print(f'connection_string={connection_string}')
# MAGIC print(f'connection_token={connection_token}')
# MAGIC print(f'connection_username={connection_username}')
# MAGIC print(f'connection_password={connection_password}')
# MAGIC print(f'partition_key={partition_key}')
# MAGIC print(f'data_format={data_format}')
# MAGIC print(f'include_views={include_views}')
# MAGIC print(f'load_warehouse={load_warehouse}')
# MAGIC print(f'tables={tables}')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC hc = honeycomb.Honeycomb(spark, client)
# MAGIC for container in containers:
# MAGIC   result = hc.filesystem.mount(container)
# MAGIC   print(result)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC 
# MAGIC src_format = "csv"
# MAGIC infer_schema = "true"
# MAGIC first_row_is_header = "true"
# MAGIC delimiter = ","
# MAGIC 
# MAGIC src_df = spark.read.format(src_format) \
# MAGIC   .option("inferSchema", infer_schema) \
# MAGIC   .option("header", first_row_is_header) \
# MAGIC   .option("sep", delimiter) \
# MAGIC   .load(src_path)
# MAGIC 
# MAGIC src_df = src_df.cache()
# MAGIC 
# MAGIC src_df.createOrReplaceTempView('src')
# MAGIC 
# MAGIC display(src_df)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC 
# MAGIC export_format = "csv"
# MAGIC infer_schema = "true"
# MAGIC first_row_is_header = "true"
# MAGIC delimiter = ","
# MAGIC 
# MAGIC export_df = spark.read.format(export_format) \
# MAGIC   .option("inferSchema", infer_schema) \
# MAGIC   .option("header", first_row_is_header) \
# MAGIC   .option("sep", delimiter) \
# MAGIC   .load(export_path)
# MAGIC export_df = export_df.cache()
# MAGIC 
# MAGIC display(export_df)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC total = src_df.count()
# MAGIC print(f'total count: {total}')
# MAGIC 
# MAGIC src_df = src_df \
# MAGIC   .withColumn('table_catalog_join', F.coalesce('table_catalog', F.lit(''))) \
# MAGIC   .withColumn('table_schema_join', F.coalesce('table_schema', F.lit(''))) \
# MAGIC   .withColumn('table_name_join', F.coalesce('table_name', F.lit(''))) 
# MAGIC 
# MAGIC if len(tables) > 0:
# MAGIC   src_df = src_df.filter(F.lower(F.col('table_name')).isin(tables))
# MAGIC   
# MAGIC if not include_views:
# MAGIC   src_df = src_df.filter(F.col('table_type') != 'VIEW')
# MAGIC 
# MAGIC filterd_count = src_df.count()
# MAGIC print(f'filtered count: {filterd_count}')
# MAGIC 
# MAGIC table_df = src_df.select('table_catalog', 'table_catalog_join', 'table_schema', 'table_schema_join', 'table_name', 'table_name_join').distinct()
# MAGIC 
# MAGIC primary_keys_df = src_df \
# MAGIC   .filter(F.col('key_type') == 'PRIMARY KEY') \
# MAGIC   .groupBy('table_catalog_join', 'table_schema_join', 'table_name_join') \
# MAGIC   .agg(F.collect_list('column_name').alias('primary_keys')) \
# MAGIC   .select(F.col('table_catalog_join'), F.col('table_schema_join'), F.col('table_name_join'), F.array_join('primary_keys', ',').alias('primary_keys'))
# MAGIC 
# MAGIC dst_df = table_df.join(primary_keys_df, ['table_catalog_join', 'table_schema_join', 'table_name_join'], 'left')
# MAGIC 
# MAGIC dst_df = dst_df.select(
# MAGIC   (F.upper(F.lit(partition_key)) if partition_key else F.upper(F.col('table_catalog'))).alias('PartitionKey'),
# MAGIC   F.upper(F.col('table_name')).alias('RowKey'),
# MAGIC   F.lit(False).alias('Include'), 
# MAGIC   F.lit('Edm.Boolean').alias('Include@odata.type'), 
# MAGIC   F.lit('FULL').alias('LoadType'), 
# MAGIC   F.lit(client).alias('clientName'), 
# MAGIC   F.lit(connection_string).alias('connectionString'), 
# MAGIC   F.lit(connection_username).cast(T.StringType()).alias('connectionUsername'), 
# MAGIC   F.lit(connection_password).cast(T.StringType()).alias('connectionUserPassword'), 
# MAGIC   F.lit(connection_token).cast(T.StringType()).alias('connectionToken'), 
# MAGIC   F.col('table_catalog').alias('instanceName'), 
# MAGIC   F.lit(load_warehouse).cast(T.BooleanType()).alias('loadWarehouse'), 
# MAGIC   F.lit('Edm.Boolean').alias('loadWarehouse@odata.type'), 
# MAGIC   F.col('primary_keys').alias('primaryKeys'), 
# MAGIC   F.lit(True).alias('processEmptyFile'), 
# MAGIC   F.lit('Edm.Boolean').alias('processEmptyFile@odata.type'), 
# MAGIC   F.regexp_replace(F.col('table_schema'), r'[\W_]', '_').alias('schemaName'), 
# MAGIC   F.lit(src_type).alias('sourceType'), 
# MAGIC   F.regexp_replace(F.col('table_name'), r'[\W_]', '_').alias('tableName'), 
# MAGIC   F.lit('transformation').alias('transformation'), 
# MAGIC   F.lit('snowflake').alias('warehouse'), 
# MAGIC   F.lit('TRUSTED').alias('warehouseSchema'), 
# MAGIC   F.lit('TRUSTED_SECURE').alias('warehouseSecureSchema'), 
# MAGIC   F.regexp_replace(
# MAGIC     F.concat_ws(
# MAGIC       '_',
# MAGIC       F.col('table_catalog'), 
# MAGIC       F.col('table_schema'), 
# MAGIC       F.col('table_name')
# MAGIC     ), r'[\W_]', '_').alias('warehouseTable'), 
# MAGIC   F.lit(data_format).cast(T.StringType()).alias('dataFormat'), 
# MAGIC   F.lit('overwrite').alias('writeDisposition'), 
# MAGIC   F.lit(None).cast(T.StringType()).alias('incrementalField'), 
# MAGIC   F.lit(None).cast(T.StringType()).alias('modificationColumn'), 
# MAGIC   F.lit(None).cast(T.StringType()).alias('deleteColumn'), 
# MAGIC   F.lit(None).cast(T.StringType()).alias('flattenColumn'),
# MAGIC   F.lit(None).cast(T.StringType()).alias('partitionColumns'),
# MAGIC   F.lit(None).cast(T.StringType()).alias('zorderColumns'),
# MAGIC   F.lit(None).cast(T.StringType()).alias('configFile'), 
# MAGIC   F.lit(None).cast(T.StringType()).alias('schemaFile'), 
# MAGIC   F.lit(None).cast(T.StringType()).alias('exportFormat'), 
# MAGIC   F.lit(None).cast(T.StringType()).alias('exportOptions'), 
# MAGIC   F.lit(None).cast(T.StringType()).alias('format'), 
# MAGIC   F.lit(None).cast(T.StringType()).alias('readOptions'), 
# MAGIC   F.lit(False).alias('validate'), 
# MAGIC   F.lit('Edm.Boolean').alias('validate@odata.type'), 
# MAGIC   F.lit(False).alias('transform'), 
# MAGIC   F.lit('Edm.Boolean').alias('transform@odata.type'),
# MAGIC   F.lit(False).alias('includeMetadata'), 
# MAGIC   F.lit('Edm.Boolean').alias('includeMetadata@odata.type'),
# MAGIC   F.lit(True).alias('failOnError'), 
# MAGIC   F.lit('Edm.Boolean').alias('failOnError@odata.type'),
# MAGIC   F.lit(True).alias('mergeSchema'), 
# MAGIC   F.lit('Edm.Boolean').alias('mergeSchema@odata.type'),
# MAGIC 
# MAGIC ).cache()
# MAGIC 
# MAGIC display(dst_df)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC if not export_df.rdd.isEmpty():
# MAGIC   dst_df = dst_df.join(
# MAGIC     export_df, 
# MAGIC     [dst_df.PartitionKey == export_df.PartitionKey, dst_df.RowKey == export_df.RowKey], 
# MAGIC     'left_anti'
# MAGIC   )
# MAGIC   display(dst_df)
# MAGIC else:
# MAGIC   print('export_df is empty.')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import os 
# MAGIC 
# MAGIC dst_dir, dst_file = os.path.split(dst_path)
# MAGIC tmp_dir = os.path.join(dst_dir, '_tmp')
# MAGIC print(f'dst_dir={dst_dir}')
# MAGIC print(f'dst_file={dst_file}')
# MAGIC print(f'tmp_dir={tmp_dir}')
# MAGIC 
# MAGIC dst_df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(tmp_dir)
# MAGIC part_file = next(iter(f for f in dbutils.fs.ls(tmp_dir) if f.name.startswith('part-')), None)
# MAGIC dst_file = next(iter(f for f in dbutils.fs.ls(dst_dir) if f.name == dst_file), None)
# MAGIC print(f'dst_file={dst_file}')
# MAGIC if dst_file:
# MAGIC   print(f'Deleting file: {dst_file.path}')
# MAGIC   dbutils.fs.rm(dst_file.path)
# MAGIC print(f'Moving from: {part_file.path} to {dst_path}')
# MAGIC dbutils.fs.mv(part_file.path, dst_path)
# MAGIC print(f'Deleting directory: {tmp_dir}')
# MAGIC dbutils.fs.rm(tmp_dir, True)
