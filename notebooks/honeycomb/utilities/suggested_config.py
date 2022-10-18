# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Configuration
# MAGIC 
# MAGIC The purpose of this application is to aid in the generation of Honeycomb configuration files. 
# MAGIC The generated configuration file will contain suggestions for schema, validation and metadata.
# MAGIC The configuration file will be commented out by default and will need to be reviewed and uncommented.
# MAGIC 
# MAGIC ## Arguments
# MAGIC 
# MAGIC * client - The client name
# MAGIC * partition_key - The partition key as it appears in the contro table (ExtractDataEntities)
# MAGIC * row_key - The row key as it appears in the contro table (ExtractDataEntities)
# MAGIC * src_path - The source directory of the data source. This data set is used to profile for validation/schema suggestions
# MAGIC * src_format - The source format ['parquet', 'orc', 'avro', 'csv', 'json', 'xml', 'excel'] Default: parquet
# MAGIC * read_options - A list of key=value pairs to be provided when loading the src_path
# MAGIC * dst_path - The destination of the generated configuration file
# MAGIC * log_path - The destination directory to store log information
# MAGIC * primary_keys - A list of primary keys. These will be added as suggestions for inclusion in metadata
# MAGIC * containers - A list of ADLS containers to mount - Default: trusted
# MAGIC * overwrite - A flag used to determine if existing configuration should be overwritten. Existing configuration will be backed up.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbutils.widgets.removeAll()
# MAGIC dbutils.widgets.text('client', '')
# MAGIC dbutils.widgets.text('partition_key', '')
# MAGIC dbutils.widgets.text('row_key', '')
# MAGIC dbutils.widgets.text('src_path', '')
# MAGIC dbutils.widgets.dropdown('src_format', 'parquet', ['parquet', 'orc', 'avro', 'csv', 'json', 'xml', 'excel'])
# MAGIC dbutils.widgets.text('read_options', '', 'read_options (k1=v1, k2=v2, ...)')
# MAGIC dbutils.widgets.text('dst_path', '')
# MAGIC dbutils.widgets.text('log_path', '')
# MAGIC dbutils.widgets.text('primary_keys', '')
# MAGIC dbutils.widgets.text('containers', 'trusted')
# MAGIC dbutils.widgets.dropdown('overwrite', 'false', ['true', 'false'])

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import re
# MAGIC from distutils.util import strtobool
# MAGIC 
# MAGIC client = dbutils.widgets.get('client')
# MAGIC partition_key = dbutils.widgets.get('partition_key')
# MAGIC partition_key = partition_key.upper() if partition_key else partition_key
# MAGIC row_key = dbutils.widgets.get('row_key')
# MAGIC row_key = row_key.upper() if row_key else row_key
# MAGIC src_path = dbutils.widgets.get('src_path')
# MAGIC src_format = dbutils.widgets.get('src_format')
# MAGIC src_format = 'com.crealytics.spark.excel' if src_format == 'excel' else src_format
# MAGIC dst_path = dbutils.widgets.get('dst_path')
# MAGIC log_path = dbutils.widgets.get('log_path')
# MAGIC read_options = dbutils.widgets.get('read_options')
# MAGIC read_options = dict(re.split('\s*=\s*', x) for x in re.split('\s*,\s*', read_options)) if read_options else {}
# MAGIC primary_keys = dbutils.widgets.get('primary_keys')
# MAGIC primary_keys = [x.strip() for x in primary_keys.split(',') if x.strip()]
# MAGIC containers = dbutils.widgets.get('containers')
# MAGIC containers = re.split('\s*,\s*', containers)
# MAGIC src_format = src_format if src_format else None
# MAGIC overwrite = bool(strtobool(dbutils.widgets.get('overwrite')))
# MAGIC 
# MAGIC if not client:
# MAGIC   raise ValueError('Missing required option: client')
# MAGIC if not partition_key:
# MAGIC   raise ValueError('Missing required option: partition_key')
# MAGIC if not row_key:
# MAGIC   raise ValueError('Missing required option: row_key')
# MAGIC if not src_path:
# MAGIC   raise ValueError('Missing required option: src_path')
# MAGIC if not dst_path:
# MAGIC   raise ValueError('Missing required option: dst_path')
# MAGIC if not src_format:
# MAGIC   raise ValueError('Missing required option: src_format')
# MAGIC   
# MAGIC print(f'client={client}')
# MAGIC print(f'partition_key={partition_key}')
# MAGIC print(f'row_key={row_key}')
# MAGIC print(f'src_path={src_path}')
# MAGIC print(f'src_format={src_format}')
# MAGIC print(f'dst_path={dst_path}')
# MAGIC print(f'log_path={log_path}')
# MAGIC print(f'read_options={read_options}')
# MAGIC print(f'primary_keys={primary_keys}')
# MAGIC print(f'containers={containers}')
# MAGIC print(f'overwrite={overwrite}')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC hc = honeycomb.Honeycomb(spark, client)
# MAGIC for container in containers:
# MAGIC   hc.filesystem.mount(container)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC if hc.filesystem.is_file(dst_path) and not overwrite:
# MAGIC   print(f'File: {dst_path} already exists')
# MAGIC   dbutils.notebook.exit(0)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_df = spark.read.format(src_format)
# MAGIC if read_options:
# MAGIC   src_df = src_df.options(**read_options)
# MAGIC src_df = src_df.load(src_path)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import json
# MAGIC 
# MAGIC schema = json.loads(src_df.schema.json())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.suggestion
# MAGIC import yaml
# MAGIC 
# MAGIC suggestion = honeycomb.suggestion.Suggestion(spark, src_df)
# MAGIC suggestion_results = suggestion.execute()
# MAGIC validation = suggestion_results.to_dict()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC columns = []
# MAGIC for primary_key in primary_keys:
# MAGIC   columns.append({ 
# MAGIC     'column': primary_key, 
# MAGIC     'metadata': [
# MAGIC       {'primary_key': 'true'}
# MAGIC     ]
# MAGIC   })
# MAGIC metadata = { 'columns': columns }

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import yaml
# MAGIC import re
# MAGIC import time
# MAGIC 
# MAGIC config = {}
# MAGIC config['schema'] = schema
# MAGIC config['validation'] = validation
# MAGIC config['metadata'] = metadata
# MAGIC 
# MAGIC config = yaml.safe_dump(config, sort_keys=False)
# MAGIC 
# MAGIC print(config)
# MAGIC config = re.sub(r'(^|\n)', '\n#', config)
# MAGIC print(config)
# MAGIC 
# MAGIC 
# MAGIC if hc.filesystem.is_file(dst_path):
# MAGIC   backup_path = f'{dst_path}.{time.strftime("%Y%m%d_%H%M%S")}'
# MAGIC   hc.filesystem.cp(dst_path, backup_path)
# MAGIC                    
# MAGIC result = hc.filesystem.write(dst_path, config, overwrite=True)
# MAGIC print(result)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import pyspark.sql.types as T
# MAGIC 
# MAGIC if log_path:
# MAGIC   log_schema = T.StructType([
# MAGIC     T.StructField('PartitionKey', T.StringType()),
# MAGIC     T.StructField('RowKey', T.StringType()),
# MAGIC     T.StructField('configFile', T.StringType()),
# MAGIC   ])
# MAGIC   prefix = '/mnt'
# MAGIC   dst_path = dst_path[dst_path.startswith(prefix) and len(prefix):]
# MAGIC   log_data = [
# MAGIC     [partition_key, row_key, dst_path],
# MAGIC   ]
# MAGIC   log_df = spark.createDataFrame(log_data, schema=log_schema).coalesce(1)
# MAGIC   log_df.write.mode('append').parquet(log_path)
# MAGIC   
