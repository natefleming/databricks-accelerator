# Databricks notebook source
# MAGIC %md
# MAGIC ### Clean up

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql import types as T
import honeycomb

client='nate-fleming'

honeycomb = honeycomb.Honeycomb(spark, client)
honeycomb.filesystem.mount('trusted')
honeycomb.filesystem.mount('trusted-secure')
honeycomb.filesystem.mount('staging')
honeycomb.filesystem.mount('elt')

database=re.sub('[^\w+]', '_', client)
sql(f'drop database if exists {database} cascade')
sql(f'drop database if exists {database}_STG cascade')
sql(f'drop database if exists {database}_ERR cascade')
sql(f'drop database if exists {database}_SECURE cascade')
sql('drop database if exists stage cascade')
dbutils.fs.rm('/mnt/staging/incoming', True)
dbutils.fs.rm(f'/mnt/trusted/*', True)
dbutils.fs.rm(f'/mnt/elt/*', True)
dbutils.fs.rm(f'/mnt/trusted/{database}/', True)
dbutils.fs.rm(f'/mnt/elt/{database}/', True)
dbutils.fs.rm(f'/mnt/trusted/{client}/*', True)
dbutils.fs.rm(f'/mnt/elt/{client}/*', True)
dbutils.fs.rm(f'/mnt/trusted/{client}', True)
dbutils.fs.rm(f'/mnt/elt/{client}', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Clean the databases and insert some intial data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import re
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC import honeycomb
# MAGIC 
# MAGIC client='nate-fleming'
# MAGIC 
# MAGIC honeycomb = honeycomb.Honeycomb(spark, client)
# MAGIC honeycomb.filesystem.mount('trusted')
# MAGIC honeycomb.filesystem.mount('staging')
# MAGIC honeycomb.filesystem.mount('elt')
# MAGIC 
# MAGIC database=re.sub('[^\w+]', '_', client)
# MAGIC sql(f'drop database if exists {database} cascade')
# MAGIC sql(f'drop database if exists {database}_STG cascade')
# MAGIC sql(f'drop database if exists {database}_ERR cascade')
# MAGIC sql('drop database if exists stage cascade')
# MAGIC dbutils.fs.rm('/mnt/staging/incoming', True)
# MAGIC dbutils.fs.rm(f'/mnt/trusted/*', True)
# MAGIC dbutils.fs.rm(f'/mnt/elt/*', True)
# MAGIC dbutils.fs.rm(f'/mnt/trusted/{database}', True)
# MAGIC dbutils.fs.rm(f'/mnt/elt/{database}', True)
# MAGIC dbutils.fs.rm(f'/mnt/trusted/{client}', True)
# MAGIC dbutils.fs.rm(f'/mnt/elt/{client}', True)
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC   T.StructField('ssn', T.StringType()),
# MAGIC ])
# MAGIC 
# MAGIC src_data = [
# MAGIC   ['1', 'nate', 'fleming', '123-456-7890'],
# MAGIC   ['2', 'danielle', 'schwarz', '321-456-7890'],
# MAGIC   ['3', 'kevin', 'kschulke', '456-456-7890'],
# MAGIC   ['4', 'nate', 'weinreich', '987-456-7890'],
# MAGIC ]
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/'
# MAGIC src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC src_df = spark.read.parquet(src_dir)
# MAGIC display(src_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Update the dataset

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql import types as T
import honeycomb

src_data = [
  ['1', 'nate', 'x'],
  ['2', 'danielle', 'y'],
  ['3', 'kevin', 'z'],
  ['4', 'nate', 'T'],
]

schema = T.StructType([
  T.StructField('id', T.StringType()),
  T.StructField('first_name', T.StringType()),
  T.StructField('last_name', T.StringType()),
])

src_dir = '/mnt/staging/incoming/'
src_df = spark.createDataFrame(src_data, schema=schema)
src_df.write.mode('overwrite').parquet(src_dir)
src_df = spark.read.parquet(src_dir)
display(src_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Update with original data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import re
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC import honeycomb
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC   T.StructField('ssn', T.StringType()),
# MAGIC ])
# MAGIC 
# MAGIC src_data = [
# MAGIC   ['1', 'nate', 'fleming', '123-456-7890'],
# MAGIC   ['2', 'danielle', 'schwarz', '321-456-7890'],
# MAGIC   ['3', 'kevin', 'kschulke', '456-456-7890'],
# MAGIC   ['4', 'nate', 'weinreich', '987-456-7890'],
# MAGIC ]
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/'
# MAGIC src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC src_df = spark.read.parquet(src_dir)
# MAGIC display(src_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Update with changed schema

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import re
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC import honeycomb
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC   T.StructField('middle_name', T.StringType()),
# MAGIC   T.StructField('age', T.IntegerType()),
# MAGIC ])
# MAGIC 
# MAGIC src_data = [
# MAGIC   ['1', 'nate', 'x', 'adam', 1],
# MAGIC   ['2', 'danielle', 'y', 'jolene', 2],
# MAGIC   ['3', 'kevin', 'z', 'x', 3],
# MAGIC   ['4', 'nate', 'T', 'y', 4],
# MAGIC ]
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/'
# MAGIC src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC src_df = spark.read.parquet(src_dir)
# MAGIC display(src_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add row with duplicate primary key

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import re
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC import honeycomb
# MAGIC 
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC ])
# MAGIC 
# MAGIC src_data = [
# MAGIC   ['1', 'nate', 'x'],
# MAGIC   ['2', 'danielle', 'y'],
# MAGIC   ['3', 'kevin', 'z'],
# MAGIC   ['4', 'nate', 'T'],
# MAGIC   ['1', 'nate', 'foo']
# MAGIC ]
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/'
# MAGIC src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC src_df = spark.read.parquet(src_dir)
# MAGIC display(src_df)
