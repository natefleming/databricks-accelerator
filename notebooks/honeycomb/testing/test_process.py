# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC dbutils.widgets.text('client', '')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC client = dbutils.widgets.get('client')
# MAGIC 
# MAGIC if not client:
# MAGIC   raise ValueError('Missing required parameter: client')
# MAGIC   

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def tear_down():
# MAGIC   spark.sql('drop database if exists testing cascade')
# MAGIC   spark.sql('drop database if exists testing_stg cascade')
# MAGIC   spark.sql('drop database if exists testing_secure cascade')
# MAGIC   spark.sql('drop database if exists testing_err cascade')

# COMMAND ----------


import requests 
import os
from urllib.parse import urlparse


def download_data(url: str, dst_dir: str):
  filename = os.path.basename(urlparse(url).path)
  path = f'/dbfs/{dst_dir}/{filename}'

  if not os.path.exists(path):
    r = requests.get(url, allow_redirects=True)
    print(f'creating path: {dst_dir}...')
    dbutils.fs.mkdirs(dst_dir)
    print(f'writing content...')
    open(path, 'wb').write(r.content)
  else:
    print(f'path exists: {path}')

def stage_data(src_dir: str, dst_dir: str, row_count: int = 10000):
  print(f'Copying: {row_count} rows from {src_dir} to {dst_dir}')
  spark.read \
    .options(header=True, inferSchema=True) \
    .csv(src_dir) \
    .limit(row_count) \
    .coalesce(1) \
    .write \
    .options(header=True, inferSchema=True) \
    .mode('overwrite') \
    .csv(dst_dir)

  dst_dir = spark.read.options(header=True, inferSchema=True).csv(dst_dir)
  print(f'count={dst_dir.count()}')
  dst_dir.printSchema()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC 
# MAGIC hc = honeycomb.Honeycomb(spark, client)
# MAGIC hc.filesystem.mount('staging')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import pyspark.sql.types as T
# MAGIC import pyspark.sql.functions as F
# MAGIC 
# MAGIC def create_data():
# MAGIC   schema = T.StructType([
# MAGIC     T.StructField('id', T.StringType()),
# MAGIC     T.StructField('id2', T.IntegerType()),
# MAGIC     T.StructField('first_name', T.StringType()),
# MAGIC     T.StructField('last_name', T.StringType()),
# MAGIC     T.StructField('ssn', T.StringType()),
# MAGIC     T.StructField('is_deleted', T.BooleanType()),
# MAGIC   ])
# MAGIC 
# MAGIC   src_data = [
# MAGIC     ['1', 1, 'nate', 'fleming', '123-456-7890', False],
# MAGIC     ['2', 2, 'danielle', 'schwarz', '321-456-7890', False],
# MAGIC     ['3', 3, 'kevin', 'kschulke', '456-456-7890', False],
# MAGIC     ['4', 4, 'nate', 'weinreich', '987-456-7890', False],
# MAGIC   ]
# MAGIC 
# MAGIC   src_dir = '/mnt/staging/incoming/test1/'
# MAGIC   src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC 
# MAGIC   src_dir = '/mnt/staging/incoming/test2/'
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC   
# MAGIC   src_data = [
# MAGIC     ['1', 1, 'nate', 'fleming', '000-000-0000', False],
# MAGIC     ['2', 2, 'danielle', 'schwarz', '111-111-1111', False],
# MAGIC     ['3', 3, 'kevin', 'kschulke', '222-222-2222', False],
# MAGIC     ['4', 4, 'nate', 'weinreich', '987-456-7890', False],
# MAGIC   ]
# MAGIC   
# MAGIC   src_dir = '/mnt/staging/incoming/test3/'
# MAGIC   src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC   
# MAGIC   src_data = [
# MAGIC     ['5', 5, 'john', 'smith', '000-000-0000', False],
# MAGIC     ['6', 6, 'sreeti', 'ravi', '111-111-1111', False],
# MAGIC     ['7', 7, 'adrienne', 'watts', '222-222-2222', False],
# MAGIC     ['8', 8, 'shaun', 'mcadams', '987-456-7890', False],
# MAGIC   ]
# MAGIC   
# MAGIC   src_dir = '/mnt/staging/incoming/test4/'
# MAGIC   src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC   
# MAGIC   src_data = [
# MAGIC     ['1', 1, 'nate', 'smith', '333-333-3333', False],
# MAGIC     ['2', 2, 'danielle', 'ravi', '444-444-4444', False],
# MAGIC     ['7', 7, 'adrienne', 'watts', '555-55-5555', False],
# MAGIC     ['8', 8, 'shaun', 'mcadams', '987-456-7890', False],
# MAGIC     ['9', 9, 'joe', 'smith', '987-456-7890', False],
# MAGIC   ]
# MAGIC   
# MAGIC   src_dir = '/mnt/staging/incoming/test5/'
# MAGIC   src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC   
# MAGIC   src_data = [
# MAGIC     ['1', 1, 'nate', 'smith', '333-333-3333', False],
# MAGIC     ['2', 2, 'danielle', 'ravi', '444-444-4444', True],
# MAGIC     ['7', 7, 'adrienne', 'watts', '555-55-5555', False],
# MAGIC     ['8', 8, 'shaun', 'mcadams', '987-456-7890', True],
# MAGIC     ['9', 9, 'joe', 'smith', '987-456-7890', False],
# MAGIC   ]
# MAGIC   
# MAGIC   src_dir = '/mnt/staging/incoming/test6/'
# MAGIC   src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC   
# MAGIC   src_data = [
# MAGIC     ['1', 2, 'nate1', 'smith', '333-333-3333', False],
# MAGIC     ['2', 3, 'danielle1', 'ravi', '444-444-4444', False],
# MAGIC     ['7', 4, 'adrienne1', 'watts', '555-55-5555', False],
# MAGIC     ['8', 5, 'shaun1', 'mcadams', '987-456-7890', False],
# MAGIC     ['9', 6, 'joe1', 'smith', '987-456-7890', False],
# MAGIC   ]
# MAGIC   
# MAGIC   src_dir = '/mnt/staging/incoming/test7/'
# MAGIC   src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC   
# MAGIC   
# MAGIC   schema = T.StructType([
# MAGIC     T.StructField('id', T.StringType()),
# MAGIC     T.StructField('id2', T.IntegerType()),
# MAGIC     T.StructField('first_name', T.StringType()),
# MAGIC     T.StructField('last_name', T.StringType()),
# MAGIC     T.StructField('ssn', T.StringType()),
# MAGIC     T.StructField('is_deleted', T.BooleanType()),
# MAGIC     T.StructField('age', T.IntegerType()),
# MAGIC     T.StructField('start_date', T.StringType()),
# MAGIC   ])
# MAGIC   
# MAGIC   src_data = [
# MAGIC     ['1', 2, 'nate1', 'smith', '333-333-3333', False, 1, '123454'],
# MAGIC     ['2', 3, 'danielle1', 'ravi', '444-444-4444', False, 2, '123454'],
# MAGIC     ['74', 4, 'adrienne1', 'watts', '555-55-5555', False, 3, '123454'],
# MAGIC     ['84', 5, 'shaun1', 'mcadams', '987-456-7890', False, 4, '123454'],
# MAGIC     ['94', 6, 'joe1', 'smith', '987-456-7890', False, 5, '123454'],
# MAGIC   ]
# MAGIC   
# MAGIC   src_dir = '/mnt/staging/incoming/test8/'
# MAGIC   src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC   
# MAGIC   
# MAGIC   src_data = [
# MAGIC     ['1', 2, 'nate1', 'smith', '333-333-3333', False, 1, '123454'],
# MAGIC     ['2', 3, 'danielle1', 'ravi', '444-444-4444', False, 2, '123454'],
# MAGIC     ['74', 4, 'adrienne1', 'watts', '555-55-5555', False, 3, '123454'],
# MAGIC     ['84', 5, 'shaun1', 'mcadams', '987-456-7890', False, 4, '123454'],
# MAGIC     ['94', 6, 'joe1', 'smith', '987-456-7890', False, 5, '123454'],
# MAGIC   ]
# MAGIC   
# MAGIC   src_dir = '/mnt/staging/incoming/test9/'
# MAGIC   src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC   
# MAGIC   src_data = [
# MAGIC     ['1', 2, 'nate2', 'smith', '111-333-3333', False, 1, '123454'],
# MAGIC     ['2', 3, 'danielle1', 'ravi', '444-444-4444', False, 2, '123454'],
# MAGIC     ['74', 4, 'adrienne2', 'watts', '444-55-5555', False, 3, '123454'],
# MAGIC     ['84', 5, 'shaun1', 'mcadams', '987-456-7890', False, 4, '123454'],
# MAGIC     ['94', 6, 'joe1', 'smith', '987-456-7890', False, 5, '123454'],
# MAGIC   ]
# MAGIC   
# MAGIC   src_dir = '/mnt/staging/incoming/test10/'
# MAGIC   src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC   
# MAGIC   
# MAGIC   src_data = [
# MAGIC     ['1', 2, 'nate1', 'smith', '333-333-3333', False, 1, '123454'],
# MAGIC     ['2', 3, 'danielle1', 'ravi', '444-444-4444', False, 2, '123454'],
# MAGIC     ['74', 4, 'adrienne1', 'watts', '555-55-5555', False, 3, '123454'],
# MAGIC     ['84', 5, 'shaun1', 'mcadams', '987-456-7890', False, 4, '123454'],
# MAGIC     ['94', 6, 'joe1', 'smith', '987-456-7890', False, 5, '123454'],
# MAGIC   ]
# MAGIC   
# MAGIC   src_dir = '/mnt/staging/incoming/test11/'
# MAGIC   src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC   
# MAGIC   src_data = [
# MAGIC     ['1', 2, 'nate2', 'smith', '111-333-3333', False, 1, '123454'],
# MAGIC     ['2', 3, 'danielle1', 'ravi', '444-444-4444', False, 2, '123454'],
# MAGIC     ['94', 6, 'joe1', 'smith', '987-456-7890', False, 5, '123454'],
# MAGIC   ]
# MAGIC   
# MAGIC   src_dir = '/mnt/staging/incoming/test12/'
# MAGIC   src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC   src_df.write.mode('overwrite').parquet(src_dir)
# MAGIC   
# MAGIC   

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC url = 'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-01.csv'
# MAGIC download_dir = '/tmp/download/taxi'
# MAGIC dst_dir =  '/mnt/staging/incoming/taxi/'
# MAGIC if not hc.filesystem.exists(dst_dir):
# MAGIC   download_data(url, download_dir)
# MAGIC   stage_data(download_dir, dst_dir)

# COMMAND ----------

create_data()

# COMMAND ----------

tear_down()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test1/'
# MAGIC 
# MAGIC dst_db = 'testing'
# MAGIC dst_schema = 'dto'
# MAGIC dst_tbl = 'test'
# MAGIC options = {
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "wh_schema": "TRUSTED",
# MAGIC   "wh_secure_schema": "TRUSTED_SECURE",
# MAGIC   "wh_tbl": f"{dst_schema}_TEST",
# MAGIC   "primary_key_cols": "id",
# MAGIC   "delete_col": "",
# MAGIC   "modification_col": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "format": "parquet",
# MAGIC   "flatten": "False",
# MAGIC   "flatten_col": "",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "false",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC print(results)

# COMMAND ----------

import json
import ast

result_map = ast.literal_eval(results) 
assert result_map['numOutputRows'] == '4'
assert result_map['has_errors'] == 'false'

assert spark.table(f'{dst_db}.{dst_tbl}').count() == 4

spark.table(f'{dst_db}.{dst_tbl}').display()



# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test2/'
# MAGIC 
# MAGIC dst_db = 'testing'
# MAGIC dst_schema = 'dto'
# MAGIC dst_tbl = 'test'
# MAGIC options = {
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_schema": dst_schema,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "primary_key_cols": "id",
# MAGIC   "delete_col": "",
# MAGIC   "modification_col": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "format": "parquet",
# MAGIC   "flatten": "False",
# MAGIC   "flatten_col": "",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "false",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC print(results)

# COMMAND ----------

import json
import ast

result_map = ast.literal_eval(results) 
assert result_map['numOutputRows'] == '4'
assert result_map['has_errors'] == 'false'
assert result_map['numTargetRowsUpdated'] == '0'

assert spark.table(f'{dst_db}.{dst_tbl}').count() == 4

spark.table(f'{dst_db}.{dst_tbl}').display()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test3/'
# MAGIC 
# MAGIC dst_db = 'testing'
# MAGIC dst_schema = 'dto'
# MAGIC dst_tbl = 'test'
# MAGIC options = {
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_schema": dst_schema,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "primary_key_cols": "id",
# MAGIC   "delete_col": "",
# MAGIC   "modification_col": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "format": "parquet",
# MAGIC   "flatten": "False",
# MAGIC   "flatten_col": "",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "false",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC print(results)

# COMMAND ----------

import json
import ast

result_map = ast.literal_eval(results) 
assert result_map['numOutputRows'] == '4'
assert result_map['has_errors'] == 'false'
assert result_map['numTargetRowsUpdated'] == '3'

assert spark.table(f'{dst_db}.{dst_tbl}').count() == 4

spark.table(f'{dst_db}.{dst_tbl}').display()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test4/'
# MAGIC 
# MAGIC dst_db = 'testing'
# MAGIC dst_schema = 'dto'
# MAGIC dst_tbl = 'test'
# MAGIC options = {
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_schema": dst_schema,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "primary_key_cols": "id",
# MAGIC   "delete_col": "",
# MAGIC   "modification_col": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "format": "parquet",
# MAGIC   "flatten": "False",
# MAGIC   "flatten_col": "",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "false",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC print(results)

# COMMAND ----------

import json
import ast

result_map = ast.literal_eval(results) 
assert result_map['numOutputRows'] == '4'
assert result_map['has_errors'] == 'false'
assert result_map['numTargetRowsUpdated'] == '0'
assert result_map['numTargetRowsInserted'] == '4'

assert spark.table(f'{dst_db}.{dst_tbl}').count() == 8

spark.table(f'{dst_db}.{dst_tbl}').display()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test5/'
# MAGIC 
# MAGIC dst_db = 'testing'
# MAGIC dst_schema = 'dto'
# MAGIC dst_tbl = 'test'
# MAGIC options = {
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_schema": dst_schema,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "primary_key_cols": "id",
# MAGIC   "delete_col": "",
# MAGIC   "modification_col": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "format": "parquet",
# MAGIC   "flatten": "False",
# MAGIC   "flatten_col": "",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "false",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC print(results)

# COMMAND ----------

import json
import ast

result_map = ast.literal_eval(results) 
assert result_map['numOutputRows'] == '9'
assert result_map['has_errors'] == 'false'
assert result_map['numTargetRowsUpdated'] == '3'
assert result_map['numTargetRowsInserted'] == '1'

assert spark.table(f'{dst_db}.{dst_tbl}').count() == 9

spark.table(f'{dst_db}.{dst_tbl}').display()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test6/'
# MAGIC 
# MAGIC dst_db = 'testing'
# MAGIC dst_schema = 'dto'
# MAGIC dst_tbl = 'test'
# MAGIC options = {
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_schema": dst_schema,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "primary_key_cols": "id",
# MAGIC   "delete_col": "is_deleted",
# MAGIC   "modification_col": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "format": "parquet",
# MAGIC   "flatten": "False",
# MAGIC   "flatten_col": "",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "false",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC print(results)

# COMMAND ----------

import json
import ast

result_map = ast.literal_eval(results) 
assert result_map['numOutputRows'] == '7'
assert result_map['has_errors'] == 'false'
assert result_map['numTargetRowsUpdated'] == '0'
assert result_map['numTargetRowsInserted'] == '0'
assert result_map['numTargetRowsDeleted'] == '2'

assert spark.table(f'{dst_db}.{dst_tbl}').count() == 7

spark.table(f'{dst_db}.{dst_tbl}').orderBy('id').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Should not load data from the past

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test1/'
# MAGIC 
# MAGIC dst_db = 'testing'
# MAGIC dst_schema = 'dto'
# MAGIC dst_tbl = 'test'
# MAGIC options = {
# MAGIC   "time_now": "2020-01-04T15:15:33.000+0000",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_schema": dst_schema,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "primary_key_cols": "id",
# MAGIC   "delete_col": "is_deleted",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "format": "parquet",
# MAGIC   "flatten": "False",
# MAGIC   "flatten_col": "",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "false",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC print(results)

# COMMAND ----------

import json
import ast

result_map = ast.literal_eval(results) 
assert result_map['numOutputRows'] == '8'
assert result_map['has_errors'] == 'false'
assert result_map['numTargetRowsUpdated'] == '0'
assert result_map['numTargetRowsInserted'] == '1'
assert result_map['numTargetRowsDeleted'] == '0'

assert spark.table(f'{dst_db}.{dst_tbl}').count() == 8

spark.table(f'{dst_db}.{dst_tbl}').orderBy('id').display()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test1/'
# MAGIC 
# MAGIC dst_db = 'testing'
# MAGIC dst_schema = 'dto'
# MAGIC dst_tbl = 'test'
# MAGIC options = {
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_schema": dst_schema,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "primary_key_cols": "id",
# MAGIC   "delete_col": "is_deleted",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "format": "parquet",
# MAGIC   "flatten": "False",
# MAGIC   "flatten_col": "",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "false",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC print(results)

# COMMAND ----------

import json
import ast

result_map = ast.literal_eval(results) 
assert result_map['numOutputRows'] == '8'
assert result_map['has_errors'] == 'false'
assert result_map['numTargetRowsUpdated'] == '2'
assert result_map['numTargetRowsInserted'] == '0'
assert result_map['numTargetRowsDeleted'] == '0'

assert spark.table(f'{dst_db}.{dst_tbl}').count() == 8

spark.table(f'{dst_db}.{dst_tbl}').orderBy('id').display()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test7/'
# MAGIC 
# MAGIC dst_db = 'testing'
# MAGIC dst_schema = 'dto'
# MAGIC dst_tbl = 'test'
# MAGIC options = {
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_schema": dst_schema,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "primary_key_cols": "id, id2",
# MAGIC   "delete_col": "is_deleted",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "format": "parquet",
# MAGIC   "flatten": "False",
# MAGIC   "flatten_col": "",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "false",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC print(results)

# COMMAND ----------

import json
import ast

result_map = ast.literal_eval(results) 
assert result_map['numOutputRows'] == '5'
assert result_map['has_errors'] == 'false'
assert result_map['numTargetRowsUpdated'] == '0'
assert result_map['numTargetRowsInserted'] == '5'
assert result_map['numTargetRowsDeleted'] == '0'

assert spark.table(f'{dst_db}.{dst_tbl}').count() == 13

spark.table(f'{dst_db}.{dst_tbl}').orderBy('id').display()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_dir = '/tmp/staging/moserit/jira/issues/'
# MAGIC 
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'issues'
# MAGIC options = {
# MAGIC   "flatten": "True",
# MAGIC   "flatten_col": "issues:fields",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "primary_key_cols": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "format": "json",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "false",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC spark.table('testing.issues').count()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC hc = honeycomb.Honeycomb(spark, client)
# MAGIC 
# MAGIC data = r'''
# MAGIC ---
# MAGIC metadata:
# MAGIC   columns:
# MAGIC   - column: ssn
# MAGIC     metadata:
# MAGIC     - hc_secure: 'true'
# MAGIC '''
# MAGIC 
# MAGIC config_file = '/tmp/test.yml'
# MAGIC hc.filesystem.write(config_file, data, overwrite=True)
# MAGIC #path = f'/dbfs/{path}'
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test1/'
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'test1_secure'
# MAGIC dst_secure_dir = '/tmp/secure/testing/test1'
# MAGIC 
# MAGIC hc.filesystem.mkdirs('/tmp/secure/testing/test1')
# MAGIC 
# MAGIC options = {
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": dst_secure_dir,
# MAGIC   "primary_key_cols": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": config_file,
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC df = spark.table('testing_secure.test1_secure')
# MAGIC 
# MAGIC assert 'hc_secure' in df.schema['ssn'].metadata
# MAGIC assert dst_secure_dir in hc.table.location('testing_secure.test1_secure') 

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test1/'
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'test_sort'
# MAGIC 
# MAGIC options = {
# MAGIC   "sort_by_cols": "first_name, last_name",
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": config_file,
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test1/'
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'test_partitioned'
# MAGIC 
# MAGIC options = {
# MAGIC   "partition_cols": "first_name, last_name",
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC assert hc.table.is_partitioned('testing.test_partitioned')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test1/'
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'test'
# MAGIC 
# MAGIC options = {
# MAGIC   "partition_cols": "",
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC assert hc.table.is_partitioned('testing_stg.test')
# MAGIC assert hc.table.is_delta('testing_stg.test')
# MAGIC 
# MAGIC options = {
# MAGIC   "partition_cols": "",
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "",
# MAGIC   "write_disposition": "overwrite",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC assert not hc.table.is_partitioned('testing_stg.test')
# MAGIC assert hc.table.is_delta('testing_stg.test')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test1/'
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'test'
# MAGIC 
# MAGIC options = {
# MAGIC   "partition_cols": "first_name, last_name",
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC assert hc.table.partitions('testing.test') == ['first_name', 'last_name']
# MAGIC assert hc.table.is_partitioned('testing.test')
# MAGIC assert hc.table.is_delta('testing.test')
# MAGIC 
# MAGIC options = {
# MAGIC   "partition_cols": "",
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "",
# MAGIC   "write_disposition": "overwrite",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC assert hc.table.partitions('testing.test') == []
# MAGIC assert not hc.table.is_partitioned('testing.test')
# MAGIC assert hc.table.is_delta('testing.test')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC from urllib.parse import urlparse
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test1/'
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'test'
# MAGIC 
# MAGIC options = {
# MAGIC   "partition_cols": "",
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC assert hc.table.partitions('testing.test') == ['first_name', 'last_name']
# MAGIC assert hc.table.is_partitioned('testing.test')
# MAGIC assert hc.table.is_delta('testing.test')
# MAGIC print(hc.table.location('testing.test'))
# MAGIC 
# MAGIC 
# MAGIC dst_dir = "/tmp/testing/test"
# MAGIC 
# MAGIC options = {
# MAGIC   "partition_cols": "",
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_dir": dst_dir,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "",
# MAGIC   "write_disposition": "overwrite",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC print(hc.table.location('testing.test'))
# MAGIC assert urlparse(hc.table.location('testing.test')).path == urlparse(dst_dir).path
# MAGIC assert hc.table.partitions('testing.test') == []
# MAGIC assert not hc.table.is_partitioned('testing.test')
# MAGIC assert hc.table.is_delta('testing.test')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC from urllib.parse import urlparse
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test1/'
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'test_zorder'
# MAGIC 
# MAGIC options = {
# MAGIC   "zorder_cols": "first_name, last_name",
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC from urllib.parse import urlparse
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test7/'
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'test_merge_schema_with_appended_columns'
# MAGIC 
# MAGIC options = {
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "id",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC orig_df = spark.table('testing.test_merge_schema_with_appended_columns')
# MAGIC orig_df.printSchema()
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test8/'
# MAGIC 
# MAGIC options['src_dir'] = src_dir
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC new_df = spark.table('testing.test_merge_schema_with_appended_columns')
# MAGIC new_df.printSchema()
# MAGIC 
# MAGIC 
# MAGIC assert 'age' not in orig_df.columns
# MAGIC assert 'start_date' not in orig_df.columns
# MAGIC 
# MAGIC assert 'age' in new_df.columns
# MAGIC assert 'start_date' in new_df.columns

# COMMAND ----------

create_data()
tear_down()

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/staging/incoming/test10/

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC 
# MAGIC import honeycomb
# MAGIC from urllib.parse import urlparse
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test9/'
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'test_slow_changing_dimensions'
# MAGIC 
# MAGIC options = {
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "id",
# MAGIC   "write_disposition": "merge2",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC orig_df = spark.table('testing.test_slow_changing_dimensions')
# MAGIC display(orig_df)

# COMMAND ----------

src_dir = '/mnt/staging/incoming/test10/'

options['src_dir'] = src_dir

results = dbutils.notebook.run(
  '/Shared/honeycomb/process', 
  1200, 
  options
)

new_df = spark.table('testing.test_slow_changing_dimensions')
display(new_df)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC 
# MAGIC import honeycomb
# MAGIC from urllib.parse import urlparse
# MAGIC 
# MAGIC tear_down()
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test11/'
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'test_slow_changing_dimension_delete'
# MAGIC 
# MAGIC options = {
# MAGIC   'load_type': 'full',
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "id",
# MAGIC   "write_disposition": "merge2",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC orig_df = spark.table('testing.test_slow_changing_dimension_delete')
# MAGIC display(orig_df)

# COMMAND ----------

src_dir = '/mnt/staging/incoming/test12/'

options['src_dir'] = src_dir

results = dbutils.notebook.run(
  '/Shared/honeycomb/process', 
  1200, 
  options
)

new_df = spark.table('testing.test_slow_changing_dimension_delete')
display(new_df)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC 
# MAGIC import honeycomb
# MAGIC from urllib.parse import urlparse
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/test11/'
# MAGIC dst_db = 'testing'
# MAGIC dst_tbl = 'load_type_full_merge_delete'
# MAGIC 
# MAGIC options = {
# MAGIC   'load_type': 'full',
# MAGIC   "flatten": "false",
# MAGIC   "flatten_col": "",
# MAGIC   "time_now": "",
# MAGIC   "modification_col": "hc_load_ts",
# MAGIC   "client": client,
# MAGIC   "src_dir": src_dir,
# MAGIC   "dst_db": dst_db,
# MAGIC   "dst_tbl": dst_tbl,
# MAGIC   "dst_secure_dir": "",
# MAGIC   "primary_key_cols": "id",
# MAGIC   "write_disposition": "merge",
# MAGIC   "merge_schema": "True",
# MAGIC   "export_dir": '',
# MAGIC   "export_format": "json",
# MAGIC   "export_options": "compression=gzip",
# MAGIC   "config_file": "",
# MAGIC   "read_options": "",
# MAGIC   "validate": "false",
# MAGIC   "transform": "false",
# MAGIC   "include_metadata": "true",
# MAGIC   "fail_on_error": "true",
# MAGIC   "postprocess_notebook": '',
# MAGIC   'cleanup': 'false',
# MAGIC }
# MAGIC 
# MAGIC results = dbutils.notebook.run(
# MAGIC   '/Shared/honeycomb/process', 
# MAGIC   1200, 
# MAGIC   options
# MAGIC )
# MAGIC 
# MAGIC orig_df = spark.table('testing.load_type_full_merge_delete')
# MAGIC display(orig_df)

# COMMAND ----------

src_dir = '/mnt/staging/incoming/test12/'

options['src_dir'] = src_dir

results = dbutils.notebook.run(
  '/Shared/honeycomb/process', 
  1200, 
  options
)

new_df = spark.table('testing.load_type_full_merge_delete')
display(new_df)
