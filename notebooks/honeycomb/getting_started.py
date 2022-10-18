# Databricks notebook source
# MAGIC %md
# MAGIC # Getting Started 
# MAGIC ## Honeycomb Library for Databricks
# MAGIC This notebook contains examples of some of the common features offered by the honeycomb library

# COMMAND ----------

# MAGIC %md ### Installation
# MAGIC 
# MAGIC The honeycomb library can be installed through the UI, Databricks CLI or the REST Api.
# MAGIC If provisioned through Honeycomb Automation the library should be built and deployed to dbfs:/mnt/lib/honeycomb-0.0.1-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC ### Most commands offer a help method to aid when using interactively

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC honeycomb = honeycomb.Honeycomb(spark)
# MAGIC 
# MAGIC honeycomb.help()
# MAGIC honeycomb.filesystem.help()
# MAGIC honeycomb.filesystem.mount.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create text widgets and access the values

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbutils.widgets.text('client', 'smc-na')
# MAGIC client = dbutils.widgets.get('client')
# MAGIC print('client=%s' % client)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Provide a specific ADLS storage account, container, DBFS mount point

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC client = 'smc-na'
# MAGIC honeycomb = honeycomb.Honeycomb(spark, client)
# MAGIC mount_point, exception = honeycomb.filesystem.mount('trusted', storage_account='smcnaadlg2', mount_point='/mnt/trusted')
# MAGIC print(mount_point)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount a container using the default storage account and mount point
# MAGIC The storage account will be inferred from the client name

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC client = 'smc-na'
# MAGIC 
# MAGIC honeycomb = honeycomb.Honeycomb(spark, client)
# MAGIC mount_point, exception = honeycomb.filesystem.mount('trusted')
# MAGIC print(mount_point)

# COMMAND ----------

# MAGIC %conda env export --no-builds

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmount a container using the default mount point

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC honeycomb = honeycomb.Honeycomb(spark)
# MAGIC result, exception = honeycomb.filesystem.unmount('trusted')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmount a container using a specific mount point

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC honeycomb = honeycomb.Honeycomb(spark)
# MAGIC result, exception = honeycomb.filesystem.unmount(mount_point='/mnt/trusted')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Honeycomb Validation API

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate a dataframe using the API

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC import honeycomb.validate
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC   T.StructField('age', T.IntegerType()),
# MAGIC ])
# MAGIC 
# MAGIC src_data = [
# MAGIC   ['1', 'nate', 'fleming', 42],
# MAGIC   ['2', 'danielle', 'schwarz', 37],
# MAGIC   ['3', 'kevin', 'kschulke', 53],
# MAGIC   ['4', 'nate', 'weinreich', 49],
# MAGIC ]
# MAGIC 
# MAGIC src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC 
# MAGIC validation = honeycomb.validate.Validation(spark, src_df) \
# MAGIC   .is_not_null('id') \
# MAGIC   .is_primary_key('id') \
# MAGIC   .is_min('age', 0) \
# MAGIC   .is_max('age', 100) \
# MAGIC   .is_between('age', 0, 100) \
# MAGIC   .has_length_between('first_name', 0, 100) \
# MAGIC   .one_of('age', [42,37,53,49])
# MAGIC   
# MAGIC result = validation.execute()
# MAGIC 
# MAGIC display(result.correct_data)
# MAGIC #display(result.erroneous_data) 
# MAGIC #display(result.errors)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate a dataframe using constraints loaded from JSON

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import yaml
# MAGIC 
# MAGIC import honeycomb
# MAGIC import honeycomb.validate
# MAGIC import honeycomb.filesystem
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC   T.StructField('age', T.IntegerType()),
# MAGIC ])
# MAGIC 
# MAGIC src_data = [
# MAGIC   ['1', 'nate', 'fleming', 42],
# MAGIC   ['2', 'danielle', 'schwarz', 37],
# MAGIC   ['3', 'kevin', 'kschulke', 53],
# MAGIC   ['4', 'nate', 'weinreich', 49],
# MAGIC ]
# MAGIC 
# MAGIC data = r'''
# MAGIC ---
# MAGIC validation:
# MAGIC   columns:
# MAGIC   - column: id
# MAGIC     constraints:
# MAGIC     - name: is_not_null
# MAGIC     - name: is_primary_key
# MAGIC   - column: first_name
# MAGIC     constraints:
# MAGIC     - name: is_not_null
# MAGIC     - name: is_unique
# MAGIC     - name: has_length_between
# MAGIC       lower_bound: '0'
# MAGIC       upper_bound: '10'
# MAGIC     - name: text_matches_regex
# MAGIC       regex: "^[a-z]{3,10}$"
# MAGIC   - column: last_name
# MAGIC     constraints:
# MAGIC     - name: is_not_null
# MAGIC     - name: is_unique
# MAGIC     - name: one_of
# MAGIC       values:
# MAGIC       - 1
# MAGIC       - 2
# MAGIC       - 3
# MAGIC   - column: age
# MAGIC     constraints:
# MAGIC     - name: is_min
# MAGIC       value: 10
# MAGIC     - name: is_max
# MAGIC       value: 20
# MAGIC     - name: in_between
# MAGIC       lower_bound: 10
# MAGIC       upper_bound: 20
# MAGIC     - name: one_of
# MAGIC       values:
# MAGIC       - 1
# MAGIC       - 2
# MAGIC       - 3
# MAGIC       - 4
# MAGIC       - 5
# MAGIC '''
# MAGIC 
# MAGIC path = '/tmp/test.yaml'
# MAGIC dbutils.fs.put(path, data, overwrite=True)
# MAGIC 
# MAGIC # Read configuration file into a string
# MAGIC config, _ = honeycomb.filesystem.read(spark, path)
# MAGIC config = yaml.safe_load(config)
# MAGIC src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC 
# MAGIC validation = honeycomb.validate.Validation(spark, src_df, config)
# MAGIC result = validation.execute()
# MAGIC 
# MAGIC #display(result.correct_data)
# MAGIC #display(result.erroneous_data) 
# MAGIC display(result.errors)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC import honeycomb.transform
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC   T.StructField('age', T.IntegerType()),
# MAGIC ])
# MAGIC 
# MAGIC src_data = [
# MAGIC   ['1', 'nate', 'fleming', 10],
# MAGIC   ['2', 'danielle', 'schwarz', 37],
# MAGIC   ['3', 'kevin', 'kschulke', 53],
# MAGIC   ['4', 'nate', 'weinreich', 100],
# MAGIC ]
# MAGIC 
# MAGIC src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC 
# MAGIC validation = honeycomb.transform.Transformation(spark, src_df) \
# MAGIC   .min_max('age', -1, 1, 'age_out')
# MAGIC   
# MAGIC result = validation.execute()
# MAGIC 
# MAGIC display(result.data)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transform a dataframe using transformations loaded from JSON

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC import honeycomb.transform
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC   T.StructField('age', T.IntegerType()),
# MAGIC ])
# MAGIC 
# MAGIC src_data = [
# MAGIC   ['1', 'nate', 'fleming', 42],
# MAGIC   ['2', 'danielle', 'schwarz', 37],
# MAGIC   ['3', 'kevin', 'kschulke', 53],
# MAGIC   ['4', 'nate', 'weinreich', 49],
# MAGIC ]
# MAGIC 
# MAGIC data = r'''
# MAGIC ---
# MAGIC transformation:
# MAGIC   columns:
# MAGIC   - column: age
# MAGIC     transformations:
# MAGIC     - name: min_max
# MAGIC       lower_bound: -1
# MAGIC       upper_bound: 1
# MAGIC       output_column: new_age
# MAGIC '''
# MAGIC 
# MAGIC path = '/tmp/test.yaml'
# MAGIC dbutils.fs.put(path, data, overwrite=True)
# MAGIC 
# MAGIC # Read configuration file into a string
# MAGIC config, _ = honeycomb.filesystem.read(spark, path)
# MAGIC config = yaml.safe_load(config)
# MAGIC 
# MAGIC src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC 
# MAGIC validation = honeycomb.transform.Transformation(spark, src_df, config)
# MAGIC result = validation.execute()
# MAGIC 
# MAGIC display(result.data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add metadata to a dataframe schema using metadata api

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC import honeycomb.metadata
# MAGIC 
# MAGIC src_data = [
# MAGIC   ['1', 'nate', 'x'],
# MAGIC   ['2', 'danielle', 'y'],
# MAGIC   ['3', 'kevin', 'z'],
# MAGIC   ['4', 'nate', 'T'],
# MAGIC ]
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC ])
# MAGIC 
# MAGIC src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC 
# MAGIC metadata = honeycomb.metadata.Metadata(spark, src_df) \
# MAGIC   .add('id', 'foo', 'bar') \
# MAGIC   .add('id', 'pii', True)
# MAGIC 
# MAGIC result = metadata.execute()
# MAGIC 
# MAGIC df = result.data
# MAGIC 
# MAGIC print(df.schema.json())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add metadata to a dataframe schema using metadata loaded from JSON

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC import honeycomb.metadata
# MAGIC 
# MAGIC src_data = [
# MAGIC   ['1', 'nate', 'x'],
# MAGIC   ['2', 'danielle', 'y'],
# MAGIC   ['3', 'kevin', 'z'],
# MAGIC   ['4', 'nate', 'T'],
# MAGIC ]
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC ])
# MAGIC 
# MAGIC src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC 
# MAGIC data = r'''
# MAGIC ---
# MAGIC metadata:
# MAGIC   columns:
# MAGIC   - column: id
# MAGIC     metadata:
# MAGIC     - name: foo
# MAGIC       value: bar
# MAGIC     - name: baz
# MAGIC       value: 'true'
# MAGIC   - column: first_name
# MAGIC     metadata:
# MAGIC     - name: phi
# MAGIC       value: 'false'
# MAGIC '''
# MAGIC 
# MAGIC path = '/tmp/test.yaml'
# MAGIC dbutils.fs.put(path, data, overwrite=True)
# MAGIC 
# MAGIC # Read configuration file into a string
# MAGIC config, _ = honeycomb.filesystem.read(spark, path)
# MAGIC config = yaml.safe_load(config)
# MAGIC 
# MAGIC metadata = honeycomb.metadata.Metadata(spark, src_df, config)
# MAGIC 
# MAGIC result = metadata.execute()
# MAGIC 
# MAGIC df = result.data
# MAGIC 
# MAGIC print(df.schema.json())

# COMMAND ----------

# MAGIC %md
# MAGIC ### The honeycomb library provides an API which generates suggested validation constraints for a dataset. The results of these suggestion can be used to populate configuration files

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.suggestion
# MAGIC 
# MAGIC import pyspark.sql.functions as F
# MAGIC import pyspark.sql.types as T
# MAGIC 
# MAGIC src_data = [
# MAGIC   ['1', 'nate', None, 'fleming', 42],
# MAGIC   ['2', 'danielle', 'jolene', 'fleming', 37],
# MAGIC   ['3', 'kevin', 'jolene', 'schulke', 50],
# MAGIC   ['4', 'nate', 'natesmiddle', 'wienrich', 50],
# MAGIC   ['5', 'jeff', 'william', 'preus', 43],
# MAGIC   ['6', 'damien', 'nicholas', 'hurm', 42],
# MAGIC ]
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('middle_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC   T.StructField('age', T.IntegerType()),
# MAGIC ])
# MAGIC 
# MAGIC src_df = spark.createDataFrame(src_data, schema=schema)
# MAGIC src_df.show()
# MAGIC 
# MAGIC suggestion = honeycomb.suggestion.Suggestion(spark, src_df)
# MAGIC suggestion_results = suggestion.execute()
# MAGIC print(suggestion_results)
# MAGIC print(suggestion_results.to_dict())
# MAGIC print(yaml.safe_dump(suggestion_results.to_dict(), sort_keys=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Honeycomb allows for a dataset's schema to be overridden in the even that it can not be inferred or if data types should be changed. The schema can be provided in YAML format. The code segment below describes how to use the Spark API to generate this YAML document

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import yaml
# MAGIC import pyspark.sql.types as T
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC ])
# MAGIC 
# MAGIC print(yaml.safe_dump(schema.json())
# MAGIC 
# MAGIC #{"fields":[{"metadata":{},"name":"id","nullable":true,"type":"string"},{"metadata":{},"name":"first_name","nullable":true,"type":"string"},{"metadata":{},"name":"last_name","nullable":true,"type":"string"}],"type":"struct"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Appending Data to the Tags Column
# MAGIC Honeycomb offers the ability to collect row based metadata. This is implemented as a JSON string stored in the 'tags' column. You can easily use the **honeycomb.udf.append_tag(col, key, value)** to add a value to this column

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import pyspark.sql.functions as F
# MAGIC import honeycomb
# MAGIC import honeycomb.udf 
# MAGIC import json
# MAGIC 
# MAGIC hc = honeycomb.Honeycomb(spark, 'nate-fleming')
# MAGIC hc.filesystem.mount('staging')
# MAGIC 
# MAGIC src_dir = '/mnt/staging/incoming/'
# MAGIC df = spark.read.parquet(src_dir).withColumn('filename', F.input_file_name()).withColumn('tags', F.lit('{ "test": "1"}'))
# MAGIC df = df.withColumn('tags', honeycomb.udf.append_tag('tags', F.lit('hc_input_file_name'), F.input_file_name()))
# MAGIC df.show()
