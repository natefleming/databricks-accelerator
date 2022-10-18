# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC dbutils.widgets.text('container', 'trusted')
# MAGIC dbutils.widgets.text('storage_account', 'smcnaadlg2')
# MAGIC dbutils.widgets.text('src_path', '/mnt/trusted/source/PRD_USD_APP_information_schema.csv')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import os
# MAGIC import re
# MAGIC 
# MAGIC container: str = dbutils.widgets.get('container')
# MAGIC src_path: str = dbutils.widgets.get('src_path')
# MAGIC storage_account: str = dbutils.widgets.get('storage_account')
# MAGIC   
# MAGIC print(f'container: {container}')
# MAGIC print(f'src_path: {src_path}')
# MAGIC print(f'storage_account: {storage_account}')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC hc = honeycomb.Honeycomb(spark)
# MAGIC 
# MAGIC result = hc.filesystem.mount(container, storage_account=storage_account)
# MAGIC print(result)

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
# MAGIC src_df.createOrReplaceTempView('information_schema')
# MAGIC 
# MAGIC display(src_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Execute Queries Against the "Filtered" Source Information Schema In CSV Located At $src_path Using the View Name "information_schema"
# MAGIC ## Example: SELECT * FROM information_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC SELECT count(1) FROM information_schema
