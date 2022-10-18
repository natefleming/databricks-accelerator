# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC import datetime
# MAGIC import pytz
# MAGIC import random
# MAGIC import string 
# MAGIC import time
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC client = 'nate-fleming'
# MAGIC honeycomb = honeycomb.Honeycomb(spark, client)
# MAGIC honeycomb.filesystem.mount('staging')
# MAGIC 
# MAGIC num_rows = 10000000
# MAGIC num_partitions = 1000
# MAGIC 
# MAGIC format = 'parquet'
# MAGIC output_dir = '/mnt/staging/incoming'
# MAGIC 
# MAGIC def row_generator_factory(schema):
# MAGIC  
# MAGIC   value_generator = {
# MAGIC     T.StringType():  lambda: ''.join(random.choices(string.ascii_uppercase, k=4)),
# MAGIC     T.IntegerType(): lambda: random.randint(0, 10),
# MAGIC     T.BooleanType(): lambda: random.choice([True, False]),
# MAGIC     T.DoubleType(): lambda: random.random(),
# MAGIC     T.FloatType(): lambda: random.random(),
# MAGIC   }
# MAGIC   def generate_row(index):   
# MAGIC     random.seed()
# MAGIC     row = [value_generator.get(f.dataType)() for f in schema.fields]
# MAGIC     return row
# MAGIC   
# MAGIC   return generate_row
# MAGIC 
# MAGIC 
# MAGIC schema = T.StructType([
# MAGIC   T.StructField('id', T.StringType()),
# MAGIC   T.StructField('first_name', T.StringType()),
# MAGIC   T.StructField('last_name', T.StringType()),
# MAGIC   T.StructField('age', T.IntegerType()),
# MAGIC   T.StructField('is_male', T.BooleanType()),
# MAGIC ])
# MAGIC 
# MAGIC row_generator = row_generator_factory(schema)
# MAGIC df = spark.range(0, num_rows, 1, num_partitions).rdd.map(row_generator).toDF(schema)
# MAGIC 
# MAGIC df.write.format(format).mode('overwrite').save(output_dir)
# MAGIC 
# MAGIC spark.read.format(format).load(output_dir).count()
