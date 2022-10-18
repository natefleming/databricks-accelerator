# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC dbutils.widgets.text('dst_db', '')
# MAGIC dbutils.widgets.text('dst_tbl', '')
# MAGIC dbutils.widgets.text('dst_schema', '')
# MAGIC dbutils.widgets.text('dst_secure_schema', '')
# MAGIC dbutils.widgets.text('secret_scope', 'honeycomb-secrets-kv')
# MAGIC dbutils.widgets.text('connection_key', 'ds-mssql-connection')

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC dst_db = dbutils.widgets.get('dst_db')
# MAGIC dst_tbl = dbutils.widgets.get('dst_tbl')
# MAGIC dst_schema = dbutils.widgets.get('dst_schema')
# MAGIC dst_secure_schema = dbutils.widgets.get('dst_secure_schema')
# MAGIC secret_scope = dbutils.widgets.get('secret_scope')
# MAGIC connection_key = dbutils.widgets.get('connection_key')
# MAGIC 
# MAGIC if not dst_db:
# MAGIC   raise ValueError('missing required option: dst_db')
# MAGIC if not dst_tbl:
# MAGIC   raise ValueError('missing required option: dst_tbl')
# MAGIC   
# MAGIC print(f'dst_db={dst_db}')
# MAGIC print(f'dst_tbl={dst_tbl}')
# MAGIC print(f'dst_schema={dst_schema}')
# MAGIC print(f'dst_secure_schema={dst_secure_schema}')
# MAGIC print(f'secret_scope={secret_scope}')
# MAGIC print(f'connection_key={connection_key}')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.database
# MAGIC 
# MAGIC secret_scope = 'honeycomb-secrets-kv'
# MAGIC connection_key = 'ds-mssql-connection'
# MAGIC odbc = honeycomb.database.Odbc(
# MAGIC   spark,
# MAGIC   secret_scope=secret_scope,
# MAGIC   connection_key=connection_key
# MAGIC )
# MAGIC 
# MAGIC odbc = odbc.query('SELECT 1')
# MAGIC rs = odbc.execute()
# MAGIC 
# MAGIC import urllib
# MAGIC quoted = urllib.parse.quote_plus(connection_string)
# MAGIC sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

# COMMAND ----------

import pyspark.sql.types as T
import pyspark.sql.functions as F

from py4j.java_gateway import java_import
jvm = sc._gateway.jvm
java_import(jvm, 'org.apache.spark.sql.jdbc.JdbcDialect')
dialect = jvm.org.apache.spark.sql.jdbc.JdbcDialects.get('jdbc:sqlserver')

jdbc_type = 


