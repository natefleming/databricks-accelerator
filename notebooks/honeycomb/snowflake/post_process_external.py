# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC dbutils.widgets.text('client', '')
# MAGIC dbutils.widgets.text('dst_db', '')
# MAGIC dbutils.widgets.text('dst_tbl', '')
# MAGIC dbutils.widgets.text('wh_schema', 'TRUSTED')
# MAGIC dbutils.widgets.text('wh_tbl', '')
# MAGIC dbutils.widgets.text('config', '')
# MAGIC dbutils.widgets.dropdown('include_metadata', 'false', ['true', 'false'])
# MAGIC dbutils.widgets.text('secret_scope', 'honeycomb-secrets-kv')
# MAGIC dbutils.widgets.text('connection_key', 'ds-snowflake-connection')

# COMMAND ----------

# MAGIC %python 
# MAGIC 
# MAGIC from distutils.util import strtobool
# MAGIC import json
# MAGIC 
# MAGIC client = dbutils.widgets.get('client')
# MAGIC dst_db = dbutils.widgets.get('dst_db')
# MAGIC dst_tbl = dbutils.widgets.get('dst_tbl')
# MAGIC wh_schema = dbutils.widgets.get('wh_schema')
# MAGIC wh_tbl = dbutils.widgets.get('wh_tbl')
# MAGIC config = dbutils.widgets.get('config')
# MAGIC secret_scope = dbutils.widgets.get('secret_scope')
# MAGIC connection_key = dbutils.widgets.get('connection_key')
# MAGIC include_metadata = bool(strtobool(dbutils.widgets.get('include_metadata')))
# MAGIC 
# MAGIC config = json.loads(config) if config else {}
# MAGIC 
# MAGIC if not client:
# MAGIC   raise ValueError('missing required option: client')
# MAGIC if not dst_db:
# MAGIC   raise ValueError('missing required option: dst_db')
# MAGIC if not dst_tbl:
# MAGIC   raise ValueError('missing required option: dst_tbl')
# MAGIC if not wh_tbl:
# MAGIC   raise ValueError('missing required option: wh_tbl')
# MAGIC   
# MAGIC print(f'client={client}')
# MAGIC print(f'dst_db={dst_db}')
# MAGIC print(f'dst_tbl={dst_tbl}')
# MAGIC print(f'wh_schema={wh_schema}')
# MAGIC print(f'wh_tbl={wh_tbl}')
# MAGIC print(f'include_metadata={include_metadata}')
# MAGIC print(f'config={config}')
# MAGIC print(f'secret_scope={secret_scope}')
# MAGIC print(f'connection_key={connection_key}')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC import honeycomb.snowflake
# MAGIC import honeycomb.metadata
# MAGIC 
# MAGIC from honeycomb.metadata import Metadata, MetadataHandler
# MAGIC from honeycomb.snowflake import SnowflakeProviderContext, SnowflakeWarehouseProvider, SnowflakeMetadataHandler
# MAGIC 
# MAGIC hc = honeycomb.Honeycomb(spark)
# MAGIC 
# MAGIC src_df = spark.table(f'{dst_db}.{dst_tbl}').limit(0)
# MAGIC 
# MAGIC metadata = Metadata(spark, src_df, config)
# MAGIC metadata.execute()
# MAGIC 
# MAGIC snowflake = honeycomb.snowflake.engine(
# MAGIC   spark, 
# MAGIC   secret_scope=secret_scope,
# MAGIC   connection_key=connection_key
# MAGIC )
# MAGIC 
# MAGIC with snowflake.connect().execution_options(autocommit=False) as connection:
# MAGIC     try:
# MAGIC         connection.execute("BEGIN")
# MAGIC    
# MAGIC         wh_stg: str = 'STG_TRUSTED'
# MAGIC         wh_secure_stg: str = 'STG_TRUSTED_SECURE'
# MAGIC         should_secure: bool = include_metadata and metadata.is_secure()
# MAGIC         stage: str = f'TRUSTED_SECURE.{wh_secure_stg}' if should_secure else '.'.join([x for x in [wh_schema, wh_stg] if x])
# MAGIC 
# MAGIC         stage_location: str = hc.table.stage_location(f'{dst_db}.{dst_tbl}')
# MAGIC         print(f'stage_location: {stage_location}')
# MAGIC 
# MAGIC         location = f'@{stage}/{stage_location}'
# MAGIC         print(f'location: {location}')
# MAGIC 
# MAGIC         provider_context: SnowflakeProviderContext = SnowflakeProviderContext(
# MAGIC             wh_schema=wh_schema,
# MAGIC             wh_tbl=wh_tbl,
# MAGIC             wh_view=f'VW_{wh_tbl}',
# MAGIC             location=location,
# MAGIC             dataframe=src_df
# MAGIC         )
# MAGIC         warehouse_provider: SnowflakeWarehouseProvider = SnowflakeWarehouseProvider(connection)
# MAGIC             
# MAGIC         warehouse_provider.create_external_delta_table(provider_context, metadata)
# MAGIC         wh_view: str = f'VW_{wh_tbl}'
# MAGIC         warehouse_provider.create_external_delta_view(provider_context, metadata)  
# MAGIC 
# MAGIC         if include_metadata:
# MAGIC             metadata_handler: honeycomb.metadata.MetadataHandler = SnowflakeMetadataHandler(warehouse_provider, provider_context)
# MAGIC             metadata.handle(metadata_handler)
# MAGIC                 
# MAGIC         print('committing...')
# MAGIC         connection.execute("COMMIT")
# MAGIC         print('complete.')
# MAGIC     except Exception as e:
# MAGIC         print(f'An exception has occurred: {e}')
# MAGIC         print('Rolling back...')
# MAGIC         connection.execute("ROLLBACK")
# MAGIC         raise
# MAGIC     finally:
# MAGIC         connection.close()
# MAGIC         
# MAGIC snowflake.dispose()
