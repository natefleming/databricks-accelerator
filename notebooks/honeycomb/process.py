# Databricks notebook source
# MAGIC %md
# MAGIC #Honeycomb ELT
# MAGIC 
# MAGIC This purpose of this application is to provide the core ELT processing logic (tranformation, validation, metadata) for honeycomb.
# MAGIC 
# MAGIC ### Arguments
# MAGIC 
# MAGIC  * client - The client name. **Required**
# MAGIC  * secret-scope - The databricks secret scope. Default: honeycomb-secrets-kv
# MAGIC  * storage-account - The ADLS storage account name. Default: [client]adlg2
# MAGIC  * storage-account-key - The ADLS storage account key. Default: rs-data-lake-account-key
# MAGIC  * src-dir - The source directory for incoming files. **Required** 
# MAGIC  * dst-db - The destination database. Default: [client]
# MAGIC  * dst-tbl - The destination table. Default: basename(dst-dir). **Required if dst-dir is not provided**
# MAGIC  * dst-secure-db - The destination database for secure data. Default: [dst-db]_SECURE
# MAGIC  * dst-secure-schema - The schema for the secure datasets
# MAGIC  * dst-dir - The destination directory. **Required if dst-tbl is not provided**
# MAGIC  * dst-secure-dir - The destination directory for secure data. 
# MAGIC  * stg-db - The staging database. Default: [client]_STG
# MAGIC  * stg-tbl - The staging table. Default: to dst-tbl
# MAGIC  * stg-dir - The staging directory. 
# MAGIC  * err-db - The error database. Default: [client]_ERR
# MAGIC  * err-tbl - The error table. Default: to dst-tbl
# MAGIC  * err-dir - The error directory. 
# MAGIC  * result-db - The result database. Default: err-db
# MAGIC  * result-tbl - The result table. Default: RESULTS
# MAGIC  * result-dir - The result directory. 
# MAGIC  * export-dir - The export directory.
# MAGIC  * export-format - The export format.  [parquet, avro, csv, json, xml, excel]. Default: parquet
# MAGIC  * wh-schema - The warehouse schema
# MAGIC  * wh-secure-schema - The warehouse secure schema
# MAGIC  * wh-tbl - The warehouse table
# MAGIC  * primary-key-cols - A comma separated list of primary keys. 
# MAGIC  * delete-col - The column which determines whether this row should be deleted on merge.
# MAGIC  * modification-col - A timestamp column which determines the last modification date of a row. This is used along with primary keys during merges
# MAGIC  * partition-cols - A comma separated list of columns to partition by. 
# MAGIC  * zorder-cols - A comma separated list of columns to zorder by.
# MAGIC  * write-disposition - The write mode. [overwrite, append, merge, error]. Default: merge
# MAGIC  * load-type - The load type. [full, incremental]. Default: full
# MAGIC  * merge-schema - A flag indicating whether or not this tables supports and evolving schema. Default: false
# MAGIC  * overwrite-schema - A flag indicating whether or not this the schema should be overwritten. Default: false
# MAGIC  * num-partitions - The number of partitions the source dataframe should use.
# MAGIC  * format - The incoming file format. [parquet, avro, csv, json, xml, excel]. Default: Inferred from file extension
# MAGIC  * flatten - A flag indicating whether or not incoming json needs to be flattened. Default: false
# MAGIC  * flatten-col - [column[:column]] The column which contains the array to be exploded. Child StructTypes can be flattened using 'Array:Struct' notation. Default: The first column in the schema which is of type array. 
# MAGIC  * tags - A comma seperated list of arbitry key value pairs. This are used to included addtional meta data related to the datasource. [key1=value, key2=value2]
# MAGIC  * validate - A flag indicating whether or not validation should occur. If configuration is not provided then validation will only occur on provided primary keys. Default: false
# MAGIC  * transform - A flag indicating whether or not transformation should occur. Default: false
# MAGIC  * include-metadata - A flag indicating whether or not include metadata. Default: false
# MAGIC  * rename-columns - A flag indicating whether or not special characters and whitespace should be removed from columns: Default: true
# MAGIC  * config - An inline string containing configuration.
# MAGIC  * config-file - A path to this configuration file.
# MAGIC  * fail-on-error - A flag indicating whether the application should continue on error
# MAGIC  * cleanup - A flag indicating whether incoming files should be removed after processed. Default: true
# MAGIC  * read-options - A comma seperated list of arbitry key value pairs. These are provided to the source data frame reader. [key1=value, key2=value2]
# MAGIC  * postprocess-notebook - The path to a notebook to be run at completion
# MAGIC  * notebook-timeout - The invoked notebook timeout in seconds - default: 300 seconds (5 minutes)
# MAGIC  * containers - A comma separated list of containers to mount - default: trusted, trusted-secure, staging, elt, export
# MAGIC  * time-now - The timestamp used as a point of reference when creating honeycomb load and modification dates. Default: datetime.datetime.now(timezone)
# MAGIC  * timezone - The timezone used when creating honeycomb load and modification dates. Default: America/New_York
# MAGIC 
# MAGIC ### Staging
# MAGIC 
# MAGIC The incoming data will intially be loaded into a staging database. This data undergoes partitioning and format conversion but is not transformed or validated. 
# MAGIC If the data is incremental/merge then the staging table will be partitioned by the current timestamp. If not then the staging table will be overwitten. 
# MAGIC If the partitioning strategy for the staging table is not compatible with the write mode (write_disposition) then the table will be repartitioned appropriately.
# MAGIC 
# MAGIC 
# MAGIC ### Enrichment
# MAGIC 
# MAGIC At this point it will be enriched with the following columns for processing.
# MAGIC 
# MAGIC * hc_key           - A single 64 bit key is created from the the primary keys (if provided).
# MAGIC * hc_hash          - A 32 bit hash of the entire source data row is created. 
# MAGIC * hc_load_ts       - The time this job started running
# MAGIC * hc_mod_ts        - For incremental/merge loads this is the timestamp that this application updated the data. NOTE: This is not supported when merge-schema is true
# MAGIC * hc_tags          - This is a dictionary of key=value pairs. It can be used to add arbitrary information to a row (example: source=MYSOURCE, file=/mnt/file.txt)
# MAGIC * hc_current       - A boolean signifying that this is the current version of the row.
# MAGIC * hc_expiration_ts - The time at which this row was replaced by a new row when using slow changing dimensions.
# MAGIC * hc_deleted       - A boolean signifying that this row has been deleted.
# MAGIC 
# MAGIC 
# MAGIC ### Transformation
# MAGIC 
# MAGIC Honeycomb leverages the Honeycomb transformation library. This library allows for the ability to configure various transformations per column. This application has the ability
# MAGIC to transform from a configuration file provided either as a YAML string or file or to skip transformations. 
# MAGIC Please reference: getting_started for examples of transformation configuration. 
# MAGIC 
# MAGIC ```python
# MAGIC # Example transformation configuration
# MAGIC ---
# MAGIC transformation:
# MAGIC   columns:
# MAGIC   - column: age
# MAGIC     transformations:
# MAGIC     - name: min_max
# MAGIC       lower_bound: -1
# MAGIC       upper_bound: 1
# MAGIC       ouput_column: age_out
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC ### Validation
# MAGIC 
# MAGIC Honeycomb leverages the Honeycomb validation library. This library allows for the ability to configure various constraints per column. This application has the ability
# MAGIC to validate from a configuration file provided either as a YAML string or file, just primary keys or to skip validation. 
# MAGIC Once data is staged it is validated before being merged into the destination database. 
# MAGIC Please reference: getting_started for examples of constraint configuration
# MAGIC ```python
# MAGIC # Example validation configuration
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
# MAGIC       values: [ 'smith', 'jones', 'williams', 'johnson' ]
# MAGIC   - column: age
# MAGIC     constraints:
# MAGIC     - name: is_min
# MAGIC       value: 1
# MAGIC     - name: is_max
# MAGIC       value: '50'
# MAGIC     - name: in_between
# MAGIC       lower_bound: 10
# MAGIC       upper_bound: 20
# MAGIC     - name: one_of
# MAGIC       values: [ 1, 2, 3, 4, 5 ]
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC ### Metadata
# MAGIC 
# MAGIC Spark schema metadata is leveraged to store additional processing information.
# MAGIC  
# MAGIC * Each primary key column - hc_primary_key = true
# MAGIC * The modification column - hc_modification_col = true
# MAGIC * The delete column - hc_delete_col = true
# MAGIC 
# MAGIC There are use cases where special considerations must be made when processing or analyizing columns. This application provides the ability to encode
# MAGIC arbitrary metadata into the data frame schema. Metadata can be provided as a YAML string or file configured in the config/config_file parameters.
# MAGIC ```python
# MAGIC # Example metadata configuration
# MAGIC ---
# MAGIC metadata:
# MAGIC   columns:
# MAGIC   - column: id
# MAGIC     metadata:
# MAGIC     - hc_primary_key: 'true'
# MAGIC   - column: is_deleted
# MAGIC     metadata:
# MAGIC     - hc_delete_col: 'true'
# MAGIC   - column: ssn
# MAGIC     metadata:
# MAGIC     - hc_pii: 'true'
# MAGIC     
# MAGIC # Example with column masking and row based filtering
# MAGIC ---
# MAGIC metadata:
# MAGIC   dataset:
# MAGIC     metadata:
# MAGIC     - hc_row_access_roles:
# MAGIC       - ACCOUNTADMIN
# MAGIC       - MY_INSIGHTS_ROLE
# MAGIC     - hc_row_access:
# MAGIC       columns:
# MAGIC       - column: id
# MAGIC         operator: "="
# MAGIC       - column: lastName
# MAGIC         operator: "!="
# MAGIC   columns:
# MAGIC   - column: firstName
# MAGIC     metadata:
# MAGIC     - hc_masking_roles:
# MAGIC       - ACCOUNTADMIN
# MAGIC       - MY_INSIGHTS_ROLE
# MAGIC     - hc_masking: 'true'
# MAGIC ```
# MAGIC Metadata can be accessed by querying the table/column schema.
# MAGIC 
# MAGIC ```sql
# MAGIC -- SQL example retrieving metadata
# MAGIC DESCRIBE mydatabase.foo myid
# MAGIC ```
# MAGIC or
# MAGIC ```python
# MAGIC # Python example retrieving metadata
# MAGIC metadata = spark.table('mydatabase.mytable').schema['mycolumn'].metadata
# MAGIC ```
# MAGIC 
# MAGIC ### Schema
# MAGIC 
# MAGIC Custom schema for incoming data can be provided through the configuration. 
# MAGIC The format for this schema must be compatible with ```pyspark.sql.types.StructType.fromJson(schema: dict)```
# MAGIC ```python
# MAGIC # Example schema configuration
# MAGIC ---
# MAGIC schema:
# MAGIC   type: struct
# MAGIC   fields:
# MAGIC   - name: a
# MAGIC     type: integer
# MAGIC     nullable: true
# MAGIC     metadata: {}
# MAGIC   - name: b
# MAGIC     type: long
# MAGIC     nullable: false
# MAGIC     metadata: {}
# MAGIC   - name: c
# MAGIC     type: timestamp
# MAGIC     nullable: true
# MAGIC     metadata: {}
# MAGIC ```
# MAGIC 
# MAGIC ### Merging
# MAGIC 
# MAGIC Once the data has been staged it is processed into the datalake. 
# MAGIC This application supports upserts and deletion and replacement of data by providing values for primary key columns, modification column, and deletion columns and write_disposition.
# MAGIC 
# MAGIC 
# MAGIC ### Cleanup
# MAGIC 
# MAGIC Once the incoming data has been successfully processed and loaded into the data lake the original files are deleted.
# MAGIC 
# MAGIC 
# MAGIC ### Results
# MAGIC 
# MAGIC The results of the job are exported in json format.
# MAGIC For example:
# MAGIC ```javascript
# MAGIC {
# MAGIC     'numOutputRows': '4',
# MAGIC     'numTargetRowsInserted': '0',
# MAGIC     'numTargetRowsUpdated': '4',
# MAGIC     'numTargetFilesAdded': '1',
# MAGIC     'numTargetFilesRemoved': '1',
# MAGIC     'numTargetRowsDeleted': '0',
# MAGIC     'numSourceRows': '4',
# MAGIC     'numTargetRowsCopied': '0',
# MAGIC     'timestamp': '2020-10-22T08:24:52-04:00',
# MAGIC     'has_errors': 'false'
# MAGIC }
# MAGIC ```
# MAGIC ```javascript
# MAGIC {
# MAGIC    "numOutputRows":"0",
# MAGIC    "numOutputBytes":"0",
# MAGIC    "numFiles":"0",
# MAGIC    "timestamp": "2020-10-22T08:24:52-04:00",
# MAGIC    "has_errors": "false"
# MAGIC    "errors":[
# MAGIC       {
# MAGIC          "column_name":"first_name",
# MAGIC          "constraint_name":"unique",
# MAGIC          "number_of_errors":2
# MAGIC       },
# MAGIC       {
# MAGIC          "column_name":"last_name",
# MAGIC          "constraint_name":"one_of",
# MAGIC          "number_of_errors":4
# MAGIC       },
# MAGIC       {
# MAGIC          "column_name":"age",
# MAGIC          "constraint_name":"min",
# MAGIC          "number_of_errors":4
# MAGIC       },
# MAGIC       {
# MAGIC          "column_name":"age",
# MAGIC          "constraint_name":"one_of",
# MAGIC          "number_of_errors":3
# MAGIC       }
# MAGIC    ]
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC ### NOTE
# MAGIC 
# MAGIC * Excel spreadsheets must be processed as individual files. If src_dir is a directory it will choose the first file in the directory. 
# MAGIC * Excel spreadsheet 'sheets' can be specified by providing appropriate parameters to read-options and export-options
# MAGIC * Excel spreadsheet reference https://github.com/crealytics/spark-excel
# MAGIC * XML files read-options require: rowTag=[xml element which represents a row].
# MAGIC * XML file reference https://github.com/databricks/spark-xml
# MAGIC * JSON files are assumed to have one record per line. If the files are multilined then you must provide read-options: multiline=true
# MAGIC 
# MAGIC ### TODO
# MAGIC 
# MAGIC  * Refactoring this application to be a spark application allows for more robust unit testing and less opportunity for deployment errors
# MAGIC  * It may be beneficial to split this into multiple applications. One for staging and one for processing.
# MAGIC  * Allow column meta data to be provided through parameters.
# MAGIC 
# MAGIC 
# MAGIC ### Known Issues
# MAGIC 
# MAGIC * Validation will fail if a configuration validation column does not exist in the incoming schema regardless of 'fail_on_error'. This may or may not be an issue.
# MAGIC * Using the merge write_disposition with 'merge_schema' set to true will fail when the the incoming schema is different then the destination. This needs some investigation.
# MAGIC 
# MAGIC ### Setting up the databricks CLI
# MAGIC 
# MAGIC Many of the configuration options available in Databricks are only provided through REST APIs or the Databricks CLI. 
# MAGIC 
# MAGIC From any terminal (ie The Azure Cloud Shell) install the databricks-cli
# MAGIC ```
# MAGIC pip install --upgrade databricks-cli
# MAGIC ```
# MAGIC Configure the databricks CLI for the appropriate instance. You can provide the optional --profile argument if you are working with multiple databricsk environments.
# MAGIC ```
# MAGIC databricks configure --profile [my-profile-name]
# MAGIC ```
# MAGIC This will prompt you for the databricks URL, your username and a password.
# MAGIC 
# MAGIC You can copy from the browser or the Azure portal (eg https://adb-1677694350714442.2.azuredatabricks.net/)
# MAGIC Your username will be your the same which you are signed in as. (eg nate.fleming@moser-inc.com)
# MAGIC The password is an **access token** generated in databricks by selecting **user settings** and then **generate token**
# MAGIC 
# MAGIC ### Create the Azure Key Vault Secret Scope 
# MAGIC 
# MAGIC Currently, Databricks does not offer an API for creating a secret scope backed by Azure KeyVault. This must be done manually.
# MAGIC 
# MAGIC From a browser navigate to https://[databricks-instance]#secrets/createScope (eg https://adb-1677694350714442.2.azuredatabricks.net/#secrets/createScope)
# MAGIC   
# MAGIC This will prompt you for the scope name, key vault DNS name and the key vault resource name.
# MAGIC 
# MAGIC Provide a name for the secret scope (ie honeycomb-secrets-kv).
# MAGIC The key vault DNS as well as the resource name can be copied by navigating to the appropropriate key vault in the Azure Portal and then selecting the properties

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define application parameters

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbutils.widgets.text('client', '')
# MAGIC dbutils.widgets.text('secret_scope', 'honeycomb-secrets-kv')
# MAGIC dbutils.widgets.text('storage_account', '')
# MAGIC dbutils.widgets.text('storage_account_key', 'rs-data-lake-account-key')
# MAGIC dbutils.widgets.text('src_dir', '')
# MAGIC 
# MAGIC dbutils.widgets.text('dst_db', '')
# MAGIC dbutils.widgets.text('dst_tbl', '')
# MAGIC dbutils.widgets.text('dst_dir', '')
# MAGIC 
# MAGIC dbutils.widgets.text('dst_secure_db', '')
# MAGIC dbutils.widgets.text('dst_secure_dir', '')
# MAGIC 
# MAGIC dbutils.widgets.text('wh_schema', '')
# MAGIC dbutils.widgets.text('wh_secure_schema', '')
# MAGIC dbutils.widgets.text('wh_tbl', '')
# MAGIC 
# MAGIC dbutils.widgets.text('stg_db', '')
# MAGIC dbutils.widgets.text('stg_tbl', '')
# MAGIC dbutils.widgets.text('stg_dir', '')
# MAGIC 
# MAGIC dbutils.widgets.text('err_db', '')
# MAGIC dbutils.widgets.text('err_tbl', '')
# MAGIC dbutils.widgets.text('err_dir', '')
# MAGIC 
# MAGIC dbutils.widgets.text('result_db', '')
# MAGIC dbutils.widgets.text('result_tbl', '')
# MAGIC dbutils.widgets.text('result_dir', '')
# MAGIC 
# MAGIC dbutils.widgets.text('export_dir', '')
# MAGIC dbutils.widgets.dropdown('export_format', 'json', ['parquet', 'avro', 'csv', 'json', 'xml', 'excel'])
# MAGIC 
# MAGIC dbutils.widgets.text('primary_key_cols', '', 'primary_key_cols (csv)')
# MAGIC dbutils.widgets.text('delete_col', '')
# MAGIC dbutils.widgets.text('modification_col', '')
# MAGIC dbutils.widgets.text('partition_cols', '', 'partition_cols (csv)')
# MAGIC dbutils.widgets.text('zorder_cols', '', 'zorder_cols (csv)')
# MAGIC dbutils.widgets.text('num_partitions', '')
# MAGIC dbutils.widgets.dropdown('write_disposition', 'merge', ['overwrite', 'append', 'merge', 'merge2', 'scd', 'error'])
# MAGIC dbutils.widgets.dropdown('load_type', 'full', ['append', 'full', 'incremental'])
# MAGIC dbutils.widgets.dropdown('merge_schema', 'false', ['true', 'false'])
# MAGIC dbutils.widgets.dropdown('overwrite_schema', 'false', ['true', 'false'])
# MAGIC dbutils.widgets.dropdown('format', '', ['', 'parquet', 'avro', 'csv', 'json', 'xml', 'excel'])
# MAGIC dbutils.widgets.text('tags', '', 'tags (k1=v1, k2=v2, ...)')
# MAGIC dbutils.widgets.dropdown('validate', 'false', ['true', 'false'])
# MAGIC dbutils.widgets.dropdown('transform', 'false', ['true', 'false'])
# MAGIC dbutils.widgets.dropdown('rename_columns', 'true', ['true', 'false'])
# MAGIC dbutils.widgets.text('config_file', '', 'config_file (path)')
# MAGIC dbutils.widgets.text('config', '', 'config (yaml)')
# MAGIC dbutils.widgets.dropdown('fail_on_error', 'false', ['true', 'false'])
# MAGIC dbutils.widgets.dropdown('cleanup', 'true', ['true', 'false'])
# MAGIC dbutils.widgets.dropdown('flatten', 'false', ['true', 'false'])
# MAGIC dbutils.widgets.text('flatten_col', '')
# MAGIC dbutils.widgets.text('read_options', '', 'read_options (k1=v1, k2=v2, ...)')
# MAGIC dbutils.widgets.text('export_options', '', 'export_options (k1=v1, k2=v2, ...)')
# MAGIC dbutils.widgets.dropdown('include_metadata', 'false', ['true', 'false'])
# MAGIC dbutils.widgets.text('postprocess_notebook', '/Shared/honeycomb/snowflake/post_process_external')
# MAGIC dbutils.widgets.text('notebook_timeout_seconds', '300')
# MAGIC dbutils.widgets.text('containers', 'trusted, trusted-secure, staging, elt, export', 'containers (csv)')
# MAGIC dbutils.widgets.text('time_now', '')
# MAGIC dbutils.widgets.text('timezone', 'America/New_York')
# MAGIC dbutils.widgets.text('source_system_id', '')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Application parameters

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import re 
# MAGIC import os.path
# MAGIC import pytz
# MAGIC import time
# MAGIC import datetime
# MAGIC import dateutil.parser
# MAGIC 
# MAGIC from distutils.util import strtobool
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC client = dbutils.widgets.get('client')
# MAGIC secret_scope = dbutils.widgets.get('secret_scope')
# MAGIC storage_account = dbutils.widgets.get('storage_account')
# MAGIC storage_account_key = dbutils.widgets.get('storage_account_key')
# MAGIC primary_key_cols = [x.strip().lower() for x in dbutils.widgets.get('primary_key_cols').split(',') if x.strip()]
# MAGIC delete_col = dbutils.widgets.get('delete_col').strip().lower()
# MAGIC modification_col = dbutils.widgets.get('modification_col').strip().lower()
# MAGIC partition_cols = [x.strip().lower() for x in dbutils.widgets.get('partition_cols').split(',') if x.strip()]
# MAGIC zorder_cols = [x.strip().lower() for x in dbutils.widgets.get('zorder_cols').split(',') if x.strip()]
# MAGIC num_partitions = dbutils.widgets.get('num_partitions')
# MAGIC num_partitions = int(num_partitions) if num_partitions else None
# MAGIC 
# MAGIC src_dir = dbutils.widgets.get('src_dir')
# MAGIC 
# MAGIC dst_db = dbutils.widgets.get('dst_db')
# MAGIC dst_tbl = dbutils.widgets.get('dst_tbl')
# MAGIC dst_dir = dbutils.widgets.get('dst_dir')
# MAGIC dst_secure_db = dbutils.widgets.get('dst_secure_db')
# MAGIC dst_secure_dir = dbutils.widgets.get('dst_secure_dir')
# MAGIC 
# MAGIC wh_schema = dbutils.widgets.get('wh_schema')
# MAGIC wh_secure_schema = dbutils.widgets.get('wh_secure_schema')
# MAGIC wh_tbl = dbutils.widgets.get('wh_tbl')
# MAGIC 
# MAGIC stg_db = dbutils.widgets.get('stg_db')
# MAGIC stg_tbl = dbutils.widgets.get('stg_tbl')
# MAGIC stg_dir = dbutils.widgets.get('stg_dir')
# MAGIC 
# MAGIC err_db = dbutils.widgets.get('err_db')
# MAGIC err_tbl = dbutils.widgets.get('err_tbl')
# MAGIC err_dir = dbutils.widgets.get('err_dir')
# MAGIC 
# MAGIC result_db = dbutils.widgets.get('result_db')
# MAGIC result_tbl = dbutils.widgets.get('result_tbl')
# MAGIC result_dir = dbutils.widgets.get('result_dir')
# MAGIC 
# MAGIC export_dir = dbutils.widgets.get('export_dir')
# MAGIC export_format = dbutils.widgets.get('export_format').strip().lower()
# MAGIC 
# MAGIC write_disposition = dbutils.widgets.get('write_disposition').strip().lower()
# MAGIC load_type = dbutils.widgets.get('load_type').strip().lower()
# MAGIC merge_schema = bool(strtobool(dbutils.widgets.get('merge_schema')))
# MAGIC overwrite_schema = bool(strtobool(dbutils.widgets.get('overwrite_schema')))
# MAGIC format = dbutils.widgets.get('format')
# MAGIC tags = dbutils.widgets.get('tags')
# MAGIC validate = bool(strtobool(dbutils.widgets.get('validate')))
# MAGIC transform = bool(strtobool(dbutils.widgets.get('transform')))
# MAGIC include_metadata = bool(strtobool(dbutils.widgets.get('include_metadata')))
# MAGIC rename_columns = bool(strtobool(dbutils.widgets.get('rename_columns')))
# MAGIC config = dbutils.widgets.get('config')
# MAGIC config_file = dbutils.widgets.get('config_file')
# MAGIC fail_on_error = bool(strtobool(dbutils.widgets.get('fail_on_error')))
# MAGIC cleanup = bool(strtobool(dbutils.widgets.get('cleanup')))
# MAGIC flatten = bool(strtobool(dbutils.widgets.get('flatten')))
# MAGIC flatten_col = dbutils.widgets.get('flatten_col').strip().lower()
# MAGIC read_options = dbutils.widgets.get('read_options')
# MAGIC export_options = dbutils.widgets.get('export_options')
# MAGIC postprocess_notebook = dbutils.widgets.get('postprocess_notebook')
# MAGIC notebook_timeout_seconds = int(dbutils.widgets.get('notebook_timeout_seconds'))
# MAGIC containers = dbutils.widgets.get('containers')
# MAGIC timezone = pytz.timezone(dbutils.widgets.get('timezone'))
# MAGIC time_now = dbutils.widgets.get('time_now')
# MAGIC source_system_id = dbutils.widgets.get('source_system_id')
# MAGIC 
# MAGIC if not client:
# MAGIC   raise ValueError('missing required option: client')
# MAGIC if not src_dir:
# MAGIC   raise ValueError('missing required option: src_dir')
# MAGIC if not dst_dir and not dst_tbl:
# MAGIC   raise ValueError('missing required option: dst_dir or dst_tbl')
# MAGIC if config and config_file:
# MAGIC   raise ValueError('options config and config_file are mutually exclusive')
# MAGIC if merge_schema and overwrite_schema:
# MAGIC   raise ValueError('options merge_schema and overwrite_schema are mutually exclusive')
# MAGIC if num_partitions is not None and num_partitions <= 0:
# MAGIC   raise ValueError('option num_partitions must be greater than zero')
# MAGIC   
# MAGIC storage_account = storage_account if storage_account else re.sub('[^\\w]+', '', f'{client}adlg2') 
# MAGIC dst_db = dst_db if dst_db else client
# MAGIC dst_db = re.sub('[^\\w]+', '_', dst_db) if dst_db else None
# MAGIC dst_tbl = dst_tbl if dst_tbl else os.path.basename(dst_dir)
# MAGIC dst_tbl = re.sub('[^\\w]+', '_', dst_tbl) if dst_tbl else None
# MAGIC dst_dir = dst_dir if dst_dir else None
# MAGIC dst_secure_db = dst_secure_db if dst_secure_db else f'{dst_db}_SECURE'
# MAGIC dst_secure_db = re.sub('[^\\w]+', '_', dst_secure_db) if dst_secure_db else None
# MAGIC dst_secure_dir = dst_secure_dir if dst_secure_dir else None
# MAGIC wh_schema = wh_schema if wh_schema else None
# MAGIC wh_schema = re.sub('[^\\w]+', '_', wh_schema) if wh_schema else None
# MAGIC wh_secure_schema = wh_secure_schema if wh_secure_schema else f'{wh_schema}_SECURE' if wh_schema else None
# MAGIC wh_secure_schema = re.sub('[^\\w]+', '_', wh_secure_schema) if wh_secure_schema else None
# MAGIC stg_db = stg_db if stg_db else f'{dst_db}_STG' 
# MAGIC stg_db = re.sub('[^\\w]+', '_', stg_db) if stg_db else None
# MAGIC stg_tbl = stg_tbl if stg_tbl else os.path.basename(stg_dir) if stg_dir else dst_tbl
# MAGIC stg_tbl = re.sub('[^\\w]+', '_', stg_tbl) if stg_tbl else None
# MAGIC stg_dir = stg_dir if stg_dir else None
# MAGIC err_db = err_db if err_db else f'{dst_db}_ERR'
# MAGIC err_db = re.sub('[^\\w]+', '_', err_db) if err_db else None
# MAGIC err_tbl = err_tbl if err_tbl else os.path.basename(err_dir) if err_dir else dst_tbl
# MAGIC err_tbl = re.sub('[^\\w]+', '_', err_tbl) if err_tbl else None
# MAGIC err_dir = err_dir if err_dir else None
# MAGIC result_db = result_db if result_db else err_db
# MAGIC result_db = re.sub('[^\\w]+', '_', result_db) if result_db else None
# MAGIC result_tbl = result_tbl if result_tbl else 'RESULTS'
# MAGIC result_tbl = re.sub('[^\\w]+', '_', result_tbl) if result_tbl else None
# MAGIC result_dir = result_dir if result_dir else None
# MAGIC export_dir = export_dir if export_dir else None
# MAGIC export_format = export_format if export_format else 'json'
# MAGIC export_options = dict(re.split('\s*=\s*', x) for x in re.split('\s*,\s*', export_options)) if export_options else {}
# MAGIC if export_format in ['csv', 'json', 'xml'] and 'compression' not in export_options:
# MAGIC   export_options['compression'] = 'gzip'
# MAGIC flatten_col, *flatten_inner_col = [c.strip() for c in flatten_col.split(':')] if flatten_col else (None, None)
# MAGIC flatten_inner_col = flatten_inner_col[0] if flatten_inner_col else None
# MAGIC tags = dict(re.split('\s*=\s*', x) for x in re.split('\s*,\s*', tags)) if tags else {}
# MAGIC read_options = dict(re.split('\s*=\s*', x) for x in re.split('\s*,\s*', read_options)) if read_options else {}
# MAGIC format = None if not format else format
# MAGIC containers = re.split('\s*,\s*', containers)
# MAGIC time_now_dt = dateutil.parser.parse(time_now) if time_now else datetime.datetime.now(timezone).replace(microsecond=0)
# MAGIC time_now = time_now_dt.isoformat()
# MAGIC today = time_now_dt.strftime('%Y-%m-%d')
# MAGIC source_system_id = source_system_id if source_system_id else None      
# MAGIC   
# MAGIC print(f'client={client}')
# MAGIC print(f'secret_scope={secret_scope}')
# MAGIC print(f'storage_account={storage_account}')
# MAGIC print(f'storage_account_key={storage_account_key}')
# MAGIC print(f'src_dir={src_dir}')
# MAGIC print(f'dst_db={dst_db}')
# MAGIC print(f'dst_tbl={dst_tbl}')
# MAGIC print(f'dst_dir={dst_dir}')
# MAGIC print(f'dst_secure_db={dst_secure_db}')
# MAGIC print(f'dst_secure_dir={dst_dir}')
# MAGIC print(f'wh_schema={wh_schema}')
# MAGIC print(f'wh_secure_schema={wh_secure_schema}')
# MAGIC print(f'wh_tbl={wh_tbl}')
# MAGIC print(f'stg_db={stg_db}')
# MAGIC print(f'stg_tbl={stg_tbl}')
# MAGIC print(f'stg_dir={stg_dir}')
# MAGIC print(f'err_db={err_db}')
# MAGIC print(f'err_tbl={err_tbl}')
# MAGIC print(f'err_dir={err_dir}')
# MAGIC print(f'result_db={result_db}')
# MAGIC print(f'result_tbl={result_tbl}')
# MAGIC print(f'result_dir={result_dir}')
# MAGIC print(f'export_dir={export_dir}')
# MAGIC print(f'export_format={export_format}')
# MAGIC print(f'primary_key_cols={primary_key_cols}')
# MAGIC print(f'modification_col={modification_col}')
# MAGIC print(f'delete_col={delete_col}')
# MAGIC print(f'partition_cols={partition_cols}')
# MAGIC print(f'zorder_cols={zorder_cols}')
# MAGIC print(f'num_partitions={num_partitions}')
# MAGIC print(f'write_disposition={write_disposition}')
# MAGIC print(f'load_type={load_type}')
# MAGIC print(f'format={format}')
# MAGIC print(f'merge_schema={merge_schema}')
# MAGIC print(f'overwrite_schema={overwrite_schema}')
# MAGIC print(f'tags={tags}')
# MAGIC print(f'validate={validate}')
# MAGIC print(f'transform={transform}')
# MAGIC print(f'include_metadata={include_metadata}')
# MAGIC print(f'rename_columns={rename_columns}')
# MAGIC print(f'config={config}')
# MAGIC print(f'config_file={config_file}')
# MAGIC print(f'fail_on_error={fail_on_error}')
# MAGIC print(f'cleanup={cleanup}')
# MAGIC print(f'flatten={flatten}')
# MAGIC print(f'flatten_col={flatten_col}')
# MAGIC print(f'flatten_inner_col={flatten_inner_col}')
# MAGIC print(f'read_options={read_options}')
# MAGIC print(f'export_options={export_options}')
# MAGIC print(f'postprocess_notebook={postprocess_notebook}')
# MAGIC print(f'notebook_timeout_seconds={notebook_timeout_seconds}')
# MAGIC print(f'containers={containers}')
# MAGIC print(f'time_now={time_now}')
# MAGIC print(f'today={today}')
# MAGIC print(f'timezone={timezone}')
# MAGIC print(f'source_system_id={source_system_id}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mount ADLS containers

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb
# MAGIC 
# MAGIC hc = honeycomb.Honeycomb(spark, client)
# MAGIC 
# MAGIC for container in containers:
# MAGIC   hc.filesystem.mount(
# MAGIC     container, 
# MAGIC     storage_account=storage_account, 
# MAGIC     secret_scope=secret_scope, 
# MAGIC     secret_key=storage_account_key
# MAGIC  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Configuration

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import yaml 
# MAGIC import honeycomb.filesystem
# MAGIC import honeycomb.schema
# MAGIC 
# MAGIC config, exception = honeycomb.filesystem.read(spark, config_file, config)
# MAGIC if exception:
# MAGIC   print(exception)
# MAGIC   
# MAGIC config = yaml.safe_load(config) if config else None
# MAGIC print(f'config={config}')
# MAGIC 
# MAGIC schema = honeycomb.schema.Schema(config).schema
# MAGIC infer_schema = schema is None 
# MAGIC 
# MAGIC print(f'schema={schema}')
# MAGIC print(f'infer_schema={infer_schema}')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Initialize globals

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled", True)
# MAGIC spark.sql('SET spark.databricks.delta.optimizeWrite.enabled = true')
# MAGIC spark.sql('SET spark.databricks.delta.autoCompact.enabled = true')
# MAGIC spark.sql('SET spark.databricks.delta.symlinkFormatManifest.fileSystemCheck.enabled = false')
# MAGIC spark.sql(f'SET spark.databricks.delta.schema.overwriteSchema.enabled = {overwrite_schema}')
# MAGIC spark.sql(f'SET spark.databricks.delta.schema.autoMerge.enabled = {merge_schema}')
# MAGIC     
# MAGIC spark.sql(f'CREATE DATABASE IF NOT EXISTS {dst_db}')
# MAGIC spark.sql(f'CREATE DATABASE IF NOT EXISTS {stg_db}')
# MAGIC spark.sql(f'CREATE DATABASE IF NOT EXISTS {err_db}')
# MAGIC spark.sql(f'CREATE DATABASE IF NOT EXISTS {result_db}')
# MAGIC spark.sql(f'CREATE DATABASE IF NOT EXISTS {dst_secure_db}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define functions
# MAGIC 
# MAGIC #### get_excel_dataframe
# MAGIC 
# MAGIC Returns a spark dataframe using the koalas excel dataframe.
# MAGIC 
# MAGIC Two read options are available:
# MAGIC * headers=[true|false],  whether headers are available or not
# MAGIC * skiprows=int,  the number of rows to be skipped from the top of the workbook sheet
# MAGIC 
# MAGIC #### map_mpp
# MAGIC 
# MAGIC This function accepts a list .mpp files and then calls mpp_to_json for each. mpp_to_json will convert
# MAGIC input files to json and then drop them into staging so the get_dataframe function will se them.
# MAGIC 
# MAGIC #### get_dataframe
# MAGIC 
# MAGIC Returns a spark dataframe.  This is the default logic for all other data sources.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import databricks.koalas as ks
# MAGIC from pathlib import Path
# MAGIC from typing import List, Any
# MAGIC 
# MAGIC def get_excel_dataframe():
# MAGIC   # process will process all excel files with the same extension in the src_dir.
# MAGIC   # configure settings for koalas excel
# MAGIC   header = None
# MAGIC   if 'header' in read_options:
# MAGIC     if read_options['header'].lower() == 'true':
# MAGIC       header = 0
# MAGIC 
# MAGIC   sheet_name: Any = 0
# MAGIC   if 'sheet_name' in read_options:
# MAGIC         sheet_name = read_options['sheet_name']
# MAGIC         if sheet_name.isnumeric():
# MAGIC             sheet_name = int(sheet_name)
# MAGIC     
# MAGIC   skiprows:int = int(read_options['skiprows'])  if ('skiprows' in read_options) else None
# MAGIC   schema_names = None if infer_schema else schema.names
# MAGIC   dtype = None if infer_schema else str
# MAGIC   na_filter = infer_schema
# MAGIC 
# MAGIC   # be sure to check for multiple extensions (xlsx and xls) - the engine will not work if both are present
# MAGIC   # set engine based upon the extension.  must be the same extension.
# MAGIC   engine = 'openpyxl' if '.xlsx' in extensions else 'xlrd'
# MAGIC 
# MAGIC   # try catch is required when running on DBR 7.3 LTS as the koalas library 1.2 throws exceptions when excel workbook is empty.
# MAGIC   # on DBR 9.1 LTS, the koalas library 1.8.1 process an empty excel workbooks with no error conditions.
# MAGIC   try:
# MAGIC     # create koalas data frame from excel file(s)
# MAGIC     ks_df = ks.read_excel(src_dir, engine=engine, header=header, skiprows=skiprows, names=schema_names, dtype=dtype, na_filter=na_filter, sheet_name=sheet_name)
# MAGIC     # add excel_filename column to koalas data frame
# MAGIC     ks_df = ks_df.assign(excel_filename=files[0].name)
# MAGIC     # convert the koalas df to spark frame
# MAGIC     src_df = ks_df.to_spark()
# MAGIC     # apply the schema if not inferred
# MAGIC     if src_df.rdd.isEmpty():
# MAGIC       if infer_schema:
# MAGIC         dbutils.notebook.exit(0)
# MAGIC         # will need to handle removed of the input file in data factory post steps
# MAGIC       else:
# MAGIC         src_df = spark.createDataFrame([], schema)
# MAGIC         src_df = src_df.withColumn('excel_filename', F.lit(files[0].name))
# MAGIC     elif not infer_schema:
# MAGIC       for col in schema:
# MAGIC         src_df = src_df.withColumn(col.name, src_df[col.name].cast(col.dataType))
# MAGIC 
# MAGIC   except ValueError as ve:
# MAGIC     print(ve)
# MAGIC     if ('can not infer schema from empty or null dataset' in str(ve)):
# MAGIC       if infer_schema:
# MAGIC         dbutils.notebook.exit(0)
# MAGIC         # will need to handle removed of the input file in data factory post steps
# MAGIC       else:
# MAGIC         src_df = spark.createDataFrame([], schema)
# MAGIC         src_df = src_df.withColumn('excel_filename', F.lit(files[0].name))
# MAGIC     else:
# MAGIC       raise(ve)
# MAGIC 
# MAGIC   return src_df
# MAGIC 
# MAGIC def mpp_to_json(src_path: Path) -> Path:
# MAGIC   from net.sf.mpxj.reader import UniversalProjectReader
# MAGIC   from net.sf.mpxj.writer import ProjectWriterUtility
# MAGIC 
# MAGIC   project: net.sf.mpxj.ProjectFile = UniversalProjectReader().read(src_path)
# MAGIC   dst_path: Path = src_path.with_suffix('.json')
# MAGIC   writer:net.sf.mpxj.json.JsonWriter = ProjectWriterUtility.getProjectWriter(dst_path.as_posix())
# MAGIC   writer.write(project, dst_path)
# MAGIC   src_path.unlink()
# MAGIC   return Path('/', *dst_path.parts[2:])
# MAGIC 
# MAGIC 
# MAGIC def map_mpp(src_path: Path) -> List[Path]:
# MAGIC   import jpype
# MAGIC   import mpxj
# MAGIC 
# MAGIC   try:
# MAGIC     jpype.startJVM()
# MAGIC   except Exception as e:
# MAGIC     print(f'An exception has occurred: {e}')
# MAGIC 
# MAGIC   src_path = Path('/dbfs' + src_path)
# MAGIC   files = [mpp_to_json(f) for f in src_path.iterdir() if f.suffix == '.mpp']
# MAGIC   jpype.shutdownJVM()
# MAGIC 
# MAGIC   return files
# MAGIC 
# MAGIC def get_dataframe():
# MAGIC   # process will process all files with the same extension in the src_dir.
# MAGIC   src_df = spark.read.format(format) 
# MAGIC   src_df = src_df.schema(schema) if schema else src_df
# MAGIC   if format in ['csv', 'json', 'xml']:
# MAGIC     if 'header' not in read_options:
# MAGIC       read_options['header'] = 'true'
# MAGIC     if 'inferSchema' not in read_options:
# MAGIC       read_options['inferSchema'] = infer_schema
# MAGIC     if infer_schema and 'dropFieldIfAllNull' not in read_options:
# MAGIC        read_options['dropFieldIfAllNull'] = 'true'
# MAGIC 
# MAGIC   print(f'read_options={read_options}')
# MAGIC 
# MAGIC   for k, v in read_options.items():
# MAGIC     src_df = src_df.option(k, v)
# MAGIC 
# MAGIC   src_df = src_df.load(src_dir)
# MAGIC 
# MAGIC   return src_df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Load and process source data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC files = [f for f in dbutils.fs.ls(src_dir) if f.size > 0 and not f.name.startswith('_')]
# MAGIC print(f'files={files}')
# MAGIC if len(files) == 0:
# MAGIC   dbutils.notebook.exit(0)
# MAGIC   
# MAGIC extensions = set([os.path.splitext(f.name)[1] for f in files])
# MAGIC if len(extensions) != 1:
# MAGIC   raise ValueError(f'Unable to infer source format. Directory: {src_dir} contains files with missing or differing extensions')
# MAGIC 
# MAGIC if not format:
# MAGIC   extension = next(iter(extensions or []), None)
# MAGIC   EXTENSION_FORMAT_MAP = {
# MAGIC     '.parquet': 'parquet', 
# MAGIC     '.orc': 'orc', 
# MAGIC     '.avro': 'avro', 
# MAGIC     '.csv': 'csv', 
# MAGIC     '.tsv': 'csv', 
# MAGIC     '.json': 'json', 
# MAGIC     '.xml': 'xml', 
# MAGIC     '.xlsx': 'excel',
# MAGIC     '.xls': 'excel',
# MAGIC     '.mpp': 'project'
# MAGIC   }
# MAGIC   if extension not in EXTENSION_FORMAT_MAP.keys():
# MAGIC     raise ValueError(f'File extension: {extension} is not supported')
# MAGIC   if extension == '.tsv' and 'delimiter' not in read_options:
# MAGIC     read_options['delimiter'] = '\t'
# MAGIC   format = EXTENSION_FORMAT_MAP.get(extension)   
# MAGIC   print(f'inferred format from extension: {format}')
# MAGIC   
# MAGIC if format == 'xml' and 'rowTag' not in read_options:
# MAGIC   raise ValueError('XML files require rowTag defined in read_options')
# MAGIC   
# MAGIC print(f'format={format}')
# MAGIC 
# MAGIC if format == 'excel':
# MAGIC   src_df = get_excel_dataframe()
# MAGIC elif format == 'project':
# MAGIC   src_files: List[Path] = map_mpp(src_dir)
# MAGIC   src_dir = ','.join(f.as_posix() for f in src_files)
# MAGIC   format = 'json'
# MAGIC   src_df = get_dataframe()
# MAGIC else:
# MAGIC   src_df = get_dataframe()
# MAGIC 
# MAGIC display(src_df)  
# MAGIC 
# MAGIC # Remove special characters and whitepace from column names
# MAGIC if rename_columns:
# MAGIC     src_df = honeycomb.schema.rename_columns_df(
# MAGIC         spark, 
# MAGIC         src_df, 
# MAGIC         honeycomb.schema.special_chars_and_ws_to_underscore)
# MAGIC   
# MAGIC src_columns = [c.lower() for c in src_df.columns]
# MAGIC 
# MAGIC if flatten and flatten_col and flatten_col not in src_columns:
# MAGIC   raise ValueError(f'missing flatten column: {flatten_col}')
# MAGIC   
# MAGIC if flatten:
# MAGIC   flatten_col = flatten_col \
# MAGIC     if flatten_col \
# MAGIC     else next((c.name for c in src_df.schema if c.dataType.simpleString().startswith('array<struct')), None)
# MAGIC   if not flatten_col:
# MAGIC     raise ValueError('Unable to identify missing flatten_col')
# MAGIC   columns = src_df.select([F.col(name) for name in src_df.columns if name.lower() != flatten_col]).limit(0).columns
# MAGIC   src_df = src_df.select(columns + [F.explode(F.col(flatten_col)).alias('exploded')])
# MAGIC   exploded_columns = src_df.select(F.col('exploded.*')).limit(0).columns  
# MAGIC   prefix = f'{flatten_col}_'
# MAGIC   src_df = src_df.select(columns + [F.col(f'exploded.{c}').alias(c if c not in columns else f'{prefix}{c}'.replace('.', '_')) for c in exploded_columns])
# MAGIC   if flatten_inner_col:
# MAGIC     flatten_inner_col = flatten_inner_col if flatten_inner_col not in columns else f'{prefix}{flatten_inner_col}'.replace('.', '_')
# MAGIC     if flatten_inner_col not in src_df.columns:
# MAGIC       raise ValueError(f'missing flatten inner column: {flatten_inner_col}')
# MAGIC     columns = [F.col(name) for name in src_df.columns if name != flatten_inner_col] + [F.col(f'{flatten_inner_col}.*')]
# MAGIC     src_df = src_df.select(*columns)
# MAGIC   src_columns = [c.lower() for c in src_df.columns]
# MAGIC      

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tag with timestamp for partitioning

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC 
# MAGIC HC_LOAD_TS = 'hc_load_ts'
# MAGIC HC_SOURCE_SYSTEM_ID = 'hc_source_system_id'
# MAGIC 
# MAGIC HC_COLUMNS = [
# MAGIC   HC_LOAD_TS,
# MAGIC   HC_SOURCE_SYSTEM_ID,
# MAGIC ]
# MAGIC 
# MAGIC src_df = src_df \
# MAGIC   .withColumn(HC_LOAD_TS, F.lit(time_now).cast(T.TimestampType())) \
# MAGIC   .withColumn(HC_SOURCE_SYSTEM_ID, F.when(F.lit(source_system_id).isNotNull(), F.lit(source_system_id)) \
# MAGIC               .when((F.input_file_name().isNotNull()) & (F.input_file_name() != ''), F.input_file_name()) \
# MAGIC               .otherwise(F.lit(src_dir)))
# MAGIC   
# MAGIC stage_columns = [c.lower() for c in src_df.columns]
# MAGIC 
# MAGIC if primary_key_cols and not all(c in stage_columns for c in primary_key_cols):
# MAGIC     raise ValueError(f'missing primary key columns: {primary_key_cols}')
# MAGIC 
# MAGIC if delete_col and delete_col not in src_columns:
# MAGIC     raise ValueError(f'missing delete column: {delete_col}')
# MAGIC     
# MAGIC if modification_col and modification_col not in stage_columns:
# MAGIC     raise ValueError(f'missing modification column: {modification_col}')
# MAGIC     
# MAGIC if partition_cols and not all(c in stage_columns for c in partition_cols):
# MAGIC     raise ValueError(f'missing partition columns: {partition_cols}')
# MAGIC         
# MAGIC if zorder_cols and not all(c in stage_columns for c in zorder_cols):
# MAGIC     raise ValueError(f'missing zorder columns: {zorder_cols}')
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cache Dataframe

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC partition_count = src_df.rdd.getNumPartitions()
# MAGIC print(f'partition_count={partition_count}')
# MAGIC 
# MAGIC if num_partitions is not None and num_partitions > 0:
# MAGIC   print(f'repartitioning to: {num_partitions} partitions')
# MAGIC   src_df = src_df.repartition(num_partitions)
# MAGIC   
# MAGIC # Rearrange columns
# MAGIC src_df = src_df.select(HC_COLUMNS + [c for c in src_df.columns if c not in HC_COLUMNS])  
# MAGIC 
# MAGIC src_df = src_df.cache()
# MAGIC src_df.printSchema()
# MAGIC src_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ensure appropriate table partitioning for the provided write_disposition

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from delta.tables import DeltaTable
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC 
# MAGIC stg_tbl_exists = hc.table.exists(f'{stg_db}.{stg_tbl}')
# MAGIC print(f'stg_tbl_exists={stg_tbl_exists}')
# MAGIC 
# MAGIC if stg_tbl_exists:
# MAGIC   stg_partition_cols = [] if load_type == 'full' else [HC_LOAD_TS]
# MAGIC   honeycomb.table.resolve(spark, f'{stg_db}.{stg_tbl}', dst_dir=stg_dir, partition_cols=stg_partition_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Save processed data in staging database

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC src_writer = src_df.write \
# MAGIC   .format('delta') \
# MAGIC   .option('mergeSchema', merge_schema) \
# MAGIC   .option('overwriteSchema', overwrite_schema) 
# MAGIC if stg_dir:
# MAGIC   src_writer = src_writer.option('path', stg_dir) 
# MAGIC 
# MAGIC if load_type != 'full':
# MAGIC   print(f'Appending table: {stg_db}.{stg_tbl} partitioned by: {HC_LOAD_TS}')
# MAGIC   src_writer \
# MAGIC     .mode('append') \
# MAGIC     .partitionBy(HC_LOAD_TS) \
# MAGIC     .saveAsTable(f'{stg_db}.{stg_tbl}')
# MAGIC else:
# MAGIC   print(f'Overwriting table: {stg_db}.{stg_tbl}')
# MAGIC   src_writer \
# MAGIC     .mode('overwrite') \
# MAGIC     .saveAsTable(f'{stg_db}.{stg_tbl}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrich with processing data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import json 
# MAGIC 
# MAGIC HC_MOD_TS = 'hc_mod_ts'
# MAGIC HC_HASH = 'hc_hash'
# MAGIC HC_KEY = 'hc_key'
# MAGIC HC_TAGS = 'hc_tags'
# MAGIC HC_EXPIRATION_TS = 'hc_expiration_ts'
# MAGIC HC_CURRENT = 'hc_current'
# MAGIC HC_DELETED = 'hc_deleted'
# MAGIC 
# MAGIC HC_COLUMNS += [
# MAGIC   HC_MOD_TS,
# MAGIC   HC_HASH,
# MAGIC   HC_KEY,
# MAGIC   HC_TAGS,
# MAGIC   HC_EXPIRATION_TS,
# MAGIC   HC_CURRENT,
# MAGIC   HC_DELETED
# MAGIC ]
# MAGIC 
# MAGIC hc_key = F.xxhash64(*[F.col(c) for c in primary_key_cols]) if primary_key_cols else F.xxhash64(*[F.col(c) for c in src_columns])
# MAGIC hc_hash = F.hash(*[F.col(c) for c in src_columns])
# MAGIC hc_tags = F.lit(json.dumps(tags)).cast(T.StringType())
# MAGIC hc_mod_ts = F.lit(None).cast(T.TimestampType())
# MAGIC hc_expiration_ts = F.lit(None).cast(T.TimestampType())
# MAGIC hc_current = F.lit(True).cast(T.BooleanType())
# MAGIC hc_deleted = F.lit(False).cast(T.BooleanType()) if not delete_col else F.col(delete_col).cast(T.BooleanType())
# MAGIC 
# MAGIC src_df = src_df \
# MAGIC   .withColumn(HC_KEY, hc_key) \
# MAGIC   .withColumn(HC_HASH, hc_hash) \
# MAGIC   .withColumn(HC_TAGS, hc_tags) \
# MAGIC   .withColumn(HC_MOD_TS, hc_mod_ts) \
# MAGIC   .withColumn(HC_EXPIRATION_TS, hc_expiration_ts) \
# MAGIC   .withColumn(HC_CURRENT, hc_current) \
# MAGIC   .withColumn(HC_DELETED, hc_deleted) 
# MAGIC 
# MAGIC src_df = src_df.select(HC_COLUMNS + [c for c in src_df.columns if c not in HC_COLUMNS])  

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transform Staged Data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.transform
# MAGIC 
# MAGIC if transform:
# MAGIC   transform = honeycomb.transform.Transformation(spark, src_df, config) 
# MAGIC   result = transform.execute()
# MAGIC   src_df = result.data
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Staged Data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.validate
# MAGIC 
# MAGIC errors = []
# MAGIC if validate:
# MAGIC   validation = honeycomb.validate.Validation(spark, src_df, config) 
# MAGIC   if not config and len(primary_key_cols) > 0:
# MAGIC     validation.are_unique(primary_key_cols) 
# MAGIC     
# MAGIC   result = validation.execute()
# MAGIC   
# MAGIC   src_df = result.correct_data
# MAGIC   errors = result.errors
# MAGIC   if len(errors) > 0:
# MAGIC     if fail_on_error:
# MAGIC       raise ValueError(f'An exception has occurred during validation: {errors}')
# MAGIC     error_df = result.erroneous_data
# MAGIC     error_df = error_df.withColumn(HC_LOAD_TS, F.lit(time_now).cast(T.TimestampType()))
# MAGIC     error_df = error_df.write.format('delta').mode('append') 
# MAGIC     if err_dir:
# MAGIC       error_df = error_df.option('path', err_dir) 
# MAGIC     error_df.partitionBy(HC_LOAD_TS).saveAsTable(f'{err_db}.{err_tbl}')
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Metadata

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.metadata
# MAGIC 
# MAGIC metadata = honeycomb.metadata.Metadata(spark, src_df) 
# MAGIC if include_metadata:
# MAGIC   metadata = honeycomb.metadata.Metadata(spark, src_df, config) 
# MAGIC   if not config:
# MAGIC     for primary_key_col in primary_key_cols:
# MAGIC       metadata.add(primary_key_col, honeycomb.metadata.HC_PRIMARY_KEY, True)
# MAGIC     if modification_col:
# MAGIC       metadata.add(modification_col, honeycomb.metadata.HC_MODIFICATION_COL, True)
# MAGIC     if delete_col:
# MAGIC       metadata.add(delete_col, honeycomb.metadata.HC_DELETE_COL, True)
# MAGIC       
# MAGIC   result = metadata.execute()
# MAGIC   src_df = result.data
# MAGIC 
# MAGIC for c in src_df.schema:
# MAGIC   print(f'{c.name}={c.metadata}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ensure appropriate location for secure data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.metadata
# MAGIC 
# MAGIC should_secure = bool(dst_secure_dir) and include_metadata and metadata.is_secure()
# MAGIC print(f'should_secure={should_secure}')
# MAGIC 
# MAGIC if should_secure and not hc.table.exists(f'{dst_secure_db}.{dst_tbl}') and hc.table.exists(f'{dst_db}.{dst_tbl}'):
# MAGIC   location = f"LOCATION '{dst_secure_dir}'" if dst_secure_dir else '' 
# MAGIC   spark.sql(f"CREATE OR REPLACE TABLE {dst_secure_db}.{dst_tbl} CLONE {dst_db}.{dst_tbl} {location}")
# MAGIC   hc.table.drop(f'{dst_db}.{dst_tbl}', force=True)
# MAGIC elif not should_secure and hc.table.exists(f'{dst_secure_db}.{dst_tbl}'):
# MAGIC   hc.table.drop(f'{dst_db}.{dst_tbl}') # Drop secure view if it exists
# MAGIC   location = f"LOCATION '{dst_dir}'" if dst_dir else '' 
# MAGIC   spark.sql(f"CREATE OR REPLACE TABLE {dst_db}.{dst_tbl} CLONE {dst_secure_db}.{dst_tbl} {location}")
# MAGIC   hc.table.drop(f'{dst_secure_db}.{dst_tbl}', force=True)
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ensure the destination table is consistent with the provided configuration

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.table
# MAGIC 
# MAGIC target_db = dst_secure_db if should_secure else dst_db
# MAGIC target_dir = dst_secure_dir if should_secure else dst_dir
# MAGIC 
# MAGIC target_tbl_exists = honeycomb.table.exists(spark, f'{target_db}.{dst_tbl}')
# MAGIC print(f'target_tbl_exists={target_tbl_exists}')
# MAGIC 
# MAGIC if target_tbl_exists:
# MAGIC   honeycomb.table.resolve(spark, f'{target_db}.{stg_tbl}', dst_dir=target_dir, partition_cols=partition_cols)
# MAGIC   
# MAGIC   # Verify that provided schema is compatible. At the moment this implementation only accomodates scenarios where new
# MAGIC   # columns have been appended by the source.
# MAGIC   # NOTE: These scenarios should be handled using merge_schema/overwrite_schema when write_disposition is overwrite
# MAGIC   src_schema = src_df.schema
# MAGIC   dst_schema = spark.table(f'{target_db}.{dst_tbl}').schema
# MAGIC   if src_schema != dst_schema and write_disposition != 'overwrite' and merge_schema:
# MAGIC     src_schema = src_schema[len(HC_COLUMNS):]
# MAGIC     dst_schema = dst_schema[len(HC_COLUMNS):]
# MAGIC     if len(src_schema) > len(dst_schema) and src_schema[:len(dst_schema)] == dst_schema:
# MAGIC       appended_cols = src_schema[len(dst_schema):]
# MAGIC       column_specs = ','.join([ f'{c.name} {c.dataType.simpleString()} AFTER {dst_schema[-1].name}' for c in reversed(appended_cols)])
# MAGIC       sql = f'ALTER TABLE {target_db}.{stg_tbl} ADD COLUMNS ({column_specs})'
# MAGIC       print(sql)
# MAGIC       spark.sql(sql)  
# MAGIC 
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform Merge

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.metadata
# MAGIC 
# MAGIC from delta.tables import DeltaTable
# MAGIC 
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import types as T
# MAGIC 
# MAGIC 
# MAGIC IMMUTABLE_COLUMNS = primary_key_cols + [HC_LOAD_TS, HC_MOD_TS, HC_EXPIRATION_TS, HC_CURRENT]
# MAGIC   
# MAGIC dst_tbl_exists = any(t.name == dst_tbl.lower() for t in spark.catalog.listTables(target_db))
# MAGIC 
# MAGIC if dst_tbl_exists and write_disposition == 'error':
# MAGIC   raise ValueError(f'table: {target_db}.{dst_tbl} already exists')
# MAGIC 
# MAGIC stg_view: str = f'{stg_tbl}_vw'
# MAGIC src_df.createOrReplaceTempView(stg_view)
# MAGIC 
# MAGIC if dst_tbl_exists and write_disposition != 'overwrite':
# MAGIC   match_condition = f's.{HC_KEY}=t.{HC_KEY}' if primary_key_cols and write_disposition == 'merge' else '1=0'
# MAGIC   partition_condition = f"{HC_LOAD_TS}='{time_now}'" if write_disposition == 'merge' else '1=1'
# MAGIC   delete_condition = f'BOOLEAN(s.{delete_col})' if delete_col else '1=0';
# MAGIC   modification_condition = f's.{modification_col}>=t.{modification_col}' if modification_col else '1=1'
# MAGIC   src_columns = [c.lower() for c in src_df.columns]
# MAGIC   update_cols = [c for c in src_columns if c not in IMMUTABLE_COLUMNS]
# MAGIC   print(f'update_cols={update_cols}')
# MAGIC   update_action = '*' if merge_schema else f"t.{HC_MOD_TS}='{time_now}'," + ','.join([f't.{c}=s.{c}' for c in update_cols])
# MAGIC   insert_action = '(' + ','.join(src_columns) + ') VALUES(' + ','.join([f's.{c}' for c in src_columns]) + ')'
# MAGIC 
# MAGIC   print('src_df.printSchema()')
# MAGIC   src_df.printSchema()
# MAGIC   
# MAGIC   if write_disposition in ['merge2', 'scd']:
# MAGIC     # In order for Type 2 (Slow Changing Dimensions) merges to correctly drop the generated _hc_merge_key column during merges
# MAGIC     # the Databricks Delta automerge feature must be disabled.
# MAGIC     if merge_schema:
# MAGIC       spark.sql(f'SET spark.databricks.delta.schema.autoMerge.enabled = False')
# MAGIC     sql = f'''
# MAGIC       MERGE INTO {target_db}.{dst_tbl} t
# MAGIC       USING (
# MAGIC         SELECT {stg_view}.{HC_KEY} AS _hc_merge_key, {stg_view}.*
# MAGIC         FROM {stg_view} WHERE {partition_condition}
# MAGIC         UNION ALL
# MAGIC         SELECT CAST(NULL AS LONG) AS _hc_merge_key, {stg_view}.*
# MAGIC         FROM {stg_view} 
# MAGIC         JOIN {target_db}.{dst_tbl} 
# MAGIC         ON {stg_view}.{HC_KEY} = {target_db}.{dst_tbl}.{HC_KEY} 
# MAGIC         WHERE {target_db}.{dst_tbl}.{HC_CURRENT} = true AND {stg_view}.{HC_HASH} != {target_db}.{dst_tbl}.{HC_HASH} 
# MAGIC       ) s
# MAGIC       ON t.{HC_KEY} = s._hc_merge_key
# MAGIC       WHEN MATCHED AND t.{HC_CURRENT} = true AND s.{HC_HASH} != t.{HC_HASH} AND {modification_condition} THEN UPDATE SET {HC_CURRENT} = false, {HC_EXPIRATION_TS} = s.{HC_LOAD_TS}, {HC_MOD_TS} = '{time_now}' 
# MAGIC       WHEN NOT MATCHED THEN INSERT {insert_action}
# MAGIC     '''
# MAGIC     print(sql)
# MAGIC     spark.sql(sql)
# MAGIC     if merge_schema:
# MAGIC       spark.sql(f'SET spark.databricks.delta.schema.autoMerge.enabled = {merge_schema}')
# MAGIC   else:
# MAGIC     sql = f'''
# MAGIC       MERGE INTO {target_db}.{dst_tbl} t
# MAGIC       USING (
# MAGIC         SELECT * FROM {stg_view} WHERE {partition_condition}
# MAGIC       ) s
# MAGIC       ON {match_condition}  
# MAGIC       WHEN MATCHED AND {delete_condition} AND {modification_condition} THEN DELETE
# MAGIC       WHEN MATCHED AND s.{HC_HASH} != t.{HC_HASH} AND {modification_condition} THEN UPDATE SET {update_action}
# MAGIC       WHEN NOT MATCHED AND NOT {delete_condition} THEN INSERT *
# MAGIC     '''
# MAGIC     print(sql)
# MAGIC     spark.sql(sql)
# MAGIC else:
# MAGIC   writer = src_df.write 
# MAGIC   if partition_cols:
# MAGIC     writer = writer.partitionBy(*partition_cols)
# MAGIC 
# MAGIC   writer = \
# MAGIC     writer.format('delta') \
# MAGIC     .mode('overwrite') \
# MAGIC     .option('mergeSchema', merge_schema) \
# MAGIC     .option('overwriteSchema', overwrite_schema) 
# MAGIC   if target_dir:
# MAGIC     writer = writer.option('path', target_dir) 
# MAGIC   writer.saveAsTable(f'{target_db}.{dst_tbl}')
# MAGIC   
# MAGIC   spark.sql(f'GENERATE symlink_format_manifest FOR TABLE {target_db}.{dst_tbl}')
# MAGIC   
# MAGIC history_df = spark.sql(f'DESCRIBE HISTORY {target_db}.{dst_tbl}')
# MAGIC operationMetrics = history_df.select('operationMetrics').take(1)[0].operationMetrics
# MAGIC 
# MAGIC # Process 
# MAGIC if load_type == 'full' and not delete_col:
# MAGIC   if write_disposition == 'merge':
# MAGIC     sql: str = f'''
# MAGIC       DELETE FROM {target_db}.{dst_tbl} t
# MAGIC       WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM {stg_view} 
# MAGIC         WHERE t.{HC_KEY} = {HC_KEY} 
# MAGIC     )
# MAGIC     '''
# MAGIC     print(sql)
# MAGIC     spark.sql(sql)
# MAGIC   elif write_disposition in ['merge2', 'scd']:
# MAGIC     sql: str = f'''
# MAGIC       UPDATE {target_db}.{dst_tbl} t
# MAGIC       SET {HC_DELETED} = TRUE, {HC_MOD_TS} = '{time_now}'
# MAGIC       WHERE NOT EXISTS (
# MAGIC         SELECT 1
# MAGIC         FROM {stg_view}
# MAGIC         WHERE t.{HC_KEY} = {HC_KEY} 
# MAGIC     )
# MAGIC     '''
# MAGIC     print(sql)
# MAGIC     spark.sql(sql)
# MAGIC 
# MAGIC spark.sql(f'ALTER TABLE {target_db}.{dst_tbl} SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)')  
# MAGIC 
# MAGIC if zorder_cols:
# MAGIC   zorder_cols = ','.join(zorder_cols)
# MAGIC   sql = f'OPTIMIZE {target_db}.{dst_tbl} ZORDER BY ({zorder_cols})'
# MAGIC   print(sql)
# MAGIC   spark.sql(sql)
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create WHITE_LIST table and insert default security group

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC white_list_tbl = 'WHITE_LIST'
# MAGIC secure_group = 'INSIGHTS_SECURE'
# MAGIC 
# MAGIC if should_secure and not hc.table.exists(f'{dst_secure_db}.{white_list_tbl}'):
# MAGIC   spark.sql(f'CREATE TABLE IF NOT EXISTS {dst_secure_db}.{white_list_tbl}(group STRING) USING DELTA')
# MAGIC   spark.sql(f'''
# MAGIC     MERGE INTO {dst_secure_db}.{white_list_tbl} dst 
# MAGIC     USING (SELECT '{secure_group}' AS group) AS src
# MAGIC     ON dst.group = src.group
# MAGIC     WHEN NOT MATCHED THEN INSERT(group) VALUES(src.group)
# MAGIC   ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Secure View for Secure Data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.metadata
# MAGIC 
# MAGIC import pyspark.sql.functions as F
# MAGIC import pyspark.sql.types as T
# MAGIC 
# MAGIC 
# MAGIC if should_secure:
# MAGIC   columns = []
# MAGIC 
# MAGIC   user_functions_enabled = spark.sparkContext.getConf().get('spark.databricks.userInfoFunctions.enabled')
# MAGIC   user_functions_enabled = bool(strtobool(user_functions_enabled)) if user_functions_enabled else False
# MAGIC   print(f'user_functions_enabled={user_functions_enabled}')
# MAGIC 
# MAGIC   if user_functions_enabled:
# MAGIC     current_user_rows = spark.sql('SELECT current_user() AS current_user').take(1)
# MAGIC     current_user = current_user_rows[0].current_user if len(current_user_rows) > 0 else None
# MAGIC     print(f'current_user={current_user}')
# MAGIC     
# MAGIC     white_list_df = spark.table(f'{dst_secure_db}.{white_list_tbl}')
# MAGIC     white_list = list(white_list_df.select(F.col('group')).distinct().toPandas()['group'])
# MAGIC     white_list_condition = ' OR '.join([f"IS_MEMBER('{g}')" for g in white_list]) if white_list else 'FALSE'  
# MAGIC   else:
# MAGIC     white_list_condition = 'TRUE'
# MAGIC   
# MAGIC   for c in src_df.schema:
# MAGIC     is_secure = metadata.has_column_metadata(c.name, honeycomb.metadata.HC_SECURE) or metadata.has_column_metadata(c.name, honeycomb.metadata.HC_MASK)
# MAGIC     column = f'''
# MAGIC       CASE 
# MAGIC         WHEN {white_list_condition} 
# MAGIC         THEN secure_table.{c.name}
# MAGIC         ELSE CAST(HASH(secure_table.{c.name}) AS {c.dataType.simpleString()}) 
# MAGIC       END AS {c.name}
# MAGIC     ''' if is_secure else c.name
# MAGIC 
# MAGIC     columns.append(column)
# MAGIC     
# MAGIC   columns = ','.join(columns)
# MAGIC   
# MAGIC   create_secure_view_sql = f'''
# MAGIC     CREATE OR REPLACE VIEW {dst_db}.{dst_tbl} AS SELECT 
# MAGIC       {columns}
# MAGIC     FROM {dst_secure_db}.{dst_tbl} secure_table
# MAGIC   '''
# MAGIC   
# MAGIC   print(create_secure_view_sql)
# MAGIC   spark.sql(create_secure_view_sql)       
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export table results to provided location and format

# COMMAND ----------

# export_dir = None # Disable exporting for smc-na

if export_dir:
  export_df = spark.table(f'{target_db}.{dst_tbl}').write.mode('overwrite').format(export_format)
  if export_format in ['csv']:
    export_df = export_df.option('header', 'true')
  for k, v in export_options.items():
    export_df = export_df.option(k, v)
  export_df.save(export_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run post processing notebook

# COMMAND ----------

# MAGIC %python
# MAGIC import json
# MAGIC 
# MAGIC if postprocess_notebook:
# MAGIC   dbutils.notebook.run(
# MAGIC     postprocess_notebook, 
# MAGIC     notebook_timeout_seconds, 
# MAGIC     {
# MAGIC       'client': client,
# MAGIC       'dst_db': dst_db if not should_secure else dst_secure_db, 
# MAGIC       'dst_tbl': dst_tbl, 
# MAGIC       'wh_schema': '' if not wh_schema else wh_schema, 
# MAGIC       'wh_tbl': wh_tbl,
# MAGIC       'config': json.dumps(config),
# MAGIC       'include_metadata': include_metadata,
# MAGIC       'secret_scope': secret_scope
# MAGIC     }
# MAGIC   )
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ###Collect and export metrics

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.utils
# MAGIC 
# MAGIC errors = [dict(e._asdict()) for e in errors] if errors else []
# MAGIC exit_results = operationMetrics
# MAGIC exit_results['has_errors'] = 'true' if errors else 'false'
# MAGIC exit_results['table'] = f'{target_db}.{dst_tbl}'
# MAGIC exit_results['view'] = f'{dst_db}.{dst_tbl}' if should_secure else ''
# MAGIC exit_results['is_secure'] = str(should_secure).lower() if should_secure else 'false'
# MAGIC 
# MAGIC if errors:
# MAGIC   exit_results['errors'] = errors
# MAGIC   
# MAGIC result_df = spark.read \
# MAGIC   .json(sc.parallelize([exit_results])) \
# MAGIC   .withColumn(HC_LOAD_TS, F.lit(time_now).cast(T.TimestampType())) 
# MAGIC 
# MAGIC result_df.printSchema()
# MAGIC 
# MAGIC result_df = result_df \
# MAGIC   .write \
# MAGIC   .format('delta') \
# MAGIC   .mode('append') \
# MAGIC   .option('mergeSchema', 'true') 
# MAGIC 
# MAGIC if result_dir:
# MAGIC   result_df = result_df.option('path', result_dir) 
# MAGIC 
# MAGIC retry_count = 3
# MAGIC delay = 5 #seconds
# MAGIC honeycomb.utils.retry_call(lambda: result_df.saveAsTable(f'{result_db}.{result_tbl}'), tries=retry_count, delay=delay)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup Staged Data

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import honeycomb.filesystem
# MAGIC 
# MAGIC if cleanup:
# MAGIC   results = honeycomb.filesystem.rm(spark, f'{src_dir}/*')
# MAGIC   print(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Report Results

# COMMAND ----------

dbutils.notebook.exit(exit_results)
