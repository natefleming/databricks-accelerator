import textwrap
from distutils.util import strtobool
from typing import Any
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

import pyspark.sql.functions as F
import pyspark.sql.types as t

import honeycomb.utils
import honeycomb.table
import honeycomb.filesystem


class _Drop(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.table.drop')
        self._spark = spark

    def __call__(self, table: str, **kwargs) -> str:
        if_exists = strtobool(str(kwargs.get('if_exists', False)))
        force = strtobool(str(kwargs.get('force', False)))
        result = self._drop(self._spark, table, if_exists, force)
        return result

    def help(self):
        docstring = f"""
             {self.name}(table: str)

             Drop the target table or view.

             Parameters
             ----------

             table : str
                     The table name. [[database.][table]]

             if_exists: bool (optional)
                     Add if exists condition

             force: bool (optional)
                     Remove delta history and external directory

             Returns
             -------
             
             bool
                    True if the table is dropped, false otherwise

        """
        print(textwrap.dedent(docstring))

    def _drop(self, spark: SparkSession, table: str, if_exists: bool,
              force: bool) -> bool:
        dbutils_wrapper = honeycomb.utils.dbutils(spark)
        parts = table.split('.', 1)
        database, table = parts if len(parts) == 2 else ('default', parts[0])
        table_type = 'TABLE' if honeycomb.table.is_table(
            spark, f'{database}.{table}') else 'VIEW'
        if_exists = 'IF EXISTS' if if_exists else ''

        is_delta = honeycomb.table.is_delta(spark, f'{database}.{table}')
        is_external = honeycomb.table.is_external(spark, f'{database}.{table}')

        if force:
            location = honeycomb.table.location(spark, f'{database}.{table}')
            spark.sql(
                f"ALTER TABLE {database}.{table} SET TBLPROPERTIES('external'='false')"
            )
            if is_delta:
                from delta.tables import DeltaTable
                spark.sql(
                    'SET spark.databricks.delta.retentionDurationCheck.enabled = false'
                )
                spark.sql(
                    f'ALTER TABLE {database}.{table} SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=false)'
                )
                DeltaTable.forName(spark, f'{database}.{table}').delete()
                DeltaTable.forName(spark, f'{database}.{table}').vacuum(0)
                dbutils_wrapper.fs.rm(f'{location}/_delta_log', True)

        drop = spark.sql(f'DROP {table_type} {if_exists} {database}.{table}')
        return drop
