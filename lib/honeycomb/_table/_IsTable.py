import textwrap
from typing import Any
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

import pyspark.sql.functions as F
import pyspark.sql.types as t

import honeycomb.table


class _IsTable(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.table.is_table')
        self._spark = spark

    def __call__(self, table: str, **kwargs) -> str:
        result = self._is_table(self._spark, table)
        return result

    def help(self):
        docstring = f"""
             {self.name}(table: str)

             Determine the target is a table.

             Parameters
             ----------

             table : str
                     The table name. [[database.][table]]

             Returns
             -------
             
             bool
                    True if the table is a table, false otherwise

        """
        print(textwrap.dedent(docstring))

    def _is_table(self, spark: SparkSession, table: str) -> bool:
        return not honeycomb.table.is_view(spark, table)
