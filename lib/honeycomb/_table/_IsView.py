import textwrap
from typing import Any
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

import pyspark.sql.functions as F
import pyspark.sql.types as t


class _IsView(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.table.is_view')
        self._spark = spark

    def __call__(self, table: str, **kwargs) -> str:
        result = self._is_view(self._spark, table)
        return result

    def help(self):
        docstring = f"""
             {self.name}(table: str)

             Determine the target is a view.

             Parameters
             ----------

             table : str
                     The table name. [[database.][table]]

             Returns
             -------
             
             bool
                    True if the table is a view, false otherwise

        """
        print(textwrap.dedent(docstring))

    def _is_view(self, spark: SparkSession, table: str) -> bool:
        parts = table.split('.', 1)
        database, table = parts if len(parts) == 2 else ('default', parts[0])
        is_view = len(spark.sql(f'DESCRIBE EXTENDED {database}.{table}') \
            .where(F.lower(F.col('col_name')) == 'type') \
            .where(F.lower(F.col('data_type')) == 'view').take(1)) > 0
        return is_view
