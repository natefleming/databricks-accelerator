import textwrap
from typing import Any
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

import pyspark.sql.functions as F
import pyspark.sql.types as t


class _IsDelta(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.table.is_delta')
        self._spark = spark

    def __call__(self, table: str, **kwargs) -> str:
        result = self._is_delta(self._spark, table)
        return result

    def help(self):
        docstring = f"""
             {self.name}(table: str)

             Determine the target is delta format.

             Parameters
             ----------

             table : str
                     The table name. [[database.][table]]

             Returns
             -------
             
             bool
                    True if the table is delta format, false otherwise

        """
        print(textwrap.dedent(docstring))

    def _is_delta(self, spark: SparkSession, table: str) -> bool:
        parts = table.split('.', 1)
        database, table = parts if len(parts) == 2 else ('default', parts[0])
        is_delta = spark.sql(f'DESCRIBE EXTENDED {database}.{table}') \
            .where((F.lower(F.col('col_name')) == 'provider') & (F.lower(F.col('data_type')) == 'delta')).take(1)
        return len(is_delta) > 0
