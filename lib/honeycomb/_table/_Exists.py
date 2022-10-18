import textwrap
from typing import Any
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession


class _Exists(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.table.exists')
        self._spark = spark

    def __call__(self, table: str, **kwargs) -> str:
        result = self._exists(self._spark, table)
        return result

    def help(self):
        docstring = f"""
             {self.name}(table: str)

             Determine the table exists.

             Parameters
             ----------

             database : str (optional)
                     The database name. [[database]

             table : str
                     The table name. [[database.][table]]

             Returns
             -------
             
             bool
                    True if the table exists, false otherwise

        """
        print(textwrap.dedent(docstring))

    def _exists(self, spark: SparkSession, table: str) -> bool:
        parts = table.split('.', 1)
        database, table = parts if len(parts) == 2 else ('default', parts[0])
        exists = any(
            t.name == table.lower() for t in spark.catalog.listTables(database))
        return exists
