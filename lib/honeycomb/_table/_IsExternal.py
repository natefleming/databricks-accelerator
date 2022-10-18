import textwrap
from typing import Any
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession


class _IsExternal(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.table.is_external')
        self._spark = spark

    def __call__(self, table: str, **kwargs) -> str:
        result = self._is_external(self._spark, table)
        return result

    def help(self):
        docstring = f"""
             {self.name}(table: str)

             Determine the target is external

             Parameters
             ----------

             table : str
                     The table name. [[database.][table]]

             Returns
             -------
             
             bool
                    True if the table is external, false otherwise

        """
        print(textwrap.dedent(docstring))

    def _is_external(self, spark: SparkSession, table: str) -> bool:
        parts = table.split('.', 1)
        database, table = parts if len(parts) == 2 else ('default', parts[0])
        found = next(
            iter(t for t in spark.catalog.listTables(database)
                 if t.name.lower() == table.lower()), None)
        if found is None:
            raise ValueError(f'Missing table: {database}.{table}')

        is_external = found.tableType == 'EXTERNAL'
        return is_external
