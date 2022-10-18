import textwrap
from typing import Any
from urllib.parse import urlsplit
import pathlib

from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

import honeycomb.table


class _StageLocation(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.table.stage_location')
        self._spark = spark

    def __call__(self, table: str, **kwargs) -> str:
        result = self._stage_location(self._spark, table)
        return result

    def help(self):
        docstring = f"""
             {self.name}(table: str)

             Determine the directory stage location of this table.

             Parameters
             ----------

             table : str
                     The table name. [[database.][table]]

             Returns
             -------
             
             str
                    The directory location of this table.

        """
        print(textwrap.dedent(docstring))

    def _stage_location(self, spark: SparkSession, table_name: str) -> str:
        df = spark.sql(f'describe detail {table_name}')
        location = df.select('location').collect()[0].location
        path = pathlib.Path(urlsplit(location).path)
        if len(path.parts) >= 3:
            location = str(pathlib.Path(*path.parts[3:]))
        return location
