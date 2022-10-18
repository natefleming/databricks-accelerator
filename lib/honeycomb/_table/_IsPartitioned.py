import textwrap
from typing import Any
from honeycomb._command._Command import _Command
import logging
from pyspark.sql.session import SparkSession

LOGGER = logging.getLogger(__name__)


class _IsPartitioned(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.table.is_partitioned')
        self._spark = spark

    def __call__(self, table: str, **kwargs) -> bool:
        result = self._is_partitioned(self._spark, table)
        return result

    def help(self):
        docstring = f"""
             {self.name}(table: str)

             Determine whether or not this table is partitioned.

             Parameters
             ----------

             table : str
                     The table name. [[database.][table]]

             Returns
             -------
             
             bool
                    True if the table is partitioned else False     
        """
        print(textwrap.dedent(docstring))

    def _is_partitioned(self, spark: SparkSession, table: str) -> bool:
        try:
            spark.sql(f'SHOW PARTITIONS {table}')
            return True
        except Exception as e:
            if 'not partitioned' not in str(e):
                LOGGER.warning(f"An exception occurred: {e}")
            return False
