import textwrap
from typing import Any, List
from honeycomb._command._Command import _Command
import logging
from pyspark.sql.session import SparkSession

LOGGER = logging.getLogger(__name__)


class _Partitions(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.table.partitions')
        self._spark = spark

    def __call__(self, table: str, **kwargs) -> List[str]:
        result = self._partitions(self._spark, table)
        return result

    def help(self):
        docstring = f"""
             {self.name}(table: str)

             List the partitions in this table

             Parameters
             ----------

             table : str
                     The table name. [[database.][table]]

             Returns
             -------
             
             List[str]
                    The list of partitions
        """
        print(textwrap.dedent(docstring))

    def _partitions(self, spark: SparkSession, table: str) -> List[str]:
        columns: List[str] = []
        try:
            columns = spark.sql(f'SHOW PARTITIONS {table}').columns
            return columns
        except Exception as e:
            if 'not partitioned' not in str(e):
                LOGGER.warning(f"An exception occurred: {e}")

        return columns
