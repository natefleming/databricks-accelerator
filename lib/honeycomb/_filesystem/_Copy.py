import textwrap
import logging
import re

import honeycomb.utils
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

LOGGER = logging.getLogger(__name__)


class _Copy(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.filesystem.cp')
        self._spark = spark

    def __call__(self,
                 source: str,
                 destination: str,
                 recurse: bool = False,
                 **kwargs) -> bool:
        return self._copy(self._spark, source, destination, recurse, **kwargs)

    def help(self):
        docstring = f"""
             {self.name}(path: str)

             Copy a file or directory

             Parameters
             ----------

             source : str
                      The source path

             desintation : str
                      The destination path


             Returns
             -------
             
             bool

                    A flag indicating whether the path was successfully copied

        """
        print(textwrap.dedent(docstring))

    def _copy(self,
              spark: SparkSession,
              source: str,
              destination: str,
              recurse: bool = False,
              **kwargs) -> bool:
        dbutils_wrapper = honeycomb.utils.dbutils(spark)
        return dbutils_wrapper.fs.cp(source, destination, recurse)
