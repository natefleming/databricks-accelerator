import textwrap
import logging
import re

import honeycomb.utils
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

LOGGER = logging.getLogger(__name__)


class _MkDirs(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.filesystem.mkdirs')
        self._spark = spark

    def __call__(self, path: str, **kwargs) -> bool:
        return self._mkdirs(self._spark, path)

    def help(self):
        docstring = f"""
             {self.name}(path: str)

             Create a directory

             Parameters
             ----------

             path : str
                    The path


             Returns
             -------
             
             bool

                    A flag indicating whether the directory was successfully created

        """
        print(textwrap.dedent(docstring))

    def _mkdirs(self, spark: SparkSession, path: str) -> bool:
        dbutils_wrapper = honeycomb.utils.dbutils(spark)
        return dbutils_wrapper.fs.mkdirs(path)
