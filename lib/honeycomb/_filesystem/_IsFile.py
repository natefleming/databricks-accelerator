import textwrap
from typing import Any

import honeycomb.utils
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession


class _IsFile(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.filesystem.is_file')
        self._spark = spark

    def __call__(self, path: str, **kwargs) -> bool:
        result = self._is_file(self._spark, path)
        return result

    def help(self):
        docstring = f"""
             {self.name}(path: str)

             Determine if this path is a file.

             Parameters
             ----------

             path : str
                    The path

             Returns
             -------
             
             bool
                    True if the path is a file else False

        """
        print(textwrap.dedent(docstring))

    def _is_file(self, spark: SparkSession, path: str) -> bool:
        if path.startswith('/dbfs'):
            import os
            return os.path.isfile(path)
        else:
            try:
                dbutils_wrapper = honeycomb.utils.dbutils(spark)
                dbutils_wrapper.fs.head(path, 0)
                return True
            except Exception as e:
                return False
