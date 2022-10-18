import textwrap
from typing import Any

import honeycomb.utils
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession


class _Exists(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.filesystem.exists')
        self._spark = spark

    def __call__(self, path: str, **kwargs):
        result = self._exists(self._spark, path)
        return result

    def help(self):
        docstring = f"""
             {self.name}(path: str)

             Determine if this path exists.

             Parameters
             ----------

             path : str
                    The directory or file path

             Returns
             -------
             
             str
                    True if the path exists else False

        """
        print(textwrap.dedent(docstring))

    def _exists(self, spark: SparkSession, path: str) -> bool:
        if path.startswith('/dbfs'):
            import os
            return os.path.exists(path)
        else:
            try:
                dbutils_wrapper = honeycomb.utils.dbutils(spark)
                dbutils_wrapper.fs.ls(path)
                return True
            except Exception as e:
                if 'java.io.FileNotFoundException' in str(e):
                    return False
                else:
                    raise
