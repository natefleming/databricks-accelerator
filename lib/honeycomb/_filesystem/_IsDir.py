import textwrap
from typing import Any

import honeycomb.utils
import honeycomb.filesystem
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession


class _IsDir(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.filesystem.is_dir')
        self._spark = spark

    def __call__(self, path: str, **kwargs) -> bool:
        return self._is_dir(self._spark, path)

    def help(self):
        docstring = f"""
             {self.name}(path: str)

             Determine if this path is a directory.

             Parameters
             ----------

             path : str
                    The path

             Returns
             -------
             
             bool
                    True if the path is a directory else False

        """
        print(textwrap.dedent(docstring))

    def _is_dir(self, spark: SparkSession, path: str) -> bool:
        if path.startswith('/dbfs'):
            import os
            return os.path.isdir(path)
        else:
            try:
                dbutils_wrapper = honeycomb.utils.dbutils(spark)
                dbutils_wrapper.fs.head(path, 0)
                return False
            except Exception as e:
                return 'Cannot head a directory' in str(e)
