import textwrap
import logging
import re

import honeycomb.utils
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

LOGGER = logging.getLogger(__name__)


class _Write(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.filesystem.write')
        self._spark = spark

    def __call__(self,
                 path: str,
                 contents: str,
                 overwrite: bool = False,
                 **kwargs) -> bool:
        return self._copy(self._spark, path, contents, overwrite, **kwargs)

    def help(self):
        docstring = f"""
             {self.name}(path: str)

             Write contents to file

             Parameters
             ----------

             path : str
                    The destination

             contents : str
                        The file contents

            overwrite : bool (optional)
                        Overwrite an existing file

             Returns
             -------
             
             bool

                    A flag indicating whether the path was successfully written

        """
        print(textwrap.dedent(docstring))

    def _copy(self,
              spark: SparkSession,
              path: str,
              contents: str,
              overwrite: bool = False,
              **kwargs) -> bool:
        dbutils_wrapper = honeycomb.utils.dbutils(spark)
        return dbutils_wrapper.fs.put(path, contents, overwrite)
