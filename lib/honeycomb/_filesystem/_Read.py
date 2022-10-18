import textwrap
import logging

from typing import Any, Tuple
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

LOGGER = logging.getLogger(__name__)


class _Read(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.filesystem.read')
        self._spark = spark

    def __call__(self,
                 path: str,
                 default=None,
                 **kwargs) -> Tuple[str, Exception]:
        try:
            contents = self._read(self._spark, path)
            result = (contents, None)
        except Exception as e:
            result = (default, e)

        return result

    def help(self):
        docstring = f"""
             {self.name}(path: str)

             Return the contents of the file specified by `path` 

             Parameters
             ----------

             path : str
                    The path

             default : str (optional)
                       The value to return if an exception occurs

             Returns
             -------
             
             Tuple[str, Exception]

                    A tuple containing the contents of the file or any raised exceptions

        """
        print(textwrap.dedent(docstring))

    def _read(self, spark: SparkSession, path: str) -> str:
        contents: str = None
        if path.startswith('/dbfs'):
            with open(path) as fin:
                contents = fin.read()
        else:
            rdd = spark.sparkContext.wholeTextFiles(path)
            contents = rdd.collect()[0][1]
        return contents
