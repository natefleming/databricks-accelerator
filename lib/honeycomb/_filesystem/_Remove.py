import os
import textwrap
import logging
import concurrent.futures

from typing import Any, Tuple, List

import honeycomb.utils
import honeycomb.filesystem
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

FileInfo = Any

LOGGER = logging.getLogger(__name__)


class _Remove(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.filesystem.rm')
        self._spark = spark

    def __call__(self, path: str,
                 **kwargs) -> List[Tuple[FileInfo, bool, Exception]]:
        DEFAULT_MAX_WORKERS: int = 10
        max_workers = int(kwargs.get('max_workers', DEFAULT_MAX_WORKERS))
        results = self._remove(self._spark, path, max_workers)
        return results

    def help(self):
        docstring = f"""
             {self.name}(path: str)

             Remove the files in the path

             Parameters
             ----------

             path : str
                    The path

             max_workers : int (optional)
                           The number of concurrent threads. Default: 10

             Returns
             -------
             
             List[Tuple[dbutils.FileInfo, bool, Exception]]

                    A list of tuples containing the file information and a bool describing whether or not the file was removed and any exception that may have occurred

        """
        print(textwrap.dedent(docstring))

    def _remove(self, spark: SparkSession, path: str,
                max_workers: int) -> List[Tuple[FileInfo, bool, Exception]]:

        dbutils_wrapper = honeycomb.utils.dbutils(spark)

        def remove(file_info) -> Tuple[FileInfo, bool, Exception]:
            result: Tuple[FileInfo, bool, Exception] = None
            try:
                result = (file_info,
                          dbutils_wrapper.fs.rm(file_info.path, True), None)
            except Exception as e:
                print(
                    f'An exception has occurred removing file: {file_info.path} - {e.message}'
                )
                result = (file_info, False, e)
            return result

        results = []
        if honeycomb.filesystem.is_dir(spark, path):
            path = path.rstrip('/')
            parent_dir = os.path.dirname(path)
            base_name = os.path.basename(path) + '/'
            file_info = next(
                f for f in honeycomb.filesystem.ls(spark, parent_dir)
                if f.name == base_name)
            results = [remove(file_info)]
        else:
            files = honeycomb.filesystem.ls(spark, path)
            if len(files) > 0:
                max_workers = min(max_workers, len(files))

                results = []
                with concurrent.futures.ThreadPoolExecutor(
                        max_workers=max_workers) as executor:
                    results = list(executor.map(remove, files))

        return results
