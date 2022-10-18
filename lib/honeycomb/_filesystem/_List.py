import textwrap
import logging
import re
import os
import errno
import os.path

from typing import Any, Tuple, List

import honeycomb.utils
import honeycomb.filesystem
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession

FileInfo = Any

LOGGER = logging.getLogger(__name__)


class _List(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.filesystem.ls')
        self._spark = spark

    def __call__(self, path: str, **kwargs) -> List[FileInfo]:
        results = self._list(self._spark, path)
        return results

    def help(self):
        docstring = f"""
             {self.name}(path: str)

             List the files in the path

             Parameters
             ----------

             path : str
                    The path


             Returns
             -------
             
             List[dbutils.FileInfo]

                    A list of file objects

        """
        print(textwrap.dedent(docstring))

    def _unix_to_posix(self, unix_regex: str) -> str:
        posix_regex = unix_regex.replace('.', '\\.')
        posix_regex = posix_regex.replace('?', '.')
        posix_regex = posix_regex.replace('*', '.*')
        posix_regex = f'^{posix_regex}$'
        return posix_regex

    def _list(self, spark: SparkSession, path: str) -> List[FileInfo]:
        dbutils_wrapper = honeycomb.utils.dbutils(spark)

        if honeycomb.filesystem.is_dir(spark, path):
            files = dbutils_wrapper.fs.ls(path)
        else:
            parent_dir = os.path.dirname(path)
            base_name = os.path.basename(path)
            pattern = self._unix_to_posix(base_name)
            files = [
                f for f in dbutils_wrapper.fs.ls(parent_dir)
                if bool(re.match(pattern, f.name))
            ]
            #if len(files) <= 0:
            #    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

        return files
