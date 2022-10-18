import textwrap
import re
import logging
from typing import Any, Tuple

import honeycomb.utils
from honeycomb._command._Command import _Command

from pyspark.sql.session import SparkSession


class _Unmount(_Command):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.filesystem.unmount')
        self._spark = spark

    def __call__(self,
                 container: str = None,
                 **kwargs) -> Tuple[bool, Exception]:
        if container is None and 'mount_point' not in kwargs:
            raise ValueError(
                '__call__(container, mount_point=/mnt/[container])')

        mount_point = kwargs.get('mount_point', '/mnt/{0}'.format(container))
        try:
            result = self._unmount(self._spark, mount_point)
            return (result, None)
        except Exception as e:
            return (False, e)

    def help(self):
        docstring = f"""
             {self.name}(container: str)

             Delete a mount point

             Parameters
             ----------

             container : str
                         The ADLS container to be mounted. default: None

             mount_point : str, optional
                           The DBFS location which the container will be mounted. default: /mnt/[container]

             Returns
             -------
             
             Tuple[bool, Exception]

                    A tuple containing a boolean indicating if the container was unmounted and any raised exceptions

            Examples
            --------

            Delete the mount location at /mnt/trusted
            >>> honeycomb.filesystem.unmount('trusted')

            Delete the mount location at /mnt/trusted
            >>> honeycomb.filesystem.unmount(mount_point='/mnt/trusted')

        """
        print(textwrap.dedent(docstring))

    def _unmount(self, spark: SparkSession, mount_point: str):
        dbutils_wrapper = honeycomb.utils.dbutils(spark)
        result = dbutils_wrapper.fs.unmount(mount_point=mount_point)
        return result
