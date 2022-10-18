from typing import Any, Tuple, List

from honeycomb._command._Command import _Command
from honeycomb._command._Nameable import _Nameable
from honeycomb._command._HelpMixin import _HelpMixin

from honeycomb._filesystem._Exists import _Exists
from honeycomb._filesystem._IsDir import _IsDir
from honeycomb._filesystem._IsFile import _IsFile
from honeycomb._filesystem._Mount import _Mount
from honeycomb._filesystem._Read import _Read
from honeycomb._filesystem._Unmount import _Unmount
from honeycomb._filesystem._Remove import _Remove
from honeycomb._filesystem._List import _List
from honeycomb._filesystem._MkDirs import _MkDirs
from honeycomb._filesystem._Copy import _Copy
from honeycomb._filesystem._Write import _Write
from honeycomb._filesystem._Move import _Move

from pyspark.sql import SparkSession

MountInfo = Any
FileInfo = Any


class Filesystem(_Nameable, _HelpMixin):

    def __init__(self, spark: SparkSession, client: str = None):
        super().__init__('honeycomb.filesystem')
        self._spark = spark
        self._client = client

        self._is_dir = _IsDir(self._spark)
        self._is_file = _IsFile(self._spark)
        self._mount = _Mount(self._spark, self._client)
        self._read = _Read(self._spark)
        self._unmount = _Unmount(self._spark)
        self._exists = _Exists(self._spark)
        self._remove = _Remove(self._spark)
        self._list = _List(self._spark)
        self._mkdirs = _MkDirs(self._spark)
        self._copy = _Copy(self._spark)
        self._write = _Write(self._spark)
        self._move = _Move(self._spark)

    def help(self):
        self._is_dir.help()
        self._is_file.help()
        self._mount.help()
        self._read.help()
        self._unmount.help()
        self._remove.help()
        self._list.help()
        self._mkdirs.help()
        self._copy.help()
        self._move.help()
        self._write.help()

    @property
    def exists(self) -> _Command:
        return self._exists

    @property
    def is_dir(self) -> _Command:
        return self._is_dir

    @property
    def is_file(self) -> _Command:
        return self._is_file

    @property
    def mount(self) -> _Command:
        return self._mount

    @property
    def read(self) -> _Command:
        return self._read

    @property
    def unmount(self) -> _Command:
        return self._unmount

    @property
    def rm(self) -> _Command:
        return self._remove

    @property
    def ls(self) -> _Command:
        return self._list

    @property
    def mkdirs(self) -> _Command:
        return self._mkdirs

    @property
    def cp(self) -> _Command:
        return self._copy

    @property
    def mv(self) -> _Command:
        return self._move

    @property
    def write(self) -> _Command:
        return self._write


def mount(spark: SparkSession,
          container: str,
          client=None,
          **kwargs) -> [MountInfo, Exception]:
    command = _Mount(spark, client)
    return command(container, **kwargs)


def unmount(spark: SparkSession, container: str,
            **kwargs) -> Tuple[bool, Exception]:
    command = _Unmount(spark)
    return command(container, **kwargs)


def exists(spark: SparkSession, path: str, **kwargs) -> bool:
    command = _Exists(spark)
    return command(path, **kwargs)


def is_file(spark: SparkSession, path: str, **kwargs) -> bool:
    command = _IsFile(spark)
    return command(path, **kwargs)


def is_dir(spark: SparkSession, path: str, **kwargs) -> bool:
    command = _IsDir(spark)
    return command(path, **kwargs)


def read(spark: SparkSession,
         path: str,
         default=None,
         **kwargs) -> Tuple[str, Exception]:
    command = _Read(spark)
    return command(path, default, **kwargs)


def rm(spark: SparkSession, path: str, **kwargs) -> List[Tuple[FileInfo, bool]]:
    command = _Remove(spark)
    return command(path, **kwargs)


def ls(spark: SparkSession, path: str, **kwargs) -> List[FileInfo]:
    command = _List(spark)
    return command(path, **kwargs)


def mkdirs(spark: SparkSession, path: str, **kwargs) -> bool:
    command = _MkDirs(spark)
    return command(path, **kwargs)


def cp(spark: SparkSession,
       source: str,
       destination: str,
       recurse: bool = False,
       **kwargs) -> bool:
    command = _Copy(spark)
    return command(source, destination, recurse, **kwargs)


def mv(spark: SparkSession,
       source: str,
       destination: str,
       recurse: bool = False,
       **kwargs) -> bool:
    command = _Move(spark)
    return command(source, destination, recurse, **kwargs)


def write(spark: SparkSession,
          path: str,
          contents: str,
          overwrite: bool = False,
          **kwargs) -> bool:
    command = _Write(spark)
    return command(path, contents, overwrite, **kwargs)
