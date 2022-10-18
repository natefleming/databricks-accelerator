from typing import List
import datetime

from honeycomb._command._Command import _Command

from honeycomb._command._Nameable import _Nameable
from honeycomb._command._HelpMixin import _HelpMixin

from honeycomb._table._IsPartitioned import _IsPartitioned
from honeycomb._table._Location import _Location
from honeycomb._table._StageLocation import _StageLocation
from honeycomb._table._Exists import _Exists
from honeycomb._table._IsManaged import _IsManaged
from honeycomb._table._IsExternal import _IsExternal
from honeycomb._table._IsTable import _IsTable
from honeycomb._table._IsView import _IsView
from honeycomb._table._Drop import _Drop
from honeycomb._table._IsDelta import _IsDelta
from honeycomb._table._Partitions import _Partitions
from honeycomb._table._Resolve import _Resolve

from pyspark.sql.session import SparkSession


class Table(_Nameable, _HelpMixin):

    def __init__(self, spark: SparkSession):
        super().__init__('honeycomb.table')
        self._spark = spark

        self._is_partitioned = _IsPartitioned(self._spark)
        self._location = _Location(self._spark)
        self._stage_location = _StageLocation(self._spark)
        self._exists = _Exists(self._spark)
        self._is_managed = _IsManaged(self._spark)
        self._is_external = _IsExternal(self._spark)
        self._is_table = _IsTable(self._spark)
        self._is_view = _IsView(self._spark)
        self._drop = _Drop(self._spark)
        self._is_delta = _IsDelta(self._spark)
        self._partitions = _Partitions(self._spark)
        self._resolve = _Resolve(self._spark)

    def help(self):
        self._is_partitioned.help()
        self._partitions.help()
        self._location.help()
        self._stage_location.help()
        self._exists.help()
        self._is_managed.help()
        self._is_external.help()
        self._is_table.help()
        self._is_view.help()
        self._drop.help()
        self._is_delta.help()
        self._resolve.help()

    @property
    def resolve(self) -> _Command:
        return self._resolve

    @property
    def is_partitioned(self) -> _Command:
        return self._is_partitioned

    @property
    def partitions(self) -> _Command:
        return self._partitions

    @property
    def location(self) -> _Command:
        return self._location

    @property
    def stage_location(self) -> _Command:
        return self._stage_location

    @property
    def exists(self) -> _Command:
        return self._exists

    @property
    def is_managed(self) -> _Command:
        return self._is_managed

    @property
    def is_external(self) -> _Command:
        return self._is_external

    @property
    def is_view(self) -> _Command:
        return self._is_view

    @property
    def is_table(self) -> _Command:
        return self._is_table

    @property
    def drop(self) -> _Command:
        return self._drop

    @property
    def is_delta(self) -> _Command:
        return self._is_delta


def resolve(spark: SparkSession,
            table: str,
            dst_dir: str = None,
            partition_cols: List[str] = [],
            time_now_dt: datetime.datetime = datetime.datetime.now(),
            **kwargs) -> bool:
    command = _Resolve(spark)
    return command(table, dst_dir, partition_cols, time_now_dt, **kwargs)


def is_partitioned(spark: SparkSession, table: str, **kwargs) -> bool:
    command = _IsPartitioned(spark)
    return command(table, **kwargs)


def partitions(spark: SparkSession, table: str, **kwargs) -> bool:
    command = _Partitions(spark)
    return command(table, **kwargs)


def location(spark: SparkSession, table: str, **kwargs) -> str:
    command = _Location(spark)
    return command(table, **kwargs)


def stage_location(spark: SparkSession, table: str, **kwargs) -> str:
    command = _StageLocation(spark)
    return command(table, **kwargs)


def exists(spark: SparkSession, table: str, **kwargs) -> bool:
    command = _Exists(spark)
    return command(table, **kwargs)


def is_managed(spark: SparkSession, table: str, **kwargs) -> bool:
    command = _IsManaged(spark)
    return command(table, **kwargs)


def is_external(spark: SparkSession, table: str, **kwargs) -> bool:
    command = _IsExternal(spark)
    return command(table, **kwargs)


def is_view(spark: SparkSession, table: str, **kwargs) -> bool:
    command = _IsView(spark)
    return command(table, **kwargs)


def is_table(spark: SparkSession, table: str, **kwargs) -> bool:
    command = _IsTable(spark)
    return command(table, **kwargs)


def drop(spark: SparkSession, table: str, **kwargs) -> bool:
    command = _Drop(spark)
    return command(table, **kwargs)


def is_delta(spark: SparkSession, table: str, **kwargs) -> bool:
    command = _IsDelta(spark)
    return command(table, **kwargs)
