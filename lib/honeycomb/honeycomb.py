from honeycomb._command._Nameable import _Nameable
from honeycomb._command._HelpMixin import _HelpMixin
from honeycomb.filesystem import Filesystem
from honeycomb.table import Table
from honeycomb.metadata import Metadata

from pyspark.sql.session import SparkSession


class Honeycomb(_Nameable, _HelpMixin):

    def __init__(self, spark: SparkSession, client: str = None):
        super().__init__('honeycomb')
        self._spark = spark
        self._client = client

        self._filesystem = Filesystem(self._spark, self._client)
        self._table = Table(self._spark)

    @property
    def filesystem(self):
        return self._filesystem

    @property
    def table(self):
        return self._table

    def metadata(self, df):
        return Metadata(self._spark, df)

    def help(self):
        self._filesystem.help()
        self._table.help()
