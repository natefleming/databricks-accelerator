import re

import honeycomb
import honeycomb.utils

from typing import NamedTuple, List, Any

from honeycomb._odbc._Statement import _Statement
from honeycomb._odbc._Query import _Query

from pyspark.sql import SparkSession

import pyodbc


class StatementResult(NamedTuple):
    data: List[Any]


class Odbc:

    def __init__(self, spark: SparkSession, **kwargs):
        dbutils = honeycomb.utils.dbutils(spark)
        self.spark: SparkSession = spark
        secret_scope = 'honeycomb-secrets-kv' if 'secret_scope' not in kwargs else kwargs.get(
            'secret_scope')
        connection_key = 'ds-mssql-connection' if 'connection_key' not in kwargs else kwargs.get(
            'connection_key')
        connection_string = dbutils.secrets.get(scope=secret_scope,
                                                key=connection_key)

        self.connection: pyodbc.Connection = pyodbc.connect(connection_string)
        self.statements: List[_Statement] = []

    def query(self, sql):
        self._add(_Query(self.spark, self.connection, sql))
        return self

    def execute(self) -> StatementResult:
        data = []
        if self.statements:
            for statement in self.statements:
                output = statement.apply()
                data += [output]
        return StatementResult(data)

    def _add(self, statement: _Statement) -> None:
        self.statements.append(statement)
