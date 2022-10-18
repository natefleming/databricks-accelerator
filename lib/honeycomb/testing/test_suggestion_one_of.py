import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.suggestion import Suggestion


def test_suggest_one_of_empty(spark):
    schema = T.StructType([T.StructField('age', T.IntegerType())])
    input_df = spark.createDataFrame([], schema)

    suggestion = Suggestion(spark, input_df)
    suggestion._suggestions = []
    suggestion.one_of(T.StructField('age', T.IntegerType()))
    suggestion_results = suggestion.execute()

    assert len(suggestion_results.data) == 0


def test_suggest_one_of_many_distinct(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
        T.StructField('age', T.IntegerType())
    ])
    input_df = spark.createDataFrame([
        ['foo', 0],
        ['bar', -10],
        ['baz', 100],
        ['foo', 1],
        ['bar', -11],
        ['baz', 101],
    ], schema)

    suggestion = Suggestion(spark, input_df)
    suggestion._suggestions = []
    suggestion.one_of(T.StructField('age', T.IntegerType()))
    suggestion_results = suggestion.execute()

    assert len(suggestion_results.data) == 0


def test_suggest_one_of(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
        T.StructField('age', T.IntegerType())
    ])
    input_df = spark.createDataFrame([
        ['foo', 0],
        ['bar', -10],
        ['baz', 100],
    ], schema)

    suggestion = Suggestion(spark, input_df)
    suggestion._suggestions = []
    suggestion.one_of(T.StructField('age', T.IntegerType()))
    suggestion_results = suggestion.execute()

    assert len(suggestion_results.data) == 1
    suggestion_result = next(iter(x for x in suggestion_results.data))
    assert suggestion_result.column_name == 'age'
    assert len(suggestion_result.constraints) == 1
    suggested_constraint = next(iter(x for x in suggestion_result.constraints))
    assert suggested_constraint.constraint_name == 'one_of'
    assert 'values' in suggested_constraint.data
    assert suggested_constraint.data['values'].sort() == [0, -10, 100].sort()
