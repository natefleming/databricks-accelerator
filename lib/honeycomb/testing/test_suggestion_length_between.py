import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.suggestion import Suggestion


def test_suggest_length_between_empty(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    suggestion = Suggestion(spark, input_df)
    suggestion._suggestions = []
    suggestion.length_between(T.StructField('name', T.StringType()))
    suggestion_results = suggestion.execute()

    assert len(suggestion_results.data) == 0


def test_suggest_length_between(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
        T.StructField('age', T.IntegerType())
    ])
    input_df = spark.createDataFrame([
        ['foox', 0],
        ['foo', 0],
        ['bar', -10],
        ['baz', 100],
    ], schema)

    suggestion = Suggestion(spark, input_df)
    suggestion._suggestions = []
    suggestion.length_between(T.StructField('name', T.StringType()))
    suggestion_results = suggestion.execute()

    assert len(suggestion_results.data) == 1
    suggestion_result = next(iter(x for x in suggestion_results.data))
    assert suggestion_result.column_name == 'name'
    assert len(suggestion_result.constraints) == 1
    suggested_constraint = next(iter(x for x in suggestion_result.constraints))
    assert suggested_constraint.constraint_name == 'length_between'
    assert 'lower_bound' in suggested_constraint.data
    assert 'upper_bound' in suggested_constraint.data
    assert suggested_constraint.data['lower_bound'] == 3
    assert suggested_constraint.data['upper_bound'] == 4
