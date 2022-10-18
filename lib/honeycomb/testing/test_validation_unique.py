import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.validate import Validation
from honeycomb._constraints._unique import _UniqueFactory, _Unique

from testing import sort_data_frame


def test_factory():

    config = {}
    factory = _UniqueFactory()
    constraint = factory.from_config('mycolumn', config)

    assert type(constraint) == _Unique
    assert constraint.column_name == 'mycolumn'


def test_should_raise_when_invalid_column(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([
        ['foo'],
        ['bar'],
        ['baz'],
    ], schema)

    validation = Validation(spark, input_df).is_unique('invalid')
    with pytest.raises(ValueError, match=r'Missing one or more columns:.*'):
        validation_results = validation.execute()


def test_validate_when_unique(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([
        ['foo'],
        ['bar'],
        ['baz'],
    ], schema)

    validation = Validation(spark, input_df).is_unique('name')
    validation_results = validation.execute()

    expected_df = input_df
    actual_df = validation_results.correct_data
    actual_df = sort_data_frame(actual_df.toPandas(), schema.fieldNames())
    expected_df = sort_data_frame(expected_df.toPandas(), schema.fieldNames())

    pd.testing.assert_frame_equal(
        expected_df,
        actual_df,
        check_like=True,
    )
    assert len(validation_results.errors) == 0
    assert validation_results.erroneous_data.rdd.isEmpty()


def test_validate_when_not_unique(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
        T.StructField('age', T.IntegerType())
    ])
    input_df = spark.createDataFrame([
        ['foo', 1],
        ['foo', 2],
        ['bar', 3],
        ['baz', 4],
    ], schema)
    expected_correct_df = spark.createDataFrame([
        ['bar', 3],
        ['baz', 4],
    ], schema)
    expected_erroneous_df = spark.createDataFrame([
        ['foo', 1],
        ['foo', 2],
    ], schema)

    validation = Validation(spark, input_df).is_unique('name')
    validation_results = validation.execute()

    actual_correct_df = validation_results.correct_data
    actual_correct_df = sort_data_frame(actual_correct_df.toPandas(),
                                        schema.fieldNames())

    actual_erroneous_df = validation_results.erroneous_data
    actual_erroneous_df = sort_data_frame(actual_erroneous_df.toPandas(),
                                          schema.fieldNames())

    errors = validation_results.errors

    expected_correct_df = sort_data_frame(expected_correct_df.toPandas(),
                                          schema.fieldNames())
    expected_erroneous_df = sort_data_frame(expected_erroneous_df.toPandas(),
                                            schema.fieldNames())

    pd.testing.assert_frame_equal(
        expected_correct_df,
        actual_correct_df,
        check_like=True,
    )

    pd.testing.assert_frame_equal(
        expected_erroneous_df,
        actual_erroneous_df,
        check_like=True,
    )

    assert len(errors) == 1

    validation_error = next(iter(e for e in errors))

    assert validation_error.column_names == ['name']
    assert validation_error.constraint_name == 'unique'
    assert validation_error.number_of_errors == 2
