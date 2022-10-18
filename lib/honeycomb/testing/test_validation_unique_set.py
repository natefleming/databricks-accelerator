import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb._constraints._uniqueset import _UniqueSetFactory, _UniqueSet
from honeycomb.validate import Validation

from testing import sort_data_frame


def test_factory():

    config = {
        'values': ['red', 'green', 'blue'],
    }
    factory = _UniqueSetFactory()
    constraint = factory.from_config(None, config)

    assert type(constraint) == _UniqueSet
    assert constraint.column_names == config['values']


def test_should_raise_when_invalid_column(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([
        ['foo'],
        ['bar'],
        ['baz'],
    ], schema)

    validation = Validation(spark, input_df).is_composite_key(['invalid'])
    with pytest.raises(ValueError, match=r'Missing one or more columns:.*'):
        validation_results = validation.execute()


def test_validate_when_unique(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([
        ['foo'],
        ['bar'],
        ['baz'],
    ], schema)

    validation = Validation(spark, input_df).is_composite_key(['name'])
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

    validation = Validation(spark, input_df).is_composite_key(['name'])
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
    assert validation_error.constraint_name == 'unique_set'
    assert validation_error.number_of_errors == 2


def test_validate_when_not_unique_set(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
        T.StructField('age', T.IntegerType()),
        T.StructField('id', T.IntegerType()),
    ])
    input_df = spark.createDataFrame([
        ['foo', 1, 10],
        ['foo', 1, 11],
        ['foo', 2, 12],
        ['bar', 3, 13],
        ['bar', 3, 14],
        ['baz', 4, 15],
    ], schema)
    expected_correct_df = spark.createDataFrame([
        ['foo', 2, 12],
        ['baz', 4, 15],
    ], schema)
    expected_erroneous_df = spark.createDataFrame([
        ['foo', 1, 10],
        ['foo', 1, 11],
        ['bar', 3, 13],
        ['bar', 3, 14],
    ], schema)

    validation = Validation(spark, input_df).is_composite_key(['name', 'age'])
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

    assert validation_error.column_names == ['name', 'age']
    assert validation_error.constraint_name == 'unique_set'
    assert validation_error.number_of_errors == 4
