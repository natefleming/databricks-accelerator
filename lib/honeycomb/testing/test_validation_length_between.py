import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.validate import Validation
from honeycomb._constraints._textlength import _TextLengthFactory, _TextLength

from testing import sort_data_frame


def test_factory():

    config = {
        'lower_bound': 10,
        'upper_bound': 11,
    }
    factory = _TextLengthFactory()
    constraint = factory.from_config('mycolumn', config)

    assert type(constraint) == _TextLength
    assert constraint.column_name == 'mycolumn'
    assert constraint._lower_bound == config['lower_bound']
    assert constraint._upper_bound == config['upper_bound']


def test_should_raise_when_invalid_column(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([
        ['foo'],
        ['bar'],
        ['baz'],
    ], schema)

    validation = Validation(spark,
                            input_df).has_length_between('invalid', 0, 100)
    with pytest.raises(ValueError, match=r'Missing one or more columns:.*'):
        validation_results = validation.execute()


def test_validate_has_length_between(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([
        ['foo'],
        ['bar'],
        ['baz'],
        ['bazx'],
    ], schema)

    validation = Validation(spark, input_df).has_length_between('name', 3, 4)
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


def test_validate_when_length_not_between(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
        T.StructField('age', T.IntegerType())
    ])
    input_df = spark.createDataFrame([
        ['aa', 1],
        ['foo', 2],
        ['bar', 3],
        ['baz', 4],
        ['bazx', 5],
    ], schema)
    expected_correct_df = spark.createDataFrame([
        ['foo', 2],
        ['bar', 3],
        ['baz', 4],
    ], schema)
    expected_erroneous_df = spark.createDataFrame([
        ['aa', 1],
        ['bazx', 5],
    ], schema)

    validation = Validation(spark, input_df).has_length_between('name', 3, 3)
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
    assert validation_error.constraint_name == 'text_length'
    assert validation_error.number_of_errors == 2
