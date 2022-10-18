import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.suggestion import Suggestion
from honeycomb.validate import Validation


def test_validation_load(spark):
    schema = T.StructType([
        T.StructField('first_name', T.StringType()),
        T.StructField('last_name', T.StringType()),
        T.StructField('age', T.IntegerType()),
        T.StructField('height', T.IntegerType()),
        T.StructField('id', T.IntegerType()),
    ])
    training_df = spark.createDataFrame([
        ['nate', 'fleming', 42, 200, 1],
        ['danielle', 'fleming', 35, 200, 2],
        ['nate', 'weinrich', 49, 210, 3],
        ['kevin', 'schulke', 51, 215, 4],
        ['shaun', None, 52, 215, 5],
    ], schema)

    input_df = spark.createDataFrame([
        ['nate', 'fleming', 42, 200, 1],
        ['danielle', 'fleming', 35, 200, 2],
        ['nate', 'weinrich', 49, 210, 3],
        ['kevin', 'schulke', 52, 215, 4],
        ['xxxxxxxkevin', 'xxxxxxxxschulke', 52, 215, 5],
        ['x', 'y', 52, 215, 6],
        [None, None, 52, -10, 7],
        [None, None, 52, 216, 8],
        [None, None, 52, 217, 9],
        [None, None, None, 218, 9],
    ], schema)

    suggestion = Suggestion(spark, training_df)
    suggestion_results = suggestion.execute()

    config = {}
    config['validation'] = suggestion_results.to_dict()

    validate = Validation(spark, input_df, config)
    validation_results = validate.execute()

    correct_df = validation_results.correct_data
    erroneous_df = validation_results.erroneous_data
    errors = validation_results.errors

    assert len(errors) == 15

    first_name_errors = [e for e in errors if e.column_names == ['first_name']]
    print(first_name_errors)
    assert any(e.constraint_name == 'not_null' for e in first_name_errors)
    assert any(e.constraint_name == 'one_of' for e in first_name_errors)
    assert any(e.constraint_name == 'text_length' for e in first_name_errors)
    assert len(first_name_errors) == 3

    last_name_errors = [e for e in errors if e.column_names == ['last_name']]
    print(last_name_errors)
    assert any(e.constraint_name == 'one_of' for e in last_name_errors)
    assert any(e.constraint_name == 'text_length' for e in last_name_errors)
    assert len(last_name_errors) == 2

    age_errors = [e for e in errors if e.column_names == ['age']]
    print(age_errors)
    assert any(e.constraint_name == 'not_null' for e in age_errors)
    assert any(e.constraint_name == 'unique' for e in age_errors)
    assert len(age_errors) == 2

    height_errors = [e for e in errors if e.column_names == ['height']]
    print(height_errors)
    assert any(e.constraint_name == 'one_of' for e in height_errors)
    assert any(e.constraint_name == 'between' for e in height_errors)
    assert any(e.constraint_name == 'min' for e in height_errors)
    assert any(e.constraint_name == 'max' for e in height_errors)
    assert len(height_errors) == 4
