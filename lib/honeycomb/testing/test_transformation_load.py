import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation


def test_transformation_load(spark):
    schema = T.StructType([
        T.StructField('first_name', T.StringType()),
        T.StructField('last_name', T.StringType()),
        T.StructField('age', T.IntegerType()),
        T.StructField('height', T.IntegerType()),
        T.StructField('id', T.IntegerType()),
    ])

    input_df = spark.createDataFrame([
        ['  NATE ', '  fleming  ', 42, 200, 1],
        ['danielle', 'fleming', 35, 200, 2],
        [None, None, None, None, None],
        ['nate', 'weinrich', 49, 210, 3],
        ['kevin', 'schulke', 52, 215, 4],
        ['xxxxxxxkevin', 'xxxxxxxxschulke', 52, 215, 5],
        ['x', 'y', 52, 215, 6],
        [None, None, 52, -10, 7],
        [None, None, 52, 216, 8],
        [None, None, 52, 217, 9],
        [None, None, None, 218, 9],
        [None, None, None, None, None],
    ], schema)

    config = {}
    config["transformation"] = {
        "transformations": [{
            "name": "skip_empty_rows"
        }],
        "columns": [{
            "column": "age",
            "transformations": [{
                "name": "drop",
            }]
        }, {
            "column": "first_name",
            "transformations": [{
                "name": "trim",
            }, {
                "name": "lower",
            }]
        }, {
            "column":
                "last_name",
            "transformations": [{
                "name": "trim",
                "output_column": "last_name_trim",
            }]
        }]
    }

    transform = Transformation(spark, input_df, config)
    transformation_results = transform.execute()

    result_df = transformation_results.data

    assert input_df.count() == 12
    assert result_df.count() == 10

    assert len(input_df.schema.names) == 5
    assert len(result_df.schema.names) == 5

    assert result_df.select('first_name').collect()[0].first_name == 'nate'
    assert result_df.select('last_name').collect()[0].last_name == '  fleming  '
    assert result_df.select(
        'last_name_trim').collect()[0].last_name_trim == 'fleming'
