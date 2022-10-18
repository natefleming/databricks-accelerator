import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation
from honeycomb._transformations._skipemptyrows import _SkipEmptyRowsFactory


def test_factory(spark):

    config = {}

    factory = _SkipEmptyRowsFactory()
    transformation = factory.from_config(spark, None, config)

    assert transformation.column_name == None


def test_transformation_skip_empty_rows(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
        T.StructField('age', T.IntegerType())
    ])
    input_df = spark.createDataFrame([
        ['foo', 0],
        [None, None],
        ['baz', 100],
        [None, 100],
        ['qui', None],
        [None, None],
    ], schema)

    transformation = Transformation(spark, input_df)
    transformation.skip_empty_rows()
    transformation_result = transformation.execute()
    output_df = transformation_result.data

    assert input_df.count() == 6
    assert output_df.count() == 4
