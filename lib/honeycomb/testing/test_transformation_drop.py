import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation
from honeycomb._transformations._drop import _DropFactory


def test_factory(spark):

    config = {}

    factory = _DropFactory()
    transformation = factory.from_config(spark, 'mycolumn', config)

    assert transformation.column_name == 'mycolumn'


def test_transformation_drop(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
        T.StructField('age', T.IntegerType())
    ])
    input_df = spark.createDataFrame([
        ['foo', 0],
        ['bar', -10],
        ['baz', 100],
    ], schema)

    transformation = Transformation(spark, input_df)
    transformation.drop('age')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    columns = output_df.columns
    assert len(columns) == 1
    assert 'name' in columns
    assert 'age' not in columns
