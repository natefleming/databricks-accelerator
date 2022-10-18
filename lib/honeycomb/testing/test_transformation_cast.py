import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation
from honeycomb._transformations._cast import _CastFactory


def test_factory(spark):

    config = {'data_type': 'int', 'output_column': 'myoutputcolumn'}

    factory = _CastFactory()
    transformation = factory.from_config(spark, 'mycolumn', config)

    assert transformation.column_name == 'mycolumn'
    assert transformation.data_type == config['data_type']
    assert transformation.output_column == config['output_column']


def test_transformation_cast(spark):
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
    transformation.cast('age', 'double')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'age'),
                 None)
    assert field is not None
    assert field.dataType == T.DoubleType()


def test_transformation_cast_new_col(spark):
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
    transformation.cast('age', 'double', 'new')
    transformation_result = transformation.execute()
    output_df = transformation_result.data

    age_field = next(
        iter(f for f in output_df.schema.fields if f.name == 'age'), None)
    assert age_field is not None
    assert age_field.dataType == T.IntegerType()

    new_field = next(
        iter(f for f in output_df.schema.fields if f.name == 'new'), None)
    assert new_field is not None
    assert new_field.dataType == T.DoubleType()
