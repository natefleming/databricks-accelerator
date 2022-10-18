import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation
from honeycomb._transformations._literal import _LiteralFactory


def test_factory(spark):

    config = {
        'value': 'myvalue',
        'data_type': 'int',
        'output_column': 'myoutputcolumn'
    }

    factory = _LiteralFactory()
    transformation = factory.from_config(spark, 'mycolumn', config)

    assert transformation.column_name == 'mycolumn'
    assert transformation.value == config['value']
    assert transformation.data_type == config['data_type']


def test_transformation_literal_default_type(spark):
    schema = T.StructType([
        T.StructField('age', T.StringType()),
    ])
    input_df = spark.createDataFrame([[10]], schema)

    transformation = Transformation(spark, input_df)
    transformation.literal('name', 'foo')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'name'),
                 None)
    assert field is not None
    assert field.dataType == T.StringType()

    value = output_df.select('name').collect()[0].name
    assert value == 'foo'


def test_transformation_literal_specific_type(spark):
    schema = T.StructType([
        T.StructField('age', T.StringType()),
    ])
    input_df = spark.createDataFrame([[10]], schema)

    transformation = Transformation(spark, input_df)
    transformation.literal('name', 2.5, 'double')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'name'),
                 None)
    assert field is not None
    assert field.dataType == T.DoubleType()

    value = output_df.select('name').collect()[0].name
    assert value == 2.5
