import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation
from honeycomb._transformations._regexreplace import _RegexReplaceFactory


def test_factory(spark):

    config = {
        'pattern': 'mypattern',
        'replacement': 'myreplacement',
        'output_column': 'myoutputcolumn'
    }

    factory = _RegexReplaceFactory()
    transformation = factory.from_config(spark, 'mycolumn', config)

    assert transformation.column_name == 'mycolumn'
    assert transformation.pattern == config['pattern']
    assert transformation.replacement == config['replacement']
    assert transformation.output_column == config['output_column']


def test_transformation_replace(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
    ])
    input_df = spark.createDataFrame([
        ['foo'],
        ['bar'],
        ['baz'],
    ], schema)

    transformation = Transformation(spark, input_df)
    transformation.replace('name', r'b.*', 'qui')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    assert output_df.select('name').filter(F.col('name') == 'qui').count() == 2


def test_transformation_replace_new_col(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
    ])
    input_df = spark.createDataFrame([
        ['foo'],
        ['bar'],
        ['baz'],
    ], schema)

    transformation = Transformation(spark, input_df)
    transformation.replace('name', r'b.*', 'qui', 'new_col')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    assert output_df.select('new_col').filter(
        F.col('new_col') == 'qui').count() == 2
    assert output_df.select('name').filter((F.col('name') == 'bar') | (
        F.col('name') == 'baz')).count() == 2
