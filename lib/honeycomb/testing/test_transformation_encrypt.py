import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation
from honeycomb._transformations._encrypt import _EncryptFactory


def test_factory(spark):

    config = {'num_bits': '256', 'output_column': 'myoutputcolumn'}

    factory = _EncryptFactory()
    transformation = factory.from_config(spark, 'mycolumn', config)

    assert transformation.column_name == 'mycolumn'
    assert transformation.num_bits == int(config['num_bits'])
    assert transformation.output_column == config['output_column']


def test_transformation_encrypt_string(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
    ])
    input_df = spark.createDataFrame([
        ['foo'],
        ['bar'],
        ['baz'],
        [None],
    ], schema)

    transformation = Transformation(spark, input_df)
    transformation.encrypt('name')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'name'),
                 None)
    assert field is not None

    rows = output_df.filter(F.col('name').isNotNull()).collect()
    for r in rows:
        assert len(r.name) == 256 / 4


def test_transformation_encrypt_int(spark):
    schema = T.StructType([
        T.StructField('name', T.IntegerType()),
    ])
    input_df = spark.createDataFrame([
        [1],
        [10],
        [200],
        [None],
    ], schema)

    transformation = Transformation(spark, input_df)
    transformation.encrypt('name')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'name'),
                 None)
    assert field is not None

    rows = output_df.filter(F.col('name').isNotNull()).collect()
    for r in rows:
        assert len(r.name) == 256 / 4


def test_transformation_encrypt_384(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
    ])
    input_df = spark.createDataFrame([
        ['foo'],
        [None],
        ['bar'],
        ['baz'],
        [None],
    ], schema)

    transformation = Transformation(spark, input_df)
    transformation.encrypt('name', 384)
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'name'),
                 None)
    assert field is not None

    rows = output_df.filter(F.col('name').isNotNull()).collect()
    for r in rows:
        assert len(r.name) == 384 / 4

    output_df.show()
