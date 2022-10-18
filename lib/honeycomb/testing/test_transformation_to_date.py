import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd
import datetime

from honeycomb.transform import Transformation
from honeycomb._transformations._todate import _ToDateFactory


def test_factory(spark):

    config = {'format': 'format', 'output_column': 'myoutputcolumn'}

    factory = _ToDateFactory()
    transformation = factory.from_config(spark, 'mycolumn', config)

    assert transformation.column_name == 'mycolumn'
    assert transformation.format == config['format']
    assert transformation.output_column == config['output_column']


def test_transformation_to_date(spark):
    schema = T.StructType([
        T.StructField('date', T.StringType()),
    ])
    input_df = spark.createDataFrame([
        ['2020-01-02'],
    ], schema)

    transformation = Transformation(spark, input_df)
    transformation.to_date('date', 'yyyy-MM-dd')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'date'),
                 None)
    assert field is not None
    assert field.dataType == T.DateType()
    assert output_df.collect()[0].date == datetime.date(2020, 1, 2)


def test_transformation_to_date_from_int(spark):
    schema = T.StructType([
        T.StructField('date', T.IntegerType()),
    ])
    input_df = spark.createDataFrame([
        [20200102],
    ], schema)

    transformation = Transformation(spark, input_df)
    transformation.to_date('date', 'yyyyMMdd')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'date'),
                 None)
    assert field is not None
    assert field.dataType == T.DateType()
    assert output_df.collect()[0].date == datetime.date(2020, 1, 2)


def test_transformation_to_date_us(spark):
    schema = T.StructType([
        T.StructField('date', T.StringType()),
    ])
    input_df = spark.createDataFrame([
        ['01/02/2020'],
    ], schema)

    transformation = Transformation(spark, input_df)
    transformation.to_date('date', 'MM/dd/yyyy')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'date'),
                 None)
    assert field is not None
    assert field.dataType == T.DateType()
    assert output_df.collect()[0].date == datetime.date(2020, 1, 2)


def test_transformation_to_date_new_column(spark):

    schema = T.StructType([
        T.StructField('date', T.StringType()),
    ])
    input_df = spark.createDataFrame([
        ['2020-01-02'],
    ], schema)

    transformation = Transformation(spark, input_df)
    transformation.to_date('date', 'yyyy-MM-dd', 'new')
    transformation_result = transformation.execute()
    output_df = transformation_result.data

    age_field = next(
        iter(f for f in output_df.schema.fields if f.name == 'date'), None)
    assert age_field is not None
    assert age_field.dataType == T.StringType()
    assert output_df.collect()[0].date == '2020-01-02'

    new_field = next(
        iter(f for f in output_df.schema.fields if f.name == 'new'), None)
    assert new_field is not None
    assert new_field.dataType == T.DateType()
    assert output_df.collect()[0].new == datetime.date(2020, 1, 2)
