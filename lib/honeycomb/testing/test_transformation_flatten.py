import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation
from honeycomb._transformations._flatten import _FlattenFactory


def test_factory(spark):

    config = {}

    factory = _FlattenFactory()
    transformation = factory.from_config(spark, 'mycolumn', config)

    assert transformation.column_name == 'mycolumn'


def test_transformation_flatten_with_prefix(spark):
    data = '''
    [
    {
        "timestamp": "2001-01-01",
        "ev": {
                    "app": {
                        "appName": "XYZ",
                        "appVersion": "1.2.0"
                    },
                    "device": {
                        "deviceId": "ABC"
                    }
        }
    }
    ]
    '''
    input_df = spark.read.option('multiline', True).json(
        spark.sparkContext.parallelize([data]))
    input_df.printSchema()

    transformation = Transformation(spark, input_df)
    transformation.flatten('ev.app', 'foo')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    output_df.printSchema()
    value = output_df.select('foo_appVersion').collect()[0].foo_appVersion
    assert value == '1.2.0'


def test_transformation_flatten(spark):
    data = '''
    [
    {
        "timestamp": "2001-01-01",
        "ev": {
                    "app": {
                        "appName": "XYZ",
                        "appVersion": "1.2.0"
                    },
                    "device": {
                        "deviceId": "ABC"
                    }
        }
    }
    ]
    '''
    input_df = spark.read.option('multiline', True).json(
        spark.sparkContext.parallelize([data]))
    input_df.printSchema()

    transformation = Transformation(spark, input_df)
    transformation.flatten('ev.app')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    output_df.printSchema()
    value = output_df.select('ev_app_appVersion').collect()[0].ev_app_appVersion
    assert value == '1.2.0'
