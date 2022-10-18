import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation
from honeycomb._transformations._rename import _RenameFactory


def test_factory(spark):

    config = {'to': 'myoutputcolumn'}

    factory = _RenameFactory()
    transformation = factory.from_config(spark, 'mycolumn', config)

    assert transformation.column_name == 'mycolumn'
    assert transformation.new_column_name == config['to']


def test_transformation_rename_complex(spark):

    data = '''
    [
    {
        "timestamp": "2001-01-01",
        "ev": {
                    "app": {
                        "app Name": "XYZ",
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
    print(f'transformation: {transformation}')
    transformation.rename('ev.app', '_app')
    transformation_result = transformation.execute()
    print(f'execute complete')
    output_df = transformation_result.data

    output_df.printSchema()

    value = output_df.select('ev._app.appVersion').collect()[0].appVersion
    assert value == '1.2.0'

    transformation = Transformation(spark, input_df)
    print(f'transformation: {transformation}')
    transformation.rename('ev.app.app Name', '_app_name')
    transformation_result = transformation.execute()
    print(f'execute complete')
    output_df = transformation_result.data

    output_df.printSchema()

    value = output_df.select('ev.app._app_name').collect()[0]._app_name
    assert value == 'XYZ'


def test_transformation_rename(spark):
    schema = T.StructType([
        T.StructField('firstnaame', T.StringType()),
        T.StructField('age', T.IntegerType()),
        T.StructField('lastname', T.StringType()),
    ])
    input_df = spark.createDataFrame([['myfirst', 10, 'mylast']], schema)

    transformation = Transformation(spark, input_df)
    transformation.rename('age', 'age1')
    transformation_result = transformation.execute()
    output_df = transformation_result.data

    output_df.printSchema()

    age_col = next(iter(f for f in output_df.schema.fields if f.name == 'age'),
                   None)

    assert age_col is None

    age1_col = next(
        iter(f for f in output_df.schema.fields if f.name == 'age1'), None)

    assert age1_col is not None

    value = output_df.select('age1').collect()[0].age1
    assert value == 10
