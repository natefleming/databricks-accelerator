import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation
from honeycomb._transformations._upper import _UpperFactory


def test_factory(spark):

    config = {'output_column': 'myoutputcolumn'}

    factory = _UpperFactory()
    transformation = factory.from_config(spark, 'mycolumn', config)

    assert transformation.column_name == 'mycolumn'


def test_transformation_upper(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
    ])
    input_df = spark.createDataFrame([['foo']], schema)

    transformation = Transformation(spark, input_df)
    transformation.upper('name')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'name'),
                 None)
    assert field is not None
    assert field.dataType == T.StringType()

    value = output_df.select('name').collect()[0].name
    assert value == 'FOO'
