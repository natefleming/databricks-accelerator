import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation
from honeycomb._transformations._lower import _LowerFactory


def test_factory(spark):

    config = {'output_column': 'myoutputcolumn'}

    factory = _LowerFactory()
    transformation = factory.from_config(spark, 'mycolumn', config)

    assert transformation.output_column == config['output_column']


def test_transformation_lower(spark):
    schema = T.StructType([
        T.StructField('name', T.StringType()),
    ])
    input_df = spark.createDataFrame([['FOO']], schema)

    transformation = Transformation(spark, input_df)
    transformation.lower('name')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'name'),
                 None)
    assert field is not None
    assert field.dataType == T.StringType()

    value = output_df.select('name').collect()[0].name
    assert value == 'foo'
