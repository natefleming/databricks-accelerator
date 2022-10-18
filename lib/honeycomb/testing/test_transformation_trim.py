import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.transform import Transformation
from honeycomb._transformations._trim import _TrimFactory


def test_factory(spark):

    config = {'output_column': 'myoutputcolumn'}

    factory = _TrimFactory()
    transformation = factory.from_config(spark, 'mycolumn', config)

    assert transformation.column_name == 'mycolumn'
    assert transformation.output_column == config['output_column']


def test_transformation_trim(spark):
    schema = T.StructType([
        T.StructField('age', T.StringType()),
    ])
    input_df = spark.createDataFrame([['  10   ']], schema)

    transformation = Transformation(spark, input_df)
    transformation.trim('age')
    transformation_result = transformation.execute()
    output_df = transformation_result.data
    field = next(iter(f for f in output_df.schema.fields if f.name == 'age'),
                 None)
    assert field is not None
    assert field.dataType == T.StringType()

    value = output_df.select('age').collect()[0].age
    assert value == '10'
