import pytest
import json

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.schema import Schema


def test_schema_load(spark):
    lhs_schema = T.StructType([
        T.StructField('name', T.StringType()),
        T.StructField('age', T.IntegerType())
    ])
    config = {}
    config['schema'] = json.loads(lhs_schema.json())

    rhs_schema = Schema(config)

    assert lhs_schema == rhs_schema.schema
