import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb import Honeycomb


def test_filesystem_is_not_null(spark):

    hc = Honeycomb(spark)
    assert hc.filesystem is not None


def test_table_is_not_null(spark):

    hc = Honeycomb(spark)
    assert hc.table is not None
