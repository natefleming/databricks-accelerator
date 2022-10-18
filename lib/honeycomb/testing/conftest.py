import sys
import os
import logging
import pytest

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
sys.path.insert(0, os.path.abspath('../..'))


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('honeycomb-testing') \
        .getOrCreate()
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)
    yield spark
    spark.stop()
