import pytest

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd

from honeycomb.metadata import Metadata

from testing import sort_data_frame


def test_metadata_add_column_metadata(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    metadata = Metadata(spark, input_df)
    metadata.add_column_metadata('name', 'primary_key', 'true')
    metadata_results = metadata.execute()

    actual_df = metadata_results.data

    field = next(iter(f for f in actual_df.schema.fields if f.name == 'name'))

    assert 'primary_key' in field.metadata
    assert field.metadata['primary_key'] == 'true'


def test_metadata_add_dataset_metadata(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    metadata = Metadata(spark, input_df)
    metadata.add_dataset_metadata('primary_key', 'true')

    assert metadata.dataset()[0].metadata_name() == 'primary_key'
    assert metadata.dataset()[0].metadata_value() == 'true'


def test_has_dataset_metadata(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    metadata = Metadata(spark, input_df)
    metadata.add_dataset_metadata('primary_key', 'true')

    assert metadata.has_dataset_metadata('primary_key')


def test_has_column_metadata(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    metadata = Metadata(spark, input_df)
    metadata.add_column_metadata('name', 'primary_key', 'true')

    assert metadata.has_column_metadata('name', 'primary_key')


def test_find_dataset_metadata(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    metadata = Metadata(spark, input_df)
    metadata.add_dataset_metadata('primary_key', 'true')

    assert metadata.find_dataset_metadata('primary_key') == 'true'


def test_find_missing_dataset_metadata(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    metadata = Metadata(spark, input_df)
    metadata.add_dataset_metadata('primary_key', 'true')

    assert metadata.find_dataset_metadata('missing') is None


def test_find_column_metadata(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    metadata = Metadata(spark, input_df)
    metadata.add_column_metadata('name', 'primary_key', 'true')

    assert metadata.find_column_metadata('name', 'primary_key') == 'true'


def test_find_missing_column_metadata(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    metadata = Metadata(spark, input_df)
    metadata.add_column_metadata('name', 'primary_key', 'true')

    assert metadata.find_column_metadata('missing', 'primary_key') is None


def test_metadata_load(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    config = {
        'metadata': {
            'dataset': {
                'metadata': [{
                    'hc_secure': {
                        'columns': ['foo', 'bar']
                    }
                }]
            },
            'columns': [{
                'column': 'name',
                'metadata': [{
                    'primary_key': 'true'
                }]
            },]
        }
    }

    metadata = Metadata(spark, input_df, config)
    metadata_results = metadata.execute()

    actual_df = metadata_results.data

    field = next(iter(f for f in actual_df.schema.fields if f.name == 'name'))

    assert 'primary_key' in field.metadata
    assert field.metadata['primary_key'] == 'true'

    assert metadata.has_dataset_metadata('hc_secure')
    hc_secure = metadata.find_dataset_metadata('hc_secure')
    assert 'columns' in hc_secure
    assert hc_secure['columns'] == ['foo', 'bar']


def test_is_secure_dataset(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    config = {
        'metadata': {
            'dataset': {
                'metadata': [{
                    'hc_row_access': {
                        'columns': ['foo', 'bar']
                    }
                }]
            },
            'columns': [{
                'column': 'name',
                'metadata': [{
                    'primary_key': 'true'
                }]
            },]
        }
    }

    metadata = Metadata(spark, input_df, config)

    assert metadata.is_secure()


def test_is_secure_column(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    config = {
        'metadata': {
            'dataset': {
                'metadata': [{
                    'hc_not_secure': {
                        'columns': ['foo', 'bar']
                    }
                }]
            },
            'columns': [{
                'column': 'name',
                'metadata': [{
                    'hc_masking': 'true'
                }]
            },]
        }
    }

    metadata = Metadata(spark, input_df, config)

    assert metadata.is_secure()


def test_is_not_secure(spark):
    schema = T.StructType([T.StructField('name', T.StringType())])
    input_df = spark.createDataFrame([], schema)

    config = {
        'metadata': {
            'dataset': {
                'metadata': [{
                    'hc_not_secure': {
                        'columns': ['foo', 'bar']
                    }
                }]
            },
            'columns': [{
                'column': 'name',
                'metadata': [{
                    'primary_key': 'true'
                }]
            },]
        }
    }

    metadata = Metadata(spark, input_df, config)

    assert not metadata.is_secure()
