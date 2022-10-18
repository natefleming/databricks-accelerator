import os

from typing import List, Callable
import re
import string

from pyspark.sql import SparkSession, DataFrame, Column

import pyspark.sql.functions as F
import pyspark.sql.types as T

import honeycomb.filesystem


class Schema(object):

    def __init__(self, config: dict = None):
        self._schema = None
        if config:
            self._load(config)

    @property
    def schema(self):
        return self._schema

    def _load(self, config: dict):
        data = config.get('schema')
        self._schema = T.StructType.fromJson(data) if data else None


def rename_nested_column(spark: SparkSession, df: DataFrame, old_col_name: str,
                         new_col_name: str) -> DataFrame:
    """
    Renames a column or nested column.
    :param column_name: The old column name. For example: app.device.id
    :param new_column_name: The new column name. For example: _ID
    :return: A new dataframe with the update schema
    """

    def process_type(data_type: T.DataType, full_col_name: str,
                     old_col_name: str, new_col_name: str) -> T.DataType:
        result: T.DataType = data_type
        if isinstance(data_type, T.StructType):
            struct_type: T.StructType = data_type
            fields: List[T.StructField] = []
            for f in struct_type.fields:
                name: str = f.name
                if full_col_name is not None:
                    name = f"{full_col_name}.{f.name}"
                fields += [process_field(f, name, old_col_name, new_col_name)]
            result = T.StructType(fields)
        return result

    def process_field(struct_field: T.StructField, full_col_name: str,
                      old_col_name: str, new_col_name: str) -> T.StructField:
        result: T.StructField = struct_field
        if full_col_name == old_col_name:
            result = T.StructField(new_col_name, struct_field.dataType,
                                   struct_field.nullable)
        elif old_col_name.startswith(full_col_name):
            result = T.StructField(
                struct_field.name,
                process_type(struct_field.dataType, full_col_name, old_col_name,
                             new_col_name), struct_field.nullable)
        return result

    return spark.createDataFrame(
        df.rdd, process_type(df.schema, None, old_col_name, new_col_name))


def special_chars_and_ws_to_underscore(name: str) -> str:
    return re.sub(
        r'[' + re.escape(string.punctuation) + string.whitespace + ']', '_',
        name)


def rename_columns(
    data_type: T.DataType,
    converter: Callable[[str], str] = special_chars_and_ws_to_underscore
) -> T.DataType:
    """
    Renames all columns and struct attributes using the provided function
    :param data type: The data type (ie struct type)
    :param data type: A new data type with converted names
    :return: A new data type
    """

    def process_field(field: T.StructField,
                      converter: Callable[[str], str]) -> T.StructField:
        if isinstance(field.dataType, T.StructType):
            return T.StructField(converter(field.name),
                                 process_type(field.dataType, converter),
                                 field.nullable)
        else:
            return T.StructField(converter(field.name), field.dataType,
                                 field.nullable)

    def process_type(data_type: T.DataType,
                     converter: Callable[[str], str]) -> T.DataType:
        if isinstance(data_type, T.StructType):
            struct_type: T.StructType = data_type
            fields: List[T.StructField] = [
                process_field(f, converter) for f in struct_type.fields
            ]
            return T.StructType(fields)
        return data_type

    return process_type(data_type, converter)


def rename_columns_df(
    spark: SparkSession,
    df: DataFrame,
    converter: Callable[[str], str] = special_chars_and_ws_to_underscore
) -> DataFrame:
    schema: T.StructType = rename_columns(df.schema, converter)
    return spark.createDataFrame(df.rdd, schema)
