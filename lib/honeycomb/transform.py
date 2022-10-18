import os
import logging
import sys

from typing import NamedTuple, List, Any

from pyspark.sql import DataFrame, SparkSession

import honeycomb.filesystem
import honeycomb.utils

from honeycomb._transformations._transformation import _Transformation
from honeycomb._transformations._minmax import _MinMax
from honeycomb._transformations._literal import _Literal
from honeycomb._transformations._cast import _Cast
from honeycomb._transformations._regexreplace import _RegexReplace
from honeycomb._transformations._encrypt import _Encrypt
from honeycomb._transformations._totimestamp import _ToTimestamp
from honeycomb._transformations._todate import _ToDate
from honeycomb._transformations._drop import _Drop
from honeycomb._transformations._skipemptyrows import _SkipEmptyRows
from honeycomb._transformations._rename import _Rename
from honeycomb._transformations._trim import _Trim
from honeycomb._transformations._upper import _Upper
from honeycomb._transformations._lower import _Lower
from honeycomb._transformations._flatten import _Flatten

from honeycomb._transformations._transformation import _TransformationFactory

LOGGER = logging.getLogger(__name__)


class TransformationResult(NamedTuple):
    data: DataFrame


class Transformation(object):
    """
    Describes the validation rules of a Spark DataFrame and performs the validation.

    // TODO update the example when there is a new validation rule
    Usage example:
        Transformation(spark_session, data_frame) \
            .min_max("column_name", lower_bound, upper_bound) \
            .execute()
    """

    def __init__(self,
                 spark: SparkSession,
                 data_frame: DataFrame,
                 config: dict = None):
        self._spark: SparkSession = spark
        self._df: DataFrame = data_frame
        self._input_columns: List[str] = data_frame.columns
        self._transformations: List[_Transformation] = []
        if config:
            self._load(config)

    def rename(self, column_name: str, new_column_name: str):
        """
        Defines a transformation that creates renames a column
        :param column_name: The name of the column
        :param new_column_name: The new column name
        :raises ValueError: if an unique transformation for a given new column already exists.
        :return: self
        """
        self._add_transformation(
            _Rename(self._spark, column_name, new_column_name))
        return self

    def encrypt(self,
                column_name: str,
                num_bits: int = 256,
                output_column=None):
        """
        Defines a transformation that creates a SHA2 hash of the provided column
        :param column_name: the name of the column
        :param num_bits: An optional number of bits. Must be one of 224, 256, 384, 512
        :param output_column: An optional output column 
        :raises ValueError: if an unique transformation for a given column already exists.
        :return: self
        """
        self._add_transformation(_Encrypt(column_name, num_bits, output_column))
        return self

    def replace(self,
                column_name: str,
                pattern: str,
                replacement: str,
                output_column=None):
        """
        Defines a transformation that replaces the matched pattern with replacememnt
        :param column_name: the name of the column
        :param pattern: the regex pattern
        :param replacement: the replacement string
        :param output_column: An optional output column 
        :raises ValueError: if an unique transformation for a given column already exists.
        :return: self
        """
        self._add_transformation(
            _RegexReplace(column_name, pattern, replacement, output_column))
        return self

    def min_max(self,
                column_name: str,
                lower_bound: int = 0,
                upper_bound: int = 1,
                output_column=None):
        """
        Defines a transformation that normalized a value to a value between lower_bound and upper_bound
        :param column_name: the name of the column
        :raises ValueError: if an unique transformation for a given column already exists.
        :return: self
        """
        self._add_transformation(
            _MinMax(column_name, lower_bound, upper_bound, output_column))
        return self

    def literal(self, column_name: str, value: Any, data_type: str = 'string'):
        """
        Defines a transformation that appends a literal value to a dataframe
        :param column_name: the name of the column
        :param value: the value of the literal
        :param data_type: the data type of the literal
        :raises ValueError: if an unique transformation for a given column already exists.
        :return: self
        """
        self._add_transformation(_Literal(column_name, value, data_type))
        return self

    def cast(self, column_name: str, data_type: str, output_col: str = None):
        """
        Defines a transformation that appends a literal value to a dataframe
        :param column_name: the name of the column
        :param value: the value of the literal
        :param data_type: the data type of the literal
        :raises ValueError: if an unique transformation for a given column already exists.
        :return: self
        """
        self._add_transformation(_Cast(column_name, data_type, output_col))
        return self

    def to_timestamp(self,
                     column_name: str,
                     format: str,
                     output_col: str = None):
        """
        Defines a transformation that converts a string value to a timestamp
        :param column_name: the name of the column
        :param format: the timestamp format
        :raises ValueError: if an unique transformation for a given column already exists.
        :return: self
        """
        self._add_transformation(_ToTimestamp(column_name, format, output_col))
        return self

    def to_date(self, column_name: str, format: str, output_col: str = None):
        """
        Defines a transformation that converts a string value to a date
        :param column_name: the name of the column
        :param format: the timestamp format
        :raises ValueError: if an unique transformation for a given column already exists.
        :return: self
        """
        self._add_transformation(_ToDate(column_name, format, output_col))
        return self

    def drop(self, column_name: str):
        """
        Defines a transformation that drops a column
        :param column_name: the name of the column
        :raises ValueError: if the column does not exist
        :return: self
        """
        self._add_transformation(_Drop(column_name))
        return self

    def skip_empty_rows(self):
        """
        Defines a transformation that skips empty rows
        :return: self
        """
        self._add_transformation(_SkipEmptyRows())
        return self

    def trim(self, column_name: str, output_col: str = None):
        """
        Defines a transformation that skips empty rows
        :return: self
        """
        self._add_transformation(_Trim(column_name, output_col))
        return self

    def upper(self, column_name: str, output_col: str = None):
        """
        Defines a transformation that skips empty rows
        :return: self
        """
        self._add_transformation(_Upper(column_name, output_col))
        return self

    def lower(self, column_name: str, output_col: str = None):
        """
        Defines a transformation that skips empty rows
        :return: self
        """
        self._add_transformation(_Lower(column_name, output_col))
        return self

    def flatten(self, column_name: str, prefix: str = None):
        """
        Defines a transformation that flattens a column
        :return: self
        """
        self._add_transformation(_Flatten(column_name, prefix))
        return self

    def execute(self) -> TransformationResult:
        """
        Returns a named tuple containing the data that passed the validation, the data that was rejected (only unique rows), and a list of violated transformations.
        Note that the order of rows and transformations is not preserved.

        :raises ValueError: if a transformation has been defined using a non-existing column.
        :return:
        """
        self._validate_transformations()

        if self._transformations:
            for transformation in self._transformations:
                self._df = transformation.prepare_df_for_check(self._df)

            correct_output = self._df
            output_columns = []
            remove_columns = []
            for transformation in self._transformations:
                correct_output = transformation.apply(correct_output)
                output_columns += transformation.output_columns()
                remove_columns += transformation.remove_columns()

            input_columns = [
                c for c in self._input_columns if c not in remove_columns
            ]
            correct_output = correct_output.select(input_columns +
                                                   output_columns)

            return TransformationResult(correct_output)
        else:
            return TransformationResult(self._df)

    def _add_transformation(self, transformation: _Transformation) -> None:
        existing = filter(
            lambda c: c.transformation_name(
            ) == transformation.transformation_name() and c.column_name ==
            transformation.column_name, self._transformations)
        if list(existing):
            LOGGER.warn(
                f'An not_null transformation for column {transformation.column_name} already exists.'
            )

        self._transformations.append(transformation)

    def _validate_transformations(self) -> None:
        columns = self._df.columns

        errors = []
        for transformation in self._transformations:
            is_correct, error_message = transformation.validate_self(
                self._df, columns)
            if not is_correct:
                errors.append(error_message)

        if errors:
            raise ValueError(', '.join(errors))

    def _load(self, config: dict):
        factories = [
            member() for member in honeycomb.utils.find_members(
                'honeycomb._transformations',
                honeycomb.utils.is_child_of(_TransformationFactory))
        ]
        factory_map = {factory.name: factory for factory in factories}

        LOGGER.debug(f'factory_map: {factory_map}')

        root = config.get('transformation', {})
        transformations = root.get('transformations', [])
        for transformation in transformations:
            transformation_name = transformation['name']
            if transformation_name not in factory_map:
                raise ValueError(
                    f'Invalid transformation parameter: {transformation_name}')
            self._add_transformation(
                factory_map[transformation_name].from_config(
                    self._spark, None, transformation))

        columns = root.get('columns', [])
        for column in columns:
            column_name = column['column']
            transformations = column['transformations']
            for transformation in transformations:
                transformation_name = transformation['name']
                if transformation_name not in factory_map:
                    raise ValueError(
                        f'Invalid transformation parameter: {transformation_name}'
                    )
                self._add_transformation(
                    factory_map[transformation_name].from_config(
                        self._spark, column_name, transformation))
