import os
import logging

from typing import NamedTuple, List

from pyspark.sql import DataFrame, SparkSession

import honeycomb.filesystem
import honeycomb.utils

from honeycomb._constraints._constraint import _Constraint, _ConstraintFactory
from honeycomb._constraints._notnull import _NotNull
from honeycomb._constraints._numbers import _Min, _Max, _Between
from honeycomb._constraints._oneof import _OneOf
from honeycomb._constraints._textlength import _TextLength
from honeycomb._constraints._textregex import _TextRegex
from honeycomb._constraints._unique import _Unique
from honeycomb._constraints._uniqueset import _UniqueSet

LOGGER = logging.getLogger(__name__)


class ValidationError(NamedTuple):
    column_names: List[str]
    constraint_name: str
    number_of_errors: int


class ValidationResult(NamedTuple):
    correct_data: DataFrame
    erroneous_data: DataFrame
    errors: List[ValidationError]


class Validation(object):
    """
    Describes the validation rules of a Spark DataFrame and performs the validation.

    // TODO update the example when there is a new validation rule
    Usage example:
        Validation(spark_session, df) \
            .is_not_null("column_name") \
            .are_not_null(["column_name_2", "column_name_3"]) \
            .is_min("numeric_column", 10) \
            .is_max("numeric_column", 20) \
            .is_unique("column_name") \
            .are_unique(["column_name_2", "column_name_3"]) \
            .execute()
    """

    def __init__(self, spark: SparkSession, df: DataFrame, config: dict = None):
        self._spark: SparkSession = spark
        self._df: DataFrame = df
        self._input_columns: List[str] = df.columns
        self._constraints: List[_Constraint] = []
        if config:
            self._load(config)

    def is_unique(self, column_name: str):
        """
        Defines a constraint that checks whether the given column contains only unique values.
        :param column_name: the name of the column
        :raises ValueError: if an unique constraint for a given column already exists.
        :return: self
        """
        self._add_constraint(_Unique(column_name))
        return self

    def is_primary_key(self, column_name: str):
        """
        Defines a constraint that checks whether the given column contains only unique values.
        :param column_name: the name of the column
        :raises ValueError: if an unique constraint for a given column already exists.
        :return: self
        """
        self._add_constraint(_Unique(column_name))
        return self

    def is_composite_key(self, column_names: List[str]):
        """
        Defines a constraint that checks whether the given columns contains only unique combinations.
        :param column_name: the name of the column
        :raises ValueError: if an unique constraint for a given column already exists.
        :return: self
        """
        return self.are_unique(column_names)

    def are_unique(self, column_names: List[str]):
        """
        Defines constraints that check whether given columns contain only unique values.
        :param column_names: a list of column names
        :return: self
        """
        self._add_constraint(_UniqueSet(column_names))
        return self

    def are_primary_keys(self, column_names: List[str]):
        """
        Defines constraints that check whether given columns contain only unique values.
        :param column_names: a list of column names
        :return: self
        """
        for column_name in column_names:
            self.is_unique(column_name)
        return self

    def is_not_null(self, column_name: str):
        """
        Defines a constraint that does not allow null values in a given column.
        :param column_name: the column name
        :return: self
        """
        self._add_constraint(_NotNull(column_name))
        return self

    def are_not_null(self, column_names: List[str]):
        """
        Defines constraints that don't allow null values in all of the given columns
        :param column_names: a list of column names
        :return: self
        """
        for column_name in column_names:
            self.is_not_null(column_name)
        return self

    def is_min(self, column_name: str, value: int):
        """
        Defines a constraint that check whether the given column contains values equal or larger than a given integer.
        :param column_name: the column name
        :param value: the minimal value
        :return: self
        """
        self._add_constraint(_Min(column_name, value))
        return self

    def is_max(self, column_name: str, value: int):
        """
        Defines a constraint that check whether the given column contains values equal or smaller than a given integer.
        :param column_name: the column name
        :param value: the maximal value
        :return: self
        """
        self._add_constraint(_Max(column_name, value))
        return self

    def is_between(self, column_name, lower_bound, upper_bound):
        """
        Defines a constraint that checks whether the given column contains a value equal to or between the lower and upper bound.
        :param column_name: the column name
        :param lower_bound: the lower bound of the range
        :param upper_bound: the upper bound of the range
        :return: self
        """
        self._add_constraint(_Between(column_name, lower_bound, upper_bound))
        return self

    def has_length_between(self, column_name, lower_bound, upper_bound):
        """
        Defines a constraint that checks whether the given column contains a text which length is equal to or between the lower and upper bound.
        :param column_name: the column name
        :param lower_bound: the lower bound of the text length
        :param upper_bound: the upper bound of the text length
        :return: self
        """
        self._add_constraint(_TextLength(column_name, lower_bound, upper_bound))
        return self

    def text_matches_regex(self, column_name, regex):
        """
        Defines a constraint that checks whether the content of a given column matches the given regex.
        :param column_name: the column name
        :param regex: the regex
        :return: self
        """
        self._add_constraint(_TextRegex(column_name, regex))
        return self

    def one_of(self, column_name, allowed_values: list):
        """
        Defines a constraint that checks whether the column value is equal to one of the given values.
        :param column_name: the column name
        :param allowed_values: a list of allowed values, the type should match the column type
        :return: self
        """
        self._add_constraint(_OneOf(column_name, allowed_values))
        return self

    def execute(self) -> ValidationResult:
        """
        Returns a named tuple containing the data that passed the validation, the data that was rejected (only unique rows), and a list of violated constraints.
        Note that the order of rows and constraints is not preserved.

        :raises ValueError: if a constraint has been defined using a non-existing column.
        :return:
        """
        self._validate_constraints()

        if self._constraints:
            for constraint in self._constraints:
                self._df = constraint.prepare_df_for_check(self._df)

            correct_output = self._df
            errors = []

            for constraint in self._constraints:
                correct_output = constraint.filter_success(correct_output)
                number_of_failures = constraint.filter_failure(self._df).count()

                if number_of_failures > 0:
                    errors.append(
                        ValidationError(constraint.column_names,
                                        constraint.constraint_name,
                                        number_of_failures))

            correct_output = correct_output.select(self._input_columns)
            incorrect_output = self._df.select(
                self._input_columns).subtract(correct_output)

            return ValidationResult(correct_output, incorrect_output, errors)
        else:
            return ValidationResult(
                self._df, self._spark.createDataFrame([], self._df.schema), [])

    def _add_constraint(self, constraint: _Constraint) -> None:
        existing = filter(lambda c: c == constraint, self._constraints)
        if list(existing):
            raise ValueError(
                f"An not_null constraint for column {constraint.column_name} already exists."
            )

        self._constraints.append(constraint)

    def _validate_constraints(self) -> None:
        columns = [c.lower() for c in self._df.columns]

        errors = []
        for constraint in self._constraints:
            is_correct, error_message = constraint.validate_self(
                self._df, columns)
            if not is_correct:
                errors.append(error_message)

        if errors:
            raise ValueError(", ".join(errors))

    def _load(self, config: dict):
        factories = [
            member() for member in honeycomb.utils.find_members(
                'honeycomb._constraints',
                honeycomb.utils.is_child_of(_ConstraintFactory))
        ]
        factory_map = {factory.name: factory for factory in factories}

        LOGGER.debug(f'factory_map: {factory_map}')

        root = config.get('validation', {})
        constraints = root.get('constraints', [])
        for constraint in constraints:
            constraint_name = constraint['name']
            if constraint_name not in factory_map:
                raise ValueError(
                    f'Invalid constraint parameter: {constraint_name}')
            self._add_constraint(factory_map[constraint_name].from_config(
                None, constraint))

        columns = root.get('columns', [])
        for column in columns:
            column_name = column['column']
            constraints = column['constraints']
            for constraint in constraints:
                constraint_name = constraint['name']
                if constraint_name not in factory_map:
                    raise ValueError(
                        f'Invalid constraint parameter: {constraint_name}')
                self._add_constraint(factory_map[constraint_name].from_config(
                    column_name, constraint))
