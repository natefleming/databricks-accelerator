import os
import json

from typing import NamedTuple, List, Dict
from collections import OrderedDict
import logging

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import honeycomb.filesystem
from honeycomb._suggestions._Suggestion import _Suggestion
from honeycomb._suggestions._NotNull import _NotNull
from honeycomb._suggestions._Unique import _Unique
from honeycomb._suggestions._Min import _Min
from honeycomb._suggestions._Max import _Max
from honeycomb._suggestions._LengthBetween import _LengthBetween
from honeycomb._suggestions._Between import _Between
from honeycomb._suggestions._OneOf import _OneOf

LOGGER = logging.getLogger(__name__)


class SuggestedConstraint(NamedTuple):
    constraint_name: str
    data: dict

    def to_dict(self):
        return {'name': self.constraint_name, **self.data}


class SuggestionResult(NamedTuple):
    column_name: str
    constraints: List[SuggestedConstraint]

    def to_dict(self):
        return {
            'column': self.column_name,
            'constraints': [c.to_dict() for c in self.constraints]
        }


class SuggestionResults(NamedTuple):
    data: List[SuggestionResult]

    def to_dict(self):
        return {'columns': [c.to_dict() for c in self.data]}


class Suggestion(object):

    def __init__(self, spark: SparkSession, data_frame: DataFrame):
        self._spark = spark
        self._data_frame: DataFrame = data_frame
        self._fields: List[str] = data_frame.schema.fields
        self._suggestions: List[_Suggestion] = []
        for field in self._fields:
            self.not_null(field)
            self.unique(field)
            self.min(field)
            self.max(field)
            self.length_between(field)
            self.between(field)
            self.one_of(field)

    def not_null(self, column: T.StructField):
        LOGGER.info('Suggestion.not_null')
        self._add_suggestion(_NotNull(self._spark, column))
        return self

    def unique(self, column: T.StructField):
        LOGGER.info('Suggestion.unique')
        self._add_suggestion(_Unique(self._spark, column))
        return self

    def min(self, column: T.StructField):
        LOGGER.info('Suggestion.min')
        self._add_suggestion(_Min(self._spark, column))
        return self

    def max(self, column: T.StructField):
        LOGGER.info('Suggestion.max')
        self._add_suggestion(_Max(self._spark, column))
        return self

    def length_between(self, column: T.StructField):
        LOGGER.info('Suggestion.length_between')
        self._add_suggestion(_LengthBetween(self._spark, column))
        return self

    def between(self, column: T.StructField):
        LOGGER.info('Suggestion.between')
        self._add_suggestion(_Between(self._spark, column))
        return self

    def one_of(self, column: T.StructField):
        LOGGER.info('Suggestion.one_of')
        self._add_suggestion(_OneOf(self._spark, column))
        return self

    def execute(self) -> SuggestionResults:
        LOGGER.info(f'Suggestion.execute')
        suggestion_results: List[SuggestionResult] = []
        self._validate_suggestions()

        is_empty = self._data_frame.rdd.isEmpty()
        if not is_empty:
            for suggestion in self._suggestions:
                prepared_df = suggestion.prepare_df_for_check(self._data_frame)
                is_suggested, suggestion_df = suggestion.apply(prepared_df)

                LOGGER.info(f'is_suggested={is_suggested}')

                if is_suggested:
                    data = json.loads(
                        next(iter(suggestion_df.toJSON().collect()), '{}'))
                    suggested_constraint = SuggestedConstraint(
                        suggestion.suggestion_name(), data)
                    LOGGER.info(
                        f'Adding suggested_constraint={suggested_constraint}')
                    suggestion_result = next(
                        (s for s in suggestion_results
                         if s.column_name == suggestion.column.name), None)
                    if suggestion_result:
                        suggestion_result.constraints.append(
                            suggested_constraint)
                    else:
                        suggestion_result = SuggestionResult(
                            suggestion.column.name, [suggested_constraint])
                        suggestion_results.append(suggestion_result)

        return SuggestionResults(suggestion_results)

    def _add_suggestion(self, suggestion: _Suggestion) -> None:
        LOGGER.info(f'Suggestion._add_suggestion(suggestion={suggestion})')
        if suggestion.can_apply():
            existing = [
                s for s in self._suggestions
                if s.suggestion_name() == suggestion.suggestion_name() and
                s.column.name == suggestion.column.name
            ]
            if list(existing):
                raise ValueError(
                    f"Suggestion: {suggestion.suggestion_name()} for column: {suggestion.column.name} already exists."
                )

            self._suggestions.append(suggestion)

    def _validate_suggestions(self) -> None:
        LOGGER.info('Suggestion._validate_suggestions')
        columns = self._data_frame.columns

        errors = []
        for suggestion in self._suggestions:
            is_correct, error_message = suggestion.validate_self(
                self._data_frame, columns)
            if not is_correct:
                errors.append(error_message)
        if errors:
            raise ValueError(", ".join(errors))
