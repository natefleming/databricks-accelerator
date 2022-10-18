from abc import ABC
from typing import List, Tuple, Dict

from pyspark.sql import DataFrame

from honeycomb._constraints._constraint import _UnaryConstraint, _Constraint, _ConstraintFactory


class _Number(_UnaryConstraint, ABC):

    def __init__(self, constraint_name: str, column_name: str):
        super().__init__(constraint_name, column_name)

    def validate_self(self, df: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        parent_validation_result = super().validate_self(df, df_columns)
        if not parent_validation_result[0]:
            return parent_validation_result
        else:
            column_type = [
                dtype for name, dtype in df.dtypes
                if name.lower() == self.column_name
            ][0]
            return column_type in [
                'tinyint', 'smallint', 'int', 'bigint', 'short', 'float',
                'double', 'decimal', 'long'
            ], f'Column {self.column_name} is not a number'


class _Min(_Number):

    def __init__(self, column_name: str, value: int):
        super().__init__('min', column_name)
        self.value = value

    def filter_success(self, df: DataFrame) -> DataFrame:
        return df.filter(f'{self.column_name} >= {self.value}')

    def filter_failure(self, df: DataFrame) -> DataFrame:
        return df.filter(f'{self.column_name} < {self.value}')


class _MinFactory(_ConstraintFactory):

    def __init__(self):
        super().__init__('min')

    def from_config(self, column_name: str, config: Dict) -> _Constraint:
        value = config['value']
        return _Min(column_name, value)


class _Max(_Number):

    def __init__(self, column_name: str, value: int):
        super().__init__('max', column_name)
        self.value = value

    def filter_success(self, df: DataFrame) -> DataFrame:
        return df.filter(f'{self.column_name} <= {self.value}')

    def filter_failure(self, df: DataFrame) -> DataFrame:
        return df.filter(f'{self.column_name} > {self.value}')


class _MaxFactory(_ConstraintFactory):

    def __init__(self):
        super().__init__('max')

    def from_config(self, column_name: str, config: Dict) -> _Constraint:
        value = config['value']
        return _Max(column_name, value)


class _Between(_Number):

    def __init__(self, column_name: str, lower_bound: int, upper_bound: int):
        super().__init__('between', column_name)
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound

    def filter_success(self, df: DataFrame) -> DataFrame:
        return df.filter(
            f'{self.column_name} >= {self._lower_bound} AND {self.column_name} <= {self._upper_bound}'
        )

    def filter_failure(self, df: DataFrame) -> DataFrame:
        return df.filter(
            f'{self.column_name} < {self._lower_bound} OR {self.column_name} > {self._upper_bound}'
        )

    def validate_self(self, df: DataFrame,
                      df_columns: List[str]) -> Tuple[bool, str]:
        parent_validation_result = super().validate_self(df, df_columns)
        if not parent_validation_result[0]:
            return parent_validation_result
        else:
            return self._lower_bound <= self._upper_bound, f'Upper bound ({self._upper_bound}) cannot be lower than lower bound ({self._lower_bound}).'


class _BetweenFactory(_ConstraintFactory):

    def __init__(self):
        super().__init__('between')

    def from_config(self, column_name: str, config: Dict) -> _Constraint:
        lower_bound = config['lower_bound']
        upper_bound = config['upper_bound']
        return _Between(column_name, lower_bound, upper_bound)
