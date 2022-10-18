from typing import List, Tuple, Dict
from pyspark.sql import DataFrame

from honeycomb._constraints._constraint import _UnaryConstraint, _Constraint, _ConstraintFactory


class _NotNull(_UnaryConstraint):

    def __init__(self, column_name: str):
        super().__init__('not_null', column_name)

    def filter_success(self, df: DataFrame) -> DataFrame:
        return df.filter(f"{self.column_name} IS NOT NULL")

    def filter_failure(self, df: DataFrame) -> DataFrame:
        return df.filter(f"{self.column_name} IS NULL")


class _NotNullFactory(_ConstraintFactory):

    def __init__(self):
        super().__init__('not_null')

    def from_config(self, column_name: str, config: Dict) -> _Constraint:
        return _NotNull(column_name)
